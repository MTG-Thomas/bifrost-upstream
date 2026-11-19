"""Reconnect-aware ProcessPoolManager adapter for a remote launcher.

The API process remains the canonical execution owner: it keeps the Redis
leases, completion callbacks, cancellation subscription, and service claim
loop.  The launcher owns every OS process and its cgroup, so this adapter
never reads or signals a local PID.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Protocol, TypeGuard, cast
from uuid import UUID, uuid4

from sqlalchemy import func, select, update

from shared.execution_context import validate_execution_context
from src.config import get_settings
from src.core.cache.keys import active_execution_key
from src.core.redis_client import ActiveExecution
from src.services.execution.process_pool import (
    DANGEROUS_MEMORY_RATIO,
    CredentialRoute,
    ExecutionInfo,
    ProcessHandle,
    ProcessPoolManager,
    ProcessState,
    ServiceInfo,
)
from src.worker.remote.protocol import (
    CLOUDFLARE_DEV_BUDGET_BYTES,
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    CloudflareRevisionRejected,
    LINUX_RUNTIME_PROFILE,
    CredentialRouteChanged,
    OpaqueDispatchInterrupted,
    OpaqueWorkflowNotStarted,
)

if TYPE_CHECKING:
    from src.worker.admission import Admission
    from src.worker.remote.ownership import RemoteOwnerLease

logger = logging.getLogger(__name__)

# Why a child still running when the drain grace runs out was stopped. Agent
# children end their runs with the same reason (``RemoteAgentConsumer``).
DRAIN_DEADLINE_ERROR_TYPE = "WorkerShutdown"
DRAIN_DEADLINE_ERROR = "Remote pool drain deadline exceeded"
OPAQUE_STOP_TIMEOUT_SECONDS = 5
OPAQUE_ATTEMPT_STATES = frozenset({"not_started", "completed", "uncertain"})


class ThinRemoteTransport(Protocol):
    """Control channel supplied by the API-side thin-runner host."""

    connected: bool
    hostname: str
    # The current launcher session (RemoteOwnerLease): its generation and
    # the credential protocol the launcher reported.
    lease: Any
    admission: Admission | None

    async def request(
        self, action: str, payload: dict[str, Any], *, expected_generation: int | None = None
    ) -> dict[str, Any]:
        """Send an acknowledged physical-runner command, refused when
        ``expected_generation`` is no longer the session's."""
        raise NotImplementedError

    def memory(self) -> tuple[int, int]:
        """Return the physical container's current usage and limit."""
        raise NotImplementedError

    def memory_request_bytes(self, key: str) -> int | None:
        """Return the canonical reservation for a running route."""
        raise NotImplementedError

    async def wait_for_opaque_terminals(
        self, receipts: set[str], lease: RemoteOwnerLease,
    ) -> dict[str, dict[str, Any]]:
        """Return retained result events replayed after the inventory."""
        raise NotImplementedError

    async def acknowledge(
        self, event_id: str, *, reconciliation_lease: RemoteOwnerLease | None = None,
    ) -> None:
        """Release a retained terminal after durable settlement."""
        raise NotImplementedError


class _RemoteProcess:
    """PID-less ProcessHandle shim; remote liveness arrives as launcher events."""

    def __init__(self) -> None:
        self.exitcode: int | None = None
        self.remote_pid: int | None = None
        self.runtime_handle: str | None = None
        self.memory_request_bytes: int | None = None
        self.stop_generation: int | None = None
        self.stop_reason: tuple[str, str] | None = None
        self.stop_confirmed = False
        self.stop_task: asyncio.Task[None] | None = None
        self.terminal_receipt: tuple[str, str] | None = None

    def is_alive(self) -> bool:
        return self.exitcode is None


class ThinRemoteProcessPool(ProcessPoolManager):
    """Use canonical pool bookkeeping while dispatching OS work remotely.

    A dropped control socket only pauses new work. Existing OS children stay
    alive in the physical launcher and are reconciled after its signed
    reconnect. ``ownership_lost`` is the separate, fenced terminal path.
    """

    def __init__(
        self,
        transport: ThinRemoteTransport,
        *,
        worker_id: str | None = None,
        runtime_profile: str = LINUX_RUNTIME_PROFILE,
        **kwargs: Any,
    ) -> None:
        if runtime_profile not in (LINUX_RUNTIME_PROFILE, CLOUDFLARE_PYTHON_RUNTIME_PROFILE):
            raise ValueError("Unsupported physical runtime profile")
        identity = worker_id or (
            f"thin-remote:{os.environ.get('HOSTNAME', 'api')}"
        )
        super().__init__(worker_id=identity, **kwargs)
        self.transport = transport
        self.runtime_profile = runtime_profile
        self._routes: dict[str, ProcessHandle] = {}
        self._route_metrics: dict[str, dict[str, float | int]] = {}
        self._remote_memory_current = -1
        self._remote_memory_limit = -1
        self._lease_refresh_task: asyncio.Task[None] | None = None
        self._result_locks: dict[str, asyncio.Lock] = {}
        self._opaque_lost = False

    async def reconcile_inventory(
        self, inventory: dict[str, Any], *, admission: Admission | None = None,
    ) -> dict[str, Any] | None:
        """Reattach physical children only to their still-authoritative claims.

        The launcher journals compact transport metadata until terminal ACK.
        PostgreSQL validates that identity; it never trusts inventory as a new
        claim. Finished but unacknowledged routes need the same reconstruction
        as live children so completion can be replayed after an API restart.
        """
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            lease = self.transport.lease
            async with self._restart_lock:
                if self.transport.lease != lease or not self.transport.connected:
                    raise ConnectionError("Opaque inventory session was replaced during dispatch")
                return await self._reconcile_opaque_inventory(inventory, lease, admission)

        from src.core.database import get_db_context
        from src.jobs.platform.catalog import get_platform_job_metadata
        from src.jobs.schedulers.platform_jobs import ClaimedPlatformJob
        from src.models.enums import ExecutionStatus
        from src.models.orm.agent_runs import AgentRun
        from src.models.orm.execution_workers import ExecutionWorkerClaim
        from src.models.orm.executions import Execution
        from src.models.orm.platform_jobs import PlatformJob
        from src.models.orm.services import ServiceAttempt, ServiceDefinition
        from src.services.service_claim import OwnedAttempt
        from src.worker.remote.fencing import fence_remote_owner

        if inventory.get("version") != 1:
            raise ValueError("Unsupported launcher inventory")
        entries = inventory.get("routes", []) + inventory.get("terminal_routes", [])
        if not all(isinstance(entry, dict) for entry in entries):
            raise ValueError("Invalid launcher inventory")
        workflows: dict[str, int] = {}
        services: list[OwnedAttempt] = []
        jobs: list[tuple[str, ClaimedPlatformJob]] = []
        agent_runs: dict[str, tuple[UUID, int]] = {}
        summaries: dict[str, tuple[UUID, int, str, UUID]] = {}
        observed: set[str] = set()
        rejected: list[tuple[str, str]] = []
        async with get_db_context() as db:
            if not await fence_remote_owner(db):
                raise ConnectionError("Remote API ownership was replaced")
            for entry in entries:
                route_id, kind = entry.get("route_id"), entry.get("kind")
                if not isinstance(route_id, str) or route_id in observed:
                    continue
                observed.add(route_id)
                subject = UUID(str(entry["subject_id"]))
                request_bytes = entry.get("memory_request_bytes")
                if isinstance(request_bytes, bool) or not isinstance(request_bytes, int) or request_bytes < 0:
                    raise ValueError("Launcher route is missing its memory reservation")
                if kind == "workflow":
                    valid = await db.scalar(select(Execution.id).join(
                        ExecutionWorkerClaim,
                        ExecutionWorkerClaim.subject_id == Execution.id,
                    ).where(
                        Execution.id == subject,
                        Execution.status.in_((ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING)),
                        ExecutionWorkerClaim.worker_id == UUID(self.worker_id),
                        ExecutionWorkerClaim.family == "workflow_execution",
                    ))
                    if valid is None:
                        rejected.append((route_id, kind))
                        continue
                    active = entry.get("active_execution")
                    if not isinstance(active, dict) or active.get("execution_id") != str(subject):
                        raise ValueError("Launcher workflow metadata is missing or mismatched")
                    started_at = datetime.fromisoformat(entry["started_at"])
                    execution = ExecutionInfo(
                        execution_id=str(subject), started_at=started_at,
                        timeout_seconds=int(entry["timeout_seconds"]),
                        active_execution=cast(ActiveExecution, active),
                    )
                    handle = self._routes.get(route_id)
                    if handle is None:
                        handle = self._adopt_handle(route_id, entry, started_at)
                        handle.current_execution = execution
                        self.processes[route_id] = handle
                    await self._write_active_execution_lease(execution)
                    workflows[str(subject)] = request_bytes
                elif kind == "service":
                    identity = entry.get("service") or {}
                    row = (await db.execute(select(ServiceAttempt, ServiceDefinition).join(
                        ServiceDefinition, ServiceDefinition.id == ServiceAttempt.service_id,
                    ).where(
                        ServiceAttempt.id == subject,
                        ServiceAttempt.worker_id == self.worker_id,
                        ServiceAttempt.lease_token == identity.get("lease_token"),
                        ServiceAttempt.state.in_(("starting", "running", "stopping")),
                        ServiceAttempt.lease_expires_at > func.now(),
                    ))).one_or_none()
                    if row is None:
                        rejected.append((route_id, kind))
                        continue
                    attempt, definition = row
                    if identity.get("service_id") != str(definition.id):
                        raise ValueError("Launcher service identity does not match its claim")
                    handle = self._routes.get(route_id)
                    if handle is None:
                        handle = self._adopt_handle(
                            route_id, entry, attempt.started_at or datetime.now(timezone.utc),
                        )
                        handle.service = ServiceInfo(
                            service_id=str(definition.id), attempt_id=str(subject),
                            lease_token=attempt.lease_token,
                            graceful_shutdown_seconds=definition.graceful_shutdown_seconds,
                        )
                        self.service_processes[route_id] = handle
                    services.append(OwnedAttempt(
                        attempt_id=subject, service_id=definition.id, lease_token=attempt.lease_token,
                        workflow_id=definition.workflow_id, memory_request_bytes=request_bytes,
                        credential_version=attempt.credential_version,
                    ))
                elif kind == "platform_job":
                    job = await db.scalar(select(PlatformJob).where(
                        PlatformJob.id == subject,
                        PlatformJob.lease_token == UUID(str(entry["lease_token"])),
                        PlatformJob.status.in_(("running", "cancel_requested")),
                        PlatformJob.lease_expires_at > func.now(),
                    ))
                    metadata = get_platform_job_metadata(job.job_type) if job is not None else None
                    if job is None or metadata is None:
                        rejected.append((route_id, kind))
                        continue
                    jobs.append((route_id, ClaimedPlatformJob(
                        id=subject, lease_token=UUID(str(job.lease_token)),
                        timeout_seconds=job.timeout_seconds,
                        hard_memory_ratio=metadata.policy.hard_memory_ratio,
                        memory_required_bytes=job.memory_required_bytes, job_type=job.job_type,
                    )))
                elif kind in ("agent_run", "agent_summary"):
                    summary = kind == "agent_summary"
                    handler = entry.get("handler") if summary else "agent.run"
                    if summary and handler not in ("agent.summarize", "agent.summarize_backfill"):
                        raise ValueError("Unknown summary handler")
                    claim_id = UUID(str(entry["claim_id"])) if summary else None
                    # A cancelling run's child is still winding down; adopt
                    # it so its exit settles the run.
                    valid = await db.scalar(select(AgentRun.id).join(
                        ExecutionWorkerClaim,
                        ExecutionWorkerClaim.subject_id == AgentRun.id,
                    ).where(
                        AgentRun.id == subject,
                        AgentRun.status.in_(("completed",) if summary else ("queued", "running", "cancelling")),
                        ExecutionWorkerClaim.worker_id == UUID(self.worker_id),
                        ExecutionWorkerClaim.family == ("agent_summary" if summary else "agent_run"),
                        *([ExecutionWorkerClaim.id == claim_id] if summary else []),
                    ))
                    if valid is None:
                        rejected.append((route_id, kind))
                        continue
                    if summary:
                        assert claim_id is not None
                        summaries[route_id] = (subject, request_bytes, str(handler), claim_id)
                    else:
                        agent_runs[route_id] = (subject, request_bytes)
                else:
                    raise ValueError("Unknown launcher route kind")
            await db.commit()
        for route_id, kind in rejected:
            await self.transport.request(
                "terminate_job" if kind == "platform_job" else "terminate",
                {"route_id": route_id, "error_type": "OwnerLost", "error": "Claim no longer belongs to this launcher"},
            )
        # An uncertain launch that never reached the launcher cannot remain
        # RUNNING forever. A present terminal journal is observed above too.
        for route_id, handle in list(self._routes.items()):
            if route_id not in observed:
                await self._handle_result(handle, {
                    "type": "result", "success": False,
                    "execution_id": handle.current_execution.execution_id if handle.current_execution else None,
                    "error_type": "ProcessCrashError", "error": "Launcher no longer owns this process",
                })
        return {
            "workflow_requests": workflows,
            "services": services,
            "platform_jobs": jobs,
            "agent_runs": agent_runs,
            "summaries": summaries,
        }

    async def _reconcile_opaque_inventory(
        self, inventory: dict[str, Any], lease: RemoteOwnerLease, admission: Admission | None,
    ) -> dict[str, Any] | None:
        from src.core.database import get_db_context
        from src.models.enums import ExecutionStatus
        from src.models.orm.execution_workers import (
            ExecutionWorker,
            ExecutionWorkerClaim,
        )
        from src.models.orm.executions import Execution
        from src.services.work_dispatch import WAKE_WORKFLOW
        from src.services.worker_pool.recovery import (
            announce_lost_runs,
            end_runs_of_lost_worker,
        )
        from src.worker.remote.fencing import (
            LeaseRef,
            fence_remote_owner,
            remote_owner_context,
        )

        version = inventory.get("version")
        live, terminals = inventory.get("routes"), inventory.get("terminal_routes")
        if (
            type(version) is not int or version != 1
            or not isinstance(live, list) or not isinstance(terminals, list)
        ):
            raise ValueError("Opaque workflow inventory must include v1 routes and terminals")

        entries: dict[str, tuple[dict[str, Any], ExecutionInfo, int, tuple[str, str] | None]] = {}
        subjects: set[UUID] = set()
        live_subjects: set[UUID] = set()
        duplicates: set[UUID] = set()
        # Preserve valid retained outcomes even if the snapshot exceeds the
        # prepared capacity. The reconciliation loss path closes admission.
        slots = admission.declared_slots if admission is not None else None
        reported_slots = inventory.get("workflow_slots")
        mismatch = slots is not None and (
            len(live) + len(terminals) > slots
            or (
                (slots > 1 or reported_slots is not None)
                and (type(reported_slots) is not int or reported_slots != slots)
            )
        )
        for is_terminal, routes in ((False, live), (True, terminals)):
            for entry in routes:
                try:
                    if not isinstance(entry, dict) or entry.get("kind") != "workflow":
                        raise ValueError("Opaque inventory supports workflow routes only")
                    route_id, subject = entry["route_id"], UUID(str(entry["subject_id"]))
                    prefix = f"workflow:{subject}:"
                    if not isinstance(route_id, str) or not route_id.startswith(prefix):
                        raise ValueError("Opaque route must match its workflow execution")
                    UUID(route_id.removeprefix(prefix))
                    if not is_terminal:
                        live_subjects.add(subject)
                    if subject in subjects:
                        duplicates.add(subject)
                        raise ValueError("Duplicate execution in opaque inventory")
                    subjects.add(subject)
                    execution, reservation = self._opaque_inventory_execution(entry, subject)
                    self._opaque_handle_value(entry)
                    receipt = self._opaque_receipt(entry, "event_id") if is_terminal else None
                except (KeyError, TypeError, ValueError, OverflowError, ConnectionError) as exc:
                    logger.warning("Rejected opaque inventory route metadata (%s)", type(exc).__name__)
                    mismatch = True
                    continue
                entries[route_id] = (entry, execution, reservation, receipt)

        worker_id = UUID(self.worker_id)
        async with get_db_context() as db:
            with remote_owner_context(LeaseRef(lease)):
                # Serialize the claim comparison and loss decision with claims,
                # terminal commits and replacement of this API owner's fence.
                if not await fence_remote_owner(db, for_update=True):
                    raise ConnectionError("Remote API ownership was replaced")
            claims = dict((await db.execute(select(Execution.id, Execution.status).join(
                ExecutionWorkerClaim, ExecutionWorkerClaim.subject_id == Execution.id,
            ).where(
                Execution.status.in_((ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING)),
                ExecutionWorkerClaim.worker_id == worker_id,
                ExecutionWorkerClaim.family == WAKE_WORKFLOW,
            ))).all())
            tracked = {
                UUID(handle.current_execution.execution_id): handle
                for handle in self._routes.values() if handle.current_execution is not None
            }
            # Claim reservations are installed under the worker row lock.
            # A starting claim with no route is still preparing or unwinding.
            pending = {
                subject for subject in claims.keys() - tracked.keys()
                if admission is not None and admission.workflow_is_starting(str(subject))
            }
            for subject in tracked.keys() - claims.keys():
                handle = tracked[subject]
                if handle.id not in entries:
                    self._cancel_opaque_stop_task(handle)
                    self._forget_route(handle)
                    if admission is not None:
                        admission.release(str(subject))
            await db.commit()

        unacknowledged = {
            subject: handle for subject, handle in tracked.items()
            if subject in claims and subject not in subjects and handle.process.runtime_handle is None
        }
        mismatch |= bool(claims.keys() - subjects - pending - unacknowledged.keys() or live_subjects - claims.keys())
        matched = {}
        for route_id, (entry, execution, reservation, receipt) in entries.items():
            subject = UUID(execution.execution_id)
            if subject not in claims or subject in duplicates:
                continue  # No canonical claim to settle, or ambiguous route identity.
            handle = tracked.get(subject)
            if handle is not None and not self._opaque_inventory_matches(
                handle, route_id, entry, execution, reservation, receipt,
            ):
                mismatch = True
                continue
            matched[route_id] = (entry, execution, reservation, receipt)
        mismatch |= any(
            subject in claims and subject not in unacknowledged and handle.id not in matched
            for subject, handle in tracked.items()
        )

        if mismatch:
            # Inventory describes receipts. Payloads arrive separately as replay
            # events, including when the inconsistency prevents opening claims.
            # A terminal whose claim already closed can also need an ACK if
            # settlement committed before a disconnect.
            terminals_to_ack = {
                route_id: (entry, execution, reservation, receipt)
                for route_id, (entry, execution, reservation, receipt) in entries.items()
                if receipt is not None and (
                    route_id in matched
                    or UUID(execution.execution_id) not in claims and UUID(execution.execution_id) not in duplicates
                )
            }
            receipts = {receipt[0] for _, _, _, receipt in terminals_to_ack.values()}
            replay = await self.transport.wait_for_opaque_terminals(receipts, lease) if receipts else {}
            failures: list[Exception] = []
            with remote_owner_context(LeaseRef(lease)):
                for route_id, (entry, execution, reservation, receipt) in terminals_to_ack.items():
                    event = replay.get(receipt[0])
                    if (
                        event is None or event.get("route_id") != route_id
                        or event.get("runtime_handle") != entry["runtime_handle"]
                        or event.get("attempt_state") != receipt[1]
                        or not self._valid_opaque_terminal_result(event.get("result"), receipt[1])
                    ):
                        continue
                    try:
                        if route_id in matched:
                            await self._attach_opaque_inventory_route(route_id, entry, execution, reservation, receipt)
                            await self._handle_opaque_terminal(event)
                        await self.transport.acknowledge(receipt[0], reconciliation_lease=lease)
                    except Exception as exc:  # noqa: BLE001 - report all failed receipts after the batch
                        exc.add_note(f"Opaque terminal replay for {route_id}")
                        failures.append(exc)
            if failures:
                # Keep the claims gate closed and the worker fenced. Reconnect
                # can retry failed receipts without overwriting their outcomes.
                raise ExceptionGroup("Opaque inventory terminal settlement failed", failures)

            async with get_db_context() as db:
                with remote_owner_context(LeaseRef(lease)):
                    if not await fence_remote_owner(db, for_update=True):
                        raise ConnectionError("Remote API ownership was replaced before settling inventory loss")
                await db.execute(update(ExecutionWorker).where(ExecutionWorker.id == worker_id).values(
                    state="lost", end_reason="opaque_inventory_mismatch",
                    ended_at=func.now(), remote_owner_lease_expires_at=func.now(),
                ))
                lost = await end_runs_of_lost_worker(
                    db, worker_id, uncertain_workflows=frozenset(subjects | tracked.keys()),
                )
                await db.commit()
            await self.ownership_lost("Opaque inventory does not match canonical claims")
            await announce_lost_runs(lost)
            return None

        with remote_owner_context(LeaseRef(lease)):
            for handle in unacknowledged.values():
                # A missing launch receipt cannot prove whether the engine ran.
                # Settle just this invocation without redispatching it.
                await self._handle_result(handle, {
                    "success": False, "error_type": "WorkflowAttemptUncertain",
                    "error": "Workflow may already have taken effect. Its launch was never acknowledged.",
                })
        workflows = {}
        live_workflows = {}
        for route_id, (entry, execution, reservation, receipt) in matched.items():
            if self._opaque_lost or self.transport.lease != lease:
                raise ConnectionError("Remote API ownership was replaced during reconciliation")
            subject = UUID(execution.execution_id)
            await self._attach_opaque_inventory_route(route_id, entry, execution, reservation, receipt)
            workflows[execution.execution_id] = reservation
            if receipt is None:
                live_workflows[route_id] = claims[subject] == ExecutionStatus.CANCELLING
        return {"workflow_requests": workflows, "opaque_live_workflows": live_workflows}

    @staticmethod
    def _same_opaque_instant(left: datetime, right: datetime) -> bool:
        # JS date serialization truncates to whole milliseconds.
        first, second = sorted((left, right))
        return first == second or first == second.replace(microsecond=second.microsecond // 1000 * 1000)

    def _opaque_inventory_execution(self, entry: dict[str, Any], subject: UUID) -> tuple[ExecutionInfo, int]:
        active, reservation = entry["active_execution"], entry["memory_request_bytes"]
        timeout, started_at = entry["timeout_seconds"], datetime.fromisoformat(entry["started_at"])
        if (
            not isinstance(active, dict) or active.get("execution_id") != str(subject)
            or not isinstance(active.get("sync"), bool)
            or type(reservation) is not int or not 0 <= reservation <= CLOUDFLARE_DEV_BUDGET_BYTES
            or type(timeout) is not int or timeout < 0 or started_at.utcoffset() is None
        ):
            raise ValueError("Invalid opaque workflow identity, timing or reservation")
        execution = ExecutionInfo(str(subject), started_at, timeout, cast(ActiveExecution, active))
        deadline, reported = self._opaque_deadline(execution), entry["workflow_deadline"]
        if deadline is None:
            matches = reported is None
        else:
            actual = datetime.fromisoformat(reported)
            matches = actual.utcoffset() is not None and self._same_opaque_instant(deadline, actual)
        if not matches:
            raise ValueError("Opaque inventory changed the dispatch deadline")
        return execution, reservation

    def _opaque_inventory_matches(
        self, handle: ProcessHandle, route_id: str, entry: dict[str, Any],
        execution: ExecutionInfo, reservation: int, receipt: tuple[str, str] | None,
    ) -> bool:
        original = handle.current_execution
        assert original is not None
        process = handle.process
        return (
            handle.id == route_id
            and original.active_execution == execution.active_execution
            and original.timeout_seconds == execution.timeout_seconds
            and self._same_opaque_instant(original.started_at, execution.started_at)
            and process.memory_request_bytes == reservation
            and (process.runtime_handle is None or process.runtime_handle == entry["runtime_handle"])
            and (process.terminal_receipt is None or process.terminal_receipt == receipt)
        )

    async def _attach_opaque_inventory_route(
        self, route_id: str, entry: dict[str, Any], execution: ExecutionInfo,
        reservation: int, receipt: tuple[str, str] | None,
    ) -> None:
        handle = self._routes.get(route_id)
        if handle is None:
            handle = self._adopt_handle(route_id, entry, execution.started_at)
            handle.current_execution = execution
            handle.process.memory_request_bytes = reservation
            self.processes[route_id] = handle
        else:
            self._bind_workflow_handle(handle, entry)
        handle.process.terminal_receipt = receipt
        assert handle.current_execution is not None
        await self._write_active_execution_lease(handle.current_execution)

    def _adopt_handle(self, route_id: str, entry: dict[str, Any], started_at: datetime) -> ProcessHandle:
        handle = ProcessHandle(
            id=route_id, process=_RemoteProcess(), pid=None, state=ProcessState.BUSY,
            work_queue=None, result_queue=None, started_at=started_at,
        )
        if self.runtime_profile == LINUX_RUNTIME_PROFILE:
            pid = entry.get("pid")
            if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0:
                handle.process.remote_pid = pid
        else:
            self._bind_workflow_handle(handle, entry)
        self._routes[route_id] = handle
        return handle

    def _workflow_route_id(self, execution_id: str) -> str:
        return f"workflow:{execution_id}:{uuid4()}"

    def _service_route_id(self, attempt_id: str) -> str:
        return f"service:{attempt_id}:{uuid4()}"

    def _workflow_route_for(self, execution_id: str) -> str | None:
        return next(
            (
                route_id
                for route_id, handle in self._routes.items()
                if handle.current_execution is not None
                and handle.current_execution.execution_id == execution_id
            ),
            None,
        )

    def _service_route_for(self, attempt_id: str) -> str | None:
        return next(
            (
                route_id
                for route_id, handle in self._routes.items()
                if handle.service is not None
                and handle.service.attempt_id == attempt_id
            ),
            None,
        )

    def _read_remote_memory(self) -> tuple[int, int]:
        current, limit = self.transport.memory()
        self._remote_memory_current = int(current)
        self._remote_memory_limit = int(limit)
        return self._remote_memory_current, self._remote_memory_limit

    def _ensure_remote_capacity(self) -> None:
        current, limit = self._read_remote_memory()
        if current < 0 or limit <= 0:
            raise MemoryError("Remote launcher cgroup memory is unavailable")
        threshold = get_settings().memory_pressure_threshold
        if current >= limit * threshold:
            raise MemoryError(
                "Remote launcher memory pressure exceeds "
                f"{threshold:.0%} threshold"
            )

    async def start(self) -> None:
        """Register the API owner and start the canonical Redis lifecycle."""
        if self._started:
            return
        if not self.transport.connected:
            raise RuntimeError("Thin remote launcher is not connected")
        if self.runtime_profile == LINUX_RUNTIME_PROFILE:
            await self.transport.request("configure", {
                "pressure_threshold": get_settings().memory_pressure_threshold,
                "dangerous_memory_ratio": DANGEROUS_MEMORY_RATIO,
            })

        self._started = True
        self._shutdown = False
        self._started_at = datetime.now(timezone.utc)
        self._last_active_execution_refresh = time.monotonic()
        if self.runtime_profile == LINUX_RUNTIME_PROFILE:
            self._read_remote_memory()
        await self._register_worker()
        redis_client = await self._get_redis()
        await cast(
            Awaitable[int],
            redis_client.hset(
                f"bifrost:pool:{self.worker_id}",
                mapping={
                    "runtime": "thin_remote",
                    "execution_owner": "remote_launcher",
                    "api_owner": os.environ.get("HOSTNAME", "unknown"),
                },
            ),
        )
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="thin-remote-pool-heartbeat"
        )
        self._lease_refresh_task = asyncio.create_task(
            self._lease_refresh_loop(), name="thin-remote-pool-lease-refresh"
        )
        self._cancel_task = asyncio.create_task(
            self._cancel_listener_loop(), name="thin-remote-pool-cancel"
        )
        if self.runtime_profile == LINUX_RUNTIME_PROFILE:
            self._command_task = asyncio.create_task(
                self._command_listener_loop(), name="thin-remote-pool-command"
            )

    async def _remote_packages(self) -> list[dict[str, Any]]:
        response = await self.transport.request(
            "package_tool", {"operation": "inventory"}
        )
        packages = response.get("packages")
        if not isinstance(packages, list):
            raise RuntimeError("Remote package inventory response is invalid")
        return [package for package in packages if isinstance(package, dict)]

    async def _register_worker(self) -> None:
        """Register canonical pool fields and Linux package inventory."""
        redis_client = await self._get_redis()
        redis_key = f"bifrost:pool:{self.worker_id}"
        packages = None
        if self.runtime_profile == LINUX_RUNTIME_PROFILE:
            packages = await self._remote_packages()
        fields = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "online",
            "hostname": self.transport.hostname,
        }
        if packages is not None:
            fields["packages"] = json.dumps(packages)
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            fields["runtime_profile"] = self.runtime_profile
        await cast(
            Awaitable[int],
            redis_client.hset(redis_key, mapping=fields),
        )
        await redis_client.expire(redis_key, self.registration_ttl_seconds)

    async def update_packages(self) -> None:
        """Refresh Redis from the package owner after a remote install."""
        if not self._started or self.runtime_profile != LINUX_RUNTIME_PROFILE:
            return
        try:
            redis_client = await self._get_redis()
            packages = await self._remote_packages()
            await cast(
                Awaitable[int],
                redis_client.hset(
                    f"bifrost:pool:{self.worker_id}",
                    "packages",
                    json.dumps(packages),
                ),
            )
        except Exception:
            logger.exception("Could not update thin remote package inventory")

    async def stop(self) -> None:
        """Stop accepting work and ask the launcher to terminate owned routes."""
        if not self._started:
            return
        self._shutdown = True
        tasks = [
            self._heartbeat_task,
            self._lease_refresh_task,
            self._cancel_task,
            self._command_task,
        ]
        for task in tasks:
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass  # Expected after cancelling this owned background task.

        owns_session = await self._owns_session()
        if owns_session and self.transport.connected:
            await asyncio.gather(
                *(
                    self._request_terminate(
                        route_id,
                        error_type="WorkerShutdown",
                        error="Remote pool is stopping",
                        grace_seconds=(
                            handle.service.graceful_shutdown_seconds
                            if handle.service is not None
                            else self.graceful_shutdown_seconds
                        ),
                    )
                    for route_id, handle in list(self._routes.items())
                ),
                return_exceptions=True,
            )
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            for handle in self._routes.values():
                self._cancel_opaque_stop_task(handle)
        if owns_session:
            await self._unregister_worker()
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
        self._started = False

    async def ownership_lost(self, reason: str = "Remote owner lease lost") -> None:
        """Detach a replaced owner without mutating the surviving children."""
        logger.info("Remote pool detaching: %s", reason)
        self._shutdown = True
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            self._opaque_lost = True
            for handle in self._routes.values():
                self._cancel_opaque_stop_task(handle)
        self.processes.clear()
        self.service_processes.clear()
        self._routes.clear()
        self._route_metrics.clear()

    async def _owns_session(self) -> bool:
        from src.worker.remote.fencing import current_remote_lease_ref
        from src.worker.remote.ownership import owns_remote_session

        ref = current_remote_lease_ref()
        if ref is None and self.runtime_profile == LINUX_RUNTIME_PROFILE:
            return True
        lease = ref.lease if ref is not None else self.transport.lease
        try:
            return await owns_remote_session(lease)
        except Exception:
            # Fail closed: result commits and shutdown side effects stay
            # fenced until a read succeeds.
            logger.warning(
                "Remote owner fence read failed; treating session as lost", exc_info=True
            )
            return False

    def _request_bytes(self, key: str) -> int:
        return self.transport.memory_request_bytes(key) or 0

    def credential_route(self) -> CredentialRoute:
        """The launcher's reported credential protocol, read with the session
        generation it was persisted under."""
        lease = self.transport.lease
        return CredentialRoute(lease.credential_protocol, lease.session_generation)

    async def route_execution(
        self,
        execution_id: str,
        context: dict[str, Any],
        active_execution: ActiveExecution,
        *,
        credential_route: CredentialRoute | None = None,
        cloudflare_revision: str | None = None,
    ) -> None:
        """Write canonical API ownership records, then await launcher ACK.

        With ``credential_route``, the context's credentials were minted for
        that launcher session; they are never sent to a replacement session.
        """
        validate_execution_context(context)
        opaque = self.runtime_profile != LINUX_RUNTIME_PROFILE
        generation = credential_route.generation if credential_route is not None else None
        if opaque and generation is None:
            raise CredentialRouteChanged("Opaque dispatch requires a credential route generation")
        dispatch_started_at = datetime.now(UTC) if opaque else None
        workspace = None
        if opaque:
            from src.services.execution.cloudflare_eligibility import (
                prepare_approved_workspace,
            )

            workflow_id = context.get("workflow_id")
            if (
                cloudflare_revision is None or not isinstance(workflow_id, str)
                or workflow_id != active_execution.get("workflow_id")
                or context.get("tags") != ["workflow"]
                or any(context.get(key) is not None for key in (
                    "artifact_workspace_id", "code", "service",
                ))
                or context.get("solution_global_repo_access")
            ):
                raise CloudflareRevisionRejected("An approved workspace workflow is required")
            workspace = await prepare_approved_workspace(
                workflow_id=workflow_id, entry_path=context.get("file_path"),
                function_name=context.get("function_name"), entry_sha256=context.get("content_hash"),
                expected_revision=cloudflare_revision,
                **({"solution_id": context["solution_id"]} if context.get("solution_id") is not None else {}),
            )
            if workspace is None:
                raise CloudflareRevisionRejected("Cloudflare workflow revision no longer matches its approval")
        await self._write_context_to_redis(execution_id, context)
        if not opaque:
            self._ensure_remote_capacity()
        route_id = self._workflow_route_id(execution_id)
        timeout = int(context.get("timeout_seconds", self.execution_timeout_seconds))

        async with self._restart_lock:
            if route_id in self._routes:
                raise RuntimeError(f"Remote route already exists: {route_id}")
            if opaque and self._opaque_lost:
                raise CredentialRouteChanged("Remote API ownership was lost before dispatch")
            started_at = dispatch_started_at or datetime.now(timezone.utc)
            context["workflow_deadline"] = (
                (started_at + timedelta(seconds=timeout)).isoformat() if timeout > 0 else None
            )
            execution = ExecutionInfo(
                execution_id=execution_id,
                started_at=started_at,
                timeout_seconds=timeout,
                active_execution=active_execution,
            )
            handle = ProcessHandle(
                id=route_id,
                process=_RemoteProcess(),
                pid=None,
                state=ProcessState.BUSY,
                work_queue=None,
                result_queue=None,
                started_at=started_at,
                current_execution=execution,
            )
            self.processes[handle.id] = handle
            self._routes[route_id] = handle
            try:
                await self._write_active_execution_lease(execution)
            except Exception as exc:  # noqa: BLE001 - dispatch keeps its durable context
                logger.warning(
                    "Could not write active execution lease for %s: %s",
                    execution_id,
                    exc,
                )
            if opaque:
                handle.process.memory_request_bytes = self._request_bytes(execution_id)
            if workspace is not None:
                context = {**context, "cloudflare_workspace": workspace.bundle}
                if context.get("solution_id") is not None:
                    context["content_hash"] = workspace.entry_sha256
            try:
                response = await self.transport.request(
                    "workflow",
                    {
                        "route_id": route_id,
                        "execution_id": execution_id,
                        "context": context,
                        "active_execution": dict(active_execution),
                        "started_at": started_at.isoformat(),
                        "timeout_seconds": timeout,
                        "memory_request_bytes": (
                            handle.process.memory_request_bytes if opaque else self._request_bytes(execution_id)
                        ),
                        **(
                            {"workflow_deadline": context["workflow_deadline"]}
                            if opaque else {"memory_kill_growth_bytes": self.memory_kill_growth_bytes}
                        ),
                    },
                    expected_generation=generation,
                )
                self._bind_workflow_handle(handle, response)
            except OpaqueDispatchInterrupted:
                logger.warning("Opaque workflow command send interrupted: %s", route_id)
            except (ConnectionError, TimeoutError):
                # The child may already exist. Inventory resolves this exact
                # route after reconnect; reporting failure now could duplicate
                # side effects while its original process continues.
                logger.warning("Remote workflow launch ACK uncertain: %s", route_id)
            except Exception as exc:
                # The opaque wire contract permits a workflow error reply
                # only for a definitive refusal before engine entry. A launch
                # that may have entered must report an uncertain terminal.
                self.processes.pop(handle.id, None)
                self._routes.pop(route_id, None)
                self._route_metrics.pop(route_id, None)
                try:
                    redis_client = await self._get_redis()
                    await redis_client.delete(
                        f"bifrost:exec:{execution_id}:context",
                        active_execution_key(execution_id),
                    )
                except Exception:
                    logger.warning("Could not clean failed remote dispatch %s", execution_id)
                if opaque and isinstance(exc, CredentialRouteChanged) and not isinstance(exc, OpaqueWorkflowNotStarted):
                    raise OpaqueWorkflowNotStarted(str(exc)) from exc
                raise
            finally:
                if opaque and self._routes.get(route_id) is handle:
                    self._schedule_opaque_stop(handle)

    async def route_service(
        self,
        *,
        service_id: str,
        attempt_id: str,
        lease_token: str,
        context: dict[str, Any],
        graceful_shutdown_seconds: int = 30,
        credential_route: CredentialRoute | None = None,
    ) -> None:
        """Dispatch a canonical claimed service attempt to the launcher.

        ``credential_route`` fences the context's credentials to the launcher
        session they were minted for, as in ``route_execution``.
        """
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            raise RuntimeError("Physical runtime does not support persistent services")
        validate_execution_context(context)
        self._ensure_remote_capacity()
        route_id = self._service_route_id(attempt_id)
        async with self._restart_lock:
            if route_id in self._routes:
                raise RuntimeError(f"Remote route already exists: {route_id}")
            handle = ProcessHandle(
                id=route_id,
                process=_RemoteProcess(),
                pid=None,
                state=ProcessState.BUSY,
                work_queue=None,
                result_queue=None,
                started_at=datetime.now(timezone.utc),
                service=ServiceInfo(
                    service_id=service_id,
                    attempt_id=attempt_id,
                    lease_token=lease_token,
                    graceful_shutdown_seconds=graceful_shutdown_seconds,
                ),
            )
            self.service_processes[handle.id] = handle
            self._routes[route_id] = handle
            try:
                response = await self.transport.request(
                    "service",
                    {
                        "route_id": route_id,
                        "execution_id": attempt_id,
                        "context": context,
                        "graceful_shutdown_seconds": graceful_shutdown_seconds,
                        "memory_request_bytes": self._request_bytes("service:" + attempt_id),
                        "service": {
                            "service_id": service_id,
                            "attempt_id": attempt_id,
                            "lease_token": lease_token,
                        },
                    },
                    expected_generation=None if credential_route is None else credential_route.generation,
                )
                handle.process.remote_pid = self._remote_pid_from_ack(response)
            except (ConnectionError, TimeoutError):
                logger.warning("Remote service launch ACK uncertain: %s", route_id)
            except Exception:
                self.service_processes.pop(handle.id, None)
                self._routes.pop(route_id, None)
                self._route_metrics.pop(route_id, None)
                raise

    @staticmethod
    def _remote_pid_from_ack(response: object) -> int:
        pid = response.get("pid") if isinstance(response, dict) else None
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            # A malformed success reply does not prove the launcher never
            # started work. Preserve the route just as for a lost ACK; only
            # canonical inventory/result reconciliation may resolve it.
            raise ConnectionError("Remote launcher ACK is missing a valid PID; launch state unknown")
        return pid

    def _bind_workflow_handle(self, handle: ProcessHandle, response: object) -> None:
        """Bind a negotiated physical identity, never infer one from the ACK.

        An invalid post-dispatch reply is uncertain, not evidence of no launch.
        Linux keeps its existing PID wire representation. An opaque invocation
        does not acquire a fake PID or promise a physical termination primitive.
        """
        if self.runtime_profile == LINUX_RUNTIME_PROFILE:
            handle.process.remote_pid = self._remote_pid_from_ack(response)
            return
        value = self._opaque_handle_value(response)
        previous = handle.process.runtime_handle
        if previous is not None and previous != value:
            raise ConnectionError("Physical workflow handle changed; reconciliation required")
        handle.process.runtime_handle = value

    @staticmethod
    def _opaque_handle_value(response: object) -> str:
        value = response.get("runtime_handle") if isinstance(response, dict) else None
        if (
            not isinstance(response, dict) or "pid" in response
            or not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9._:-]{1,255}", value) is None
        ):
            raise ConnectionError("Opaque workflow handle missing or invalid; launch state unknown")
        return value

    @staticmethod
    def _opaque_receipt(event: dict[str, Any], event_id_key: str) -> tuple[str, str]:
        event_id, state = event.get(event_id_key), event.get("attempt_state")
        if (
            not isinstance(event_id, str) or not event_id
            or not isinstance(state, str) or state not in OPAQUE_ATTEMPT_STATES
        ):
            raise ValueError("Opaque terminal requires an event ID and attempt state")
        return event_id, state

    @staticmethod
    def _opaque_deadline(execution: ExecutionInfo) -> datetime | None:
        if execution.timeout_seconds > 0:
            return execution.started_at + timedelta(seconds=execution.timeout_seconds)
        return None

    @staticmethod
    def _cancel_opaque_stop_task(handle: ProcessHandle) -> None:
        task = handle.process.stop_task
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    def resume_opaque_stops(self, live_workflows: dict[str, bool]) -> None:
        for route_id, cancelling in live_workflows.items():
            handle = self._routes.get(route_id)
            if handle is not None:
                self._schedule_opaque_stop(handle, cancelling=cancelling)

    def _schedule_opaque_stop(self, handle: ProcessHandle, *, cancelling: bool = False) -> None:
        assert handle.current_execution is not None
        deadline = self._opaque_deadline(handle.current_execution)
        process = handle.process
        retry = process.stop_reason
        if (
            process.stop_confirmed or process.stop_generation == self.transport.lease.session_generation
            or (retry is None and deadline is None and not cancelling)
        ):
            return
        self._cancel_opaque_stop_task(handle)

        async def stop_due_route() -> None:
            if retry is not None:
                await self._request_opaque_stop(handle, error_type=retry[0], error=retry[1])
            elif cancelling:
                await self._handle_cancel_request(execution_id)
            elif deadline is not None:
                await asyncio.sleep(max(0, (deadline - datetime.now(UTC)).total_seconds()))
                await self._request_terminate(
                    handle.id, error_type="TimeoutError", error="Workflow dispatch deadline exceeded",
                    grace_seconds=self.graceful_shutdown_seconds,
                )

        # Capture the execution ID before a terminal clears the handle.
        execution_id = handle.current_execution.execution_id
        handle.process.stop_task = asyncio.create_task(stop_due_route(), name=f"opaque-stop:{execution_id}")

    async def _request_opaque_stop(self, handle: ProcessHandle, *, error_type: str, error: str) -> None:
        if self._opaque_lost or not self.transport.connected or handle.process.stop_confirmed:
            return
        from src.worker.remote.fencing import LeaseRef, remote_owner_context

        lease = self.transport.lease
        process = handle.process
        try:
            async with asyncio.timeout(OPAQUE_STOP_TIMEOUT_SECONDS):
                with remote_owner_context(LeaseRef(lease)):
                    if not await self._owns_session() or self._routes.get(handle.id) is not handle:
                        return
                    # Keep this route busy and reserved until terminal settlement.
                    # One attempt per generation until the stop is confirmed.
                    if (
                        self.transport.lease != lease or process.stop_confirmed
                        or process.stop_generation == lease.session_generation
                    ):
                        return
                    process.stop_generation = lease.session_generation
                    process.stop_reason = (error_type, error)
                    reply = await self.transport.request(
                        "terminate", {"route_id": handle.id, "error_type": error_type, "error": error},
                        expected_generation=lease.session_generation,
                    )
                    if not isinstance(reply, dict) or reply.get("received") is not True:
                        raise ConnectionError("Opaque stop receipt is missing")
                    process.stop_confirmed = True
        except CredentialRouteChanged:
            if process.stop_generation == lease.session_generation:
                process.stop_generation = None  # Refused before send.
        except (ConnectionError, TimeoutError, RuntimeError, MemoryError):
            logger.warning("Remote workflow stop receipt uncertain: %s", handle.id)

    async def _request_terminate(
        self,
        route_id: str,
        *,
        error_type: str,
        error: str,
        grace_seconds: float,
    ) -> None:
        handle = self._routes.get(route_id)
        if handle is None:
            return
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            await self._request_opaque_stop(handle, error_type=error_type, error=error)
            return
        handle.state = ProcessState.KILLED
        handle.killed_at = datetime.now(timezone.utc)
        await self.transport.request(
            "terminate",
            {
                "route_id": route_id,
                "error_type": error_type,
                "error": error,
                "grace_seconds": grace_seconds,
            },
        )

    async def _handle_cancel_request(self, execution_id: str) -> None:
        route_id = self._workflow_route_for(execution_id)
        if route_id is None:
            return
        await self._request_terminate(
            route_id,
            error_type="CancelledError",
            error="Execution was cancelled",
            grace_seconds=self.graceful_shutdown_seconds,
        )

    async def stop_service_child(self, attempt_id: str) -> bool:
        route_id = self._service_route_for(attempt_id)
        if route_id is None:
            return False
        handle = self._routes.get(route_id)
        if handle is None or handle.service is None:
            return False
        await self._request_terminate(
            route_id,
            error_type="CancelledError",
            error="Service stop requested",
            grace_seconds=handle.service.graceful_shutdown_seconds,
        )
        return True

    async def drain_active_executions(self, drain_timeout: float) -> bool:
        deadline = time.monotonic() + drain_timeout
        while self.active_execution_count() > 0:
            if time.monotonic() >= deadline:
                await asyncio.gather(
                    *(
                        self._request_terminate(
                            route_id,
                            error_type=DRAIN_DEADLINE_ERROR_TYPE,
                            error=DRAIN_DEADLINE_ERROR,
                            grace_seconds=self.graceful_shutdown_seconds,
                        )
                        for route_id, handle in list(self._routes.items())
                        if handle.current_execution is not None
                    ),
                    return_exceptions=True,
                )
                return False
            await asyncio.sleep(0.05)
        return True

    async def drain_and_restart_template(self, drain_timeout: float = 60.0) -> None:
        """Drain remote workflows, retire services, then recycle remotely."""
        async with self._restart_lock:
            await self.drain_active_executions(drain_timeout)
            terminate_services = []
            for route_id, handle in list(self._routes.items()):
                if handle.service is None:
                    continue
                handle.service.recycled = True
                handle.service.recycle_reason = "template_recycle"
                terminate_services.append(
                    self._request_terminate(
                        route_id,
                        error_type="WorkerShutdown",
                        error="Remote template recycle",
                        grace_seconds=handle.service.graceful_shutdown_seconds,
                    )
                )
            await asyncio.gather(*terminate_services)
            await self.transport.request("recycle", {})

    async def _recycle_via_drain(self, reason: str) -> None:
        await self.drain_and_restart_template()
        logger.info("Thin remote pool recycled (%s)", reason)

    async def _lease_refresh_loop(self) -> None:
        """Refresh durable run leases even though this pool has no OS monitor."""
        while not self._shutdown:
            try:
                await self._refresh_active_execution_leases_if_due(time.monotonic())
            except Exception:
                logger.exception("Thin remote active-execution lease refresh failed")
            await asyncio.sleep(min(self.heartbeat_interval_seconds, 30.0))

    async def _heartbeat_loop(self) -> None:
        """Publish remote ownership without sampling the API host cgroup."""
        while not self._shutdown:
            try:
                await self._refresh_registration()
                await self._publish_heartbeat(self._build_heartbeat())
            except Exception:
                logger.exception("Thin remote pool heartbeat failed")
            await asyncio.sleep(self.heartbeat_interval_seconds)

    def workflow_growth(self) -> dict[str, int]:
        return {
            handle.current_execution.execution_id: int(
                self._route_metrics.get(route_id, {}).get("growth_bytes", 0)
            )
            for route_id, handle in self._routes.items()
            if handle.current_execution is not None
        }

    def get_service_memory_mb(self, attempt_id: str) -> float | None:
        route_id = self._service_route_for(attempt_id)
        if route_id is None:
            return None
        rss = self._route_metrics.get(route_id, {}).get("rss_bytes")
        return float(rss) / 1024 / 1024 if rss is not None else None

    def _attach_resource_peaks(self, handle: ProcessHandle, result: dict[str, Any]) -> None:
        metrics = self._route_metrics.get(handle.id)
        if not metrics:
            return
        result_metrics = result.setdefault("metrics", {})
        if not isinstance(result_metrics, dict):
            return
        for source, target in (
            ("growth_bytes", "peak_memory_growth_bytes"),
            ("rss_bytes", "peak_process_rss_bytes"),
            ("cpu_seconds", "cpu_total_seconds"),
            ("peak_cpu_cores", "peak_cpu_cores"),
        ):
            value = metrics.get(source)
            if value is not None:
                result_metrics[target] = max(result_metrics.get(target) or 0, value)

    async def handle_event(self, event: dict[str, Any]) -> None:
        """Accept launcher events after their control-channel authentication."""
        event_type = event.get("event")
        if event_type == "sample" and self.runtime_profile == LINUX_RUNTIME_PROFILE:
            self._remote_memory_current = int(event.get("current", -1))
            self._remote_memory_limit = int(event.get("limit", -1))
            runs = event.get("runs", {})
            if not isinstance(runs, dict):
                return
            for route_id, sample in runs.items():
                if route_id not in self._routes or not isinstance(sample, dict):
                    continue
                metrics = self._route_metrics.setdefault(route_id, {})
                for key in ("growth_bytes", "memory_peak_bytes", "cpu_seconds", "rss_bytes", "peak_cpu_cores"):
                    value = sample.get(key)
                    if isinstance(value, (int, float)):
                        metrics[key] = max(metrics.get(key, 0), value)
            return
        if event_type != "result":
            logger.debug("Ignoring thin remote launcher event %r", event_type)
            return
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            await self._handle_opaque_terminal(event)
            return
        route_id = event.get("route_id")
        result = event.get("result")
        if not isinstance(route_id, str) or not isinstance(result, dict):
            logger.warning("Ignoring malformed thin remote result event")
            return
        handle = self._routes.get(route_id)
        if handle is None:
            logger.info("Ignoring late thin remote result for %s", route_id)
            return
        await self._handle_result(handle, result)

    async def _handle_opaque_terminal(self, event: dict[str, Any]) -> None:
        if not await self._owns_session() or self._opaque_lost:
            raise ConnectionError("Remote API ownership was replaced")
        receipt = self._opaque_receipt(event, "_event_id")
        _, state = receipt
        route_id, result = event.get("route_id"), event.get("result")
        self._opaque_handle_value(event)
        if not isinstance(route_id, str) or not self._valid_opaque_terminal_result(result, state):
            raise ValueError("Invalid opaque workflow terminal")
        handle = self._routes.get(route_id)
        if handle is None:
            return  # The fenced result may have already committed before ACK.
        previous = handle.process.terminal_receipt
        if previous is not None and previous != receipt:
            raise ValueError("Opaque terminal does not match its retained receipt")
        self._bind_workflow_handle(handle, event)
        handle.process.terminal_receipt = receipt
        if state == "uncertain":
            error_type = result.get("error_type")
            # Preserve known stop and resource-limit reasons on replay, even
            # when a new API owner has no in-memory stop.
            if error_type not in ("CancelledError", "TimeoutError", "CpuLimitExceeded", "MemoryLimitExceeded"):
                error_type = "WorkflowAttemptUncertain"
            result = {
                **result,
                "error_type": error_type,
                "error": f"Workflow may already have taken effect. {result.get('error') or ''}".strip(),
            }
        await self._handle_result(handle, result)

    @staticmethod
    def _valid_opaque_terminal_result(result: Any, state: str) -> TypeGuard[dict[str, Any]]:
        return (
            isinstance(result, dict) and type(result.get("success")) is bool
            and (not result["success"] or state == "completed")
        )

    async def _handle_result(self, handle: ProcessHandle, result: dict[str, Any]) -> None:
        """Release a remote route only after its canonical callback succeeds.

        Local pipe delivery may consume a result once; a reconnecting remote
        launcher retains it until ACK. Keep the handle and propagate callback
        failures so that transport replay can safely try the durable commit.
        """
        async with self._result_locks.setdefault(handle.id, asyncio.Lock()):
            if handle.id not in self._routes:
                return
            if not await self._owns_session():
                raise ConnectionError("Remote API ownership was replaced")
            if handle.service is not None:
                if self.on_service_result is not None:
                    await self.on_service_result(self._service_envelope(handle, result))
            elif handle.current_execution is not None:
                result = handle.current_execution.attach_transport_metadata(result)
                if self.runtime_profile == LINUX_RUNTIME_PROFILE:
                    self._attach_resource_peaks(handle, result)
                else:
                    # Opaque launchers have no process measurement. The
                    # result callback also uses growth to teach request sizing.
                    result.pop("metrics", None)
                if self.on_result is not None:
                    await self.on_result(result)
            if not await self._owns_session():
                raise ConnectionError("Remote API ownership was replaced before ACK")
            if self.runtime_profile != LINUX_RUNTIME_PROFILE:
                self._cancel_opaque_stop_task(handle)
                if self.transport.admission is not None and handle.current_execution is not None:
                    self.transport.admission.release(handle.current_execution.execution_id)
            handle.result_reported = True
            handle.process.exitcode = 0
            handle.current_execution = None
            self._forget_route(handle)

    def _forget_route(self, handle: ProcessHandle) -> None:
        self.processes.pop(handle.id, None)
        self.service_processes.pop(handle.id, None)
        self._routes.pop(handle.id, None)
        self._route_metrics.pop(handle.id, None)
        self._result_locks.pop(handle.id, None)

    def _build_heartbeat(self) -> dict[str, Any]:
        """Publish the remote runtime's own capacity and usage."""
        processes = [
            {
                "pid": handle.process.remote_pid,
                "process_id": handle.id,
                "state": handle.state.value,
                "memory_mb": float(self._route_metrics.get(handle.id, {}).get("rss_bytes", 0)) / 2**20,
                "private_dirty_kb": -1,
                "uptime_seconds": handle.uptime_seconds,
                "executions_completed": handle.executions_completed,
                "execution": {
                    "execution_id": handle.current_execution.execution_id,
                    "started_at": handle.current_execution.started_at.isoformat(),
                    "elapsed_seconds": handle.current_execution.elapsed_seconds,
                },
            }
            for handle in self.processes.values()
            if handle.current_execution is not None and handle.process.remote_pid is not None
        ]
        services = [
            {
                "pid": handle.process.remote_pid,
                "process_id": handle.id,
                "state": handle.state.value,
                "memory_mb": self.get_service_memory_mb(handle.service.attempt_id) or 0.0,
                "private_dirty_kb": -1,
                "uptime_seconds": handle.uptime_seconds,
                "service": {
                    "service_id": handle.service.service_id,
                    "attempt_id": handle.service.attempt_id,
                },
            }
            for handle in self.service_processes.values()
            if handle.service is not None and handle.process.remote_pid is not None
        ]
        heartbeat: dict[str, Any] = {
            "type": "worker_heartbeat",
            "worker_id": self.worker_id,
            "hostname": self.transport.hostname,
            "status": "online",
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "processes": processes,
            "pool_size": len(processes),
            "idle_count": 0,
            "busy_count": len(processes),
            "service_count": len(services),
            "service_children": services,
            **({
                "requirements_installed": self._requirements_installed,
                "requirements_total": self._requirements_total,
                "memory_current_bytes": self._remote_memory_current,
                "memory_max_bytes": self._remote_memory_limit,
            } if self.runtime_profile == LINUX_RUNTIME_PROFILE else {}),
        }
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            # OS-process diagnostics retain their existing integer-PID contract.
            # Invocations, including uncertain ACKs, still count toward load.
            invocations = [{
                "runtime_profile": self.runtime_profile,
                "runtime_handle": handle.process.runtime_handle,
                "route_id": handle.id,
                "execution_id": handle.current_execution.execution_id,
                "state": handle.state.value,
                "memory_request_bytes": handle.process.memory_request_bytes,
            } for handle in self.processes.values() if handle.current_execution is not None]
            heartbeat["runtime_invocations"] = invocations
            heartbeat["pool_size"] = heartbeat["busy_count"] = len(invocations)
            heartbeat["runtime_profile"] = self.runtime_profile
            admission = self.transport.admission
            if admission is not None:
                heartbeat["declared_capacity"] = {
                    "slots": admission.declared_slots,
                    "held_slots": admission.held_slots(),
                    "budget_bytes": admission.budget_bytes,
                    "reserved_bytes": admission.held_bytes(),
                }
        return heartbeat
