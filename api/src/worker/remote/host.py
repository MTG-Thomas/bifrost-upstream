"""API-side control host for physical remote execution launchers.

This module deliberately does not import ``src.main`` or mutate application
startup. The API lifespan owns ``start_remote_control_host`` and registers the
router before the MCP catch-all route.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, nullcontext
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from re import fullmatch
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from shared.solution_git_stage import SOLUTION_GIT_CAPABILITY, SOLUTION_UPDATE_CHECK_JOB
from shared.worker_control_auth import external_worker_control_key
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.websockets import WebSocketState

from src.config import get_settings
from src.core.database import get_db_context, get_liveness_db_context
from src.jobs.consumers.package_install import PackageInstallConsumer
from src.jobs.schedulers.platform_jobs import (
    claim_platform_job,
    run_claimed_platform_job,
)
from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareWorkerReservation,
    CloudflareWorkflowSlot,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.services.execution.template_process import TEMPLATE_START_TIMEOUT_SECONDS
from src.services.memory_requests import AGENT_RUN_REQUEST_BYTES, MemoryBudget
from src.services.pool_cadence import CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS
from src.services.worker_pool.claims import (
    PoolClaim,
    admit_external_worker,
    admit_worker,
    register_static_worker,
    report_worker_handlers,
)
from src.services.worker_pool.cloudflare_admission import (
    cloudflare_dispatch_started_at,
    scoped_cloudflare_claim_allowed,
)
from src.services.worker_pool.cloudflare_containers_policy import (
    CONTAINERS_SCOPE,
    container_identity_matches,
)
from src.services.worker_pool.cloudflare_credentials import load_managed_control_key
from src.services.worker_pool.cloudflare_slots import (
    PREPARE_SECONDS,
    WorkflowSlotTicket,
    authorized_queue_preparation,
    fence_workflow_slot_send,
)
from src.services.worker_pool.cloudflare_spend import (
    PreparationBooking,
    record_launcher_terminal_meter,
    reserve_engine_generation,
    reserve_tenant_host_usage,
    settle_unsent_preparation,
)
from src.worker.admission import Admission
from src.worker.pool_main import PoolWorker
from src.worker.remote.agent_route import AgentRoute, RemoteAgentConsumer
from src.worker.remote.cloudflare_agent import (
    CloudflareAgentRoute,
    CloudflareAgentRoutes,
)
from src.worker.remote.cloudflare_summary import CloudflareSummaryRoutes, SummaryRoute
from src.worker.remote.container_egress import CONTAINER_EGRESS_DENIALS
from src.worker.remote.fencing import (
    LeaseRef,
    current_remote_lease_ref,
    fence_remote_owner,
    remote_owner_context,
)
from src.worker.remote.guest_diagnostics import GUEST_DIAGNOSTIC_EVENTS, close_evidence, verify_guest_diagnostic
from src.worker.remote.ownership import (
    DEFAULT_OWNER_LEASE_SECONDS,
    CloudflareEnrollment,
    RemoteOwnerLease,
    acquire_remote_owner,
    owns_remote_session,
    release_remote_owner,
    remote_enrollment_is_live,
    renew_remote_owner,
)
from src.worker.remote.platform_stage_host import PlatformStageBackend
from src.worker.remote.protocol import (
    CLOUDFLARE_AGENT_HANDLER,
    CLOUDFLARE_AGENT_PROTOCOL,
    CLOUDFLARE_CREDENTIAL_PROTOCOL,
    CLOUDFLARE_DEV_BUDGET_BYTES,
    CLOUDFLARE_DEV_SLOTS,
    CLOUDFLARE_INCARNATION_ENDED,
    CLOUDFLARE_MAX_LOGICAL_BYTES,
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    CLOUDFLARE_SUMMARY_HANDLER,
    CLOUDFLARE_WORKFLOW_HANDLER,
    LEGACY_EXECUTION_HANDLERS,
    LINUX_RUNTIME_PROFILE,
    STATIC_INCARNATION_LOST,
    SUMMARY_CLAIM_PROTOCOL,
    AuthenticationError,
    CloudflareRevisionRejected,
    ControlEnvelope,
    CredentialRouteChanged,
    FragmentAssembler,
    OpaqueDispatchInterrupted,
    ProtocolError,
    SignedHello,
    fragment,
    hello_credential_protocol,
    hello_execution_handlers,
    nonce_digest,
    verify_hello,
)

RENEW_RETRY_SECONDS = 2.0
# Allow the same three-second delay that pauses admission on stale telemetry.
# A 20-second DB lease still has seven seconds after its halfway renewal.
CONTAINER_LEASE_TRANSIT_MARGIN_MS = 3000
# API-owned capacity for the dev build. Provider enrollment will own this
# policy when multiple Cloudflare capacity profiles are supported.
ADMISSION_FRESHNESS_SECONDS = 3


router = APIRouter()

COMMAND_REPLY_SECONDS = 15
CLOUDFLARE_PREPARE_IO_SECONDS = 1


def _cloudflare_tenant_key(org_id: UUID | str | None, worker_id: UUID | None = None, slot: int | None = None) -> str:
    tenant = "launcher:platform" if org_id is None else f"launcher:org:{org_id}"
    if slot is not None:
        tenant += f":slot:{slot}:incarnation:{worker_id}"
    return sha256(tenant.encode()).hexdigest()[:32]


def _reply_timeout(action: str) -> float:
    """How long a launcher command may take to reply."""
    if action in ("package_tool", "recycle"):
        # Package installation uses the existing long bounded pip calls.
        return 600
    if action in ("agent_run", "agent_summary"):
        # It may wait while the launcher replaces a dead agent template.
        return TEMPLATE_START_TIMEOUT_SECONDS + COMMAND_REPLY_SECONDS
    if action == "prepare_workflow":
        return PREPARE_SECONDS
    return COMMAND_REPLY_SECONDS


def _container_lease_grant(remaining_ms: int, began_at: float) -> int:
    # Subtract work elapsed before send and allow for one bounded transit delay.
    return remaining_ms - int((time.monotonic() - began_at) * 1000) - CONTAINER_LEASE_TRANSIT_MARGIN_MS


class RemoteProcess:
    def __init__(self, bridge: Bridge, route_id: str) -> None:
        self.bridge = bridge
        self.route_id = route_id
        self.returncode: int | None = None
        self.exited: asyncio.Future[int] = asyncio.get_running_loop().create_future()

    async def wait(self) -> int:
        return await asyncio.shield(self.exited)

    async def terminate(self) -> None:
        if self.returncode is None:
            await self.bridge.request("terminate_job", {"route_id": self.route_id})
            await self.wait()


class Bridge:
    """Ephemeral command transport; PostgreSQL remains the durable authority."""
    def __init__(
        self, socket: WebSocket, hello: dict[str, Any], lease: RemoteOwnerLease
    ) -> None:
        self.socket = socket
        self.connected = True
        self.runtime_profile = hello.get("runtime_profile", LINUX_RUNTIME_PROFILE)
        self.managed_cloudflare = hello.get("managed_cloudflare", False)
        self.containers = hello.get("containers", False)
        if self.runtime_profile == LINUX_RUNTIME_PROFILE:
            self.current = int(hello["current"])
            self.limit = int(hello["limit"])
            self.pressure_ratio = float(hello["pressure_ratio"]) if "pressure_ratio" in hello else (
                self.current / self.limit if self.limit > 0 else 1.0
            )
            self.baseline_bytes = self.current
        self.hostname = hello["hostname"]
        self.execution_lane = hello.get("execution_lane", "normal")
        self.handlers = hello.get(
            "handlers", LEGACY_EXECUTION_HANDLERS if self.runtime_profile == LINUX_RUNTIME_PROFILE else (),
        )
        self.platform_stage_backend: PlatformStageBackend | None = None
        if self.containers and (SOLUTION_GIT_CAPABILITY, 1) in self.handlers:
            from src.jobs.platform.solution_update_stage import (
                run_solution_update_stage,
            )

            self.platform_stage_backend = PlatformStageBackend(
                self, {SOLUTION_UPDATE_CHECK_JOB: (SOLUTION_GIT_CAPABILITY, run_solution_update_stage)},
            )
        self.declared_slots = lease.declared_slots
        self.meter_exhausted = False
        self.launcher_retired = False
        self._reset_freshness()
        self.pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._preparations: dict[str, tuple[int, datetime]] = {}
        self._workflow_timings: dict[str, dict[str, datetime]] = {}
        self._session_ready_at: datetime | None = None
        self.jobs: dict[str, RemoteProcess] = {}
        self.platform_routes: dict[tuple[str, str], str] = {}
        self.pool: Any = None
        # Agent runs in launcher children; kept across reconnects like jobs.
        self.agents: RemoteAgentConsumer | None = None
        self.summaries: CloudflareSummaryRoutes | None = None
        self.autonomous: CloudflareAgentRoutes | None = None
        self.admission: Admission | None = None
        self.requirements = hello["requirements"] if self.runtime_profile == LINUX_RUNTIME_PROFILE else {}
        self.inventory: dict[str, Any] | None = None
        self.inventory_ready = asyncio.Event()
        self.reconciliation_ready = asyncio.Event()
        self._deferred_terminal: list[dict[str, Any]] = []
        self._terminal_replay_changed = asyncio.Event()
        self.lease = lease
        self.handoff = False
        self.lease_ref: LeaseRef | None = None
        self._sequence = 0
        self.results: set[asyncio.Task[Any]] = set()
        self.lock = asyncio.Lock()

    def _reset_freshness(self) -> None:
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
            self.heartbeat_at = time.monotonic()
            self._opaque_received: int | None = None
            self._opaque_routes: dict[str, int] = {}
            self._opaque_legacy_busy = False
        else:
            self.sample_at = time.monotonic()

    def opaque_slot_idle(self) -> bool:
        """A current launcher heartbeat confirmed every physical slot empty."""
        return (
            self.connected and self.reconciliation_ready.is_set()
            and self.freshness_age() <= ADMISSION_FRESHNESS_SECONDS
            and self._opaque_received is not None
            and not self._opaque_routes and not self._opaque_legacy_busy
        )

    def opaque_slots_used(self, admission: Admission) -> int:
        # A route only adds occupancy beyond the holds if it has already been
        # settled (or was a send refused but not yet seen in a heartbeat).
        occupied = admission.held_slots() + sum(
            admission.request_bytes(self._opaque_reservation_key(route)) is None for route in self._opaque_routes
        )
        return max(occupied, int(self._opaque_legacy_busy))

    def _opaque_reservation_key(self, route_id: str) -> str:
        if route_id.startswith("agent:") and self.autonomous is not None:
            agent = self.autonomous.routes.get(route_id)
            return agent.reservation_key if agent is not None else route_id
        if route_id.startswith("summary:") and self.summaries is not None:
            route = self.summaries.routes.get(route_id)
            return route.reservation_key if route is not None else route_id
        return route_id.split(":")[1]

    def _receive_opaque_heartbeat(self, event: dict[str, Any]) -> None:
        self.meter_exhausted |= event.get("meter_exhausted") is True
        received = event.get("received_sequence")
        if self.declared_slots == CLOUDFLARE_DEV_SLOTS:
            occupied = event.get("slot_occupied")
            if type(occupied) is not bool or type(received) is not int or received < 0:
                # Older single-slot launchers can send liveness without occupancy.
                self._opaque_received = None
                self.heartbeat_at = time.monotonic()
                return
            self._opaque_legacy_busy = occupied
            routes = set(self._opaque_routes) if occupied else set()
        else:
            listed = event.get("occupied_routes")
            if (
                type(received) is not int or received < (self._opaque_received or 0)
                or not isinstance(listed, list) or len(listed) > self.declared_slots
                or any(not isinstance(route, str) or route not in self._opaque_routes for route in listed)
                or len(set(listed)) != len(listed)
            ):
                raise ProtocolError("Invalid Cloudflare slot occupancy heartbeat")
            routes = set(listed)
        self._opaque_routes = {
            route: sent for route, sent in self._opaque_routes.items()
            if sent > received or route in routes
        }
        self._opaque_received = received
        self.heartbeat_at = time.monotonic()

    def freshness_age(self) -> float:
        observed_at = (
            self.heartbeat_at if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE else self.sample_at
        )
        return time.monotonic() - observed_at

    def memory(self) -> tuple[int, int]:
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
            return -1, -1
        if not self.connected or self.freshness_age() > ADMISSION_FRESHNESS_SECONDS:
            return -1, self.limit
        return self.current, self.limit

    async def _reserve_cloudflare_usage(
        self, route_id: str, tenant_id: str, deadline: datetime | None,
        *, workflow_cpu_ms: int | None = None, subrequests: int | None = None,
        execution_id: str | None = None, engine_slot: int | None = None, org_id: str | None = None,
        queue_warm_until: datetime | None = None,
        preparation_bookings: list[PreparationBooking] | None = None,
        timeout_seconds: int | None = None,
    ) -> datetime | None:
        # Reserve under the current owner before emitting any frames. The
        # caller also fences the socket generation before sending secrets.
        lease = self.lease
        preparation = route_id.startswith("prepare:")
        started_at = None
        with remote_owner_context(LeaseRef(lease)):
            async with get_db_context() as db:
                if not preparation:
                    assert timeout_seconds is not None
                    started_at = await cloudflare_dispatch_started_at(db, lease.worker_id, timeout_seconds=timeout_seconds)
                    if started_at is None:
                        raise CredentialRouteChanged("Cloudflare admission closed before dispatch")
                    deadline = started_at + timedelta(seconds=timeout_seconds)
                if not await fence_remote_owner(db):
                    raise CredentialRouteChanged("Cloudflare ownership changed before dispatch")
                queue_installation = None
                if queue_warm_until is not None:
                    slot = await authorized_queue_preparation(db, lease.worker_id, UUID(route_id.rsplit(":", 1)[1]))
                    if slot is None or slot.ordinal != engine_slot or slot.tenant != (org_id or "platform"):
                        raise CredentialRouteChanged("Cloudflare queue preparation is no longer held")
                    queue_installation = slot.installation_id
                elif engine_slot is not None:
                    assert execution_id is not None
                    ticket = WorkflowSlotTicket(engine_slot, UUID(route_id.rsplit(":", 1)[1]))
                    if not await fence_workflow_slot_send(
                        db, lease.worker_id, UUID(execution_id), ticket,
                        tenant_id=org_id,
                        preparation=preparation,
                    ):
                        raise CredentialRouteChanged("Cloudflare workflow slot is no longer held")
                if not await reserve_tenant_host_usage(
                    db, lease.worker_id, deadline, workflow_cpu_ms=workflow_cpu_ms,
                    subrequests=subrequests, tenant_id=tenant_id, route_id=route_id, queue_warm_until=queue_warm_until,
                    preparation_bookings=preparation_bookings,
                ):
                    raise CloudflareRevisionRejected("Managed Cloudflare workflow usage could not be reserved")
                # The activity upsert serializes competing bookings. Check the
                # updated ledger before committing, without locking admission.
                if preparation and not await scoped_cloudflare_claim_allowed(
                    db, lease.worker_id, queue_installation,
                ):
                    raise CredentialRouteChanged("Cloudflare preparation admission closed")
        return started_at

    def _check_preparation_session(self, generation: int) -> None:
        if self.lease.session_generation != generation or not self.connected or not self.reconciliation_ready.is_set():
            raise CredentialRouteChanged("Cloudflare preparation session is no longer reconciled")
        if self.freshness_age() > ADMISSION_FRESHNESS_SECONDS or self.meter_exhausted or self.launcher_retired:
            raise CredentialRouteChanged("Cloudflare preparation admission is no longer open")

    def _stamp_workflow(self, route_id: str, **observations: datetime) -> None:
        stages = self._workflow_timings.setdefault(route_id, {})
        if self._session_ready_at is not None:
            stages.setdefault("session_ready_at", self._session_ready_at)
        for stage, at in observations.items():
            stages.setdefault(stage, at)
        # Refused dispatches can leave a preparation with no terminal to
        # consume it. Bound these samples independently of route authority.
        if len(self._workflow_timings) > 2 * max(1, self.declared_slots):
            self._workflow_timings.pop(next(iter(self._workflow_timings)))

    def take_workflow_timing(self, route_id: str) -> dict[str, datetime]:
        return self._workflow_timings.pop(route_id, {})

    async def _send_preparation(
        self, request_id: str, payload: dict[str, Any], expected_generation: int | None,
    ) -> bool:
        prepare_by = payload.get("prepare_by")
        if not self.managed_cloudflare or (prepare_by is None and self.lock.locked()):
            return False
        deadline = (
            datetime.fromtimestamp(prepare_by / 1000, UTC) if prepare_by is not None
            else datetime.now(UTC) + timedelta(seconds=PREPARE_SECONDS)
        )
        if datetime.now(UTC) >= deadline:
            return False
        generation = self.lease.session_generation if expected_generation is None else expected_generation
        socket = self.socket
        self._check_preparation_session(generation)
        engine_slot = payload.get("engine_slot")
        worker_id = self.lease.worker_id
        route_id = payload["route_id"]
        tenant = _cloudflare_tenant_key(payload["org_id"], worker_id, engine_slot)
        bookings: list[PreparationBooking] = []
        send_started = False
        try:
            async with asyncio.timeout(CLOUDFLARE_PREPARE_IO_SECONDS):
                await self._reserve_cloudflare_usage(
                    route_id, tenant, deadline,
                    execution_id=payload.get("execution_id"), engine_slot=engine_slot,
                    preparation_bookings=bookings,
                    org_id=payload["org_id"],
                    queue_warm_until=datetime.fromtimestamp(payload["warm_until"] / 1000, UTC)
                    if prepare_by is not None else None,
                )
            if prepare_by is None and self.lock.locked():
                return False
            async with contextlib.AsyncExitStack() as send_scope:
                # A released lock can still have a waiter entitled to acquire it first.
                async with asyncio.timeout(CLOUDFLARE_PREPARE_IO_SECONDS):
                    await send_scope.enter_async_context(self.lock)
                self._check_preparation_session(generation)
                if self.socket is not socket or datetime.now(UTC) >= deadline:
                    return False
                message = ControlEnvelope(
                    session_generation=generation, sequence=self._sequence + 1, kind="command",
                    payload={"id": request_id, "action": "prepare_workflow", "payload": payload},
                )
                frames = list(fragment(message))
                if len(frames) != 1 or frames[0].kind != "command":
                    raise ProtocolError("Cloudflare preparation must fit one frame")
                self._preparations[route_id] = (generation, deadline)
                # A lost single-frame send can leave a sequence gap without
                # leaving a partial fragment. Normal control traffic owns disconnects.
                self._sequence = message.sequence
                try:
                    async with asyncio.timeout(CLOUDFLARE_PREPARE_IO_SECONDS):
                        frame = frames[0].to_json()
                        send_started = True
                        await socket.send_text(frame)
                        execution_id = payload.get("execution_id")
                        if prepare_by is None and isinstance(execution_id, str):
                            self._stamp_workflow(
                                f"workflow:{execution_id}:{route_id.removeprefix('prepare:')}",
                                prepare_sent_at=datetime.now(UTC),
                            )
                except (WebSocketDisconnect, OSError, RuntimeError):
                    return False
        except TimeoutError:
            return False
        finally:
            if bookings and not send_started:
                async with get_db_context() as db:
                    await settle_unsent_preparation(db, worker_id, route_id, bookings[0])
        return True

    async def request(
        self, action: str, payload: dict[str, Any], *, expected_generation: int | None = None,
        authorization: AbstractAsyncContextManager[None] | None = None,
        workflow_started: Callable[[datetime], None] | None = None,
    ) -> dict[str, Any]:
        """Send one acknowledged command to the launcher.

        ``expected_generation`` fences a command that carries credentials
        minted for one session: if the session was replaced since, the
        command is refused with ``CredentialRouteChanged`` before anything is
        sent.
        """
        dispatch = action in ("workflow", "cloudflare_summary", "cloudflare_agent")
        opaque = self.runtime_profile != LINUX_RUNTIME_PROFILE and (dispatch or action == "terminate")
        if not self.connected:
            if opaque:
                raise CredentialRouteChanged("Opaque launcher disconnected before dispatch")
            raise ConnectionError("Launcher disconnected")
        request_id = str(uuid4())
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending[request_id] = future
        preparation = action == "prepare_workflow"
        socket = None
        try:
            if preparation:
                if not await self._send_preparation(request_id, payload, expected_generation):
                    return {}
                async with asyncio.timeout(_reply_timeout(action)):
                    return await asyncio.shield(future)
            try:
                async with self.lock:
                    if expected_generation is not None and self.lease.session_generation != expected_generation:
                        raise CredentialRouteChanged(
                            f"launcher session {expected_generation} was replaced by "
                            f"{self.lease.session_generation} before {action} dispatch"
                        )
                    if opaque:
                        summary_stop = (
                            action == "terminate" and self.summaries is not None
                            and self.inventory_ready.is_set()
                            and str(payload.get("route_id", "")).startswith("summary:")
                        )
                        if not self.connected or (not self.reconciliation_ready.is_set() and not summary_stop):
                            raise CredentialRouteChanged("Opaque launcher inventory is not reconciled")
                        if dispatch and self.freshness_age() > ADMISSION_FRESHNESS_SECONDS:
                            raise CredentialRouteChanged("Opaque launcher heartbeat is stale")
                        if dispatch and self.meter_exhausted:
                            raise CredentialRouteChanged("Cloudflare launcher workload allowance ended")
                        if dispatch and self.launcher_retired:
                            raise CredentialRouteChanged("Cloudflare launcher incarnation lifetime ended")
                        socket = self.socket
                    elif authorization is not None:
                        socket = self.socket
                    async with authorization if authorization is not None else nullcontext():
                        message = ControlEnvelope(
                            session_generation=self.lease.session_generation,
                            sequence=self._sequence + 1,
                            kind="command",
                            payload={"id": request_id, "action": action, "payload": payload},
                        )
                        if (
                            self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
                            and message.encoded_size() > CLOUDFLARE_MAX_LOGICAL_BYTES
                        ):
                            if action == "workflow":
                                raise CloudflareRevisionRejected("Cloudflare workflow exceeds the control payload limit")
                            raise ProtocolError("Cloudflare command exceeds the control payload limit")
                        if action == "workflow" and self.managed_cloudflare:
                            workspace = payload["context"]["cloudflare_workspace"]
                            engine_slot = payload["engine_slot"]
                            org_id = payload["active_execution"].get("org_id")
                            tenant = _cloudflare_tenant_key(
                                org_id, self.lease.worker_id, engine_slot,
                            )
                            timeout_seconds = payload["timeout_seconds"]
                            started_at = await self._reserve_cloudflare_usage(
                                payload["route_id"], tenant, None,
                                workflow_cpu_ms=workspace["cpu_budget_ms"], subrequests=workspace["subrequest_allowance"],
                                execution_id=payload["execution_id"], engine_slot=engine_slot,
                                org_id=org_id, timeout_seconds=timeout_seconds,
                            )
                            assert started_at is not None
                            deadline = (started_at + timedelta(seconds=timeout_seconds)).isoformat(timespec="milliseconds")
                            payload["started_at"] = started_at.isoformat(timespec="milliseconds")
                            payload["workflow_deadline"] = payload["context"]["workflow_deadline"] = deadline
                            if workflow_started is not None:
                                workflow_started(started_at)
                        sent = 0
                        send_deadline = loop.time() + _reply_timeout(action)
                        for frame in fragment(message):
                            if opaque and dispatch:
                                self._opaque_routes[payload["route_id"]] = message.sequence + (
                                    frame.payload["total"] - 1 if frame.kind == "fragment" else 0
                                )
                            if socket is None:
                                await self.socket.send_text(frame.to_json())
                            else:
                                if self.socket is not socket:
                                    raise OpaqueDispatchInterrupted("Opaque launcher socket was replaced during dispatch")
                                try:
                                    async with asyncio.timeout_at(send_deadline):
                                        await socket.send_text(frame.to_json())
                                except (asyncio.CancelledError, WebSocketDisconnect, OSError, RuntimeError) as exc:
                                    # A partial send leaves the fragment stream ambiguous.
                                    self.pending.pop(request_id, None)
                                    self.disconnect(socket)
                                    close = asyncio.create_task(socket.close(code=1011, reason="interrupted command"))
                                    close.add_done_callback(_observe_task)
                                    if isinstance(exc, asyncio.CancelledError):
                                        raise
                                    raise OpaqueDispatchInterrupted("Opaque launcher command send was interrupted") from exc
                                if self.socket is not socket:
                                    raise OpaqueDispatchInterrupted("Opaque launcher socket was replaced during dispatch")
                            sent += 1
                        self._sequence += sent
                        if action == "workflow" and self.managed_cloudflare:
                            self._stamp_workflow(payload["route_id"], dispatch_sent_at=datetime.now(UTC))
            except OpaqueDispatchInterrupted:
                raise
            except (WebSocketDisconnect, OSError, RuntimeError) as exc:
                if socket is None:
                    self.connected = False
                raise ConnectionError("Launcher control transport is unavailable") from exc
            async with asyncio.timeout(_reply_timeout(action)):
                return await asyncio.shield(future)
        finally:
            self.pending.pop(request_id, None)
            if preparation:
                self._preparations.pop(payload["route_id"], None)
            if future.done() and not future.cancelled():
                future.exception()  # A transport failure may precede awaiting the reply.
            elif not future.done():
                future.cancel()

    async def acknowledge(
        self, event_id: str, *, reconciliation_lease: RemoteOwnerLease | None = None,
    ) -> None:
        """Release a launcher-retained terminal result after canonical delivery."""
        ref = LeaseRef(reconciliation_lease) if reconciliation_lease is not None else (
            current_remote_lease_ref() or self.lease_ref
        )
        lease = ref.lease if ref is not None else self.lease
        socket = self.socket
        async with self.lock, contextlib.AsyncExitStack() as stack:
            if self.runtime_profile != LINUX_RUNTIME_PROFILE:
                if reconciliation_lease is not None:
                    if not self.connected or not self.inventory_ready.is_set():
                        raise ConnectionError("Opaque terminal replay session is unavailable")
                elif not self.reconciliation_ready.is_set():
                    raise ConnectionError("Opaque launcher inventory is not reconciled")
            if ref is not None:
                if self.lease != lease or self.socket is not socket:
                    raise ConnectionError("Remote session was replaced before ACK")
                await stack.enter_async_context(asyncio.timeout(DEFAULT_OWNER_LEASE_SECONDS))
                db = await stack.enter_async_context(get_db_context())
                with remote_owner_context(LeaseRef(lease)):
                    if not await fence_remote_owner(db):
                        raise ConnectionError("Remote ownership was lost before ACK")
                # Hold the ownership row lock through send, serializing ACK
                # with cross-API takeover. Bound a slow transport by the DB's
                # remaining lease, measured conservatively before the read.
                reading_at = time.monotonic()
                seconds = await db.scalar(select(func.extract(
                    "epoch", ExecutionWorker.remote_owner_lease_expires_at - func.clock_timestamp()
                )).where(ExecutionWorker.id == lease.worker_id))
                remaining = float(seconds or 0) - (time.monotonic() - reading_at)
                if remaining <= 0:
                    raise ConnectionError("Remote lease expired before ACK")
                if self.lease != lease or self.socket is not socket:
                    raise ConnectionError("Remote session was replaced during ACK check")
                await stack.enter_async_context(asyncio.timeout(remaining))
            self._sequence += 1
            await socket.send_text(ControlEnvelope(
                session_generation=lease.session_generation,
                sequence=self._sequence,
                kind="ack",
                payload={"event_id": event_id},
            ).to_json())

    async def spawn(self, claim: Any) -> RemoteProcess:
        key = (str(claim.id), str(claim.lease_token))
        adopted_route = self.platform_routes.get(key)
        if adopted_route is not None:
            process = self.jobs.get(adopted_route)
            if process is not None:
                return process
        route_id = str(uuid4())
        process = RemoteProcess(self, route_id)
        self.jobs[route_id] = process
        self.platform_routes[key] = route_id
        request_bytes = self.memory_request_bytes(str(claim.id))
        try:
            await self.request("platform_job", {"route_id": route_id,
                               "job_id": str(claim.id), "lease_token": str(claim.lease_token),
                               "memory_request_bytes": (
                                   request_bytes
                                   if request_bytes is not None
                                   else claim.memory_required_bytes
                               )})
        except (ConnectionError, TimeoutError):
            # The launcher may have forked before its reply was lost. Keep the
            # route until authoritative inventory proves it absent; otherwise
            # the canonical supervisor could requeue a second attempt.
            return process
        except BaseException:
            self.jobs.pop(route_id, None)
            if self.platform_routes.get(key) == route_id:
                self.platform_routes.pop(key, None)
            raise
        return process

    async def adopt_platform_job(self, route_id: str, claim: Any) -> RemoteProcess:
        """Attach the canonical platform supervisor to an already-live process."""
        key = (str(claim.id), str(claim.lease_token))
        self.platform_routes[key] = route_id
        process = self.jobs.get(route_id)
        if process is None:
            process = RemoteProcess(self, route_id)
            self.jobs[route_id] = process
        await self._release_deferred_terminals()
        return process

    def reject_missing_platform_routes(self, inventory: dict[str, Any]) -> None:
        """Resolve only launches the physical launcher did not report.

        A lost platform-launch reply is uncertain until the signed inventory
        arrives. Once that inventory omits the route (including its retained
        terminal journal), unblock the canonical supervisor as a failed child
        instead of leaving its heartbeat alive until the job timeout.
        """
        observed = {
            entry.get("route_id")
            for entry in [*inventory.get("routes", []), *inventory.get("terminal_routes", [])]
            if isinstance(entry, dict) and isinstance(entry.get("route_id"), str)
        }
        for route_id, process in tuple(self.jobs.items()):
            if route_id in observed:
                continue
            self.jobs.pop(route_id, None)
            for key, mapped_route in tuple(self.platform_routes.items()):
                if mapped_route == route_id:
                    self.platform_routes.pop(key, None)
            process.returncode = 1
            if not process.exited.done():
                process.exited.set_result(process.returncode)

    def memory_request_bytes(self, key: str) -> int | None:
        """Read an already-held admission request for reconnect inventory."""
        if self.admission is None:
            return None
        return self.admission.request_bytes(key)

    async def receive(self, event: dict[str, Any]) -> None:
        if "reply" in event:
            future = self.pending.get(event["reply"])
            if future is not None and not future.done():
                if "error" in event:
                    error_type = event.get("error_type")
                    if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and error_type in (
                        "CloudflareLauncherMeterLimitExceeded", "LauncherRetired",
                    ):
                        if error_type == "CloudflareLauncherMeterLimitExceeded":
                            self.meter_exhausted = True
                        else:
                            self.launcher_retired = True
                        error = CredentialRouteChanged
                    else:
                        error = MemoryError if error_type == "MemoryError" else RuntimeError
                    future.set_exception(error(event["error"]))
                else:
                    future.set_result(event["result"])
        elif event.get("event") == "heartbeat" and self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
            self._receive_opaque_heartbeat(event)
        elif event.get("event") == "sample":
            if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
                raise ProtocolError("Cloudflare launcher must send liveness heartbeats")
            self.current, self.limit = int(event["current"]), int(event["limit"])
            self.pressure_ratio = float(event["pressure_ratio"]) if "pressure_ratio" in event else (
                self.current / self.limit if self.limit > 0 else 1.0
            )
            self.sample_at = time.monotonic()
            if self.pool is not None:
                await self.pool.handle_event(event)
        elif event.get("event") == "inventory":
            inventory = event.get("inventory")
            if not isinstance(inventory, dict):
                raise ProtocolError("launcher inventory must be an object")
            if self.runtime_profile != LINUX_RUNTIME_PROFILE:
                version = inventory.get("version")
                if self.inventory_ready.is_set() or type(version) is not int or version != 1:
                    raise ProtocolError("expected one opaque inventory v1 per launcher generation")
                if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
                    self.meter_exhausted |= inventory.get("meter_exhausted") is True
                    live, terminals = inventory.get("routes"), inventory.get("terminal_routes")
                    if not isinstance(live, list) or not isinstance(terminals, list):
                        raise ProtocolError("Opaque inventory must include routes and terminals")
                    self._opaque_routes = {
                        entry["route_id"]: 0 for entry in live + terminals
                        if isinstance(entry, dict) and isinstance(entry.get("route_id"), str)
                        and entry["route_id"].startswith(("workflow:", "summary:", "agent:"))
                    }
            if self.runtime_profile == LINUX_RUNTIME_PROFILE:
                baseline_bytes = inventory.get("baseline_bytes")
                if (
                    isinstance(baseline_bytes, bool)
                    or not isinstance(baseline_bytes, int)
                    or baseline_bytes < 0
                ):
                    raise ProtocolError("launcher inventory must include its baseline memory")
                self.baseline_bytes = baseline_bytes
                requirements = event.get("requirements")
                if requirements is not None:
                    if not isinstance(requirements, dict):
                        raise ProtocolError("launcher requirements must be an object")
                    self.requirements = requirements
            self.inventory = inventory
            self.inventory_ready.set()
        elif event.get("event") in {
            "result", "job_exit", "agent_exit", "summary_result", "cloudflare_agent_result", "cloudflare_agent_request",
        }:
            if event["event"] == "summary_result" and self.summaries is None:
                raise ProtocolError("Launcher did not negotiate Cloudflare summaries")
            if event["event"].startswith("cloudflare_agent_") and self.autonomous is None:
                raise ProtocolError("Launcher did not negotiate Cloudflare autonomous agents")
            if self.runtime_profile != LINUX_RUNTIME_PROFILE and event["event"] in {"job_exit", "agent_exit"}:
                raise ProtocolError("Unexpected opaque launcher result")
            # Inventory is authoritative when an API takes over. In
            # particular, a terminal replay can arrive before the old route
            # has been recreated in this API process. Retain it until that
            # reconciliation succeeds; an ACK before then would lose it.
            if not self.reconciliation_ready.is_set():
                self._deferred_terminal.append(event)
                self._terminal_replay_changed.set()
            else:
                await self._deliver_terminal(event)
        elif event.get("event") == "cloudflare_generation_request":
            if not self.managed_cloudflare or not self.reconciliation_ready.is_set():
                raise ProtocolError("Unexpected Cloudflare generation request")
            await self._reserve_generation(event)
        elif event.get("event") == "draining":
            # The router tells this session's executor to drain after the
            # transport event is accepted. It is not a pool result.
            return
        else:
            # Completion may call SDK/Redis/DB or send a new command. Never
            # block this receiver, which must also deliver command replies.
            async def handle_event() -> None:
                if self.lease_ref is None:
                    await self.pool.handle_event(event)
                    return
                with remote_owner_context(self.lease_ref):
                    await self.pool.handle_event(event)
            task = asyncio.create_task(handle_event())
            self.results.add(task)
            task.add_done_callback(self.results.discard)
            task.add_done_callback(_observe_task)

    async def _reserve_generation(self, event: dict[str, Any]) -> None:
        route_id, identity, request_id = (event.get(key) for key in ("route_id", "identity", "request_id"))
        preparation = self._preparations.get(route_id) if isinstance(route_id, str) else None
        if isinstance(route_id, str) and route_id.startswith("prepare:") and (
            preparation is None or preparation[0] != self.lease.session_generation or datetime.now(UTC) >= preparation[1]
        ):
            # The hint timed out or belonged to a replaced control session.
            # Never authorize a late Loader lookup or disconnect the workflow.
            return
        if (
            not isinstance(route_id, str) or (route_id not in self._opaque_routes and preparation is None)
            or not isinstance(identity, str)
            or len(identity) > 128
            or not fullmatch(r"tenant-eng-tenant-py-[a-f0-9]{32}-[a-f0-9]{16}-cpu(?:default|\d+)-sr(?:default|\d+)-g\d+", identity)
            or not isinstance(request_id, str) or not fullmatch(r"[A-Za-z0-9_-]{43}", request_id)
        ):
            raise ProtocolError("Invalid Cloudflare generation reservation")
        lease = self.lease

        async def reserve() -> None:
            with remote_owner_context(LeaseRef(lease)):
                async with get_db_context() as db:
                    fence = UUID(route_id.rsplit(":", 1)[1])
                    slot = None
                    accepted = False
                    if await fence_remote_owner(db):
                        slot = await db.scalar(select(CloudflareWorkflowSlot).where(
                            CloudflareWorkflowSlot.worker_id == lease.worker_id,
                            CloudflareWorkflowSlot.fence == fence,
                            CloudflareWorkflowSlot.released_at.is_(None),
                        ))
                        if slot is None and preparation is not None:
                            slot = await authorized_queue_preparation(db, lease.worker_id, fence)
                    if slot is not None:
                        org_id = None if slot.tenant == "platform" else slot.tenant
                        key = _cloudflare_tenant_key(org_id, lease.worker_id, slot.ordinal)
                        accepted = (
                            identity.startswith(f"tenant-eng-tenant-py-{key}-")
                            and (preparation is None or (
                                self._preparations.get(route_id) == preparation and datetime.now(UTC) < preparation[1]
                            ))
                            and await reserve_engine_generation(db, lease.worker_id, identity)
                        )
                await self.request("cloudflare_generation_reply", {
                    "request_id": request_id, "route_id": route_id, "ok": accepted, "result": {},
                }, expected_generation=lease.session_generation)

        task = asyncio.create_task(reserve())
        self.results.add(task)
        task.add_done_callback(self.results.discard)
        task.add_done_callback(_observe_task)

    async def wait_for_opaque_terminals(
        self, receipts: set[str], lease: RemoteOwnerLease,
    ) -> dict[str, dict[str, Any]]:
        """Read retained result payloads replayed after the inventory."""
        socket = self.socket
        observed: dict[str, dict[str, Any]] = {}
        try:
            async with asyncio.timeout(COMMAND_REPLY_SECONDS):
                while True:
                    if not self.connected or self.lease != lease or self.socket is not socket:
                        raise ConnectionError("Opaque terminal replay session was replaced")
                    self._terminal_replay_changed.clear()
                    observed = {
                        event_id: event for event in self._deferred_terminal
                        if isinstance(event_id := event.get("_event_id"), str) and event_id in receipts
                    }
                    if observed.keys() == receipts:
                        break
                    await self._terminal_replay_changed.wait()
        except TimeoutError:
            logging.getLogger(__name__).warning("Opaque retained terminal replay was incomplete")
        return observed

    async def reconciliation_complete(self) -> None:
        """Permit terminal replay only after canonical routes were restored."""
        if self.managed_cloudflare:
            self._session_ready_at = datetime.now(UTC)
        self.reconciliation_ready.set()
        deferred, self._deferred_terminal = self._deferred_terminal, []
        for event in deferred:
            if await self._deliver_terminal(event):
                continue
            # A terminal platform runner already committed its own canonical
            # result. Reconciliation rejected its old route, so retaining the
            # exit would hold a dead launcher open forever.
            if event.get("event") == "job_exit" and isinstance(event.get("_event_id"), str):
                await self.acknowledge(event["_event_id"])
            else:
                self._deferred_terminal.append(event)

    async def _release_deferred_terminals(self) -> None:
        deferred, self._deferred_terminal = self._deferred_terminal, []
        for event in deferred:
            if not await self._deliver_terminal(event):
                self._deferred_terminal.append(event)

    async def _deliver_terminal(self, event: dict[str, Any]) -> bool:
        event_id = event.get("_event_id")
        usage = event.get("usage")
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and isinstance(usage, dict):
            self.meter_exhausted |= usage.get("exhausted") is True
        if event.get("event") == "job_exit":
            route_id = event.get("route_id")
            process = self.jobs.pop(route_id, None) if isinstance(route_id, str) else None
            if process is None:
                # The platform supervisor is attached from inventory. Do not
                # discard an exit that beat that attachment.
                return False
            process.returncode = int(event["returncode"])
            if not process.exited.done():
                process.exited.set_result(process.returncode)
            for key, mapped_route in tuple(self.platform_routes.items()):
                if mapped_route == route_id:
                    self.platform_routes.pop(key, None)
            if isinstance(event_id, str):
                await self.acknowledge(event_id)
            return True

        if event.get("event") == "summary_result":
            assert self.summaries is not None
            deliver = self.summaries.handle_result
        elif event.get("event") == "cloudflare_agent_result":
            assert self.autonomous is not None
            deliver = self.autonomous.handle_result
        elif event.get("event") == "cloudflare_agent_request":
            agents = self.autonomous
            assert agents is not None
            generation = self.lease.session_generation

            async def handle_agent_request(event: dict[str, Any]) -> None:
                await agents.handle_request(event, generation)
            deliver = handle_agent_request
        elif event.get("event") == "agent_exit":
            agents = self.agents
            if agents is None or not agents.tracks(event.get("route_id")):
                if not self.reconciliation_ready.is_set():
                    # Its route may still be adopted from inventory.
                    return False
                # Reconciliation rejected the route, or its run was already
                # settled: nothing here waits for this exit.
                if isinstance(event_id, str):
                    await self.acknowledge(event_id)
                return True
            deliver = agents.handle_exit
        elif self.pool is None:
            return False
        else:
            deliver = self.pool.handle_event

        # Completion may call SDK/Redis/DB or send a new command. Never
        # block this receiver, which must also deliver command replies. The
        # launcher receives ACK only after that canonical mutation returns.
        inherited_ref = current_remote_lease_ref() or self.lease_ref
        # A terminal event belongs to the generation that delivered it. Unlike
        # the executor's mutable reconnect ref, it cannot adopt a newer owner.
        terminal_ref = LeaseRef(inherited_ref.lease) if inherited_ref is not None else None

        async def handle_event() -> None:
            if terminal_ref is None:
                await deliver(event)
                if isinstance(event_id, str):
                    await self.acknowledge(event_id)
            else:
                with remote_owner_context(terminal_ref):
                    await deliver(event)
                    if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and event.get("event") in (
                        "result", "summary_result", "cloudflare_agent_result",
                    ):
                        async with get_db_context() as db:
                            if not await fence_remote_owner(db):
                                raise ConnectionError("Remote API ownership was replaced before metering")
                            uncertain_attempt = event.get("attempt_state") in ("stopped", "uncertain")
                            route = event.get("route_id")
                            uncertain_route = (
                                route if uncertain_attempt and event.get("event") == "result" and isinstance(route, str)
                                else None
                            )
                            await record_launcher_terminal_meter(
                                db, terminal_ref.lease.worker_id, event.get("usage"),
                                uncertain_route=uncertain_route,
                                incarnation_uncertain=uncertain_attempt and uncertain_route is None,
                            )
                    if isinstance(event_id, str):
                        await self.acknowledge(event_id)

        task = asyncio.create_task(handle_event())
        self.results.add(task)
        task.add_done_callback(self.results.discard)
        task.add_done_callback(_observe_task)
        socket = self.socket

        def reconnect_on_failure(done: asyncio.Task[Any]) -> None:
            if done.cancelled() or done.exception() is None or self.socket is not socket:
                return
            async def close_current_socket() -> None:
                with contextlib.suppress(Exception):
                    await socket.close(code=1011, reason="terminal delivery failed")
            asyncio.create_task(close_current_socket())

        task.add_done_callback(reconnect_on_failure)
        return True

    def replace_socket(self, socket: WebSocket, lease: RemoteOwnerLease) -> None:
        """Fence the old socket while preserving the launcher's live routes."""
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            self.disconnect()
        self.socket = socket
        self.lease = lease
        self.connected = True
        self._reset_freshness()
        self._session_ready_at = None
        self.inventory = None
        self.inventory_ready.clear()
        self.reconciliation_ready.clear()
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            self._deferred_terminal.clear()  # The new socket replays its own journal.

    def disconnect(self, socket: WebSocket | None = None) -> None:
        if socket is not None and socket is not self.socket:
            return
        self.connected = False
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            self.reconciliation_ready.clear()
            self._terminal_replay_changed.set()
        for future in self.pending.values():
            if not future.done():
                future.set_exception(ConnectionError("Launcher disconnected"))


def _observe_task(task: asyncio.Task[Any]) -> None:
    if not task.cancelled() and task.exception() is not None:
        logging.getLogger(__name__).error("Thin control task failed", exc_info=task.exception())


class PackageProxy(PackageInstallConsumer):
    def __init__(self, bridge: Bridge) -> None:
        self.bridge = bridge

    @property
    def _worker_id(self) -> str:
        return self.bridge.pool.worker_id

    async def _package_tool(self, operation: str, **payload: Any) -> str | None:
        result = await self.bridge.request(
            "package_tool", {"operation": operation, **payload}
        )
        error = result.get("error")
        return error if isinstance(error, str) else None

    async def _pip_install(self, package: str, version: str | None) -> str | None:
        return await self._package_tool("install", package=package, version=version)

    async def _pip_uninstall(self, package: str) -> str | None:
        return await self._package_tool("uninstall", package=package)

    async def _pip_install_requirements(self) -> str | None:
        if self.bridge.containers:
            from src.core.requirements_cache import get_requirements

            cached = await get_requirements()
            return await self._package_tool("requirements", content=cached["content"] if cached else None)
        return await self._package_tool("requirements")

    async def _recycle_workers(self) -> None:
        await self.bridge.pool.drain_and_restart_template()

    async def _update_pool_packages(self) -> None:
        pool = self.bridge.pool
        if not pool._started:
            return
        try:
            result = await self.bridge.request("package_tool", {"operation": "inventory"})
            packages = result["packages"]
            redis = await pool._get_redis()
            await redis.hset(
                f"bifrost:pool:{pool.worker_id}", "packages", json.dumps(packages)
            )
        except Exception as exc:
            logging.getLogger(__name__).warning("Failed to update packages in Redis: %s", exc)


class ApiExecutor(PoolWorker):
    """One admitted remote worker: workflows, agent runs, services and
    platform jobs, each in a launcher child."""

    def __init__(self, bridge: Bridge, incarnation_id: UUID, *, remote_static: bool = False) -> None:
        def ready() -> None:
            if self.runtime_profile != LINUX_RUNTIME_PROFILE and self._lost:
                return
            notification = asyncio.create_task(bridge.request("ready", {}))
            notification.add_done_callback(_observe_task)
        super().__init__(
            incarnation_id=None if remote_static else incarnation_id,
            job_uid="remote-controlled",
            pod_uid="remote-controlled",
            on_ready=ready,
            handlers=bridge.handlers,
            execution_lane=bridge.execution_lane,
            runtime_profile=bridge.runtime_profile,
        )
        self.incarnation_id = incarnation_id
        self.bridge = bridge
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and CLOUDFLARE_SUMMARY_HANDLER in bridge.handlers:
            from src.services.execution.cloudflare_summary import summary_endpoint

            endpoint = summary_endpoint(self.settings.cloudflare_summary_endpoint)
            assert endpoint is not None  # Hello validates the opt-in endpoint.
            bridge.summaries = CloudflareSummaryRoutes(bridge, incarnation_id, endpoint)
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and CLOUDFLARE_AGENT_HANDLER in bridge.handlers:
            from src.services.execution.cloudflare_summary import summary_endpoint

            endpoint = summary_endpoint(self.settings.cloudflare_agent_endpoint)
            assert endpoint is not None
            bridge.autonomous = CloudflareAgentRoutes(bridge, incarnation_id, endpoint)
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
            self.admission = Admission(
                budget_bytes=bridge.declared_slots * CLOUDFLARE_DEV_BUDGET_BYTES,
                declared_slots=bridge.declared_slots,
                occupied_slots=self._opaque_slots_used,
                under_pressure=lambda: False,
                running_workflow_ids=set,
                workflow_growth=dict,
                claims_open=self._claims_open,
            )
        self._reconcile_lock = asyncio.Lock()
        self._reconciled_generation = 0
        self._admission_prepared = False

    @property
    def supports_linux_background_work(self) -> bool:
        return not self.bridge.containers and super().supports_linux_background_work

    def _opaque_slots_used(self) -> int:
        assert self.admission is not None
        return self.bridge.opaque_slots_used(self.admission)

    def active_runs(self) -> int:
        if self.bridge.summaries is not None or self.bridge.autonomous is not None:
            return self._workflow_runs()  # Declared occupancy includes summary holds and terminals.
        return super().active_runs()

    def _workflow_runs(self) -> int:
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
            return max(self._opaque_slots_used(), int(not self.bridge.opaque_slot_idle()))
        return super()._workflow_runs()

    async def _initialize_runtime(self) -> bool:
        return await report_worker_handlers(
            self.incarnation_id, [handler for handler, _version in self._handlers],
        )

    async def _start_executor(self) -> None:
        """Keep the canonical heartbeat alive while the launcher bootstraps."""
        await self.bridge.inventory_ready.wait()
        if self.runtime_profile == LINUX_RUNTIME_PROFILE:
            from src.services.execution.process_pool import (
                _notify_requirements_failures,
            )
            from src.services.execution.requirements_setup_result import (
                RequirementsInstallResult,
            )

            requirements = RequirementsInstallResult.from_json_dict(self.bridge.requirements)
            await _notify_requirements_failures(requirements)
        try:
            await super()._start_executor()
        except BaseException:
            if self.runtime_profile != LINUX_RUNTIME_PROFILE and self.bridge.pool is not None:
                await self.bridge.pool.ownership_lost("Opaque executor startup interrupted")
                if self._workflow_consumer is not None:
                    await self._workflow_consumer.stop_pool()
            raise

    async def _close_runtime(self) -> None:
        # The app lifespan owns this API replica's database engine.
        pass

    async def _start_background_control(self) -> list[asyncio.Task[Any]]:
        # src.main already starts this API replica's canonical controllers.
        return []

    @property
    def _queue_prewarm_slots(self) -> int:
        if self.bridge.managed_cloudflare and (self.bridge.inventory or {}).get("queue_prewarm") is True:
            return self.bridge.lease.declared_slots
        return 0

    def _start(self, claim: PoolClaim) -> None:
        bridge = self.bridge
        if claim.workflow_slot is not None and claim.cloudflare_claim_committed_at is not None:
            bridge._stamp_workflow(
                claim.workflow_slot.workflow_route(claim.subject_id),
                queued_at=claim.queued_at,
                gate_eligible_at=claim.queued_at + timedelta(seconds=CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS),
                claim_at=claim.cloudflare_claim_committed_at,
            )
        preparations = []
        org_id = str(claim.tenant_id) if claim.tenant_id is not None else None
        if (
            bridge.managed_cloudflare and claim.handler == "workflow.execute"
            and claim.claimed is not None and claim.cloudflare_revision is not None
            and claim.workflow_slot is not None
            and (bridge.inventory or {}).get("admission_prepare") is True
        ):
            preparations.append({
                "route_id": f"prepare:{claim.workflow_slot.fence}", "execution_id": str(claim.subject_id),
                "org_id": org_id, "engine_slot": claim.workflow_slot.ordinal,
            })
        preparations.extend({
            "route_id": f"prepare:{hint.fence}", "execution_id": str(hint.execution_id),
            "org_id": org_id, "engine_slot": hint.ordinal,
            "prepare_by": int(hint.prepare_by.timestamp() * 1000),
            "warm_until": int(hint.warm_until.timestamp() * 1000),
        } for hint in claim.queue_preparations)
        generation = bridge.lease.session_generation

        async def prepare(payload: dict[str, Any]) -> None:
            try:
                await bridge.request("prepare_workflow", payload, expected_generation=generation)
            except (CredentialRouteChanged, ConnectionError, TimeoutError, RuntimeError):
                # Preparation has no authority to run the workflow. The
                # canonical dispatch owns normal entry and failure handling.
                logging.getLogger(__name__).debug("Cloudflare preparation did not complete", exc_info=True)

        for payload in preparations:
            task = asyncio.create_task(prepare(payload), name=f"cloudflare-{payload['route_id']}")
            bridge.results.add(task)
            task.add_done_callback(bridge.results.discard)
            task.add_done_callback(_observe_task)
        super()._start(claim)

    def _make_workflow_consumer(self) -> Any:
        from src.jobs.consumers.workflow_execution import WorkflowExecutionConsumer
        from src.worker.remote.remote_pool import ThinRemoteProcessPool

        self.bridge.pool = ThinRemoteProcessPool(
            self.bridge, worker_id=str(self.incarnation_id), runtime_profile=self.runtime_profile,
            baked_wheels_only=self.bridge.lease.container_release is not None,
        )
        if self.runtime_profile == LINUX_RUNTIME_PROFILE:
            from src.services.execution.requirements_setup_result import (
                RequirementsInstallResult,
            )

            self.bridge.pool._apply_requirements_status(
                RequirementsInstallResult.from_json_dict(self.bridge.requirements)
            )
        return WorkflowExecutionConsumer(pool=self.bridge.pool)

    def _make_package_consumer(self) -> PackageProxy:
        return PackageProxy(self.bridge)

    def _make_agent_consumer(self) -> RemoteAgentConsumer:
        if self.bridge.agents is None:
            self.bridge.agents = RemoteAgentConsumer(self.bridge, worker_id=self.incarnation_id)
        return self.bridge.agents

    async def _execute_summary(self, claim: PoolClaim) -> None:
        if self.bridge.summaries is not None:
            await self.bridge.summaries.run(claim, self.bridge.lease.session_generation)
        else:
            await self._make_agent_consumer().process_message(claim.message, handler=claim.handler)

    async def _execute_agent(self, claim: PoolClaim) -> None:
        if self.bridge.autonomous is not None:
            await self.bridge.autonomous.run(claim, self.bridge.lease.session_generation)
        else:
            await super()._execute_agent(claim)

    def _measure_budget(self) -> MemoryBudget:
        current, limit = self.bridge.memory()
        if current < 0 or limit <= 0:
            raise RuntimeError("Launcher telemetry unavailable")
        return MemoryBudget(limit_bytes=limit, baseline_bytes=self.bridge.baseline_bytes,
                            threshold=self.settings.memory_pressure_threshold)

    def _under_pressure(self) -> bool:
        current, limit = self.bridge.memory()
        return (
            current < 0
            or limit <= 0
            or self.bridge.pressure_ratio >= self.settings.memory_pressure_threshold
        )

    def _uses_process_lifetime(self) -> bool:
        return not self.bridge.managed_cloudflare and super()._uses_process_lifetime()

    def _claims_open(self) -> bool:
        """Closed from a reconnect until that socket's inventory is
        reconciled. The launcher takes its inventory before it reads any
        command, so work launched in between would be missing from it and
        reconciliation would fail its live child. Every claim (workflow and
        agent runs, services, platform jobs) passes ``Admission.claiming``."""
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and (
            self._lost or self.bridge.meter_exhausted or self.bridge.launcher_retired or not self.bridge.connected
            or self.bridge.freshness_age() > ADMISSION_FRESHNESS_SECONDS
            or self.bridge.managed_cloudflare and (self.bridge.inventory or {}).get("tenant_slot_protocol") != 1
        ):
            return False
        stages = self.bridge.platform_stage_backend
        return self.bridge.reconciliation_ready.is_set() and (stages is None or not stages.releasing)

    @property
    def supports_platform_jobs(self) -> bool:
        return self.bridge.platform_stage_backend is not None or super().supports_platform_jobs

    def active_handler_counts(self) -> dict[str, int]:
        counts = super().active_handler_counts()
        stages = self.bridge.platform_stage_backend
        if stages is not None:
            counts[SOLUTION_GIT_CAPABILITY] = int(bool(self._platform_jobs) or stages.active is not None)
        return counts

    async def _execute_platform_job(self, claim: Any) -> bool:
        return await run_claimed_platform_job(claim, backend=self.bridge.platform_stage_backend or self.bridge)

    async def _claim_platform_job(self, fits_bytes: int) -> Any:
        stages = self.bridge.platform_stage_backend
        if stages is not None:
            if self._platform_jobs or stages.busy or stages.active is not None:
                return None
            return await claim_platform_job(
                backend="local", fits_bytes=fits_bytes, memory_sample=self.bridge.memory(),
                job_types=frozenset(stages.handlers),
            )
        return await claim_platform_job(backend="local", fits_bytes=fits_bytes,
                                        memory_sample=self.bridge.memory())

    async def _prepare_admission(self) -> None:
        """Adopt physical routes after memory admission exists, before claims run."""
        assert self.admission is not None
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            while not self._lost:
                await self.bridge.inventory_ready.wait()
                generation = self.bridge.lease.session_generation
                snapshot = await self._reconcile_transport_inventory(generation)
                if snapshot is not None:
                    await self._apply_reconciled_snapshot(snapshot)
                    self.bridge.admission = self.admission
                    await self.bridge.reconciliation_complete()
                    self.bridge.pool.resume_opaque_stops(snapshot.get("opaque_live_workflows", {}))
                    self._admission_prepared = True
                    return
            return
        self.bridge.admission = self.admission
        snapshot = await self._reconcile_transport_inventory()
        assert snapshot is not None
        await super()._prepare_admission()
        await self._apply_reconciled_snapshot(snapshot)
        await self.bridge.reconciliation_complete()
        self._admission_prepared = True

    async def reconcile_transport_inventory(self, *, expected_generation: int | None = None) -> None:
        """Reconcile a same-owner reconnect before its terminal replay runs."""
        if (
            expected_generation is not None
            and self.bridge.lease.session_generation != expected_generation
        ):
            return
        if not self._admission_prepared:
            # Initial inventory is consumed by _prepare_admission,
            # after PoolWorker has constructed its admission gate.
            return
        snapshot = await self._reconcile_transport_inventory(expected_generation)
        if snapshot is None:
            return
        await self._apply_reconciled_snapshot(snapshot)
        if (
            expected_generation is not None
            and self.bridge.lease.session_generation != expected_generation
        ):
            return
        await self.bridge.reconciliation_complete()
        if self.runtime_profile != LINUX_RUNTIME_PROFILE:
            self.bridge.pool.resume_opaque_stops(snapshot.get("opaque_live_workflows", {}))

    async def _reconcile_transport_inventory(
        self, expected_generation: int | None = None
    ) -> dict[str, Any] | None:
        await self.bridge.inventory_ready.wait()
        assert self.bridge.pool is not None and self.bridge.inventory is not None
        async with self._reconcile_lock:
            generation = self.bridge.lease.session_generation
            if expected_generation is not None and generation != expected_generation:
                return None
            if self._reconciled_generation == generation:
                return {
                    "workflow_requests": {}, "services": [], "platform_jobs": [], "agent_runs": {},
                }
            if self.runtime_profile != LINUX_RUNTIME_PROFILE:
                inventory = self.bridge.inventory
                if autonomous := self.bridge.autonomous:
                    live, terminals = inventory["routes"], inventory["terminal_routes"]
                    if len(live) + len(terminals) > self.bridge.declared_slots:
                        raise ProtocolError("Opaque inventory exceeds the prepared capacity")

                    def is_autonomous(entry: Any) -> bool:
                        return isinstance(entry, dict) and entry.get("kind") == "agent_run"

                    for route in await autonomous.reconcile([entry for entry in live + terminals if is_autonomous(entry)]):
                        self._wait_for_adopted_autonomous_run(autonomous, route)
                    inventory = {
                        **inventory, "routes": [entry for entry in live if not is_autonomous(entry)],
                        "terminal_routes": [entry for entry in terminals if not is_autonomous(entry)],
                    }
                summaries = self.bridge.summaries
                adopted: list[SummaryRoute] = []
                if summaries is not None:
                    def is_summary(entry: Any) -> bool:
                        return isinstance(entry, dict) and entry.get("kind") == "agent_summary"

                    live, terminals = inventory["routes"], inventory["terminal_routes"]
                    if len(live) + len(terminals) > self.bridge.declared_slots:
                        raise ProtocolError("Opaque inventory exceeds the prepared capacity")
                    adopted = await summaries.reconcile(
                        [entry for entry in live if is_summary(entry)],
                        [entry for entry in terminals if is_summary(entry)],
                    )
                    inventory = {
                        **inventory, "routes": [entry for entry in live if not is_summary(entry)],
                        "terminal_routes": [entry for entry in terminals if not is_summary(entry)],
                    }
                snapshot = await self.bridge.pool.reconcile_inventory(inventory, admission=self.admission)
                if snapshot is not None and summaries is not None:
                    for route in adopted:
                        self._wait_for_adopted_summary(summaries, route)
            else:
                if self.bridge.containers:
                    for key in ("routes", "terminal_routes"):
                        entries = self.bridge.inventory.get(key)
                        if not isinstance(entries, list) or any(
                            not isinstance(entry, dict) or entry.get("kind") != "workflow" for entry in entries
                        ):
                            raise ProtocolError("Containers inventory must contain only workflow routes")
                    stages = self.bridge.platform_stage_backend
                    if stages is not None:
                        await stages.reconcile(self.bridge.inventory.get("compute_stages"))
                    elif self.bridge.inventory.get("compute_stages"):
                        raise ProtocolError("Unnegotiated Container stage inventory")
                snapshot = await self.bridge.pool.reconcile_inventory(self.bridge.inventory)
            if self.runtime_profile != LINUX_RUNTIME_PROFILE and snapshot is None:
                self.bridge.disconnect()
                self._lost = True
                self.request_stop()
                return None
            if expected_generation is not None and self.bridge.lease.session_generation != generation:
                return None
            if self.runtime_profile == LINUX_RUNTIME_PROFILE and not self.bridge.containers:
                self.bridge.reject_missing_platform_routes(self.bridge.inventory)
                assert self.bridge.agents is not None
                await self.bridge.agents.reject_missing(self.bridge.inventory)
            self._reconciled_generation = generation
            return snapshot

    async def _apply_reconciled_snapshot(self, snapshot: dict[str, Any]) -> None:
        assert self.admission is not None
        for execution_id, request_bytes in snapshot["workflow_requests"].items():
            self.admission.hold(execution_id, request_bytes, workflow=True)
            self.admission.started(execution_id)
            self._remember_started(execution_id)
        if self.runtime_profile != LINUX_RUNTIME_PROFILE or self.bridge.containers:
            return
        agents = self.bridge.agents
        assert agents is not None
        for route_id, (run_id, request_bytes) in snapshot["agent_runs"].items():
            if agents.tracks(route_id):
                # A same-owner reconnect: its original wait still runs.
                continue
            self._wait_for_adopted_agent_run(agents.adopt(route_id, run_id), request_bytes)
        for route_id, (run_id, request_bytes, handler, claim_id) in snapshot.get("summaries", {}).items():
            if not agents.tracks(route_id):
                self._wait_for_adopted_agent_run(
                    agents.adopt(route_id, run_id, handler=handler, claim_id=claim_id), request_bytes,
                )
        if self._service_claim_loop is None:
            return
        for owned in snapshot["services"]:
            key = str(owned.attempt_id)
            previous = self._service_claim_loop._owned.get(key)
            if previous is not None:
                owned.stop_mirrored = previous.stop_mirrored
                owned.peak_memory_bytes = max(
                    owned.peak_memory_bytes, previous.peak_memory_bytes
                )
            self._service_claim_loop._owned[key] = owned
            self._hold_service_memory(str(owned.attempt_id), owned.memory_request_bytes)
        for route_id, claim in snapshot["platform_jobs"]:
            await self.bridge.adopt_platform_job(route_id, claim)
            if claim.id in self._platform_jobs:
                continue
            self.admission.hold(str(claim.id), claim.memory_required_bytes)
            self._platform_jobs.add(claim.id)
            asyncio.create_task(
                self._run_platform_job(claim), name=f"pool-platform-job-{claim.id}"
            )

    def _wait_for_adopted_autonomous_run(
        self, autonomous: CloudflareAgentRoutes, route: CloudflareAgentRoute,
    ) -> None:
        admission = self.admission
        assert admission is not None
        admission.hold(route.reservation_key, AGENT_RUN_REQUEST_BYTES)

        async def wait() -> None:
            try:
                await autonomous.supervise_adopted(route)
            finally:
                admission.release(route.reservation_key)

        task = asyncio.create_task(wait(), name=f"pool-adopted-autonomous-{route.run_id}")
        tasks = self._tasks[CLOUDFLARE_AGENT_HANDLER[0]]
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        task.add_done_callback(_observe_task)

    def _wait_for_adopted_summary(self, summaries: CloudflareSummaryRoutes, route: SummaryRoute) -> None:
        admission = self.admission
        assert admission is not None
        admission.hold(route.reservation_key, route.memory_request_bytes)

        async def wait() -> None:
            try:
                await summaries.wait(route)
            finally:
                admission.release(route.reservation_key)

        task = asyncio.create_task(wait(), name=f"pool-adopted-summary-{route.run_id}")
        tasks = self._tasks[CLOUDFLARE_SUMMARY_HANDLER[0]]
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        task.add_done_callback(_observe_task)

    def _wait_for_adopted_agent_run(self, route: AgentRoute, request_bytes: int) -> None:
        """Hold an adopted child's memory and count it as a run until it ends."""
        assert self.admission is not None and self.bridge.agents is not None
        key = f"{route.handler}:{route.run_id}"
        self.admission.hold(key, request_bytes)

        async def wait() -> None:
            try:
                assert self.bridge.agents is not None
                await self.bridge.agents.supervise(route)
            except AssertionError:
                raise
            except Exception:
                logging.getLogger(__name__).exception(
                    "Adopted agent run failed", extra={"run_id": str(route.run_id)}
                )
            finally:
                assert self.admission is not None
                self.admission.release(key)

        task = asyncio.create_task(wait(), name=f"pool-adopted-agent-{route.run_id}")
        in_flight = self._tasks[route.handler]
        in_flight.add(task)
        task.add_done_callback(in_flight.discard)

    async def _drain(self) -> None:
        """Drain, then end the agent runs the drain grace did not outlast.

        The workflow pool terminates its children at the drain deadline with a
        recorded shutdown outcome. Agent children get the same treatment here,
        before ``_retire`` tells the launcher to stop (which kills them with no
        exit report). An owner that lost the launcher or is handing it over
        leaves its children alone: the next owner adopts or ends them.
        """
        from src.worker.remote.remote_pool import (
            DRAIN_DEADLINE_ERROR,
            DRAIN_DEADLINE_ERROR_TYPE,
        )

        await super()._drain()
        agents = self.bridge.agents
        if agents is None or not agents.routes or self._lost or self.bridge.handoff:
            return
        try:
            owns = await owns_remote_session(self.bridge.lease)
        except Exception:
            logging.getLogger(__name__).warning(
                "Remote owner fence read failed; skipping drain terminate", exc_info=True
            )
            return
        if not owns:
            return
        await agents.terminate_active_runs(
            error_type=DRAIN_DEADLINE_ERROR_TYPE, error=DRAIN_DEADLINE_ERROR
        )

    async def _retire(self) -> None:
        """Only a drained owner can retire the physical launcher.

        An ownership loss leaves the launcher and its retained terminal
        results available for a fenced successor to adopt. Retiring its row
        here would strand those still-running durable claims.
        """
        if self._lost or self.bridge.handoff:
            return
        try:
            owns = await owns_remote_session(self.bridge.lease)
        except Exception:
            logging.getLogger(__name__).warning(
                "Remote owner fence read failed; skipping launcher retire", exc_info=True
            )
            return
        if not owns:
            return
        await super()._retire()
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
            # Retirement revoked the Cloudflare lease. Session cleanup closes
            # the socket without sending a command under the retired fence.
            return
        if not self.bridge.connected:
            return
        try:
            still_owns = await owns_remote_session(self.bridge.lease)
        except Exception:
            logging.getLogger(__name__).warning(
                "Remote owner fence read failed; skipping retired notice", exc_info=True
            )
            return
        if still_owns:
            with contextlib.suppress(ConnectionError):
                await self.bridge.request("retired", {})


class RemoteSession:
    """A durable worker owner with replaceable socket transport."""

    def __init__(
        self, bridge: Bridge, worker: ApiExecutor, lease: RemoteOwnerLease, *, owned_until: float
    ) -> None:
        self.bridge = bridge
        self.worker = worker
        self.lease = lease
        # Monotonic time before which the database lease is known to be held.
        # Only an expired lease can pass to another owner.
        self.owned_until = owned_until
        self.lease_ref = LeaseRef(lease)
        bridge.lease_ref = self.lease_ref
        self.last_sequence = 0
        self.handoff = False
        self._lease_released = False
        self._release_lock = asyncio.Lock()
        self.cleanup_task: asyncio.Task[None] | None = None
        self.reconciliation_tasks: set[asyncio.Task[None]] = set()
        async def run_worker() -> int:
            with remote_owner_context(self.lease_ref):
                return await worker.run()
        self.task = asyncio.create_task(run_worker(), name=f"remote-worker:{lease.worker_id}")
        self.renewal = asyncio.create_task(self._renew(), name=f"remote-owner:{lease.worker_id}")
        self.task.add_done_callback(_observe_task)

    def begin_physical_drain(self) -> None:
        self.worker.request_stop()

    def begin_handoff(self) -> None:
        self.handoff = True
        self.bridge.handoff = True

    async def release_lease(self) -> None:
        """Release once, after either a drained retirement or a handoff."""
        async with self._release_lock:
            if self._lease_released:
                return
            self.owned_until = 0.0
            self._lease_released = True
            await release_remote_owner(self.lease)

    def reconcile_transport_inventory(self, socket: WebSocket) -> asyncio.Task[None]:
        """Reconcile without blocking control replies needed by that work."""
        generation = self.lease.session_generation

        async def reconcile() -> None:
            try:
                if self.bridge.socket is not socket or self.lease.session_generation != generation:
                    return
                with remote_owner_context(self.lease_ref):
                    await self.worker.reconcile_transport_inventory(
                        expected_generation=generation
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger(__name__).exception("Remote inventory reconciliation failed")
                if self.bridge.socket is socket and self.lease.session_generation == generation:
                    with contextlib.suppress(Exception):
                        await socket.close(code=1011, reason="inventory reconciliation failed")

        task = asyncio.create_task(reconcile(), name=f"remote-reconcile:{self.lease.worker_id}")
        self.reconciliation_tasks.add(task)
        task.add_done_callback(self.reconciliation_tasks.discard)
        return task

    async def _renew(self) -> None:
        try:
            renewal_delay = DEFAULT_OWNER_LEASE_SECONDS / 2
            while True:
                await asyncio.sleep(renewal_delay)
                renewal_delay = DEFAULT_OWNER_LEASE_SECONDS / 2
                # Admission's three-second freshness window pauses new claims
                # during brief API event-loop delays. It must not also skip
                # ownership renewal: repeated short delays could otherwise
                # expire a healthy launcher's twenty-second owner lease.
                if (
                    not self.bridge.connected
                    or (
                        self.bridge.runtime_profile == LINUX_RUNTIME_PROFILE
                        and (self.bridge.current < 0 or self.bridge.limit <= 0)
                    )
                    or self.bridge.freshness_age() >= DEFAULT_OWNER_LEASE_SECONDS
                ):
                    # A WebSocket is only transport. Stop fresh claims as
                    # soon as telemetry is stale, but keep the database lease
                    # until its bounded expiry so a transient reconnect or a
                    # rolling API shutdown can hand the physical children to
                    # a fenced successor without manufacturing failures.
                    if self.bridge.freshness_age() < DEFAULT_OWNER_LEASE_SECONDS:
                        continue
                    await self.lose_ownership(
                        "Remote launcher heartbeat expired"
                        if self.bridge.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
                        else "Remote launcher telemetry expired"
                    )
                    return
                lease = self.lease
                renewing_at = time.monotonic()
                try:
                    renewed = await asyncio.wait_for(
                        renew_remote_owner(lease),
                        timeout=max(0.0, self.owned_until - time.monotonic()),
                    )
                except Exception:
                    if self.lease != lease:
                        continue
                    logging.getLogger(__name__).exception("Remote owner renewal failed")
                    # A failed database call does not revoke this owner until
                    # the current, conservatively tracked lease expires.
                    if time.monotonic() + RENEW_RETRY_SECONDS < self.owned_until:
                        logging.getLogger(__name__).warning(
                            "Remote owner renewal failed; retrying before the lease expires"
                        )
                        renewal_delay = RENEW_RETRY_SECONDS
                        continue
                    await self.lose_ownership("Remote API owner lease could not be renewed")
                    return
                if self.lease != lease:
                    continue
                if not renewed:
                    await self.lose_ownership("Remote API owner lease expired")
                    return
                self.owned_until = renewing_at + DEFAULT_OWNER_LEASE_SECONDS
                if lease.container_release is not None:
                    socket = self.bridge.socket
                    try:
                        async with asyncio.timeout_at(self.owned_until):
                            async with self.bridge.lock:
                                if self.bridge.connected and self.bridge.lease == lease and self.bridge.socket is socket:
                                    ttl_ms = _container_lease_grant(renewed, renewing_at)
                                    if ttl_ms <= 0:
                                        raise TimeoutError("Container runtime grant expired before delivery")
                                    frame = ControlEnvelope(
                                        session_generation=lease.session_generation,
                                        sequence=self.bridge._sequence + 1,
                                        kind="runtime_lease",
                                        payload={"ttl_ms": ttl_ms},
                                    )
                                    await socket.send_text(frame.to_json())
                                    self.bridge._sequence = frame.sequence
                    except (TimeoutError, WebSocketDisconnect, OSError, RuntimeError):
                        if self.lease == lease and self.bridge.socket is socket:
                            self.bridge.disconnect(socket)
                            try:
                                await self.detach()
                            except Exception:
                                logging.getLogger(__name__).exception("Could not detach failed Container transport")
                            with contextlib.suppress(WebSocketDisconnect, OSError, RuntimeError):
                                await socket.close()
                            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger(__name__).exception("Remote owner renewal failed")
            await self.lose_ownership("Remote API owner lease could not be renewed")

    async def may_accept(self, kind: str, payload: dict[str, Any]) -> bool:
        """Defer messages during DB outages; never substitute time for a fence.

        Only liveness or samples may use the known lease without a DB
        confirmation. A retry cannot outlive the lease captured at entry,
        borrow a reconnect's generation, or deliver anything while ownership
        is unknown.
        """
        if kind == "event" and time.monotonic() < self.owned_until:
            event = payload.get("event")
            if event == "sample" or (
                event == "heartbeat" and self.bridge.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
            ):
                return True
        lease, deadline = self.lease, self.owned_until
        while True:
            if self.lease != lease:
                raise ProtocolError("superseded remote control session")
            remaining = min(deadline, self.owned_until) - time.monotonic()
            if remaining <= 0:
                return False
            try:
                owns = await asyncio.wait_for(owns_remote_session(lease), timeout=remaining)
            except (DBAPIError, SQLAlchemyTimeoutError, TimeoutError):
                if self.lease != lease:
                    raise ProtocolError("superseded remote control session")
                if min(deadline, self.owned_until) - time.monotonic() <= RENEW_RETRY_SECONDS:
                    return False
                logging.getLogger(__name__).warning(
                    "Remote owner fence read failed; deferring message until ownership is confirmed",
                    exc_info=True,
                )
                await asyncio.sleep(RENEW_RETRY_SECONDS)
                continue
            if self.lease != lease:
                raise ProtocolError("superseded remote control session")
            return owns and time.monotonic() < min(deadline, self.owned_until)

    async def lose_ownership(self, reason: str) -> None:
        self.owned_until = 0.0
        self.worker._lost = True
        if self.bridge.runtime_profile != LINUX_RUNTIME_PROFILE:
            self.bridge.disconnect()
        if self.bridge.pool is not None:
            await self.bridge.pool.ownership_lost(reason)
        self.worker.request_stop()

    async def stop(self) -> None:
        """Drain a still-connected remote executor before releasing its fence."""
        self.worker.request_stop()
        await asyncio.gather(self.task, return_exceptions=True)
        self.renewal.cancel()
        await asyncio.gather(self.renewal, return_exceptions=True)
        for task in tuple(self.reconciliation_tasks):
            task.cancel()
        await asyncio.gather(*self.reconciliation_tasks, return_exceptions=True)
        await self.release_lease()

    async def detach(self) -> None:
        """Drop an API owner after transport closure without killing children."""
        self.begin_handoff()
        if self.bridge.agents is not None:
            self.bridge.agents.detach()
        try:
            await self.release_lease()
        finally:
            if self.renewal is not asyncio.current_task():
                self.renewal.cancel()
                await asyncio.gather(self.renewal, return_exceptions=True)
            for task in tuple(self.reconciliation_tasks):
                task.cancel()
            await asyncio.gather(*self.reconciliation_tasks, return_exceptions=True)
            self.worker.request_stop()
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)


class RemoteControlHost:
    """Per-API-replica remote control state; no cluster-wide singleton."""

    def __init__(self) -> None:
        self.owner_id = uuid4()
        self.sessions: dict[UUID, RemoteSession] = {}
        self.stopping = False

    async def stop(self) -> None:
        """Begin canonical drains before the lifespan closes control sockets."""
        self.stopping = True
        await asyncio.gather(*(
            session.stop() if session.bridge.connected else session.detach()
            for session in self.sessions.values()
        ), return_exceptions=True)

    async def _welcome(self, bridge: Bridge, requirements_content: str | None, *, acquiring_at: float) -> None:
        """Send Containers startup inputs before any ordinary launcher commands."""
        startup = None
        if bridge.containers:
            payload: dict[str, Any] = {"requirements_content": requirements_content}
            if bridge.lease.container_release is not None:
                remaining_ms = bridge.lease.runtime_lease_ms
                if remaining_ms is None:
                    raise AuthenticationError("Managed Container has no runtime lease")
                ttl_ms = _container_lease_grant(remaining_ms, acquiring_at)
                if ttl_ms <= 0:
                    raise AuthenticationError("Managed Container runtime lease expired before bootstrap")
                payload["ttl_ms"] = ttl_ms
            startup = ControlEnvelope(
                session_generation=bridge.lease.session_generation,
                sequence=bridge._sequence + 1,
                kind="bootstrap",
                payload=payload,
            )
        await bridge.socket.send_text(json.dumps({
            "event": "welcome", "session_generation": bridge.lease.session_generation,
        }))
        if startup is not None:
            for frame in fragment(startup):
                await bridge.socket.send_text(frame.to_json())
                bridge._sequence += 1

    def _start_capacity_canary(self, session: RemoteSession) -> None:
        async def probe() -> None:
            from src.services.worker_ingress_probes import CanaryFailure
            from src.services.worker_pool.cloudflare_canary import (
                cloudflare_sdk_canary_proof,
                container_probe_target,
                record_canary_failure,
                record_container_network_denials,
            )

            generation = session.lease.session_generation
            worker_id = session.lease.worker_id
            if session.bridge.containers:
                await session.bridge.reconciliation_ready.wait()
                if session.lease.session_generation != generation:
                    return
            proof = await cloudflare_sdk_canary_proof(worker_id, generation)
            if proof is None:
                await record_canary_failure(worker_id, generation, CanaryFailure("proof", "unavailable"))
                return
            command = {"proof": proof}
            try:
                if session.bridge.containers:
                    command["alternate"], command["address"] = await container_probe_target(worker_id)
                reply = await session.bridge.request("capacity_canary", command, expected_generation=generation)
            except TimeoutError:
                failure = CanaryFailure("ack", "timeout")
            except (ConnectionError, CredentialRouteChanged):
                failure = CanaryFailure("launcher", "connection_lost")
            except (OSError, RuntimeError, ValueError):
                failure = CanaryFailure("ack", "invalid_reply")
            else:
                if session.bridge.containers:
                    denied = reply.get("denied") if isinstance(reply, dict) and set(reply) == {"denied"} else None
                    if (
                        isinstance(denied, list) and all(isinstance(item, str) for item in denied)
                        and set(denied) <= CONTAINER_EGRESS_DENIALS
                        and await record_container_network_denials(worker_id, generation, denied)
                        and await cloudflare_sdk_canary_proof(worker_id, generation, acknowledged=True) is not None
                    ):
                        return
                    failure = CanaryFailure("ack", "invalid_reply")
                elif reply == {}:
                    if await cloudflare_sdk_canary_proof(worker_id, generation, acknowledged=True) is not None:
                        return
                    failure = CanaryFailure("ack", "missing_sdk_receipt")
                else:
                    detail = reply.get("failure") if isinstance(reply, dict) and set(reply) == {"failure"} else None
                    failure = CanaryFailure.from_result(detail)
            await record_canary_failure(worker_id, generation, failure)

        task = asyncio.create_task(probe(), name=f"capacity-canary:{session.lease.worker_id}")
        session.reconciliation_tasks.add(task)
        task.add_done_callback(session.reconciliation_tasks.discard)
        task.add_done_callback(_observe_task)

    async def connect(self, socket: WebSocket) -> tuple[RemoteSession, FragmentAssembler]:
        await socket.accept()
        try:
            raw = await asyncio.wait_for(socket.receive_text(), timeout=30)
            initial = json.loads(raw)
            if not isinstance(initial, dict) or initial.get("event") != "hello":
                raise AuthenticationError("expected signed hello")
            signed = SignedHello.from_dict(initial.get("hello", {}))
            admit_ingress = socket.scope.get("worker_ingress_authenticate")
            if signed.container_release is not None and admit_ingress is None:
                raise AuthenticationError("Managed Container control requires dedicated worker ingress")
            settings = get_settings()
            cloudflare = signed.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
            containers = False
            capacity_canary = False
            managed_cloudflare = False
            enrollment = None
            if cloudflare:
                if signed.form != "external" or signed.resource_uid is None:
                    raise AuthenticationError("Cloudflare workers require an external capacity resource")
                async with get_liveness_db_context() as db:
                    binding = (await db.execute(select(ExecutionWorker.capacity_scope).where(
                        ExecutionWorker.id == signed.incarnation_id,
                        ExecutionWorker.form == "external",
                        ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
                        ExecutionWorker.capacity_resource_uid == signed.resource_uid,
                    ))).one_or_none()
                if binding is None:
                    if not settings.cloudflare_worker_hello_enabled:
                        raise ProtocolError("Cloudflare worker hello is disabled")
                    raise AuthenticationError("Cloudflare worker lacks its reserved resource binding")
                if binding.capacity_scope is None:
                    if not settings.cloudflare_worker_hello_enabled:
                        raise ProtocolError("Cloudflare worker hello is disabled")
                    token = settings.worker_capacity_token
                    if not token:
                        raise AuthenticationError("Manual Cloudflare worker control is not configured")
                    control_key = external_worker_control_key(
                        token.get_secret_value(), signed.incarnation_id, signed.resource_uid,
                    )
                else:
                    async with get_liveness_db_context() as db:
                        pin = await db.scalar(
                            select(CloudflareWorkerReservation)
                            .join(CloudflareInstallation, CloudflareInstallation.id == CloudflareWorkerReservation.installation_id)
                            .where(
                                CloudflareWorkerReservation.worker_id == signed.incarnation_id,
                                CloudflareWorkerReservation.scope == binding.capacity_scope,
                                CloudflareInstallation.retired_at.is_(None),
                            )
                        )
                        if pin is None:
                            raise AuthenticationError("Cloudflare worker lacks its current installation pin")
                        managed_cloudflare = True
                        capacity_canary = pin.canary_job_id is not None
                        try:
                            control_key = await load_managed_control_key(db, pin, signed.resource_uid)
                        except ValueError:
                            raise AuthenticationError("Invalid managed Cloudflare resource binding") from None
            elif signed.container_release is not None:
                assert signed.resource_uid is not None
                async with get_liveness_db_context() as db:
                    pin = await db.scalar(select(CloudflareWorkerReservation).join(
                        ExecutionWorker, ExecutionWorker.id == CloudflareWorkerReservation.worker_id,
                    ).join(
                        CloudflareInstallation, CloudflareInstallation.id == CloudflareWorkerReservation.installation_id,
                    ).where(
                        ExecutionWorker.id == signed.incarnation_id,
                        ExecutionWorker.form == "external",
                        ExecutionWorker.runtime_profile == LINUX_RUNTIME_PROFILE,
                        ExecutionWorker.execution_lane == "normal",
                        ExecutionWorker.capacity_scope == CONTAINERS_SCOPE,
                        ExecutionWorker.capacity_resource_uid == signed.resource_uid,
                        ExecutionWorker.state.in_(("starting", "ready", "draining")),
                        CloudflareWorkerReservation.scope == CONTAINERS_SCOPE,
                        CloudflareWorkerReservation.container_release == signed.container_release,
                        CloudflareWorkerReservation.container_reserved_at.is_not(None),
                        CloudflareWorkerReservation.container_stopped_at.is_(None),
                        CloudflareInstallation.retired_at.is_(None),
                    ))
                    if pin is None:
                        raise AuthenticationError("Managed Container lacks its live incarnation and release binding")
                    control_key = await load_managed_control_key(db, pin, signed.resource_uid)
                    capacity_canary = pin.canary_job_id is not None
                containers = True
            else:
                control_key = settings.secret_key
            hello = verify_hello(control_key, signed)
            if admit_ingress is not None and not admit_ingress(hello.incarnation_id):
                raise AuthenticationError("Worker ingress incarnation quota exceeded")
            if "runtime_profile" in initial:
                raise ProtocolError("physical runtime profile must be inside signed hello")
            if not cloudflare and hello.runtime_profile not in (None, LINUX_RUNTIME_PROFILE):
                raise ProtocolError("unsupported physical runtime profile")
            if hello.form == "external" and hello.execution_lane != "normal":
                raise AuthenticationError("External capacity workers only support the Normal lane")
            bootstrap: dict[str, Any] = {}
            if not cloudflare:
                sample = initial.get("sample")
                if not isinstance(sample, dict):
                    raise ProtocolError("hello sample is required")
                current, limit = int(sample["current"]), int(sample["limit"])
                pressure_ratio = sample.get("pressure_ratio")
                if pressure_ratio is None:
                    pressure_ratio = current / limit if limit > 0 else 1.0
                bootstrap.update(
                    current=current, limit=limit, pressure_ratio=float(pressure_ratio),
                    requirements=initial.get("requirements", {}),
                )
            bootstrap.update(
                hostname=hello.hostname,
                handlers=hello_execution_handlers(initial),
                execution_lane=hello.execution_lane,
                runtime_profile=hello.runtime_profile or LINUX_RUNTIME_PROFILE,
                containers=containers,
                managed_cloudflare=managed_cloudflare,
            )
            if hello.execution_lane == "fast_path" and ("workflow.execute", 2) not in bootstrap["handlers"]:
                raise AuthenticationError("Fast Path workers must support workflow.execute version 2")
            credential_protocol = hello_credential_protocol(initial)
            if hello.container_release is not None and (
                credential_protocol != CLOUDFLARE_CREDENTIAL_PROTOCOL
                or initial.get("handlers") != [list(CLOUDFLARE_WORKFLOW_HANDLER)]
            ):
                raise ProtocolError("Managed Containers require protocol 2 and the Normal workflow handler")
            if containers:
                base = [list(CLOUDFLARE_WORKFLOW_HANDLER)]
                staged = [*base, [SOLUTION_GIT_CAPABILITY, 1]]
                if initial.get("handlers") == staged:
                    bootstrap["handlers"] = (*bootstrap["handlers"], (SOLUTION_GIT_CAPABILITY, 1))
                elif initial.get("handlers") != base:
                    raise ProtocolError("Invalid Containers capability advertisement")
            if cloudflare:
                # Negotiation filters unsupported handlers. Check the full advertisement.
                handlers: list[list[str | int]] = [list(CLOUDFLARE_WORKFLOW_HANDLER)]
                advertised = initial.get("handlers")
                from src.services.execution.cloudflare_summary import summary_endpoint

                if isinstance(advertised, list) and list(CLOUDFLARE_SUMMARY_HANDLER) in advertised:
                    if initial.get("summary_claim_protocol") != SUMMARY_CLAIM_PROTOCOL:
                        raise ProtocolError("Cloudflare summaries require claim protocol 1")
                    if summary_endpoint(settings.cloudflare_summary_endpoint) is None:
                        raise ProtocolError("Cloudflare agent summaries are disabled")
                    handlers.append(list(CLOUDFLARE_SUMMARY_HANDLER))
                if isinstance(advertised, list) and list(CLOUDFLARE_AGENT_HANDLER) in advertised:
                    if initial.get("agent_claim_protocol") != CLOUDFLARE_AGENT_PROTOCOL:
                        raise ProtocolError("Cloudflare agents require the autonomous claim protocol")
                    if (
                        summary_endpoint(settings.cloudflare_agent_endpoint) is None
                        or not settings.cloudflare_agent_fixture_ids
                    ):
                        raise ProtocolError("Cloudflare autonomous agents are disabled")
                    handlers.append(list(CLOUDFLARE_AGENT_HANDLER))
                if advertised != handlers:
                    raise ProtocolError("Unsupported Cloudflare handler advertisement")
                if credential_protocol != CLOUDFLARE_CREDENTIAL_PROTOCOL:
                    raise ProtocolError("Cloudflare workers require credential protocol 2")
                version = initial.get("version")
                if not isinstance(version, str) or not version:
                    raise AuthenticationError("Cloudflare workers require a prepared build version")
                assert hello.resource_uid is not None
                enrollment = CloudflareEnrollment(hello.resource_uid, version, hello.hostname)
        except (AuthenticationError, ProtocolError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            await socket.close(code=1008)
            raise

        requirements_content = None
        if containers and hello.container_release is None:
            from src.core.requirements_cache import get_requirements

            try:
                cached = await get_requirements()
                requirements_content = cached["content"] if cached else None
            except Exception:
                await socket.close(code=1011, reason="launcher requirements unavailable")
                raise

        # Authentication precedes canonical admission. Reconnects naturally
        # get None here because their pod was already admitted.
        if hello.form == "job":
            assert hello.job_uid is not None and hello.pod_uid is not None
            # The launcher's own build, not this API's: an API admitting a pod
            # launched from an older image must leave it outdated.
            version = initial.get("version")
            admitted = await admit_worker(
                hello.incarnation_id, job_uid=hello.job_uid, pod_uid=hello.pod_uid,
                version=version if isinstance(version, str) else None,
                execution_lane=hello.execution_lane,
            )
            if admitted is None:
                async with get_liveness_db_context() as db:
                    bound_pod = await db.scalar(
                        select(ExecutionWorker.id).where(
                            ExecutionWorker.id == hello.incarnation_id,
                            ExecutionWorker.form == "job",
                            ExecutionWorker.state.in_(("ready", "draining")),
                            ExecutionWorker.kubernetes_job_uid == hello.job_uid,
                            ExecutionWorker.kubernetes_pod_uid == hello.pod_uid,
                            ExecutionWorker.execution_lane == hello.execution_lane,
                        )
                    )
                if bound_pod is None:
                    raise AuthenticationError("launcher pod is not the admitted Job pod")
        elif hello.form == "external" and enrollment is None:
            assert hello.resource_uid is not None
            version = initial.get("version")
            admitted = await admit_external_worker(
                hello.incarnation_id, resource_uid=hello.resource_uid,
                launcher_id=hello.launcher_id, version=version if isinstance(version, str) else None,
                hostname=hello.hostname,
                **({"container_release": hello.container_release} if hello.container_release is not None else {}),
            )
            if admitted is None:
                async with get_liveness_db_context() as db:
                    bound_resource = await db.scalar(
                        select(ExecutionWorker.id).where(
                            ExecutionWorker.id == hello.incarnation_id,
                            ExecutionWorker.form == "external",
                            ExecutionWorker.runtime_profile != CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
                            ExecutionWorker.state.in_(("ready", "draining")),
                            ExecutionWorker.capacity_resource_uid == hello.resource_uid,
                            container_identity_matches(hello.incarnation_id, hello.container_release),
                            ExecutionWorker.remote_launcher_id == hello.launcher_id,
                            ExecutionWorker.version.is_not(None),
                            ExecutionWorker.version == version,
                        )
                    )
                if bound_resource is None:
                    raise AuthenticationError("launcher is not bound to the prepared capacity resource")
        elif hello.form == "static":
            async with get_liveness_db_context() as db:
                existing_worker = (await db.execute(
                    select(ExecutionWorker.form, ExecutionWorker.execution_lane).where(
                        ExecutionWorker.id == hello.incarnation_id,
                    )
                )).one_or_none()
            if existing_worker is not None and existing_worker.form != "static":
                raise AuthenticationError("static launcher cannot claim a managed incarnation")
            if existing_worker is not None and existing_worker.execution_lane != hello.execution_lane:
                raise AuthenticationError("static launcher execution lane changed")
            if existing_worker is None:
                version = initial.get("version")
                await register_static_worker(
                    hostname=hello.hostname,
                    version=version if isinstance(version, str) else None,
                    incarnation_id=hello.incarnation_id,
                    launcher_id=hello.launcher_id,
                    supported_handlers=[handler for handler, _version in bootstrap["handlers"]],
                    execution_lane=hello.execution_lane,
                )
        acquiring_at = time.monotonic()
        lease = await acquire_remote_owner(
            worker_id=hello.incarnation_id,
            owner_id=self.owner_id,
            launcher_id=hello.launcher_id,
            nonce_digest=nonce_digest(hello.nonce),
            hello_sequence=hello.hello_sequence,
            credential_protocol=credential_protocol,
            runtime_profile=bootstrap["runtime_profile"],
            cloudflare_enrollment=enrollment,
            **({"container_release": hello.container_release} if hello.container_release is not None else {}),
        )
        if lease is None:
            if enrollment is not None:
                # Only a verified, matching enrollment can learn that it ended.
                async with get_liveness_db_context() as db:
                    ended = await db.scalar(select(ExecutionWorker.id).where(
                        ExecutionWorker.id == hello.incarnation_id,
                        ExecutionWorker.form == "external",
                        ExecutionWorker.execution_lane == "normal",
                        ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
                        ExecutionWorker.capacity_resource_uid == enrollment.resource_uid,
                        ExecutionWorker.version == enrollment.version,
                        ExecutionWorker.remote_launcher_id == hello.launcher_id,
                        ExecutionWorker.state.in_(("retired", "lost")),
                    ))
                if ended is not None:
                    await socket.close(code=CLOUDFLARE_INCARNATION_ENDED, reason="incarnation ended")
                    raise AuthenticationError("launcher incarnation ended")
            elif hello.form == "static":
                async with get_liveness_db_context() as db:
                    lost = await db.scalar(select(ExecutionWorker.id).where(
                        ExecutionWorker.id == hello.incarnation_id,
                        ExecutionWorker.form == "static",
                        ExecutionWorker.execution_lane == hello.execution_lane,
                        ExecutionWorker.remote_launcher_id == hello.launcher_id,
                        ExecutionWorker.state == "lost",
                    ))
                if lost is not None:
                    await socket.close(code=STATIC_INCARNATION_LOST, reason="static incarnation lost")
                    raise AuthenticationError("static launcher incarnation lost")
            await socket.close(code=1008)
            raise AuthenticationError("launcher ownership was refused")
        existing = self.sessions.get(lease.worker_id)
        if existing is not None and not existing.bridge.handoff:
            existing.lease = lease
            existing.lease_ref.lease = lease
            existing.owned_until = acquiring_at + DEFAULT_OWNER_LEASE_SECONDS
            existing.last_sequence = 0
            if existing.bridge.containers:
                async with existing.bridge.lock:
                    existing.bridge.replace_socket(socket, lease)
                    await self._welcome(existing.bridge, requirements_content, acquiring_at=acquiring_at)
            else:
                existing.bridge.replace_socket(socket, lease)
                await self._welcome(existing.bridge, requirements_content, acquiring_at=acquiring_at)
            if capacity_canary:
                self._start_capacity_canary(existing)
            return existing, FragmentAssembler()
        bridge = Bridge(socket, bootstrap, lease)
        worker = ApiExecutor(bridge, lease.worker_id, remote_static=hello.form == "static")
        await self._welcome(bridge, requirements_content, acquiring_at=acquiring_at)
        session = RemoteSession(
            bridge, worker, lease, owned_until=acquiring_at + DEFAULT_OWNER_LEASE_SECONDS
        )
        self.sessions[lease.worker_id] = session
        session.task.add_done_callback(
            lambda task: self._session_finished(lease.worker_id, session, task)
        )
        if capacity_canary:
            self._start_capacity_canary(session)
        return session, FragmentAssembler()

    def _session_finished(
        self, worker_id: UUID, session: RemoteSession, task: asyncio.Task[Any]
    ) -> None:
        if session.handoff:
            if self.sessions.get(worker_id) is session:
                self.sessions.pop(worker_id)
            return

        async def cleanup() -> None:
            if self.sessions.get(worker_id) is not session:
                return
            self.sessions.pop(worker_id, None)
            session.renewal.cancel()
            await asyncio.gather(session.renewal, return_exceptions=True)
            for reconciliation in tuple(session.reconciliation_tasks):
                reconciliation.cancel()
            await asyncio.gather(*session.reconciliation_tasks, return_exceptions=True)
            await session.release_lease()
            if session.bridge.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE or session.lease.container_release is not None:
                socket = session.bridge.socket
                session.bridge.disconnect(socket)
                if socket.application_state is not WebSocketState.DISCONNECTED:
                    with contextlib.suppress(WebSocketDisconnect, OSError, RuntimeError):
                        await socket.close()

        session.cleanup_task = asyncio.create_task(cleanup())


_host: RemoteControlHost | None = None


async def _record_guest_socket_evidence(
    lease: RemoteOwnerLease, *, event: dict[str, Any] | None = None, sequence: int = 0,
    close_code: int | None = None, close_reason: str | None = None,
) -> None:
    """Store diagnostic evidence without borrowing a replacement socket's lease."""
    if lease.container_release is None:
        if event is not None:
            raise AuthenticationError("Guest diagnostic requires a managed Container session")
        return
    try:
        async with asyncio.timeout(0.5):
            fatal_authorized = False
            while True:
                try:
                    async with get_db_context() as db:
                        if not fatal_authorized and event is not None and event.get("event") == "guest_fatal":
                            # Check arrival authority without waiting on a
                            # renewal or a release. The write still fences
                            # the exact owner and generation after locking.
                            fatal_authorized = await db.scalar(select(ExecutionWorker.id).where(
                                ExecutionWorker.id == lease.worker_id,
                                ExecutionWorker.remote_owner_id == lease.owner_id,
                                ExecutionWorker.remote_launcher_id == lease.launcher_id,
                                ExecutionWorker.remote_session_generation == lease.session_generation,
                                ExecutionWorker.remote_owner_lease_expires_at > func.now(),
                                remote_enrollment_is_live(lease.container_release),
                            )) is not None
                            if not fatal_authorized:
                                raise AuthenticationError("Guest diagnostic session was replaced")
                        # Release the worker share lock between attempts so
                        # a busy reservation cannot hold up owner renewal.
                        await db.execute(text("SET LOCAL lock_timeout = '50ms'"))
                        await _write_guest_socket_evidence(
                            db, lease, event, sequence, close_code, close_reason, fatal_authorized,
                        )
                    return
                except DBAPIError as exc:
                    if getattr(exc.orig, "sqlstate", None) != "55P03":
                        raise
    except (DBAPIError, SQLAlchemyTimeoutError, TimeoutError):
        # Diagnostic writes are best effort. The ordinary session and
        # qualification fences continue to enforce readiness independently.
        return


async def _write_guest_socket_evidence(
    db: AsyncSession, lease: RemoteOwnerLease, event: dict[str, Any] | None, sequence: int,
    close_code: int | None, close_reason: str | None, fatal_authorized: bool,
) -> None:
    query = select(ExecutionWorker).where(ExecutionWorker.id == lease.worker_id)
    if event is not None:
        query = query.where(remote_enrollment_is_live(lease.container_release))
        if not fatal_authorized:
            query = query.where(ExecutionWorker.remote_owner_lease_expires_at > func.now())
    worker = await db.scalar(query.with_for_update(read=True, of=ExecutionWorker))
    if (
        worker is None or worker.remote_launcher_id != lease.launcher_id
        or worker.remote_session_generation != lease.session_generation
        or (worker.remote_owner_id != lease.owner_id and (close_code is None or worker.remote_owner_id is not None))
        or worker.capacity_resource_uid is None
    ):
        if event is not None:
            raise AuthenticationError("Guest diagnostic session was replaced")
        return
    pin = await db.get(CloudflareWorkerReservation, lease.worker_id, with_for_update=True)
    if pin is None or pin.container_release != lease.container_release or pin.container_envelope is None:
        if event is not None:
            raise AuthenticationError("Guest diagnostic lacks a managed reservation")
        return
    previous = pin.container_envelope.get("guest_diagnostic") or {}
    evidence = dict(previous)
    if close_code is not None:
        evidence.update(close_evidence(close_code, close_reason), close_generation=lease.session_generation)
    elif event is None:
        evidence = {"stage": "hello", "generation": lease.session_generation}
    else:
        key = await load_managed_control_key(db, pin, worker.capacity_resource_uid)
        diagnostic = verify_guest_diagnostic(key, lease.session_generation, sequence, event)
        evidence.update(stage=diagnostic["stage"], generation=lease.session_generation)
        if "startup_facts" in diagnostic:
            evidence["startup_facts"] = diagnostic["startup_facts"]
        if diagnostic["event"] == "guest_fatal":
            evidence.update(
                fatal_stage=diagnostic["stage"], fatal_generation=lease.session_generation,
                exception_class=diagnostic["exception_class"], reason=diagnostic["reason"],
            )
            if "child_returncode" in diagnostic:
                evidence["child_returncode"] = diagnostic["child_returncode"]
    if evidence != previous:
        pin.container_envelope = {**pin.container_envelope, "guest_diagnostic": evidence}


async def start_remote_control_host(_application: Any) -> None:
    """Lifespan hook; the caller mounts :data:`router` before ``/`` first."""
    global _host
    if _host is None:
        _host = RemoteControlHost()


async def stop_remote_control_host() -> None:
    global _host
    if _host is not None:
        await _host.stop()
        _host = None


@router.websocket("/api/worker-control")
async def remote_control(socket: WebSocket) -> None:
    if _host is None or _host.stopping:
        await socket.close(code=1013)
        return
    session: RemoteSession | None = None
    close_code: int | None = None
    close_reason: str | None = None
    try:
        session, assembler = await _host.connect(socket)
        await _record_guest_socket_evidence(session.lease)
        while True:
            frame = ControlEnvelope.from_json(await socket.receive_text())
            if session.bridge.socket is not socket:
                raise ProtocolError("superseded remote control socket")
            if frame.session_generation != session.lease.session_generation:
                raise ProtocolError("stale remote control session")
            if frame.sequence <= session.last_sequence:
                raise ProtocolError("stale remote control message")
            session.last_sequence = frame.sequence
            logical = assembler.feed(frame)
            if logical is None:
                continue
            if logical.kind == "event" and logical.payload.get("event") in GUEST_DIAGNOSTIC_EVENTS:
                # Diagnostic persistence checks the exact generation, expiry,
                # and live enrollment on the regular pool. No heartbeat-pool
                # read is needed for events that have no other consumers.
                await _record_guest_socket_evidence(session.lease, event=logical.payload, sequence=logical.sequence)
                continue
            owns = await session.may_accept(logical.kind, logical.payload)
            if (
                session.bridge.socket is not socket
                or frame.session_generation != session.lease.session_generation
            ):
                raise ProtocolError("superseded remote control socket")
            if not owns:
                await session.lose_ownership("Remote API owner fence was replaced")
                raise ProtocolError("remote API owner fence was replaced")
            if logical.kind != "event":
                raise ProtocolError("launcher may only send events")
            with remote_owner_context(LeaseRef(session.lease)):
                await session.bridge.receive(logical.payload)
            if logical.payload.get("event") == "inventory":
                session.reconcile_transport_inventory(socket)
            elif logical.payload.get("event") == "draining":
                session.begin_physical_drain()
    except WebSocketDisconnect as exc:
        # A transport disconnect does not kill children or release claims.
        close_code = exc.code
        close_reason = exc.reason
    except (AuthenticationError, ProtocolError):
        close_code = 1008
        if socket.application_state is not WebSocketState.DISCONNECTED:
            await socket.close(code=1008)
    except Exception:
        close_code = 1011
        raise
    finally:
        if session is not None:
            lease = session.lease
            current = session.bridge.socket is socket
            session.bridge.disconnect(socket)
            if current:
                await _record_guest_socket_evidence(lease, close_code=close_code or 1001, close_reason=close_reason)
