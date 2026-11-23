"""External resource adapter for canonical physical-worker capacity."""

import asyncio
import logging
import time
from dataclasses import replace
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
from shared.models import WorkerCapacityProfile, WorkerCapacityReservation
from sqlalchemy import and_, func, or_, select, update

from src.config import Settings
from src.core.database import get_db_context
from src.models.orm.cloudflare_capacity import CloudflareWorkerReservation, CloudflareWorkflowSlot
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.services.kubernetes_execution import KubernetesPolicy
from src.services.work_dispatch import (
    DISPATCH_WAKE_FAMILIES,
    WAKE_PLATFORM_JOB,
    WAKE_SERVICE,
    listen_for_pool_wake,
    wait_for_next_pass,
)
from src.services.worker_pool.cloudflare_canary import active_cloudflare_canary
from src.services.worker_pool.cloudflare_credentials import load_managed_control_key
from src.services.worker_pool.cloudflare_slots import delete_workflow_slots
from src.services.worker_pool.cloudflare_spend import (
    MANAGED_CLEANUP_WINDOW,
    compact_ledger_receipt,
    pending_ledger_receipt,
    record_capacity_cleanup_do_requests,
    seal_launcher_meter,
)
from src.services.worker_pool.cloudflare_timing import record_transport_timing
from src.services.worker_pool.controller import (
    RECONCILE_SECONDS,
    SWEEP_SECONDS,
    LiveControllers,
    WorkerCapacityController,
    _now,
    _withdraw_live,
)
from src.services.worker_pool.eligibility import cloudflare_active_claims
from src.services.worker_pool.external_client import (
    CAPACITY_REQUEST_SECONDS,
    CapacityProtocolError,
    ExternalCapacityClient,
)
from src.services.worker_pool.external_policy import validate_external_policy
from src.services.worker_pool.liveness import silent_executor
from src.services.worker_pool.recovery import (
    announce_lost_runs,
    end_runs_of_lost_worker,
)
from src.services.worker_pool.sizing import JobSizing
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    LINUX_RUNTIME_PROFILE,
    WORKER_DRAIN_GRACE_SECONDS,
    WORKERS_DEADLINE_DRAIN_SECONDS,
)

logger = logging.getLogger(__name__)

# Allow bounded in-flight PUTs and provider work to finish before accepting
# absence for a terminal incarnation whose UID was never bound.
UNBOUND_PREPARE_GRACE = timedelta(minutes=1)

class ExternalWorkerPoolController(WorkerCapacityController):
    def __init__(self, client: ExternalCapacityClient, settings: Settings, profile: WorkerCapacityProfile) -> None:
        super().__init__(settings, worker_form="external")
        self.client = client
        self.profile = profile
        self.version = profile.worker_version
        self.live = LiveControllers(ids=frozenset({self.id}), versions=frozenset({self.version}))
        self.cleanup_states = ("lost", "retired")
        self.runtime_profile = profile.runtime_profile
        self.burst_launch_allowed = True

    async def _reserve_managed_cleanup(self, worker_id: UUID) -> bool:
        async with get_db_context() as db:
            pin = await db.get(CloudflareWorkerReservation, worker_id, with_for_update=True)
            if pin is None:
                return False
            assert pin.cleanup_started_at is not None
            now = await db.scalar(select(func.clock_timestamp()))
            assert now is not None
            if pin.cleanup_retry_at is not None and now < pin.cleanup_retry_at:
                return False
            if now >= pin.cleanup_started_at + MANAGED_CLEANUP_WINDOW:
                return False
            pin.cleanup_retry_at = now + timedelta(seconds=min(2 ** min(pin.cleanup_attempts + 1, 6), 60))
            pin.cleanup_attempts += 1
            await record_capacity_cleanup_do_requests(db, pin.installation_id)
        return True

    async def launch_for_demand(self, policy: KubernetesPolicy) -> list[UUID]:
        if not self.burst_launch_allowed:
            return []
        if self.managed_cloudflare_scope is not None:
            return await super().launch_for_demand(replace(
                policy, max_workers=self.profile.max_workers, warm_workers=0,
                reserved_fast_path_workers=0,
            ))
        return await super().launch_for_demand(policy)

    @property
    def profile(self) -> WorkerCapacityProfile:
        assert self.capacity_profile is not None
        return self.capacity_profile

    @profile.setter
    def profile(self, profile: WorkerCapacityProfile) -> None:
        self.capacity_profile = profile

    def _memory_environment(self, generation: int) -> dict[str, Any]:
        return {**super()._memory_environment(generation), "scope": self.profile.scope}

    def _managed_scope(self) -> Any:
        scope = and_(ExecutionWorker.form == "external", ExecutionWorker.capacity_scope == self.profile.scope)
        if self.managed_cloudflare_scope is not None:
            return scope & self.managed_cloudflare_scope.owns(ExecutionWorker.id) & ~active_cloudflare_canary(ExecutionWorker.id)
        return scope & ~select(CloudflareWorkerReservation.worker_id).where(
            CloudflareWorkerReservation.worker_id == ExecutionWorker.id,
        ).exists()

    def _cleanup_scope(self) -> Any:
        # An outage must not turn an unconfirmed physical deletion into an orphan.
        pending = ExecutionWorker.capacity_resource_deleted_at.is_(None)
        if self.managed_cloudflare_scope is not None:
            pending = pending | pending_ledger_receipt(ExecutionWorker.id).exists()
        return and_(
            ExecutionWorker.state.in_(self.cleanup_states),
            pending,
        )

    def _binding_fields(self, worker_id: UUID) -> dict[str, Any]:
        fields = {"capacity_scope": self.profile.scope, "version": self.profile.worker_version}
        if self.profile.protocol_version == 2:
            fields["capacity_resource_uid"] = uuid4().hex
        return fields

    def _sizing(self, policy: KubernetesPolicy, workers: Any) -> JobSizing:
        if self.managed_cloudflare_scope is not None:
            return super()._sizing(policy, workers)
        try:
            validate_external_policy(
                policy, self.profile, max_lifetime_seconds=self.settings.pool_max_lifetime_seconds,
                incomplete_topology=self.containers_scope is not None,
            )
        except ValueError as exc:
            raise CapacityProtocolError(f"Worker policy differs from provider capabilities: {exc}") from exc
        return super()._sizing(policy, workers)

    async def reconcile(self) -> None:
        current = await self.client.profile()
        if self.managed_cloudflare_scope is not None and current != self.profile:
            raise CapacityProtocolError("Managed Cloudflare deployment capabilities changed")
        if current.protocol_version != self.profile.protocol_version:
            raise CapacityProtocolError("Capacity protocol changed; drain the original provider first")
        if (current.scope, current.runtime_profile) != (self.profile.scope, self.runtime_profile):
            raise CapacityProtocolError("Capacity provider scope or runtime profile changed; drain the original provider first")
        self.profile = current
        self.version = current.worker_version
        if (self.managed_cloudflare_scope is None and self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
                and not self.settings.cloudflare_worker_hello_enabled):
            raise CapacityProtocolError("Cloudflare capacity requires Cloudflare hello opt-in")
        await super().reconcile()

    async def _start_prepared(self, worker_id: UUID, uid: str) -> None:
        scope = self.managed_cloudflare_scope
        if scope is None:
            await self.client.start(worker_id, self.profile.scope, uid)
            return
        async with get_db_context() as db:
            pin = CloudflareWorkerReservation
            if not await scope.lock_admission(db) or not await db.scalar(select(pin.worker_id).where(
                pin.worker_id == worker_id,
                pin.workers_deadline_at > func.clock_timestamp(),
                pin.workers_lifetime_seconds.is_not(None),
            )):
                return
            start_sent_at = _now()
            await self.client.start(worker_id, scope.name, uid)
            start_accepted_at = _now()
        await record_transport_timing(
            worker_id=worker_id, launcher_start_sent_at=start_sent_at, launcher_start_accepted_at=start_accepted_at,
        )

    async def _retire_closed_worker(self, worker_id: UUID) -> None:
        scope = self.managed_cloudflare_scope
        if scope is None:
            return
        async with get_db_context() as db:
            root = await scope.lock_admission(db, for_retirement=True)
            pin = CloudflareWorkerReservation
            expired = await db.scalar(select(pin.worker_id).where(
                pin.worker_id == worker_id,
                or_(pin.workers_deadline_at.is_(None), pin.workers_deadline_at <= func.clock_timestamp(),
                    pin.workers_lifetime_seconds.is_(None)),
            ))
            if root is not None and expired is None:
                return
            worker = await db.scalar(select(ExecutionWorker).where(
                ExecutionWorker.id == worker_id, self._managed_scope(),
                ExecutionWorker.state.in_(("reserved", "starting", "ready", "draining")),
            ).with_for_update())
            if worker is None:
                return
            active = await db.scalar(
                cloudflare_active_claims().where(
                    ExecutionWorkerClaim.worker_id == worker_id,
                ).exists().select()
            )
            if worker.end_reason is None:
                worker.end_reason = "max_lifetime" if expired is not None else "cloudflare_admission_closed"
            elif active and expired is None:
                worker.end_reason = "cloudflare_admission_closed"
            if worker.active_runs or worker.memory_reserved_bytes or active:
                worker.state = "draining"
            else:
                worker.state = "retired"
                worker.ended_at = _now()

    async def reconcile_worker(self, worker_id: UUID, policy: KubernetesPolicy) -> None:
        self._sizing(policy, [])
        # Provider I/O never holds the worker row lock. Managed sends take the
        # installation's admission lock for their bounded request.
        await self._retire_closed_worker(worker_id)
        async with get_db_context() as db:
            worker = await db.scalar(select(ExecutionWorker).where(
                ExecutionWorker.id == worker_id, self._managed_scope(),
            ))
            if worker is None:
                return
            state, uid, limit = worker.state, worker.capacity_resource_uid, worker.memory_limit_bytes
            version = worker.version
            runtime_profile = worker.runtime_profile
            created_at = worker.created_at
            deleted_at = worker.capacity_resource_deleted_at
            ended_at = worker.ended_at
            if self.managed_cloudflare_scope is not None and state in self.cleanup_states and deleted_at is None:
                # Commit the clock before GET so outages also consume the cleanup window.
                pin = CloudflareWorkerReservation
                await db.execute(update(pin).where(
                    pin.worker_id == worker_id, pin.cleanup_started_at.is_(None),
                ).values(cleanup_started_at=func.clock_timestamp()))
        bounded_prepare = runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE or self.containers_mode
        if bounded_prepare and state in ("reserved", "starting"):
            if _now() - created_at >= timedelta(seconds=self.settings.pool_start_timeout_seconds):
                reason = "prepare_timeout" if state == "reserved" else "start_timeout"
                await self._record_loss(worker_id, uid, reason, False)
                return
        if state == "reserved" and (
            version != self.profile.worker_version or runtime_profile != self.runtime_profile
            or (
                runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
                and limit != self.profile.memory_limit_bytes
            )
        ):
            # A provider rollout cannot relabel an already reserved incarnation.
            # Fence it before cleanup; queued demand may reserve a fresh UUID.
            await self._record_loss(worker_id, uid, "provider_version_changed", False)
            return
        if state in self.cleanup_states and deleted_at is not None:
            if self.managed_cloudflare_scope is not None and uid is not None:
                await compact_ledger_receipt(
                    worker_id, lambda: self.client.acknowledge_receipt(worker_id, self.profile.scope, uid),
                )
            return
        resource = await self.client.get(worker_id, self.profile.scope)
        if uid is not None and resource is not None and resource.resource_uid != uid:
            raise CapacityProtocolError("Provider resource UID changed for an existing incarnation")
        if state in self.cleanup_states:
            if resource is None and self.managed_cloudflare_scope is not None:
                async with get_db_context() as db:
                    may_be_running = await db.scalar(select(select(CloudflareWorkflowSlot.fence).where(
                        CloudflareWorkflowSlot.worker_id == worker_id,
                        CloudflareWorkflowSlot.sent, CloudflareWorkflowSlot.released_at.is_(None),
                    ).exists()))
                if may_be_running:
                    return  # A missing ledger is not a stop receipt.
            if resource is None and (uid is None or self.profile.protocol_version == 2) and (
                not bounded_prepare
                or ended_at is None or _now() - ended_at < UNBOUND_PREPARE_GRACE
            ):
                # Another controller may still prepare this unbound incarnation.
                # Absence does not fence that delayed mutation; retain cleanup.
                return
            if resource is not None and resource.state != "deleted":
                if self.managed_cloudflare_scope is not None and not await self._reserve_managed_cleanup(worker_id):
                    return
                resource = await self.client.remove(worker_id, self.profile.scope, resource.resource_uid)
            # GET absence/a deleted tombstone or a validated DELETE response confirms
            # cleanup. Persist only for this still-terminal, unchanged incarnation.
            async with get_db_context() as db:
                matched = await db.scalar(update(ExecutionWorker).where(
                    ExecutionWorker.id == worker_id, self._managed_scope(),
                    ExecutionWorker.state.in_(self.cleanup_states),
                    ExecutionWorker.capacity_resource_uid == uid,
                    ExecutionWorker.capacity_resource_deleted_at.is_(None),
                ).values(capacity_resource_deleted_at=func.clock_timestamp()).returning(ExecutionWorker.id))
                if matched is not None and runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and resource is not None:
                    await seal_launcher_meter(db, worker_id, resource.usage)
                if matched is not None and self.managed_cloudflare_scope is not None:
                    await delete_workflow_slots(db, CloudflareWorkflowSlot.worker_id == worker_id)
                await db.commit()
            if self.managed_cloudflare_scope is not None and uid is not None and resource is not None and (
                resource.usage is not None and resource.usage.ledger_receipt_pending
            ):
                await compact_ledger_receipt(
                    worker_id, lambda: self.client.acknowledge_receipt(worker_id, self.profile.scope, uid),
                )
            return
        if resource is not None and resource.worker_version != version:
            raise CapacityProtocolError("Provider resource build changed for an existing incarnation")
        if state == "reserved":
            if not self.burst_launch_allowed:
                return
            created = resource is None
            if resource is None:
                if limit is None:
                    raise CapacityProtocolError("Reserved worker has no immutable memory limit")
                managed = self.managed_cloudflare_scope
                reservation = None if managed is not None else WorkerCapacityReservation(
                    worker_id=worker_id, worker_version=version,
                    memory_limit_bytes=limit,
                    max_lifetime_seconds=self.settings.pool_max_lifetime_seconds,
                    drain_grace_seconds=WORKER_DRAIN_GRACE_SECONDS,
                )
                if bounded_prepare:
                    # GET may have raced a loss while it was in flight. Refuse
                    # to send a new PUT once the reservation has ended.
                    async with get_db_context() as db:
                        if managed is not None and not await managed.lock_admission(db):
                            return
                        still_reserved = await db.scalar(select(ExecutionWorker.id).where(
                            ExecutionWorker.id == worker_id, self._managed_scope(),
                            ExecutionWorker.state == "reserved",
                            ExecutionWorker.created_at > _now() - timedelta(
                                seconds=self.settings.pool_start_timeout_seconds,
                            ),
                        ))
                        if still_reserved is None:
                            return
                        async with asyncio.timeout(CAPACITY_REQUEST_SECONDS):
                            if self.profile.protocol_version == 2:
                                pin = await db.get(CloudflareWorkerReservation, worker_id)
                                if uid is None or pin is None or managed is None:
                                    raise CapacityProtocolError("Managed Cloudflare reservation lacks its control binding")
                                now = await db.scalar(select(func.clock_timestamp()))
                                if (now is None or pin.workers_deadline_at is None
                                        or pin.workers_lifetime_seconds is None or pin.workers_deadline_at <= now):
                                    return
                                reservation = WorkerCapacityReservation(
                                    worker_id=worker_id, worker_version=version, memory_limit_bytes=limit,
                                    max_lifetime_seconds=pin.workers_lifetime_seconds,
                                    drain_grace_seconds=WORKERS_DEADLINE_DRAIN_SECONDS,
                                )
                                key = await load_managed_control_key(db, pin, uid)
                                resource = await self.client.prepare(
                                    reservation, self.profile.scope, managed_identity=(uid, key),
                                    **({"managed_deadline": pin.workers_deadline_at} if pin.workers_deadline_wire else {}),
                                )
                            else:
                                assert reservation is not None
                                resource = await self.client.prepare(reservation, self.profile.scope)
                else:
                    assert reservation is not None
                    resource = await self.client.prepare(reservation, self.profile.scope)
            if resource.state != "prepared":
                async with get_db_context() as db:
                    fresh = await db.scalar(select(ExecutionWorker.state).where(ExecutionWorker.id == worker_id))
                if fresh != "reserved":
                    return
                reason = "provider_started_before_binding" if resource.state in ("starting", "running") else "provider_resource_stopped"
                await self._record_loss(worker_id, uid, reason, False)
                return
            async with get_db_context() as db:
                bound = await db.scalar(update(ExecutionWorker).where(
                    ExecutionWorker.id == worker_id, self._managed_scope(),
                    ExecutionWorker.state == "reserved", ExecutionWorker.capacity_resource_uid == uid,
                ).values(
                    capacity_resource_uid=resource.resource_uid, state="starting",
                    launched_by=self.id if created else None,
                ).returning(ExecutionWorker.id))
                await db.commit()
            if bound is not None:
                await self._start_prepared(worker_id, resource.resource_uid)
            return
        if state not in ("starting", "ready", "draining"):
            return
        reason = None
        if resource is None or resource.state in ("deleted", "stopped"):
            reason = "provider_resource_stopped"
        elif state == "starting":
            if _now() - created_at >= timedelta(seconds=self.settings.pool_start_timeout_seconds):
                reason = "start_timeout"
            elif resource.state == "prepared":
                if not self.burst_launch_allowed:
                    return
                assert uid is not None
                await self._start_prepared(worker_id, uid)
                return
        elif resource.state == "prepared":
            reason = "provider_resource_regressed"
        elif self.reachability.steady():
            reason = "heartbeat_lost"
        if reason is not None:
            await self._record_loss(worker_id, uid, reason, resource is not None and resource.out_of_memory)

    async def _record_loss(self, worker_id: UUID, uid: str | None, reason: str, out_of_memory: bool) -> None:
        async with get_db_context() as db:
            worker = await db.scalar(select(ExecutionWorker).where(
                ExecutionWorker.id == worker_id, self._managed_scope(),
                ExecutionWorker.capacity_resource_uid == uid,
                ExecutionWorker.state.in_(("reserved", "starting", "ready", "draining")),
            ).with_for_update())
            if worker is None:
                return
            if reason == "heartbeat_lost" and not await db.scalar(select(ExecutionWorker.id).where(
                ExecutionWorker.id == worker_id, silent_executor(),
            )):
                return
            if reason == "start_timeout" and worker.state != "starting":
                return
            if reason == "prepare_timeout" and worker.state != "reserved":
                return
            self._lose(worker, "out_of_memory" if out_of_memory else reason, _now())
            ended = await end_runs_of_lost_worker(db, worker.id, out_of_memory=out_of_memory)
            await db.commit()
        await announce_lost_runs(ended)


async def external_worker_pool_loop(stop_event: asyncio.Event, settings: Settings) -> None:
    """Run only when explicitly selected; Kubernetes is never a failure fallback."""
    assert settings.worker_capacity_url and settings.worker_capacity_token
    async with httpx.AsyncClient() as http:
        client = ExternalCapacityClient(
            settings.worker_capacity_url, settings.worker_capacity_token.get_secret_value(), http=http,
        )
        primary: ExternalWorkerPoolController | None = None
        wake = asyncio.Event()
        listener = asyncio.create_task(
            listen_for_pool_wake(
                settings.redis_url,
                {
                    **{family: wake for family in DISPATCH_WAKE_FAMILIES},
                    WAKE_PLATFORM_JOB: wake,
                    WAKE_SERVICE: wake,
                },
            )
        )
        try:
            while not stop_event.is_set():
                started = time.monotonic()
                try:
                    if primary is None:
                        profile = await client.profile()
                        if profile.runtime_profile != LINUX_RUNTIME_PROFILE:
                            raise CapacityProtocolError("External primary must advertise Linux capacity")
                        primary = ExternalWorkerPoolController(client, settings, profile)
                    if await primary.reachability.check():
                        await primary.reconcile()
                except Exception:
                    logger.exception("Primary worker capacity reconciliation failed")
                await wait_for_next_pass(
                    stop_event, wake, since=started, spacing=RECONCILE_SECONDS, sweep=SWEEP_SECONDS,
                )
        finally:
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)
            if primary is not None:
                await _withdraw_live(primary.id)
