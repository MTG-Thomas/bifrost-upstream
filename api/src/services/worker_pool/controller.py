"""Capacity controller for reusable workers (runs in the control plane).

One owner for worker capacity: every control replica runs this, and launch
decisions are serialized by a transaction advisory lock, so replicas never
over-launch. Each incarnation is a durable row first; its Job is created
suspended under a deterministic name, its UID persisted, and only then
started. A restarted controller adopts existing Jobs by name and UID.

Workers drain themselves when idle. The controller only marks a worker
``lost`` when it misses its start deadline or its heartbeat, or its Job
disappears; work it held is released by the dispatch reconciler, and runs it
had started follow their domain's terminal sweeps (never replayed).

After an upgrade the controller replaces Jobs running a build no live
controller runs, launched by a controller that is gone (``LiveControllers``):
the ready warm minimum remains available while outdated Jobs drain (never
deleted), so their runs finish. Replacements launch after normal capacity
becomes available.

Scaling (Warm workers, Max workers, Worker memory, Largest worker) is read
from Settings -> Kubernetes at the start of every pass, so a change applies
without a restart.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from shared.models import CloudflareWorkersPolicy, WorkerCapacityProfile
from shared.solution_git_stage import SOLUTION_GIT_CAPABILITY, SOLUTION_UPDATE_CHECK_JOB
from shared.version import get_version
from sqlalchemy import and_, func, literal, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import Settings, get_settings
from src.core.database import get_db_context
from src.jobs.platform.kubernetes_client import (
    KubernetesConfigurationError,
    KubernetesJobClient,
    oom_killed,
)
from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import MANAGED_WORKER_FORMS, ExecutionWorker
from src.models.orm.executions import Execution
from src.models.orm.package_changes import PackageChange
from src.models.orm.platform_jobs import PlatformJob
from src.models.orm.services import ServiceDefinition
from src.models.orm.work_dispatches import WorkDispatch
from src.models.orm.workflows import Workflow
from src.services.kubernetes_execution import KubernetesPolicy, load_kubernetes_policy
from src.services.memory_requests import (
    LAUNCHABLE_GENERATION_KEY,
    LAUNCHABLE_KEY,
    workflow_request_sql,
)
from src.services.pool_cadence import (
    CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS,
    POOL_RECONCILE_SPACING_SECONDS,
    POOL_RECONCILE_SWEEP_SECONDS,
)
from src.services.service_lifecycle import pending_service_statement
from src.services.work_dispatch import (
    DISPATCH_WAKE_FAMILIES,
    WAKE_PLATFORM_JOB,
    WAKE_SERVICE,
    listen_for_pool_wake,
    wait_for_next_pass,
)
from src.services.worker_pool.calibration import measured_budget
from src.services.worker_pool.claims import CAPACITY_LOCK, REMOTE_WORKER_HANDLER_NAMES
from src.services.worker_pool.cloudflare_admission import (
    ManagedCloudflareScope,
    PrimaryPool,
)
from src.services.worker_pool.cloudflare_slots import (
    WorkflowSlotAdmission,
    read_workflow_slot_admission,
    released_prewarm_dispatches,
    tenant_key,
)
from src.services.worker_pool.demand import pending_local_platform_job_ids
from src.services.worker_pool.eligibility import (
    cloudflare_active_claims,
    containers_workflow_fits,
    profile_matches,
)
from src.services.worker_pool.liveness import (
    HEARTBEAT_LOST_AFTER,
    Reachability,
    silent_executor,
)
from src.services.worker_pool.manifest import (
    SAFE_TO_EVICT,
    WorkerJobSpec,
    build_worker_job_manifest,
    resolve_job_spec_when_ready,
    worker_job_name,
)
from src.services.worker_pool.recovery import (
    LostRuns,
    announce_lost_runs,
    end_runs_of_lost_worker,
)
from src.services.worker_pool.sizing import (
    LIVE_STATES,
    JobSizing,
    job_baseline,
    launch_limit,
)
from src.worker.remote.protocol import (
    CLOUDFLARE_DEV_BUDGET_BYTES,
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    CLOUDFLARE_WORKFLOW_HANDLER,
    HANDLER_CONCURRENCY_LIMITS,
    LINUX_RUNTIME_PROFILE,
    cloudflare_slots,
)

logger = logging.getLogger(__name__)
_LIMITED_HANDLERS = frozenset(handler for handler, _ in HANDLER_CONCURRENCY_LIMITS)

# How long a lost worker's Job is kept under watch for deletion.
LOST_JOB_CLEANUP = timedelta(hours=1)
# The controller reconciles on a pool wake-up (new work may need a worker), at
# most every RECONCILE_SECONDS, and sweeps every SWEEP_SECONDS without one:
# often enough for worker-loss detection and idle-worker eviction marks.
RECONCILE_SECONDS = POOL_RECONCILE_SPACING_SECONDS
SWEEP_SECONDS = POOL_RECONCILE_SWEEP_SECONDS
# A ready worker holding no work this long may be evicted by the cluster
# autoscaler to empty a node; it drains on SIGTERM and is replaced.
EVICTABLE_AFTER_IDLE_SECONDS = 10.0
# The reconciler treats a missing value as "no controller"; it outlives a
# few failed reconciles.
LAUNCHABLE_TTL_SECONDS = 300
# Every controller instance's build, by instance id, with when it last wrote.
CONTROLLERS_KEY = "bifrost:worker-pool:controllers"


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class LiveControllers:
    """Controller instances that wrote within ``HEARTBEAT_LOST_AFTER``."""

    ids: frozenset[UUID]
    versions: frozenset[str]

    def outdated(self, worker: ExecutionWorker) -> bool:
        """Rotate admitted workers once no live controller advertises their build.

        Kubernetes controllers have a fixed build, so their own Jobs remain
        protected from replacement loops. External controllers can adopt a
        new provider build without restarting; their identity cannot protect
        workers of the previous build. Unadmitted workers never rotate here.
        """
        return (
            worker.form in MANAGED_WORKER_FORMS
            and worker.state in ("ready", "draining")
            and worker.version not in self.versions
            and (worker.form == "external" or worker.launched_by not in self.ids)
        )


class WorkerCapacityController(ABC):
    """Shared reservation, demand, rotation and lifecycle policy."""

    def __init__(self, settings: Settings, *, worker_form: str) -> None:
        self.settings = settings
        self.worker_form = worker_form
        self.cleanup_states = ("lost",)
        # Lost needs a steadily reachable database (worker_pool.liveness).
        self.reachability = Reachability()
        # This instance, as the launcher recorded on the Jobs it creates.
        self.id = uuid4()
        self.version = get_version()
        self.live = LiveControllers(ids=frozenset({self.id}), versions=frozenset({self.version}))
        # Draining workers with active runs, by when this instance first saw them.
        self._draining_since: dict[UUID, float] = {}
        self._warned_long_drains: set[UUID] = set()
        # Ready workers holding no work, by when this instance first saw them idle.
        self._idle_since: dict[UUID, float] = {}
        # The safe-to-evict value this instance last applied to each worker's
        # pod. Unknown until applied once, so a restarted controller corrects
        # a mark left by its predecessor.
        self._evictable: dict[UUID, bool] = {}
        self.runtime_profile = LINUX_RUNTIME_PROFILE
        self.capacity_profile: WorkerCapacityProfile | None = None
        self.peer_profile: WorkerCapacityProfile | None = None
        self.additional_profile: WorkerCapacityProfile | None = None
        self.managed_cloudflare_scope: ManagedCloudflareScope | None = None
        self.cloudflare_primary: PrimaryPool | None = None

    @property
    def containers_scope(self) -> str | None:
        return getattr(self.settings, "cloudflare_containers_scope", None)

    @property
    def containers_mode(self) -> bool:
        return self.capacity_profile is not None and self.capacity_profile.scope == self.containers_scope

    @property
    def dual_capacity(self) -> bool:
        return self.peer_profile is not None

    def _counts_for_warm_floor(self, worker: ExecutionWorker) -> bool:
        return (
            not self.dual_capacity
            or worker.execution_lane != "normal"
            or (
                worker.runtime_profile == LINUX_RUNTIME_PROFILE
                and (self.containers_scope is None or worker.capacity_scope != self.containers_scope)
            )
        )

    def _outdated(self, worker: ExecutionWorker) -> bool:
        if worker.form == "external" and worker.state == "ready":
            for profile in (self.capacity_profile, self.peer_profile, self.additional_profile):
                if profile is not None and worker.capacity_scope == profile.scope and (
                    self.dual_capacity or profile.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
                ):
                    return worker.version != profile.worker_version
        return self.live.outdated(worker)

    @abstractmethod
    def _managed_scope(self) -> Any:
        """SQL predicate selecting resources this configured provider owns."""
        raise NotImplementedError

    @abstractmethod
    def _binding_fields(self, worker_id: UUID) -> dict[str, Any]:
        """Persist provider identity before the resource can start."""
        raise NotImplementedError

    @abstractmethod
    async def reconcile_worker(self, worker_id: UUID, policy: KubernetesPolicy) -> None:
        """Reconcile one physical resource without changing domain ownership."""
        raise NotImplementedError

    def _cleanup_scope(self) -> Any:
        return (
            ExecutionWorker.state.in_(self.cleanup_states)
            & (ExecutionWorker.ended_at >= _now() - LOST_JOB_CLEANUP)
        )

    async def reconcile(self) -> None:
        self.live = await _publish_live(self.id, self.version)
        async with get_db_context() as db:
            policy = await load_kubernetes_policy(db)
            ids = (
                await db.scalars(
                    select(ExecutionWorker.id).where(
                        self._managed_scope(),
                        ExecutionWorker.state.in_(LIVE_STATES) | self._cleanup_scope(),
                    )
                )
            ).all()
        for worker_id in set(self._draining_since) - set(ids):
            del self._draining_since[worker_id]
        for worker_id in set(self._evictable) - set(ids):
            self._evictable.pop(worker_id, None)
            self._idle_since.pop(worker_id, None)
        # Refresh capacity under its lock before any provider I/O can stall.
        # New rows are included in this pass; provider losses free their slots
        # for the next bounded reconciliation pass.
        launched = await self.launch_for_demand(policy)
        for worker_id in dict.fromkeys([*ids, *launched]):
            await self._reconcile_one(worker_id, policy)
        await self.replace_outdated(policy)

    async def _reconcile_one(self, worker_id: UUID, policy: KubernetesPolicy) -> None:
        """One worker's failure (a slow Kubernetes API, a replaced Job) must
        not stop the others from being reconciled."""
        try:
            await self.reconcile_worker(worker_id, policy)
        except Exception as exc:
            logger.exception(
                "Reusable worker reconciliation failed (%s): %s",
                type(exc).__name__,
                exc,
                extra={"worker_id": str(worker_id)},
            )

    def _watch_drain(self, worker: ExecutionWorker) -> None:
        """Warn once when a draining worker's runs outlast the maximum
        lifetime: it still counts toward Max workers."""
        if worker.state != "draining" or worker.active_runs == 0:
            self._draining_since.pop(worker.id, None)
            return
        since = self._draining_since.setdefault(worker.id, time.monotonic())
        if (
            time.monotonic() - since >= self.settings.pool_max_lifetime_seconds
            and worker.id not in self._warned_long_drains
        ):
            self._warned_long_drains.add(worker.id)
            logger.warning(
                "A draining worker still has runs after its maximum lifetime",
                extra={
                    "worker_id": str(worker.id),
                    "active_runs": worker.active_runs,
                    "end_reason": worker.end_reason,
                },
            )

    @staticmethod
    def _lose(worker: ExecutionWorker, reason: str, now: datetime) -> None:
        logger.warning(
            "Reusable worker lost",
            extra={"worker_id": str(worker.id), "reason": reason, "state": worker.state},
        )
        worker.state = "lost"
        worker.end_reason = reason
        worker.ended_at = now

    def _sizing(self, policy: KubernetesPolicy, workers: Any) -> JobSizing:
        """Settings -> Kubernetes sizes at the baseline live Jobs report."""
        threshold = self.settings.memory_pressure_threshold
        return JobSizing.of(
            policy, threshold, job_baseline(workers, policy.worker_memory_mib * 2**20, threshold)
        )

    def _lifetime_handoff_imminent(self, worker: ExecutionWorker, now: datetime) -> bool:
        """Whether a standard warm Job needs its replacement started before it drains.

        ``ready_at`` precedes the worker's claim-loop lifetime clock, so it is
        a durable, conservative-early deadline for starting the replacement.
        The existing start-timeout window preserves a logical warm worker while
        the new Job initializes. Short lifetimes use at most half their
        lifetime as lead time, so a new worker is never itself immediately
        considered due for replacement.
        """
        if (
            worker.form not in MANAGED_WORKER_FORMS
            or worker.resource_class != "standard"
            or worker.state != "ready"
            or worker.ready_at is None
        ):
            return False
        lead_seconds = min(
            self.settings.pool_start_timeout_seconds,
            self.settings.pool_max_lifetime_seconds // 2,
        )
        return worker.ready_at + timedelta(
            seconds=self.settings.pool_max_lifetime_seconds - lead_seconds
        ) <= now

    async def launch_for_demand(self, policy: KubernetesPolicy) -> list[UUID]:
        """Reserve incarnations for waiting work, sized by memory, within ceilings.

        Waiting requests (dispatches, pending services, and eligible local
        platform jobs) are placed oldest first into the free budget of
        executors that can claim (a Job that has not reported its budget
        counts as the one expected). What does not fit goes into new
        standard Jobs, or a Job sized for one request a standard Job cannot
        hold. Actual claims always wait for measured fit. Idle insufficient
        workers can yield the warm minimum; Jobs never exceed Max workers.
        """
        async with get_db_context() as db:
            locked = (
                await db.execute(
                    text("SELECT pg_try_advisory_xact_lock(hashtext(:key))"),
                    {"key": CAPACITY_LOCK},
                )
            ).scalar_one()
            if not locked:
                return []
            managed_scope = self.managed_cloudflare_scope
            control_root_version = None
            workers_policy = None
            deadline_wire = False
            workflow_slots = None
            if managed_scope is not None:
                control_root_version = await managed_scope.lock_admission(db)
                if control_root_version is None:
                    return []
                policy_data = await db.scalar(select(CloudflareInstallation.policy).where(
                    CloudflareInstallation.id == managed_scope.installation_id,
                ))
                assert policy_data is not None
                workers_policy = CloudflareWorkersPolicy.model_validate(policy_data["limits"]["workers"])
                deadline_wire = "per_second_caps" in policy_data.get("meter_launcher", {})
                workflow_slots = await read_workflow_slot_admission(db, managed_scope.installation_id, workers_policy)
            platform_job_ids = await pending_local_platform_job_ids(db, policy, _now())
            waiting = await waiting_capacity_rows(db, platform_job_ids, _now())
            # Managed resources share the worker ceiling across provider forms.
            # Only Linux workers participate in memory calibration and probes.
            workers = (
                await db.execute(
                    select(ExecutionWorker).where(
                        ExecutionWorker.state.in_(LIVE_STATES),
                    ).with_for_update()
                )
            ).scalars().all()
            linux_workers = [worker for worker in workers if worker.runtime_profile == LINUX_RUNTIME_PROFILE]
            if managed_scope is not None and (
                self.cloudflare_primary is None or not self.cloudflare_primary.saturated(linux_workers, _now())
            ):
                return []
            generation = int(await db.scalar(select(func.coalesce(func.max(PackageChange.id), 0))) or 0)
            environment = self._memory_environment(generation)
            history = (await db.scalars(select(ExecutionWorker).where(
                self._managed_scope(),
                ExecutionWorker.runtime_profile == LINUX_RUNTIME_PROFILE,
                ExecutionWorker.state.in_((*LIVE_STATES, "retired")),
                ExecutionWorker.memory_calibration["environment"] == environment,
                ExecutionWorker.memory_calibration["measurement"].is_not(None),
            ).distinct(ExecutionWorker.memory_limit_bytes).order_by(
                ExecutionWorker.memory_limit_bytes, ExecutionWorker.created_at.desc(),
            ))).all()
            measurements = [sample for worker in history
                            if (sample := measured_budget(worker, environment)) is not None]
            sizing = self._sizing(policy, [])
            if measurements:
                sizing = replace(sizing, baseline_bytes=max(sample.baseline_bytes for sample in measurements))
            standard_reports = [sample.budget_bytes for sample in measurements
                                if sample.limit_bytes == sizing.standard_limit_bytes]
            largest_reports = [sample.budget_bytes for sample in measurements
                               if sample.limit_bytes == sizing.largest_limit_bytes]
            confirmed_largest = largest_reports[-1] if largest_reports else None
            # Publication and decisions share one ordered snapshot. Predictions
            # choose resources; only a measured largest size can reject work.
            cloudflare = self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
            if not cloudflare and not self.containers_mode:
                await _publish_launchable(confirmed_largest, generation)

            launch_cutoff = None
            if managed_scope is not None:
                database_now = await db.scalar(select(func.clock_timestamp()))
                assert database_now is not None
                launch_cutoff = database_now - timedelta(seconds=CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS)
            waiting_requests = [
                Request(
                    row.id, row.memory_request_bytes, row.handler, row.handler_version,
                    row.cloudflare_revision, row.tenant_id,
                    platform_job_type=row.platform_job_type,
                )
                for row in waiting
                if launch_cutoff is None or row.created_at <= launch_cutoff
            ]
            requests = {request.id: request for request in waiting_requests}
            free: list[FreeBudget] = []
            covered: set[UUID] = set()
            for worker in linux_workers:
                # Only physical workers contribute launcher capacity.
                if (
                    "workflow.execute" not in worker.supported_handlers
                    or worker.execution_lane != "normal"
                ):
                    continue
                handlers = frozenset(worker.supported_handlers)
                containers = self.containers_scope is not None and worker.capacity_scope == self.containers_scope
                if worker.state == "draining" or worker.memory_pressured:
                    continue
                # A worker launched for one dispatch holds it until it claims it.
                reservation = requests.get(worker.reserved_for_dispatch_id)
                held = reservation.bytes if reservation else 0
                if reservation:
                    covered.add(reservation.id)
                handler_slots = linux_handler_slots(
                    worker, reservation.handler if reservation else None, containers=containers,
                )
                # A Job admitted but still starting its pool has not measured
                # its budget yet; until it reports, count the one expected.
                # A static executor counts only once ready and measured.
                if worker.form in MANAGED_WORKER_FORMS and (
                    worker.state in ("reserved", "starting") or worker.memory_budget_bytes is None
                ):
                    budget = sizing.budget_bytes(launch_limit(worker, sizing.standard_limit_bytes))
                    free.append(FreeBudget(max(0, budget - held), handlers, handler_slots, containers=containers))
                elif worker.state == "ready" and worker.memory_budget_bytes is not None:
                    free.append(FreeBudget(
                        max(0, worker.memory_budget_bytes - worker.memory_reserved_bytes - held),
                        handlers, handler_slots, containers=containers,
                    ))
            cloudflare_workers = [
                worker for worker in workers
                if worker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
                and worker.form == "external" and worker.execution_lane == "normal"
            ]
            active_claims = (await db.execute(
                cloudflare_active_claims()
            )).all() if cloudflare_workers else []
            managed_cloudflare_ids = set(await db.scalars(select(CloudflareWorkerReservation.worker_id).where(
                CloudflareWorkerReservation.worker_id.in_([worker.id for worker in cloudflare_workers]),
            ))) if cloudflare_workers else set()
            active_counts = Counter(row.worker_id for row in active_claims)
            prewarm_dispatches = await released_prewarm_dispatches(
                db, [worker.id for worker in cloudflare_workers],
            ) if cloudflare_workers else {}
            active_tenants = {row.tenant_id for row in active_claims if row.execution_id is not None}
            leased = set((await db.scalars(select(ExecutionWorker.id).where(
                ExecutionWorker.id.in_([worker.id for worker in cloudflare_workers]),
                ExecutionWorker.remote_owner_lease_expires_at > func.now(),
            ))).all()) if cloudflare_workers else set()
            idle_declared: dict[UUID, FreeBudget] = {}
            available_declared = []
            for worker in cloudflare_workers:
                try:
                    slots = cloudflare_slots(worker.memory_limit_bytes)
                except ValueError:
                    continue
                if worker.state in ("reserved", "starting") and worker.capacity_scope is not None:
                    available_declared.append(_cloudflare_budget(slots))
                    continue
                if (
                    worker.state != "ready" or worker.id not in leased or worker.memory_pressured
                    or CLOUDFLARE_WORKFLOW_HANDLER[0] not in worker.supported_handlers
                ):
                    continue
                held = prewarm_dispatches.get(worker.id, ())
                used = max(worker.active_runs, active_counts[worker.id]) + len(held)
                if worker.memory_budget_bytes is None:
                    # Hello can promote a fresh start before its first report.
                    # Forecast that pending capacity without offering it for yield.
                    if (
                        worker.capacity_scope is not None and worker.remote_session_generation <= 1
                        and used == 0 and worker.memory_reserved_bytes == 0
                    ):
                        available_declared.append(_cloudflare_budget(slots))
                    continue
                if worker.memory_budget_bytes != slots * CLOUDFLARE_DEV_BUDGET_BYTES:
                    continue
                covered.update(held)
                budget = _cloudflare_budget(slots, used=used, reserved_bytes=worker.memory_reserved_bytes)
                available_declared.append(budget)
                if used == 0 and worker.memory_reserved_bytes == 0:
                    idle_declared[worker.id] = budget
            # Prefer Cloudflare slots for eligible work, leaving Linux capacity
            # for requests only Linux can execute.
            free = [*available_declared, *sorted(free, key=lambda budget: budget.containers)]
            needed = [request for request in waiting_requests if request.id not in covered]
            profile, peer = self.capacity_profile, self.peer_profile
            new_declared = None
            if cloudflare:
                assert profile is not None
                new_declared = _cloudflare_budget(cloudflare_slots(profile.memory_limit_bytes))
            if new_declared is not None:
                # Give Linux-only work first use of any remaining Linux budget.
                needed.sort(key=new_declared.fits)
            jobs = sum(1 for worker in workers if worker.form in MANAGED_WORKER_FORMS)
            # DB-managed Workers have their own ceiling. Their live rows use
            # primary capacity only through an explicitly shared ceiling.
            primary_jobs = jobs if self.dual_capacity else jobs - len(managed_cloudflare_ids)
            room = policy.max_workers if managed_scope is not None else max(0, policy.max_workers - primary_jobs)
            shared_cap = await db.scalar(
                select(CloudflareInstallation.policy["limits"]["shared_managed_worker_cap"].as_integer())
                .where(
                    CloudflareInstallation.retired_at.is_(None),
                    CloudflareInstallation.admitted_revision.is_not(None),
                )
            )
            shared_room: int | None = None
            if shared_cap is not None:
                pins = set((await db.scalars(
                    select(CloudflareWorkerReservation.worker_id)
                    .join(ExecutionWorker, ExecutionWorker.id == CloudflareWorkerReservation.worker_id)
                    .where(ExecutionWorker.capacity_resource_deleted_at.is_(None))
                )).all())
                pinned_live = sum(worker.id in pins for worker in workers)
                shared_room = max(0, shared_cap - jobs - (len(pins) - pinned_live))
                if managed_scope is not None:
                    room = shared_room
                elif not self.dual_capacity:
                    room = min(shared_room, room)
            scope_room = room
            if profile is not None and (cloudflare or self.dual_capacity):
                scope_counts = await self._external_scope_counts(db, profile, peer)
                live_in_scopes = sum(
                    worker.form == "external" and worker.capacity_scope in scope_counts for worker in workers
                )
                if managed_scope is None:
                    room = min(room, max(0, policy.max_workers - jobs - (sum(scope_counts.values()) - live_in_scopes)))
                scope_room = max(0, profile.max_workers - scope_counts.get(profile.scope, 0))
                if scope_room == 0:
                    return []
                if (self.containers_mode or (cloudflare and self.dual_capacity)) and (
                    peer is None or sum(
                        worker.form == "external" and worker.capacity_scope == peer.scope
                        for worker in linux_workers
                    ) < peer.max_workers
                ):
                    return []
            plan = _plan_launches(
                needed,
                free,
                standard_budget=standard_reports[-1] if standard_reports else sizing.standard_budget_bytes,
                largest_budget=confirmed_largest if confirmed_largest is not None else sizing.largest_budget_bytes,
                cold=not measurements, probe_largest=confirmed_largest is None,
                new_declared=new_declared,
                new_containers=self.containers_mode,
                active_tenants=active_tenants,
                workflow_slots=workflow_slots,
            )
            now = _now()
            # Only standard Jobs make up the warm minimum; a sized Job exists
            # for its large work and drains once idle. A standard Job near its
            # self-managed lifetime is excluded here so its handoff can start
            # before it stops claiming. An outdated Job still claims until it
            # drains, so replacements stay within Max workers.
            undrained = sum(
                1
                for worker in workers
                if worker.form in MANAGED_WORKER_FORMS
                and worker.execution_lane == "normal"
                and worker.resource_class == "standard"
                and worker.state != "draining"
                and worker.id not in managed_cloudflare_ids
                and self._counts_for_warm_floor(worker)
                and not self._outdated(worker)
                and not self._lifetime_handoff_imminent(worker, now)
            )
            warm = 0 if self.containers_mode else max(0, policy.warm_workers - undrained - plan.standard - len(plan.sized))
            if new_declared is not None:
                launched = [
                    self._reserve(
                        db, resource_class="standard", memory_limit_bytes=new_declared.bytes,
                        environment=environment,
                    )
                    for _ in range(min(
                        room, scope_room, plan.standard + (0 if self.dual_capacity or managed_scope else warm),
                    ))
                ]
                if launched:
                    if managed_scope is not None and control_root_version is not None:
                        assert workers_policy is not None
                        decision_at = await db.scalar(select(func.clock_timestamp()))
                        await db.flush()
                        reserved_at = await db.scalar(select(func.clock_timestamp()))
                        assert decision_at is not None and reserved_at is not None
                        for worker_id in launched:
                            pin = managed_scope.pin(
                                worker_id, control_root_version, reserved_at=reserved_at,
                                lifetime=workers_policy.max_lifetime_seconds, deadline_wire=deadline_wire,
                            )
                            pin.launch_timing = {
                                "controller_decision_at": decision_at.isoformat(),
                                "launcher_reserved_at": reserved_at.isoformat(),
                            }
                            db.add(pin)
                    await db.commit()
                return launched
            # Fast Path capacity is a fixed exclusive reservation, not an
            # elastic overflow lane. It intentionally ignores background
            # demand and therefore cannot be borrowed by it.
            fast_path_undrained = sum(
                1
                for worker in workers
                if worker.form in MANAGED_WORKER_FORMS
                and worker.execution_lane == "fast_path"
                and worker.resource_class == "standard"
                and worker.state != "draining"
                and not self._outdated(worker)
                and not self._lifetime_handoff_imminent(worker, now)
            )
            fast_path_warm = max(0, policy.reserved_fast_path_workers - fast_path_undrained)
            # Keep the reservation's slots out of the Normal planner. Without
            # this, a Normal burst arriving in the same pass could consume the
            # final slots before the Fast Path floor is reserved.
            normal_room = max(0, min(room, scope_room) - fast_path_warm)
            # First initialization is singular; it may be sized by the hint,
            # but an uncertain prediction can never veto that initial attempt.
            if not measurements and any(
                (worker.state in ("reserved", "starting") or
                 (worker.state == "ready" and worker.memory_budget_bytes is None))
                and worker.memory_calibration
                and worker.memory_calibration.get("environment") == environment
                for worker in linux_workers if worker.form in MANAGED_WORKER_FORMS
            ):
                return []
            # Pace failures per resource size without disabling recovery. An
            # unmeasured size gets only one probe at a time, even after another
            # size has already calibrated the environment.
            failed_limits = set((await db.scalars(select(ExecutionWorker.memory_limit_bytes).where(
                self._managed_scope(), ExecutionWorker.state == "lost",
                ExecutionWorker.runtime_profile == LINUX_RUNTIME_PROFILE,
                ExecutionWorker.memory_budget_bytes.is_(None),
                ExecutionWorker.version == self.version,
                ExecutionWorker.memory_calibration["environment"] == environment,
                ExecutionWorker.ended_at > _now() - timedelta(seconds=self.settings.pool_start_timeout_seconds),
            ))).all())
            known_limits = {sample.limit_bytes for sample in measurements}
            probing_limits = {
                worker.memory_limit_bytes for worker in linux_workers
                if worker.form in MANAGED_WORKER_FORMS and worker.memory_budget_bytes is None
                and worker.memory_calibration
                and worker.memory_calibration.get("environment") == environment
                and worker.memory_limit_bytes not in known_limits
            }
            blocked_limits = failed_limits | probing_limits
            plan = LaunchPlan(
                standard=0 if sizing.standard_limit_bytes in blocked_limits else plan.standard,
                sized=[(identity, request) for identity, request in plan.sized
                       if (sizing.pod_limit_bytes(request) or sizing.largest_limit_bytes) not in blocked_limits],
            )
            if sizing.standard_limit_bytes in blocked_limits:
                warm = 0
            if room == 0 and fast_path_warm:
                # A Settings transition can reserve slots that were already
                # occupied by Normal workers. Drain only idle Normal workers
                # above the new Normal cap; active work is never preempted and
                # Fast Path capacity reports degraded until a slot is freed.
                normal_cap = policy.max_workers - policy.reserved_fast_path_workers
                normal_live = [
                    worker for worker in workers
                    if worker.form in MANAGED_WORKER_FORMS
                    and worker.id not in managed_cloudflare_ids
                    and worker.execution_lane == "normal"
                    and worker.state != "draining"
                ]
                excess = max(0, len(normal_live) - normal_cap)
                if excess:
                    owned = set((await db.scalars(select(ExecutionWorker.id).where(
                        self._managed_scope(), ExecutionWorker.state == "ready"
                    ))).all())
                    candidates = [
                        worker for worker in normal_live
                        if worker.id in owned and worker.state == "ready"
                        # Draining is non-preemptive: current work keeps
                        # running, but this excess Normal worker cannot claim
                        # another dispatch while the reservation converges.
                        and not worker.reserved_for_dispatch_id
                    ]
                    for worker in candidates[:excess]:
                        await db.execute(update(ExecutionWorker).where(
                            ExecutionWorker.id == worker.id, ExecutionWorker.state == "ready",
                            ExecutionWorker.reserved_for_dispatch_id.is_(None),
                        ).values(state="draining", end_reason="fast_path_reservation"))
                    await db.commit()
                    return []
            fill_linux_floor = self.dual_capacity and warm > 0
            at_ceiling = primary_jobs >= policy.max_workers or (shared_cap is not None and room == 0)
            if at_ceiling and (plan.sized or plan.standard or fill_linux_floor):
                # An idle undersized worker may yield the warm minimum, but it
                # still occupies its slot until the canonical owner retires it.
                owned = set((await db.scalars(select(ExecutionWorker.id).where(self._managed_scope(), ExecutionWorker.state == "ready"))).all())
                ready = [worker for worker in linux_workers
                         if worker.id in owned and worker.state == "ready"
                         and worker.active_runs == 0 and worker.memory_reserved_bytes == 0
                         and worker.active_handler_counts is not None
                         and not any(worker.active_handler_counts.values())
                         and not worker.reserved_for_dispatch_id
                         and worker.memory_budget_bytes is not None]
                for worker_id, budget in (() if self.containers_mode else idle_declared.items()):
                    if worker_id in owned or (shared_room != 0 and worker_id in managed_cloudflare_ids):
                        continue
                    if fill_linux_floor or not any(
                        request.tenant_id not in active_tenants and budget.fits(request) for request in needed
                    ):
                        # Capacity demand is global. The fenced launcher owner
                        # observes draining and retires its own resource.
                        await db.execute(update(ExecutionWorker).where(
                            ExecutionWorker.id == worker_id, ExecutionWorker.state == "ready",
                            ExecutionWorker.active_runs == 0, ExecutionWorker.memory_reserved_bytes == 0,
                        ).values(state="draining", end_reason="capacity_replacement"))
                        await db.commit()
                        return []
                if not (plan.sized or plan.standard):
                    return []
                for worker in ready:
                    assert worker.memory_budget_bytes is not None
                    budget = FreeBudget(
                        worker.memory_budget_bytes, frozenset(worker.supported_handlers), containers=self.containers_mode,
                    )
                    if not any(request.bytes > budget.bytes and budget.takes(request) for request in needed):
                        continue
                    await db.execute(update(ExecutionWorker).where(
                        ExecutionWorker.id == worker.id, ExecutionWorker.state == "ready",
                        ExecutionWorker.active_runs == 0, ExecutionWorker.memory_reserved_bytes == 0,
                    ).values(state="draining", end_reason="capacity_replacement"))
                    break
                await db.commit()
                return []
            launched: list[UUID] = []
            for _dispatch_id, request in plan.sized:
                if len(launched) >= normal_room:
                    break
                limit = sizing.pod_limit_bytes(request) or sizing.largest_limit_bytes
                if limit in probing_limits:
                    continue
                if limit not in known_limits:
                    probing_limits.add(limit)
                launched.append(
                    self._reserve(
                        db,
                        resource_class="standard" if limit == sizing.standard_limit_bytes else "sized",
                        memory_limit_bytes=limit,
                        # New workers claim through measured fit, including
                        # sized workers whose actual overhead differs.
                        environment=environment,
                    )
                )
            standard_launches = plan.standard + warm
            if sizing.standard_limit_bytes not in known_limits:
                standard_launches = min(1, standard_launches)
            for _ in range(standard_launches):
                if len(launched) >= normal_room:
                    break
                launched.append(
                    self._reserve(
                        db,
                        resource_class="standard",
                        memory_limit_bytes=sizing.standard_limit_bytes,
                        environment=environment,
                    )
                )
            for _ in range(fast_path_warm):
                if len(launched) >= room:
                    break
                launched.append(
                    self._reserve(
                        db,
                        resource_class="standard",
                        memory_limit_bytes=sizing.standard_limit_bytes,
                        environment=environment,
                        execution_lane="fast_path",
                    )
                )
            await db.commit()
        if launched:
            logger.info(
                "Launching reusable workers",
                extra={
                    "count": len(launched),
                    "waiting": len(waiting),
                    "sized": len(plan.sized),
                    "free_mib": sum(budget.bytes for budget in free) // 2**20,
                },
            )
        return launched

    async def _external_scope_counts(
        self, db: Any, profile: WorkerCapacityProfile, peer: WorkerCapacityProfile | None,
    ) -> dict[str, int]:
        # An unbound Cloudflare reservation may have prepared with a lost
        # reply. Keep its slot through the bounded absence check as well.
        scopes = ExecutionWorker.capacity_scope.in_([
            item.scope for item in (profile, peer, self.additional_profile) if item is not None
        ])
        unbound_can_prepare = ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
        if self.containers_scope is not None:
            scopes = scopes | (ExecutionWorker.capacity_scope == self.containers_scope)
            unbound_can_prepare = unbound_can_prepare | (ExecutionWorker.capacity_scope == self.containers_scope)
        if self.dual_capacity and (peer is None or (self.containers_mode and self.additional_profile is None)):
            # Discovery can be down while a previous burst incarnation still
            # occupies shared physical capacity. Its persisted profile is enough.
            scopes = scopes | (
                (ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE)
                & ExecutionWorker.capacity_scope.is_not(None)
            )
        counts = await db.execute(
            select(ExecutionWorker.capacity_scope, func.count())
            .where(
                ExecutionWorker.form == "external",
                scopes,
                ExecutionWorker.state.in_(LIVE_STATES)
                | (
                    ExecutionWorker.state.in_(("lost", "retired"))
                    & (
                        ExecutionWorker.capacity_resource_uid.is_not(None)
                        | unbound_can_prepare
                    )
                    & ExecutionWorker.capacity_resource_deleted_at.is_(None)
                ),
            )
            .group_by(ExecutionWorker.capacity_scope)
        )
        return dict(counts.tuples().all())

    async def replace_outdated(self, policy: KubernetesPolicy) -> list[UUID]:
        """Drain only old ready Jobs whose removal keeps the warm minimum.

        Current replacements launch first whenever a slot is available. If
        a full pool has no current standard Job making progress, standard
        ready Jobs above the warm minimum let one old Job drain and free an
        ordinary slot. Draining Jobs continue to count toward Max workers
        until they retire.
        """
        async with get_db_context() as db:
            locked = (
                await db.execute(
                    text("SELECT pg_try_advisory_xact_lock(hashtext(:key))"),
                    {"key": CAPACITY_LOCK},
                )
            ).scalar_one()
            if not locked:
                return []
            managed = (
                await db.execute(
                    select(ExecutionWorker, self._managed_scope().label("owned")).where(
                        ExecutionWorker.form.in_(MANAGED_WORKER_FORMS),
                        ExecutionWorker.state.in_(LIVE_STATES),
                    )
                )
            ).all()
            jobs = [worker for worker, _ in managed]
            ready = [worker for worker in jobs if worker.state == "ready"]
            # Scope-local rotation compares the configured build for that scope.
            outdated = [
                worker
                for worker, owned in managed
                if worker.state == "ready"
                and (owned or (not self.dual_capacity and self.runtime_profile == LINUX_RUNTIME_PROFILE
                               and worker.runtime_profile == LINUX_RUNTIME_PROFILE))
                and self._outdated(worker)
                and worker.reserved_for_dispatch_id is None
            ]
            if not outdated:
                return []
            lane_floor = {
                "normal": policy.warm_workers,
                "fast_path": policy.reserved_fast_path_workers,
            }
            # Rotation must not borrow a current Normal worker to replace an
            # old Fast Path worker (or the reverse): each lane keeps its own
            # ready floor during a rolling replacement.
            current_ready = {
                lane: sum(
                    1
                    for worker in ready
                    if worker.execution_lane == lane
                    and worker.resource_class == "standard"
                    and self._counts_for_warm_floor(worker)
                    and worker.memory_budget_bytes is not None
                    and not self._outdated(worker)
                )
                for lane in lane_floor
            }
            drainable: list[ExecutionWorker] = []
            for lane, floor in lane_floor.items():
                lane_outdated = [worker for worker in outdated if worker.execution_lane == lane]
                drainable.extend(worker for worker in lane_outdated if not self._counts_for_warm_floor(worker))
                lane_outdated = [worker for worker in lane_outdated if self._counts_for_warm_floor(worker)]
                if floor == 0 or current_ready[lane] >= floor:
                    drainable.extend(lane_outdated)
                    continue
                # At the hard total cap, one outdated standard worker above
                # this lane's own floor can make room for its replacement.
                # Do not use another lane's ready count for that decision.
                current_starting = any(
                    worker.execution_lane == lane
                    and worker.resource_class == "standard"
                    and self._counts_for_warm_floor(worker)
                    and worker.state in ("reserved", "starting")
                    and not self._outdated(worker)
                    for worker in jobs
                )
                ready_standard = sum(
                    1
                    for worker in ready
                    if worker.execution_lane == lane
                    and worker.resource_class == "standard"
                    and self._counts_for_warm_floor(worker)
                    and worker.memory_budget_bytes is not None
                )
                excess = max(0, ready_standard - floor)
                if len(jobs) >= policy.max_workers and not current_starting:
                    drainable.extend(
                        worker
                        for worker in lane_outdated[:excess]
                        if worker.resource_class == "standard"
                    )
            if not drainable:
                return []
            drained = list(
                (
                    await db.scalars(
                        update(ExecutionWorker)
                        .where(
                            ExecutionWorker.id.in_([worker.id for worker in drainable]),
                            ExecutionWorker.state == "ready",
                        )
                        .values(state="draining", end_reason="rotated")
                        .returning(ExecutionWorker.id)
                    )
                ).all()
            )
            await db.commit()
        if drained:
            logger.info(
                "Replacing workers running an older build",
                extra={"workers": [str(worker_id) for worker_id in drained], "version": self.version},
            )
        return drained

    def _memory_environment(self, generation: int) -> dict[str, Any]:
        return {"form": self.worker_form, "version": self.version,
                "package_generation": generation, "threshold": self.settings.memory_pressure_threshold}

    def _reserve(
        self,
        db: Any,
        *,
        resource_class: str,
        memory_limit_bytes: int,
        environment: dict[str, Any],
        execution_lane: str = "normal",
    ) -> UUID:
        """A durable row for one Job, recording the limit it launches with:
        a later Worker memory change applies to new Jobs only."""
        worker_id = uuid4()
        cloudflare = self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
        db.add(
            ExecutionWorker(
                id=worker_id,
                form=self.worker_form,
                state="reserved",
                resource_class=resource_class,
                memory_limit_bytes=memory_limit_bytes,
                memory_calibration=None if cloudflare else {"environment": environment},
                supported_handlers=(
                    [CLOUDFLARE_WORKFLOW_HANDLER[0], SOLUTION_GIT_CAPABILITY] if self.containers_mode
                    else [CLOUDFLARE_WORKFLOW_HANDLER[0]] if cloudflare
                    else list(REMOTE_WORKER_HANDLER_NAMES)
                ),
                runtime_profile=self.runtime_profile,
                remote_launcher_id=worker_id if cloudflare else None,
                execution_lane=execution_lane,
                **self._binding_fields(worker_id),
            )
        )
        return worker_id


class WorkerPoolController(WorkerCapacityController):
    """Kubernetes adapter for the shared physical-worker controller."""

    def __init__(
        self, client: KubernetesJobClient, settings: Settings, spec: WorkerJobSpec,
    ) -> None:
        super().__init__(settings, worker_form="job")
        self.client = client
        self.spec = spec

    def _memory_environment(self, generation: int) -> dict[str, Any]:
        return {**super()._memory_environment(generation), "image": self.spec.image}

    def _managed_scope(self) -> Any:
        return and_(
            ExecutionWorker.form == "job",
            ExecutionWorker.kubernetes_namespace == self.client.namespace,
        )

    def _binding_fields(self, worker_id: UUID) -> dict[str, Any]:
        return {
            "kubernetes_namespace": self.client.namespace,
            "kubernetes_job_name": worker_job_name(worker_id),
            # Remember the intended build even if startup fails before hello.
            # Admission still replaces it with the physical worker's report.
            "version": self.version,
        }

    async def reconcile_worker(self, worker_id: UUID, policy: KubernetesPolicy) -> None:
        async with get_db_context() as db:
            worker = await db.scalar(
                select(ExecutionWorker)
                .where(ExecutionWorker.id == worker_id)
                .with_for_update(skip_locked=True)
            )
            if worker is None or worker.kubernetes_job_name is None:
                return
            name = worker.kubernetes_job_name
            remote = await self.client.get_job(name)
            if remote is not None and worker.kubernetes_job_uid is not None:
                if remote["metadata"]["uid"] != worker.kubernetes_job_uid:
                    # Never adopt or delete a replacement that reused the name.
                    raise RuntimeError("Worker Job UID does not match its incarnation")
            now = _now()

            if worker.state == "lost":
                if remote is not None:
                    await self.client.delete_job(name, remote["metadata"]["uid"])
                return

            if worker.state == "reserved":
                if remote is None:
                    remote = await self.client.create_job(
                        build_worker_job_manifest(
                            worker.id,
                            self.settings,
                            self.spec,
                            memory_limit_bytes=launch_limit(
                                worker, policy.worker_memory_mib * 2**20
                            ),
                            sized=worker.resource_class == "sized",
                            execution_lane=worker.execution_lane,
                        )
                    )
                    # Whoever creates the Job launches it, whichever replica
                    # reserved the row. A Job adopted from an instance that
                    # died before binding it keeps no launcher: it runs that
                    # instance's image, not this one's.
                    worker.launched_by = self.id
                worker.kubernetes_job_uid = remote["metadata"]["uid"]
                worker.state = "starting"
                await db.commit()
                # The binding is durable; only now may the pod start.
                await self.client.start_job(name, remote["metadata"]["uid"])
                return

            reason = None
            if remote is None:
                reason = "job_missing"
            elif worker.state == "starting":
                if remote.get("spec", {}).get("suspend"):
                    await self.client.start_job(name, remote["metadata"]["uid"])
                elif now - worker.created_at >= timedelta(
                    seconds=self.settings.pool_start_timeout_seconds
                ):
                    reason = "start_timeout"
            elif self.reachability.steady() and await db.scalar(
                select(func.count())
                .select_from(ExecutionWorker)
                .where(ExecutionWorker.id == worker.id, silent_executor())
            ):
                reason = "heartbeat_lost"
            self._watch_drain(worker)
            if reason is None:
                await self._mark_evictable(worker)
            ended = LostRuns()
            if reason is not None:
                # Its runs teach their workflows a larger request only when
                # Kubernetes stopped the worker for memory.
                out_of_memory = (
                    remote is not None
                    and worker.kubernetes_job_uid is not None
                    and oom_killed(await self.client.list_pods(worker.kubernetes_job_uid))
                )
                self._lose(worker, "out_of_memory" if out_of_memory else reason, now)
                ended = await end_runs_of_lost_worker(db, worker.id, out_of_memory=out_of_memory)
            await db.commit()
        await announce_lost_runs(ended)

    async def _mark_evictable(self, worker: ExecutionWorker) -> None:
        """Let the cluster autoscaler move a ready worker only while it is idle.

        Kubernetes decides where pods run and which node to empty; this only
        says when moving this worker interrupts nothing. The pod is patched
        only when the answer changes.
        """
        now = time.monotonic()
        if worker.state == "ready" and worker.memory_reserved_bytes == 0:
            since = self._idle_since.setdefault(worker.id, now)
            evictable = now - since >= EVICTABLE_AFTER_IDLE_SECONDS
        else:
            self._idle_since.pop(worker.id, None)
            evictable = False
        if self._evictable.get(worker.id) == evictable or worker.kubernetes_job_uid is None:
            return
        for pod in await self.client.list_pods(worker.kubernetes_job_uid):
            await self.client.annotate_pod(
                pod["metadata"]["name"], {SAFE_TO_EVICT: "true" if evictable else "false"}
            )
        self._evictable[worker.id] = evictable


@dataclass(frozen=True)
class LaunchPlan:
    standard: int
    sized: list[tuple[UUID, int]]


async def waiting_capacity_rows(db: AsyncSession, platform_job_ids: list[UUID], now: datetime):
    return (await db.execute(
        select(
            WorkDispatch.id.label("id"),
            WorkDispatch.memory_request_bytes.label("memory_request_bytes"),
            WorkDispatch.created_at.label("created_at"),
            WorkDispatch.handler.label("handler"),
            WorkDispatch.handler_version,
            WorkDispatch.cloudflare_revision,
            Execution.organization_id.label("tenant_id"),
            literal(None).label("platform_job_type"),
        )
        .outerjoin(Execution, Execution.id == WorkDispatch.subject_id)
        .where(
            WorkDispatch.assigned_worker_id.is_(None),
            WorkDispatch.execution_lane == "normal",
            # Every advertised execution handler, including live
            # and backfill summaries, gains physical capacity.
            WorkDispatch.handler.in_(REMOTE_WORKER_HANDLER_NAMES),
        )
        .union_all(
            select(
                PlatformJob.id,
                PlatformJob.memory_required_bytes,
                PlatformJob.created_at,
                # Any physical worker runs local platform jobs.
                literal(None),
                literal(None),
                literal(None),
                literal(None),
                PlatformJob.job_type,
            ).where(
                PlatformJob.id.in_(platform_job_ids),
                PlatformJob.status == "queued",
                PlatformJob.execution_backend == "local",
                PlatformJob.available_at <= now,
            ),
            pending_service_statement(now).with_only_columns(
                ServiceDefinition.id,
                workflow_request_sql(Workflow),
                ServiceDefinition.updated_at,
                # Every physical executor supervises services.
                literal(None),
                literal(None),
                literal(None),
                literal(None),
                literal(None),
            ),
        )
        .order_by(text("created_at"))
    )).all()


@dataclass(frozen=True)
class Request:
    """Waiting work: a dispatch (with its handler) or a local platform job (None)."""

    id: UUID
    bytes: int
    handler: str | None = None
    handler_version: int | None = None
    cloudflare_revision: str | None = None
    tenant_id: UUID | None = None
    platform_job_type: str | None = None


@dataclass(frozen=True)
class FreeBudget:
    """An executor's free memory, capabilities, and limited handler slots."""

    bytes: int
    handlers: frozenset[str]
    handler_slots: dict[str, int] = field(default_factory=lambda: dict(HANDLER_CONCURRENCY_LIMITS))
    runtime_profile: str = LINUX_RUNTIME_PROFILE
    containers: bool = False

    def takes(self, request: Request) -> bool:
        if self.containers:
            if request.platform_job_type == SOLUTION_UPDATE_CHECK_JOB:
                return SOLUTION_GIT_CAPABILITY in self.handlers
            return containers_workflow_fits(
                request.handler, request.handler_version, request.cloudflare_revision, request.bytes,
            )
        if self.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and request.bytes > CLOUDFLARE_DEV_BUDGET_BYTES:
            return False
        return (request.handler is None or request.handler in self.handlers) and profile_matches(
            self.runtime_profile, request.handler, request.handler_version, request.cloudflare_revision,
        )

    def fits(self, request: Request) -> bool:
        return request.bytes <= self.bytes and self.takes(request)


def take_first_budget(
    item: Request, budgets: list[FreeBudget], rooms: list[int], slots: list[dict[str, int]],
    *, allow_cloudflare: bool = True,
) -> FreeBudget | None:
    for index, budget in enumerate(budgets):
        if not allow_cloudflare and budget.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
            continue
        handler = (
            SOLUTION_GIT_CAPABILITY
            if budget.containers and item.platform_job_type == SOLUTION_UPDATE_CHECK_JOB else item.handler
        )
        remaining = slots[index]
        available = None if handler is None else remaining.get(handler, 0 if handler in _LIMITED_HANDLERS else None)
        if rooms[index] >= item.bytes and budget.takes(item) and (available is None or available > 0):
            rooms[index] -= item.bytes
            if handler is not None and available is not None:
                remaining[handler] = available - 1
            return budget
    return None


def linux_handler_slots(worker: ExecutionWorker, held_handler: str | None, *, containers: bool = False) -> dict[str, int]:
    slots = dict(HANDLER_CONCURRENCY_LIMITS)
    if containers:
        slots[SOLUTION_GIT_CAPABILITY] = 1
    if worker.state not in ("reserved", "starting"):
        if worker.active_handler_counts is None:
            slots = dict.fromkeys(slots, 0)
        else:
            slots = {
                handler: max(0, limit - worker.active_handler_counts.get(handler, 0))
                for handler, limit in HANDLER_CONCURRENCY_LIMITS
            }
    if held_handler is not None and held_handler in slots:
        slots[held_handler] = max(0, slots[held_handler] - 1)
    return slots


def _cloudflare_budget(slots: int, *, used: int = 0, reserved_bytes: int = 0) -> FreeBudget:
    handler = CLOUDFLARE_WORKFLOW_HANDLER[0]
    return FreeBudget(
        max(0, slots * CLOUDFLARE_DEV_BUDGET_BYTES - reserved_bytes), frozenset({handler}),
        {handler: max(0, slots - used)}, CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    )


def _plan_launches(
    waiting: list[Request],
    free: list[FreeBudget],
    *,
    standard_budget: int,
    largest_budget: int,
    cold: bool = False,
    probe_largest: bool = False,
    new_declared: FreeBudget | None = None,
    new_containers: bool = False,
    active_tenants: set[UUID | None] | None = None,
    workflow_slots: WorkflowSlotAdmission | None = None,
) -> LaunchPlan:
    """Place requests (oldest first) into free budgets, then into new Jobs.

    First fit: each request takes the first budget that holds it, on an
    executor that can claim its handler. A request no standard Job can hold
    gets its own sized Job; one larger than the largest launchable budget is
    left for the reconciler to fail only after the largest size was measured.
    Linux cold planning chooses one initial resource. Cloudflare starts have
    declared workflow slots and no measured-memory probes.
    """
    bins = list(free)
    rooms = [budget.bytes for budget in bins]
    slots = [dict(budget.handler_slots) for budget in bins]
    limited = dict(HANDLER_CONCURRENCY_LIMITS)
    busy_tenants = set(active_tenants or ())
    if workflow_slots is not None:
        workflow_slots = replace(workflow_slots, occupied=workflow_slots.occupied.copy())

    def allows(tenant_id: UUID | None) -> bool:
        return workflow_slots.allows(tenant_id) if workflow_slots is not None else tenant_id not in busy_tenants

    def occupy(tenant_id: UUID | None) -> None:
        if workflow_slots is None:
            busy_tenants.add(tenant_id)
        else:
            workflow_slots.occupied[tenant_key(tenant_id)] += 1

    sized: list[tuple[UUID, int]] = []
    for item in waiting:
        dispatch_id, request = item.id, item.bytes
        placed = take_first_budget(item, bins, rooms, slots, allow_cloudflare=allows(item.tenant_id))
        if placed is not None:
            if placed.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
                occupy(item.tenant_id)
            continue
        new_handlers = frozenset({CLOUDFLARE_WORKFLOW_HANDLER[0], SOLUTION_GIT_CAPABILITY}) if new_containers else frozenset(REMOTE_WORKER_HANDLER_NAMES)
        new_budget = FreeBudget(standard_budget, new_handlers, containers=new_containers)
        if new_containers and not new_budget.takes(item):
            continue
        if new_declared is not None:
            if allows(item.tenant_id) and new_declared.fits(item):
                occupy(item.tenant_id)
                bins.append(new_declared)
                rooms.append(new_declared.bytes - request)
                handler = CLOUDFLARE_WORKFLOW_HANDLER[0]
                slots.append({handler: new_declared.handler_slots[handler] - 1})
            continue
        if cold:
            return (LaunchPlan(standard=1, sized=[]) if request <= standard_budget
                    else LaunchPlan(standard=0, sized=[(dispatch_id, request)]))
        if request > standard_budget:
            if request <= largest_budget or probe_largest:
                sized.append((dispatch_id, request))
            continue
        bins.append(new_budget)
        rooms.append(standard_budget - request)
        new_slots = dict(limited)
        if new_containers:
            new_slots[SOLUTION_GIT_CAPABILITY] = int(item.platform_job_type != SOLUTION_UPDATE_CHECK_JOB)
        if item.handler in limited:
            new_slots[item.handler] -= 1
        slots.append(new_slots)
    return LaunchPlan(standard=len(bins) - len(free), sized=sized)


async def _publish_launchable(largest_budget: int | None, generation: int) -> None:
    """Tell the dispatch reconciler the largest budget this controller can launch."""
    from src.core.cache.redis_client import get_redis

    async with get_redis() as redis:
        # Empty means a managed provider is present but its capacity is not
        # measured yet. It is distinct from no active capacity controller.
        async with redis.pipeline(transaction=True) as pipe:
            pipe.set(LAUNCHABLE_KEY, "" if largest_budget is None else str(largest_budget),
                     ex=LAUNCHABLE_TTL_SECONDS)
            value = "" if largest_budget is None else str(largest_budget)
            pipe.set(LAUNCHABLE_GENERATION_KEY, f"{generation}:{value}", ex=LAUNCHABLE_TTL_SECONDS)
            await pipe.execute()


async def _publish_live(controller_id: UUID, version: str) -> LiveControllers:
    """Record this instance as live and read every instance that is."""
    from src.core.cache.redis_client import get_redis

    async with get_redis() as redis:
        # Redis's clock, so every instance's "at" compares on one clock.
        seconds, microseconds = await redis.time()  # type: ignore[misc]
        now = seconds + microseconds / 1_000_000
        await redis.hset(  # type: ignore[misc]
            CONTROLLERS_KEY, str(controller_id), json.dumps({"version": version, "at": now})
        )
        entries = await redis.hgetall(CONTROLLERS_KEY)  # type: ignore[misc]
        ids: set[UUID] = set()
        versions: set[str] = set()
        expired: list[str] = []
        for key, value in entries.items():
            entry = json.loads(value)
            if now - entry["at"] > HEARTBEAT_LOST_AFTER.total_seconds():
                expired.append(key)
                continue
            ids.add(UUID(key))
            versions.add(entry["version"])
        if expired:
            await redis.hdel(CONTROLLERS_KEY, *expired)  # type: ignore[misc]
    return LiveControllers(ids=frozenset(ids), versions=frozenset(versions))


async def _withdraw_live(controller_id: UUID) -> None:
    """A stopping controller is gone at once, not after the lost window."""
    from src.core.cache.redis_client import get_redis

    async with get_redis() as redis:
        await redis.hdel(CONTROLLERS_KEY, str(controller_id))  # type: ignore[misc]


async def worker_pool_loop(stop_event: asyncio.Event, settings: Any | None = None) -> None:
    settings = settings or get_settings()
    # Pool work waits visibly (and fails at its claim deadline) instead of
    # taking down the process that hosts this loop.
    try:
        client = KubernetesJobClient(settings)
    except KubernetesConfigurationError as exc:
        logger.exception("Reusable worker controller disabled: %s", exc)
        await stop_event.wait()
        return
    try:
        spec = await resolve_job_spec_when_ready(
            client, settings, stop_event, "Reusable worker controller"
        )
        if spec is None:
            return
        controller = WorkerPoolController(client, settings, spec)
        wake = asyncio.Event()
        listener = asyncio.create_task(
            listen_for_pool_wake(
                settings.redis_url,
                {
                    **{family: wake for family in DISPATCH_WAKE_FAMILIES},
                    WAKE_PLATFORM_JOB: wake,
                    WAKE_SERVICE: wake,
                },
            ),
            name="worker-pool-controller-wake",
        )
        try:
            while not stop_event.is_set():
                started = time.monotonic()
                try:
                    if await controller.reachability.check():
                        await controller.reconcile()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        "Reusable worker reconciliation failed (%s): %s",
                        type(exc).__name__,
                        exc,
                    )
                await wait_for_next_pass(
                    stop_event, wake, since=started, spacing=RECONCILE_SECONDS, sweep=SWEEP_SECONDS
                )
        finally:
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)
            try:
                await _withdraw_live(controller.id)
            except Exception:
                logger.warning("Could not withdraw the worker controller", exc_info=True)
    finally:
        await client.close()


__all__ = ["LiveControllers", "WorkerCapacityController", "WorkerPoolController", "worker_pool_loop"]
