"""The reusable worker's side of the pool protocol (all fenced in PostgreSQL)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import String, case, cast, exists, false, func, null, select, text, tuple_, update

from src.config import get_settings
from src.core.database import get_db_context, get_liveness_db_context
from src.models.orm.agent_runs import AgentRun
from src.models.orm.cloudflare_capacity import CloudflareWorkerReservation
from src.models.orm.execution_workers import MANAGED_WORKER_FORMS, ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.models.orm.work_dispatches import WorkDispatch
from src.services.kubernetes_execution import load_kubernetes_policy
from src.services.execution.summary_dispatch import LINUX_ONLY_SUMMARY
from src.services.worker_pool.cloudflare_admission import cloudflare_claim_allowed
from src.services.worker_pool.cloudflare_containers_policy import CONTAINERS_SCOPE, container_identity_matches
from src.services.worker_pool.cloudflare_slots import (
    QueuePreparation,
    WorkflowSlotTicket,
    adoptable_workflow_slot,
    count_released_prewarms,
    load_workflow_slot_admission,
    occupy_workflow_slot,
    stage_queue_preparations,
    tenant_key,
)
from src.services.worker_pool.eligibility import cloudflare_active_claims, cloudflare_dispatch_sql, containers_dispatch_sql
from src.worker.remote.fencing import current_remote_lease_ref, fence_remote_owner
from src.worker.remote.protocol import (
    CLOUDFLARE_DEV_BUDGET_BYTES,
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    CLOUDFLARE_SUMMARY_HANDLER,
    EXECUTION_HANDLERS,
    LINUX_RUNTIME_PROFILE,
)

# Shared with the capacity controller: launches and idle drains serialize.
CAPACITY_LOCK = "bifrost:worker-pool:capacity"
# Work that has waited this long without fitting anywhere stops executors
# that could hold it from claiming newer work, so it is never starved.
HEAD_OF_LINE_WAIT = timedelta(seconds=30)

# Handlers this worker build can run, by version. A dispatch for any other
# (handler, version) stays for a compatible worker.
SUPPORTED_HANDLERS = EXECUTION_HANDLERS
REMOTE_WORKER_HANDLERS = EXECUTION_HANDLERS
# Capacity stores names once; claim eligibility still uses every negotiated version.
REMOTE_WORKER_HANDLER_NAMES: tuple[str, ...] = tuple(
    dict.fromkeys(handler for handler, _version in REMOTE_WORKER_HANDLERS)
)


@dataclass(frozen=True)
class PoolClaim:
    dispatch_id: UUID
    handler: str
    message: dict[str, Any]
    subject_id: UUID
    memory_request_bytes: int
    # When the work was accepted: work handed back keeps its place in line.
    queued_at: datetime
    # A workflow run is claimed (PENDING -> RUNNING) in the same transaction
    # as its dispatch; this is its context. None when the consumer still has
    # to claim (agent runs) or when the run had already moved on.
    claimed: dict[str, Any] | None = None
    cloudflare_revision: str | None = None
    tenant_id: UUID | None = None
    workflow_slot: WorkflowSlotTicket | None = None
    queue_preparations: tuple[QueuePreparation, ...] = ()


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def admit_worker(
    incarnation_id: UUID, *, job_uid: str, pod_uid: str, version: str | None,
    execution_lane: str = "normal",
) -> UUID | None:
    """Bind this pod to its incarnation once; None if it was refused.

    Only the first pod of the UID-bound Job can admit: a second start of the
    same Job, or a pod of another Job, is refused. ``version`` is the build
    the pod runs (not the admitting API's), which decides when an upgrade
    replaces it; a pod that reports none counts as outdated.
    """
    now = _now()
    async with get_db_context() as db:
        if not await fence_remote_owner(db, for_update=True):
            return None
        return await db.scalar(
            update(ExecutionWorker)
            .where(
                ExecutionWorker.id == incarnation_id,
                ExecutionWorker.state == "starting",
                ExecutionWorker.kubernetes_job_uid == job_uid,
                ExecutionWorker.kubernetes_pod_uid.is_(None),
                ExecutionWorker.execution_lane == execution_lane,
            )
            .values(
                state="ready",
                kubernetes_pod_uid=pod_uid,
                version=version,
                ready_at=now,
                heartbeat_at=func.now(),
                last_claim_at=now,
            )
            .returning(ExecutionWorker.id)
        )


async def admit_external_worker(
    incarnation_id: UUID, *, resource_uid: str, launcher_id: UUID, version: str | None, hostname: str,
    container_release: str | None = None,
) -> UUID | None:
    """Bind the first physical process to a prepared external incarnation."""
    async with get_db_context() as db:
        if not await fence_remote_owner(db, for_update=True):
            return None
        return await db.scalar(
            update(ExecutionWorker)
            .where(
                ExecutionWorker.id == incarnation_id,
                ExecutionWorker.form == "external",
                ExecutionWorker.runtime_profile != CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
                ExecutionWorker.state == "starting",
                container_identity_matches(incarnation_id, container_release),
                ExecutionWorker.capacity_resource_uid == resource_uid,
                ExecutionWorker.remote_launcher_id.is_(None),
                ExecutionWorker.version.is_not(None),
                ExecutionWorker.version == version,
            )
            .values(
                state="ready", remote_launcher_id=launcher_id, version=version, hostname=hostname,
                ready_at=_now(), heartbeat_at=func.now(), last_claim_at=_now(),
            )
            .returning(ExecutionWorker.id)
        )


async def register_static_worker(
    *,
    hostname: str,
    supported_handlers: list[str] | None = None,
    incarnation_id: UUID | None = None,
    version: str | None = None,
    execution_lane: str = "normal",
) -> UUID:
    """A long-running executor joins the pool as ``starting``.

    Its pool can take a minute or more to start, so it offers no capacity
    (and claims nothing) until ``mark_static_ready`` reports its measured
    budget. A remote physical launcher may supply its immutable incarnation
    UUID; this always creates a new row and never adopts or overwrites one.
    ``version`` is the executor's reported build; missing reports stay unknown.
    """
    worker = ExecutionWorker(
        **({"id": incarnation_id} if incarnation_id is not None else {}),
        form="static",
        state="starting",
        resource_class="standard",
        hostname=hostname,
        supported_handlers=list(dict.fromkeys(
            supported_handlers if supported_handlers is not None else REMOTE_WORKER_HANDLER_NAMES
        )),
        version=version,
        execution_lane=execution_lane,
        heartbeat_at=func.now(),
    )
    async with get_db_context() as db:
        db.add(worker)
        await db.flush()
        worker_id = worker.id
    return worker_id


async def mark_static_ready(incarnation_id: UUID, *, memory_budget_bytes: int) -> bool:
    """``starting → ready`` with the budget measured once the pool is up."""
    now = _now()
    async with get_db_context() as db:
        if not await fence_remote_owner(db, for_update=True):
            return False
        ready = await db.scalar(
            update(ExecutionWorker)
            .where(ExecutionWorker.id == incarnation_id, ExecutionWorker.state == "starting")
            .values(
                state="ready",
                memory_budget_bytes=memory_budget_bytes,
                ready_at=now,
                last_claim_at=now,
            )
            .returning(ExecutionWorker.id)
        )
    return ready is not None


async def report_worker_handlers(incarnation_id: UUID, handlers: list[str]) -> bool:
    """Persist negotiated physical capabilities under the current owner fence."""
    async with get_db_context() as db:
        if not await fence_remote_owner(db, for_update=True):
            return False
        return await db.scalar(
            update(ExecutionWorker)
            .where(
                ExecutionWorker.id == incarnation_id,
                ExecutionWorker.state.in_(("starting", "ready", "draining")),
            )
            .values(supported_handlers=list(dict.fromkeys(handlers)))
            .returning(ExecutionWorker.id)
        ) is not None


async def claim_next(
    incarnation_id: UUID,
    handlers: tuple[tuple[str, int], ...] = SUPPORTED_HANDLERS,
    *,
    fits_bytes: int,
    budget_bytes: int,
    reserve_claim: Callable[[PoolClaim], None] | None = None,
    release_reservation: Callable[[PoolClaim], None] | None = None,
    cloudflare_autonomous_agents: frozenset[UUID] = frozenset(),
    cloudflare_chat_endpoint: str | None = None,
    queue_prewarm_slots: int = 0,
) -> PoolClaim | None:
    """Assign the oldest unassigned dispatch this worker can take now.

    ``handlers`` narrows the claim to the (handler, version) pairs with free
    capacity on this worker; it is always a subset of what the build supports.
    Only work whose memory request fits ``fits_bytes`` (the free budget) is
    taken, except that a worker launched for one dispatch takes it first
    whatever a Linux worker measured: it exists for that work, and the
    real-memory gate and the dangerous-memory stop still apply. Declared
    capacity always requires a fit, including reserved dispatches. When
    older work that fits ``budget_bytes`` (but not the free budget) has waited past
    HEAD_OF_LINE_WAIT, nothing newer is taken until it fits. When supplied,
    ``reserve_claim`` runs after the assignment is fenced but before the
    transaction commits; ``release_reservation`` undoes that local hold if
    the transaction fails.
    """
    pairs = [pair for pair in handlers if pair in SUPPORTED_HANDLERS]
    active_cloudflare = cloudflare_active_claims().subquery()
    managed_workers = CloudflareWorkerReservation.__table__.alias()
    managed = exists().where(managed_workers.c.worker_id == incarnation_id)
    dispatch_tenant = select(Execution.organization_id).where(
        Execution.id == WorkDispatch.subject_id,
    ).correlate(WorkDispatch).scalar_subquery()
    occupied = select(func.count()).select_from(active_cloudflare).where(
        active_cloudflare.c.worker_id == incarnation_id,
    ).scalar_subquery()
    autonomous = (AgentRun.trigger_type != "chat") & AgentRun.conversation_id.is_(None) & ~WorkDispatch.payload.contains(
        {"trigger_type": "chat"},
    )
    chat = (AgentRun.trigger_type == "chat") & AgentRun.conversation_id.is_not(None) & (
        WorkDispatch.payload["cloudflare_chat_queued_sequence"].as_integer() > 0
    )
    eligible_agent_run = (
        select(AgentRun.id).where(
            AgentRun.id == WorkDispatch.subject_id,
            AgentRun.agent_id.in_(cloudflare_autonomous_agents),
            autonomous | chat if cloudflare_chat_endpoint else autonomous,
            AgentRun.parent_run_id.is_(None),
            AgentRun.event_delivery_id.is_(None),
            func.coalesce(func.jsonb_typeof(AgentRun.output_schema), "null") == "null",
            AgentRun.status == "queued",
        ).correlate(WorkDispatch).exists()
    )
    # Managed Linux reservations are provisionable only after their dedicated
    # readiness and send path exists. A generic Linux hello grants no claim.
    managed_container = exists().where(
        managed_workers.c.worker_id == incarnation_id, managed_workers.c.container_reserved_at.is_not(None),
    )
    linux_eligible = (ExecutionWorker.runtime_profile == LINUX_RUNTIME_PROFILE) & ~managed_container
    containers_scope = getattr(get_settings(), "cloudflare_containers_scope", None)
    if containers_scope is not None:
        linux_eligible = linux_eligible & (
            ExecutionWorker.capacity_scope.is_distinct_from(containers_scope) | containers_dispatch_sql()
        )
    profile_eligible = exists().where(
        ExecutionWorker.id == incarnation_id,
        linux_eligible
        | (
            (ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE)
            & (
                (cloudflare_dispatch_sql() & (managed | ~exists().where(
                    active_cloudflare.c.execution_id.is_not(None),
                    active_cloudflare.c.tenant_id.is_not_distinct_from(dispatch_tenant),
                )))
                | (
                    (WorkDispatch.handler == CLOUDFLARE_SUMMARY_HANDLER[0])
                    & (WorkDispatch.handler_version == CLOUDFLARE_SUMMARY_HANDLER[1])
                    & ~WorkDispatch.payload.contains({LINUX_ONLY_SUMMARY: True})
                    & (WorkDispatch.memory_request_bytes <= CLOUDFLARE_DEV_BUDGET_BYTES)
                    & ExecutionWorker.capacity_scope.is_(None)
                )
                | (
                    (WorkDispatch.handler == "agent.run")
                    & (WorkDispatch.handler_version == 1)
                    & ~WorkDispatch.payload.contains({"cloudflare_agent_linux_only": True})
                    & (WorkDispatch.memory_request_bytes <= CLOUDFLARE_DEV_BUDGET_BYTES)
                    & ExecutionWorker.capacity_scope.is_(None)
                    & eligible_agent_run
                )
            )
            & (occupied * CLOUDFLARE_DEV_BUDGET_BYTES < func.coalesce(
                ExecutionWorker.memory_limit_bytes, CLOUDFLARE_DEV_BUDGET_BYTES,
            ))
        ),
    )
    reserved_for = (
        select(ExecutionWorker.reserved_for_dispatch_id)
        .where(ExecutionWorker.id == incarnation_id)
        .scalar_subquery()
    )
    container_worker = managed_container | exists().where(
        ExecutionWorker.id == incarnation_id,
        ExecutionWorker.capacity_scope.in_(
            [CONTAINERS_SCOPE, containers_scope] if containers_scope is not None else [CONTAINERS_SCOPE],
        ),
    )
    unassigned = (
        WorkDispatch.assigned_worker_id.is_(None),
        WorkDispatch.execution_lane == select(ExecutionWorker.execution_lane)
        .where(ExecutionWorker.id == incarnation_id).scalar_subquery(),
        tuple_(WorkDispatch.handler, WorkDispatch.handler_version).in_(pairs),
        profile_eligible,
        ~container_worker | (WorkDispatch.id == reserved_for),
    )
    worker_supports_handler = exists().where(
        ExecutionWorker.id == incarnation_id,
        ExecutionWorker.supported_handlers.op("?")(WorkDispatch.handler),
    )
    declared_capacity = exists().where(
        ExecutionWorker.id == incarnation_id,
        ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    )
    fits = (WorkDispatch.memory_request_bytes <= fits_bytes) | (
        (WorkDispatch.id == reserved_for)
        & (WorkDispatch.handler == "workflow.execute")
        & ~declared_capacity
    )
    candidate = (
        select(
            WorkDispatch.id, WorkDispatch.created_at,
            declared_capacity.label("opaque"), dispatch_tenant.label("tenant_id"),
            WorkDispatch.subject_id, WorkDispatch.payload["trigger_type"].as_string().label("trigger_type"),
        )
        .where(
            *unassigned,
            worker_supports_handler,
            fits,
        )
        .order_by(
            (WorkDispatch.id == reserved_for).is_(True).desc(),
            WorkDispatch.created_at.asc(),
            WorkDispatch.id.asc(),
        )
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    ready = exists().where(
        ExecutionWorker.id == incarnation_id,
        ExecutionWorker.state == "ready",
        ExecutionWorker.supported_handlers.op("?")(WorkDispatch.handler),
    )
    claim: PoolClaim | None = None
    reserved = False
    try:
        for attempt in range(2):
            async with get_db_context() as db:
                owner = current_remote_lease_ref()
                if owner is None or owner.lease.worker_id != incarnation_id or (
                    owner.lease.declared_slots > 0
                ):
                    if not await cloudflare_claim_allowed(db, incarnation_id):
                        return None
                if not await fence_remote_owner(db, for_update=True):
                    return None
                slot_admission = await load_workflow_slot_admission(db, incarnation_id)
                eligible_candidate = candidate
                slot_filter = ()
                released_prewarms = 0
                prewarm_ordinal = null()
                if slot_admission is not None:
                    tenant = func.coalesce(cast(dispatch_tenant, String), "platform")
                    released_prewarms = await count_released_prewarms(db, incarnation_id)
                    room = tenant.not_in(slot_admission.blocked_tenants) if not slot_admission.full else false()
                    if released_prewarms:
                        if (await db.scalar(select(occupied)) or 0) + released_prewarms >= (
                            budget_bytes // CLOUDFLARE_DEV_BUDGET_BYTES
                        ):
                            room = false()
                        prewarm_ordinal = adoptable_workflow_slot(slot_admission, incarnation_id, tenant)
                        room = room | prewarm_ordinal.is_not(None)
                    elif slot_admission.full:
                        return None
                    slot_filter = (room,)
                    eligible_candidate = candidate.where(*slot_filter)
                # Capture adoption while the dispatch is still pending. Claiming
                # the pinned dispatch consumes its queue row in this transaction.
                picked = (await db.execute(
                    eligible_candidate.add_columns(prewarm_ordinal.label("prewarm_ordinal")),
                )).one_or_none()
                if picked is None:
                    return None
                if picked.opaque and picked.trigger_type == "chat" and cloudflare_chat_endpoint is not None:
                    from src.models.orm.agents import Agent
                    from src.services.execution.cloudflare_chat import load_chat_input

                    run = await db.get(AgentRun, picked.subject_id)
                    agent = await db.get(Agent, run.agent_id) if run is not None and run.agent_id is not None else None
                    try:
                        if run is None or agent is None:
                            raise ValueError("Workers chat requires a configured agent")
                        await load_chat_input(db, run, agent, cloudflare_chat_endpoint)
                    except ValueError:
                        await db.execute(update(WorkDispatch).where(WorkDispatch.id == picked.id).values(
                            payload=WorkDispatch.payload.op("||")({"cloudflare_agent_linux_only": True}),
                        ))
                        return None
                assignment = [WorkDispatch.id == picked.id, ready, profile_eligible]
                if picked.opaque and slot_admission is None:
                    # The dispatch row is locked, but another launcher can pick a
                    # different dispatch for this tenant. Recheck eligibility in
                    # the UPDATE's fresh snapshot after taking the tenant lock.
                    await db.execute(
                        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                        {"key": f"bifrost:worker-pool:cloudflare-claim:{picked.tenant_id or 'platform'}"},
                    )
                if picked.opaque:
                    assignment.append(dispatch_tenant.is_not_distinct_from(picked.tenant_id))
                blocked = await db.scalar(
                    select(
                        exists().where(
                            *unassigned,
                            *slot_filter,
                            WorkDispatch.created_at < picked.created_at,
                            WorkDispatch.created_at < _now() - HEAD_OF_LINE_WAIT,
                            WorkDispatch.memory_request_bytes > fits_bytes,
                            WorkDispatch.memory_request_bytes <= budget_bytes,
                        )
                    )
                )
                if blocked:
                    return None
                row = (
                    await db.execute(
                        update(WorkDispatch)
                        .where(*assignment)
                        .values(assigned_worker_id=incarnation_id)
                        .returning(
                            WorkDispatch.id.label("dispatch_id"),
                            WorkDispatch.handler.label("handler"),
                            WorkDispatch.message.label("message"),
                            WorkDispatch.family.label("dispatch_family"),
                            WorkDispatch.subject_id.label("subject"),
                            WorkDispatch.memory_request_bytes.label("memory_request_bytes"),
                            WorkDispatch.created_at.label("queued_at"),
                            WorkDispatch.cloudflare_revision,
                        )
                    )
                ).one_or_none()
                if row is None:
                    if picked.opaque and attempt == 0:
                        # Leave this transaction before taking another tenant's
                        # lock. The retry still fences ownership and queue order.
                        other_tenant = dispatch_tenant.is_distinct_from(picked.tenant_id)
                        unassigned = (*unassigned, other_tenant)
                        candidate = candidate.where(other_tenant)
                        continue
                    return None
                claim_id = uuid4()
                db.add(
                    ExecutionWorkerClaim(
                        id=claim_id,
                        worker_id=incarnation_id,
                        family=row.dispatch_family,
                        subject_id=row.subject,
                    )
                )
                claimed = None
                workflow_slot = None
                queue_preparations = ()
                if row.handler == "workflow.execute":
                    from src.services.execution.workflow_dispatch import claim_workflow_dispatch

                    # One transaction for assignment and the run's own claim. A run
                    # that moved on (cancelled) leaves claimed None: the consumer then
                    # finds nothing to claim and discards it, waking a sync caller.
                    claimed = await claim_workflow_dispatch(
                        db, dispatch_id=row.dispatch_id, execution_id=row.subject
                    )
                    if claimed is not None and slot_admission is not None:
                        workflow_slot = await occupy_workflow_slot(
                            db, slot_admission, incarnation_id, picked.tenant_id,
                            row.subject, row.dispatch_id, claim_id, prewarm_ordinal=picked.prewarm_ordinal,
                        )
                        if queue_prewarm_slots and slot_admission.policy.queue_prewarm_per_tenant:
                            if not workflow_slot.prewarming:
                                slot_admission.occupied[tenant_key(picked.tenant_id)] += 1
                            # Flush the workflow claim before reading the live
                            # launcher seats. An adopted preparation reuses a seat.
                            await db.flush()
                            queue_preparations = await stage_queue_preparations(
                                db, slot_admission, incarnation_id, picked.tenant_id,
                                launcher_slots=queue_prewarm_slots,
                                occupied_launcher_slots=(await db.scalar(select(occupied)) or 0)
                                + released_prewarms - int(workflow_slot.prewarming),
                            )
                await db.execute(
                    update(ExecutionWorker)
                    .where(ExecutionWorker.id == incarnation_id)
                    .values(
                        last_claim_at=_now(), reserved_for_dispatch_id=None,
                        # Opaque settlement can beat the next heartbeat. Publish
                        # the busy slot atomically with its durable claim.
                        active_runs=case(
                            (
                                ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
                                ExecutionWorker.active_runs + 1,
                            ),
                            else_=ExecutionWorker.active_runs,
                        ),
                        memory_reserved_bytes=case(
                            (
                                ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
                                ExecutionWorker.memory_reserved_bytes + row.memory_request_bytes,
                            ),
                            else_=ExecutionWorker.memory_reserved_bytes,
                        ),
                    )
                )
                claim = PoolClaim(
                    dispatch_id=row.dispatch_id,
                    handler=row.handler,
                    message={
                        **row.message,
                        **({"claim_id": str(claim_id)} if (
                            row.dispatch_family == "agent_summary"
                            or (row.dispatch_family == "agent_run" and picked.opaque)
                        ) else {}),
                    },
                    subject_id=row.subject,
                    memory_request_bytes=row.memory_request_bytes,
                    queued_at=row.queued_at,
                    claimed=dict(claimed) if claimed is not None else None,
                    cloudflare_revision=row.cloudflare_revision,
                    tenant_id=picked.tenant_id if picked.opaque else None,
                    workflow_slot=workflow_slot,
                    queue_preparations=queue_preparations,
                )
                if reserve_claim is not None:
                    reserve_claim(claim)
                    reserved = True
                # Keep commit inside the reservation's cleanup guard so a failed
                # transaction cannot retain a local admission hold.
                await db.commit()
                break
    except BaseException:
        if reserved and claim is not None and release_reservation is not None:
            release_reservation(claim)
        raise
    return claim


async def heartbeat_worker(
    incarnation_id: UUID,
    *,
    active_runs: int | Callable[[], int],
    runs_handled: int,
    package_generation: int | None,
    memory_pressured: bool,
    memory_budget_bytes: int | None,
    memory_reserved_bytes: int | Callable[[], int],
    active_handler_counts: dict[str, int] | None = None,
) -> str | None:
    """Renew liveness; return the state the controller sees (None if gone).

    A static executor heartbeats while ``starting`` too: its pool start can
    outlast the window after which a silent executor is lost. On the
    executor's own heartbeat connection, so a heartbeat never waits behind
    the runs' queries. Missing handler occupancy is stored as unknown, never
    inferred to mean that constrained handler slots are idle."""
    async with get_liveness_db_context() as db:
        if not await fence_remote_owner(db, for_update=True):
            return None
        return await db.scalar(
            update(ExecutionWorker)
            .where(
                ExecutionWorker.id == incarnation_id,
                ExecutionWorker.state.in_(("starting", "ready", "draining")),
            )
            .values(
                # The database's clock: liveness is judged against it.
                heartbeat_at=func.now(),
                # Sample opaque occupancy under the same row lock as claims.
                active_runs=active_runs() if callable(active_runs) else active_runs,
                active_handler_counts=active_handler_counts,
                runs_handled=runs_handled,
                package_generation=package_generation,
                memory_pressured=memory_pressured,
                memory_budget_bytes=memory_budget_bytes,
                memory_reserved_bytes=(
                    memory_reserved_bytes() if callable(memory_reserved_bytes) else memory_reserved_bytes
                ),
            )
            .returning(ExecutionWorker.state)
        )


async def begin_drain(incarnation_id: UUID, reason: str) -> bool:
    """``ready → draining``; after this the worker can never claim again."""
    async with get_db_context() as db:
        if not await fence_remote_owner(db, for_update=True):
            return False
        drained = await db.scalar(
            update(ExecutionWorker)
            .where(ExecutionWorker.id == incarnation_id, ExecutionWorker.state == "ready")
            .values(state="draining", end_reason=reason)
            .returning(ExecutionWorker.id)
        )
    return drained is not None


async def begin_idle_drain(incarnation_id: UUID) -> bool:
    """Drain an idle job worker unless it is needed for the warm minimum.

    Serialized with launches by the capacity lock, so idle workers never
    collectively drop below Warm workers, read from Settings -> Kubernetes
    under the lock (lowering it lets running idle workers drain).
    """
    async with get_db_context() as db:
        await db.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
            {"key": CAPACITY_LOCK},
        )
        # Keep the capacity lock before the worker lock: the controller uses
        # this order while changing worker state, so reversing it can deadlock.
        if not await fence_remote_owner(db, for_update=True):
            return False
        policy = await load_kubernetes_policy(db)
        # A sized Job always drains once idle: only standard Jobs make up the
        # warm minimum.
        worker = await db.execute(
            select(
                ExecutionWorker.resource_class, ExecutionWorker.execution_lane, ExecutionWorker.runtime_profile,
                ExecutionWorker.active_runs, ExecutionWorker.memory_reserved_bytes, ExecutionWorker.capacity_scope,
            ).where(ExecutionWorker.id == incarnation_id)
        )
        worker_row = worker.one_or_none()
        if worker_row is None:
            return False
        if worker_row.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and (
            worker_row.active_runs or worker_row.memory_reserved_bytes
            or await count_released_prewarms(db, incarnation_id) or await db.scalar(
                cloudflare_active_claims().where(ExecutionWorkerClaim.worker_id == incarnation_id).exists().select()
            )
        ):
            return False
        sized = worker_row.resource_class == "sized"
        keep_ready = (
            policy.reserved_fast_path_workers
            if worker_row.execution_lane == "fast_path"
            else policy.warm_workers
        )
        settings = get_settings()
        containers_scope = getattr(settings, "cloudflare_containers_scope", None)
        containers_worker = containers_scope is not None and worker_row.capacity_scope == containers_scope
        scoped_cloudflare = (
            worker_row.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE and worker_row.capacity_scope is not None
        )
        if not sized and not containers_worker and not scoped_cloudflare:
            ready = select(func.count(ExecutionWorker.id)).where(
                ExecutionWorker.form.in_(MANAGED_WORKER_FORMS),
                ExecutionWorker.resource_class == "standard",
                ExecutionWorker.state == "ready",
                ExecutionWorker.execution_lane == worker_row.execution_lane,
            )
            if worker_row.execution_lane == "normal":
                ready = ready.where(
                    (ExecutionWorker.runtime_profile != CLOUDFLARE_PYTHON_RUNTIME_PROFILE)
                    | ExecutionWorker.capacity_scope.is_(None)
                )
                if containers_scope is not None:
                    ready = ready.where(ExecutionWorker.capacity_scope.is_distinct_from(containers_scope))
            ready_jobs = await db.scalar(ready) or 0
            if ready_jobs <= keep_ready:
                return False
        drained = await db.scalar(
            update(ExecutionWorker)
            .where(ExecutionWorker.id == incarnation_id, ExecutionWorker.state == "ready")
            .values(state="draining", end_reason="idle")
            .returning(ExecutionWorker.id)
        )
    return drained is not None


async def retire_worker(incarnation_id: UUID, *, runs_handled: int) -> None:
    async with get_db_context() as db:
        if not await fence_remote_owner(db, for_update=True):
            return
        await db.execute(
            update(ExecutionWorker)
            .where(
                ExecutionWorker.id == incarnation_id,
                ExecutionWorker.state.in_(("ready", "draining")),
            )
            .values(
                state="retired",
                active_runs=0,
                memory_reserved_bytes=0,
                runs_handled=runs_handled,
                ended_at=_now(),
            )
        )


__all__ = [
    "PoolClaim",
    "REMOTE_WORKER_HANDLERS",
    "REMOTE_WORKER_HANDLER_NAMES",
    "SUPPORTED_HANDLERS",
    "admit_worker",
    "register_static_worker",
    "mark_static_ready",
    "begin_drain",
    "begin_idle_drain",
    "claim_next",
    "heartbeat_worker",
    "retire_worker",
]
