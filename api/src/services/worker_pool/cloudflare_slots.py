"""Durable, fenced ownership of managed Cloudflare workflow engines."""

from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from shared.models import CloudflareWorkersPolicy
from sqlalchemy import (
    String,
    and_,
    case,
    cast,
    delete,
    exists,
    func,
    or_,
    select,
    tuple_,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import ScalarSelect

from src.models.enums import ExecutionStatus
from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareWorkerReservation,
    CloudflareWorkflowSlot,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.executions import Execution
from src.models.orm.work_dispatches import WorkDispatch
from src.services.worker_pool.eligibility import (
    cloudflare_dispatch_sql,
    managed_workflow_fits,
)

PREPARE_SECONDS = 20


@dataclass(frozen=True)
class WorkflowSlotTicket:
    ordinal: int
    fence: UUID
    prewarming: bool = False

    def workflow_route(self, execution_id: UUID | str) -> str:
        return f"workflow:{execution_id}:{self.fence}"


@dataclass(frozen=True)
class QueuePreparation:
    ordinal: int
    fence: UUID
    execution_id: UUID
    prepare_by: datetime
    warm_until: datetime


@dataclass(frozen=True)
class WorkflowSlotAdmission:
    installation_id: UUID
    policy: CloudflareWorkersPolicy
    occupied: Counter[str]

    @property
    def full(self) -> bool:
        return self.occupied.total() >= self.policy.installation_slots

    @property
    def blocked_tenants(self) -> frozenset[str]:
        last_slot = self.occupied.total() == self.policy.installation_slots - 1
        return frozenset(
            tenant for tenant, count in self.occupied.items()
            if last_slot or count >= self.policy.slots_per_tenant
        )

    def allows(self, tenant_id: UUID | None) -> bool:
        return not self.full and tenant_key(tenant_id) not in self.blocked_tenants

    def prewarm_count(self, tenant: str, *, prepared: int, launcher_free: int) -> int:
        if not self.policy.linger_seconds:
            return 0
        return max(0, min(
            self.policy.queue_prewarm_per_tenant - prepared,
            self.policy.slots_per_tenant - self.occupied[tenant],
            self.policy.installation_slots - self.occupied.total() - 1,
            launcher_free,
        ))


def tenant_key(tenant_id: UUID | str | None) -> str:
    return str(tenant_id) if tenant_id is not None else "platform"


async def read_workflow_slot_admission(
    db: AsyncSession, installation_id: UUID, policy: CloudflareWorkersPolicy,
) -> WorkflowSlotAdmission:
    slot = CloudflareWorkflowSlot
    reservation = CloudflareWorkerReservation
    # One physical seat per incarnation, including starting, idle and
    # uncertain-stop instances. An attached dispatch ticket uses that seat.
    occupied_seats = select(slot.tenant.label("tenant")).where(
        slot.installation_id == installation_id,
        or_(and_(slot.released_at.is_(None), ~_stale_unsent()), _live_prewarm()),
    ).union_all(
        select(reservation.container_tenant).where(
            reservation.installation_id == installation_id, reservation.container_reserved_at.is_not(None),
            reservation.container_stopped_at.is_(None),
            ~exists().where(
                slot.worker_id == reservation.worker_id, slot.released_at.is_(None), ~_stale_unsent(),
            ).correlate(reservation),
        )
    ).subquery()
    counts = (await db.execute(
        select(occupied_seats.c.tenant, func.count()).group_by(occupied_seats.c.tenant),
    )).all()
    occupied = Counter({tenant: count for tenant, count in counts if tenant is not None})
    return WorkflowSlotAdmission(installation_id, policy, occupied)


def _pending_prewarm_dispatch() -> ColumnElement[bool]:
    slot = CloudflareWorkflowSlot
    return exists(select(WorkDispatch.id).join(Execution, Execution.id == WorkDispatch.subject_id).where(
        WorkDispatch.id == slot.dispatch_id, WorkDispatch.subject_id == slot.execution_id,
        cloudflare_dispatch_sql(), WorkDispatch.assigned_worker_id.is_(None),
        Execution.status == ExecutionStatus.PENDING,
        func.coalesce(cast(Execution.organization_id, String), "platform") == slot.tenant,
    ).correlate(slot))


def _live_prewarm() -> ColumnElement[bool]:
    slot = CloudflareWorkflowSlot
    return and_(
        slot.prewarm_fence.is_not(None), slot.warm_until.is_not(None), slot.warm_until > func.now(),
        exists().where(ExecutionWorker.id == slot.worker_id, ExecutionWorker.state == "ready").correlate(slot),
        or_(slot.released_at.is_(None), _pending_prewarm_dispatch()),
    )


def adoptable_workflow_slot(
    admission: WorkflowSlotAdmission, worker_id: UUID, tenant: ColumnElement[str],
) -> ScalarSelect[int]:
    slot = CloudflareWorkflowSlot
    return select(slot.ordinal).where(
        slot.installation_id == admission.installation_id, slot.worker_id == worker_id,
        slot.tenant == tenant, slot.ordinal < admission.policy.slots_per_tenant,
        slot.released_at.is_not(None), _live_prewarm(),
    ).order_by((slot.dispatch_id == WorkDispatch.id).desc(), slot.ordinal).limit(1).correlate(WorkDispatch).scalar_subquery()


def _stale_unsent() -> ColumnElement[bool]:
    slot = CloudflareWorkflowSlot
    # An unsent claim cannot have entered. A concurrent send has to acquire
    # this same row and prove it still owns the ticket before writing frames.
    live_execution = exists().where(
        Execution.id == slot.execution_id,
        Execution.status.in_((ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING)),
    )
    live_owner = exists().where(
        ExecutionWorker.id == slot.worker_id,
        ExecutionWorker.state.in_(("ready", "draining")),
        ExecutionWorker.remote_owner_lease_expires_at > func.clock_timestamp(),
    )
    return ~slot.sent & ~(live_execution & live_owner)


async def delete_workflow_slots(db: AsyncSession, *conditions: ColumnElement[bool]) -> None:
    slot = CloudflareWorkflowSlot
    ordered = select(slot.fence).where(*conditions).order_by(
        slot.installation_id, slot.tenant, slot.ordinal,
    ).with_for_update()
    await db.execute(delete(slot).where(slot.fence.in_(ordered)))


async def load_workflow_slot_admission(db: AsyncSession, worker_id: UUID) -> WorkflowSlotAdmission | None:
    """Read occupancy after the installation admission lock has been taken."""
    row = (await db.execute(
        select(CloudflareInstallation.id, CloudflareInstallation.policy)
        .join(CloudflareWorkerReservation)
        .where(CloudflareWorkerReservation.worker_id == worker_id)
    )).one_or_none()
    if row is None:
        return None
    installation_id, policy = row
    slot = CloudflareWorkflowSlot
    stale = (slot.installation_id == installation_id, slot.released_at.is_(None), _stale_unsent())
    await db.execute(update(slot).where(*stale, _live_prewarm()).values(released_at=func.clock_timestamp()))
    await delete_workflow_slots(db, *stale, ~_live_prewarm())
    return await read_workflow_slot_admission(
        db,
        installation_id, CloudflareWorkersPolicy.model_validate(policy["limits"]["workers"]),
    )


async def occupy_workflow_slot(
    db: AsyncSession, admission: WorkflowSlotAdmission, worker_id: UUID, tenant_id: UUID | None,
    execution_id: UUID, dispatch_id: UUID, fence: UUID, *, prewarm_ordinal: int | None,
) -> WorkflowSlotTicket:
    tenant = tenant_key(tenant_id)
    # The caller holds the installation lock, including when the slot row is absent.
    slot = CloudflareWorkflowSlot
    seats = (await db.execute(select(slot, _live_prewarm()).where(
        slot.installation_id == admission.installation_id, slot.tenant == tenant,
    ))).all()
    rows = {row.ordinal: row for row, _ in seats}
    now = await db.scalar(select(func.now()))
    assert now is not None
    prepared = {row.ordinal for row, live in seats if live}
    free = [
        i for i in range(admission.policy.slots_per_tenant) if i not in prepared
        and (i not in rows or rows[i].released_at is not None)
    ]
    ordinal = prewarm_ordinal if prewarm_ordinal is not None else min(free, key=lambda i: (
        not (i in rows and rows[i].worker_id == worker_id and (warm_until := rows[i].warm_until) is not None
             and warm_until > now),
        i,
    ))
    row = rows.get(ordinal)
    if row is None:
        row = slot(installation_id=admission.installation_id, tenant=tenant, ordinal=ordinal)
        db.add(row)
    row.worker_id = worker_id
    row.execution_id = execution_id
    row.dispatch_id = dispatch_id
    row.fence = fence
    row.sent = False
    row.released_at = None
    prewarming = prewarm_ordinal is not None
    if not prewarming:
        row.warm_until = None
        row.prewarm_fence = None
    return WorkflowSlotTicket(ordinal, fence, prewarming)


async def released_prewarm_dispatches(db: AsyncSession, worker_ids: Sequence[UUID]) -> dict[UUID, list[UUID]]:
    slot = CloudflareWorkflowSlot
    rows = (await db.execute(select(slot.worker_id, slot.dispatch_id).where(
        slot.worker_id.in_(worker_ids), slot.released_at.is_not(None), _live_prewarm(),
    ))).all()
    dispatches: dict[UUID, list[UUID]] = defaultdict(list)
    for worker_id, dispatch_id in rows:
        dispatches[worker_id].append(dispatch_id)
    return dispatches


async def count_released_prewarms(db: AsyncSession, worker_id: UUID) -> int:
    return len((await released_prewarm_dispatches(db, (worker_id,))).get(worker_id, ()))


async def stage_queue_preparations(
    db: AsyncSession, admission: WorkflowSlotAdmission, worker_id: UUID, tenant_id: UUID | None,
    *, launcher_slots: int, occupied_launcher_slots: int,
) -> tuple[QueuePreparation, ...]:
    """Hold free engines under the same installation lock as normal claims."""
    policy = admission.policy
    if not policy.queue_prewarm_per_tenant or not policy.linger_seconds:
        return ()
    slot = CloudflareWorkflowSlot
    tenant = tenant_key(tenant_id)
    now = await db.scalar(select(func.now()))
    assert now is not None
    seats = (await db.execute(select(slot, _live_prewarm()).where(
        slot.installation_id == admission.installation_id, slot.tenant == tenant,
    ))).all()
    rows = {row.ordinal: row for row, _ in seats}
    prepared = {row.ordinal: row for row, live in seats if live}
    reusable_ordinals = {
        i for i, row in rows.items() if i not in prepared and i < policy.slots_per_tenant
        and row.released_at is not None and row.worker_id == worker_id
        and row.warm_until is not None and row.warm_until > now
    }
    reusable = len(reusable_ordinals)
    headroom = admission.prewarm_count(
        tenant, prepared=len(prepared), launcher_free=launcher_slots - occupied_launcher_slots,
    )
    if not headroom:
        return ()
    already_prepared = [row.dispatch_id for row in prepared.values()]
    # Read only enough eligible queue entries to fill the remaining slots.
    # Their dispatch and execution are rechecked before Loader authorization.
    eligible = (
        cloudflare_dispatch_sql(), WorkDispatch.assigned_worker_id.is_(None), Execution.status == ExecutionStatus.PENDING,
        managed_workflow_fits(worker_id),
    )
    queue = select(WorkDispatch.id, Execution.id, WorkDispatch.created_at).join(
        Execution, Execution.id == WorkDispatch.subject_id,
    ).where(*eligible)
    queued = (await db.execute(queue.where(
        WorkDispatch.id.not_in(already_prepared), Execution.organization_id.is_not_distinct_from(tenant_id),
    ).order_by(WorkDispatch.created_at, WorkDispatch.id).limit(headroom + reusable))).all()[reusable:]
    if not queued:
        return ()
    # Keep the ordinary queue order before allocating seats that an adoption
    # could later use despite a full launcher or the installation's last seat.
    youngest = queued[-1]
    older_other_tenant = queue.where(
        Execution.organization_id.is_distinct_from(tenant_id),
        func.coalesce(cast(Execution.organization_id, String), "platform").not_in(admission.blocked_tenants),
        tuple_(WorkDispatch.created_at, WorkDispatch.id) < tuple_(youngest.created_at, youngest[0]),
    )
    if await db.scalar(select(older_other_tenant.exists())):
        return ()
    available = [
        i for i in range(policy.slots_per_tenant) if i not in prepared and i not in reusable_ordinals
        and (i not in rows or rows[i].released_at is not None)
    ]
    available.sort(key=lambda i: (
        i in rows and rows[i].worker_id != worker_id
        and (until := rows[i].warm_until) is not None and until > now,
        i,
    ))
    warm_until = now + timedelta(seconds=policy.linger_seconds)
    prepare_by = min(warm_until, now + timedelta(seconds=PREPARE_SECONDS))
    preparations = []
    for ordinal, (dispatch_id, execution_id, _) in zip(available[:headroom], queued, strict=False):
        fence = uuid4()
        row = rows.get(ordinal)
        if row is None:
            row = slot(installation_id=admission.installation_id, tenant=tenant, ordinal=ordinal)
            db.add(row)
        row.worker_id = worker_id
        row.execution_id = execution_id
        row.dispatch_id = dispatch_id
        row.fence = uuid4()
        row.sent = False
        row.released_at = now
        row.warm_until = warm_until
        row.prewarm_fence = fence
        preparations.append(QueuePreparation(ordinal, fence, execution_id, prepare_by, warm_until))
    return tuple(preparations)


async def authorized_queue_preparation(
    db: AsyncSession, worker_id: UUID, fence: UUID,
) -> CloudflareWorkflowSlot | None:
    slot = CloudflareWorkflowSlot
    same_tenant = func.coalesce(cast(Execution.organization_id, String), "platform") == slot.tenant
    running = exists().where(
        Execution.id == slot.execution_id, Execution.status == ExecutionStatus.RUNNING, same_tenant,
    )
    return await db.scalar(select(slot).where(
        slot.worker_id == worker_id, slot.prewarm_fence == fence, slot.warm_until > func.clock_timestamp(),
        or_(and_(slot.released_at.is_not(None), _pending_prewarm_dispatch()), and_(slot.released_at.is_(None), running)),
        exists().where(ExecutionWorker.id == worker_id, ExecutionWorker.state == "ready"),
    ))


async def fence_workflow_slot_send(
    db: AsyncSession, worker_id: UUID, execution_id: UUID, ticket: WorkflowSlotTicket,
    *, tenant_id: UUID | str | None, preparation: bool,
) -> bool:
    slot = CloudflareWorkflowSlot
    target = (
        slot.worker_id == worker_id, slot.execution_id == execution_id,
        slot.tenant == tenant_key(tenant_id),
        slot.ordinal == ticket.ordinal, slot.fence == ticket.fence, slot.released_at.is_(None),
    )
    if preparation:
        return await db.scalar(select(slot.fence).where(*target)) is not None
    return await db.scalar(update(slot).where(*target, ~slot.sent).values(sent=True).returning(slot.fence)) is not None


async def confirm_workflow_slot_release(
    db: AsyncSession, worker_id: UUID, execution_id: UUID, fence: UUID,
    *, completed: bool = False,
) -> None:
    """Apply definitive stop or completion evidence to the exact holder."""
    slot = CloudflareWorkflowSlot
    where = (
        slot.worker_id == worker_id, slot.execution_id == execution_id,
        slot.fence == fence, slot.released_at.is_(None),
    )
    warm_until = None
    if completed:
        policy = await db.scalar(select(
            CloudflareInstallation.policy,
        ).join(slot).where(*where))
        if policy is None:
            return  # The exact holder was already released or replaced.
        linger = CloudflareWorkersPolicy.model_validate(policy["limits"]["workers"]).linger_seconds
        warm_until = func.clock_timestamp() + timedelta(seconds=linger)
    values: dict[str, object] = {"released_at": func.clock_timestamp()}
    if completed:
        values.update(warm_until=warm_until, prewarm_fence=None)
    else:
        # A preparation already in flight can still finish after an adopted
        # workflow stops. Keep its bounded hold; the old dispatch cannot
        # authorize a new Loader lookup.
        values["warm_until"] = case((_live_prewarm(), slot.warm_until), else_=None)
    await db.execute(update(slot).where(*where).values(**values))


async def settle_workflow_slot(
    db: AsyncSession, worker_id: UUID, execution_id: UUID, fence: UUID, state: str,
) -> None:
    if state in ("completed", "not_started", "stopped"):
        await confirm_workflow_slot_release(db, worker_id, execution_id, fence, completed=state == "completed")
        return
    # The provider's normal retirement path must fence this incarnation before
    # a lost result can return its capacity to the installation.
    slot = CloudflareWorkflowSlot
    await db.execute(update(ExecutionWorker).where(
        ExecutionWorker.id == worker_id, ExecutionWorker.state == "ready",
        exists().where(
            slot.worker_id == worker_id, slot.execution_id == execution_id,
            slot.fence == fence, slot.sent, slot.released_at.is_(None),
        ),
    ).values(state="draining", end_reason="cloudflare_stop_unconfirmed"))
