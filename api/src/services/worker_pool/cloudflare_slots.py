"""Durable, fenced ownership of managed Cloudflare workflow engines."""

from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

from sqlalchemy import delete, exists, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from shared.models import CloudflareWorkersPolicy
from src.models.enums import ExecutionStatus
from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareWorkerReservation,
    CloudflareWorkflowSlot,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.executions import Execution


@dataclass(frozen=True)
class WorkflowSlotTicket:
    ordinal: int
    fence: UUID

    def workflow_route(self, execution_id: UUID | str) -> str:
        return f"workflow:{execution_id}:{self.fence}"


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


def tenant_key(tenant_id: UUID | str | None) -> str:
    return str(tenant_id) if tenant_id is not None else "platform"


async def read_workflow_slot_admission(
    db: AsyncSession, installation_id: UUID, policy: CloudflareWorkersPolicy,
) -> WorkflowSlotAdmission:
    slot = CloudflareWorkflowSlot
    counts = (await db.execute(
        select(slot.tenant, func.count()).where(
            slot.installation_id == installation_id, slot.released_at.is_(None), ~_stale_unsent(),
        )
        .group_by(slot.tenant)
    )).all()
    return WorkflowSlotAdmission(installation_id, policy, Counter({tenant: count for tenant, count in counts}))


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
    await delete_workflow_slots(db, slot.installation_id == installation_id, slot.released_at.is_(None), _stale_unsent())
    return await read_workflow_slot_admission(
        db,
        installation_id, CloudflareWorkersPolicy.model_validate(policy["limits"]["workers"]),
    )


async def occupy_workflow_slot(
    db: AsyncSession, admission: WorkflowSlotAdmission, worker_id: UUID, tenant_id: UUID | None,
    execution_id: UUID, dispatch_id: UUID, fence: UUID,
) -> WorkflowSlotTicket:
    tenant = tenant_key(tenant_id)
    # The caller holds the installation lock, including when the slot row is absent.
    slot = CloudflareWorkflowSlot
    rows = {row.ordinal: row for row in await db.scalars(select(slot).where(
        slot.installation_id == admission.installation_id, slot.tenant == tenant,
    ))}
    now = await db.scalar(select(func.clock_timestamp()))
    assert now is not None
    free = [i for i in range(admission.policy.slots_per_tenant) if i not in rows or rows[i].released_at is not None]
    ordinal = min(free, key=lambda i: (
        not (i in rows and rows[i].worker_id == worker_id and rows[i].warm_until is not None
             and rows[i].warm_until > now),
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
    row.warm_until = None
    return WorkflowSlotTicket(ordinal, fence)


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
    await db.execute(update(slot).where(*where).values(released_at=func.clock_timestamp(), warm_until=warm_until))


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
