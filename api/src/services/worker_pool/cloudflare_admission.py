"""Serialize installation admission changes with managed Workers claims."""

from uuid import UUID

from sqlalchemy import and_, exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareInstallation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker


async def cloudflare_claim_allowed(db: AsyncSession, worker_id: UUID) -> bool:
    """Hold the installation's read lock through assignment, before locking the worker.

    A reservation pin survives scope removal. A missing or replaced deployment
    therefore closes this worker's claims while its terminal work can settle.
    Writers must take the installation lock before changing its scope binding.
    """
    pin = CloudflareWorkerReservation
    installation = CloudflareInstallation
    scope = CloudflareCapacityScope
    matches_scope = exists().where(
        scope.installation_id == pin.installation_id,
        scope.scope == pin.scope,
        scope.deployment_revision == pin.deployment_revision,
    ).correlate(pin)
    allowed = and_(
        installation.retired_at.is_(None),
        installation.desired_state == "enabled",
        installation.admitted_revision.is_not(None),
        installation.admitted_revision == pin.deployment_revision,
        ExecutionWorker.capacity_scope.is_not_distinct_from(pin.scope),
        matches_scope,
    )
    result = (await db.execute(
        select(allowed.label("allowed"))
        .select_from(pin)
        .join(installation, installation.id == pin.installation_id)
        .join(ExecutionWorker, ExecutionWorker.id == pin.worker_id)
        .where(pin.worker_id == worker_id)
        .with_for_update(read=True, of=installation)
    )).one_or_none()
    # The reservation is inserted in the same transaction as its worker.
    # Existing primary and manually enrolled workers have no installation pin.
    return result is None or result.allowed is True
