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
from src.worker.remote.protocol import CLOUDFLARE_PYTHON_RUNTIME_PROFILE


async def cloudflare_claim_allowed(db: AsyncSession, worker_id: UUID) -> bool:
    """Hold the installation's read lock through assignment, before locking the worker.

    A reservation pin survives scope removal. A missing or replaced deployment
    therefore closes this worker's claims while its terminal work can settle.
    Writers must take the installation lock before changing its scope binding.
    """
    pin = CloudflareWorkerReservation
    installation = CloudflareInstallation
    scope = CloudflareCapacityScope
    locked_installation = await db.scalar(
        select(installation.id)
        .join(pin, pin.installation_id == installation.id)
        .where(pin.worker_id == worker_id)
        .with_for_update(read=True, of=installation)
    )
    if locked_installation is None:
        # Existing primary and manually enrolled workers have no installation pin.
        return True
    # Read the scope in a fresh statement after taking the lock. The scope can
    # change while this transaction waits behind a writer, even if that writer
    # leaves the installation row unchanged.
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
        installation.policy["limits"]["workers"]["work_kinds"].contains(["workflow"]),
        ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        ExecutionWorker.capacity_scope.is_not_distinct_from(pin.scope),
        matches_scope,
    )
    result = await db.scalar(
        select(allowed)
        .select_from(pin)
        .join(installation, installation.id == pin.installation_id)
        .join(ExecutionWorker, ExecutionWorker.id == pin.worker_id)
        .where(pin.worker_id == worker_id, pin.installation_id == locked_installation)
    )
    return result is True
