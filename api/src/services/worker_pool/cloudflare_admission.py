"""Serialize installation admission changes with managed Workers claims."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import and_, exists, false, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareInstallation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.services.worker_pool.cloudflare_release import installed_workers_release_digest
from src.services.worker_pool.cloudflare_spend import cloudflare_usage_within_limit
from src.services.worker_pool.liveness import HEARTBEAT_LOST_AFTER
from src.worker.remote.protocol import CLOUDFLARE_PYTHON_RUNTIME_PROFILE


@dataclass(frozen=True, slots=True)
class ManagedCloudflareScope:
    installation_id: UUID
    name: str
    revision: int

    def owns(self, worker_id: ColumnElement[UUID]) -> ColumnElement[bool]:
        return exists().where(
            CloudflareWorkerReservation.worker_id == worker_id,
            CloudflareWorkerReservation.installation_id == self.installation_id,
            CloudflareWorkerReservation.scope == self.name,
            CloudflareWorkerReservation.deployment_revision == self.revision,
        )

    async def lock_admission(self, db: AsyncSession, *, for_retirement: bool = False) -> int | None:
        """Fence admission and return the version of the root for a new reservation."""
        locked = await db.scalar(select(CloudflareInstallation.id).where(
            CloudflareInstallation.id == self.installation_id,
        ).with_for_update(read=True))
        if locked is None:
            return None
        return await db.scalar(select(CloudflareInstallation.control_root_version)
            .join(CloudflareCapacityScope, CloudflareCapacityScope.installation_id == CloudflareInstallation.id)
            .where(
                CloudflareInstallation.id == self.installation_id,
                CloudflareInstallation.retired_at.is_(None),
                CloudflareInstallation.desired_state == "enabled",
                cloudflare_usage_within_limit(),
                CloudflareInstallation.admitted_revision == self.revision,
                *(() if for_retirement else (workers_release_matches(),)),
                CloudflareInstallation.policy["limits"]["workers"]["work_kinds"].contains(["workflow"]),
                CloudflareCapacityScope.scope == self.name,
                CloudflareCapacityScope.deployment_revision == self.revision,
            ))

    def pin(self, worker_id: UUID, control_root_version: int) -> CloudflareWorkerReservation:
        return CloudflareWorkerReservation(
            worker_id=worker_id, installation_id=self.installation_id, scope=self.name,
            deployment_revision=self.revision, control_root_version=control_root_version,
        )


def workers_release_matches() -> ColumnElement[bool]:
    release = installed_workers_release_digest()
    if release is None:
        return false()
    return and_(
        CloudflareInstallation.upgrade_release.is_(None),
        CloudflareInstallation.policy["bounds_release"].astext == release,
        or_(
            ~CloudflareInstallation.containers_policy.has_key("enabled"),
            CloudflareInstallation.containers_policy["enabled"].as_boolean().is_(False),
            CloudflareInstallation.containers_ready_revision == CloudflareInstallation.admitted_revision,
        ),
    )


@dataclass(frozen=True, slots=True)
class PrimaryPool:
    form: Literal["job", "static", "external"]
    scope: str | None = None
    ceiling: int | None = None

    def saturated(self, linux_workers: list[ExecutionWorker], now: datetime) -> bool:
        cohort = [
            worker for worker in linux_workers
            if worker.form == self.form and worker.capacity_scope == self.scope
        ]
        normal = [worker for worker in cohort if worker.execution_lane == "normal" and worker.state == "ready"]
        if not normal or any(
            worker.heartbeat_at is None or worker.heartbeat_at < now - HEARTBEAT_LOST_AFTER
            for worker in normal
        ):
            return False
        if self.form == "static":
            return len(normal) == len([worker for worker in cohort if worker.execution_lane == "normal"])
        return self.ceiling is not None and len(cohort) >= self.ceiling


async def cloudflare_claim_allowed(db: AsyncSession, worker_id: UUID) -> bool:
    """Serialize scoped assignments through the installation before locking the worker.

    A reservation pin survives scope removal. A missing or replaced deployment
    therefore closes this worker's claims while its terminal work can settle.
    Writers take the installation lock before changing its scope binding.
    """
    pin = CloudflareWorkerReservation
    installation = CloudflareInstallation
    installation_id = await db.scalar(select(installation.id).join(
        pin, pin.installation_id == installation.id,
    ).where(pin.worker_id == worker_id).with_for_update(key_share=True, of=installation))
    if installation_id is None:
        unpinned_scoped = await db.scalar(select(exists().where(
            ExecutionWorker.id == worker_id,
            ExecutionWorker.capacity_scope.is_not(None),
            ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        )))
        return not unpinned_scoped
    # Read the scope in a fresh statement after taking the lock. The scope can
    # change while this transaction waits behind a writer, even if that writer
    # leaves the installation row unchanged.
    return await scoped_cloudflare_claim_allowed(db, worker_id, installation_id)


async def scoped_cloudflare_claim_allowed(
    db: AsyncSession, worker_id: UUID, installation_id: UUID | None = None,
) -> bool:
    """Read a managed worker's scoped admission and spend without taking the installation lock."""
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
        workers_release_matches(),
        pin.canary_job_id.is_(None),
        pin.container_reserved_at.is_(None),
        pin.control_root_version.is_not(None),
        cloudflare_usage_within_limit(),
        installation.policy["limits"]["workers"]["work_kinds"].contains(["workflow"]),
        ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        ExecutionWorker.capacity_scope.is_not_distinct_from(pin.scope),
        matches_scope,
    )
    query = (
        select(allowed)
        .select_from(pin)
        .join(installation, installation.id == pin.installation_id)
        .join(ExecutionWorker, ExecutionWorker.id == pin.worker_id)
        .where(pin.worker_id == worker_id)
    )
    if installation_id is not None:
        query = query.where(pin.installation_id == installation_id)
    return await db.scalar(query) is True
