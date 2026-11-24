"""Serialize installation admission changes with managed Workers claims."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from sqlalchemy import SQLColumnExpression, and_, exists, false, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import MANAGED_WORKER_FORMS, ExecutionWorker
from src.services.worker_pool.cloudflare_containers_policy import (
    CONTAINERS_SCOPE,
    QUALIFICATION_DEFERRED_KEY,
    container_deadline_sql,
)
from src.services.worker_pool.cloudflare_release import installed_workers_release_digest
from src.services.worker_pool.cloudflare_spend import cloudflare_usage_within_limit
from src.services.worker_pool.eligibility import timeout_fits_workers_deadline
from src.services.worker_pool.liveness import HEARTBEAT_LOST_AFTER
from src.services.worker_pool.sizing import LIVE_STATES
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    LINUX_RUNTIME_PROFILE,
    WORKERS_DEADLINE_DISPATCH_SECONDS,
)


@dataclass(frozen=True, slots=True)
class ManagedWorkerOccupancy:
    live: frozenset[UUID]
    cloudflare: frozenset[UUID]
    shared_cap: int | None
    cleanup: frozenset[UUID]
    draining: frozenset[UUID]

    @property
    def primary_count(self) -> int:
        return len(self.live - self.cloudflare)

    @property
    def shared_count(self) -> int:
        return len(self.live | self.cloudflare)


async def read_shared_managed_worker_cap(db: AsyncSession, *, admitting_installation_id: UUID | None = None) -> int | None:
    """Use the tightest cap among admitted, still-occupied, and qualifying installations."""
    pin = CloudflareWorkerReservation
    # Durable state keeps a stale-heartbeat worker counted until retirement
    # records that the provider resource was deleted.
    unsettled = select(pin.worker_id).join(ExecutionWorker).where(
        pin.installation_id == CloudflareInstallation.id,
        (ExecutionWorker.form.in_(MANAGED_WORKER_FORMS) & ExecutionWorker.state.in_(LIVE_STATES))
        | ExecutionWorker.capacity_resource_deleted_at.is_(None),
    ).exists()
    return await db.scalar(select(
        func.min(CloudflareInstallation.policy["limits"]["shared_managed_worker_cap"].as_integer())
    ).where(
        (CloudflareInstallation.retired_at.is_(None) & CloudflareInstallation.admitted_revision.is_not(None))
        | unsettled
        | (CloudflareInstallation.id == admitting_installation_id),
    ))


async def read_managed_worker_occupancy(
    db: AsyncSession, live_workers: Sequence[ExecutionWorker], *, admitting_installation_id: UUID | None = None,
) -> ManagedWorkerOccupancy:
    """Count each live managed worker once and keep pins until deletion is confirmed."""
    live = frozenset(worker.id for worker in live_workers if worker.form in MANAGED_WORKER_FORMS)
    pin = CloudflareWorkerReservation
    live_pin = pin.worker_id.in_(live)
    occupied = live_pin | ExecutionWorker.capacity_resource_deleted_at.is_(None)
    cap = await read_shared_managed_worker_cap(db, admitting_installation_id=admitting_installation_id)
    rows = (await db.execute(select(
        pin.worker_id, pin.scope, ExecutionWorker.state, ExecutionWorker.end_reason,
    ).join(
        ExecutionWorker, ExecutionWorker.id == pin.worker_id,
    ).where(occupied if cap is not None else live_pin))).all()
    return ManagedWorkerOccupancy(
        live=live,
        cloudflare=frozenset(worker_id for worker_id, _, _, _ in rows),
        shared_cap=cap,
        cleanup=frozenset(worker_id for worker_id, _, _, _ in rows if worker_id not in live),
        draining=frozenset(
            worker_id for worker_id, scope, state, cause in rows
            if scope == "workers" and worker_id in live and state == "draining" and cause == "cloudflare_admission_closed"
        ),
    )


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

    def pin(
        self, worker_id: UUID, control_root_version: int, *, reserved_at: datetime, lifetime: int, deadline_wire: bool,
    ) -> CloudflareWorkerReservation:
        return CloudflareWorkerReservation(
            worker_id=worker_id, installation_id=self.installation_id, scope=self.name,
            deployment_revision=self.revision, control_root_version=control_root_version,
            workers_lifetime_seconds=lifetime, workers_deadline_at=reserved_at + timedelta(seconds=lifetime),
            workers_deadline_wire=deadline_wire,
        )


def joint_container_readiness(revision: SQLColumnExpression[int | None] | int) -> ColumnElement[bool]:
    """Bind joint admission to a ready scope and its owned application."""
    scope_ready = exists(select(CloudflareCapacityScope.installation_id).where(
        CloudflareCapacityScope.installation_id == CloudflareInstallation.id,
        CloudflareCapacityScope.scope == CONTAINERS_SCOPE,
        CloudflareCapacityScope.deployment_revision == revision,
    ).correlate(CloudflareInstallation))
    application_ready = exists(select(CloudflareOwnedResource.id).where(
        CloudflareOwnedResource.installation_id == CloudflareInstallation.id,
        CloudflareOwnedResource.account_id == CloudflareInstallation.account_id,
        CloudflareOwnedResource.scope == CONTAINERS_SCOPE,
        CloudflareOwnedResource.kind == "container_application",
        CloudflareOwnedResource.desired_revision == revision,
        CloudflareOwnedResource.artifact_digest == CloudflareInstallation.containers_release_digest,
        CloudflareOwnedResource.state == "present",
        CloudflareOwnedResource.provider_id.is_not(None),
        CloudflareOwnedResource.provider_version.is_not(None),
        CloudflareOwnedResource.observed_at.is_not(None),
    ).correlate(CloudflareInstallation))
    return or_(
        ~CloudflareInstallation.containers_policy.has_key("enabled"),
        CloudflareInstallation.containers_policy["enabled"].as_boolean().is_(False),
        and_(CloudflareInstallation.containers_ready_revision == revision, scope_ready, application_ready),
    )


def workers_release_matches() -> ColumnElement[bool]:
    release = installed_workers_release_digest()
    if release is None:
        return false()
    return and_(
        CloudflareInstallation.upgrade_release.is_(None),
        CloudflareInstallation.policy["bounds_release"].astext == release,
        or_(
            joint_container_readiness(CloudflareInstallation.admitted_revision),
            and_(
                CloudflareInstallation.containers_policy["enabled"].as_boolean().is_(True),
                CloudflareInstallation.containers_ready_revision.is_distinct_from(CloudflareInstallation.admitted_revision),
                func.coalesce(
                    CloudflareInstallation.deployment_evidence[QUALIFICATION_DEFERRED_KEY].as_integer()
                    == CloudflareInstallation.admitted_revision, False,
                ),
            ),
        ),
    )


@dataclass(frozen=True, slots=True)
class PrimaryPool:
    form: Literal["job", "static", "external"]
    scope: str | None = None
    ceiling: int | None = None

    def saturated(self, linux_workers: list[ExecutionWorker], now: datetime) -> bool:
        return self.capacity_state(linux_workers, now) == "saturated"

    def capacity_state(
        self, linux_workers: list[ExecutionWorker], now: datetime,
    ) -> Literal["saturated", "below_ceiling", "unhealthy"]:
        cohort = [
            worker for worker in linux_workers
            if worker.form == self.form and worker.capacity_scope == self.scope
        ]
        normal = [worker for worker in cohort if worker.execution_lane == "normal" and worker.state == "ready"]
        if not normal or any(
            worker.heartbeat_at is None or worker.heartbeat_at < now - HEARTBEAT_LOST_AFTER
            for worker in normal
        ):
            return "unhealthy"
        if self.form == "static":
            return "saturated" if len(normal) == sum(w.execution_lane == "normal" for w in cohort) else "unhealthy"
        if self.ceiling is None:
            return "unhealthy"
        return "saturated" if len(cohort) >= self.ceiling else "below_ceiling"


async def _lock_worker_installation(db: AsyncSession, worker_id: UUID) -> UUID | None:
    pin = CloudflareWorkerReservation
    installation = CloudflareInstallation
    return await db.scalar(select(installation.id).join(
        pin, pin.installation_id == installation.id,
    ).where(pin.worker_id == worker_id).with_for_update(key_share=True, of=installation))


async def cloudflare_claim_allowed(db: AsyncSession, worker_id: UUID) -> bool:
    """Serialize scoped assignments through the installation before locking the worker.

    A reservation pin survives scope removal. A missing or replaced deployment
    therefore closes this worker's claims while its terminal work can settle.
    Writers take the installation lock before changing its scope binding.
    """
    installation_id = await _lock_worker_installation(db, worker_id)
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
    return await db.scalar(_scoped_cloudflare_admission(worker_id, installation_id)) is not None


async def cloudflare_dispatch_started_at(db: AsyncSession, worker_id: UUID, *, timeout_seconds: int) -> datetime | None:
    """Fence the send and return the exact database instant used to price its timeout."""
    if timeout_seconds <= 0:
        return None
    installation_id = await _lock_worker_installation(db, worker_id)
    if installation_id is None:
        return None
    return await db.scalar(_scoped_cloudflare_admission(worker_id, installation_id, timeout_seconds))


def _scoped_cloudflare_admission(worker_id: UUID, installation_id: UUID | None, timeout_seconds: int | None = None):
    pin = CloudflareWorkerReservation
    installation = CloudflareInstallation
    scope = CloudflareCapacityScope
    # Materialize the volatile clock once so both predicates and the returned
    # workflow start observe the same instant after taking the lock.
    clock = select(func.clock_timestamp().label("started_at")).cte().prefix_with("MATERIALIZED")
    now = clock.c.started_at
    matches_scope = exists().where(
        scope.installation_id == pin.installation_id,
        scope.scope == pin.scope,
        scope.deployment_revision == pin.deployment_revision,
    ).correlate(pin)
    workers = and_(
        pin.container_reserved_at.is_(None),
        pin.workers_deadline_at > now + timedelta(seconds=WORKERS_DEADLINE_DISPATCH_SECONDS),
        pin.workers_lifetime_seconds.is_not(None),
        *(() if timeout_seconds is None else (timeout_fits_workers_deadline(timeout_seconds, now=now),)),
        installation.policy["limits"]["workers"]["work_kinds"].contains(["workflow"]),
        ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    )
    runtime = workers
    if timeout_seconds is None:
        runtime = or_(workers, and_(
            pin.scope == CONTAINERS_SCOPE, pin.container_reserved_at.is_not(None),
            pin.container_stopped_at.is_(None), container_deadline_sql() > now,
            pin.container_release == installation.containers_release_digest,
            ExecutionWorker.runtime_profile == LINUX_RUNTIME_PROFILE,
            ExecutionWorker.reserved_for_dispatch_id.is_not(None),
        ))
    allowed = and_(
        installation.retired_at.is_(None),
        installation.desired_state == "enabled",
        installation.admitted_revision.is_not(None),
        installation.admitted_revision == pin.deployment_revision,
        workers_release_matches(),
        pin.canary_job_id.is_(None),
        pin.control_root_version.is_not(None),
        cloudflare_usage_within_limit(),
        ExecutionWorker.capacity_scope.is_not_distinct_from(pin.scope),
        matches_scope,
        runtime,
    )
    query = (
        select(now)
        .select_from(clock)
        .join(pin, pin.worker_id == worker_id)
        .join(installation, installation.id == pin.installation_id)
        .join(ExecutionWorker, ExecutionWorker.id == pin.worker_id)
        .where(allowed)
    )
    if installation_id is not None:
        query = query.where(pin.installation_id == installation_id)
    return query
