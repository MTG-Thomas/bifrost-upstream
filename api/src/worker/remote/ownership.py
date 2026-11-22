"""Database-clock fencing for a remote launcher's API control owner."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import case, func, or_, select, text, update
from sqlalchemy.sql.elements import ColumnElement

from src.core.database import get_liveness_db_context
from src.models.orm.execution_workers import ExecutionWorker
from src.services.worker_pool.cloudflare_containers_policy import container_identity_matches
from src.worker.remote.protocol import (
    CLOUDFLARE_CREDENTIAL_PROTOCOL,
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    LINUX_RUNTIME_PROFILE,
    cloudflare_slots,
)


DEFAULT_OWNER_LEASE_SECONDS = 20


@dataclass(frozen=True)
class RemoteOwnerLease:
    worker_id: UUID
    owner_id: UUID
    launcher_id: UUID
    session_generation: int
    # The runtime credential protocol the launcher reported for this
    # session, persisted with the generation (1: legacy tokens only).
    credential_protocol: int = 1
    declared_slots: int = 0


@dataclass(frozen=True)
class CloudflareEnrollment:
    resource_uid: str
    version: str
    hostname: str


def remote_enrollment_is_live() -> ColumnElement[bool]:
    """A terminal Cloudflare row revokes the lease even before clock expiry."""
    return or_(
        ExecutionWorker.runtime_profile != CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        ExecutionWorker.state.in_(("ready", "draining")),
    )


async def acquire_remote_owner(
    *,
    worker_id: UUID,
    owner_id: UUID,
    launcher_id: UUID,
    nonce_digest: str,
    hello_sequence: int,
    credential_protocol: int = 1,
    runtime_profile: str = LINUX_RUNTIME_PROFILE,
    cloudflare_enrollment: CloudflareEnrollment | None = None,
    container_release: str | None = None,
    lease_seconds: int = DEFAULT_OWNER_LEASE_SECONDS,
) -> RemoteOwnerLease | None:
    """Acquire one fenced API owner session for an admitted launcher.

    PostgreSQL decides expiry.  A new API owner may only replace an expired
    one; the current owner may rotate its own session on reconnect.  Every
    accepted hello consumes its nonce, preventing a captured hello from being
    replayed after a replica restart.

    Credential protocol and physical runtime are recorded with the session
    generation. Reconnects keep the physical runtime of the first session.
    Cloudflare admission and ownership use one conditional update on the
    prepared external row, including on reconnect and takeover.
    """
    if lease_seconds <= 0:
        raise ValueError("remote owner lease must be positive")
    if hello_sequence < 1:
        raise ValueError("remote hello sequence must be positive")
    interval = f"{lease_seconds} seconds"
    worker_scope = [ExecutionWorker.id == worker_id]
    enrollment_values = {}
    if runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
        if cloudflare_enrollment is None or credential_protocol != CLOUDFLARE_CREDENTIAL_PROTOCOL:
            return None
        starting = ExecutionWorker.state == "starting"
        worker_scope.extend((
            ExecutionWorker.form == "external",
            ExecutionWorker.execution_lane == "normal",
            ExecutionWorker.runtime_profile == runtime_profile,
            ExecutionWorker.capacity_resource_uid == cloudflare_enrollment.resource_uid,
            ExecutionWorker.capacity_resource_deleted_at.is_(None),
            ExecutionWorker.version == cloudflare_enrollment.version,
            ExecutionWorker.remote_launcher_id == launcher_id,
        ))
        enrollment_values = {
            "state": case((starting, "ready"), else_=ExecutionWorker.state),
            "hostname": case((starting, cloudflare_enrollment.hostname), else_=ExecutionWorker.hostname),
            "ready_at": case((starting, func.now()), else_=ExecutionWorker.ready_at),
            "heartbeat_at": case((starting, func.now()), else_=ExecutionWorker.heartbeat_at),
            "last_claim_at": case((starting, func.now()), else_=ExecutionWorker.last_claim_at),
            "memory_budget_bytes": None,
        }
    else:
        # A legacy hello cannot take over even a never-connected Cloudflare row.
        worker_scope.append(ExecutionWorker.runtime_profile != CLOUDFLARE_PYTHON_RUNTIME_PROFILE)
        worker_scope.append(container_identity_matches(worker_id, container_release))
    async with get_liveness_db_context() as db:
        declared_slots = 0
        if runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
            prepared = (await db.execute(
                select(ExecutionWorker.memory_limit_bytes).where(*worker_scope).with_for_update()
            )).one_or_none()
            if prepared is None:
                return None
            try:
                declared_slots = cloudflare_slots(prepared.memory_limit_bytes)
            except ValueError:
                return None
        row = await db.execute(
            update(ExecutionWorker)
            .where(
                *worker_scope,
                # Static launchers register a fresh canonical row before the
                # API-side executor has measured its admission budget.
                # Cloudflare moves starting to ready in this same update.
                ExecutionWorker.state.in_(("starting", "ready", "draining")),
                or_(
                    ExecutionWorker.remote_launcher_id.is_(None),
                    ExecutionWorker.remote_launcher_id == launcher_id,
                ),
                ExecutionWorker.remote_hello_sequence < hello_sequence,
                or_(
                    ExecutionWorker.remote_hello_sequence == 0,
                    ExecutionWorker.runtime_profile == runtime_profile,
                ),
                or_(
                    ExecutionWorker.remote_owner_id.is_(None),
                    ExecutionWorker.remote_owner_id == owner_id,
                    ExecutionWorker.remote_owner_lease_expires_at <= func.now(),
                ),
            )
            .values(
                remote_launcher_id=launcher_id,
                remote_owner_id=owner_id,
                remote_session_generation=ExecutionWorker.remote_session_generation + 1,
                remote_owner_lease_expires_at=func.now() + text(f"INTERVAL '{interval}'"),
                remote_auth_nonce_digest=nonce_digest,
                remote_hello_sequence=hello_sequence,
                remote_credential_protocol=credential_protocol,
                runtime_profile=runtime_profile,
                **enrollment_values,
            )
            .returning(ExecutionWorker.remote_session_generation, ExecutionWorker.remote_credential_protocol)
        )
        acquired = row.one_or_none()
    if acquired is None:
        return None
    generation, protocol = acquired
    return RemoteOwnerLease(worker_id, owner_id, launcher_id, generation, protocol, declared_slots)


async def renew_remote_owner(
    lease: RemoteOwnerLease, *, lease_seconds: int = DEFAULT_OWNER_LEASE_SECONDS
) -> bool:
    """Renew only the still-current owner generation using the DB clock."""
    if lease_seconds <= 0:
        raise ValueError("remote owner lease must be positive")
    interval = f"{lease_seconds} seconds"
    async with get_liveness_db_context() as db:
        renewed = await db.scalar(
            update(ExecutionWorker)
            .where(
                ExecutionWorker.id == lease.worker_id,
                ExecutionWorker.remote_owner_id == lease.owner_id,
                ExecutionWorker.remote_launcher_id == lease.launcher_id,
                ExecutionWorker.remote_session_generation == lease.session_generation,
                ExecutionWorker.remote_owner_lease_expires_at > func.now(),
                remote_enrollment_is_live(),
            )
            .values(remote_owner_lease_expires_at=func.now() + text(f"INTERVAL '{interval}'"))
            .returning(ExecutionWorker.id)
        )
    return renewed is not None


async def owns_remote_session(lease: RemoteOwnerLease) -> bool:
    """Fence commands and result commits before they reach canonical consumers."""
    async with get_liveness_db_context() as db:
        worker_id = await db.scalar(
            select(ExecutionWorker.id).where(
                ExecutionWorker.id == lease.worker_id,
                ExecutionWorker.remote_owner_id == lease.owner_id,
                ExecutionWorker.remote_launcher_id == lease.launcher_id,
                ExecutionWorker.remote_session_generation == lease.session_generation,
                ExecutionWorker.remote_owner_lease_expires_at > func.now(),
                remote_enrollment_is_live(),
            )
        )
    return worker_id is not None


async def release_remote_owner(lease: RemoteOwnerLease) -> bool:
    """Best-effort graceful release; stale owners cannot clear a newer fence."""
    async with get_liveness_db_context() as db:
        released = await db.scalar(
            update(ExecutionWorker)
            .where(
                ExecutionWorker.id == lease.worker_id,
                ExecutionWorker.remote_owner_id == lease.owner_id,
                ExecutionWorker.remote_session_generation == lease.session_generation,
            )
            .values(remote_owner_lease_expires_at=func.now())
            .returning(ExecutionWorker.id)
        )
    return released is not None
