"""Desired-policy writes and their admission revision fence."""

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from pydantic import SecretStr
from shared.models import CloudflareCapacityPolicy
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    seal_credential,
)
from src.services.worker_pool.cloudflare_spend import (
    WORKERS_RATE_TABLE_VERSION,
    workers_fleet_envelope_usd,
)

CONSENT_TEXT_VERSION = "existing-https-v1"


@dataclass(frozen=True, slots=True)
class WorkersReleaseMeterBounds:
    """Verified metering ceilings supplied by the trusted release artifact."""

    cleanup_margin_seconds: int
    per_launcher_second: Mapping[str, Decimal]
    per_fleet_fixed: Mapping[str, Decimal]
    release: str
    max_slots_per_launcher: int


class CloudflarePolicyConflict(ValueError):
    """The installation changed since the caller read its desired policy."""


class CloudflareBudgetTooLow(ValueError):
    """The full supported fleet lifetime exceeds the policy's monthly limit."""


class CloudflareInstallationExists(ValueError):
    """A current installation must finish disconnecting before another is created."""


def _price_policy(
    policy: CloudflareCapacityPolicy, bounds: WorkersReleaseMeterBounds
) -> dict[str, object]:
    if policy.workers.slots_per_launcher > bounds.max_slots_per_launcher:
        raise ValueError("Workers meter bounds do not cover the configured slot count")
    digest = sha256(json.dumps({
        "cleanup_margin_seconds": bounds.cleanup_margin_seconds,
        "max_slots_per_launcher": bounds.max_slots_per_launcher,
        "per_launcher_second": {k: str(v.normalize()) for k, v in bounds.per_launcher_second.items()},
        "per_fleet_fixed": {k: str(v.normalize()) for k, v in bounds.per_fleet_fixed.items()},
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    envelope = workers_fleet_envelope_usd(
        launcher_cap=policy.workers.launcher_cap,
        max_lifetime_seconds=policy.workers.max_lifetime_seconds,
        cleanup_margin_seconds=bounds.cleanup_margin_seconds,
        per_launcher_second=bounds.per_launcher_second,
        per_fleet_fixed=bounds.per_fleet_fixed,
    )
    if envelope > policy.monthly_limit_usd:
        raise CloudflareBudgetTooLow(f"The fleet spend envelope is USD {envelope}")
    limits = policy.model_dump(mode="json")
    limits["workers"]["work_kinds"] = sorted(policy.workers.work_kinds)
    return {
        "limits": limits,
        "rate_table_version": WORKERS_RATE_TABLE_VERSION,
        "fleet_envelope_usd": str(envelope),
        "bounds_digest": digest,
        "bounds_release": bounds.release,
    }


async def create_cloudflare_installation(
    db: AsyncSession,
    *,
    account_id: str,
    worker_ingress_origin: str,
    consent_version: str,
    actor_id: UUID,
    verified_api_token: SecretStr,
    policy: CloudflareCapacityPolicy,
    bounds: WorkersReleaseMeterBounds,
) -> UUID:
    """Persist an already validated connection in the caller's transaction."""
    if not re.fullmatch("[a-f0-9]{32}", account_id):
        raise ValueError("Invalid Cloudflare account ID")
    hostname = urlsplit(worker_ingress_origin).hostname
    canonical_origin = f"https://{hostname}"
    if (
        not hostname
        or worker_ingress_origin not in (canonical_origin, f"{canonical_origin}/")
        or len(canonical_origin) > 255
        or "." not in hostname
        or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", part) for part in hostname.split("."))
    ):
        raise ValueError("Cloudflare worker ingress requires a dedicated HTTPS origin")
    if consent_version != CONSENT_TEXT_VERSION:
        raise ValueError("Cloudflare installation requires consent to the current text")
    actor = str(actor_id)
    desired = _price_policy(policy, bounds)
    installation_id = uuid4()
    now = datetime.now(UTC)
    result = await db.execute(
        insert(CloudflareInstallation)
        .values(
            id=installation_id,
            account_id=account_id,
            worker_ingress_origin=canonical_origin,
            consent_version=consent_version,
            consent_actor=actor,
            consent_at=now,
            policy=desired,
            desired_revision=1,
            desired_state="scaled_in",
            api_token_version=1,
        )
        .on_conflict_do_nothing(
            index_elements=[CloudflareInstallation.singleton],
            index_where=CloudflareInstallation.retired_at.is_(None),
        )
        .returning(CloudflareInstallation.id)
    )
    if result.scalar_one_or_none() is None:
        raise CloudflareInstallationExists("A Cloudflare installation already exists")

    ref = CredentialRef(installation_id, account_id, "account_token", "", 1)
    db.add(seal_credential(ref, verified_api_token, actor=actor, now=now))
    await db.flush()
    return installation_id


async def retire_cloudflare_installation(
    db: AsyncSession, *, installation_id: UUID, expected_revision: int,
) -> None:
    """Finish a fenced disconnect after launcher drain and verified provider absence."""
    row = await db.get(
        CloudflareInstallation, installation_id, with_for_update=True, populate_existing=True,
    )
    if row is None or row.desired_revision != expected_revision or row.desired_state != "disconnected":
        raise CloudflarePolicyConflict("Cloudflare disconnect revision changed")
    if row.retired_at is not None:
        return
    remaining = await db.scalar(
        select(CloudflareOwnedResource.id).where(
            CloudflareOwnedResource.installation_id == installation_id,
            or_(CloudflareOwnedResource.state != "absent", CloudflareOwnedResource.observed_at.is_(None)),
        ).limit(1)
    )
    scope = await db.scalar(select(CloudflareCapacityScope.scope).where(
        CloudflareCapacityScope.installation_id == installation_id,
    ).limit(1))
    unconfirmed_worker = await db.scalar(
        select(CloudflareWorkerReservation.worker_id)
        .join(ExecutionWorker, ExecutionWorker.id == CloudflareWorkerReservation.worker_id)
        .where(
            CloudflareWorkerReservation.installation_id == installation_id,
            or_(
                ExecutionWorker.state.not_in(("lost", "retired")),
                ExecutionWorker.capacity_resource_deleted_at.is_(None),
            ),
        )
        .limit(1)
    )
    if remaining is not None or scope is not None or unconfirmed_worker is not None:
        raise CloudflarePolicyConflict("Cloudflare cleanup is pending")
    row.api_token_version = None
    row.retired_at = datetime.now(UTC)
    await db.execute(update(CloudflareCredential).where(
        CloudflareCredential.installation_id == installation_id,
    ).values(ciphertext=None))
    await db.flush()


async def save_cloudflare_policy(
    db: AsyncSession,
    *,
    installation_id: UUID,
    expected_revision: int,
    policy: CloudflareCapacityPolicy,
    bounds: WorkersReleaseMeterBounds,
) -> int:
    desired = _price_policy(policy, bounds)
    result = await db.execute(
        select(CloudflareInstallation)
        .where(CloudflareInstallation.id == installation_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    row = result.scalar_one_or_none()
    if row is None or row.desired_revision != expected_revision or row.desired_state == "disconnected":
        raise CloudflarePolicyConflict("Cloudflare installation changed. Reload its current policy.")
    if row.policy != desired:
        row.policy = desired
        row.admitted_revision = None
        row.desired_revision += 1
        await db.flush()
    return row.desired_revision
