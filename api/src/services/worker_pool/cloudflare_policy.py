"""Desired-policy writes and their admission revision fence."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from pydantic import SecretStr
from shared.models import CloudflareCapacityPolicy
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    seal_credential,
)
from src.services.worker_pool.cloudflare_spend import (
    WORKERS_RATE_TABLE_VERSION,
    workers_fleet_envelope_usd,
)


@dataclass(frozen=True, slots=True)
class WorkersReleaseMeterBounds:
    """Verified metering ceilings supplied by the trusted release artifact."""

    cleanup_margin_seconds: int
    per_launcher_second: Mapping[str, Decimal]
    per_fleet_fixed: Mapping[str, Decimal]


class CloudflarePolicyConflict(ValueError):
    """The installation changed since the caller read its desired policy."""


class CloudflareBudgetTooLow(ValueError):
    """The full supported fleet lifetime exceeds the policy's monthly limit."""


def _price_policy(
    policy: CloudflareCapacityPolicy, bounds: WorkersReleaseMeterBounds
) -> dict[str, object]:
    envelope = workers_fleet_envelope_usd(
        launcher_cap=policy.workers.launcher_cap,
        max_lifetime_seconds=policy.workers.max_lifetime_seconds,
        cleanup_margin_seconds=bounds.cleanup_margin_seconds,
        per_launcher_second=bounds.per_launcher_second,
        per_fleet_fixed=bounds.per_fleet_fixed,
    )
    if envelope > policy.monthly_limit_usd:
        raise CloudflareBudgetTooLow(f"The fleet spend envelope is USD {envelope}")
    return {
        "limits": policy.model_dump(mode="json"),
        "rate_table_version": WORKERS_RATE_TABLE_VERSION,
        "fleet_envelope_usd": str(envelope),
    }


async def create_cloudflare_installation(
    db: AsyncSession,
    *,
    account_id: str,
    worker_ingress_origin: str,
    consent_version: str,
    actor: str,
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
    if not consent_version or not actor:
        raise ValueError("Cloudflare installation requires recorded operator consent")
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
        )
        .on_conflict_do_nothing(index_elements=[CloudflareInstallation.singleton])
        .returning(CloudflareInstallation.id)
    )
    if result.scalar_one_or_none() is None:
        raise CloudflarePolicyConflict("A Cloudflare installation already exists")

    ref = CredentialRef(installation_id, account_id, "account_token", "", 1)
    db.add(seal_credential(ref, verified_api_token, actor=actor, now=now))
    await db.flush()
    return installation_id


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
