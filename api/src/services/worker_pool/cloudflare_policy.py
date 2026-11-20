"""Desired-policy writes and their admission revision fence."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from shared.models import CloudflareCapacityPolicy
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.cloudflare_capacity import CloudflareInstallation
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


async def save_cloudflare_policy(
    db: AsyncSession,
    *,
    installation_id: UUID,
    expected_revision: int,
    policy: CloudflareCapacityPolicy,
    bounds: WorkersReleaseMeterBounds,
) -> int:
    envelope = workers_fleet_envelope_usd(
        launcher_cap=policy.workers.launcher_cap,
        max_lifetime_seconds=policy.workers.max_lifetime_seconds,
        cleanup_margin_seconds=bounds.cleanup_margin_seconds,
        per_launcher_second=bounds.per_launcher_second,
        per_fleet_fixed=bounds.per_fleet_fixed,
    )
    if envelope > policy.monthly_limit_usd:
        raise CloudflareBudgetTooLow(f"The fleet spend envelope is USD {envelope}")

    result = await db.execute(
        select(CloudflareInstallation)
        .where(CloudflareInstallation.id == installation_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    row = result.scalar_one_or_none()
    if row is None or row.desired_revision != expected_revision or row.desired_state == "disconnected":
        raise CloudflarePolicyConflict("Cloudflare installation changed. Reload its current policy.")
    desired = {
        "limits": policy.model_dump(mode="json"),
        "rate_table_version": WORKERS_RATE_TABLE_VERSION,
        "fleet_envelope_usd": str(envelope),
    }
    if row.policy != desired:
        row.policy = desired
        row.admitted_revision = None
        row.desired_revision += 1
        await db.flush()
    return row.desired_revision
