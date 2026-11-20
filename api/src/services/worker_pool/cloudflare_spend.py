"""Cloudflare Workers spend envelope and observed admission guard."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from types import MappingProxyType
from uuid import UUID

from sqlalchemy import Boolean, Numeric, case, column, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareUsageActivity,
)


@dataclass(frozen=True, slots=True)
class MeterRate:
    unit: int
    usd: Decimal
    round_up_units: bool = False

    def cost(self, usage: Decimal) -> Decimal:
        units = usage / self.unit
        if self.round_up_units:
            units = units.to_integral_value(rounding=ROUND_CEILING)
        return units * self.usd


# Standard Workers Paid, SQLite Durable Objects, and Workers Observability.
# Use gross rates without shared-account allowances and charge both log
# schedules across the announced December 2026 pricing transition.
# https://developers.cloudflare.com/workers/platform/pricing/
# https://developers.cloudflare.com/durable-objects/platform/pricing/
# https://developers.cloudflare.com/observability/pricing/
WORKERS_RATE_TABLE_VERSION = "workers-paid-2026-11-20"
WORKERS_RATES = MappingProxyType({
    "worker_requests": MeterRate(1_000_000, Decimal("0.30")),
    "worker_cpu_ms": MeterRate(1_000_000, Decimal("0.02")),
    "do_requests": MeterRate(1_000_000, Decimal("0.15"), round_up_units=True),
    "do_gb_seconds": MeterRate(1_000_000, Decimal("12.50"), round_up_units=True),
    "sqlite_rows_read": MeterRate(1_000_000, Decimal("0.001")),
    "sqlite_rows_written": MeterRate(1_000_000, Decimal("1.00")),
    "sqlite_stored_gb_month": MeterRate(1, Decimal("0.20")),
    "log_events": MeterRate(1_000_000, Decimal("0.60")),
    "log_ingested_gb": MeterRate(1, Decimal("0.25")),
    "log_stored_gb_month": MeterRate(1, Decimal("0.10")),
    "transfer_gb": MeterRate(1, Decimal(0)),
})
WORKERS_PAID_MINIMUM_USD = Decimal("5.00")
# Estimate at the published marginal rates, with no account-wide free tiers.
# Each resident 128 MiB object is estimated separately. Work and gateway
# activity use the packaged Worker CPU cap and one request as pricing proxies.
_CPU_MS_PER_ACTIVITY = 30_000
_OBJECT_GIB = Decimal("0.125")
_DURATION_USD_PER_SECOND = _OBJECT_GIB * WORKERS_RATES["do_gb_seconds"].usd / WORKERS_RATES["do_gb_seconds"].unit
_ACTIVITY_USD = (
    WORKERS_RATES["worker_requests"].cost(Decimal(1))
    + WORKERS_RATES["worker_cpu_ms"].cost(Decimal(_CPU_MS_PER_ACTIVITY))
    + WORKERS_RATES["do_requests"].usd / WORKERS_RATES["do_requests"].unit
)


def workers_fleet_envelope_usd(
    *,
    launcher_cap: int,
    max_lifetime_seconds: int,
    cleanup_margin_seconds: int,
    per_launcher_second: Mapping[str, Decimal],
    per_fleet_fixed: Mapping[str, Decimal],
) -> Decimal:
    """Price qualified bounds from the release manifest.

    Fixed charges cover shared provider costs across the lifetime and
    retained storage that can outlive the runtime. Require explicit bounds
    even for meters that the qualified artifact does not use.
    """
    if launcher_cap < 1 or max_lifetime_seconds < 1 or cleanup_margin_seconds < 1:
        raise ValueError("Cloudflare fleet limits must be positive")
    for bounds in (per_launcher_second, per_fleet_fixed):
        if bounds.keys() != WORKERS_RATES.keys() or any(
            not value.is_finite() or value < 0 for value in bounds.values()
        ):
            raise ValueError("Cloudflare Workers meter bounds are incomplete or invalid")
    launcher_seconds = launcher_cap * (max_lifetime_seconds + cleanup_margin_seconds)
    gross = WORKERS_PAID_MINIMUM_USD + sum(
        rate.cost(per_launcher_second[meter] * launcher_seconds + per_fleet_fixed[meter])
        for meter, rate in WORKERS_RATES.items()
    )
    return gross.quantize(Decimal("0.01"), rounding=ROUND_CEILING)


def usage_period(now: datetime) -> tuple[datetime, datetime]:
    """Calendar months in UTC define Bifrost's policy window."""
    if now.tzinfo is None:
        raise ValueError("Cloudflare usage requires a timezone-aware clock")
    start = now.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
    return start, end


def _cost(launcher_seconds, slot_seconds, dispatched_runs, sdk_requests):
    return (
        WORKERS_PAID_MINIMUM_USD
        + _DURATION_USD_PER_SECOND * (launcher_seconds + slot_seconds)
        + _ACTIVITY_USD * (dispatched_runs + sdk_requests)
    )


@dataclass(frozen=True, slots=True)
class WorkersLedgerUsage:
    launcher_seconds: Decimal
    slot_seconds: Decimal
    dispatched_runs: int
    sdk_requests: int

    def estimate(self, rate_version: str = WORKERS_RATE_TABLE_VERSION) -> Decimal:
        if rate_version != WORKERS_RATE_TABLE_VERSION:
            raise ValueError("Cloudflare usage requires the current rate version")
        if not self.launcher_seconds.is_finite() or not self.slot_seconds.is_finite() or not (
            0 <= self.launcher_seconds <= self.slot_seconds <= 4 * self.launcher_seconds
            and self.dispatched_runs >= 0 and self.sdk_requests >= 0
        ):
            raise ValueError("Cloudflare usage ledger is inconsistent")
        return _cost(self.launcher_seconds, self.slot_seconds, self.dispatched_runs, self.sdk_requests)


def _totals(installation_id, as_of):
    return func.cloudflare_usage_totals(installation_id, as_of).table_valued(
        column("launcher_seconds", Numeric), column("slot_seconds", Numeric),
        column("dispatched_runs", Numeric), column("sdk_requests", Numeric),
        column("complete", Boolean),
    )


async def read_workers_usage(db: AsyncSession, installation_id: UUID, as_of: datetime) -> WorkersLedgerUsage | None:
    totals = _totals(installation_id, as_of)
    row = (await db.execute(select(totals))).one()
    if not row.complete:
        return None
    return WorkersLedgerUsage(row.launcher_seconds, row.slot_seconds, int(row.dispatched_runs), int(row.sdk_requests))


def cloudflare_estimated_usage() -> ColumnElement[Decimal | None]:
    """Price the live account ledger in the same database snapshot as admission."""
    installation = CloudflareInstallation
    totals = _totals(installation.id, func.statement_timestamp())
    return select(case(
        (totals.c.complete & (installation.policy["rate_table_version"].as_string() == WORKERS_RATE_TABLE_VERSION),
         _cost(totals.c.launcher_seconds, totals.c.slot_seconds, totals.c.dispatched_runs, totals.c.sdk_requests)),
    )).select_from(totals).correlate(installation).scalar_subquery()


def cloudflare_usage_within_limit() -> ColumnElement[bool]:
    return func.coalesce(
        cloudflare_estimated_usage() < CloudflareInstallation.policy["limits"]["monthly_limit_usd"].as_numeric(20, 6),
        False,
    )


async def record_sdk_gateway_request(db: AsyncSession, installation_id: UUID) -> None:
    activity = CloudflareUsageActivity
    await db.execute(insert(activity).values(
        installation_id=installation_id, period_start=func.date_trunc("month", func.clock_timestamp(), "UTC"),
        sdk_requests=1,
    ).on_conflict_do_update(
        index_elements=[activity.installation_id, activity.period_start],
        set_={"sdk_requests": activity.sdk_requests + 1},
    ))


