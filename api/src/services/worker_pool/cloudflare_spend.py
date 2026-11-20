"""Cloudflare Workers spend envelope and observed admission guard."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from types import MappingProxyType
from typing import Literal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.sql.elements import ColumnElement

from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityUsage,
    CloudflareInstallation,
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
MAX_USAGE_AGE = timedelta(minutes=15)


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


@dataclass(frozen=True, slots=True)
class UsageSample:
    installation_id: UUID
    account_id: str
    period_start: datetime
    period_end: datetime
    measured_at: datetime
    attributed_usd: Decimal


def cloudflare_spend_status(
    sample: UsageSample | None,
    *,
    installation_id: UUID,
    account_id: str,
    monthly_limit_usd: Decimal,
    now: datetime,
) -> Literal["ready", "spend_data_stale", "budget_reached"]:
    """Close admission on unattributed, stale, future, or prior-period usage."""
    if (
        sample is None
        or (sample.installation_id, sample.account_id) != (installation_id, account_id)
        or not (sample.period_start <= sample.measured_at <= now < sample.period_end)
        or now - sample.measured_at > MAX_USAGE_AGE
        or not sample.attributed_usd.is_finite()
        or sample.attributed_usd < 0
    ):
        return "spend_data_stale"
    if not monthly_limit_usd.is_finite() or monthly_limit_usd <= sample.attributed_usd:
        return "budget_reached"
    return "ready"


def cloudflare_usage_within_limit() -> ColumnElement[bool]:
    """Correlate fresh usage evidence with the current installation and policy."""
    usage = CloudflareCapacityUsage
    installation = CloudflareInstallation
    now = func.statement_timestamp()
    return select(usage.installation_id).where(
        usage.installation_id == installation.id,
        usage.account_id == installation.account_id,
        usage.deployment_revision == installation.desired_revision,
        usage.rate_table_version == WORKERS_RATE_TABLE_VERSION,
        usage.rate_table_version == installation.policy["rate_table_version"].as_string(),
        usage.bounds_digest == installation.policy["bounds_digest"].as_string(),
        usage.period_start <= usage.measured_at,
        usage.measured_at <= now,
        usage.measured_at >= now - MAX_USAGE_AGE,
        now < usage.period_end,
        usage.attributed_usd < installation.policy["limits"]["monthly_limit_usd"].as_numeric(20, 6),
    ).correlate(installation).exists()
