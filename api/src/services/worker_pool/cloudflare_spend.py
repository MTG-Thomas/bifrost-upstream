"""Cloudflare Workers spend envelope and observed admission guard."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from types import MappingProxyType
from uuid import UUID

from pydantic import ValidationError
from shared.models import CloudflareLauncherUsage
from sqlalchemy import Boolean, Numeric, case, cast, column, func, literal, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareUsageActivity,
    CloudflareUsageIncarnation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorkerClaim


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
WORKERS_LEGACY_RATE_TABLE_VERSION = "workers-paid-2026-11-20"
WORKERS_CALIBRATED_RATE_TABLE_VERSION = "workers-paid-2026-11-20-calibrated"
WORKERS_RATE_TABLE_VERSION = "workers-paid-2026-11-20-launcher-receipts"
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
WORKERS_PAID_SUBSCRIPTION_USD = Decimal("5.00")
# Estimate at the published marginal rates, with no account-wide free tiers.
# Each resident 128 MiB object is estimated separately. Work and gateway
# activity use the packaged Worker CPU cap and one request as pricing proxies.
_CPU_MS_PER_ACTIVITY = 30_000
_OBJECT_GIB = Decimal("0.125")
_DURATION_USD_PER_SECOND = _OBJECT_GIB * WORKERS_RATES["do_gb_seconds"].usd / WORKERS_RATES["do_gb_seconds"].unit
_METER_BOUNDS = json.loads(Path(__file__).with_name("cloudflare_meter_bounds.json").read_text())
_LAUNCHER_BOUNDS = _METER_BOUNDS["per_launcher_second"]
_QUALIFICATION = _METER_BOUNDS["qualification"]
_LAUNCHER_CAPS = {
    name: count * _QUALIFICATION["safety_margin"]
    for name, count in _QUALIFICATION["launcher_reference"]["observed"].items()
}
_RECEIPT_METERS = frozenset(_LAUNCHER_CAPS)
_LAUNCHER_USD_PER_SECOND = sum(
    (Decimal(30_425) if meter == "worker_cpu_ms" else Decimal(_LAUNCHER_BOUNDS[meter])) * rate.usd / rate.unit
    for meter, rate in WORKERS_RATES.items()
)
_LEGACY_CPU_PREMIUM_PER_SECOND = WORKERS_RATES["worker_cpu_ms"].cost(Decimal(60_000 - 30_425))
_ACTIVITY_USD = (
    WORKERS_RATES["worker_requests"].cost(Decimal(1))
    + WORKERS_RATES["worker_cpu_ms"].cost(Decimal(_CPU_MS_PER_ACTIVITY))
    + WORKERS_RATES["do_requests"].usd / WORKERS_RATES["do_requests"].unit
)


def launcher_lifetime_rate(bounds: Mapping[str, Decimal]) -> Decimal:
    return _DURATION_USD_PER_SECOND + sum(
        bounds[meter] * rate.usd / rate.unit
        for meter, rate in WORKERS_RATES.items()
        if meter not in _RECEIPT_METERS and meter != "do_gb_seconds"
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
    gross = sum(
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
        _LAUNCHER_USD_PER_SECOND * launcher_seconds
        + _DURATION_USD_PER_SECOND * slot_seconds
        + _ACTIVITY_USD * (dispatched_runs + sdk_requests)
    )


def _legacy_cost(launcher_seconds, slot_seconds, dispatched_runs, sdk_requests):
    return _cost(launcher_seconds, slot_seconds, dispatched_runs, sdk_requests) + (
        launcher_seconds * _LEGACY_CPU_PREMIUM_PER_SECOND
    )


def _reported_cost(launcher_seconds, slot_seconds, dispatched_runs, sdk_requests, meter_cost):
    return (launcher_lifetime_rate({name: Decimal(value) for name, value in _LAUNCHER_BOUNDS.items()})
            * launcher_seconds + _DURATION_USD_PER_SECOND * slot_seconds
            + _ACTIVITY_USD * (dispatched_runs + sdk_requests) + meter_cost)


def launcher_meter_cost(receipt: CloudflareLauncherUsage | None, *, uncertain: bool = False) -> Decimal:
    """Use only sealed counters. Unknown usage reserves the complete allowance."""
    return sum((
        Decimal(_LAUNCHER_CAPS[name] if uncertain or receipt is None else min(_LAUNCHER_CAPS[name], getattr(receipt, name)))
        * WORKERS_RATES[name].usd / WORKERS_RATES[name].unit
        for name in sorted(_RECEIPT_METERS)
    ), Decimal(0))


def _incarnation_cost_sql(installation: type[CloudflareInstallation]):
    usage = CloudflareUsageIncarnation
    now = func.statement_timestamp()
    period = func.date_trunc("month", now, "UTC")
    seconds = func.extract("epoch", func.least(func.coalesce(usage.deleted_at, now), now)
                           - func.greatest(usage.started_at, period))
    line_items = []
    for name in sorted(_RECEIPT_METERS):
        cap = func.coalesce(usage.meter_bounds["caps"][name].as_numeric(20, 0), _LAUNCHER_CAPS[name])
        units = case(
            (usage.deleted_at.is_not(None) & ~usage.meter_uncertain,
             func.least(func.coalesce(usage.meter_final[name].as_numeric(20, 0), cap), cap)),
            else_=cap,
        )
        rate = WORKERS_RATES[name]
        # Do not let the integer-scale counter coerce the fractional price.
        line_items.append(units * literal(rate.usd / rate.unit, type_=Numeric(24, 12)))
    lifetime_rate = cast(usage.meter_bounds["second_usd"].as_string(), Numeric)
    version = usage.meter_rate_version
    priced = case(
        (usage.meter_release_digest.is_not(None) & (version == WORKERS_RATE_TABLE_VERSION) & lifetime_rate.is_not(None),
         seconds * lifetime_rate + sum(line_items)),
        (usage.meter_release_digest.is_not(None) & (version == WORKERS_CALIBRATED_RATE_TABLE_VERSION),
         seconds * _LAUNCHER_USD_PER_SECOND),
        else_=seconds * (_LAUNCHER_USD_PER_SECOND + _LEGACY_CPU_PREMIUM_PER_SECOND),
    )
    owner = aliased(CloudflareInstallation)
    return select(func.coalesce(func.sum(priced), 0)).select_from(usage).join(
        owner, owner.id == usage.installation_id,
    ).where(
        owner.account_id == installation.account_id,
        or_(usage.deleted_at.is_(None), usage.deleted_at > period),
        usage.started_at <= now,
    ).correlate(installation).scalar_subquery()


async def record_launcher_terminal_meter(
    db: AsyncSession, worker_id: UUID, raw: object, *, uncertain: bool,
) -> None:
    row = await db.get(CloudflareUsageIncarnation, worker_id, with_for_update=True)
    if row is None or row.deleted_at is not None:
        return
    try:
        receipt = CloudflareLauncherUsage.model_validate(raw).model_dump()
    except ValidationError:
        row.meter_uncertain = True
        return
    row.meter_uncertain |= uncertain
    row.meter_terminal = {
        name: max(value, (row.meter_terminal or {}).get(name, 0)) for name, value in receipt.items()
    }


async def seal_launcher_meter(db: AsyncSession, worker_id: UUID, receipt: CloudflareLauncherUsage | None) -> None:
    if receipt is None:
        return
    row = await db.get(CloudflareUsageIncarnation, worker_id, with_for_update=True)
    if row is None or row.meter_final is not None:
        return
    counts = receipt.model_dump()
    if any(value < (row.meter_terminal or {}).get(name, 0) for name, value in counts.items()):
        row.meter_uncertain = True
    row.meter_final = counts


@dataclass(frozen=True, slots=True)
class WorkersLedgerUsage:
    launcher_seconds: Decimal
    slot_seconds: Decimal
    dispatched_runs: int
    sdk_requests: int


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
    known_policy = installation.policy["rate_table_version"].as_string().in_(
        (WORKERS_LEGACY_RATE_TABLE_VERSION, WORKERS_CALIBRATED_RATE_TABLE_VERSION, WORKERS_RATE_TABLE_VERSION),
    )
    return select(case(
        (totals.c.complete & known_policy,
         _incarnation_cost_sql(installation) + _DURATION_USD_PER_SECOND * totals.c.slot_seconds
         + _ACTIVITY_USD * (totals.c.dispatched_runs + totals.c.sdk_requests)),
    )).select_from(totals).correlate(installation).scalar_subquery()


def cloudflare_usage_within_limit() -> ColumnElement[bool]:
    return func.coalesce(
        cloudflare_estimated_usage() < CloudflareInstallation.policy["limits"]["monthly_limit_usd"].as_numeric(20, 6),
        False,
    )


async def record_sdk_gateway_request(db: AsyncSession, *, origin: str, execution_id: UUID) -> None:
    """Attribute only the current assignment of an authenticated workflow run."""
    claim = ExecutionWorkerClaim
    worker_id = select(claim.worker_id).where(
        claim.family == "workflow_execution", claim.subject_id == execution_id,
    ).order_by(claim.claimed_at.desc(), claim.id.desc()).limit(1).scalar_subquery()
    pin, installation = CloudflareWorkerReservation, CloudflareInstallation
    attribution = select(
        pin.installation_id, func.date_trunc("month", func.clock_timestamp(), "UTC"), literal(1),
    ).join(installation, installation.id == pin.installation_id).where(
        pin.worker_id == worker_id, installation.worker_ingress_origin == origin,
        installation.retired_at.is_(None),
    )
    activity = CloudflareUsageActivity
    await db.execute(insert(activity).from_select(
        ["installation_id", "period_start", "sdk_requests"], attribution,
    ).on_conflict_do_update(
        index_elements=[activity.installation_id, activity.period_start],
        set_={"sdk_requests": activity.sdk_requests + 1},
    ))
