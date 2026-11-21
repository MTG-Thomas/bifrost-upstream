"""Cloudflare Workers spend envelope and observed admission guard."""

import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from math import ceil
from pathlib import Path
from types import MappingProxyType
from uuid import UUID

from pydantic import ValidationError
from shared.models import CloudflareEngineInvocation, CloudflareLauncherUsage
from sqlalchemy import (
    Boolean,
    Numeric,
    and_,
    case,
    cast,
    column,
    delete,
    func,
    literal,
    or_,
    select,
    text,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from src.core.database import get_db_context
from src.models.orm.cloudflare_capacity import (
    CloudflareDispatchMeter,
    CloudflareEngineUsageKey,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareUsageActivity,
    CloudflareUsageIncarnation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorkerClaim
from src.services.pool_cadence import POOL_RECONCILE_SPACING_SECONDS
from src.worker.remote.protocol import CLOUDFLARE_MAX_CONTROL_FRAGMENTS


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
WORKERS_RECEIPT_RATE_TABLE_VERSION = "workers-paid-2026-11-20-launcher-receipts"
WORKERS_CADENCE_RATE_TABLE_VERSION = "workers-paid-2026-11-20-control-cadence"
WORKERS_INVENTORY_RATE_TABLE_VERSION = "workers-paid-2026-11-20-meter-inventory"
WORKERS_QUALIFIED_RATE_TABLE_VERSION = "workers-paid-2026-11-20-qualified-cpu"
WORKERS_LIFECYCLE_RATE_TABLE_VERSION = "workers-paid-2026-11-20-meter-lifecycle"
WORKERS_RATE_TABLE_VERSION = "workers-paid-2026-11-20-settled-usage"
_LIFECYCLE_RATE_VERSIONS = (WORKERS_LIFECYCLE_RATE_TABLE_VERSION, WORKERS_RATE_TABLE_VERSION)
_QUALIFIED_RATE_VERSIONS = (WORKERS_QUALIFIED_RATE_TABLE_VERSION, *_LIFECYCLE_RATE_VERSIONS)
_INVENTORY_RATE_VERSIONS = (WORKERS_INVENTORY_RATE_TABLE_VERSION, *_QUALIFIED_RATE_VERSIONS)
_RECEIPT_RATE_VERSIONS = (
    WORKERS_RECEIPT_RATE_TABLE_VERSION, WORKERS_CADENCE_RATE_TABLE_VERSION, *_INVENTORY_RATE_VERSIONS,
)
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
# Each resident object is estimated separately. Work and gateway
# activity use the packaged Worker CPU cap and one request as pricing proxies.
_CPU_MS_PER_ACTIVITY = 30_000
_DO_GB_SECOND_USD = WORKERS_RATES["do_gb_seconds"].usd / WORKERS_RATES["do_gb_seconds"].unit
_DURATION_USD_PER_SECOND = Decimal("0.125") * _DO_GB_SECOND_USD
# Cloudflare's billing examples convert the 128 MB class to 0.128 GB.
# Earlier rate versions retain their original conversion.
_INVENTORY_DURATION_USD_PER_SECOND = Decimal("0.128") * _DO_GB_SECOND_USD
_INVENTORY_DURATION_DELTA = _INVENTORY_DURATION_USD_PER_SECOND - _DURATION_USD_PER_SECOND
_METER_BOUNDS = json.loads(Path(__file__).with_name("cloudflare_meter_bounds.json").read_text())
_LAUNCHER_BOUNDS = _METER_BOUNDS["per_launcher_second"]
LAUNCHER_RECEIPT_METERS = ("do_requests", "log_events", "sqlite_rows_written")
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
# A poll hits both WorkerLedger and Launcher. Keep additional units for
# watchdog/reconnect and the heartbeat lane. Startup and cleanup have a
# separate burst allowance, even on an incarnation that lives <1 second.
_POLL_DO_PER_SECOND = Decimal(2) / Decimal(str(POOL_RECONCILE_SPACING_SECONDS))
_DO_REQUEST_USD = WORKERS_RATES["do_requests"].usd / WORKERS_RATES["do_requests"].unit
_SQL_WRITE_USD = WORKERS_RATES["sqlite_rows_written"].usd / WORKERS_RATES["sqlite_rows_written"].unit
_LOG_EVENT_USD = WORKERS_RATES["log_events"].usd / WORKERS_RATES["log_events"].unit
# Reserve WorkerLedger and ScopeRegistry residency alongside the launcher.
# ScopeRegistry is shared, so reserving it per incarnation overcounts it.
_CONTROL_CADENCE_USD = (
    (_POLL_DO_PER_SECOND + 2) * _DO_REQUEST_USD + 10 * _SQL_WRITE_USD + 4 * _LOG_EVENT_USD
    + 2 * _DURATION_USD_PER_SECOND
)
_CONTROL_LIFECYCLE_USD = 24 * _DO_REQUEST_USD + 32 * _SQL_WRITE_USD + 12 * _LOG_EVENT_USD
# Each child gets at most 50 approved subrequests. Reserve up to two TenantHost
# RPCs per call, plus 24 storage writes and diagnostics per dispatch.
_TENANT_DISPATCH_USD = 100 * _DO_REQUEST_USD + 24 * _SQL_WRITE_USD + 4 * _LOG_EVENT_USD
_TENANT_WARM_SECONDS = 20 * 60
_TENANT_WATCHDOG_SECONDS = 45
_LEGACY_TENANT_HOST_SECOND_USD = _DURATION_USD_PER_SECOND + (
    2 * _DO_REQUEST_USD + _SQL_WRITE_USD + 8 * _LOG_EVENT_USD
) / _TENANT_WATCHDOG_SECONDS
_WORKER_REQUEST_USD = WORKERS_RATES["worker_requests"].usd / WORKERS_RATES["worker_requests"].unit
_WORKER_CPU_MS_USD = WORKERS_RATES["worker_cpu_ms"].usd / WORKERS_RATES["worker_cpu_ms"].unit
_SQL_READ_USD = WORKERS_RATES["sqlite_rows_read"].usd / WORKERS_RATES["sqlite_rows_read"].unit
_DYNAMIC_CREATION_USD = Decimal("0.002")
_DYNAMIC_STARTUP_MS = 1_000
_DYNAMIC_ENTRY_USD = (
    _DYNAMIC_CREATION_USD + _WORKER_REQUEST_USD + (_CPU_MS_PER_ACTIVITY + _DYNAMIC_STARTUP_MS) * _WORKER_CPU_MS_USD
)
_STORAGE_MARGIN = 10
_SQLITE_OTHER_DO_BYTES_PER_LAUNCHER = 10_000_000
_SQLITE_SCOPE_BYTES = 1_000_000
_SQLITE_MISSING_RECEIPT_BYTES = 10_000_000
# Three possible warm calls, one execution and up to 50 gateway entries.
_INVENTORY_DISPATCH_USD = (
    _TENANT_DISPATCH_USD + (CLOUDFLARE_MAX_CONTROL_FRAGMENTS + 4) * _DO_REQUEST_USD
    + 4 * _DYNAMIC_ENTRY_USD + 50 * (_WORKER_REQUEST_USD + _CPU_MS_PER_ACTIVITY * _WORKER_CPU_MS_USD)
    + 128 * _SQL_READ_USD
)
# A stale warm RPC may run for 20 seconds. One alarm tick includes a 15-second
# exercise and 2-second origin health probe before the next check.
_INVENTORY_TAIL_SECONDS = _TENANT_WARM_SECONDS + _TENANT_WATCHDOG_SECONDS + 20 + 15 + 2
_TENANT_PING_DO_USD = 2 * _DO_REQUEST_USD + 2 * _SQL_WRITE_USD + 8 * _LOG_EVENT_USD + 8 * _SQL_READ_USD
_INVENTORY_TENANT_PING_USD = _DYNAMIC_ENTRY_USD + _TENANT_PING_DO_USD
_QUALIFIED_DISPATCH_USD = (
    _TENANT_DISPATCH_USD + (CLOUDFLARE_MAX_CONTROL_FRAGMENTS + 4) * _DO_REQUEST_USD
    + 54 * _WORKER_REQUEST_USD + 128 * _SQL_READ_USD
)
_QUALIFIED_PING_USD = _TENANT_PING_DO_USD + 2 * _WORKER_REQUEST_USD
CPU_LIMIT_PATHS = ("capacity", "gateway", "workflow", "boot", "warm", "exercise", "startup")


def launcher_root_rate(bounds: Mapping[str, Decimal]) -> Decimal:
    return bounds["worker_requests"] * (_CPU_MS_PER_ACTIVITY * _WORKER_CPU_MS_USD + _WORKER_REQUEST_USD)


def launcher_lifetime_rate(bounds: Mapping[str, Decimal]) -> Decimal:
    # Launcher, WorkerLedger and ScopeRegistry each reserve one memory class.
    return launcher_root_rate(bounds) + _DURATION_USD_PER_SECOND + _CONTROL_CADENCE_USD + 3 * _INVENTORY_DURATION_DELTA + sum(
        bounds[meter] * rate.usd / rate.unit
        for meter, rate in WORKERS_RATES.items()
        if meter not in LAUNCHER_RECEIPT_METERS and meter not in ("do_gb_seconds", "worker_cpu_ms", "worker_requests")
    )


def workers_fleet_envelope_usd(
    *,
    launcher_cap: int,
    max_lifetime_seconds: int,
    cleanup_margin_seconds: int,
    per_launcher_second: Mapping[str, Decimal],
    per_fleet_fixed: Mapping[str, Decimal],
) -> Decimal:
    """Price the release's fleet usage scenario and cleanup allowances."""
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


_COVERAGE = {
    WORKERS_LEGACY_RATE_TABLE_VERSION: "Legacy launcher and activity reserves.",
    WORKERS_CALIBRATED_RATE_TABLE_VERSION: "Calibrated launcher CPU and activity reserves.",
    WORKERS_RECEIPT_RATE_TABLE_VERSION: "Launcher workload receipts and historical lifetime reserves.",
    WORKERS_CADENCE_RATE_TABLE_VERSION: "Launcher receipts, control cadence and historical tenant host reserves.",
    WORKERS_INVENTORY_RATE_TABLE_VERSION: "Launcher receipts, control cadence and inventory reserves at the historical CPU ceilings.",
    WORKERS_QUALIFIED_RATE_TABLE_VERSION: "Launcher receipts, control cadence and historical qualified CPU reserves.",
    WORKERS_LIFECYCLE_RATE_TABLE_VERSION: "Launcher receipts, control cadence, qualified CPU and Dynamic Worker creation reserves.",
    WORKERS_RATE_TABLE_VERSION: "Live reserves and settled foreground engine usage, launcher counters, and identity receipts.",
}


async def spend_coverage(db: AsyncSession, account_id: str, period_start: datetime, version: str) -> list[str]:
    usage, owner = CloudflareUsageIncarnation, CloudflareInstallation
    historical = await db.scalars(select(usage.meter_rate_version).distinct().join(
        owner, owner.id == usage.installation_id,
    ).where(
        owner.account_id == account_id,
        or_(usage.deleted_at.is_(None), usage.deleted_at > period_start,
            usage.meter_rate_version.in_(_INVENTORY_RATE_VERSIONS)
            & or_(owner.retired_at.is_(None), owner.retired_at > period_start)),
    ))
    activity = CloudflareUsageActivity
    # Tenant tails and idle capacity requests can contribute independently
    # of a launcher row in the current period.
    rated_activity = (await db.execute(select(
        func.coalesce(func.sum(activity.tenant_dispatches + activity.tenant_host_seconds), 0),
        func.coalesce(func.sum(activity.inventory_dispatches + activity.inventory_warm_pings), 0),
        func.coalesce(func.sum(activity.capacity_requests), 0),
        func.coalesce(func.sum(activity.qualified_dispatches + activity.qualified_preparations + activity.qualified_warm_pings), 0),
    ).join(owner, owner.id == activity.installation_id).where(
        owner.account_id == account_id, activity.period_start == period_start,
    ))).one()
    versions = {value or WORKERS_LEGACY_RATE_TABLE_VERSION for value in historical}
    if rated_activity[0]:
        versions.add(WORKERS_CADENCE_RATE_TABLE_VERSION)
    if rated_activity[1]:
        versions.add(WORKERS_INVENTORY_RATE_TABLE_VERSION)
    coverage = [f"{rate}\u00a0– {_COVERAGE[rate]}" for rate in sorted(versions) if rate in _COVERAGE]
    if rated_activity[3] and not versions.intersection(_QUALIFIED_RATE_VERSIONS):
        coverage.append("Retained tenant usage includes qualified dispatch and idle-tail reservations.")
    if rated_activity[2]:
        coverage.append("Managed capacity calls include requests and estimated CPU.")
    drift = await db.scalar(select(usage.worker_id).join(owner, owner.id == usage.installation_id).where(
        owner.account_id == account_id, usage.deleted_at >= period_start,
        usage.meter_final["reconciliation_drift"].as_boolean().is_(True),
    ).limit(1))
    if drift is not None:
        coverage.append("Launcher receipts and ledger counts diverged. Unmatched usage keeps its reservation.")
    coverage.append(
        "SQLite storage uses gross rates through verified namespace deletion and includes an estimate for other "
        "Durable Objects. Shared account allowances are excluded."
    )
    coverage.append(
        "Bifrost enforces the monthly target against its estimate. CPU termination can overrun the configured "
        "limits. The pilot observed overruns of about 2–2.3 seconds per terminated invocation."
    )
    if not coverage and version in _COVERAGE:
        coverage.append(f"Configured rate {version} applies to future usage.")
    coverage.append(
        "External traffic, provider retries and overages, exact retained bytes, operator-enabled logs, "
        "and account billing adjustments are outside the estimate."
    )
    return coverage


def _has_launcher_caps(bounds):
    return and_(*(bounds["caps"][name].as_numeric(20, 0) > 0 for name in LAUNCHER_RECEIPT_METERS))


def _has_cpu_limits(bounds):
    return and_(*(bounds["cpu_ms"][name].as_integer().between(1, 300_000 if name == "startup" else 30_000)
                  for name in CPU_LIMIT_PATHS))


def _has_settlement_bounds(bounds):
    return and_(
        bounds["startup_p99_us"].as_integer().between(1, bounds["cpu_ms"]["startup"].as_integer() * 1000),
        cast(bounds["root_second_usd"].as_string(), Numeric).between(
            0, cast(bounds["second_usd"].as_string(), Numeric),
        ),
    )


def _incarnation_cost_sql(installation: type[CloudflareInstallation]):
    usage = CloudflareUsageIncarnation
    now = func.statement_timestamp()
    period = func.date_trunc("month", now, "UTC")
    seconds = func.extract("epoch", func.least(func.coalesce(usage.deleted_at, now), now)
                           - func.greatest(usage.started_at, period))
    version = usage.meter_rate_version
    settled = and_(
        version == WORKERS_RATE_TABLE_VERSION, usage.deleted_at.is_not(None), ~usage.meter_uncertain,
        usage.meter_final["settled"].as_boolean().is_(True)
        | (func.jsonb_typeof(usage.meter_final["routes"]) == "object"),
    )
    line_items = []
    for name in LAUNCHER_RECEIPT_METERS:
        actual = func.greatest(
            usage.meter_final[name].as_numeric(20, 0),
            usage.meter_terminal[name].as_numeric(20, 0),
        )
        units = case((settled, actual), else_=func.greatest(usage.meter_bounds["caps"][name].as_numeric(20, 0), actual))
        rate = WORKERS_RATES[name]
        # Do not let the integer-scale counter coerce the fractional price.
        line_items.append(units * literal(rate.usd / rate.unit, type_=Numeric(24, 12)))
    lifetime_rate = cast(usage.meter_bounds["second_usd"].as_string(), Numeric)
    # The receipt release is private and has no default fetch or scheduled
    # entrypoint. Its named Fetcher relay is reserved independently.
    lifetime_rate -= case((settled, cast(usage.meter_bounds["root_second_usd"].as_string(), Numeric)), else_=0)
    slot_delta = case(
        (version.in_(_INVENTORY_RATE_VERSIONS), usage.slots * literal(_INVENTORY_DURATION_DELTA, type_=Numeric(24, 12))),
        else_=0,
    )
    receipt_price = case(
        (usage.meter_release_digest.is_not(None) & _has_launcher_caps(usage.meter_bounds) & (lifetime_rate > 0)
         & or_(version.not_in(_QUALIFIED_RATE_VERSIONS), _has_cpu_limits(usage.meter_bounds))
         & or_(version != WORKERS_RATE_TABLE_VERSION, _has_settlement_bounds(usage.meter_bounds)),
         seconds * (lifetime_rate + slot_delta) + sum(line_items) + case(
             (version.in_((*_INVENTORY_RATE_VERSIONS, WORKERS_CADENCE_RATE_TABLE_VERSION)), _CONTROL_LIFECYCLE_USD),
             else_=0,
         )),
    )
    priced = case(
        (version.in_(_RECEIPT_RATE_VERSIONS), receipt_price),
        (usage.meter_release_digest.is_not(None) & (version == WORKERS_CALIBRATED_RATE_TABLE_VERSION),
         seconds * _LAUNCHER_USD_PER_SECOND),
        else_=seconds * (_LAUNCHER_USD_PER_SECOND + _LEGACY_CPU_PREMIUM_PER_SECOND),
    )
    owner = aliased(CloudflareInstallation)
    # SUM would silently drop incarnations whose exact release has no bounds.
    return select(case(
        (func.count() == func.count(priced), func.coalesce(func.sum(priced), 0)),
    )).select_from(usage).join(
        owner, owner.id == usage.installation_id,
    ).where(
        owner.account_id == installation.account_id,
        or_(usage.deleted_at.is_(None), usage.deleted_at > period),
        usage.started_at <= now,
    ).correlate(installation).scalar_subquery()


def _retained_storage_cost(installation: type[CloudflareInstallation]):
    usage = CloudflareUsageIncarnation
    owner = aliased(CloudflareInstallation)
    now = func.statement_timestamp()
    period = func.date_trunc("month", now, "UTC")
    month_end = func.timezone("UTC", func.timezone("UTC", period) + text("interval '1 month'"))
    resource = CloudflareOwnedResource
    # The launcher and TenantHost namespaces belong to the script, not
    # the worker incarnation. Retain bytes while any matching deployment
    # could still hold them. ScopeRegistry and WorkerLedger live in capacity.
    related = (
        resource.installation_id == usage.installation_id,
        resource.artifact_digest == usage.meter_release_digest,
        resource.kind == "worker_script",
        or_(resource.target_name.endswith("-launcher"), resource.target_name.endswith("-capacity")),
    )
    namespace_end = select(func.max(case(
        (and_(resource.state == "absent", resource.observed_at.is_not(None)), resource.observed_at),
        else_=now,
    ))).where(*related).correlate(usage).scalar_subquery()
    storage_end = func.greatest(func.coalesce(usage.deleted_at, now), namespace_end)
    elapsed = func.extract("epoch", func.least(storage_end, now) - func.greatest(period, usage.started_at))
    month_seconds = func.extract("epoch", month_end - period)
    measured = func.greatest(
        usage.meter_terminal["sqlite_database_bytes"].as_numeric(24, 0),
        usage.meter_final["sqlite_database_bytes"].as_numeric(24, 0),
    )
    bytes_month = func.coalesce(func.sum(
        (func.coalesce(measured, _SQLITE_MISSING_RECEIPT_BYTES) * _STORAGE_MARGIN
         + _SQLITE_OTHER_DO_BYTES_PER_LAUNCHER) * elapsed / month_seconds
    ), 0)
    rate_per_byte = WORKERS_RATES["sqlite_stored_gb_month"].usd / 1_000_000_000
    scope_owner = aliased(CloudflareInstallation)
    scope_end = case((resource.state == "absent", func.coalesce(resource.observed_at, now)), else_=now)
    scope_seconds = func.extract("epoch", func.least(now, scope_end) - func.greatest(period, scope_owner.consent_at))
    # Use the consent time as an approximate start for the small scope baseline.
    scope_bytes_month = select(func.coalesce(func.sum(
        _SQLITE_SCOPE_BYTES * func.greatest(0, scope_seconds) / month_seconds,
    ), 0)).select_from(resource).join(scope_owner, scope_owner.id == resource.installation_id).where(
        scope_owner.account_id == installation.account_id, scope_end > period,
        resource.kind == "worker_script", resource.target_name.endswith("-capacity"),
        resource.provider_id.is_not(None),
    ).correlate(installation).scalar_subquery()
    return select((bytes_month + scope_bytes_month) * literal(
        rate_per_byte, type_=Numeric(30, 20),
    )).select_from(usage).join(
        owner, owner.id == usage.installation_id,
    ).where(
        owner.account_id == installation.account_id,
        usage.meter_rate_version.in_(_INVENTORY_RATE_VERSIONS),
        usage.started_at <= now,
        storage_end > period,
    ).correlate(installation).scalar_subquery()


async def record_launcher_terminal_meter(
    db: AsyncSession, worker_id: UUID, raw: object, *, uncertain: bool,
) -> None:
    row = await db.get(CloudflareUsageIncarnation, worker_id, with_for_update=True)
    if row is None or row.deleted_at is not None:
        return
    try:
        receipt = CloudflareLauncherUsage.model_validate(raw).model_dump(exclude={"exhausted", "routes"}, exclude_none=True)
    except ValidationError:
        row.meter_uncertain = True
        return
    row.meter_uncertain |= uncertain
    previous = row.meter_terminal or {}
    row.meter_terminal = {**previous, **{name: max(value, previous.get(name, 0)) for name, value in receipt.items()}}


async def seal_launcher_meter(db: AsyncSession, worker_id: UUID, receipt: CloudflareLauncherUsage | None) -> None:
    if receipt is None:
        return
    row = await db.get(CloudflareUsageIncarnation, worker_id, with_for_update=True)
    if row is None or row.meter_final is not None:
        return
    if row.meter_rate_version == WORKERS_RATE_TABLE_VERSION and row.deleted_at is None:
        return
    counts = receipt.model_dump(mode="json", exclude={"routes"}, exclude_none=True)
    if any(counts[name] < (row.meter_terminal or {}).get(name, 0) for name in LAUNCHER_RECEIPT_METERS):
        row.meter_uncertain = True
    if row.meter_rate_version == WORKERS_RATE_TABLE_VERSION and receipt.routes is not None:
        meter = CloudflareDispatchMeter
        booked = await db.scalar(select(func.count()).select_from(meter).where(meter.worker_id == worker_id))
        reported = len(receipt.routes)
        assert booked is not None
        drift = max(booked, reported) > 4 * max(2, min(booked, reported))
        # A skipped preparation records an empty receipt without using a
        # TenantHost RPC. Only workflows witness both counters.
        workflows = sum(not route.startswith("prepare:") for route in receipt.routes)
        counter_drift = workflows > 4 * min(receipt.do_requests, receipt.sqlite_rows_written)
        counts.update(settled=True, reconciliation_drift=drift or counter_drift)
        row.meter_uncertain |= counter_drift
        if not row.meter_uncertain and receipt.routes:
            records = list(await db.scalars(select(meter).where(
                meter.worker_id == worker_id, meter.route_id.in_(receipt.routes),
            )))
            await _settle_foreground(db, row, receipt.routes, records)
    row.meter_final = counts
    if row.deleted_at is not None:
        await db.execute(delete(CloudflareDispatchMeter).where(CloudflareDispatchMeter.worker_id == worker_id))


async def reserve_tenant_host_usage(
    db: AsyncSession, worker_id: UUID, deadline: datetime | None, *,
    workflow_cpu_ms: int | None = None, subrequests: int | None = None, tenant_id: str | None = None,
    route_id: str | None = None,
) -> bool:
    """Book expected usage and startup headroom before dispatch."""
    usage = await db.get(CloudflareUsageIncarnation, worker_id)
    if usage is None or usage.deleted_at is not None:
        return False
    preparation = route_id is not None and route_id.startswith("prepare:")
    if usage.meter_rate_version not in (*_INVENTORY_RATE_VERSIONS, WORKERS_CADENCE_RATE_TABLE_VERSION):
        return not preparation
    if deadline is None:
        return False
    qualified = usage.meter_rate_version in _QUALIFIED_RATE_VERSIONS
    lifecycle_meter = usage.meter_rate_version in _LIFECYCLE_RATE_VERSIONS
    bounds = usage.meter_bounds or {}
    cpu = bounds.get("cpu_ms") if qualified else None
    expected = bounds.get("cpu_expected_ms")
    linger = bounds.get("linger_seconds")
    bounded_linger = type(linger) is int and 0 <= linger <= 300
    if preparation and (usage.meter_rate_version != WORKERS_RATE_TABLE_VERSION or expected is None
                        or not bounded_linger):
        return False
    if qualified and (not valid_cpu_limits(cpu) or (not preparation and (
        type(workflow_cpu_ms) is not int or workflow_cpu_ms < 1 or type(subrequests) is not int or not 0 <= subrequests <= 50
    ))):
        return False
    now = await db.scalar(select(func.clock_timestamp()))
    assert now is not None
    now = now.astimezone(UTC)
    if bounded_linger and deadline <= now:
        return False
    if usage.meter_rate_version == WORKERS_RATE_TABLE_VERSION:
        if not tenant_id or not route_id:
            return False
        meter = CloudflareDispatchMeter
        reserved = await db.scalar(insert(meter).values(
            worker_id=worker_id, route_id=route_id, period_start=usage_period(now)[0], tenant_id=tenant_id,
        ).on_conflict_do_nothing().returning(meter.route_id))
        if reserved is None:
            return True
    # Prior releases retain their deployed alarm pricing. The bounded linger
    # adds duration only, with no scheduled requests or detached engine starts.
    inventory_rate = usage.meter_rate_version in _INVENTORY_RATE_VERSIONS
    tail = 0 if preparation else linger if bounded_linger else (
        _INVENTORY_TAIL_SECONDS if inventory_rate else _TENANT_WARM_SECONDS + _TENANT_WATCHDOG_SECONDS
    )
    seconds_field = "inventory_tenant_seconds" if inventory_rate else "tenant_host_seconds"
    dispatch_field = (
        "qualified_preparations" if preparation else
        "qualified_dispatches" if qualified else "inventory_dispatches" if inventory_rate else "tenant_dispatches"
    )
    pings_field = "qualified_warm_pings" if qualified else "inventory_warm_pings"
    end = max(now, deadline) + timedelta(seconds=tail)
    cursor = now
    periods = []
    while cursor < end:
        start, boundary = usage_period(cursor)
        until = min(boundary, end)
        seconds = ceil((until - cursor).total_seconds())
        segment = {
            "installation_id": usage.installation_id,
            "period_start": start,
            seconds_field: seconds,
            dispatch_field: int(cursor == now),
        }
        if inventory_rate:
            segment[pings_field] = 0 if bounded_linger else ceil(seconds / _TENANT_WATCHDOG_SECONDS)
        if qualified:
            assert cpu is not None
            dispatch = int(cursor == now)
            pings = segment[pings_field]
            if lifecycle_meter:
                if tenant_id is None:
                    return False
                # Only time that can contain engine entry needs a daily identity.
                day = cursor.replace(hour=0, minute=0, second=0, microsecond=0)
                identity_until = min(until, deadline) if bounded_linger else until
                if not preparation:
                    while day < identity_until:
                        await _reserve_engine_identity(
                            db, usage.installation_id, f"day:{tenant_id}:{day.date()}", day, 1,
                        )
                        day += timedelta(days=1)
                segment["generation_control_requests"] = (1 if preparation else 4) * dispatch
            else:
                days = ((until - timedelta(microseconds=1)).date() - cursor.date()).days + 1
                segment["qualified_creations"] = 4 * dispatch + days
            if preparation:
                assert expected is not None
                segment["qualified_cpu_ms"] = dispatch * _prepare_cpu_reserve(cpu, expected)
            else:
                assert workflow_cpu_ms is not None and subrequests is not None
                segment["qualified_cpu_ms"] = qualified_cpu_reserve(
                    cpu, workflow_cpu_ms, subrequests, dispatch, pings,
                    split_gateway=lifecycle_meter, expected=expected,
                )
        periods.append(segment)
        cursor = until
    activity = CloudflareUsageActivity
    reservation = insert(activity).values(periods)
    fields = [seconds_field, dispatch_field]
    if inventory_rate:
        fields.append(pings_field)
    if qualified:
        fields.append("qualified_cpu_ms")
        fields.append("generation_control_requests" if lifecycle_meter else "qualified_creations")
    await db.execute(reservation.on_conflict_do_update(
        index_elements=[activity.installation_id, activity.period_start],
        set_={field: activity.__table__.c[field] + reservation.excluded[field] for field in fields},
    ))
    return True


async def _reserve_engine_identity(
    db: AsyncSession, installation_id: UUID, key: str, booked_at: datetime, units: int,
) -> bool:
    journal = CloudflareEngineUsageKey
    claimed = await db.scalar(insert(journal).values(
        installation_id=installation_id, usage_key=key, booked_at=booked_at,
    ).on_conflict_do_nothing().returning(journal.usage_key))
    if claimed is not None and units:
        activity = CloudflareUsageActivity
        await db.execute(insert(activity).values(
            installation_id=installation_id, period_start=usage_period(booked_at)[0], qualified_creations=units,
        ).on_conflict_do_update(
            index_elements=[activity.installation_id, activity.period_start],
            set_={"qualified_creations": activity.qualified_creations + units},
        ))
    return claimed is not None


async def reserve_engine_generation(db: AsyncSession, worker_id: UUID, identity: str) -> bool:
    usage = await db.get(CloudflareUsageIncarnation, worker_id)
    if (usage is None or usage.deleted_at is not None or usage.meter_release_digest is None
            or usage.meter_rate_version not in _LIFECYCLE_RATE_VERSIONS):
        return False
    now = await db.scalar(select(func.clock_timestamp()))
    assert now is not None
    await _book_daily_engine(db, usage, identity, now)
    return True


async def _book_daily_engine(
    db: AsyncSession, usage: CloudflareUsageIncarnation, identity: str, started_at: datetime,
) -> None:
    day = started_at.astimezone(UTC).date()
    # The Loader identity already embeds the engine entry and runtime digest.
    daily_key = f"daily:{identity}:{day}"
    # The tenant-day forecast pays for one identity. Every additional identity
    # pays its own creation fee.
    tenant = identity.removeprefix("tenant-eng-tenant-py-").split("-", 1)[0]
    day_key = f"day:{tenant}:{day}"
    await _reserve_engine_identity(db, usage.installation_id, day_key, started_at, 1)
    if await _reserve_engine_identity(db, usage.installation_id, daily_key, started_at, 1):
        await _reserve_engine_identity(db, usage.installation_id, f"settled:{day_key}", started_at, -1)


async def _settle_foreground(
    db: AsyncSession, usage: CloudflareUsageIncarnation, observed: dict[str, list[CloudflareEngineInvocation]],
    records: list[CloudflareDispatchMeter],
) -> None:
    bounds = usage.meter_bounds or {}
    cpu = bounds.get("cpu_ms")
    # Existing release snapshots use this key for the expected per-load cost.
    startup_us = bounds.get("startup_p99_us")
    if not valid_cpu_limits(cpu) or type(startup_us) is not int or not 0 < startup_us <= cpu["startup"] * 1000:
        return
    assert cpu is not None
    expected = bounds.get("cpu_expected_ms", cpu)
    generations: dict[tuple[str, date], datetime] = {}
    assert usage.deleted_at is not None
    month, _ = usage_period(datetime.now(UTC))
    oldest, _ = usage_period(month - timedelta(days=1))
    activity = CloudflareUsageActivity
    for record in records:
        invocations = observed[record.route_id]
        kinds = [call.kind for call in invocations]
        if any(not call.identity.startswith(f"tenant-eng-tenant-py-{record.tenant_id}-") for call in invocations):
            continue
        if record.route_id.startswith("prepare:"):
            if kinds not in ([], ["boot"], ["prime"]):
                continue
            entry_reserve, cpu_reserve = 1, _prepare_cpu_reserve(cpu, expected)
        else:
            if kinds[-1:] != ["workflow"] or kinds.count("workflow") != 1:
                continue
            entry_reserve = 4
            cpu_reserve = 4 * cpu["startup"] + 3 * max(expected["boot"], expected["warm"])
        actual = sum(
            startup_us * call.cold
            + (expected["boot"] if call.kind == "boot" else expected["warm"] if call.kind == "prime" else 0) * 1000
            for call in invocations
        )
        await db.execute(update(activity).where(
            activity.installation_id == usage.installation_id, activity.period_start == record.period_start,
        ).values(released_cpu_us=activity.released_cpu_us + cpu_reserve * 1000 - actual,
                 released_engine_requests=activity.released_engine_requests + entry_reserve - len(invocations)))
        for call in invocations:
            if max(oldest, usage.started_at) <= call.started_at <= usage.deleted_at:
                generations[call.identity, call.started_at.astimezone(UTC).date()] = call.started_at
    # An invocation can cross UTC midnight after the loader was authorized.
    for (identity, _), started_at in sorted(generations.items()):
        await _book_daily_engine(db, usage, identity, started_at)


def valid_cpu_limits(cpu: object) -> bool:
    return isinstance(cpu, Mapping) and set(cpu) == set(CPU_LIMIT_PATHS) and all(
        type(value) is int and 0 < value <= (300_000 if name == "startup" else 30_000) for name, value in cpu.items()
    )


def _prepare_cpu_reserve(limits: Mapping[str, int], expected: Mapping[str, int]) -> int:
    return limits["startup"] + max(expected["boot"], expected["warm"])


def qualified_cpu_reserve(
    limits: Mapping[str, int], approved_cpu_ms: int, subrequests: int, dispatches: int, pings: int,
    *, split_gateway: bool = True, expected: Mapping[str, int] | None = None,
) -> int:
    # Up to three warm RPCs, one engine invocation, plus any approved gateway
    # calls. The exercise can make one gateway call carrying its dummy handle.
    cpu = limits if expected is None else expected
    execution = min(approved_cpu_ms, cpu["workflow"])
    # The measured gateway path includes the relay. Historical snapshots
    # priced that relay separately using the launcher script's limit.
    gateway = cpu["gateway"] + (_CPU_MS_PER_ACTIVITY if split_gateway and expected is None else 0)
    dispatch = execution + 3 * max(cpu["boot"], cpu["warm"]) + 4 * limits["startup"] + subrequests * gateway
    ping = cpu["exercise"] + limits["startup"] + gateway
    return dispatches * dispatch + pings * ping


async def prune_spend_receipt_keys(db: AsyncSession) -> None:
    """Expire deduplication detail after the previous UTC billing month."""
    now = await db.scalar(select(func.clock_timestamp()))
    assert now is not None
    start, _ = usage_period(now)
    cutoff, _ = usage_period(start - timedelta(days=1))
    meter = CloudflareDispatchMeter
    await db.execute(delete(meter).where(tuple_(meter.worker_id, meter.route_id).in_(
        select(meter.worker_id, meter.route_id).where(meter.period_start < cutoff).limit(1000)
    )))
    key = CloudflareEngineUsageKey
    # Older tenant-day and settlement keys encode their date but lack booked_at.
    legacy_day = (
        key.booked_at.is_(None)
        & or_(key.usage_key.startswith("day:"), key.usage_key.startswith("settled:day:"))
        & (func.right(key.usage_key, 10) < cutoff.date().isoformat())
    )
    await db.execute(delete(key).where(tuple_(key.installation_id, key.usage_key).in_(
        select(key.installation_id, key.usage_key).where(or_(key.booked_at < cutoff, legacy_day)).limit(1000)
    )))


async def record_capacity_request(db: AsyncSession, installation_id: UUID, cpu_ms: int) -> None:
    """Book one capacity request and its expected CPU before sending."""
    activity = CloudflareUsageActivity
    await db.execute(insert(activity).values(
        installation_id=installation_id, period_start=func.date_trunc("month", func.clock_timestamp(), "UTC"),
        capacity_requests=1, capacity_cpu_ms=cpu_ms,
    ).on_conflict_do_update(
        index_elements=[activity.installation_id, activity.period_start],
        set_={
            "capacity_requests": activity.capacity_requests + 1,
            "capacity_cpu_ms": activity.capacity_cpu_ms + cpu_ms,
        },
    ))


def capacity_request_reservation(installation_id: UUID, cpu_ms: int) -> Callable[[], Awaitable[None]]:
    async def reserve() -> None:
        async with get_db_context() as db:
            await record_capacity_request(db, installation_id, cpu_ms)
    return reserve


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
    policy_version = installation.policy["rate_table_version"].as_string()
    bounds = installation.policy["meter_launcher"]
    known_policy = or_(
        policy_version.in_((WORKERS_LEGACY_RATE_TABLE_VERSION, WORKERS_CALIBRATED_RATE_TABLE_VERSION)),
        policy_version.in_(_RECEIPT_RATE_VERSIONS) & _has_launcher_caps(bounds)
        & or_(policy_version.not_in(_QUALIFIED_RATE_VERSIONS), _has_cpu_limits(bounds))
        & or_(policy_version != WORKERS_RATE_TABLE_VERSION, _has_settlement_bounds(bounds)),
    )
    owner = aliased(CloudflareInstallation)
    activity = CloudflareUsageActivity
    # Qualified claim and SDK events still appear in the account totals.
    # Remove their legacy CPU proxy because pre-dispatch pricing already covers them.
    hosts = select(func.coalesce(func.sum(
        activity.tenant_host_seconds * literal(_LEGACY_TENANT_HOST_SECOND_USD, type_=Numeric(30, 20))
        + activity.tenant_dispatches * literal(_TENANT_DISPATCH_USD, type_=Numeric(24, 12))
        + activity.inventory_tenant_seconds * literal(_INVENTORY_DURATION_USD_PER_SECOND, type_=Numeric(30, 20))
        + activity.inventory_dispatches * literal(_INVENTORY_DISPATCH_USD, type_=Numeric(24, 12))
        + activity.inventory_warm_pings * literal(_INVENTORY_TENANT_PING_USD, type_=Numeric(24, 12))
        + activity.capacity_requests * literal(_WORKER_REQUEST_USD, type_=Numeric(24, 12))
        + activity.capacity_cpu_ms * literal(_WORKER_CPU_MS_USD, type_=Numeric(24, 12))
        + activity.qualified_cpu_ms * literal(_WORKER_CPU_MS_USD, type_=Numeric(24, 12))
        + activity.qualified_dispatches * literal(_QUALIFIED_DISPATCH_USD, type_=Numeric(24, 12))
        + activity.qualified_preparations * literal(_WORKER_REQUEST_USD, type_=Numeric(24, 12))
        + activity.qualified_warm_pings * literal(_QUALIFIED_PING_USD, type_=Numeric(24, 12))
        + activity.qualified_creations * literal(_DYNAMIC_CREATION_USD, type_=Numeric(24, 12))
        + activity.generation_control_requests * literal(_DO_REQUEST_USD, type_=Numeric(24, 12))
        - activity.released_cpu_us * literal(_WORKER_CPU_MS_USD / 1000, type_=Numeric(30, 20))
        - activity.released_engine_requests * literal(_WORKER_REQUEST_USD, type_=Numeric(24, 12))
        - activity.qualified_legacy_events * literal(_CPU_MS_PER_ACTIVITY * _WORKER_CPU_MS_USD, type_=Numeric(24, 12)),
    ), 0)).select_from(activity).join(
        owner, owner.id == activity.installation_id,
    ).where(
        owner.account_id == installation.account_id,
        activity.period_start == func.date_trunc("month", func.statement_timestamp(), "UTC"),
    ).correlate(installation).scalar_subquery()
    return select(case(
        (totals.c.complete & known_policy,
         _incarnation_cost_sql(installation) + _DURATION_USD_PER_SECOND * totals.c.slot_seconds
         + _ACTIVITY_USD * (totals.c.dispatched_runs + totals.c.sdk_requests) + hosts
         + _retained_storage_cost(installation)),
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
    usage = CloudflareUsageIncarnation
    attribution = select(
        pin.installation_id, func.date_trunc("month", func.clock_timestamp(), "UTC"), literal(1),
        case((usage.meter_rate_version.in_(_QUALIFIED_RATE_VERSIONS), 1), else_=0),
    ).join(installation, installation.id == pin.installation_id).outerjoin(usage, usage.worker_id == pin.worker_id).where(
        pin.worker_id == worker_id, installation.worker_ingress_origin == origin,
        installation.retired_at.is_(None),
    )
    activity = CloudflareUsageActivity
    statement = insert(activity).from_select(
        ["installation_id", "period_start", "sdk_requests", "qualified_legacy_events"], attribution,
    )
    await db.execute(statement.on_conflict_do_update(
        index_elements=[activity.installation_id, activity.period_start],
        set_={
            "sdk_requests": activity.sdk_requests + 1,
            "qualified_legacy_events": activity.qualified_legacy_events + statement.excluded.qualified_legacy_events,
        },
    ))
