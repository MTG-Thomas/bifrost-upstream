"""Cloudflare Workers spend envelope and observed admission guard."""

import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from math import ceil
from pathlib import Path
from types import MappingProxyType
from uuid import UUID

from pydantic import ValidationError
from shared.models import CloudflareLauncherUsage
from sqlalchemy import (
    Boolean,
    Numeric,
    and_,
    case,
    cast,
    column,
    func,
    literal,
    or_,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from src.core.database import get_db_context
from src.models.orm.cloudflare_capacity import (
    CloudflareEngineUsageKey,
    CloudflareInstallation,
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
WORKERS_RATE_TABLE_VERSION = "workers-paid-2026-11-20-meter-lifecycle"
_QUALIFIED_RATE_VERSIONS = (WORKERS_QUALIFIED_RATE_TABLE_VERSION, WORKERS_RATE_TABLE_VERSION)
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
_SQLITE_INCLUDED_BYTES = 5_000_000_000
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


def launcher_lifetime_rate(bounds: Mapping[str, Decimal]) -> Decimal:
    # Launcher, WorkerLedger and ScopeRegistry each reserve one memory class.
    launcher_cpu = bounds["worker_requests"] * _CPU_MS_PER_ACTIVITY * _WORKER_CPU_MS_USD
    return launcher_cpu + _DURATION_USD_PER_SECOND + _CONTROL_CADENCE_USD + 3 * _INVENTORY_DURATION_DELTA + sum(
        bounds[meter] * rate.usd / rate.unit
        for meter, rate in WORKERS_RATES.items()
        if meter not in LAUNCHER_RECEIPT_METERS and meter not in ("do_gb_seconds", "worker_cpu_ms")
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


_COVERAGE = {
    WORKERS_LEGACY_RATE_TABLE_VERSION: "Legacy launcher and activity reserves.",
    WORKERS_CALIBRATED_RATE_TABLE_VERSION: "Calibrated launcher CPU and activity reserves.",
    WORKERS_RECEIPT_RATE_TABLE_VERSION: "Launcher workload receipts and historical lifetime reserves.",
    WORKERS_CADENCE_RATE_TABLE_VERSION: "Launcher receipts, control cadence and historical tenant host reserves.",
    WORKERS_INVENTORY_RATE_TABLE_VERSION: "Launcher receipts, control cadence and inventory reserves at the historical CPU ceilings.",
    WORKERS_QUALIFIED_RATE_TABLE_VERSION: "Launcher receipts, control cadence and historical qualified CPU reserves.",
    WORKERS_RATE_TABLE_VERSION: "Launcher receipts, control cadence, qualified CPU limits and Dynamic Worker creation reserves.",
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
        func.coalesce(func.sum(activity.qualified_dispatches + activity.qualified_warm_pings), 0),
    ).join(owner, owner.id == activity.installation_id).where(
        owner.account_id == account_id, activity.period_start == period_start,
    ))).one()
    versions = {value or WORKERS_LEGACY_RATE_TABLE_VERSION for value in historical}
    if rated_activity[0]:
        versions.add(WORKERS_CADENCE_RATE_TABLE_VERSION)
    if rated_activity[1]:
        versions.add(WORKERS_INVENTORY_RATE_TABLE_VERSION)
    if rated_activity[3]:
        versions.add(WORKERS_RATE_TABLE_VERSION)
    coverage = [f"{rate}\u00a0– {_COVERAGE[rate]}" for rate in sorted(versions) if rate in _COVERAGE]
    if rated_activity[2]:
        coverage.append("Managed capacity calls include request and deployment CPU reservations.")
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


def _incarnation_cost_sql(installation: type[CloudflareInstallation]):
    usage = CloudflareUsageIncarnation
    now = func.statement_timestamp()
    period = func.date_trunc("month", now, "UTC")
    seconds = func.extract("epoch", func.least(func.coalesce(usage.deleted_at, now), now)
                           - func.greatest(usage.started_at, period))
    line_items = []
    for name in LAUNCHER_RECEIPT_METERS:
        units = func.greatest(
            usage.meter_bounds["caps"][name].as_numeric(20, 0),
            usage.meter_final[name].as_numeric(20, 0),
            usage.meter_terminal[name].as_numeric(20, 0),
        )
        rate = WORKERS_RATES[name]
        # Do not let the integer-scale counter coerce the fractional price.
        line_items.append(units * literal(rate.usd / rate.unit, type_=Numeric(24, 12)))
    version = usage.meter_rate_version
    lifetime_rate = cast(usage.meter_bounds["second_usd"].as_string(), Numeric)
    slot_delta = case(
        (version.in_(_INVENTORY_RATE_VERSIONS), usage.slots * literal(_INVENTORY_DURATION_DELTA, type_=Numeric(24, 12))),
        else_=0,
    )
    receipt_price = case(
        (usage.meter_release_digest.is_not(None) & _has_launcher_caps(usage.meter_bounds) & (lifetime_rate > 0)
         & or_(version.not_in(_QUALIFIED_RATE_VERSIONS), _has_cpu_limits(usage.meter_bounds)),
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
    elapsed = func.extract("epoch", func.least(func.coalesce(usage.deleted_at, now), now)
                           - func.greatest(period, usage.started_at))
    month_seconds = func.extract("epoch", month_end - period)
    measured = func.greatest(
        usage.meter_terminal["sqlite_database_bytes"].as_numeric(24, 0),
        usage.meter_final["sqlite_database_bytes"].as_numeric(24, 0),
    )
    bytes_month = func.coalesce(func.sum(
        func.coalesce(measured, _SQLITE_MISSING_RECEIPT_BYTES) * _STORAGE_MARGIN * elapsed / month_seconds
    ), 0)
    rate_per_byte = WORKERS_RATES["sqlite_stored_gb_month"].usd / 1_000_000_000
    return select(func.greatest(0, bytes_month - _SQLITE_INCLUDED_BYTES) * literal(
        rate_per_byte, type_=Numeric(30, 20),
    )).select_from(usage).join(
        owner, owner.id == usage.installation_id,
    ).where(
        owner.account_id == installation.account_id,
        usage.meter_rate_version.in_(_INVENTORY_RATE_VERSIONS),
        usage.started_at <= now,
        or_(usage.deleted_at.is_(None), usage.deleted_at > period),
    ).correlate(installation).scalar_subquery()


async def record_launcher_terminal_meter(
    db: AsyncSession, worker_id: UUID, raw: object, *, uncertain: bool,
) -> None:
    row = await db.get(CloudflareUsageIncarnation, worker_id, with_for_update=True)
    if row is None or row.deleted_at is not None:
        return
    try:
        receipt = CloudflareLauncherUsage.model_validate(raw).model_dump(exclude={"exhausted"}, exclude_none=True)
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
    counts = receipt.model_dump()
    if any(counts[name] < (row.meter_terminal or {}).get(name, 0) for name in LAUNCHER_RECEIPT_METERS):
        row.meter_uncertain = True
    row.meter_final = counts


async def reserve_tenant_host_usage(
    db: AsyncSession, worker_id: UUID, deadline: datetime | None, *,
    workflow_cpu_ms: int | None = None, subrequests: int | None = None, tenant_id: str | None = None,
) -> bool:
    """Persist an upper residency reservation before a route can reach the host."""
    usage = await db.get(CloudflareUsageIncarnation, worker_id)
    if usage is None or usage.deleted_at is not None:
        return False
    if usage.meter_rate_version not in (*_INVENTORY_RATE_VERSIONS, WORKERS_CADENCE_RATE_TABLE_VERSION):
        return True
    if deadline is None:
        return False
    qualified = usage.meter_rate_version in _QUALIFIED_RATE_VERSIONS
    lifecycle_meter = usage.meter_rate_version == WORKERS_RATE_TABLE_VERSION
    cpu = usage.meter_bounds.get("cpu_ms") if qualified and usage.meter_bounds else None
    if qualified and (not valid_cpu_limits(cpu) or type(workflow_cpu_ms) is not int or workflow_cpu_ms < 1
                      or type(subrequests) is not int or not 0 <= subrequests <= 50):
        return False
    now = await db.scalar(select(func.clock_timestamp()))
    assert now is not None
    now = now.astimezone(UTC)
    # Reserve warm residency and the last possible alarm after the warm window.
    # Keep the portions on either side of UTC rollover in their own periods.
    inventory_rate = usage.meter_rate_version in _INVENTORY_RATE_VERSIONS
    tail = _INVENTORY_TAIL_SECONDS if inventory_rate else _TENANT_WARM_SECONDS + _TENANT_WATCHDOG_SECONDS
    seconds_field = "inventory_tenant_seconds" if inventory_rate else "tenant_host_seconds"
    dispatch_field = (
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
            segment[pings_field] = ceil(seconds / _TENANT_WATCHDOG_SECONDS)
        if qualified:
            assert cpu is not None and workflow_cpu_ms is not None and subrequests is not None
            dispatch = int(cursor == now)
            pings = segment[pings_field]
            if lifecycle_meter:
                if tenant_id is None:
                    return False
                # The idle tail reuses the dispatch's daily tenant identity.
                day = cursor.replace(hour=0, minute=0, second=0, microsecond=0)
                while day < until:
                    await _reserve_engine_identity(
                        db, usage.installation_id, f"day:{tenant_id}:{day.date()}", start, 1,
                    )
                    day += timedelta(days=1)
                segment["generation_control_requests"] = 4 * dispatch
            else:
                days = ((until - timedelta(microseconds=1)).date() - cursor.date()).days + 1
                segment["qualified_creations"] = 4 * dispatch + days
            segment.update(qualified_dispatches=dispatch,
                           qualified_cpu_ms=qualified_cpu_reserve(
                               cpu, workflow_cpu_ms, subrequests, dispatch, pings, split_gateway=lifecycle_meter,
                           ))
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
    db: AsyncSession, installation_id: UUID, key: str, period: datetime, units: int,
) -> None:
    journal = CloudflareEngineUsageKey
    claimed = await db.scalar(insert(journal).values(
        installation_id=installation_id, usage_key=key,
    ).on_conflict_do_nothing().returning(journal.usage_key))
    if claimed is not None:
        activity = CloudflareUsageActivity
        await db.execute(insert(activity).values(
            installation_id=installation_id, period_start=period, qualified_creations=units,
        ).on_conflict_do_update(
            index_elements=[activity.installation_id, activity.period_start],
            set_={"qualified_creations": activity.qualified_creations + units},
        ))


async def reserve_engine_generation(db: AsyncSession, worker_id: UUID, identity: str) -> bool:
    usage = await db.get(CloudflareUsageIncarnation, worker_id)
    if (usage is None or usage.deleted_at is not None or usage.meter_release_digest is None
            or usage.meter_rate_version != WORKERS_RATE_TABLE_VERSION):
        return False
    now = await db.scalar(select(func.clock_timestamp()))
    assert now is not None
    period, _ = usage_period(now)
    await _reserve_engine_identity(
        db, usage.installation_id, f"generation:{usage.meter_release_digest}:{identity}", period, 4,
    )
    return True


def valid_cpu_limits(cpu: object) -> bool:
    return isinstance(cpu, Mapping) and set(cpu) == set(CPU_LIMIT_PATHS) and all(
        type(value) is int and 0 < value <= (300_000 if name == "startup" else 30_000) for name, value in cpu.items()
    )


def qualified_cpu_reserve(
    limits: Mapping[str, int], approved_cpu_ms: int, subrequests: int, dispatches: int, pings: int,
    *, split_gateway: bool = True,
) -> int:
    # Up to three warm RPCs, one engine invocation, plus any approved gateway
    # calls. The exercise can make one gateway call carrying its dummy handle.
    execution = min(approved_cpu_ms, limits["workflow"])
    gateway = limits["gateway"] + (_CPU_MS_PER_ACTIVITY if split_gateway else 0)
    dispatch = execution + 3 * max(limits["boot"], limits["warm"]) + 4 * limits["startup"] + subrequests * gateway
    ping = limits["exercise"] + limits["startup"] + gateway
    return dispatches * dispatch + pings * ping


async def record_capacity_request(db: AsyncSession, installation_id: UUID, cpu_ms: int) -> None:
    """Reserve the enrolled capacity script's request and CPU before sending."""
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
        & or_(policy_version.not_in(_QUALIFIED_RATE_VERSIONS), _has_cpu_limits(bounds)),
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
        + activity.qualified_warm_pings * literal(_QUALIFIED_PING_USD, type_=Numeric(24, 12))
        + activity.qualified_creations * literal(_DYNAMIC_CREATION_USD, type_=Numeric(24, 12))
        + activity.generation_control_requests * literal(_DO_REQUEST_USD, type_=Numeric(24, 12))
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
