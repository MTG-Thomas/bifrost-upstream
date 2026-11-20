from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from src.services.worker_pool.cloudflare_spend import (
    WORKERS_RATES,
    usage_period,
    workers_fleet_envelope_usd,
)


def test_workers_fleet_envelope_includes_cleanup_and_shared_usage() -> None:
    per_second = dict.fromkeys(WORKERS_RATES, Decimal(0))
    per_second.update(worker_cpu_ms=Decimal(1000), do_gb_seconds=Decimal("0.125"))
    fixed = dict.fromkeys(WORKERS_RATES, Decimal(0))
    fixed.update(worker_requests=Decimal(1_000_000), sqlite_stored_gb_month=Decimal(10))

    assert workers_fleet_envelope_usd(
        launcher_cap=2,
        max_lifetime_seconds=3600,
        cleanup_margin_seconds=600,
        per_launcher_second=per_second,
        per_fleet_fixed=fixed,
    ) == Decimal("14.97")


def test_unbounded_meter_rejects_cost_estimate() -> None:
    with pytest.raises(ValueError, match="bounds are incomplete"):
        workers_fleet_envelope_usd(
            launcher_cap=1,
            max_lifetime_seconds=3600,
            cleanup_margin_seconds=600,
            per_launcher_second={},
            per_fleet_fixed=dict.fromkeys(WORKERS_RATES, Decimal(0)),
        )


def test_policy_window_rolls_at_the_utc_month_boundary() -> None:
    start = datetime(2026, 12, 1, tzinfo=UTC)
    next_start = datetime(2027, 1, 1, tzinfo=UTC)
    assert usage_period(next_start - timedelta(microseconds=1)) == (start, next_start)
    assert usage_period(datetime(2026, 12, 31, 19, tzinfo=timezone(timedelta(hours=-5)))) == (
        next_start, datetime(2027, 2, 1, tzinfo=UTC),
    )
