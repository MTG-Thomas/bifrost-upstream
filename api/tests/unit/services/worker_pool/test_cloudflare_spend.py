from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from src.services.worker_pool.cloudflare_spend import (
    WORKERS_RATES,
    UsageSample,
    cloudflare_spend_status,
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
    ) == Decimal("19.97")


def test_unbounded_meter_rejects_cost_estimate() -> None:
    with pytest.raises(ValueError, match="bounds are incomplete"):
        workers_fleet_envelope_usd(
            launcher_cap=1,
            max_lifetime_seconds=3600,
            cleanup_margin_seconds=600,
            per_launcher_second={},
            per_fleet_fixed=dict.fromkeys(WORKERS_RATES, Decimal(0)),
        )


def test_spend_guard_requires_attributable_current_sample_below_limit() -> None:
    now = datetime(2026, 11, 20, tzinfo=UTC)
    installation_id = uuid4()
    account_id = "a" * 32
    sample = UsageSample(
        installation_id, account_id, now - timedelta(days=30), now + timedelta(days=1),
        now, Decimal("9.99"),
    )

    def status(candidate: UsageSample | None) -> str:
        return cloudflare_spend_status(
            candidate, installation_id=installation_id, account_id=account_id,
            monthly_limit_usd=Decimal(10), now=now,
        )

    assert status(sample) == "ready"
    assert status(replace(sample, attributed_usd=Decimal("10.00"))) == "budget_reached"
    assert status(None) == "spend_data_stale"
    assert status(replace(sample, account_id="b" * 32)) == "spend_data_stale"
    assert status(replace(sample, measured_at=now + timedelta(seconds=1))) == "spend_data_stale"
    assert status(replace(sample, measured_at=now - timedelta(minutes=16))) == "spend_data_stale"
    assert status(replace(sample, measured_at=now - timedelta(seconds=1), period_end=now)) == "spend_data_stale"
