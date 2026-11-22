from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

from src.services.pool_cadence import CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS
from src.services.worker_pool import cloudflare_supervisor, controller
from src.services.worker_pool.cloudflare_timing import worker_terminal_timing


def test_workers_launch_age_is_separate_from_reconciliation_cadence() -> None:
    assert CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS == 2
    assert controller.SWEEP_SECONDS == 5
    assert controller.RECONCILE_SECONDS == 2


def test_launch_age_schedules_existing_wake_without_setting_it_early(monkeypatch) -> None:
    queued_at = datetime(2026, 11, 22, tzinfo=UTC)
    deadline = queued_at + cloudflare_supervisor.LAUNCH_AGE
    now = queued_at + timedelta(seconds=0.5)
    loop = Mock()
    wake = Mock()
    monkeypatch.setattr(cloudflare_supervisor, "asyncio", SimpleNamespace(get_running_loop=lambda: loop))
    monkeypatch.setattr(cloudflare_supervisor, "datetime", SimpleNamespace(now=lambda _: now))
    monkeypatch.setattr(cloudflare_supervisor, "time", SimpleNamespace(
        get_clock_info=lambda _: SimpleNamespace(resolution=1e-9),
    ))

    timer = cloudflare_supervisor._schedule_launch_wake(wake, deadline)

    loop.call_later.assert_called_once_with(1.5 + 1e-9, wake.set)
    wake.set.assert_not_called()
    assert timer is loop.call_later.return_value
    loop.call_later.call_args.args[1]()
    wake.set.assert_called_once()


def test_terminal_timing_accepts_only_known_integer_stamps() -> None:
    assert worker_terminal_timing({
        "engine_ready_at": 1795305600000,
        "entry_at": True,
        "tenant": "ignored",
    }) == {"engine_ready_at": datetime(2026, 11, 22, tzinfo=UTC)}
