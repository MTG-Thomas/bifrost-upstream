import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from src.services.pool_cadence import (
    CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS,
    wait_for_next_pass,
)
from src.services.work_dispatch import WAKE_WORKFLOW
from src.services.worker_pool import cloudflare_supervisor, controller
from src.services.worker_pool.cloudflare_timing import worker_terminal_timing


def test_workers_launch_age_is_separate_from_reconciliation_cadence() -> None:
    assert CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS == 2
    assert controller.SWEEP_SECONDS == 5
    assert controller.RECONCILE_SECONDS == 2


def test_launch_age_schedules_existing_wake_without_setting_it_early(monkeypatch) -> None:
    loop = Mock()
    wake = Mock()
    monkeypatch.setattr(cloudflare_supervisor, "asyncio", SimpleNamespace(get_running_loop=lambda: loop))
    monkeypatch.setattr(cloudflare_supervisor, "time", SimpleNamespace(
        get_clock_info=lambda _: SimpleNamespace(resolution=1e-9),
    ))

    timer = cloudflare_supervisor._schedule_launch_wake(wake, 1.5)

    loop.call_later.assert_called_once_with(1.5 + 1e-9, wake.set)
    wake.set.assert_not_called()
    assert timer is loop.call_later.return_value
    loop.call_later.call_args.args[1]()
    wake.set.assert_called_once()


@pytest.mark.asyncio
async def test_launch_wake_delay_is_relative_to_database_time(monkeypatch) -> None:
    queued_at = datetime(2026, 11, 22, tzinfo=UTC)
    result = Mock()
    result.one.return_value = (queued_at + timedelta(milliseconds=500), queued_at)
    db = SimpleNamespace(execute=AsyncMock(return_value=result))
    context = MagicMock()
    context.return_value.__aenter__.return_value = db
    monkeypatch.setattr(cloudflare_supervisor, "get_db_context", context)

    assert await cloudflare_supervisor._next_launch_age(cloudflare_supervisor.time.monotonic()) == (
        queued_at + timedelta(seconds=2), 1.5,
    )


def test_terminal_timing_accepts_only_known_integer_stamps() -> None:
    assert worker_terminal_timing({
        "engine_ready_at": 1795305600000,
        "entry_at": True,
        "tenant": "ignored",
    }) == {"engine_ready_at": datetime(2026, 11, 22, tzinfo=UTC)}


@pytest.mark.asyncio
async def test_early_workflow_wake_keeps_the_launch_deadline(monkeypatch) -> None:
    queued_at = datetime(2026, 11, 22, tzinfo=UTC)
    deadline = queued_at + cloudflare_supervisor.LAUNCH_AGE
    now = queued_at
    stop = asyncio.Event()
    binding = object()
    passes = []
    scheduled = []

    async def reconcile(scope, _settings, _http, controllers, *, kubernetes):
        passes.append(now)
        controllers[scope] = SimpleNamespace(burst_launch_allowed=True, id="fixture")

    async def listen(_redis_url, wakes):
        wakes[WAKE_WORKFLOW].set()

    async def next_age(_since):
        return (deadline, (deadline - now).total_seconds()) if now < deadline else None

    def schedule(wake, at):
        timer = Mock()
        scheduled.append((timer, at, wake.set))
        return timer

    async def wait(stop, wake, *, since, spacing, sweep):
        nonlocal now
        if len(passes) == 1:
            await asyncio.sleep(0)  # Let the ordinary workflow hint reach the event.
            now = deadline - timedelta(milliseconds=1)
        elif len(passes) == 2:
            assert not wake.is_set()
            scheduled[0][0].cancel.assert_not_called()
            now = deadline
            scheduled[0][2]()
        else:
            stop.set()
        assert wake.is_set() or stop.is_set()
        await wait_for_next_pass(stop, wake, since=since, spacing=0, sweep=sweep)

    monkeypatch.setattr(cloudflare_supervisor, "datetime", SimpleNamespace(now=lambda _: now))
    monkeypatch.setattr(cloudflare_supervisor, "_installations", AsyncMock(return_value=[binding]))
    monkeypatch.setattr(cloudflare_supervisor, "_reconcile_scope", reconcile)
    monkeypatch.setattr(cloudflare_supervisor, "_next_launch_age", next_age)
    monkeypatch.setattr(cloudflare_supervisor, "_schedule_launch_wake", schedule)
    monkeypatch.setattr(cloudflare_supervisor, "listen_for_pool_wake", listen)
    monkeypatch.setattr(cloudflare_supervisor, "wait_for_next_pass", wait)
    monkeypatch.setattr(cloudflare_supervisor, "may_manage_jobs", AsyncMock(return_value=False))
    monkeypatch.setattr(cloudflare_supervisor, "_withdraw_live", AsyncMock())
    monkeypatch.setattr(cloudflare_supervisor, "RECONCILE_INTERVAL_SECONDS", float("inf"))
    monkeypatch.setattr(cloudflare_supervisor, "schedule_cloudflare_reconcile", AsyncMock())
    monkeypatch.setattr(cloudflare_supervisor, "prune_spend_receipt_keys", AsyncMock())
    monkeypatch.setattr(cloudflare_supervisor, "get_db_context", MagicMock())

    await cloudflare_supervisor.cloudflare_supervisor_loop(
        stop, SimpleNamespace(redis_url="redis://fixture"),
        lease=SimpleNamespace(holds_lease=lambda: True, seconds_remaining=lambda: 1),
    )
    assert passes == [queued_at, deadline - timedelta(milliseconds=1), deadline]
    assert [delay for _, delay, _ in scheduled] == [2.0]
    scheduled[0][0].cancel.assert_called_once()
