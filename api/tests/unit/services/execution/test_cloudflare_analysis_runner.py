"""The API analyzes source in a resource-limited, cancellable child."""

import asyncio
import json
import subprocess
import sys
from pathlib import Path

import pytest

from bifrost._cloudflare_analysis_process import ADDRESS_SPACE_BYTES, CPU_SECONDS
from src.services.execution import cloudflare_analysis_runner as runner


@pytest.mark.parametrize(
    ("shape", "expected_rules"),
    [
        ("concat", ["CF_URL_REVIEW"]),
        ("f-string", ["CF_URL_REVIEW"]),
        ("dense-list", []),
        ("dense-token-refusal", ["CF_PARSE_BUDGET"]),
        ("nested-scopes", []),
        ("large-literal", ["CF_URL_REVIEW"]),
    ],
)
def test_synthetic_source_stays_within_analysis_budget(shape, expected_rules):
    measure = Path(__file__).with_name("cloudflare_analysis_measure.py")
    result = subprocess.run(
        [sys.executable, "-I", str(measure), str(runner._WORKER), shape],
        capture_output=True, check=True, timeout=runner._WALL_SECONDS,
    )
    [receipt] = json.loads(result.stdout)
    assert receipt["rules"] == expected_rules
    assert receipt["wall_s"] < runner._WALL_SECONDS and receipt["cpu_s"] < CPU_SECONDS
    assert receipt["peak_rss_mib"] < ADDRESS_SPACE_BYTES / 1024**2


async def test_child_limits_are_installed_before_the_first_source_read(monkeypatch):
    start = asyncio.create_subprocess_exec
    process = None

    async def spawn(*args, **kwargs):
        nonlocal process
        process = await start(*args, **kwargs)
        return process

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", spawn)

    async def read(_path: str, _maximum: int) -> bytes:
        assert process is not None
        limits = Path(f"/proc/{process.pid}/limits").read_text().splitlines()
        cpu = next(line for line in limits if line.startswith("Max cpu time")).split()
        memory = next(line for line in limits if line.startswith("Max address space")).split()
        assert cpu[-3:] == [str(CPU_SECONDS), str(CPU_SECONDS + 1), "seconds"]
        assert memory[-3:] == [str(ADDRESS_SPACE_BYTES), str(ADDRESS_SPACE_BYTES), "bytes"]
        return b"def run(): return 1\n"

    proposal = await runner.analyze(read, "entry.py", "run", subrequest_allowance=5, cpu_budget_ms=1000)
    assert proposal["estimated_subrequests"] == 1
    assert process is not None and process.returncode == 0


async def test_cancellation_reaps_child_waiting_for_source(monkeypatch):
    start = asyncio.create_subprocess_exec
    entered = asyncio.Event()
    process = None

    async def spawn(*args, **kwargs):
        nonlocal process
        process = await start(*args, **kwargs)
        return process

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", spawn)

    async def read(_path: str, _maximum: int) -> bytes:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("read resumed without cancellation")

    task = asyncio.create_task(runner.diagnose(read, "entry.py"))
    await entered.wait()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
    assert process is not None and process.returncode is not None


async def test_wall_deadline_is_retryable_for_approvals(monkeypatch):
    monkeypatch.setattr(runner, "_WALL_SECONDS", 1)

    async def read(_path: str, _maximum: int) -> bytes:
        await asyncio.Event().wait()
        raise AssertionError("read resumed without cancellation")

    with pytest.raises(TimeoutError, match="analysis timed out"):
        await runner.analyze(read, "entry.py", "run", subrequest_allowance=5, cpu_budget_ms=1000)
