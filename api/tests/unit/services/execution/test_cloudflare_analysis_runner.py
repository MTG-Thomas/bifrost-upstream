"""The API analyzes source in a resource-limited, cancellable child."""

import asyncio
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

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
def test_synthetic_source_produces_expected_rules(shape, expected_rules):
    measure = Path(__file__).with_name("cloudflare_analysis_measure.py")
    result = subprocess.run(
        [sys.executable, "-I", str(measure), str(runner._WORKER), shape],
        capture_output=True, check=True, timeout=runner._WALL_SECONDS,
    )
    [receipt] = json.loads(result.stdout)
    assert receipt["rules"] == expected_rules


@pytest.mark.parametrize("exit_code", [-6, -9, -11, 1, 0])
async def test_child_exit_without_result_has_budget_rule(exit_code):
    process = SimpleNamespace(
        stdin=SimpleNamespace(write=Mock(), drain=AsyncMock()),
        stdout=SimpleNamespace(readline=AsyncMock(return_value=b"")),
        returncode=exit_code,
    )
    with pytest.raises(runner.AnalysisRejected) as failure:
        await runner._exchange(process, AsyncMock(), {})
    assert failure.value.rule == "CF_PARSE_BUDGET"


async def test_oversized_child_response_has_budget_rule():
    reader = asyncio.StreamReader(limit=8)
    reader.feed_data(b" " * 9 + b"\n")
    process = SimpleNamespace(stdin=SimpleNamespace(write=Mock(), drain=AsyncMock()), stdout=reader)
    with pytest.raises(runner.AnalysisRejected) as failure:
        await runner._exchange(process, AsyncMock(), {})
    assert failure.value.rule == "CF_PARSE_BUDGET"


async def test_serialization_memory_error_has_budget_rule(monkeypatch):
    from bifrost import _cloudflare_analysis_process as worker

    analyzer = SimpleNamespace(analyze=AsyncMock(return_value={}), AnalysisRejected=runner.AnalysisRejected)
    monkeypatch.setattr(worker, "importlib", SimpleNamespace(import_module=lambda _: analyzer))
    monkeypatch.setattr(worker, "sys", SimpleNamespace(stdin=SimpleNamespace(
        buffer=SimpleNamespace(readline=lambda: b'{"diagnostics": false}\n'),
    )))
    send = Mock(side_effect=[MemoryError(), None])
    monkeypatch.setattr(worker, "_send", send)

    await worker._run()
    assert send.call_args.args[0]["rule"] == "CF_PARSE_BUDGET"


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


async def test_linux_review_pins_native_sources_and_baked_wheels():
    files = {
        "entry.py": b"import _ssl\nimport subprocess\nimport example_wheel\nimport helper\ndef run(): return helper.value\n",
        "helper.py": b"value = 1\n",
    }

    async def read(path, maximum):
        return files.get(path)

    proposal = await runner.analyze(
        read, "entry.py", "run", subrequest_allowance=5, cpu_budget_ms=1000,
        linux_packages={"example_wheel": "example_wheel-1-py3-none-any.whl"},
    )
    assert proposal["linux_findings"] == ["CF_NATIVE", "CF_PACKAGE", "CF_PROCESS"]
    assert set(proposal["module_sha256"]) == {"helper.py"}
    assert proposal["packages"] == ["example_wheel-1-py3-none-any.whl"]
    with pytest.raises(runner.AnalysisRejected) as failure:
        await runner.analyze(read, "entry.py", "run", subrequest_allowance=5, cpu_budget_ms=1000, linux_packages={})
    assert failure.value.rule == "CF_PACKAGE"


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


async def test_analysis_wall_deadline_raises_timeout_for_approvals(monkeypatch):
    monkeypatch.setattr(runner, "_WALL_SECONDS", 1)

    async def read(_path: str, _maximum: int) -> bytes:
        await asyncio.Event().wait()
        raise AssertionError("read resumed without cancellation")

    with pytest.raises(TimeoutError, match="analysis timed out"):
        await runner.analyze(read, "entry.py", "run", subrequest_allowance=5, cpu_budget_ms=1000)


async def test_busy_admission_does_not_start_a_child_or_return_a_source_finding(monkeypatch):
    slot = asyncio.Semaphore(1)
    monkeypatch.setattr(runner, "_SLOT", slot)
    monkeypatch.setattr(runner, "_ADMISSION_SECONDS", 0)
    spawn = AsyncMock()
    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", spawn)

    async with slot:
        with pytest.raises(runner.AnalysisBusy):
            await runner.diagnose_file("entry.py", b"import subprocess")
    spawn.assert_not_awaited()
    assert not slot.locked()
