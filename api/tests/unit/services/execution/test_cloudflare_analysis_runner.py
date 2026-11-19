"""The API analyzes source in a resource-limited, cancellable child."""

import asyncio
from pathlib import Path

from bifrost._cloudflare_analysis_process import ADDRESS_SPACE_BYTES, CPU_SECONDS
from src.services.execution import cloudflare_analysis_runner as runner


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
