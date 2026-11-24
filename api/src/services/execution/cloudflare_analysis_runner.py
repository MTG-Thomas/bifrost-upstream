"""Run Cloudflare analysis off-loop with a hard CPU and address-space limit."""

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from bifrost import _cloudflare_analysis_process
from bifrost.cloudflare_analysis import (
    MAX_SOURCE_BYTES,
    MAX_SUBREQUESTS,
    AnalysisRejected,
    Finding,
    SourceReader,
    valid_source_path,
)

_WORKER = Path(_cloudflare_analysis_process.__file__).resolve()
_SLOT = asyncio.Semaphore(1)
_ADMISSION_SECONDS = 2
_WALL_SECONDS = 20
_MAX_MESSAGE_BYTES = 2 * 1024**2


class AnalysisBusy(TimeoutError):
    """No analysis slot became available before the admission deadline."""


def _child_refusal() -> AnalysisRejected:
    return AnalysisRejected("Workspace analysis child exceeded its resource or response budget", "CF_PARSE_BUDGET")


async def _exchange(
    process: asyncio.subprocess.Process, read: SourceReader, options: dict[str, Any],
) -> Any:
    assert process.stdin is not None and process.stdout is not None
    writer = process.stdin

    def send(message: dict[str, Any]) -> None:
        writer.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")

    async def drain() -> None:
        try:
            await writer.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise _child_refusal() from exc

    send(options)
    await drain()
    for _ in range(129):
        try:
            line = await process.stdout.readline()
        except ValueError as exc:
            raise _child_refusal() from exc
        if not line:
            raise _child_refusal()
        try:
            response = json.loads(line)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise _child_refusal() from exc
        if not isinstance(response, dict):
            raise _child_refusal()
        if "result" in response:
            if await process.wait() != 0:
                raise _child_refusal()
            return response["result"]
        if "error" in response:
            error = AnalysisRejected(response["error"], response["rule"], detail=response.get("detail"))
            error.path, error.line, error.column = response.get("path"), response.get("line"), response.get("column")
            raise error
        path, maximum = response.get("read"), response.get("maximum")
        if not isinstance(path, str) or not isinstance(maximum, int):
            raise _child_refusal()
        if not valid_source_path(path) or not 0 <= maximum <= MAX_SOURCE_BYTES:
            raise _child_refusal()
        try:
            content = await read(path, maximum)
        except AnalysisRejected as exc:
            # Approval readers own observation fencing and retry semantics.
            # Diagnostics can preserve earlier findings on a read refusal.
            if not options["diagnostics"]:
                raise
            send({"error": str(exc), "rule": exc.rule})
        else:
            content = content[:maximum + 1] if content is not None else None
            send({"size": len(content) if content is not None else None})
            if content is not None:
                writer.write(content)
        await drain()
    raise _child_refusal()


async def _run(read: SourceReader, **options: Any) -> Any:
    try:
        async with asyncio.timeout(_ADMISSION_SECONDS):
            await _SLOT.acquire()
    except TimeoutError as exc:
        raise AnalysisBusy("Cloudflare analysis is busy") from exc
    try:
        return await _run_admitted(read, options)
    finally:
        _SLOT.release()


async def _run_admitted(read: SourceReader, options: dict[str, Any]) -> Any:
    deadline = asyncio.timeout(_WALL_SECONDS)
    try:
        async with deadline:
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-I", str(_WORKER), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL, limit=_MAX_MESSAGE_BYTES, env={},
            )
            try:
                return await _exchange(process, read, options)
            finally:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    # Closing the pipes also releases a paused reader if an
                    # oversized or incomplete response was interrupted.
                    if process.stdin is not None:
                        process.stdin.close()
                    if process.stdout is not None:
                        await process.stdout.read()
                    await process.wait()
    except TimeoutError as exc:
        if not deadline.expired():
            raise
        raise TimeoutError("Source analysis timed out") from exc


async def analyze(
    read: SourceReader, entry_path: str, function_name: str | None,
    *, subrequest_allowance: int, cpu_budget_ms: int, linux_packages: dict[str, str] | None = None,
) -> dict[str, object]:
    return await _run(
        read, diagnostics=False, entry_path=entry_path, function_name=function_name,
        subrequest_allowance=subrequest_allowance, cpu_budget_ms=cpu_budget_ms,
        linux_packages=linux_packages,
    )


async def diagnose(
    read: SourceReader, path: str, function_name: str | tuple[str, ...] | None = None,
    *, single_file: bool = False, subrequest_allowance: int = MAX_SUBREQUESTS, cpu_budget_ms: int = 1000,
) -> list[Finding]:
    try:
        result = await _run(
            read, diagnostics=True, path=path, function_name=function_name, single_file=single_file,
            subrequest_allowance=subrequest_allowance, cpu_budget_ms=cpu_budget_ms,
        )
    except AnalysisRejected as exc:
        exc.locate(path)
        return [Finding.from_refusal(exc)]
    except AnalysisBusy:
        raise
    except TimeoutError:
        refusal = AnalysisRejected("Source analysis timed out", "CF_PARSE_BUDGET")
        refusal.locate(path)
        return [Finding.from_refusal(refusal)]
    return [Finding(**finding) for finding in result]


async def diagnose_file(path: str, source: bytes) -> list[Finding]:
    async def read(_path: str, _maximum: int) -> bytes:
        return source

    return await diagnose(read, path, single_file=True)
