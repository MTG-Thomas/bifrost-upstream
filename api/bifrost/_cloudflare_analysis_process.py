"""Private Linux analysis subprocess. Source is supplied over bounded pipes."""

import asyncio
import importlib
import json
import sys
from dataclasses import asdict
from pathlib import Path

ADDRESS_SPACE_BYTES = 128 * 1024**2
CPU_SECONDS = 8

def _send(message: object) -> None:
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


async def _run() -> None:
    # Start the standalone analyzer without initializing the SDK package.
    analysis = importlib.import_module("cloudflare_analysis")

    options = json.loads(sys.stdin.buffer.readline())

    async def read(path: str, maximum: int) -> bytes | None:
        _send({"read": path, "maximum": maximum})
        response = json.loads(sys.stdin.buffer.readline())
        if "error" in response:
            raise analysis.AnalysisRejected(response["error"], response["rule"])
        size = response["size"]
        return None if size is None else sys.stdin.buffer.read(size)

    try:
        result: object
        if options.pop("diagnostics"):
            if isinstance(options["function_name"], list):
                options["function_name"] = tuple(options["function_name"])
            result = [asdict(finding) for finding in await analysis.diagnose(read, **options)]
        else:
            result = await analysis.analyze(read, **options)
        _send({"result": result})
    except analysis.AnalysisRejected as exc:
        _send({
            "error": str(exc), "rule": exc.rule, "detail": exc.detail,
            "path": exc.path, "line": exc.line, "column": exc.column,
        })
    except MemoryError:
        _send({"error": "Workspace source exceeded the analysis memory limit", "rule": "CF_PARSE_BUDGET"})


if __name__ == "__main__":
    # Apply limits before importing the analyzer, decoding source, or
    # constructing an AST. This interpreter has no storage or DB clients.
    import resource

    resource.setrlimit(resource.RLIMIT_AS, (ADDRESS_SPACE_BYTES, ADDRESS_SPACE_BYTES))
    resource.setrlimit(resource.RLIMIT_CPU, (CPU_SECONDS, CPU_SECONDS + 1))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    asyncio.run(_run())
