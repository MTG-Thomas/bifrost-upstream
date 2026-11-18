"""Bridge the offline workspace census to the server's approval analyzer."""

import argparse
import asyncio
import json
from pathlib import Path

from cloudflare_analysis import AnalysisRejected, analyze


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("entry")
    parser.add_argument("--function")
    parser.add_argument("--fallback", type=Path)
    parser.add_argument("--allowance", type=int, required=True)
    parser.add_argument("--cpu", type=int, required=True)
    args = parser.parse_args()

    async def read(path: str, max_bytes: int) -> bytes | None:
        for root in (args.root, args.fallback):
            if root is None:
                continue
            target = (root / path).resolve()
            if not target.is_relative_to(root.resolve()):
                raise AnalysisRejected("Source resolves outside the workspace")
            try:
                with target.open("rb") as handle:
                    return handle.read(max_bytes + 1)
            except FileNotFoundError:
                continue
        return None

    try:
        result = await analyze(
            read, args.entry, args.function,
            subrequest_allowance=args.allowance, cpu_budget_ms=args.cpu,
        )
    except (ValueError, OSError, TimeoutError) as error:
        print(json.dumps({"error": str(error)}))
    else:
        print(json.dumps(result))


if __name__ == "__main__":
    asyncio.run(main())
