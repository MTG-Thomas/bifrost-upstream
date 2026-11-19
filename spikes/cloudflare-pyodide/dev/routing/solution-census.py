"""Count Cloudflare Workers diagnostics without printing source identities."""

import argparse
import asyncio
import json
from pathlib import Path

from bifrost.cloudflare_census import workspace_reports
from bifrost.cloudflare_report import count_rules


async def census(root: Path, allowances: list[int], cpu_ms: int) -> dict[int, dict[str, int]]:
    return {
        allowance: count_rules(await workspace_reports(root, allowance=allowance, cpu_budget_ms=cpu_ms))
        for allowance in allowances
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workspace", type=Path)
    parser.add_argument("--allowance", type=int, action="append")
    parser.add_argument("--cpu", type=int, default=1000)
    args = parser.parse_args()
    try:
        report = asyncio.run(census(args.workspace.resolve(strict=True), args.allowance or [12, 50], args.cpu))
    except Exception as exc:
        parser.exit(1, f"Census could not read the workspace ({type(exc).__name__}).\n")
    print(json.dumps(report, indent=2, sort_keys=True))
