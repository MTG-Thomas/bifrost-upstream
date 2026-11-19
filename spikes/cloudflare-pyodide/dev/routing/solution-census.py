"""Count install-local Cloudflare approval candidates without printing source identities."""

import argparse
import asyncio
import json
from collections import Counter
from pathlib import Path

import yaml
from bifrost.commands.solution import _collect_python_files, _collect_workflows
from bifrost.solution_definition import read_solution_definition
from cloudflare_analysis import AnalysisRejected, analyze


async def census(root: Path, allowances: list[int], cpu_ms: int) -> dict:
    counts: Counter[str] = Counter()
    analyses: dict[int, Counter[str]] = {allowance: Counter() for allowance in allowances}
    for descriptor in sorted(root.rglob("bifrost.solution.yaml")):
        workspace = descriptor.parent
        parts = workspace.relative_to(root).parts
        if parts and parts[0] in {"reference", "tests", "test"}:
            continue
        if not (workspace / ".bifrost/workflows.yaml").is_file():
            continue

        definition = read_solution_definition(workspace)
        metadata = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
        workflows = _collect_workflows(workspace, definition=definition)
        counts["installs"] += 1
        counts["entries"] += len(workflows)
        if metadata.get("allow_outbound_access", metadata.get("global_repo_access", False)):
            counts["outbound_installs"] += 1
            counts["outbound_entries"] += len(workflows)
            continue
        sources = _collect_python_files(workspace, definition=definition)

        async def read(path, maximum):
            text = sources.get(path)
            return text.encode() if text is not None else None

        for workflow in workflows:
            if workflow.get("type", "workflow") != "workflow":
                counts["other_type"] += 1
                continue
            if workflow.get("execution_lane") == "fast_path":
                counts["fast_path"] += 1
                continue
            for allowance, analysis_counts in analyses.items():
                try:
                    result = await analyze(
                        read, workflow["path"], workflow["function_name"],
                        subrequest_allowance=allowance, cpu_budget_ms=cpu_ms,
                    )
                except AnalysisRejected:
                    analysis_counts["refused"] += 1
                else:
                    analysis_counts["eligible_after_approval"] += 1
                    analysis_counts["unresolved_url"] += bool(result["unresolved_url_calls"])
    return {"scope": dict(counts), "cpu_budget_ms": cpu_ms, "by_subrequest_allowance": analyses}


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
