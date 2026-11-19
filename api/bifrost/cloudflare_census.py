"""Offline workspace inventory shared by the CLI report and Solution census."""

from dataclasses import replace
from pathlib import Path

from bifrost.cloudflare_report import ArtifactReport, inspect_bundle
from bifrost.solution_definition import read_solution_definition
from bifrost.solution_descriptor import DESCRIPTOR_FILENAME


async def workspace_reports(root: Path, *, allowance: int, cpu_budget_ms: int) -> list[ArtifactReport]:
    from bifrost.commands.solution import _iter_python_paths

    root = root.resolve(strict=True)
    if (root / DESCRIPTOR_FILENAME).is_file():
        workspaces = [root]
    else:
        workspaces = [root]
        for descriptor in sorted(root.rglob(DESCRIPTOR_FILENAME)):
            workspace = descriptor.parent
            parts = workspace.relative_to(root).parts
            if any(part.startswith(".") for part in parts) or parts[0] in {"reference", "test", "tests"}:
                continue
            if (workspace / ".bifrost").is_dir() and not descriptor.is_symlink():
                workspaces.append(workspace)

    reports = []
    for workspace in workspaces:
        definition = read_solution_definition(workspace)
        workflows = definition.entries("workflows")
        agents = definition.entries("agents")
        if not (workflows or agents):
            continue
        outbound = bool(definition.descriptor and definition.descriptor.allow_outbound_access)
        sources = dict(_iter_python_paths(workspace, definition=definition)) if workflows and not outbound else {}

        async def read(path: str, limit: int, sources: dict[str, Path] = sources) -> bytes | None:
            source_path = sources.get(path)
            if source_path is None:
                return None
            with source_path.open("rb") as source:
                return source.read(limit + 1)

        bundle_reports = await inspect_bundle(
            read, workflows, agents, outbound=outbound,
            subrequest_allowance=allowance, cpu_budget_ms=cpu_budget_ms,
        )
        reports.extend(replace(report, workspace=workspace.relative_to(root).as_posix()) for report in bundle_reports)
    return reports
