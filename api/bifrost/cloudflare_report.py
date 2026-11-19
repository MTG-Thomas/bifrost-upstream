"""Cloudflare source diagnostics for an isolated workspace or Solution bundle."""

from collections import Counter
from dataclasses import asdict, dataclass
from typing import Literal

from bifrost.cloudflare_analysis import (
    MAX_SUBREQUESTS,
    Finding,
    SourceReader,
    agent_finding,
    diagnose,
    placement_findings,
)


@dataclass(frozen=True)
class ArtifactReport:
    kind: str
    name: str
    findings: list[Finding]
    path: str | None = None
    function_name: str | None = None
    workspace: str | None = None

    @property
    def status(self) -> Literal["linux", "review", "candidate", "info"]:
        if any(f.severity == "warning" for f in self.findings):
            return "linux"
        if self.kind != "workflow":
            return "info"
        return "review" if self.findings else "candidate"

    def to_dict(self) -> dict:
        return {**asdict(self), "status": self.status}


async def inspect_bundle(
    read: SourceReader,
    workflows: list[dict],
    agents: list[dict],
    *,
    outbound: bool = False,
    subrequest_allowance: int = MAX_SUBREQUESTS,
    cpu_budget_ms: int = 1000,
) -> list[ArtifactReport]:
    """Use the caller's workspace-isolated source reader for every artifact."""
    reports = []
    for workflow in workflows:
        path, function = workflow["path"], workflow["function_name"]
        kind = workflow.get("type", "workflow")
        findings = placement_findings(
            executable_type=kind,
            execution_lane=workflow.get("execution_lane"),
            outbound=outbound,
            path=path,
        )
        if not findings:
            findings = await diagnose(
                read, path, function, subrequest_allowance=subrequest_allowance, cpu_budget_ms=cpu_budget_ms,
            )
        reports.append(ArtifactReport(kind, workflow.get("name") or function, findings, path, function))
    for agent in agents:
        reports.append(ArtifactReport("agent", agent["name"], [agent_finding()]))
    return reports


def count_rules(reports: list[ArtifactReport]) -> dict[str, int]:
    """Count affected artifacts per rule, even when a rule appears on multiple lines."""
    return dict(sorted(Counter(rule for report in reports for rule in {f.rule_id for f in report.findings}).items()))
