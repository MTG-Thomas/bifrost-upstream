"""CLI mirrors for workflow approval request flags."""

from pydantic import BaseModel, Field


DEFAULT_SUBREQUEST_ALLOWANCE = 10
DEFAULT_CPU_BUDGET_MS = 1000


class CloudflareApprovalOptions(BaseModel):
    subrequest_allowance: int = DEFAULT_SUBREQUEST_ALLOWANCE
    cpu_budget_ms: int = DEFAULT_CPU_BUDGET_MS
    https_hosts: list[str] = Field(default_factory=list)


class CloudflareApprovalAccept(CloudflareApprovalOptions):
    subrequest_allowance: int
    cpu_budget_ms: int
    review_sha256: str
