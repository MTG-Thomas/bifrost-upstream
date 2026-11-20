"""CLI mirrors for workflow approval request flags."""

from pydantic import BaseModel, Field


class CloudflareApprovalOptions(BaseModel):
    subrequest_allowance: int
    cpu_budget_ms: int
    https_hosts: list[str] = Field(default_factory=list)


class CloudflareApprovalAccept(CloudflareApprovalOptions):
    review_sha256: str
