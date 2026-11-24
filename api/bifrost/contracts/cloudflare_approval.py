"""CLI mirrors for workflow approval request flags."""

from typing import Literal

from pydantic import BaseModel, Field


DEFAULT_SUBREQUEST_ALLOWANCE = 10
DEFAULT_CPU_BUDGET_MS = 1000


class CloudflareApprovalOptions(BaseModel):
    subrequest_allowance: int = DEFAULT_SUBREQUEST_ALLOWANCE
    cpu_budget_ms: int = DEFAULT_CPU_BUDGET_MS
    https_hosts: list[str] = Field(default_factory=list)
    runtime: Literal["workers", "containers"] | None = None


class CloudflareApprovalAccept(BaseModel):
    subrequest_allowance: int
    cpu_budget_ms: int
    https_hosts: list[str] = Field(default_factory=list)
    review_sha256: str
    runtime: Literal["workers", "containers"] | None = None
