"""Contracts for workflow approval review and operator actions."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from bifrost.cloudflare_analysis import MAX_CPU_MS, MAX_MODULES, MAX_SUBREQUESTS

from bifrost.contracts.cloudflare_approval import DEFAULT_CPU_BUDGET_MS, DEFAULT_SUBREQUEST_ALLOWANCE

_SourceDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class SolutionApprovalScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    install_id: UUID
    version: str | None
    source_generation: UUID


class ApprovalDeclaration(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entry_path: str
    function_name: str
    entry_sha256: _SourceDigest
    module_sha256: dict[str, _SourceDigest] = Field(max_length=MAX_MODULES - 1)
    manifest_sha256: _SourceDigest
    packages: list[str] = Field(max_length=32)
    https_hosts: list[str] = Field(max_length=32)
    required_https_hosts: list[str] = Field(max_length=32)
    unresolved_url_calls: int = Field(default=0, strict=True, ge=0)
    estimated_subrequests: int = Field(strict=True, ge=0)
    subrequest_allowance: int = Field(strict=True, ge=0, le=MAX_SUBREQUESTS)
    estimated_cpu_ms: int = Field(strict=True, ge=0)
    cpu_budget_ms: int = Field(strict=True, gt=0, le=MAX_CPU_MS)
    solution: SolutionApprovalScope | None = None

    @property
    def manifest(self) -> dict[str, str]:
        return {**self.module_sha256, self.entry_path: self.entry_sha256}

    def review_fields(self) -> dict[str, object]:
        fields = self.model_dump(mode="json")
        if self.solution is None:
            fields.pop("solution")
        return fields



class CloudflareApprovalOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subrequest_allowance: int = Field(default_factory=lambda: DEFAULT_SUBREQUEST_ALLOWANCE, strict=True, ge=0, le=MAX_SUBREQUESTS)
    cpu_budget_ms: int = Field(default_factory=lambda: DEFAULT_CPU_BUDGET_MS, strict=True, gt=0, le=MAX_CPU_MS)
    https_hosts: list[str] = Field(default_factory=list, max_length=32)


class CloudflareApprovalAccept(CloudflareApprovalOptions):
    subrequest_allowance: int = Field(strict=True, ge=0, le=MAX_SUBREQUESTS)
    cpu_budget_ms: int = Field(strict=True, gt=0, le=MAX_CPU_MS)
    review_sha256: _SourceDigest


class CloudflareApprovalReview(ApprovalDeclaration):
    policy_version: str
    workflow_id: UUID
    organization_id: UUID | None
    previous_revision: str | None
    inactive_history_count: int
    review_sha256: _SourceDigest
    wildcard_https_hosts: list[str]


class CloudflareApprovalDiagnostic(BaseModel):
    rule_id: str
    severity: Literal["warning", "info"]
    message: str
    hint: str
    path: str | None = None
    line: int | None = None
    column: int | None = None


CloudflareApprovalStatus = Literal[
    "approved", "pending_review", "revision_changed", "ineligible", "temporarily_unavailable",
]


class CloudflareApprovalState(BaseModel):
    status: CloudflareApprovalStatus
    can_manage: bool
    current_revision: _SourceDigest | None = None
    current_declaration: ApprovalDeclaration | None = None
    proposal: CloudflareApprovalReview | None = None
    proposal_sources: dict[str, str] | None = Field(default=None, exclude_if=lambda sources: sources is None)
    diagnostics: list[CloudflareApprovalDiagnostic] = Field(default_factory=list)



class CloudflareApprovalListItem(BaseModel):
    workflow_id: UUID
    name: str
    organization_id: UUID | None
    scope: Literal["provider", "global", "organization"]
    status: CloudflareApprovalStatus
    has_proposal: bool
    needs_larger_budget: bool = False


class CloudflareApprovalList(BaseModel):
    counts: dict[CloudflareApprovalStatus, int]
    workflows: list[CloudflareApprovalListItem]
    can_propose: bool


class StoredCloudflareReview(BaseModel):
    scope: tuple[UUID | None, UUID | None, str, str]
    solution: SolutionApprovalScope | None
    current_revision: _SourceDigest | None
    observed_pins: dict[str, _SourceDigest | None]
    status: CloudflareApprovalStatus
    proposal: CloudflareApprovalReview | None = None
    diagnostics: list[CloudflareApprovalDiagnostic]
