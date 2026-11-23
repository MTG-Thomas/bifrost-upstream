"""Admin contracts for workload trust rules."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

WorkloadProvider = Literal["github_actions", "azure_managed_identity"]


class ClaimConditionModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    equals: str = Field(min_length=1, max_length=512)


class WorkloadTrustRuleWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=120)
    provider: WorkloadProvider
    enabled: bool = True
    issuer: str = Field(min_length=1, max_length=512)
    audiences: list[str] = Field(min_length=1, max_length=4)
    jwks_uri: str | None = Field(default=None, max_length=512)
    pinned_jwks: dict[str, Any] | None = None
    claim_conditions: list[ClaimConditionModel] = Field(min_length=1, max_length=8)
    identity_id: UUID
    token_lifetime_seconds: int = Field(default=900, ge=60, le=3600)


class WorkloadTrustRuleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=120)
    provider: WorkloadProvider | None = None
    enabled: bool | None = None
    issuer: str | None = Field(default=None, min_length=1, max_length=512)
    audiences: list[str] | None = Field(default=None, min_length=1, max_length=4)
    jwks_uri: str | None = Field(default=None, max_length=512)
    pinned_jwks: dict[str, Any] | None = None
    claim_conditions: list[ClaimConditionModel] | None = Field(
        default=None, min_length=1, max_length=8
    )
    identity_id: UUID | None = None
    token_lifetime_seconds: int | None = Field(default=None, ge=60, le=3600)


class WorkloadTrustRulePublic(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    name: str
    provider: WorkloadProvider
    enabled: bool
    issuer: str
    subject_token_type: str
    audiences: list[str]
    jwks_uri: str | None
    pinned_jwks: dict[str, Any] | None
    claim_conditions: list[ClaimConditionModel]
    identity_id: UUID
    token_lifetime_seconds: int
    created_at: datetime
    updated_at: datetime


class WorkloadTrustRuleList(BaseModel):
    rules: list[WorkloadTrustRulePublic]
