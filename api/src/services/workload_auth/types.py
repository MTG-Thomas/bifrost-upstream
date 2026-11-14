"""In-memory trust rule used by validation. Persistence maps onto this shape."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class ClaimCondition:
    claim: str
    equals: str


@dataclass(frozen=True)
class TrustRule:
    id: UUID
    name: str
    provider: str
    enabled: bool
    issuer: str
    subject_token_type: str
    audiences: tuple[str, ...]
    jwks_uri: str | None
    pinned_jwks: dict[str, Any] | None
    conditions: tuple[ClaimCondition, ...]
    identity_id: UUID
    token_lifetime_seconds: int


@dataclass(frozen=True)
class ExchangeSuccess:
    access_token: str
    expires_in: int
    rule_id: UUID
    identity_id: UUID
    provider: str
    issuer: str
    subject: str


@dataclass(frozen=True)
class ExchangeFailure:
    reason: str
    unverified_issuer: str | None = None
    unverified_subject: str | None = None
