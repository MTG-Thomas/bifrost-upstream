"""Load and administer workload trust rules."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from shared.identities import is_identity
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.contracts.workload_trust import (
    ClaimConditionModel,
    WorkloadTrustRulePublic,
    WorkloadTrustRuleUpdate,
    WorkloadTrustRuleWrite,
)
from src.models.orm.users import User
from src.models.orm.workload_trust import WorkloadTrustRule
from src.services.workload_auth.constants import MAX_AUDIENCES, MAX_RULES
from src.services.workload_auth.errors import RuleValidationError
from src.services.workload_auth.profiles import profile_for
from src.services.workload_auth.types import ClaimCondition, TrustRule


def _audiences(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for value in values:
        audience = value.strip()
        if not audience or any(char.isspace() for char in audience):
            raise RuleValidationError(
                "Each audience must be a non-empty string without whitespace."
            )
        if len(audience) > 512:
            raise RuleValidationError("An audience is too long.")
        if audience in cleaned:
            raise RuleValidationError("Audiences must be unique.")
        cleaned.append(audience)
    if not 1 <= len(cleaned) <= MAX_AUDIENCES:
        raise RuleValidationError("Set one to four audiences.")
    return cleaned


def _conditions(values: list[ClaimConditionModel]) -> tuple[ClaimCondition, ...]:
    return tuple(
        ClaimCondition(claim=item.claim, equals=item.equals) for item in values
    )


def to_public(row: WorkloadTrustRule) -> WorkloadTrustRulePublic:
    return WorkloadTrustRulePublic(
        id=row.id,
        name=row.name,
        provider=row.provider,  # type: ignore[arg-type]
        enabled=row.enabled,
        issuer=row.issuer,
        subject_token_type=row.subject_token_type,
        audiences=list(row.audiences),
        jwks_uri=row.jwks_uri,
        pinned_jwks=row.pinned_jwks,
        claim_conditions=[
            ClaimConditionModel.model_validate(item) for item in row.claim_conditions
        ],
        identity_id=row.identity_id,
        token_lifetime_seconds=row.token_lifetime_seconds,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def to_trust_rule(row: WorkloadTrustRule) -> TrustRule:
    return TrustRule(
        id=row.id,
        name=row.name,
        provider=row.provider,
        enabled=row.enabled,
        issuer=row.issuer,
        subject_token_type=row.subject_token_type,
        audiences=tuple(row.audiences),
        jwks_uri=row.jwks_uri,
        pinned_jwks=row.pinned_jwks,
        conditions=tuple(
            ClaimCondition(claim=str(item["claim"]), equals=str(item["equals"]))
            for item in row.claim_conditions
        ),
        identity_id=row.identity_id,
        token_lifetime_seconds=row.token_lifetime_seconds,
    )


async def _require_identity(db: AsyncSession, identity_id: UUID) -> None:
    user = await db.get(User, identity_id)
    if user is None or not is_identity(user) or not user.is_active or user.is_system:
        raise RuleValidationError("Identity is not an active BiFrost identity.")


def _validated(
    *,
    provider: str,
    issuer: str,
    audiences: list[str],
    conditions: tuple[ClaimCondition, ...],
    jwks_uri: str | None,
    pinned_jwks: dict[str, Any] | None,
) -> tuple[str, list[str], list[dict[str, str]], dict[str, Any] | None]:
    profile = profile_for(provider)
    normalized_keys = profile.validate_rule(
        issuer=issuer,
        conditions=conditions,
        jwks_uri=jwks_uri,
        pinned_jwks=pinned_jwks,
    )
    return (
        profile.subject_token_type,
        _audiences(audiences),
        [{"claim": item.claim, "equals": item.equals} for item in conditions],
        normalized_keys,
    )


async def list_rules(db: AsyncSession) -> list[WorkloadTrustRulePublic]:
    rows = (
        (
            await db.execute(
                select(WorkloadTrustRule).order_by(
                    WorkloadTrustRule.name, WorkloadTrustRule.id
                )
            )
        )
        .scalars()
        .all()
    )
    return [to_public(row) for row in rows]


async def get_rule(db: AsyncSession, rule_id: UUID) -> WorkloadTrustRule | None:
    return await db.get(WorkloadTrustRule, rule_id)


async def load_enabled_rules(
    db: AsyncSession, subject_token_type: str
) -> list[TrustRule]:
    rows = (
        (
            await db.execute(
                select(WorkloadTrustRule).where(
                    WorkloadTrustRule.enabled.is_(True),
                    WorkloadTrustRule.subject_token_type == subject_token_type,
                )
            )
        )
        .scalars()
        .all()
    )
    return [to_trust_rule(row) for row in rows]


async def create_rule(
    db: AsyncSession,
    body: WorkloadTrustRuleWrite,
    *,
    created_by: UUID | None,
) -> WorkloadTrustRule:
    count = await db.scalar(select(func.count()).select_from(WorkloadTrustRule))
    if count is not None and int(count) >= MAX_RULES:
        raise RuleValidationError("The trust rule limit has been reached.")
    name = body.name.strip()
    if not name:
        raise RuleValidationError("Name is required.")
    await _require_identity(db, body.identity_id)
    subject_token_type, audiences, conditions, pinned = _validated(
        provider=body.provider,
        issuer=body.issuer.strip(),
        audiences=body.audiences,
        conditions=_conditions(body.claim_conditions),
        jwks_uri=body.jwks_uri,
        pinned_jwks=body.pinned_jwks,
    )
    row = WorkloadTrustRule(
        name=name,
        provider=body.provider,
        enabled=body.enabled,
        issuer=body.issuer.strip(),
        subject_token_type=subject_token_type,
        audiences=audiences,
        jwks_uri=None if pinned is not None else body.jwks_uri,
        pinned_jwks=pinned,
        claim_conditions=conditions,
        identity_id=body.identity_id,
        token_lifetime_seconds=body.token_lifetime_seconds,
        created_by=created_by,
    )
    db.add(row)
    await db.flush()
    return row


async def update_rule(
    db: AsyncSession,
    row: WorkloadTrustRule,
    body: WorkloadTrustRuleUpdate,
) -> WorkloadTrustRule:
    changes = body.model_dump(exclude_unset=True)
    if not changes:
        raise RuleValidationError("No fields to update.")
    if "jwks_uri" in changes or "pinned_jwks" in changes:
        jwks_uri = changes.get("jwks_uri")
        pinned_jwks = changes.get("pinned_jwks")
    else:
        jwks_uri = row.jwks_uri
        pinned_jwks = row.pinned_jwks
    provider = changes.get("provider", row.provider)
    issuer = str(changes.get("issuer", row.issuer)).strip()
    audiences = list(changes.get("audiences", list(row.audiences)))
    condition_models = (
        body.claim_conditions
        if "claim_conditions" in changes and body.claim_conditions is not None
        else [ClaimConditionModel.model_validate(item) for item in row.claim_conditions]
    )
    identity_id = changes.get("identity_id", row.identity_id)
    await _require_identity(db, identity_id)
    subject_token_type, cleaned_audiences, conditions, pinned = _validated(
        provider=provider,
        issuer=issuer,
        audiences=audiences,
        conditions=_conditions(condition_models),
        jwks_uri=jwks_uri,
        pinned_jwks=pinned_jwks,
    )
    name = str(changes.get("name", row.name)).strip()
    if not name:
        raise RuleValidationError("Name is required.")
    row.name = name
    row.provider = provider
    row.enabled = bool(changes.get("enabled", row.enabled))
    row.issuer = issuer
    row.subject_token_type = subject_token_type
    row.audiences = cleaned_audiences
    row.jwks_uri = None if pinned is not None else jwks_uri
    row.pinned_jwks = pinned
    row.claim_conditions = conditions
    row.identity_id = identity_id
    if "token_lifetime_seconds" in changes and body.token_lifetime_seconds is not None:
        row.token_lifetime_seconds = body.token_lifetime_seconds
    row.updated_at = datetime.now(UTC)
    await db.flush()
    return row
