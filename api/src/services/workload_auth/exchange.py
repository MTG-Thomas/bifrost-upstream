"""Match a subject token to exactly one enabled trust rule."""

from __future__ import annotations

import hmac
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from src.services.workload_auth.constants import (
    ISSUED_TOKEN_TYPE,
    MAX_SUBJECT_TOKEN_CHARS,
    SUPPORTED_SUBJECT_TOKEN_TYPES,
    WORKLOAD_SCOPE,
)
from src.services.workload_auth.errors import (
    JwksFetchError,
    RuleValidationError,
    WorkloadAuthError,
)
from src.services.workload_auth.jwks import fetch_jwks
from src.services.workload_auth.jwt_validation import (
    REASON_EXPIRED,
    REASON_INVALID,
    REASON_WRONG_AUDIENCE,
    REASON_WRONG_ISSUER,
    SubjectTokenRejected,
    unverified_issuer_and_subject,
    verify_subject_jwt,
)
from src.services.workload_auth.profiles import profile_for
from src.services.workload_auth.types import ClaimCondition, ExchangeFailure, TrustRule

logger = logging.getLogger(__name__)

JwksFetcher = Callable[[str], Awaitable[dict[str, Any]]]

_REASON_RANK = {
    "claim_mismatch": 5,
    REASON_EXPIRED: 4,
    REASON_WRONG_AUDIENCE: 3,
    REASON_WRONG_ISSUER: 2,
    REASON_INVALID: 1,
}


def _condition_match(
    claims: dict[str, Any], conditions: tuple[ClaimCondition, ...]
) -> bool:
    for condition in conditions:
        value = claims.get(condition.claim)
        if not isinstance(value, str) or len(value) != len(condition.equals):
            return False
        if not hmac.compare_digest(value, condition.equals):
            return False
    return True


def _worse(current: str | None, candidate: str) -> str:
    if current is None or _REASON_RANK.get(candidate, 0) > _REASON_RANK.get(current, 0):
        return candidate
    return current


async def _keys_for(rule: TrustRule, fetcher: JwksFetcher) -> dict[str, Any]:
    if rule.pinned_jwks is not None:
        return rule.pinned_jwks
    if rule.jwks_uri is None:
        raise JwksFetchError("Trust rule has no keys.")
    return await fetcher(rule.jwks_uri)


async def select_rule(
    rules: Sequence[TrustRule],
    token: str,
    subject_token_type: str,
    *,
    fetcher: JwksFetcher | None = None,
) -> tuple[TrustRule, dict[str, Any]] | ExchangeFailure:
    """Return the single matching rule and its verified claims.

    Subject tokens are not single-use. A second call with the same still-valid
    token matches again.
    """
    if len(token) > MAX_SUBJECT_TOKEN_CHARS or token.count(".") != 2:
        return ExchangeFailure(REASON_INVALID)
    load = fetcher or fetch_jwks
    matches: list[tuple[TrustRule, dict[str, Any]]] = []
    failure = REASON_INVALID
    for rule in rules:
        if not rule.enabled or rule.subject_token_type != subject_token_type:
            continue
        try:
            profile = profile_for(rule.provider)
        except RuleValidationError:
            failure = _worse(failure, REASON_INVALID)
            continue
        if profile.subject_token_type != subject_token_type:
            continue
        try:
            keys = await _keys_for(rule, load)
            claims = verify_subject_jwt(
                token,
                keys=keys,
                issuer=rule.issuer,
                audiences=rule.audiences,
            )
        except (SubjectTokenRejected, JwksFetchError) as exc:
            reason = (
                exc.reason if isinstance(exc, SubjectTokenRejected) else REASON_INVALID
            )
            failure = _worse(failure, reason)
            continue
        if not _condition_match(claims, rule.conditions) or not profile.token_ok(
            claims
        ):
            failure = _worse(failure, "claim_mismatch")
            continue
        matches.append((rule, claims))
    if len(matches) > 1:
        logger.info("workload exchange ambiguous for %s rules", len(matches))
        return ExchangeFailure("ambiguous_rule")
    if len(matches) == 1:
        return matches[0]
    issuer, subject = unverified_issuer_and_subject(token)
    return ExchangeFailure(failure, issuer, subject)


def reject_request_shape(
    *,
    subject_token: str | None,
    subject_token_type: str | None,
    requested_token_type: str | None,
    scope: str | None,
    actor_token: str | None,
) -> tuple[str, str]:
    """Validate the OAuth form. Returns ``(subject_token, subject_token_type)``."""
    if actor_token:
        raise WorkloadAuthError(
            "invalid_request",
            "Delegation is not supported.",
            reason="actor_token",
        )
    if not subject_token or not subject_token_type:
        raise WorkloadAuthError(
            "invalid_request",
            "subject_token and subject_token_type are required.",
            reason="missing_subject_token",
        )
    if subject_token_type not in SUPPORTED_SUBJECT_TOKEN_TYPES:
        raise WorkloadAuthError(
            "invalid_request",
            "Unsupported subject_token_type.",
            reason="unsupported_subject_token_type",
        )
    if requested_token_type not in (None, "", ISSUED_TOKEN_TYPE):
        raise WorkloadAuthError(
            "invalid_target",
            "requested_token_type must be an access token.",
            reason="unsupported_requested_token_type",
        )
    if scope not in (None, "", WORKLOAD_SCOPE):
        raise WorkloadAuthError(
            "invalid_scope",
            "Unsupported scope.",
            reason="invalid_scope",
        )
    if len(subject_token) > MAX_SUBJECT_TOKEN_CHARS:
        raise WorkloadAuthError(
            "invalid_grant",
            "The subject token could not be exchanged.",
            reason=REASON_INVALID,
        )
    return subject_token, subject_token_type
