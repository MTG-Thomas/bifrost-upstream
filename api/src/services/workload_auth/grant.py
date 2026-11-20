"""HTTP adapter for the RFC 8693 grant on the OAuth token endpoint."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import JSONResponse

from src.core.database import get_db_context
from src.core.rate_limit import get_client_ip, workload_exchange_limiter
from src.services.audit import emit_audit
from src.services.audit_context import ActorContext, current_actor, set_actor
from src.services.authorization.tokens import (
    TokenSubjectUnavailable,
    issue_workload_access_token,
)
from src.services.workload_auth.constants import ISSUED_TOKEN_TYPE, WORKLOAD_SCOPE
from src.services.workload_auth.errors import WorkloadAuthError
from src.services.workload_auth.exchange import reject_request_shape, select_rule
from src.services.workload_auth.rules import load_enabled_rules
from src.services.workload_auth.types import ExchangeFailure, ExchangeSuccess

logger = logging.getLogger(__name__)

_NO_STORE = {"Cache-Control": "no-store", "Pragma": "no-cache"}
_GENERIC_GRANT = "The subject token could not be exchanged."


def _oauth_error(error: str, description: str, status_code: int) -> JSONResponse:
    return JSONResponse(
        {"error": error, "error_description": description},
        status_code=status_code,
        headers=_NO_STORE,
    )


def _form_string(form: Any, name: str) -> str | None:
    value = form.get(name)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise WorkloadAuthError(
            "invalid_request",
            "Request fields must be strings.",
            reason="malformed_request",
        )
    return value


async def _audit(
    db: AsyncSession,
    action: str,
    *,
    outcome: str,
    details: dict[str, Any],
    identity_id: UUID | None,
) -> None:
    if current_actor() is None:
        set_actor(
            ActorContext(
                user_id=None, organization_id=None, source="http", surface="cli"
            )
        )
    await emit_audit(
        db,
        action,
        resource_type="user" if identity_id is not None else "workload_trust_rule",
        resource_id=identity_id,
        outcome=outcome,  # type: ignore[arg-type]
        details=details,
    )


async def exchange_for_identity(
    db: AsyncSession,
    subject_token: str,
    subject_token_type: str,
) -> ExchangeSuccess | ExchangeFailure:
    rules = await load_enabled_rules(db, subject_token_type)
    selected = await select_rule(rules, subject_token, subject_token_type)
    if isinstance(selected, ExchangeFailure):
        return selected
    rule, claims = selected
    subject = claims.get("sub")
    subject_text = subject if isinstance(subject, str) else ""
    try:
        issued = await issue_workload_access_token(
            rule.identity_id, rule.token_lifetime_seconds
        )
    except TokenSubjectUnavailable:
        return ExchangeFailure("identity_unavailable", rule.issuer, subject_text[:256])
    expires_in = max(0, int((issued.expires_at - datetime.now(UTC)).total_seconds()))
    return ExchangeSuccess(
        access_token=issued.token,
        expires_in=expires_in,
        rule_id=rule.id,
        identity_id=rule.identity_id,
        provider=rule.provider,
        issuer=rule.issuer,
        subject=subject_text[:256],
    )


async def handle_token_exchange(request: Request, form: Any) -> JSONResponse:
    """Exchange one subject token. Never logs the token or returns it."""
    await workload_exchange_limiter.check("workload_exchange", get_client_ip(request))
    try:
        subject_token, subject_token_type = reject_request_shape(
            subject_token=_form_string(form, "subject_token"),
            subject_token_type=_form_string(form, "subject_token_type"),
            requested_token_type=_form_string(form, "requested_token_type"),
            scope=_form_string(form, "scope"),
            actor_token=_form_string(form, "actor_token"),
        )
    except WorkloadAuthError as exc:
        async with get_db_context() as db:
            await _audit(
                db,
                "auth.workload_exchange.failed",
                outcome="failure",
                details={"reason": exc.reason},
                identity_id=None,
            )
        return _oauth_error(exc.error, exc.description, exc.status_code)

    async with get_db_context() as db:
        result = await exchange_for_identity(db, subject_token, subject_token_type)
        if isinstance(result, ExchangeFailure):
            logger.info("workload exchange refused: %s", result.reason)
            await _audit(
                db,
                "auth.workload_exchange.failed",
                outcome="failure",
                details={
                    "reason": result.reason,
                    "unverified_issuer": result.unverified_issuer,
                    "unverified_subject": result.unverified_subject,
                },
                identity_id=None,
            )
            return _oauth_error("invalid_grant", _GENERIC_GRANT, 400)
        await _audit(
            db,
            "auth.workload_exchange.success",
            outcome="success",
            details={
                "rule_id": str(result.rule_id),
                "provider": result.provider,
                "issuer": result.issuer,
                "subject": result.subject,
                "expires_in": result.expires_in,
            },
            identity_id=result.identity_id,
        )
    return JSONResponse(
        {
            "access_token": result.access_token,
            "issued_token_type": ISSUED_TOKEN_TYPE,
            "token_type": "Bearer",
            "expires_in": result.expires_in,
            "scope": WORKLOAD_SCOPE,
        },
        headers=_NO_STORE,
    )
