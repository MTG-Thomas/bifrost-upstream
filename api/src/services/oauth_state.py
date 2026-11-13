"""Tamper-proof ``state`` parameter for integration OAuth.

Thin wrapper over :mod:`src.services.oauth_state_core` that pins the audience
to integration OAuth and validates the payload shape (``provider_id``,
``user_id``, and ``mapping_id`` for per-mapping flows).

Every integration authorize endpoint encodes; the callback decodes, checks the
state was issued to the same user for the same provider, and consumes the
nonce. Without this, a callback URL carrying an attacker's authorization code
could be forwarded to an admin, whose session would store the attacker's
account token as the integration's token.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from src.services.oauth_state_core import (
    StateDecodeError,
    consume_nonce as _consume_nonce,
    decode_state_jwt,
    encode_state_jwt,
    remember_nonce as _remember_nonce,
)

# Distinct from MCP state tokens AND from JWT access tokens so neither can
# masquerade as the other.
_STATE_AUDIENCE = "bifrost-integration-oauth-state"


# Backwards-compat alias for existing callers / tests that imported the old name.
OAuthStateError = StateDecodeError


def encode_state(
    *,
    provider_id: UUID,
    user_id: UUID,
    mapping_id: UUID | None = None,
) -> tuple[str, str]:
    """Encode an integration OAuth ``state`` token and return ``(token, nonce)``.

    ``mapping_id`` is set only by the per-mapping flow. The caller is expected
    to record the nonce via :func:`remember_nonce` so the callback can reject
    replays.
    """
    claims: dict[str, Any] = {
        "provider_id": str(provider_id),
        "user_id": str(user_id),
    }
    if mapping_id is not None:
        claims["mapping_id"] = str(mapping_id)
    return encode_state_jwt(audience=_STATE_AUDIENCE, claims=claims)


def decode_state(token: str) -> dict[str, Any]:
    """Verify an integration OAuth ``state`` JWT and return its payload.

    Raises:
        OAuthStateError: any signature/expiry/format issue, or missing
        required fields.
    """
    payload = decode_state_jwt(token, audience=_STATE_AUDIENCE)

    for required in ("provider_id", "user_id"):
        if required not in payload:
            raise OAuthStateError(f"state missing required field: {required}")

    return payload


async def remember_nonce(nonce: str) -> None:
    """Persist an integration OAuth state nonce in Redis. Best-effort."""
    await _remember_nonce(nonce, audience=_STATE_AUDIENCE)


async def consume_nonce(nonce: str) -> bool:
    """Atomically check-and-delete an integration OAuth state nonce. See core docstring."""
    return await _consume_nonce(nonce, audience=_STATE_AUDIENCE)


__all__ = [
    "OAuthStateError",
    "consume_nonce",
    "decode_state",
    "encode_state",
    "remember_nonce",
]
