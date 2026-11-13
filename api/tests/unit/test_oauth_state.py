"""Unit tests for ``src.services.oauth_state`` — the per-mapping OAuth state wrapper.

The underlying JWT primitives are tested separately in
``test_oauth_state_core.py``; this file only covers the per-mapping payload
shape and audience-isolation behaviour.
"""

from datetime import timedelta
from uuid import uuid4

import pytest

from src.services.oauth_state import (
    OAuthStateError,
    decode_state,
    encode_state,
)


def test_round_trip_carries_provider_user_and_mapping():
    provider_id = uuid4()
    user_id = uuid4()
    mapping_id = uuid4()
    token, nonce = encode_state(provider_id=provider_id, user_id=user_id, mapping_id=mapping_id)

    payload = decode_state(token)
    assert payload["provider_id"] == str(provider_id)
    assert payload["user_id"] == str(user_id)
    assert payload["mapping_id"] == str(mapping_id)
    assert payload["nonce"] == nonce


def test_integration_level_state_has_no_mapping():
    token, _ = encode_state(provider_id=uuid4(), user_id=uuid4())

    assert "mapping_id" not in decode_state(token)


def test_state_without_user_binding_is_rejected():
    """Per-mapping tokens issued before user binding fail once instead of
    being accepted without knowing who started the flow."""
    from src.services.oauth_state_core import encode_state_jwt

    token, _ = encode_state_jwt(
        audience="bifrost-integration-oauth-state",
        claims={"provider_id": str(uuid4()), "mapping_id": str(uuid4())},
    )
    with pytest.raises(OAuthStateError, match="user_id"):
        decode_state(token)


def test_nonce_is_unique_per_call():
    """Each encode call generates a fresh nonce so replay detection has something to bite on."""
    p, u = uuid4(), uuid4()
    _, n1 = encode_state(provider_id=p, user_id=u)
    _, n2 = encode_state(provider_id=p, user_id=u)
    assert n1 != n2


def test_tampered_token_rejected():
    token, _ = encode_state(provider_id=uuid4(), user_id=uuid4())
    # Flip a char inside the JWT body (not the signature segment)
    parts = token.split(".")
    parts[1] = parts[1][:-1] + ("A" if parts[1][-1] != "A" else "B")
    with pytest.raises(OAuthStateError):
        decode_state(".".join(parts))


def test_garbage_token_rejected():
    with pytest.raises(OAuthStateError):
        decode_state("not-a-real-jwt")


def test_audience_isolation_mcp_token_rejected_here():
    """An MCP-audience state token must not decode as a per-mapping state token."""
    from src.services.mcp_client.oauth_state import encode_state as mcp_encode_state

    mcp_token, _ = mcp_encode_state(
        connection_id=uuid4(),
        flow_type="service",
        pkce_verifier="x" * 64,
    )
    with pytest.raises(OAuthStateError):
        decode_state(mcp_token)


def test_expired_token_rejected():
    """A token whose exp is in the past must be rejected even with a valid signature."""
    from src.services.oauth_state_core import encode_state_jwt

    # Encode directly via the core so we can set a negative TTL — the wrapper's default
    # arg captures the module constant at definition time, so monkeypatching it doesn't help.
    token, _ = encode_state_jwt(
        audience="bifrost-integration-oauth-state",
        claims={"provider_id": "p", "user_id": "u"},
        ttl=timedelta(seconds=-60),
    )
    with pytest.raises(OAuthStateError):
        decode_state(token)
