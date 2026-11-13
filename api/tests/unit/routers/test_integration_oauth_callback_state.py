"""The integration OAuth callback only exchanges codes for flows this user started.

A callback that accepted any ``state`` let an attacker forward a callback URL
carrying their own authorization code to an admin; the admin's session then
stored the attacker's account token as the integration's token.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from src.models.contracts.oauth import OAuthCallbackRequest
from src.routers.oauth_connections import oauth_callback
from src.services.oauth_state import encode_state


def _provider():
    provider = MagicMock()
    provider.id = uuid4()
    provider.organization_id = None
    provider.token_url = "https://login.example.test/token"
    provider.client_id = "client-id"
    provider.encrypted_client_secret = None
    provider.scopes = ["openid"]
    provider.audience = None
    return provider


def _exchange():
    return AsyncMock(
        return_value=(
            True,
            {
                "access_token": "access-token",
                "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
            },
        )
    )


async def _callback(provider, user, state, exchange, *, nonce_fresh=True):
    ctx = MagicMock()
    ctx.db = AsyncMock()
    request = OAuthCallbackRequest(
        code="authorization-code",
        state=state,
        redirect_uri="https://bifrost.example.test/oauth/callback/partner",
    )
    with (
        patch("src.routers.oauth_connections.consume_nonce", new=AsyncMock(return_value=nonce_fresh)),
        patch("src.routers.oauth_connections.load_caller", new=AsyncMock(return_value=MagicMock())),
        patch("src.routers.oauth_connections.resolve_effective_scope", return_value=None),
        patch("src.routers.oauth_connections.user_reach") as reach,
        patch("src.routers.oauth_connections.require_permission"),
        patch(
            "src.repositories.oauth.OAuthProviderRepository.get_by_connection_name",
            new=AsyncMock(return_value=provider),
        ),
        patch("src.repositories.oauth.OAuthProviderRepository.store_token", new=AsyncMock()) as store,
        patch("src.routers.oauth_connections.get_url_resolution_defaults", new=AsyncMock(return_value={})),
        patch("src.routers.oauth_connections.resolve_url_template", return_value=provider.token_url),
        patch(
            "src.services.oauth_provider.OAuthProviderClient.exchange_code_for_token",
            new=exchange,
        ),
        patch("src.routers.oauth_connections.CACHE_INVALIDATION_AVAILABLE", False),
    ):
        reach.return_value.covers.return_value = True
        result = await oauth_callback(
            connection_name="partner", request=request, ctx=ctx, user=user
        )
    return result, store


@pytest.mark.asyncio
async def test_state_issued_to_this_user_for_this_provider_stores_the_token():
    provider = _provider()
    user = SimpleNamespace(user_id=uuid4())
    state, _ = encode_state(provider_id=provider.id, user_id=user.user_id)

    exchange = _exchange()

    result, store = await _callback(provider, user, state, exchange)

    assert result.success is True
    exchange.assert_awaited_once()
    store.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["missing", "unsigned", "other_user", "other_provider", "replayed"],
)
async def test_untrusted_state_is_rejected_before_code_exchange(case):
    provider = _provider()
    user = SimpleNamespace(user_id=uuid4())
    state = {
        "missing": None,
        "unsigned": "random-legacy-state",
        "other_user": encode_state(provider_id=provider.id, user_id=uuid4())[0],
        "other_provider": encode_state(provider_id=uuid4(), user_id=user.user_id)[0],
        "replayed": encode_state(provider_id=provider.id, user_id=user.user_id)[0],
    }[case]

    exchange = _exchange()

    with pytest.raises(HTTPException) as raised:
        await _callback(provider, user, state, exchange, nonce_fresh=case != "replayed")

    assert raised.value.status_code in (400, 404)
    exchange.assert_not_awaited()
