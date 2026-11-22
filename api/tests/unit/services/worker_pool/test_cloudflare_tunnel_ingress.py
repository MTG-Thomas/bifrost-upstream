from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from src.services.worker_pool.cloudflare_provisioning import (
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
)
from src.services.worker_pool.cloudflare_tunnel_rest import TunnelRest

DEFAULT_RULE = {"service": "http_status:404"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("creation", "config", "version", "matching_account", "allowed"),
    [
        ("fresh", {"ingress": [DEFAULT_RULE]}, 0, True, True),
        ("fresh", {"ingress": [DEFAULT_RULE], "warp-routing": {"enabled": False}}, 1, True, True),
        ("fresh", {"ingress": [{**DEFAULT_RULE, "hostname": ""}], "warp-routing": {}}, 1, True, True),
        ("adopted", {"ingress": [DEFAULT_RULE]}, 1, True, False),
        ("different_receipt", {"ingress": [DEFAULT_RULE]}, 1, True, False),
        ("fresh", {"ingress": [DEFAULT_RULE]}, 1, False, False),
        ("fresh", {"ingress": [DEFAULT_RULE]}, 2, True, False),
        ("fresh", {"ingress": [{**DEFAULT_RULE, "hostname": "other.example.test"}]}, 1, True, False),
        ("fresh", {"ingress": [{"service": "http://origin.example.test"}]}, 1, True, False),
        ("fresh", {"ingress": [DEFAULT_RULE, DEFAULT_RULE]}, 1, True, False),
        ("fresh", {"ingress": [{**DEFAULT_RULE, "path": "/test"}]}, 1, True, False),
        ("fresh", {"ingress": [{**DEFAULT_RULE, "originRequest": {"noTLSVerify": True}}]}, 1, True, False),
        ("fresh", {"ingress": [DEFAULT_RULE], "warp-routing": {"enabled": True}}, 1, True, False),
        ("fresh", {"ingress": [DEFAULT_RULE], "other": {}}, 1, True, False),
    ],
)
async def test_only_exact_fresh_tunnel_replaces_default_ingress(
    monkeypatch, creation, config, version, matching_account, allowed,
) -> None:
    account = "a" * 32
    intent = ResourceIntent(ResourceTarget(uuid4(), account, "reachability", "tunnel", "fixture"), uuid4(), 1, "digest")
    observation = ResourceObservation(intent, str(uuid4()), "created")
    response_id = str(uuid4()) if creation == "different_receipt" else observation.provider_id
    origin = "http://api:8001"
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected network request"))) as http:
        client = TunnelRest(http, account_id=account, zone_id="b" * 32, hostname="managed.example.test", token=SecretStr("fixture"))
        read_tunnel = AsyncMock(side_effect=[observation] if creation == "adopted" else [None, observation])
        create = AsyncMock(return_value={"id": response_id})
        monkeypatch.setattr(client, "tunnel", read_tunnel)
        monkeypatch.setattr(client, "_result", create)
        receipt, fresh = await client.create_tunnel(intent, SecretStr("fixture-secret"))
        assert fresh == (creation == "fresh")
        assert create.await_count == (creation != "adopted")

        current = {
            "account_id": account if matching_account else "c" * 32, "tunnel_id": observation.provider_id,
            "version": version, "config": config,
        }
        read_config = AsyncMock(return_value=current)
        write = AsyncMock()
        verify = AsyncMock()
        exact_read = AsyncMock(return_value=receipt)
        monkeypatch.setattr(client, "_result", read_config)
        monkeypatch.setattr(client, "_put_ingress", write)
        monkeypatch.setattr(client, "verify_ingress", verify)
        monkeypatch.setattr(client, "tunnel", exact_read)
        if allowed:
            await client.configure_ingress(receipt, origin, freshly_created=fresh)
            exact_read.assert_awaited_once_with(intent, receipt)
            write.assert_awaited_once()
            verify.assert_awaited_once_with(receipt, origin)
        else:
            with pytest.raises(OwnershipConflict, match="different ingress configuration"):
                await client.configure_ingress(receipt, origin, freshly_created=fresh)
            write.assert_not_awaited()
