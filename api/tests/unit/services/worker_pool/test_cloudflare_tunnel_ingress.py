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
    ("config", "version", "mismatched_field", "allow_default", "allowed"),
    [
        ({"ingress": [DEFAULT_RULE]}, 0, None, True, True),
        ({"ingress": [DEFAULT_RULE], "warp-routing": {"enabled": False}}, 12, None, True, True),
        ({"ingress": [{**DEFAULT_RULE, "hostname": ""}], "warp-routing": {}}, 1, None, True, True),
        ({"ingress": [DEFAULT_RULE]}, 1, None, False, False),
        ({"ingress": [DEFAULT_RULE]}, 1, "account_id", True, False),
        ({"ingress": [DEFAULT_RULE]}, 1, "tunnel_id", True, False),
        ({"ingress": [{**DEFAULT_RULE, "hostname": "other.example.test"}]}, 1, None, True, False),
        ({"ingress": [{"service": "http://origin.example.test"}]}, 1, None, True, False),
        ({"ingress": [DEFAULT_RULE, DEFAULT_RULE]}, 1, None, True, False),
        ({"ingress": [{**DEFAULT_RULE, "path": "/test"}]}, 1, None, True, False),
        ({"ingress": [{**DEFAULT_RULE, "originRequest": {"noTLSVerify": True}}]}, 1, None, True, False),
        ({"ingress": [DEFAULT_RULE], "originRequest": {"noTLSVerify": True}}, 1, None, True, False),
        ({"ingress": [DEFAULT_RULE], "warp-routing": {"enabled": True}}, 1, None, True, False),
        ({"ingress": [DEFAULT_RULE], "warp-routing": {"enabled": False, "other": False}}, 1, None, True, False),
        ({"ingress": [DEFAULT_RULE], "other": {}}, 1, None, True, False),
    ],
)
async def test_only_exact_creation_intent_replaces_default_ingress(
    monkeypatch, config, version, mismatched_field, allow_default, allowed,
) -> None:
    account = "a" * 32
    intent = ResourceIntent(ResourceTarget(uuid4(), account, "reachability", "tunnel", "fixture"), uuid4(), 1, "digest")
    receipt = ResourceObservation(intent, str(uuid4()), "created")
    origin = "http://api:8001"
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected network request"))) as http:
        client = TunnelRest(http, account_id=account, zone_id="b" * 32, hostname="managed.example.test", token=SecretStr("fixture"))
        current = {
            "account_id": account, "tunnel_id": receipt.provider_id,
            "version": version, "config": config,
        }
        if mismatched_field:
            current[mismatched_field] = str(uuid4())
        read_config = AsyncMock(return_value=current)
        write = AsyncMock()
        verify = AsyncMock()
        exact_read = AsyncMock(return_value=receipt)
        monkeypatch.setattr(client, "_result", read_config)
        monkeypatch.setattr(client, "_put_ingress", write)
        monkeypatch.setattr(client, "verify_ingress", verify)
        monkeypatch.setattr(client, "tunnel", exact_read)
        if allowed:
            await client.configure_ingress(receipt, origin, allow_default=allow_default)
            exact_read.assert_awaited_once_with(intent, receipt)
            write.assert_awaited_once()
            verify.assert_awaited_once_with(receipt, origin)
        else:
            with pytest.raises(OwnershipConflict, match="different ingress configuration"):
                await client.configure_ingress(receipt, origin, allow_default=allow_default)
            write.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_tunnel_without_post_id_uses_secret_verified_readback(monkeypatch) -> None:
    account = "a" * 32
    intent = ResourceIntent(ResourceTarget(uuid4(), account, "reachability", "tunnel", "fixture"), uuid4(), 1, "digest")
    receipt = ResourceObservation(intent, str(uuid4()), "created")
    secret = SecretStr("fixture-secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected network request"))) as http:
        client = TunnelRest(http, account_id=account, zone_id="b" * 32, hostname="managed.example.test", token=SecretStr("fixture"))
        read_tunnel = AsyncMock(side_effect=[None, receipt])
        create = AsyncMock(return_value={"status": "inactive"})
        monkeypatch.setattr(client, "tunnel", read_tunnel)
        monkeypatch.setattr(client, "_result", create)

        assert await client.create_tunnel(intent, secret) == receipt
        create.assert_awaited_once()
        assert read_tunnel.await_count == 2
        read_tunnel.assert_awaited_with(intent, None, secret=secret)


@pytest.mark.asyncio
async def test_ingress_conflict_reports_only_sanitized_shape(monkeypatch) -> None:
    account = "a" * 32
    intent = ResourceIntent(ResourceTarget(uuid4(), account, "reachability", "tunnel", "fixture"), uuid4(), 1, "digest")
    receipt = ResourceObservation(intent, str(uuid4()), "created")
    current = {
        "account_id": account, "tunnel_id": receipt.provider_id, "version": 17, "secret-response-key": "secret-value",
        "config": {
            "ingress": [
                {"service": "https://private.example.test", "hostname": "test.example.test", "secret-rule-key": "secret"},
                DEFAULT_RULE,
            ],
            "warp-routing": {"enabled": False, "secret-warp-key": True},
            "secret-config-key": "secret",
        },
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected network request"))) as http:
        client = TunnelRest(http, account_id=account, zone_id="b" * 32, hostname="managed.example.test", token=SecretStr("fixture"))
        monkeypatch.setattr(client, "_result", AsyncMock(return_value=current))
        with pytest.raises(OwnershipConflict) as failure:
            await client.configure_ingress(receipt, "http://api:8001", allow_default=True)

    assert failure.value.as_result() == {
        "resource_class": "tunnel_ingress",
        "check": "ingress_occupied",
        "observed_category": "already_configured",
        "expected_category": "empty_or_matching",
        "ingress_shape": {
            "version": 17,
            "response_keys": ["account_id", "config", "tunnel_id", "version", "other"],
            "config_keys": ["ingress", "warp-routing", "other"],
            "warp_routing_keys": ["enabled", "other"],
            "rule_count": 2,
            "rules": [
                {"keys": ["hostname", "service", "other"], "service_kind": "https_origin"},
                {"keys": ["service"], "service_kind": "http_status_404"},
            ],
        },
    }
