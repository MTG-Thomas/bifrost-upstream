from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
)
from src.services.worker_pool.cloudflare_tunnel_rest import TunnelRest

DEFAULT_RULE = {"service": "http_status:404"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("config", "version", "mismatched_field", "allow_unconfigured", "allowed"),
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
    monkeypatch, config, version, mismatched_field, allow_unconfigured, allowed,
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
            await client.configure_ingress(receipt, origin, allow_unconfigured=allow_unconfigured)
            exact_read.assert_awaited_once_with(intent, receipt)
            write.assert_awaited_once()
            verify.assert_awaited_once_with(receipt, origin)
        else:
            with pytest.raises(OwnershipConflict, match="different ingress configuration"):
                await client.configure_ingress(receipt, origin, allow_unconfigured=allow_unconfigured)
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
            await client.configure_ingress(receipt, "http://api:8001", allow_unconfigured=True)

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("envelope", "allow_unconfigured", "writes"),
    [
        ({"config": None, "tunnel_id": "owned"}, True, True),
        ({"config": None}, True, True),
        ({}, True, True),
        ({"config": None, "tunnel_id": "different"}, True, False),
        ({"config": None, "account_id": "different"}, True, False),
        ({"config": None, "account_id": None}, True, False),
        ({"config": "malformed"}, True, False),
        ({"config": None, "version": False}, True, False),
        ({"config": None}, False, False),
        (None, False, False),
    ],
)
async def test_version_zero_configuration_is_only_initialized_for_unrecorded_receipt(
    monkeypatch, envelope, allow_unconfigured, writes,
) -> None:
    account = "a" * 32
    target = ResourceTarget(uuid4(), account, "reachability", "tunnel", "fixture")
    receipt = ResourceObservation(ResourceIntent(target, uuid4(), 1, "digest"), "owned", "created")
    origin = "http://api:8001"
    approved = {
        "version": 1, "source": "cloudflare",
        "config": {"ingress": [
            {"hostname": "managed.example.test", "service": origin}, DEFAULT_RULE,
        ], "warp-routing": {"enabled": False}},
    }
    calls = []

    def provider(request):
        assert request.url.path == f"/client/v4/accounts/{account}/cfd_tunnel/owned/configurations"
        calls.append(request.method)
        if envelope is None:
            return httpx.Response(404)
        initial = {"version": 0, "source": "cloudflare", "created_at": "created", **envelope}
        return httpx.Response(200, json={"success": True, "result": initial if len(calls) == 1 else approved})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = TunnelRest(http, account_id=account, zone_id="b" * 32, hostname="managed.example.test", token=SecretStr("fixture"))
        exact_read = AsyncMock(return_value=receipt)
        monkeypatch.setattr(client, "tunnel", exact_read)
        if writes:
            await client.configure_ingress(receipt, origin, allow_unconfigured=allow_unconfigured)
            exact_read.assert_awaited_once_with(receipt.intent, receipt)
            assert calls == ["GET", "PUT", "GET"]
        else:
            with pytest.raises(OwnershipConflict):
                await client.configure_ingress(receipt, origin, allow_unconfigured=allow_unconfigured)
            exact_read.assert_not_awaited()
            assert calls == ["GET"]


@pytest.mark.asyncio
async def test_connector_inventory_recovers_one_nested_client_id_and_rejects_unidentified_rows() -> None:
    account = "a" * 32
    tunnel_id = str(uuid4())
    connector_id = str(uuid4())
    rows = [{"conns": [{"client_id": connector_id}, {"client_id": connector_id}]}]

    def provider(request):
        assert request.url.path == f"/client/v4/accounts/{account}/cfd_tunnel/{tunnel_id}/connections"
        return httpx.Response(200, json={"success": True, "result": rows})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = TunnelRest(http, account_id=account, zone_id="b" * 32, hostname="managed.example.test", token=SecretStr("fixture"))
        assert await client.connectors(tunnel_id) == frozenset({connector_id})
        rows[0] = {"conns": [{"id": str(uuid4())}]}
        with pytest.raises(ObservationPending, match="malformed"):
            await client.connectors(tunnel_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metadata", "accepted"),
    [
        ({"config_src": "cloudflare"}, True),
        ({"remote_config": True}, True),
        ({"account_tag": "foreign", "config_src": "cloudflare"}, False),
        ({"config_src": "local", "remote_config": True}, False),
        ({}, False),
    ],
)
async def test_tunnel_read_preserves_exact_receipt_and_remote_mode(metadata, accepted) -> None:
    account = "a" * 32
    tunnel_id = str(uuid4())
    target = ResourceTarget(uuid4(), account, "reachability", "tunnel", "fixture")
    intent = ResourceIntent(target, uuid4(), 1, "digest")
    receipt = ResourceObservation(intent, tunnel_id, "created")

    def provider(request):
        assert request.url.path == f"/client/v4/accounts/{account}/cfd_tunnel"
        assert request.url.params["name"] == target.name
        item = {"id": tunnel_id, "name": target.name, "created_at": "created", **metadata}
        return httpx.Response(200, json={"success": True, "result": [item]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = TunnelRest(http, account_id=account, zone_id="b" * 32, hostname="managed.example.test", token=SecretStr("fixture"))
        if accepted:
            assert await client.tunnel(intent, receipt) == receipt
        else:
            with pytest.raises(OwnershipConflict, match="differs from its journal"):
                await client.tunnel(intent, receipt)


@pytest.mark.asyncio
async def test_private_routes_without_tunnel_id_still_block_the_filtered_tunnel() -> None:
    account = "a" * 32
    tunnel_id = str(uuid4())
    rows = [{"network": "192.0.2.0/24"}]

    def provider(request):
        assert request.url.path == f"/client/v4/accounts/{account}/teamnet/routes"
        assert request.url.params["tunnel_id"] == tunnel_id
        return httpx.Response(200, json={
            "success": True, "result": rows, "result_info": {"total_count": 1},
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = TunnelRest(http, account_id=account, zone_id="b" * 32, hostname="managed.example.test", token=SecretStr("fixture"))
        with pytest.raises(OwnershipConflict, match="private-network routes target"):
            await client.verify_private_routes(tunnel_id)
        rows.clear()
        with pytest.raises(ObservationPending, match="could not be confirmed"):
            await client.verify_private_routes(tunnel_id)
