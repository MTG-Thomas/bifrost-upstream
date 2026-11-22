"""Cloudflare preflight and activation use exact, read-only provider observations."""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from scripts.test_cloudflare_fixture import _release
from pydantic import SecretStr
from shared.models import CloudflareActivateRequest, CloudflarePreflightRequest

from src.services.worker_pool import cloudflare_activation as activation, cloudflare_policy
from src.services.worker_pool.cloudflare_credentials import StagedCloudflareCredentialUnavailable
from src.services.worker_pool.cloudflare_provisioning import OwnershipCheck, OwnershipConflict

ACCOUNT = "a" * 32
ZONE = "b" * 32
SECOND_ZONE = "c" * 32
TOKEN = "synthetic-management-token"


@pytest.fixture
def discovery(monkeypatch):
    provider = SimpleNamespace(list_allowed=True, zones=[ZONE], paid=True, denied=False, occupied=False, reads=[])

    def respond(request):
        assert request.method == "GET"
        assert request.headers["Authorization"] == f"Bearer {TOKEN}"
        path = request.url.path.removeprefix("/client/v4/")
        provider.reads.append(path)
        if path == "accounts":
            if not provider.list_allowed:
                return httpx.Response(403)
            result = [{"id": ACCOUNT, "name": "Example Account"}]
        elif path == f"accounts/{ACCOUNT}":
            result = {"id": ACCOUNT}
        elif path == f"accounts/{ACCOUNT}/entitlements":
            if provider.denied:
                return httpx.Response(403)
            result = [
                {"id": feature, "deleted_date": "", "allocation": {"type": "bool", "value": True}}
                for feature in ("workers.enabled", "containers.enabled")
            ] if provider.paid else []
        elif path == "zones":
            assert request.url.params["account.id"] == ACCOUNT
            if not provider.list_allowed:
                return httpx.Response(403)
            result = [
                {"id": identity, "name": name, "status": "active", "account": {"id": ACCOUNT}}
                for identity, name in zip(provider.zones, ("example.test", "second.test"))
            ]
        elif path == f"zones/{ZONE}":
            result = {"id": ZONE, "name": "example.test", "status": "active", "account": {"id": ACCOUNT}}
        elif path == f"zones/{ZONE}/dns_records":
            result = [{"name": request.url.params["name.exact"]}] if provider.occupied else []
        else:
            raise AssertionError(path)
        return httpx.Response(200, json={"success": True, "result": result})

    release = _release(lifetime_meters=True)
    monkeypatch.setattr(activation, "load_workers_release", lambda: release)
    monkeypatch.setattr(cloudflare_policy, "load_workers_release", lambda: release)
    monkeypatch.setattr(activation, "connector_channel_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(activation, "httpx", SimpleNamespace(
        AsyncClient=lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(respond), **kw),
    ))
    cloudflare_policy._qualified_workers_suggestion.cache_clear()
    yield provider
    cloudflare_policy._qualified_workers_suggestion.cache_clear()


@pytest.fixture
def context():
    return SimpleNamespace(scalar=AsyncMock(return_value=None), get=AsyncMock(return_value=None)), SimpleNamespace(
        principal=SimpleNamespace(user_id=uuid4()),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["unique", "multiple", "unlisted", "unpaid", "denied", "occupied"])
async def test_preflight_discovery(discovery, context, case):
    db, caller = context
    if case == "multiple":
        discovery.zones.append(SECOND_ZONE)
    if case == "unlisted":
        discovery.list_allowed = False
    if case == "unpaid":
        discovery.paid = False
    if case == "denied":
        discovery.denied = True
    if case == "occupied":
        discovery.occupied = True
    result = await activation.preflight_cloudflare(db, caller, CloudflarePreflightRequest(
        api_token=TOKEN, account_id=ACCOUNT if case == "unlisted" else None,
    ))
    assert TOKEN not in result.model_dump_json()
    assert result.account_id == ACCOUNT
    assert result.recommended_reachability == "managed_tunnel"
    assert result.preview is not None
    assert str(result.preview.fleet_envelope_usd) == "32.13"
    if case == "unique":
        assert result.zone_id == ZONE
        assert result.worker_ingress_origin.endswith(".example.test")
        assert result.preflight_receipt
    else:
        assert result.preflight_receipt is None
    if case == "multiple":
        assert result.zones is not None
        assert [zone.id for zone in result.zones] == [ZONE, SECOND_ZONE]
        assert result.zone_id is None
    if case == "unlisted":
        assert result.accounts is None and result.zones is None
        selected = await activation.preflight_cloudflare(db, caller, CloudflarePreflightRequest(
            api_token=TOKEN, account_id=ACCOUNT, zone_id=ZONE,
        ))
        assert selected.zone_name == "example.test"
        assert selected.preflight_receipt
    if case == "unpaid":
        assert result.workers_paid == "unverified"
    if case == "denied":
        assert result.workers_paid == "denied"
    if case == "occupied":
        assert result.hostname_collision
        assert result.zone_name == "example.test"


@pytest.mark.asyncio
async def test_submit_rechecks_account_zone_and_dns_before_persisting(discovery, context, monkeypatch):
    db, caller = context
    result = await activation.preflight_cloudflare(db, caller, CloudflarePreflightRequest(api_token=TOKEN))
    discovery.reads.clear()
    discovery.occupied = True
    create = AsyncMock()
    monkeypatch.setattr(activation, "create_cloudflare_installation", create)
    body = CloudflareActivateRequest(
        api_token=TOKEN, preflight_receipt=result.preflight_receipt, consent=True,
        consent_version=result.consent_version, monthly_limit_usd="75",
    )
    with pytest.raises(OwnershipConflict, match="already in use"):
        await activation.activate_cloudflare(db, caller, body)
    assert discovery.reads == [f"accounts/{ACCOUNT}", f"accounts/{ACCOUNT}/entitlements",
                               f"zones/{ZONE}", f"zones/{ZONE}/dns_records"]
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_activation_replay_uses_enable_deduplication(discovery, context, monkeypatch):
    db, caller = context
    result = await activation.preflight_cloudflare(db, caller, CloudflarePreflightRequest(api_token=TOKEN))
    body = CloudflareActivateRequest(
        api_token=TOKEN, preflight_receipt=result.preflight_receipt, consent=True,
        consent_version=result.consent_version, monthly_limit_usd="75.00",
    )
    job = SimpleNamespace(id=uuid4(), status="queued")
    db.get.return_value = SimpleNamespace(
        retired_at=None, desired_state="enabled", desired_revision=1,
        policy={"activation_fingerprint": activation._digest(f"{body.preflight_receipt}:{body.monthly_limit_usd.normalize()}")},
    )
    discovery.reads.clear()
    enqueue = AsyncMock(return_value=(job, True))
    monkeypatch.setattr(activation, "enqueue_cloudflare_enable", enqueue)
    assert await activation.activate_cloudflare(db, caller, body) == (job, True)
    assert discovery.reads == []
    enqueue.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(("case", "error", "message"), [
    ("limit", cloudflare_policy.CloudflareBudgetTooLow, "estimated fleet usage"),
    ("expired", cloudflare_policy.CloudflarePolicyConflict, "expired"),
    ("generation", cloudflare_policy.CloudflarePolicyConflict, "changed"),
    ("release", cloudflare_policy.CloudflarePolicyConflict, "changed"),
    ("token", cloudflare_policy.CloudflarePolicyConflict, "run Cloudflare preflight"),
    ("reference", cloudflare_policy.CloudflarePolicyConflict, "run Cloudflare preflight"),
    ("actor", cloudflare_policy.CloudflarePolicyConflict, "run Cloudflare preflight"),
])
async def test_activation_refuses_changed_consent_input(discovery, context, monkeypatch, case, error, message):
    db, caller = context
    result = await activation.preflight_cloudflare(db, caller, CloudflarePreflightRequest(api_token=TOKEN))
    assert result.preflight_receipt is not None
    receipt = json.loads(activation.decrypt_secret(result.preflight_receipt))
    if case == "expired":
        receipt["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    if case == "generation":
        db.scalar.return_value = datetime.now(UTC)
    if case == "release":
        receipt["release"] = "previous-release"
    if case == "actor":
        caller.principal.user_id = uuid4()
    body = CloudflareActivateRequest(
        api_token=None if case == "reference" else "different-management-token" if case == "token" else TOKEN,
        credential_ref=uuid4() if case == "reference" else None,
        preflight_receipt=activation.encrypt_secret(json.dumps(receipt)), consent=True,
        consent_version=result.consent_version, monthly_limit_usd="32.12" if case == "limit" else "75",
    )
    discovery.reads.clear()
    create = AsyncMock()
    monkeypatch.setattr(activation, "create_cloudflare_installation", create)
    with pytest.raises(error, match=f"(?i){message}"):
        await activation.activate_cloudflare(db, caller, body)
    create.assert_not_awaited()
    assert not discovery.reads


@pytest.mark.asyncio
async def test_staged_expiry_caps_the_review_and_missing_reference_refuses_activation(discovery, context, monkeypatch):
    db, caller = context
    expires = datetime.now(UTC) + timedelta(minutes=1)
    staged = AsyncMock(return_value=(ACCOUNT, SecretStr(TOKEN), expires))
    monkeypatch.setattr(activation, "read_staged_connect_token", staged)
    ref = uuid4()
    result = await activation.preflight_cloudflare(db, caller, CloudflarePreflightRequest(credential_ref=ref))
    assert result.expires_at == expires
    assert result.preflight_receipt is not None
    assert json.loads(activation.decrypt_secret(result.preflight_receipt))["expires_at"] == expires.isoformat()
    staged.side_effect = StagedCloudflareCredentialUnavailable("Invalid or expired Cloudflare credential reference")
    with pytest.raises(StagedCloudflareCredentialUnavailable):
        await activation.activate_cloudflare(db, caller, CloudflareActivateRequest(
            credential_ref=ref, preflight_receipt=result.preflight_receipt, consent=True,
            consent_version=result.consent_version, monthly_limit_usd="75",
        ))


@pytest.mark.asyncio
@pytest.mark.parametrize("check", [OwnershipCheck.MULTIPLE_OCCUPANTS, OwnershipCheck.EXACT_INVENTORY])
async def test_ambiguous_hostname_keeps_discovery(discovery, context, monkeypatch, check):
    db, caller = context
    monkeypatch.setattr(activation.TunnelRest, "require_unused_hostname", AsyncMock(
        side_effect=OwnershipConflict("Ambiguous DNS inventory", check=check),
    ))
    result = await activation.preflight_cloudflare(db, caller, CloudflarePreflightRequest(api_token=TOKEN))
    assert result.hostname_collision and result.zone_id == ZONE and result.preflight_receipt is None


@pytest.mark.asyncio
async def test_edited_policy_hides_the_preset_forecast_and_existing_mode_keeps_the_zone(discovery, context):
    db, caller = context
    policy = cloudflare_policy.suggested_cloudflare_policy()
    assert policy is not None
    workers = policy.workers.model_copy(update={"installation_slots": 8})
    result = await activation.preflight_cloudflare(db, caller, CloudflarePreflightRequest(
        api_token=TOKEN, reachability_mode="existing_https_endpoint",
        policy=policy.model_copy(update={"workers": workers}),
    ))
    assert result.zone_name == "example.test" and result.zone_id is None
    assert result.preview and result.preview.estimated_usage_min_usd is None
