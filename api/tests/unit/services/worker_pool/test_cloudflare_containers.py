"""Release, provider readback and placement checks without Cloudflare calls."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr, ValidationError
from src.models.orm.execution_workers import ExecutionWorker
from src.services.worker_pool.cloudflare_admission import PrimaryPool
from src.services.worker_pool.cloudflare_containers_admission import (
    LinuxReview,
    linux_review_digest,
    primary_has_no_fit,
)
from src.services.worker_pool.cloudflare_containers_egress import (
    REQUIRED_DENIALS,
    VerifiedEgress,
    verify_container_egress,
)
from src.services.worker_pool.cloudflare_containers_policy import (
    container_envelope,
    container_reserve_cost,
)
from src.services.worker_pool.cloudflare_containers_release import (
    ContainersManifest,
    ContainersReleaseFetchError,
    ContainersReleaseUnavailable,
    load_containers_release,
    verify_containers_release,
)
from src.services.worker_pool.cloudflare_containers_rest import (
    ContainerRegistry,
    ContainersRest,
)
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    OwnershipConflict,
    ResourceIntent,
    ResourceTarget,
)
from tests.helpers.cloudflare_containers import (
    APP_ID,
    APP_VERSION,
    json_bytes,
    make_release,
    policy,
    successful_probe,
)

ACCOUNT = "a" * 32
NAMESPACE = "b" * 32
APP = "c" * 32
TOKEN = SecretStr("synthetic-token")


def test_image_requires_both_exact_manifest_and_config() -> None:
    release, image, config = make_release()
    release.verify_image(image, config)
    with pytest.raises(ContainersReleaseUnavailable):
        release.verify_image(image, config.replace(b"amd64", b"arm64"))
    with pytest.raises(ContainersReleaseUnavailable):
        release.verify_image(image + b" ", config)


@pytest.mark.parametrize(("image_user", "options"), [
    ("1000:1000", ("BIFROST_BAKED_WHEELS_ONLY=1",)),
    ("0:0", ()),
    ("0:0", ("BIFROST_BAKED_WHEELS_ONLY=1", "BIFROST_BAKED_WHEELS_ONLY=0")),
])
def test_image_requires_the_privilege_split_and_baked_wheel_profile(image_user, options) -> None:
    release, image, config = make_release(image_user=image_user, options=options)
    with pytest.raises(ContainersReleaseUnavailable):
        release.verify_image(image, config)


@pytest.mark.asyncio
async def test_descriptor_requires_release_signer_and_exact_source(tmp_path, monkeypatch) -> None:
    release, _, _ = make_release()
    (tmp_path / "manifest.sigstore.json").write_text("{}", encoding="utf-8")
    module = b"export class ScopeRegistry {}"
    data = release.manifest.model_dump(mode="json")
    data["provider_modules"]["container-registry.mjs"] = sha256(module).hexdigest()
    (tmp_path / "manifest.json").write_text(json.dumps(data), encoding="utf-8")
    (tmp_path / "container-registry.mjs").write_bytes(module)
    workspace = tmp_path / "verification"
    workspace.mkdir()
    signer = "https://github.com/example/bifrost/.github/workflows/ci.yml@refs/tags/v1.0.0"
    workers = SimpleNamespace(
        manifest=SimpleNamespace(source_revision=release.manifest.source_revision),
        digest=release.manifest.workers_release_digest,
    )
    trusted = False
    trust_metadata_available = True

    async def tool(executable, *args, **kwargs):
        assert executable == Path("cosign")
        if args == ("version",):
            return b"GitVersion:    v2.4.1\n"
        if args == ("initialize",):
            if not trust_metadata_available:
                raise ContainersReleaseUnavailable("Fixture TUF download failed")
            return b""
        assert args[:4] == ("verify-blob", "--certificate-identity", signer, "--offline")
        if not trusted:
            raise ContainersReleaseUnavailable("Invalid signature")
        return b""

    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_release.run_oci_tool", tool)
    with pytest.raises(ContainersReleaseUnavailable, match="Invalid signature"):
        await verify_containers_release(tmp_path, workers, cosign=Path("cosign"), signer=signer, workspace=workspace)
    trusted = True
    trust_metadata_available = False
    with pytest.raises(ContainersReleaseFetchError, match="Sigstore trust metadata"):
        await verify_containers_release(tmp_path, workers, cosign=Path("cosign"), signer=signer, workspace=workspace)
    trust_metadata_available = True
    assert (await verify_containers_release(
        tmp_path, workers, cosign=Path("cosign"), signer=signer, workspace=workspace,
    )).manifest == ContainersManifest.model_validate(data)
    workers.manifest.source_revision = "f" * 40
    with pytest.raises(ContainersReleaseUnavailable, match="source pin"):
        await verify_containers_release(tmp_path, workers, cosign=Path("cosign"), signer=signer, workspace=workspace)


@pytest.mark.asyncio
async def test_load_matched_release_follows_only_github_asset_redirects(tmp_path, monkeypatch) -> None:
    release, _, _ = make_release()
    module = b"export class ScopeRegistry {}"
    manifest = release.manifest.model_dump(mode="json")
    manifest["provider_modules"]["container-registry.mjs"] = sha256(module).hexdigest()
    assets = {
        "manifest.json": json_bytes(manifest),
        "manifest.sigstore.json": b"{}",
        "container-registry.mjs": module,
    }
    workers = Mock(
        manifest=SimpleNamespace(source_revision=release.manifest.source_revision, release_version="1.2.3"),
        digest=release.manifest.workers_release_digest,
    )
    repo = "Example/Bifrost-fork"
    signer = f"https://github.com/{repo}/.github/workflows/ci.yml@refs/tags/v1.2.3"
    trust = tmp_path / "linux-release-signer"
    trust.write_text(signer + "\n", encoding="ascii")
    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_release._SIGNER_FILE", trust)
    redirect_host = "release-assets.githubusercontent.com"
    downloads = []

    def packager(request):
        assert "authorization" not in request.headers
        filename = request.url.path.rsplit("/", 1)[-1]
        if request.url.host == "github.com":
            asset = filename.removeprefix("cloudflare-containers-")
            assert asset in assets
            assert request.url.path == f"/{repo}/releases/download/v1.2.3/cloudflare-containers-{asset}"
            return httpx.Response(302, headers={"location": f"https://{redirect_host}/{asset}"})
        assert request.url.host == "release-assets.githubusercontent.com"
        downloads.append(filename)
        return httpx.Response(200, content=assets[filename])

    async def tool(executable, *args, **kwargs):
        assert executable == Path("/app/artifacts/cloudflare/bin/cosign")
        if args == ("version",):
            return b"GitVersion: v2.4.1\n"
        if args != ("initialize",):
            assert args[:4] == ("verify-blob", "--certificate-identity", signer, "--offline")
        return b""

    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_release.run_oci_tool", tool)
    async with httpx.AsyncClient(transport=httpx.MockTransport(packager)) as http:
        assert (await load_containers_release(http, workers)).digest == sha256(assets["manifest.json"]).hexdigest()
        assert downloads == list(assets)
        redirect_host = "other.example.test"
        with pytest.raises(ContainersReleaseUnavailable, match="Unexpected Linux release asset location"):
            await load_containers_release(http, workers)
        trust.write_text(signer.removesuffix("v1.2.3") + "v0.0.1", encoding="ascii")
        with pytest.raises(ContainersReleaseUnavailable, match="does not match its release"):
            await load_containers_release(http, workers)


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "error"), [
    (None, ContainersReleaseFetchError),
    (503, ContainersReleaseFetchError),
    (404, ContainersReleaseUnavailable),
])
async def test_release_fetch_distinguishes_transport_from_missing_assets(tmp_path, monkeypatch, status, error):
    trust = tmp_path / "linux-release-signer"
    trust.write_text("https://github.com/example/bifrost/.github/workflows/ci.yml@refs/tags/v1.0.0\n", encoding="ascii")
    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_release._SIGNER_FILE", trust)
    workers = SimpleNamespace(manifest=SimpleNamespace(release_version="1.0.0"))

    def fetch(request):
        if status is None:
            raise httpx.ReadTimeout("Fixture network failure", request=request)
        return httpx.Response(status)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fetch)) as http:
        with pytest.raises(error):
            await load_containers_release(http, workers)


@pytest.mark.asyncio
@pytest.mark.parametrize(("initial_tags", "previously_sent", "conflict"), [
    (None, True, False),
    ([], True, False),
    (["release"], True, False),
    (["other"], True, True),
    (["release"], False, True),
    (["release", "other"], True, True),
    (["release", "other"], False, True),
])
async def test_registry_replays_only_into_an_empty_repository(
    tmp_path, monkeypatch, initial_tags, previously_sent, conflict,
) -> None:
    release, image, config = make_release()
    installation = uuid4()
    name = f"bifrost-{installation.hex}-r1"
    intent = ResourceIntent(ResourceTarget(installation, ACCOUNT, "containers", "container_image", name), uuid4(), 1, release.digest)
    tags = list(initial_tags or [])
    permissions = []
    copies = []

    async def tool(executable, *args, **kwargs):
        if args == ("version",):
            return b"v0.20.6"
        if args == ("--version",):
            return b"1.0.0-beta.31"
        if args[:2] == ("accounts", "get"):
            return json_bytes({"id": ACCOUNT})
        if args[:3] == ("containers", "images", "list"):
            return json_bytes([{"name": name, "tags": tags}] if tags or initial_tags is not None else [])
        if args[0] == "manifest":
            return image + b"\n"
        if args[0] == "config":
            return config + b"\n"
        if args[0] == "copy":
            copies.append(args[:3])
            tags.append("release")
            return b""
        pytest.fail(f"Unexpected OCI tool call {args[0]}")

    def provider(request: httpx.Request) -> httpx.Response:
        permissions.append(json.loads(request.content)["permissions"])
        return httpx.Response(200, json={"success": True, "result": {
            "account_id": ACCOUNT, "registry_host": "registry.cloudflare.com",
            "username": "synthetic", "password": "synthetic",
        }})

    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_rest.run_oci_tool", tool)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        registry = ContainerRegistry(ContainersRest(http), crane=tmp_path / "crane", cf=tmp_path / "cf")
        if conflict:
            with pytest.raises(OwnershipConflict):
                await registry.copy(TOKEN, intent, release, previously_sent=previously_sent)
            assert not permissions and not copies
            return
        result = await registry.copy(TOKEN, intent, release, previously_sent=previously_sent)
        assert result.provider_version == release.manifest.image_digest
        if initial_tags:
            assert permissions == [["pull"]] and not copies
        else:
            assert permissions == [["pull", "push"]]
            assert copies == [("copy", release.manifest.image, result.provider_id)]


def test_linux_review_digest_distinguishes_replacement_and_reacceptance() -> None:
    workflow = uuid4()
    declaration = {"path": "workflow.py", "previous_revision": None, "inactive_history_count": 0}
    first = linux_review_digest(workflow, None, declaration)
    replaced = linux_review_digest(workflow, None, {**declaration, "previous_revision": first})
    reaccepted = linux_review_digest(workflow, None, {**declaration, "inactive_history_count": 1})
    assert len({first, replaced, reaccepted}) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"provider_version": "f" * 32}, {"internet_enabled": True}, {"signed_control_round_trip": False},
    {"bounded_relay": False}, {"denied": frozenset()}, {"intercepted_origins": frozenset()},
])
async def test_egress_proof_requires_exact_release_and_observed_network_controls(change) -> None:
    origin = "https://worker.example.test"
    installation_id = uuid4()
    observation = successful_probe(installation_id, 1, "a" * 64, origin)
    if "signed_control_round_trip" in change:
        observation = replace(observation, **change)
    else:
        observation = replace(observation, report=observation.report.model_copy(update=change))
    with patch(
        "src.services.worker_pool.cloudflare_containers_egress._probe_container_egress",
        new=AsyncMock(return_value=observation),
    ):
        assert await verify_container_egress(
            None, None, installation_id=installation_id, revision=1, release_digest="a" * 64, origin=origin,
            probe_worker_id=observation.report.worker_id,
        ) is None
    with pytest.raises(TypeError, match="must be verified"):
        VerifiedEgress(observation, _mint=object())


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["verified", "wrong-nonce", "no-ingress-witness", "redirect"])
async def test_owned_egress_probe_challenges_scope_and_requires_api_witness(case) -> None:
    installation, worker = uuid4(), uuid4()
    now, origin, digest = datetime.now(UTC), "https://worker.example.test", "a" * 64
    endpoint = "https://scope.example.test"
    token = SecretStr("fixture-scope-authentication-token")
    reservation = SimpleNamespace(
        installation_id=installation, deployment_revision=1, worker_id=worker,
        container_tenant="platform", container_reserved_at=now,
        container_envelope={
            "seconds": 500, "start_deadline": (now + timedelta(seconds=20)).isoformat(),
            "image_digest": "sha256:" + "e" * 64, "instance_type": "standard-1",
            "credential_version": 1, "ingress_bytes": 100, "outbound_bytes": 100,
        },
    )
    db = AsyncMock()
    db.get.side_effect = [
        SimpleNamespace(id=installation, account_id="e" * 32, retired_at=None,
                        desired_state="enabled", desired_revision=1, worker_ingress_origin=origin),
        SimpleNamespace(endpoint=endpoint, bearer_version=1), object(),
    ]
    db.execute.side_effect = [
        Mock(one_or_none=Mock(return_value=SimpleNamespace(provider_id=APP_ID, provider_version=APP_VERSION))),
        Mock(one_or_none=Mock(return_value=(reservation, "e" * 32))),
    ]
    db.scalar.side_effect = [now, None if case == "no-ingress-witness" else worker]

    def probe(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST" and str(request.url) == endpoint + "/v1/egress-probe"
        assert request.headers["authorization"] == f"Bearer {token.get_secret_value()}"
        body = json.loads(request.content)
        pin = body["pin"]
        assert pin["worker"] == str(worker) and len(body["nonce"]) == 43
        assert pin["deadline"] - pin["reservedAt"] == 500_000
        assert pin["startDeadline"] - pin["reservedAt"] == 20_000
        assert body["control_key"] == "f" * 64
        if case == "redirect":
            return httpx.Response(307, headers={"location": "https://unrelated.example.test"})
        return httpx.Response(200, json={
            "nonce": "stale" if case == "wrong-nonce" else body["nonce"], "worker_id": str(worker),
            "application_id": APP_ID, "provider_version": APP_VERSION, "origin": origin,
            "internet_enabled": False, "intercepted_origins": [origin],
            "bounded_relay": True, "denied": sorted(REQUIRED_DENIALS),
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(probe), follow_redirects=True) as client:
        with (
            patch("src.services.worker_pool.cloudflare_containers_egress.open_credential", return_value=token),
            patch("src.services.worker_pool.cloudflare_containers_egress.load_managed_control_key",
                  new=AsyncMock(return_value="f" * 64)),
        ):
            proof = await verify_container_egress(
                db, client, installation_id=installation, revision=1, release_digest=digest,
                origin=origin, probe_worker_id=worker,
            )
    if case == "verified":
        assert proof is not None and proof.observed_at == now and proof.provider_version == APP_VERSION
    else:
        assert proof is None


def test_baked_wheel_closure_is_required_for_package_finding() -> None:
    release, _, _ = make_release()
    fields = {
        "findings": {"CF_PROCESS", "CF_PACKAGE"}, "release_digest": release.digest, "installation_id": uuid4(),
        "deployment_revision": 1, "consent_version": "fixture", "worker_ingress_origin": "https://worker.example.test",
        "egress": "worker-ingress-only-v1", "path": "workflow.py", "function_name": "run",
        "source_pins": {"workflow.py": "1" * 64}, "memory_bytes": 123, "timeout_seconds": 123,
        "ingress_bytes": 100, "outbound_bytes": 100,
        "previous_revision": None, "inactive_history_count": 0,
    }
    assert LinuxReview(**fields, wheels=release.manifest.wheels).matches_release(release)
    assert not LinuxReview(**fields, wheels={}).matches_release(release)
    with pytest.raises(ValidationError):
        LinuxReview(**{**fields, "findings": {"CF_NATIVE", "CF_URL_REVIEW"}}, wheels={})


def test_pilot_defaults_and_reserve_splits_at_utc_rollover() -> None:
    limits = policy()
    assert not limits.enabled
    assert (limits.installation_instances, limits.tenant_instances, limits.workflow_slots) == (2, 1, 1)
    assert (limits.instance_type, limits.idle_seconds) == ("standard-1", 120)
    release, _, _ = make_release()
    reserve = container_envelope(limits, release.manifest.rates)
    start = datetime(2026, 11, 1, tzinfo=UTC)
    end = datetime(2026, 12, 1, tzinfo=UTC)
    now = end - timedelta(seconds=60)
    fixed = Decimal(reserve["fixed_usd"])
    assert container_reserve_cost(reserve, now, start, end) == fixed + Decimal("0.00002056") * 60
    assert container_reserve_cost(reserve, now, end, end + timedelta(days=31)) == fixed + Decimal("0.00002056") * 330


def test_primary_requires_healthy_saturation_and_no_memory_fit() -> None:
    now = datetime.now(UTC)
    primary = PrimaryPool("job", ceiling=1)
    worker = ExecutionWorker(
        id=uuid4(), form="job", state="ready", execution_lane="normal", heartbeat_at=now,
        supported_handlers=["workflow.execute"], memory_budget_bytes=200, memory_reserved_bytes=100,
    )
    assert not primary_has_no_fit(primary, [], now, 200)
    assert not primary_has_no_fit(primary, [worker], now, 100)
    assert primary_has_no_fit(primary, [worker], now, 101)
    worker.heartbeat_at = None
    assert not primary_has_no_fit(primary, [worker], now, 101)


@pytest.mark.asyncio
async def test_opaque_application_readback_and_ambiguous_create_use_exact_inventory() -> None:
    installation = uuid4()
    name = f"bifrost-{installation.hex}-r1"
    intent = ResourceIntent(ResourceTarget(installation, ACCOUNT, "containers", "container_application", name), uuid4(), 1, "1" * 64)
    row = {
        "id": APP, "account_id": ACCOUNT, "name": name, "scheduling_policy": "durable_object",
        "durable_objects": {"namespace_id": NAMESPACE},
    }
    requests = []

    def provider(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/applications"):
            return httpx.Response(200, json={"success": True, "result": [row]})
        assert request.url.path.endswith(f"/{APP}")
        return httpx.Response(200, json={"success": True, "result": row})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        adapter = ContainersRest(http)
        result = await adapter.application(TOKEN, intent, NAMESPACE, previously_sent=True)
        assert result.provider_id == APP
        assert all(request.method == "GET" for request in requests)
        with pytest.raises(OwnershipConflict):
            await adapter.application(TOKEN, intent, "d" * 32, previously_sent=True)


@pytest.mark.asyncio
async def test_inventory_missing_suffix_cannot_confirm_prior_create() -> None:
    installation = uuid4()
    name = f"bifrost-{installation.hex}-r1"
    intent = ResourceIntent(ResourceTarget(installation, ACCOUNT, "containers", "container_application", name), uuid4(), 1, "1" * 64)

    def provider(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, json={"success": True, "result": [], "result_info": {"total_count": 1}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        with pytest.raises(ObservationPending, match="incomplete"):
            await ContainersRest(http).application(TOKEN, intent, NAMESPACE, previously_sent=True)
