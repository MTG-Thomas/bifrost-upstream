"""Release, provider readback and placement checks without Cloudflare calls."""

import asyncio
import json
from asyncio import gather
from collections import Counter
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from hmac import digest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import httpx
import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from pydantic import SecretStr, ValidationError
from src.jobs.platform import cloudflare_enable
from src.jobs.platform.base import PlatformJobContext, PlatformJobRequiresAction
from src.models.orm.cloudflare_capacity import CloudflareWorkerReservation
from src.models.orm.execution_workers import ExecutionWorker
from src.services.worker_pool import cloudflare_containers_admission, cloudflare_containers_rest
from src.services.worker_pool.cloudflare_admission import PrimaryPool
from src.services.worker_pool.cloudflare_containers_admission import (
    LinuxReview,
    linux_review_digest,
    primary_has_no_fit,
)
from src.services.worker_pool.cloudflare_containers_auth import container_scope_public_key, request_container_control
from src.services.worker_pool.cloudflare_containers_egress import (
    ContainerEgressMismatch,
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
    DeletionSettling,
    ObservationPending,
    ObservationSettling,
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
)
from tests.helpers.cloudflare_containers import (
    APP_ID,
    APP_VERSION,
    json_bytes,
    make_release,
    policy,
    successful_probe,
    verified_egress,
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
        assert args == (
            "verify-blob", "--new-bundle-format", "--offline", "--trusted-root",
            ".sigstore/root/targets/trusted_root.json", "--certificate-identity", signer,
            "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
            "--bundle", "manifest.sigstore.json", "manifest.json",
        )
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
            assert args[:3] == ("verify-blob", "--new-bundle-format", "--offline")
            assert args[5:7] == ("--certificate-identity", signer)
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
    (404, ContainersReleaseFetchError),
    (403, ContainersReleaseUnavailable),
])
async def test_release_fetch_distinguishes_settling_from_terminal_errors(tmp_path, monkeypatch, status, error):
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
@pytest.mark.parametrize(("initial_tags", "previously_sent", "conflict", "preparation"), [
    (None, True, False, "ready"),
    ([], True, False, "ready"),
    (["release"], True, False, "ready"),
    (["other"], True, True, "ready"),
    (["release"], False, True, "ready"),
    (["release", "other"], True, True, "ready"),
    (["release", "other"], False, True, "ready"),
    ([], True, False, "pending"),
    ([], True, False, "error"),
    ([], True, False, "wrong_image"),
    ([], True, False, "foreign_after_copy"),
    ([], True, False, "tag_lag"),
    ([], True, False, "timeout"),
    ([], True, False, "wrong_digest"),
])
async def test_registry_replays_only_into_an_empty_repository(
    tmp_path, monkeypatch, initial_tags, previously_sent, conflict, preparation,
) -> None:
    release, image, config = make_release()
    installation = uuid4()
    name = f"bifrost-{installation.hex}-r1"
    intent = ResourceIntent(ResourceTarget(installation, ACCOUNT, "containers", "container_image", name), uuid4(), 1, release.digest)
    tags = list(initial_tags or [])
    permissions = []
    copies = []
    preparations = []
    tag_reads = 0

    async def tool(executable, *args, **kwargs):
        nonlocal tag_reads
        if args == ("version",):
            return b"0.20.6"
        if args == ("--version",):
            return b"1.0.0-beta.31"
        if args[:2] == ("accounts", "get"):
            return json_bytes({"id": ACCOUNT})
        if args[:3] == ("containers", "images", "list"):
            tag_reads += 1
            if preparation == "tag_lag" and tag_reads == 2:
                return json_bytes([])
            return json_bytes([{"name": name, "tags": tags}] if tags or initial_tags is not None else [])
        if args[0] == "digest":
            return (("sha256:" + "b" * 64) if preparation == "wrong_digest" else release.manifest.image_digest).encode()
        if args[0] == "manifest":
            return image + b"\n"
        if args[0] == "config":
            return config + b"\n"
        if args[0] == "copy":
            copies.append(args[:3])
            tags.append("release")
            if preparation == "foreign_after_copy":
                tags.append("another-tag")
            return b""
        pytest.fail(f"Unexpected OCI tool call {args[0]}")

    def provider(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert request.method == "POST"
        if request.url.path.endswith("/containers/image-preparations"):
            digest = release.manifest.image_digest
            assert body == {"image": f"registry.cloudflare.com/{ACCOUNT}/{name}@{digest}"}
            status = preparation if not preparations else "ready"
            if preparation == "tag_lag":
                status = "ready"
            if preparation == "timeout":
                status = "pending"
            preparations.append(status)
            return httpx.Response(202 if status == "pending" else 200, json={"success": True, "result": {
                "status": "ready" if status == "wrong_image" else status,
                "image": "different-image" if status == "wrong_image" else body["image"],
                "reason": "private-provider-value",
                "private-provider-field": "private-provider-value",
            }})
        assert request.url.path.endswith("/containers/registries/registry.cloudflare.com/credentials")
        permissions.append(body["permissions"])
        return httpx.Response(200, json={"success": True, "result": {
            "account_id": ACCOUNT, "registry_host": "registry.cloudflare.com",
            "username": "synthetic", "password": "synthetic",
        }})

    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_rest.run_oci_tool", tool)
    monkeypatch.setattr(cloudflare_containers_rest, "_IMAGE_SETTLEMENT_INTERVAL", 0)
    if preparation == "timeout":
        monkeypatch.setattr(cloudflare_containers_rest, "_IMAGE_SETTLEMENT_SECONDS", 0)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        registry = ContainerRegistry(ContainersRest(http), crane=tmp_path / "crane", cf=tmp_path / "cf")
        if conflict:
            with pytest.raises(OwnershipConflict):
                await registry.copy(TOKEN, intent, release, previously_sent=previously_sent)
            assert not permissions and not copies and not preparations
            return
        if preparation in {"foreign_after_copy", "wrong_digest"}:
            with pytest.raises(OwnershipConflict):
                await registry.copy(TOKEN, intent, release, previously_sent=previously_sent)
            assert not preparations
            return
        if preparation in {"error", "wrong_image"}:
            error = ObservationPending if preparation == "error" else OwnershipConflict
            with pytest.raises(error):
                await registry.copy(TOKEN, intent, release, previously_sent=previously_sent)
            assert preparations == [preparation]
            return
        if preparation == "timeout":
            with pytest.raises(ObservationSettling) as failure:
                await registry.copy(TOKEN, intent, release, previously_sent=previously_sent)
            details = failure.value.as_result()
            assert details["preparation_http_status"] == "202"
            assert details["preparation_status"] == "pending"
            assert details["preparation_shape"] == "object"
            assert details["preparation_fields"] == "image,status,reason"
            assert "private-provider" not in json.dumps(details)
            assert preparations == ["pending"] and len(copies) == 1
            return
        result = await registry.copy(TOKEN, intent, release, previously_sent=previously_sent)
        assert preparations == (["pending", "ready"] if preparation == "pending" else ["ready"])
        assert result.provider_version == release.manifest.image_digest
        if initial_tags:
            assert permissions == [["pull"]] and not copies
        else:
            assert permissions == [["pull", "push"]]
            assert copies == [("copy", release.manifest.image, result.provider_id)]


@pytest.mark.asyncio
@pytest.mark.parametrize("same_digest", [False, True])
async def test_registry_removes_only_the_exact_tag(tmp_path, monkeypatch, same_digest) -> None:
    installation = uuid4()
    name = f"bifrost-{installation.hex}-r1"
    intent = ResourceIntent(ResourceTarget(installation, ACCOUNT, "containers", "container_image", name), uuid4(), 1, "6" * 64)
    digest = "sha256:" + "a" * 64
    tags = ["release"]
    deletions = []

    async def tool(_executable, *args, **_kwargs):
        match args:
            case ("--version",):
                return b"1.0.0-beta.31"
            case ("accounts", "get", "-q"):
                return json_bytes({"id": ACCOUNT})
            case ("containers", "images", "list", "--filter", _):
                return json_bytes([{"name": name, "tags": tags}])
            case ("digest", tag):
                assert tag == f"registry.cloudflare.com/{ACCOUNT}/{name}:release"
                return digest.encode() if same_digest else ("sha256:" + "b" * 64).encode()
            case ("containers", "images", "delete", tag, "--force"):
                deletions.append(tag)
                tags.clear()
                return b""
            case _:
                pytest.fail(f"Unexpected OCI tool call {args[0]}")

    def provider(request):
        assert json.loads(request.content) == {"expiration_minutes": 15, "permissions": ["pull"]}
        return httpx.Response(200, json={"success": True, "result": {
            "account_id": ACCOUNT, "registry_host": "registry.cloudflare.com",
            "username": "synthetic", "password": "synthetic",
        }})

    monkeypatch.setattr(cloudflare_containers_rest, "run_oci_tool", tool)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        registry = ContainerRegistry(ContainersRest(http), crane=tmp_path / "crane", cf=tmp_path / "cf")
        if same_digest:
            assert await registry.delete(TOKEN, intent, digest)
            assert not await registry.delete(TOKEN, intent, digest)
        else:
            with pytest.raises(OwnershipConflict, match="different digest"):
                await registry.delete(TOKEN, intent, digest)
    assert deletions == ([f"{name}:release"] if same_digest else [])


def test_linux_review_digest_distinguishes_replacement_and_reacceptance() -> None:
    workflow = uuid4()
    declaration = {"path": "workflow.py", "previous_revision": None, "inactive_history_count": 0}
    first = linux_review_digest(workflow, None, declaration)
    replaced = linux_review_digest(workflow, None, {**declaration, "previous_revision": first})
    reaccepted = linux_review_digest(workflow, None, {**declaration, "inactive_history_count": 1})
    assert len({first, replaced, reaccepted}) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"origin": "https://other.example.test"},
    {"revision": 3}, {"release_digest": "c" * 64},
    {"signed_control_round_trip": False}, {"denied": frozenset()},
])
async def test_egress_proof_requires_exact_release_and_observed_network_controls(change) -> None:
    origin = "https://worker.example.test"
    installation_id = uuid4()
    observation = successful_probe(installation_id, 1, "a" * 64, origin)
    if "signed_control_round_trip" in change or "denied" in change:
        observation = replace(observation, **change)
    else:
        observation = replace(observation, report=observation.report.model_copy(update=change))
    with (
        patch("src.services.worker_pool.cloudflare_containers_egress._probe_container_egress",
              new=AsyncMock(return_value=observation)),
        nullcontext() if "signed_control_round_trip" in change else pytest.raises(ContainerEgressMismatch),
    ):
        assert await verify_container_egress(
            None, None, installation_id=installation_id, revision=1, release_digest="a" * 64, origin=origin,
            probe_worker_id=observation.report.worker_id,
        ) is None
    with pytest.raises(TypeError, match="must be verified"):
        VerifiedEgress(observation, _mint=object())


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["verified", "wrong-nonce", "no-ingress-witness", "missing-denial", "redirect"])
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
            "probe_nonce": "p" * 43,
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
    expected = successful_probe(installation, 1, digest, origin).denied
    db.scalar.side_effect = [now, None if case == "no-ingress-witness" else {
        "egress_denials": sorted(expected if case != "missing-denial" else expected - {"raw-tcp"}),
    }]

    def probe(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST" and str(request.url) == endpoint + "/v1/egress-probe"
        assert "authorization" not in request.headers
        assert len(request.headers["x-bifrost-incarnation-certificate"]) == 128
        assert len(request.headers["x-bifrost-request-signature"]) == 128
        body = json.loads(request.content)
        pin = body["pin"]
        assert pin["worker"] == str(worker) and body["nonce"] == "p" * 43
        assert pin["deadline"] - pin["reservedAt"] == 500_000
        assert pin["startDeadline"] - pin["reservedAt"] == 20_000
        assert body["control_key"] == "f" * 64
        if case == "redirect":
            return httpx.Response(307, headers={"location": "https://unrelated.example.test"})
        return httpx.Response(200, json={
            "nonce": "stale" if case == "wrong-nonce" else body["nonce"], "worker_id": str(worker),
            "origin": origin,
            "revision": 1, "release_digest": digest,
        })

    proof = None
    async with httpx.AsyncClient(transport=httpx.MockTransport(probe), follow_redirects=True) as client:
        with (
            patch("src.services.worker_pool.cloudflare_containers_egress.open_credential", return_value=token),
            patch("src.services.worker_pool.cloudflare_containers_egress.load_managed_control_key",
                  new=AsyncMock(return_value="f" * 64)),
            pytest.raises(ContainerEgressMismatch) if case in ("wrong-nonce", "missing-denial") else nullcontext(),
        ):
            proof = await verify_container_egress(
                db, client, installation_id=installation, revision=1, release_digest=digest,
                origin=origin, probe_worker_id=worker,
            )
    if case == "verified":
        assert proof is not None and proof.observed_at == now and proof.provider_version == APP_VERSION
    else:
        assert proof is None


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [
    "transient", "mismatch", "mismatch-retire-pending", "verified", "retire-pending",
    "no-spare-seat", "backoff", "qualification-backoff",
])
async def test_refresh_changes_admission_only_on_conflicting_evidence(monkeypatch, outcome):
    release, _, _ = make_release()
    installation_id = uuid4()
    origin = "https://worker.example.test"
    proof = await verified_egress(installation_id, 1, release.digest, origin)
    row = SimpleNamespace(
        id=installation_id, account_id=ACCOUNT, worker_ingress_origin=origin, deployment_evidence={}, desired_revision=1,
        admitted_revision=1, containers_ready_revision=1, containers_release_digest=release.digest,
    )
    db = AsyncMock()
    db.scalars.return_value = []
    db.execute.return_value = Mock(one_or_none=Mock(return_value=(APP_ID, APP_VERSION)))

    @asynccontextmanager
    async def transaction():
        yield db

    monkeypatch.setattr(cloudflare_enable, "get_db_context", transaction)
    monkeypatch.setattr(cloudflare_enable, "_locked_current", AsyncMock(return_value=row))
    monkeypatch.setattr(cloudflare_enable, "wake_pool_workers_after_commit", Mock())
    monkeypatch.setattr(cloudflare_enable, "canary_session_failure", Mock(
        return_value=cloudflare_enable.CanaryFailure("ack", "invalid_reply"),
    ))
    reserve = AsyncMock(return_value=None if outcome == "no-spare-seat" else CloudflareWorkerReservation(worker_id=uuid4()))
    monkeypatch.setattr(cloudflare_enable, "reserve_container_canary", reserve)
    monkeypatch.setattr(cloudflare_enable, "reconcile_container_stop", AsyncMock(return_value=True))
    if outcome in ("retire-pending", "mismatch-retire-pending"):
        async def pending_stop(*args):
            raise cloudflare_enable.PlatformJobFailure("CF_CONTAINERS_CLEANUP_PENDING", "fixture")

        monkeypatch.setattr(cloudflare_enable, "reconcile_container_stop", pending_stop)
    verify = AsyncMock(return_value=proof if outcome in ("verified", "retire-pending") else None)
    mismatch = outcome in ("mismatch", "mismatch-retire-pending")
    if mismatch:
        from src.services.worker_pool import cloudflare_canary
        from src.worker.remote.host import RemoteControlHost

        worker_id = reserve.return_value.worker_id
        observation = successful_probe(installation_id, 1, release.digest, origin)
        partial = sorted(observation.denied - {"raw-tcp"})
        record = AsyncMock(return_value=True)
        sdk_proof = AsyncMock(return_value="fixture-proof")
        failure = AsyncMock()
        monkeypatch.setattr(cloudflare_canary, "record_container_network_denials", record)
        monkeypatch.setattr(cloudflare_canary, "cloudflare_sdk_canary_proof", sdk_proof)
        monkeypatch.setattr(cloudflare_canary, "record_canary_failure", failure)
        monkeypatch.setattr(cloudflare_canary, "container_probe_target", AsyncMock(
            return_value=("https://scope.example.test", "192.0.2.10"),
        ))
        ready = asyncio.Event()
        ready.set()
        bridge = SimpleNamespace(containers=True, reconciliation_ready=ready, request=AsyncMock(
            return_value={"denied": partial},
        ))
        session = SimpleNamespace(
            bridge=bridge, lease=SimpleNamespace(worker_id=worker_id, session_generation=1),
            reconciliation_tasks=set(),
        )
        RemoteControlHost()._start_capacity_canary(session)
        await asyncio.gather(*session.reconciliation_tasks)
        record.assert_awaited_once_with(worker_id, 1, partial)
        sdk_proof.assert_awaited_with(worker_id, 1, acknowledged=True)
        failure.assert_not_awaited()
        monkeypatch.setattr(
            "src.services.worker_pool.cloudflare_containers_egress._probe_container_egress",
            AsyncMock(return_value=replace(observation, denied=frozenset(record.await_args.args[2]))),
        )
        verify.side_effect = verify_container_egress
    monkeypatch.setattr(cloudflare_enable, "verify_container_egress", verify)
    if outcome == "transient":
        db.execute.return_value = Mock(one_or_none=Mock(return_value=SimpleNamespace(result={}, remote_session_generation=1)))

    if outcome in ("backoff", "qualification-backoff"):
        cloudflare_enable.record_egress_refresh_failure(
            row, datetime.now(UTC), qualification=outcome == "qualification-backoff",
        )
    if outcome in ("verified", "retire-pending"):
        cloudflare_enable.record_egress_refresh_failure(row, datetime.now(UTC) - timedelta(hours=2))
    payload = cloudflare_enable.CloudflareEnablePayload(installation_id=installation_id, revision=1)
    context = PlatformJobContext(uuid4(), uuid4(), None, str(uuid4()), "", "")
    if outcome == "qualification-backoff":
        row.admitted_revision = row.containers_ready_revision = row.containers_release_digest = None
        with pytest.raises(PlatformJobRequiresAction) as pending:
            await cloudflare_enable._qualify_container_revision(context, payload, None, None, TOKEN, release)
        retry = cloudflare_enable.read_egress_refresh(row, qualification=True)
        assert retry is not None
        assert pending.value.result == {
            "code": "CF_CONTAINERS_QUALIFICATION_BACKOFF", "retry_at": retry.retry_at.isoformat(),
            "instructions": [f"Retry this capacity action at or after {retry.retry_at.isoformat()}."],
        }
        reserve.assert_not_awaited()
        return
    with pytest.raises(PlatformJobRequiresAction) if mismatch else nullcontext():
        result = await cloudflare_enable._qualify_container_revision(
            context, payload, None, None, TOKEN, release, refresh_primary=PrimaryPool("static"),
        )
        assert result == {"verified": True, "retire-pending": True, "transient": "failed", "backoff": "backoff"}.get(outcome)
    assert row.admitted_revision == row.containers_ready_revision == (None if mismatch else 1)
    assert row.containers_release_digest == (None if mismatch else release.digest)
    assert ("container_egress" in row.deployment_evidence) == (outcome in ("verified", "retire-pending"))
    refresh = cloudflare_enable.read_egress_refresh(row)
    if outcome in ("transient", "backoff"):
        assert refresh is not None and refresh.failures == 1 and refresh.retry_at > datetime.now(UTC)
    else:
        assert refresh is None
    assert reserve.await_count == int(outcome != "backoff")
    assert verify.await_count == int(outcome not in ("no-spare-seat", "backoff"))


@pytest.mark.asyncio
@pytest.mark.parametrize("blocker", [None, "no-demand", "primary-fit", "last-instance", "last-workflow-seat", "last-shared-seat", "duplicate-tenant"])
async def test_refresh_canary_leaves_user_headroom(monkeypatch, blocker):
    release, _, _ = make_release()
    installation = SimpleNamespace(id=uuid4(), account_id=ACCOUNT)
    seats = SimpleNamespace(
        occupied=Counter({"tenant": 1}) if blocker == "last-workflow-seat" else Counter(),
        policy=SimpleNamespace(installation_slots=2),
    )
    capacity = cloudflare_containers_admission._ContainerCapacity(
        installation, SimpleNamespace(deployment_revision=1),
        policy().model_copy(update={"installation_instances": 3}) if blocker == "duplicate-tenant" else policy(),
        Decimal(100), datetime.now(UTC), [], seats,
        frozenset({"tenant"}) if blocker in ("last-instance", "duplicate-tenant") else frozenset(),
        2 if blocker == "duplicate-tenant" else int(blocker == "last-instance"),
        1 if blocker == "last-shared-seat" else 2,
    )
    db = AsyncMock()
    db.scalar.return_value = 1
    db.execute.return_value = Mock(
        one_or_none=Mock(return_value=(APP_ID, APP_VERSION)),
        tuples=Mock(return_value=[] if blocker == "no-demand" else [(SimpleNamespace(memory_request_bytes=100), object(), object(), object())]),
    )
    monkeypatch.setattr(cloudflare_containers_admission, "_lock_container_capacity", AsyncMock(return_value=capacity))
    monkeypatch.setattr(cloudflare_containers_admission, "_container_queue", Mock())
    monkeypatch.setattr(cloudflare_containers_admission, "eligible_linux_review", Mock(return_value=object()))
    monkeypatch.setattr(cloudflare_containers_admission, "primary_has_no_fit", Mock(return_value=blocker != "primary-fit"))
    reservation = CloudflareWorkerReservation(worker_id=uuid4())
    create = AsyncMock(return_value=reservation)
    monkeypatch.setattr(cloudflare_containers_admission, "_create_container_reservation", create)
    result = await cloudflare_containers_admission.reserve_container_canary(
        db, installation_id=installation.id, controller_id=uuid4(), release=release,
        job_id=uuid4(), lease_token=uuid4(), refresh_primary=PrimaryPool("static"),
    )
    assert result is (reservation if blocker is None else None)
    assert create.await_count == int(blocker is None)


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
            extended_name = {**row, "id": "e" * 32, "name": f"{name}0"}
            return httpx.Response(200, json={"success": True, "result": [row, extended_name]})
        assert request.url.path.endswith(f"/{APP}")
        return httpx.Response(200, json={"success": True, "result": row})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        adapter = ContainersRest(http)
        result = await adapter.observe_application(TOKEN, intent, NAMESPACE)
        assert result is not None and result.provider_id == APP
        assert all(request.method == "GET" for request in requests)
        with pytest.raises(OwnershipConflict):
            await adapter.observe_application(TOKEN, intent, "d" * 32)


@pytest.mark.asyncio
@pytest.mark.parametrize("health", [
    {"instances": {"active": 1, "starting": 0}},
    {"instances": {"active": 0, "starting": 1}},
    {"instances": {"active": 0, "starting": 0}, "summary": "pending"},
    {"instances": {"active": 0}},
])
async def test_container_health_vetoes_deletion(health) -> None:
    installation = uuid4()
    name = f"bifrost-{installation.hex}-r1"
    intent = ResourceIntent(ResourceTarget(installation, ACCOUNT, "containers", "container_application", name), uuid4(), 1, "1" * 64)
    row = {
        "id": APP, "account_id": ACCOUNT, "name": name, "scheduling_policy": "durable_object",
        "durable_objects": {"namespace_id": NAMESPACE}, "health": health,
    }

    def provider(request):
        assert request.method == "GET"
        if request.url.path.endswith("/applications"):
            result = [row]
        else:
            assert request.url.path.endswith(f"/{APP}")
            result = row
        return httpx.Response(200, json={"success": True, "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        with pytest.raises(DeletionSettling, match="activity or an incomplete health sweep"):
            await ContainersRest(http).delete_application(TOKEN, ResourceObservation(intent, APP, NAMESPACE))


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
            await ContainersRest(http).observe_application(TOKEN, intent, NAMESPACE)


@pytest.fixture
def observed_container():
    installation = uuid4()
    name = f"bifrost-{installation.hex}-r1"
    target = ResourceTarget(installation, ACCOUNT, "containers", "container_application", name)
    intent = ResourceIntent(target, uuid4(), 1, "1" * 64)
    row = {
        "id": APP, "account_id": ACCOUNT, "name": name, "scheduling_policy": "durable_object",
        "durable_objects": {"namespace_id": NAMESPACE},
    }
    return ResourceObservation(intent, APP, NAMESPACE), row


@pytest.mark.asyncio
@pytest.mark.parametrize("style", ["cursor", "numbered", "complete", "truncated", "cursor-loop", "numbered-drift"])
async def test_application_absence_requires_complete_inventory(observed_container, style) -> None:
    application, row = observed_container
    first = [{"id": f"{index:032x}", "name": f"{row['name']}-{index}"} for index in range(100)]
    next_page = "next-page"
    pages = []

    def provider(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        if request.url.path.endswith(f"/applications/{APP}"):
            return httpx.Response(200, json={"success": True, "result": row})
        assert request.url.path.endswith("/applications")
        assert request.url.params["per_page"] == "100"
        assert request.url.params["name"] == row["name"]
        pages.append(request.url.params)
        numbered = style.startswith("numbered")
        if len(pages) == 1:
            assert "page_token" not in request.url.params and "page" not in request.url.params
            if numbered:
                info = {"page": 1, "total_pages": 2, "per_page": 100, "count": 100}
            elif style == "truncated":
                info = {"per_page": 100, "count": 100}
            else:
                info = {"per_page": 100, "next_page_token": next_page}
            result = first
        else:
            assert len(pages) == 2
            if numbered:
                assert request.url.params["page"] == "2" and "page_token" not in request.url.params
                info = {"page": 2, "total_pages": 3 if style == "numbered-drift" else 2, "per_page": 100, "count": 1}
            else:
                assert request.url.params["page_token"] == next_page and "page" not in request.url.params
                info = {"per_page": 100, "page_token": next_page}
                if style == "cursor-loop":
                    info["next_page_token"] = next_page
            result = [{**row, "name": f"{row['name']}-other"} if style == "complete" else row]
        return httpx.Response(200, json={"success": True, "result": result, "result_info": info})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        adapter = ContainersRest(http)
        async def observe():
            return await adapter.observe_application(TOKEN, application.intent, application.provider_version)

        if style in {"complete", "cursor", "numbered"}:
            assert await observe() == (None if style == "complete" else application)
        else:
            with pytest.raises(ObservationPending, match="inventory is incomplete"):
                await observe()
    assert len(pages) == (1 if style == "truncated" else 2)


@pytest.mark.asyncio
async def test_exact_size_stopped_history_does_not_block_deletion(observed_container) -> None:
    application, row = observed_container
    row["health"] = {"instances": {"active": 0, "starting": 0}}
    instances = [
        {"id": f"{index:064x}", "application_id": APP, "status": {"state": "stopped"}}
        for index in range(100)
    ]
    present = True
    observed_states = []

    def provider(request):
        nonlocal present
        path = request.url.path
        if path.endswith("/instances-v2"):
            state = request.url.params.get("state")
            observed_states.append(state)
            result = [] if state == "active" else instances
            return httpx.Response(200, json={"success": True, "result": result, "result_info": {"per_page": 100}})
        if request.method == "DELETE":
            assert path.endswith(f"/{APP}") and present
            present = False
            return httpx.Response(204)
        if path.endswith("/applications"):
            result = [row] if present else []
        elif present:
            assert path.endswith(f"/{APP}")
            result = row
        else:
            return httpx.Response(404)
        return httpx.Response(200, json={"success": True, "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        await ContainersRest(http).delete_application(TOKEN, application)
    assert observed_states == ["active"] and not present


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["start", "stop", "egress-probe"])
async def test_container_control_signs_each_route_for_the_incarnation(operation) -> None:
    pin = {"installation": str(uuid4()), "revision": 2, "worker": str(uuid4()), "uid": uuid4().hex}
    scope_secret = "synthetic-private-scope-material-000001"
    control_key = "f" * 64
    path = "/v1/" + operation
    now = 1795442400000

    def provider(request):
        assert request.url.path == path and request.method == "POST"
        assert "authorization" not in request.headers and scope_secret.encode() not in request.content
        headers = request.headers
        key = headers["x-bifrost-incarnation-key"]
        expires = int(headers["x-bifrost-incarnation-expires"])
        nonce = headers["x-bifrost-request-nonce"]
        assert expires == now + 60_000 and len(nonce) == 32
        certificate = json.dumps([sorted(pin.items()), key, expires],
                                 separators=(",", ":")).encode()
        Ed25519PublicKey.from_public_bytes(bytes.fromhex(container_scope_public_key(scope_secret))).verify(
            bytes.fromhex(headers["x-bifrost-incarnation-certificate"]), b"bifrost.containers.incarnation.v1\0" + certificate,
        )
        message = b"bifrost.containers.request.v1\0" + json.dumps(
            [path, now, expires, nonce, sha256(request.content).hexdigest()], separators=(",", ":"),
        ).encode()
        public = Ed25519PublicKey.from_public_bytes(bytes.fromhex(key))
        public.verify(bytes.fromhex(headers["x-bifrost-request-signature"]), message)
        guest_key = Ed25519PrivateKey.from_private_bytes(digest(control_key.encode(), b"bifrost.containers.request.v1\0", "sha256"))
        with pytest.raises(InvalidSignature):
            public.verify(guest_key.sign(message), message)
        assert (control_key.encode() in request.content) == (operation != "stop")
        return httpx.Response(200, json={"status": "pending"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        receipt = await request_container_control(
            http, "https://containers.example.test", scope_secret, control_key,
            operation=operation, pin=pin, now_ms=now, payload={},
        )
    assert json.loads(receipt) == {"status": "pending"}


@pytest.mark.asyncio
async def test_concurrent_placement_reads_share_one_request_pacing_budget(observed_container, monkeypatch) -> None:
    application, row = observed_container
    now = 100.0
    starts = []

    def provider(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        starts.append(now)
        if "/instances/" in request.url.path:
            return httpx.Response(404)
        return httpx.Response(200, json={"success": True, "result": row})

    monkeypatch.setattr(cloudflare_containers_rest, "monotonic", lambda: now)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        adapter = ContainersRest(http)
        results = await gather(
            adapter.observe_no_placement(TOKEN, application, "e" * 64),
            adapter.observe_no_placement(TOKEN, application, "f" * 64),
            return_exceptions=True,
        )
        assert results[0] is None and isinstance(results[1], ObservationSettling)
        assert starts == [100.0, 100.0]
        now += 1
        await adapter.observe_no_placement(TOKEN, application, "f" * 64)
    assert starts == [100.0, 100.0, 101.0, 101.0]
