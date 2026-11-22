"""Release, provider readback and placement checks without Cloudflare calls."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
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
from src.services.worker_pool.cloudflare_containers_policy import (
    container_envelope,
    container_reserve_cost,
)
from src.services.worker_pool.cloudflare_containers_release import (
    ContainersManifest,
    ContainersReleaseUnavailable,
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
from tests.helpers.cloudflare_containers import json_bytes, make_release, policy

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

    async def tool(executable, *args, **kwargs):
        assert executable == Path("cosign")
        if args == ("version",):
            return b"GitVersion:    v2.4.1\n"
        assert args[:3] == ("verify-blob", "--certificate-identity", signer)
        if not trusted:
            raise ContainersReleaseUnavailable("Invalid signature")
        return b""

    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_release.run_oci_tool", tool)
    with pytest.raises(ContainersReleaseUnavailable, match="Invalid signature"):
        await verify_containers_release(tmp_path, workers, cosign=Path("cosign"), signer=signer, workspace=workspace)
    trusted = True
    assert (await verify_containers_release(
        tmp_path, workers, cosign=Path("cosign"), signer=signer, workspace=workspace,
    )).manifest == ContainersManifest.model_validate(data)
    workers.manifest.source_revision = "f" * 40
    with pytest.raises(ContainersReleaseUnavailable, match="source pin"):
        await verify_containers_release(tmp_path, workers, cosign=Path("cosign"), signer=signer, workspace=workspace)


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
