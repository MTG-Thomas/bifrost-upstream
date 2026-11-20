import json
from email import policy as email_policy
from email.parser import BytesParser
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from shared.models import CloudflareWorkersPolicy
from src.services.worker_pool import cloudflare_release
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    ResourceIntent,
    ResourceTarget,
    resume_creation,
)
from src.services.worker_pool.cloudflare_release import (
    WorkersReleaseUnavailable,
    load_workers_release,
)
from src.services.worker_pool.cloudflare_workers_rest import (
    WorkersDeployment,
    WorkersPaidUnverified,
    WorkersRestCleanup,
    WorkersRestProvisioner,
)


@pytest.fixture
def release(tmp_path: Path, monkeypatch) -> Path:
    manifest = {
        "manifest_version": 1,
        "source_revision": "a" * 40,
        "runtime_sha256": "fd03ddf5bd8e87a4b11941678b526455f118eb7913b31a0a0be473521cd7f0d5",
        "pyodide_version": "314.0.6",
        "compatibility_date": "2026-10-30",
        "compatibility_flags": [],
        "capacity_protocol": 2,
        "credential_protocol": 2,
        "runtime_profile": "cloudflare-python-v1",
        "capabilities": [["workflow.execute", 1]],
        "max_slots_per_launcher": 4,
        "meter_bounds": json.loads(Path(cloudflare_release.__file__).with_name("cloudflare_meter_bounds.json").read_bytes()),
        "workers": {},
    }
    raw_bounds = manifest["meter_bounds"]
    for role, exports in (
        ("launcher", ("CloudflareLauncher", "TenantHost")),
        ("capacity", ("ScopeRegistry", "WorkerLedger")),
    ):
        name = f"{role}.mjs"
        content = f"export default '{role}';".encode()
        (tmp_path / name).write_bytes(content)
        manifest["workers"][role] = {
            "main_module": name,
            "modules": [{
                "name": name, "bytes": len(content), "sha256": sha256(content).hexdigest(),
                "gzip_bytes": 64, "mime_type": "application/javascript+module",
            }],
            "exports": {value: {"type": "durable-object", "storage": "sqlite"} for value in exports},
            "bindings": [
                {"name": "BIFROST_METER_SDK_REQUESTS", "type": "plain_text", "text": "40"},
                {"name": "BIFROST_METER_ENGINE_CPU_MS", "type": "plain_text", "text": "30000"},
                {"name": "BIFROST_LAUNCHER_METERS", "type": "plain_text", "text": json.dumps(
                    {name: value * raw_bounds["qualification"]["safety_margin"]
                     for name, value in raw_bounds["qualification"]["launcher_reference"]["observed"].items()},
                    sort_keys=True, separators=(",", ":"),
                )},
            ] if role == "launcher" else [],
            "cpu_ms": 30_000 if role == "launcher" else (
                raw_bounds["qualification"]["capacity_cpu_us"] * raw_bounds["qualification"]["safety_margin"] + 999
            ) // 1000,
            "public_endpoint": role == "capacity",
            "preview_urls": False,
        }
    modules = [manifest["workers"][role]["modules"][0] for role in ("launcher", "capacity")]
    raw_bounds["qualification"]["capacity_module_sha256"] = modules[-1]["sha256"]
    raw_bounds["qualification"]["bundle_sha256"] = sha256("\n".join(
        f'{module["name"]}:{module["sha256"]}' for module in modules
    ).encode()).hexdigest()
    monkeypatch.setattr(cloudflare_release, "QUALIFIED_METER_BOUNDS_SHA256",
                        cloudflare_release.meter_bounds_digest(cloudflare_release.MeterBounds.model_validate(raw_bounds)))
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return tmp_path


def test_release_loads_the_verified_bytes(release: Path) -> None:
    loaded = load_workers_release(release)
    assert loaded.digest == sha256((release / "manifest.json").read_bytes()).hexdigest()
    assert loaded.modules["launcher"]["launcher.mjs"] == (release / "launcher.mjs").read_bytes()
    assert loaded.modules["capacity"]["capacity.mjs"] == (release / "capacity.mjs").read_bytes()


def test_repository_meter_bounds_match_the_api_pin() -> None:
    path = Path(cloudflare_release.__file__).with_name("cloudflare_meter_bounds.json")
    bounds = cloudflare_release.MeterBounds.model_validate_json(path.read_bytes())
    assert cloudflare_release.meter_bounds_digest(bounds) == cloudflare_release.QUALIFIED_METER_BOUNDS_SHA256


def test_release_rejects_same_size_module_substitution(release: Path) -> None:
    module = release / "capacity.mjs"
    module.write_bytes(b"X" + module.read_bytes()[1:])
    with pytest.raises(WorkersReleaseUnavailable, match="digest"):
        load_workers_release(release)


def test_release_rejects_bundle_even_with_updated_module_digest(release: Path) -> None:
    path = release / "manifest.json"
    manifest = json.loads(path.read_bytes())
    module = release / "capacity.mjs"
    content = b"X" + module.read_bytes()[1:]
    module.write_bytes(content)
    manifest["workers"]["capacity"]["modules"][0]["sha256"] = sha256(content).hexdigest()
    path.write_text(json.dumps(manifest))
    with pytest.raises(WorkersReleaseUnavailable, match="qualified fingerprint"):
        load_workers_release(release)


def test_release_rejects_capacity_bytes_without_a_matching_measurement(release: Path, monkeypatch) -> None:
    path = release / "manifest.json"
    manifest = json.loads(path.read_bytes())
    bounds = manifest["meter_bounds"]
    bounds["qualification"]["capacity_module_sha256"] = "0" * 64
    monkeypatch.setattr(cloudflare_release, "QUALIFIED_METER_BOUNDS_SHA256",
                        cloudflare_release.meter_bounds_digest(cloudflare_release.MeterBounds.model_validate(bounds)))
    path.write_text(json.dumps(manifest))
    with pytest.raises(WorkersReleaseUnavailable, match="measured bytes"):
        load_workers_release(release)


def test_release_rejects_manifest_with_an_unqualified_runtime(release: Path) -> None:
    path = release / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["runtime_sha256"] = "0" * 64
    path.write_text(json.dumps(manifest))
    with pytest.raises(WorkersReleaseUnavailable, match="qualified digest"):
        load_workers_release(release)


def test_release_rejects_substituted_meter_bounds(release: Path) -> None:
    path = release / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["meter_bounds"]["per_launcher_second"]["worker_cpu_ms"] = "0"
    path.write_text(json.dumps(manifest))
    with pytest.raises(WorkersReleaseUnavailable, match="pinned digest"):
        load_workers_release(release)


def test_release_rejects_an_uploaded_cpu_limit_higher_than_the_estimate(release: Path) -> None:
    path = release / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["workers"]["capacity"]["cpu_ms"] = 30_000
    path.write_text(json.dumps(manifest))
    with pytest.raises(WorkersReleaseUnavailable, match="resource limits"):
        load_workers_release(release)


def test_release_rejects_a_module_path_outside_the_package(release: Path) -> None:
    path = release / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["workers"]["capacity"]["modules"][0]["name"] = "../capacity.mjs"
    path.write_text(json.dumps(manifest))
    with pytest.raises(WorkersReleaseUnavailable, match="missing or invalid"):
        load_workers_release(release)


async def test_paid_check_identifies_the_missing_entitlement() -> None:
    account = "a" * 32

    def remote(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        if request.url.path.endswith("/entitlements"):
            result = [{"id": "workers.enabled", "deleted_date": "", "allocation": {"type": "bool", "value": True}}]
        else:
            assert request.url.path.endswith(account)
            result = {"id": account}
        return httpx.Response(200, json={"success": True, "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        with pytest.raises(WorkersPaidUnverified, match="active containers.enabled entitlement"):
            await WorkersRestCleanup(http).verify_workers_paid(account, SecretStr("synthetic-account-token"))


@pytest.mark.parametrize("confirmed", [True, False])
async def test_rest_upload_scopes_the_bearer_and_finalizes_the_verified_endpoint(release: Path, confirmed: bool) -> None:
    path = release / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["workers"]["capacity"]["bindings"] = [
        {"name": "CAPACITY_TOKEN", "type": "secret_text", "source": "scope_bearer"},
        {"name": "CLOUDFLARE_LAUNCHER", "type": "durable_object_namespace", "class_name": "CloudflareLauncher", "script": "launcher"},
    ]
    path.write_text(json.dumps(manifest))
    packaged = load_workers_release(release)
    target = ResourceTarget(uuid4(), "a" * 32, "workers", "worker_script", "bifrost-capacity")
    intent = ResourceIntent(target, uuid4(), 1, packaged.digest)
    deployment, version = str(uuid4()), str(uuid4())
    bindings = []
    endpoint = {}
    mutations = []
    endpoint_reads = 0
    propagated = False
    namespaces = {"ScopeRegistry": "1" * 32, "WorkerLedger": "2" * 32}

    async def read_provider(request: httpx.Request) -> httpx.Response:
        nonlocal bindings, endpoint, endpoint_reads
        assert request.headers["Authorization"] == "Bearer synthetic-management"
        if request.method == "PUT":
            mutations.append("PUT")
            content_type = request.headers["Content-Type"].encode()
            message = BytesParser(policy=email_policy.default).parsebytes(
                b"Content-Type: " + content_type + b"\r\n\r\n" + await request.aread(),
            )
            parts = {part.get_param("name", header="content-disposition"): part.get_payload(decode=True)
                     for part in message.iter_parts()}
            assert parts["capacity.mjs"] == packaged.modules["capacity"]["capacity.mjs"]
            metadata = json.loads(parts["metadata"])
            assert "synthetic-management" not in json.dumps(metadata)
            bindings = metadata["bindings"]
            assert {"name": "CAPACITY_TOKEN", "type": "secret_text", "text": "synthetic-bearer"} in bindings
            assert any(binding.get("script_name") == "bifrost-launcher" for binding in bindings)
            result = {"id": target.name}
        elif request.method == "POST":
            mutations.append("POST")
            endpoint = json.loads(await request.aread())
            result = {**endpoint, "previews_enabled": not confirmed}
        elif request.url.path.endswith("/settings"):
            if not bindings:
                return httpx.Response(404)
            result = {"bindings": bindings}
        elif request.url.path.endswith("/deployments"):
            result = {"deployments": [{"id": deployment, "versions": [{"version_id": version, "percentage": 100}]}]}
        elif request.url.path.endswith("/namespaces"):
            result = [{"script": target.name, "class": name, "id": identity, "preview": False}
                      for name, identity in namespaces.items()]
        elif request.url.path.endswith("/workers/subdomain"):
            result = {"subdomain": "test-account"}
        else:
            endpoint_reads += 1
            # The first read after the update may still show the old state.
            result = endpoint if propagated else {"enabled": False, "previews_enabled": False}
        return httpx.Response(200, json={"success": True, "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(read_provider)) as client:
        adapter = WorkersRestProvisioner(client, WorkersDeployment(
            packaged, "https://workers.example.test", "bifrost-launcher", target.name,
            CloudflareWorkersPolicy(
                launcher_cap=1, slots_per_launcher=1, max_lifetime_seconds=60,
                work_kinds=frozenset({"workflow"}),
            ),
        ))
        token = SecretStr("synthetic-management")
        observed = await resume_creation(
            adapter, api_token=token, scope_bearer=SecretStr("synthetic-bearer"), committed_intent=intent,
        )
        assert observed.provider_version == version
        if not confirmed:
            with pytest.raises(ObservationPending, match="did not confirm the requested settings"):
                await adapter.configure_endpoint(api_token=token, expected=observed)
            assert endpoint_reads == 1
            return
        configured = await adapter.configure_endpoint(api_token=token, expected=observed)
        assert configured.endpoint == "https://bifrost-capacity.test-account.workers.dev"
        assert configured.namespaces == namespaces
        assert endpoint_reads == 1
        propagated = True
        assert await adapter.configure_endpoint(api_token=token, expected=observed) == configured
    assert mutations == ["PUT", "POST"]
    assert endpoint == {"enabled": True, "previews_enabled": False}
