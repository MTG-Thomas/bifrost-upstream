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
from src.services.worker_pool.cloudflare_provisioning import (
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
    WorkersRestProvisioner,
)


@pytest.fixture
def release(tmp_path: Path) -> Path:
    manifest = {
        "manifest_version": 1,
        "source_revision": "a" * 40,
        "runtime_sha256": "b" * 64,
        "pyodide_version": "314.0.6",
        "compatibility_date": "2026-10-30",
        "compatibility_flags": [],
        "capacity_protocol": 2,
        "credential_protocol": 2,
        "runtime_profile": "cloudflare-python-v1",
        "capabilities": [["workflow.execute", 1]],
        "max_slots_per_launcher": 4,
        "workers": {},
    }
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
            "bindings": [],
            "cpu_ms": 30_000,
            "public_endpoint": role == "capacity",
            "preview_urls": False,
        }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return tmp_path


def test_release_loads_the_verified_bytes(release: Path) -> None:
    loaded = load_workers_release(release)
    assert loaded.digest == sha256((release / "manifest.json").read_bytes()).hexdigest()
    assert loaded.modules["launcher"]["launcher.mjs"] == (release / "launcher.mjs").read_bytes()
    assert loaded.modules["capacity"]["capacity.mjs"] == (release / "capacity.mjs").read_bytes()


def test_release_rejects_same_size_module_substitution(release: Path) -> None:
    module = release / "capacity.mjs"
    module.write_bytes(b"X" + module.read_bytes()[1:])
    with pytest.raises(WorkersReleaseUnavailable, match="digest"):
        load_workers_release(release)


def test_release_rejects_a_module_path_outside_the_package(release: Path) -> None:
    path = release / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["workers"]["capacity"]["modules"][0]["name"] = "../capacity.mjs"
    path.write_text(json.dumps(manifest))
    with pytest.raises(WorkersReleaseUnavailable, match="missing or invalid"):
        load_workers_release(release)


async def test_rest_upload_scopes_the_bearer_and_finalizes_the_verified_endpoint(release: Path) -> None:
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
    namespaces = {"ScopeRegistry": "1" * 32, "WorkerLedger": "2" * 32}

    async def read_provider(request: httpx.Request) -> httpx.Response:
        nonlocal bindings, endpoint
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
            result = {}
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
            result = endpoint
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
        configured = await adapter.configure_endpoint(api_token=token, expected=observed)
        assert configured.endpoint == "https://bifrost-capacity.test-account.workers.dev"
        assert configured.namespaces == namespaces
        assert await adapter.configure_endpoint(api_token=token, expected=observed) == configured
    assert mutations == ["PUT", "POST"]
    assert endpoint == {"enabled": True, "previews_enabled": False}
