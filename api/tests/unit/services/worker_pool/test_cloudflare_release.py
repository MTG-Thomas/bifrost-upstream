import json
from hashlib import sha256
from pathlib import Path

import pytest
from src.services.worker_pool.cloudflare_release import (
    WorkersReleaseUnavailable,
    load_workers_release,
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
