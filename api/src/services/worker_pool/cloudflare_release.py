"""Verify the immutable Workers release carried in the API image."""

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

WORKERS_RELEASE_PATH = Path("/app/artifacts/cloudflare/workers")
MAX_WORKER_BYTES = 64 * 1024 * 1024
Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
WorkerRole = Literal["launcher", "capacity"]


class WorkersReleaseUnavailable(Exception):
    """The image has no complete, compatible Workers release."""


class WorkerModule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[a-z][a-z0-9-]*\.mjs$")
    sha256: Digest
    bytes: int = Field(gt=0, le=MAX_WORKER_BYTES)
    gzip_bytes: int = Field(gt=0)
    mime_type: Literal["application/javascript+module"]


class WorkerExport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["durable-object"]
    storage: Literal["sqlite"]


class WorkerBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[A-Z][A-Z0-9_]*$")
    type: Literal["worker_loader", "durable_object_namespace", "secret_text", "plain_text"]
    source: Literal[
        "worker_ingress_origin", "scope_bearer", "launcher_cap", "max_lifetime_seconds",
        "slots_per_launcher", "worker_version",
    ] | None = None
    class_name: str | None = None
    script: Literal["launcher"] | None = None
    text: str | None = None


class WorkerArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    main_module: str
    modules: tuple[WorkerModule, ...] = Field(min_length=1)
    exports: dict[str, WorkerExport]
    bindings: tuple[WorkerBinding, ...]
    cpu_ms: int = Field(gt=0, le=300_000)
    public_endpoint: bool
    preview_urls: Literal[False]


class WorkersManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    manifest_version: Literal[1]
    source_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    runtime_sha256: Digest
    pyodide_version: Literal["314.0.6"]
    compatibility_date: date
    compatibility_flags: tuple[str, ...]
    capacity_protocol: Literal[2]
    credential_protocol: Literal[2]
    runtime_profile: Literal["cloudflare-python-v1"]
    capabilities: tuple[tuple[Literal["workflow.execute"], Literal[1]], ...] = Field(min_length=1, max_length=1)
    max_slots_per_launcher: int = Field(ge=1, le=4)
    workers: dict[WorkerRole, WorkerArtifact]


@dataclass(frozen=True, slots=True)
class WorkersRelease:
    manifest: WorkersManifest
    digest: str
    modules: dict[WorkerRole, dict[str, bytes]]


def load_workers_release(path: Path = WORKERS_RELEASE_PATH) -> WorkersRelease:
    """Read all module bytes once and retain the verified snapshot for upload."""
    try:
        raw = (path / "manifest.json").read_bytes()
        manifest = WorkersManifest.model_validate_json(raw)
        if set(manifest.workers) != {"launcher", "capacity"}:
            raise WorkersReleaseUnavailable("Workers release must contain the launcher and capacity roles")
        modules: dict[WorkerRole, dict[str, bytes]] = {}
        seen: set[str] = set()
        for role, worker in manifest.workers.items():
            expected_exports = {"CloudflareLauncher", "TenantHost"} if role == "launcher" else {"ScopeRegistry", "WorkerLedger"}
            if set(worker.exports) != expected_exports or worker.public_endpoint != (role == "capacity"):
                raise WorkersReleaseUnavailable("Workers exports or endpoint visibility do not match the managed release")
            if sum(module.bytes for module in worker.modules) > MAX_WORKER_BYTES:
                raise WorkersReleaseUnavailable("Workers upload exceeds the size limit")
            payload: dict[str, bytes] = {}
            for module in worker.modules:
                module_path = path / module.name
                if module.name in seen or module_path.is_symlink() or module_path.stat().st_size != module.bytes:
                    raise WorkersReleaseUnavailable("Workers module size or identity could not be verified")
                content = module_path.read_bytes()
                if len(content) != module.bytes or sha256(content).hexdigest() != module.sha256:
                    raise WorkersReleaseUnavailable("Workers module digest could not be verified")
                seen.add(module.name)
                payload[module.name] = content
            if worker.main_module not in payload:
                raise WorkersReleaseUnavailable("Workers entry module is missing")
            modules[role] = payload
    except (OSError, ValidationError):
        raise WorkersReleaseUnavailable("Workers release is missing or invalid") from None
    return WorkersRelease(manifest=manifest, digest=sha256(raw).hexdigest(), modules=modules)
