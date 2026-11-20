"""Verify the immutable Workers release carried in the API image."""

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from shared.models import CloudflareLauncherUsage

WORKERS_RELEASE_PATH = Path("/app/artifacts/cloudflare/workers")
MAX_WORKER_BYTES = 64 * 1024 * 1024
QUALIFIED_RUNTIME_SHA256 = "fd03ddf5bd8e87a4b11941678b526455f118eb7913b31a0a0be473521cd7f0d5"
QUALIFIED_METER_BOUNDS_SHA256 = "ec57811a7e51bfa202f7af644b26f4550c40b7e88394ae7f723e37cbfdaf1aa6"
LAUNCHER_CPU_CEILING_MS = 30_000
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


class LauncherReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    observed: CloudflareLauncherUsage


class MeterQualification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_run: UUID
    observed_manifest_sha256: Digest
    capacity_module_sha256: Digest
    bundle_sha256: Digest
    capacity_cpu_us: int = Field(gt=0)
    sdk_requests: int = Field(gt=0)
    safety_margin: int = Field(gt=1)
    launcher_reference: LauncherReference

    @property
    def capacity_cpu_ms(self) -> int:
        return (self.capacity_cpu_us * self.safety_margin + 999) // 1000

    @property
    def sdk_request_limit(self) -> int:
        return min(50, self.sdk_requests * self.safety_margin)

    @property
    def launcher_caps(self) -> dict[str, int]:
        return {name: count * self.safety_margin for name, count in self.launcher_reference.observed.model_dump().items()}


class MeterBounds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    qualification: MeterQualification
    cleanup_margin_seconds: int = Field(gt=0, le=3600)
    per_launcher_second: dict[str, Decimal]
    per_fleet_fixed: dict[str, Decimal]


def meter_bounds_digest(bounds: MeterBounds) -> str:
    return sha256(json.dumps(bounds.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


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
    meter_bounds: MeterBounds
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
        if manifest.runtime_sha256 != QUALIFIED_RUNTIME_SHA256:
            raise WorkersReleaseUnavailable("Workers runtime does not match the qualified digest")
        if meter_bounds_digest(manifest.meter_bounds) != QUALIFIED_METER_BOUNDS_SHA256:
            raise WorkersReleaseUnavailable("Workers meter bounds do not match the pinned digest")
        if set(manifest.workers) != {"launcher", "capacity"}:
            raise WorkersReleaseUnavailable("Workers release must contain the launcher and capacity roles")
        qualifier = manifest.meter_bounds.qualification
        launcher = manifest.workers["launcher"]
        resource_limits = {binding.name: binding.text for binding in launcher.bindings if binding.type == "plain_text"}
        serialized_caps = json.dumps(qualifier.launcher_caps, sort_keys=True, separators=(",", ":"))
        if (
            manifest.workers["capacity"].cpu_ms != qualifier.capacity_cpu_ms
            or launcher.cpu_ms != LAUNCHER_CPU_CEILING_MS
            or resource_limits.get("BIFROST_METER_SDK_REQUESTS") != str(qualifier.sdk_request_limit)
            or resource_limits.get("BIFROST_METER_ENGINE_CPU_MS") != str(LAUNCHER_CPU_CEILING_MS)
            or resource_limits.get("BIFROST_LAUNCHER_METERS") != serialized_caps
        ):
            raise WorkersReleaseUnavailable("Workers resource limits do not match the measured release")
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
        fingerprint = sha256("\n".join(
            f"{name}:{sha256(payload).hexdigest()}"
            for role in ("launcher", "capacity") for name, payload in modules[role].items()
        ).encode()).hexdigest()
        if fingerprint != qualifier.bundle_sha256:
            raise WorkersReleaseUnavailable("Workers bundle differs from the qualified fingerprint")
        capacity = manifest.workers["capacity"]
        if sha256(modules["capacity"][capacity.main_module]).hexdigest() != qualifier.capacity_module_sha256:
            raise WorkersReleaseUnavailable("Workers capacity module differs from the measured bytes")
    except (OSError, ValidationError):
        raise WorkersReleaseUnavailable("Workers release is missing or invalid") from None
    return WorkersRelease(manifest=manifest, digest=sha256(raw).hexdigest(), modules=modules)
