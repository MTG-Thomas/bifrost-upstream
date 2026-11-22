"""Immutable Linux child image and its matched Workers release."""

import asyncio
import json
import os
import re
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from src.services.worker_pool.cloudflare_release import Digest, WorkersRelease

OCIDigest = Annotated[str, Field(pattern=r"^sha256:[a-f0-9]{64}$")]
ENTRYPOINT = ("/usr/local/bin/python", "-m", "src.worker.remote.container_entrypoint")
_OCI_MEDIA_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}


class ContainersReleaseUnavailable(Exception):
    """The image, signed declaration or matched release cannot be verified."""


class ContainerRates(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal["standard-1-v1"]
    instance_second_usd: Literal["0.00002056"]
    outbound_gb_usd: Literal["0.05"]
    control_usd: Decimal = Field(gt=0, max_digits=20, decimal_places=12)
    cleanup_seconds: int = Field(ge=1, le=3600)


class ContainersManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_version: Literal[1]
    source_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    repository: str = Field(pattern=r"^ghcr\.io/[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+$")
    image_digest: OCIDigest
    platform: Literal["linux/amd64"]
    instance_type: Literal["standard-1"]
    entrypoint: tuple[Literal["/usr/local/bin/python"], Literal["-m"], Literal["src.worker.remote.container_entrypoint"]]
    capacity_protocol: Literal[2]
    sdk_protocol: Literal[1]
    ingress_contract: Literal["worker-ingress-only-v1"]
    workers_release_digest: Digest
    provider_modules: dict[Literal["container-registry.mjs"], Digest] = Field(min_length=1)
    wheels: dict[str, Digest]
    rates: ContainerRates

    @property
    def image(self) -> str:
        return f"{self.repository}@{self.image_digest}"


async def run_oci_tool(
    executable: Path, *args: str, cwd: Path, env: dict[str, str] | None = None, timeout: int = 30,
) -> bytes:
    """Execute a pinned tool under a private home with a bounded lifetime."""
    environment = {"HOME": str(cwd), "PATH": os.defpath, "DOCKER_CONFIG": str(cwd), "NO_COLOR": "1"}
    if env:
        environment.update(env)
    try:
        process = await asyncio.create_subprocess_exec(
            str(executable.resolve(strict=True)), *args, cwd=cwd, env=environment,
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            async with asyncio.timeout(timeout):
                output, _ = await process.communicate()
        except (TimeoutError, asyncio.CancelledError):
            if process.returncode is None:
                process.kill()
            await process.wait()
            raise
    except (OSError, TimeoutError):
        raise ContainersReleaseUnavailable("Container release tool did not complete") from None
    if process.returncode or len(output) > 4 * 1024 * 1024:
        raise ContainersReleaseUnavailable("Container release tool rejected the operation")
    return output


@dataclass(frozen=True, slots=True)
class VerifiedContainerRelease:
    manifest: ContainersManifest
    digest: str

    def verify_image(self, manifest_bytes: bytes, config_bytes: bytes) -> None:
        """Require the exact child manifest and the config referenced by it."""
        try:
            manifest = json.loads(manifest_bytes)
            config = json.loads(config_bytes)
            runtime = config["config"]
            if (
                "sha256:" + sha256(manifest_bytes).hexdigest() != self.manifest.image_digest
                or manifest["schemaVersion"] != 2
                or manifest["mediaType"] not in _OCI_MEDIA_TYPES
                or not manifest["layers"]
                or manifest["config"]["digest"] != "sha256:" + sha256(config_bytes).hexdigest()
                or config["os"] != "linux"
                or config["architecture"] != "amd64"
                or tuple(runtime["Entrypoint"]) != ENTRYPOINT
                or runtime["User"] != "0:0"
                or not isinstance(runtime["Env"], list)
                or [entry for entry in runtime["Env"] if entry.startswith("BIFROST_BAKED_WHEELS_ONLY=")]
                != ["BIFROST_BAKED_WHEELS_ONLY=1"]
                or runtime["Labels"]["org.opencontainers.image.revision"] != self.manifest.source_revision
            ):
                raise ValueError
        except (AttributeError, ValueError, KeyError, TypeError):
            raise ContainersReleaseUnavailable("Container image differs from its signed Linux child") from None


async def verify_containers_release(
    path: Path, workers: WorkersRelease, *, cosign: Path, signer: str, workspace: Path,
) -> VerifiedContainerRelease:
    """Verify trusted CI identity before any registry credential is requested."""
    if not re.fullmatch(r"https://github\.com/[^/]+/[^/]+/\.github/workflows/[^@]+@refs/tags/[^\s]+", signer):
        raise ContainersReleaseUnavailable("Expected an exact trusted release workflow identity")
    version = await run_oci_tool(cosign, "version", cwd=workspace)
    if re.findall(rb"(?m)^GitVersion:[ \t]*(\S+)[ \t]*\r?$", version) != [b"v2.4.1"]:
        raise ContainersReleaseUnavailable("Expected cosign v2.4.1")
    try:
        content = (path / "manifest.json").read_bytes()
        bundle = (path / "manifest.sigstore.json").read_bytes()
        if max(len(content), len(bundle)) > 1024 * 1024:
            raise ValueError
        (workspace / "manifest.json").write_bytes(content)
        (workspace / "manifest.sigstore.json").write_bytes(bundle)
        await run_oci_tool(
            cosign, "verify-blob", "--certificate-identity", signer,
            "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
            "--bundle", "manifest.sigstore.json", "manifest.json", cwd=workspace,
        )
        manifest = ContainersManifest.model_validate_json(content)
        if manifest.source_revision != workers.manifest.source_revision or manifest.workers_release_digest != workers.digest:
            raise ValueError
        for name, digest in manifest.provider_modules.items():
            if sha256((path / name).read_bytes()).hexdigest() != digest:
                raise ValueError
    except (OSError, ValueError, ValidationError):
        raise ContainersReleaseUnavailable("Container release descriptor or source pin is invalid") from None
    return VerifiedContainerRelease(manifest, sha256(content).hexdigest())
