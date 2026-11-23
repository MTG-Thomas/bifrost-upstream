"""Immutable Linux child image and its matched Workers release."""

import asyncio
import json
import os
import re
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Literal

import httpx

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from src.services.worker_pool.cloudflare_release import Digest, WorkersRelease

OCIDigest = Annotated[str, Field(pattern=r"^sha256:[a-f0-9]{64}$")]
ENTRYPOINT = ("/usr/local/bin/python", "-m", "src.worker.remote.container_entrypoint")
_OCI_MEDIA_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}
_SIGNER_FILE = Path("/app/artifacts/cloudflare/linux-release-signer")
_RELEASE_SIGNER = re.compile(
    r"https://github\.com/([A-Za-z0-9-]+/[A-Za-z0-9_.-]+)/\.github/workflows/ci\.yml@refs/tags/([^/\s]+)"
)
_RELEASE_FILES = {
    "manifest.json": 1024 * 1024,
    "manifest.sigstore.json": 1024 * 1024,
    "container-registry.mjs": 4 * 1024 * 1024,
}
_ASSET_ORIGINS = frozenset({"github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com"})
_COSIGN = Path("/app/artifacts/cloudflare/bin/cosign")


class ContainersReleaseUnavailable(Exception):
    """The image, signed declaration or matched release cannot be verified."""


class ContainersReleaseFetchError(Exception):
    """The release assets or Sigstore trust metadata could not be fetched."""


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
    environment = {
        "HOME": str(cwd), "PATH": f"/usr/local/bin{os.pathsep}{os.defpath}",
        "DOCKER_CONFIG": str(cwd), "NO_COLOR": "1",
    }
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
    launcher_module: bytes

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
    if _RELEASE_SIGNER.fullmatch(signer) is None:
        raise ContainersReleaseUnavailable("Expected an exact trusted release workflow identity")
    version = await run_oci_tool(cosign, "version", cwd=workspace)
    if re.findall(rb"(?m)^GitVersion:[ \t]*(\S+)[ \t]*\r?$", version) != [b"v2.4.1"]:
        raise ContainersReleaseUnavailable("Expected cosign v2.4.1")
    try:
        content = (path / "manifest.json").read_bytes()
        bundle = (path / "manifest.sigstore.json").read_bytes()
        if max(len(content), len(bundle)) > 1024 * 1024:
            raise ValueError
        manifest = ContainersManifest.model_validate_json(content)
        if manifest.source_revision != workers.manifest.source_revision or manifest.workers_release_digest != workers.digest:
            raise ValueError
        launcher_module = (path / "container-registry.mjs").read_bytes()
        if sha256(launcher_module).hexdigest() != manifest.provider_modules["container-registry.mjs"]:
            raise ValueError
        (workspace / "manifest.json").write_bytes(content)
        (workspace / "manifest.sigstore.json").write_bytes(bundle)
        # Populate a private, trust-checked TUF cache first. A failed fetch
        # must be distinguishable from a rejected release signature.
        try:
            await run_oci_tool(cosign, "initialize", cwd=workspace)
        except ContainersReleaseUnavailable:
            raise ContainersReleaseFetchError("Sigstore trust metadata is unavailable") from None
        await run_oci_tool(
            cosign, "verify-blob", "--certificate-identity", signer, "--offline",
            "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
            "--bundle", "manifest.sigstore.json", "manifest.json", cwd=workspace,
        )
    except (OSError, ValueError, ValidationError):
        raise ContainersReleaseUnavailable("Container release descriptor or source pin is invalid") from None
    return VerifiedContainerRelease(manifest, sha256(content).hexdigest(), launcher_module)


async def _download_release_asset(
    http: httpx.AsyncClient, url: httpx.URL, target: Path, limit: int,
) -> None:
    for _ in range(3):
        if (
            url.scheme != "https" or url.host not in _ASSET_ORIGINS or url.port is not None
            or url.username or url.password
        ):
            raise ContainersReleaseUnavailable("Unexpected Linux release asset location")
        async with http.stream("GET", url, follow_redirects=False) as response:
            if response.is_redirect:
                url = url.join(response.headers["location"])
                continue
            response.raise_for_status()
            length = 0
            with target.open("xb") as output:
                async for chunk in response.aiter_bytes():
                    length += len(chunk)
                    if length > limit:
                        raise ContainersReleaseUnavailable("Linux release asset exceeds its size limit")
                    output.write(chunk)
            return
    raise ContainersReleaseUnavailable("Unexpected Linux release redirect chain")


async def load_containers_release(http: httpx.AsyncClient, workers: WorkersRelease) -> VerifiedContainerRelease:
    """Install the signed assets published with this exact API image's Workers release."""
    tag = f"v{workers.manifest.release_version}"
    try:
        signer = _SIGNER_FILE.read_text(encoding="ascii").removesuffix("\n")
    except (OSError, UnicodeError):
        raise ContainersReleaseUnavailable("The API image has no trusted Linux release signer") from None
    binding = _RELEASE_SIGNER.fullmatch(signer)
    if binding is None or binding[2] != tag or any(part in (".", "..") for part in binding[1].split("/")):
        raise ContainersReleaseUnavailable("The API image Linux release signer does not match its release")
    release_root = f"https://github.com/{binding[1]}/releases/download/{tag}"
    with TemporaryDirectory(prefix="bifrost-linux-release-") as temporary:
        root = Path(temporary)
        source = root / "assets"
        workspace = root / "verify"
        source.mkdir()
        workspace.mkdir()
        try:
            for name, limit in _RELEASE_FILES.items():
                await _download_release_asset(
                    http, httpx.URL(f"{release_root}/cloudflare-containers-{name}"), source / name, limit,
                )
            return await verify_containers_release(
                source, workers, cosign=_COSIGN,
                signer=signer, workspace=workspace,
            )
        except httpx.RequestError:
            raise ContainersReleaseFetchError("The matched Cloudflare Linux release could not be fetched") from None
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise ContainersReleaseFetchError("The matched Cloudflare Linux release has not been published") from None
            if exc.response.status_code in (408, 429) or exc.response.status_code >= 500:
                raise ContainersReleaseFetchError("The matched Cloudflare Linux release could not be fetched") from None
            raise ContainersReleaseUnavailable("The matched Cloudflare Linux release is unavailable") from None
        except (OSError, KeyError, ValueError):
            raise ContainersReleaseUnavailable("The matched Cloudflare Linux release is unavailable") from None
