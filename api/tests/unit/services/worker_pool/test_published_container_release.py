"""Verify the published rc02 bundle with the production Cosign command."""

import base64
import json
from hashlib import sha256
from pathlib import Path
from shutil import copyfile, which

import httpx
import pytest

from src.services.worker_pool import cloudflare_containers_release as containers
from src.services.worker_pool.cloudflare_release import WorkersManifest, WorkersRelease, _deployment_digest

_FIXTURE = Path(__file__).resolve().parents[3] / "fixtures" / "cloudflare-containers" / "rc02"
_TAG = "v2.0.0-rc.2026112402"
_REPOSITORY = "MTG-Thomas/bifrost-upstream"
_SIGNER = f"https://github.com/{_REPOSITORY}/.github/workflows/ci.yml@refs/tags/{_TAG}"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["published", "wrong-identity", "wrong-issuer", "modified-manifest", "tampered-bundle"])
async def test_published_keyless_release(case, tmp_path, monkeypatch) -> None:
    manifest = WorkersManifest.model_validate_json((_FIXTURE / "workers-manifest.json").read_bytes())
    assert manifest.release_version == _TAG[1:]
    workers = WorkersRelease(manifest, _deployment_digest(manifest), {})
    signer_file = _FIXTURE / "linux-release-signer"
    assert signer_file.read_text(encoding="ascii") == _SIGNER + "\n"
    repository = _REPOSITORY
    if case == "wrong-identity":
        repository = "example/another-fork"
        signer_file = tmp_path / "wrong-signer"
        signer_file.write_text(_SIGNER.replace(_REPOSITORY, repository) + "\n", encoding="ascii")
    if case == "wrong-issuer":
        monkeypatch.setattr(containers, "_GITHUB_OIDC_ISSUER", "https://issuer.example.test")

    assets = {name: (_FIXTURE / name).read_bytes() for name in containers._RELEASE_FILES}
    assert json.loads(assets["manifest.sigstore.json"])["mediaType"] == "application/vnd.dev.sigstore.bundle.v0.3+json"
    if case == "modified-manifest":
        altered = json.loads(assets["manifest.json"])
        altered["image_digest"] = "sha256:" + "1" * 64
        assets["manifest.json"] = json.dumps(altered).encode()
    if case == "tampered-bundle":
        altered = json.loads(assets["manifest.sigstore.json"])
        signature = bytearray(base64.b64decode(altered["messageSignature"]["signature"]))
        signature[0] ^= 1
        altered["messageSignature"]["signature"] = base64.b64encode(signature).decode()
        assets["manifest.sigstore.json"] = json.dumps(altered).encode()

    def asset(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "github.com"
        prefix = f"/{repository}/releases/download/{_TAG}/cloudflare-containers-"
        assert request.url.path.startswith(prefix)
        return httpx.Response(200, content=assets[request.url.path.removeprefix(prefix)])

    real_tool = containers.run_oci_tool

    async def tool(executable: Path, *args: str, cwd: Path) -> bytes:
        if args == ("initialize",):
            # Only the TUF network bootstrap is replaced. Signature, claims,
            # transparency log and artifact bytes are checked by real Cosign.
            target = cwd / ".sigstore/root/targets/trusted_root.json"
            target.parent.mkdir(parents=True)
            copyfile(_FIXTURE / "trusted-root.json", target)
            return b""
        return await real_tool(executable, *args, cwd=cwd)

    executable = which("cosign")
    assert executable, "The unit-test image must provide pinned Cosign"
    monkeypatch.setattr(containers, "_COSIGN", Path(executable))
    monkeypatch.setattr(containers, "_SIGNER_FILE", signer_file)
    monkeypatch.setattr(containers, "run_oci_tool", tool)
    async with httpx.AsyncClient(transport=httpx.MockTransport(asset)) as http:
        if case == "published":
            release = await containers.load_containers_release(http, workers)
            assert release.digest == sha256(assets["manifest.json"]).hexdigest()
        else:
            with pytest.raises(containers.ContainersReleaseUnavailable, match="tool rejected"):
                await containers.load_containers_release(http, workers)
