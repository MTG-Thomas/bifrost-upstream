"""Local release, registry and policy inputs for the combined Container rehearsal."""

import asyncio
import json
import re
from hashlib import sha256
from ipaddress import IPv4Address
from pathlib import Path
from tempfile import TemporaryDirectory

PROVIDER = json.loads(Path("/rehearsal/containers.json").read_text())
FIXTURE = PROVIDER["containers"]
ACCOUNT = "a" * 32
MANAGEMENT_TOKEN = "local-rehearsal-management-token"
SOURCE = "ghcr.io/rehearsal/containers@" + FIXTURE["digest"]
REPOSITORY = r"bifrost-[0-9a-f]{32}-r[1-9][0-9]*"
BASE = f"http://{PROVIDER['address']}:{PROVIDER['providerPort']}"


async def registry(method, repository, body=None):
    import httpx

    if repository and re.fullmatch(REPOSITORY, repository) is None:
        raise ValueError("Unexpected rehearsal image repository")
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as http:
        response = await http.request(
            method, f"{BASE}/local/containers-registry" + (f"/{repository}" if repository else ""), json=body,
            headers={"Authorization": f"Bearer {MANAGEMENT_TOKEN}"},
        )
    response.raise_for_status()
    return response.json()["result"]


def configure_release(module):
    async def local_tool(executable, *args, cwd, env=None, timeout=30):
        match executable.name, args:
            case "cosign", ("version",):
                return b"GitVersion: v2.4.1\n"
            case "cosign", ("initialize",):
                return b""
            case "cosign", ("verify-blob", "--certificate-identity", signer, "--offline",
                            "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
                            "--bundle", "manifest.sigstore.json", "manifest.json"):
                if re.fullmatch(r"https://github\.com/rehearsal/containers/\.github/workflows/ci\.yml@refs/tags/v[^/\s]+", signer):
                    bundle = json.loads((cwd / "manifest.sigstore.json").read_bytes())
                    if bundle == {"fixture_manifest_sha256": sha256((cwd / "manifest.json").read_bytes()).hexdigest()}:
                        return b""
            case "crane", ("version",):
                return b"0.20.6"
            case "crane", ("copy", source, destination) if source == SOURCE:
                match = re.fullmatch(rf"registry\.cloudflare\.com/{ACCOUNT}/({REPOSITORY}):release", destination)
                if match:
                    await registry("POST", match[1], {"digest": FIXTURE["digest"]})
                    return b""
            case "crane", (operation, reference) if operation in ("manifest", "config", "digest"):
                match = re.fullmatch(rf"registry\.cloudflare\.com/{ACCOUNT}/({REPOSITORY}):release", reference)
                if reference == SOURCE or (match and await registry("GET", match[1]) == FIXTURE["digest"]):
                    return FIXTURE[operation].encode()
            case "cf", ("--version",):
                return b"1.0.0-beta.31"
            case "cf", ("accounts", "get", "-q"):
                if env == {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_API_TOKEN": MANAGEMENT_TOKEN}:
                    return json.dumps({"id": ACCOUNT}).encode()
            case "cf", ("containers", "images", "list", "--filter", exact):
                prefix = f"^(?:{ACCOUNT}/)?"
                name = exact.removeprefix(prefix).removesuffix("$")
                if exact == prefix + name + "$" and env and env["CLOUDFLARE_ACCOUNT_ID"] == ACCOUNT:
                    present = await registry("GET", name)
                    return json.dumps([{"name": name, "tags": ["release"]}] if present else []).encode()
            case "cf", ("containers", "images", "list"):
                if env == {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_API_TOKEN": MANAGEMENT_TOKEN}:
                    return json.dumps([{"name": row["repository"], "tags": ["release"]}
                                       for row in await registry("GET", "")]).encode()
            case "cf", ("containers", "images", "delete", tag, "--force") if tag.endswith(":release"):
                if env == {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_API_TOKEN": MANAGEMENT_TOKEN}:
                    await registry("DELETE", tag.removesuffix(":release"))
                    return b""
        raise ValueError("Unexpected local OCI fixture operation")

    async def local_release(_http, workers):
        if json.loads(FIXTURE["config"])["config"]["Labels"]["org.opencontainers.image.revision"] != workers.manifest.source_revision:
            raise ValueError("Local Docker image must match the bundled Workers release")
        manifest = {
            "manifest_version": 1, "source_revision": workers.manifest.source_revision,
            "repository": "ghcr.io/rehearsal/containers", "image_digest": FIXTURE["digest"],
            "platform": "linux/amd64", "instance_type": "standard-1", "entrypoint": list(module.ENTRYPOINT),
            "capacity_protocol": 2, "sdk_protocol": 1, "ingress_contract": "worker-ingress-only-v1",
            "workers_release_digest": workers.digest, "wheels": {},
            "rates": {
                "version": "standard-1-v1", "instance_second_usd": "0.00002056", "outbound_gb_usd": "0.05",
                "control_usd": "0.000001", "cleanup_seconds": 60,
            },
        }
        bundle = Path("/rehearsal-container-bundle")
        # Equivalent byte-for-byte concatenation to the release recipe. The
        # cosign fixture proves call shape and digest binding, never hosted trust.
        launcher = (bundle / "container-registry.mjs").read_bytes() + (bundle / "container-scope.mjs").read_bytes()
        manifest["provider_modules"] = {"container-registry.mjs": sha256(launcher).hexdigest()}
        raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        with TemporaryDirectory(prefix="rehearsal-oci-") as package, TemporaryDirectory() as workspace:
            path = Path(package)
            (path / "manifest.json").write_bytes(raw)
            (path / "manifest.sigstore.json").write_text(json.dumps({"fixture_manifest_sha256": sha256(raw).hexdigest()}))
            (path / "container-registry.mjs").write_bytes(launcher)
            return await module.verify_containers_release(
                path, workers, cosign=Path("/fixture/cosign"), workspace=Path(workspace),
                signer=("https://github.com/rehearsal/containers/.github/workflows/ci.yml@refs/tags/v"
                        + workers.manifest.release_version.removeprefix("v")),
            )

    module.run_oci_tool = local_tool
    module.load_containers_release = local_release


def configure_challenge(module):
    class LocalAsyncio:
        def __getattr__(self, name):
            return getattr(asyncio, name)

        async def open_connection(self, host, port):
            if port not in (2053, 8443) or (
                port == 2053 and re.fullmatch(rf"{REPOSITORY}-containers\.rehearsal\.workers\.dev", host) is None
            ):
                raise ValueError("Unexpected alternate Container challenge destination")
            reader, writer = await asyncio.open_connection("container-probe-peer", port)
            address = writer.get_extra_info("peername")[0]
            assert IPv4Address(address).is_private and address != PROVIDER["address"]
            if port == 8443:
                assert address == host
            return reader, writer

    module.asyncio = LocalAsyncio()


HOOKS = {
    "src.services.worker_pool.cloudflare_containers_release": configure_release,
    "src.services.worker_pool.cloudflare_canary": configure_challenge,
}
