"""Local release, registry and policy inputs for the combined Container rehearsal."""

import asyncio
import json
import re
import sys
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

PROVIDER = json.loads(Path("/rehearsal/containers.json").read_text())
FIXTURE = PROVIDER["containers"]
ACCOUNT = "a" * 32
MANAGEMENT_TOKEN = "local-rehearsal-management-token"
SOURCE = "ghcr.io/rehearsal/containers@" + FIXTURE["digest"]
REPOSITORY = r"bifrost-[0-9a-f]{32}-r[1-9][0-9]*"
BASE = f"http://{PROVIDER['address']}:{PROVIDER['providerPort']}"


async def registry(method, repository, body=None):
    import httpx

    if re.fullmatch(REPOSITORY, repository) is None:
        raise ValueError("Unexpected rehearsal image repository")
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as http:
        response = await http.request(
            method, f"{BASE}/local/containers-registry/{repository}", json=body,
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
                return b"v0.20.6"
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


def configure_activation(module):
    from src.models.orm.cloudflare_capacity import CloudflareInstallation
    from src.services.worker_pool.cloudflare_containers_policy import ContainersPolicy

    create = module.create_cloudflare_installation

    async def create_opted_in(db, **payload):
        assert payload["account_id"] == ACCOUNT and payload["verified_api_token"].get_secret_value() == MANAGEMENT_TOKEN
        installation_id = await create(db, **payload)
        installation = await db.get(CloudflareInstallation, installation_id)
        assert installation is not None
        # The public opt-in contract is added later. This private fixture
        # supplies the choice atomically before Activate enqueues Enable.
        installation.containers_policy = ContainersPolicy(
            enabled=True, installation_instances=2, idle_seconds=120,
            start_timeout_seconds=20, lifetime_seconds=300, queue_wait_seconds=1, queue_depth=1,
            ingress_bytes=250_000, outbound_bytes=250_000,
        ).model_dump(mode="json")
        await db.flush()
        return installation_id

    module.create_cloudflare_installation = create_opted_in


def configure_challenge(module):
    class LocalAsyncio:
        def __getattr__(self, name):
            return getattr(asyncio, name)

        async def open_connection(self, host, port):
            if port != 443 or re.fullmatch(rf"{REPOSITORY}-containers\.rehearsal\.workers\.dev", host) is None:
                raise ValueError("Unexpected alternate Container challenge destination")
            return await asyncio.open_connection(PROVIDER["address"], port)

    module.asyncio = LocalAsyncio()


HOOKS = {
    "src.services.worker_pool.cloudflare_containers_release": configure_release,
    "src.services.worker_pool.cloudflare_activation": configure_activation,
    "src.services.worker_pool.cloudflare_canary": configure_challenge,
}


async def approve_workflow(installation_id, workflow_id):
    """Pin a fixture workflow through the private review model for this revision."""
    from sqlalchemy import select

    from src.core.database import get_db_context
    from src.models.orm.cloudflare_approvals import CloudflareApproval
    from src.models.orm.cloudflare_capacity import CloudflareInstallation
    from src.models.orm.file_index import FileIndex
    from src.models.orm.workflows import Workflow
    from src.services.worker_pool.cloudflare_containers_admission import (
        LINUX_REVIEW_VERSION,
        LinuxReview,
        linux_review_digest,
    )

    async with get_db_context() as db:
        installation = await db.get(CloudflareInstallation, installation_id)
        workflow = await db.get(Workflow, workflow_id)
        assert installation is not None and workflow is not None and workflow.solution_id is None
        assert installation.account_id == ACCOUNT and installation.containers_release_digest
        assert installation.admitted_revision == installation.desired_revision
        digest = await db.scalar(select(FileIndex.content_hash).where(FileIndex.path == workflow.path))
        assert digest and workflow.path == "workflows/qualification_container.py"
        review = LinuxReview(
            findings=frozenset({"CF_NATIVE", "CF_PROCESS"}), release_digest=installation.containers_release_digest,
            installation_id=installation_id, deployment_revision=installation.desired_revision,
            consent_version=installation.consent_version, worker_ingress_origin=installation.worker_ingress_origin,
            egress="worker-ingress-only-v1", path=workflow.path, function_name=workflow.function_name,
            source_pins={workflow.path: digest}, wheels={}, memory_bytes=128 * 2**20, timeout_seconds=30,
            ingress_bytes=250_000, outbound_bytes=250_000, previous_revision=None, inactive_history_count=0,
        ).model_dump(mode="json")
        db.add(CloudflareApproval(
            revision=linux_review_digest(workflow_id, workflow.organization_id, review), workflow_id=workflow_id,
            organization_id=workflow.organization_id, policy_version=LINUX_REVIEW_VERSION,
            declaration=review, is_current=True, accepted_by="local-rehearsal",
        ))
        await db.commit()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Expected the installation and fixture workflow IDs")
    asyncio.run(approve_workflow(*map(UUID, sys.argv[1:])))
