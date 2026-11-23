"""Synthetic Linux image and policy fixtures shared by unit and database tests."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareOwnedResource,
)
from src.services.worker_pool.cloudflare_containers_egress import (
    ContainerEgressObservation,
    ProbeReport,
    VerifiedEgress,
    verify_container_egress,
)
from src.services.worker_pool.cloudflare_containers_policy import ContainersPolicy
from src.services.worker_pool.cloudflare_containers_release import (
    ENTRYPOINT,
    ContainersManifest,
    VerifiedContainerRelease,
)
from src.worker.remote.container_egress import CONTAINER_EGRESS_DENIALS

APP_ID = "c" * 32
APP_VERSION = "d" * 32


def json_bytes(value) -> bytes:
    return json.dumps(value, sort_keys=True).encode()


def make_release(
    *, image_user: str = "0:0", options: tuple[str, ...] = ("BIFROST_BAKED_WHEELS_ONLY=1",),
) -> tuple[VerifiedContainerRelease, bytes, bytes]:
    config = json_bytes({
        "os": "linux", "architecture": "amd64",
        "config": {"Entrypoint": list(ENTRYPOINT), "User": image_user, "Env": options, "Labels": {
            "org.opencontainers.image.revision": "1" * 40,
        }},
    })
    image = json_bytes({
        "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [{"digest": "sha256:" + "4" * 64}], "config": {"digest": "sha256:" + sha256(config).hexdigest()},
    })
    launcher = b"export class ContainerLauncher {}"
    manifest = ContainersManifest.model_validate({
        "manifest_version": 1, "source_revision": "1" * 40, "repository": "ghcr.io/example/linux",
        "image_digest": "sha256:" + sha256(image).hexdigest(),
        "platform": "linux/amd64", "instance_type": "standard-1", "entrypoint": ENTRYPOINT,
        "capacity_protocol": 2, "sdk_protocol": 1, "ingress_contract": "worker-ingress-only-v1",
        "workers_release_digest": "2" * 64, "provider_modules": {
            "container-registry.mjs": sha256(launcher).hexdigest(),
        },
        "wheels": {"example-1.0-py3-none-any.whl": "5" * 64},
        "rates": {
            "version": "standard-1-v1", "instance_second_usd": "0.00002056", "outbound_gb_usd": "0.05",
            "control_usd": "0.000001", "cleanup_seconds": 60,
        },
    })
    return VerifiedContainerRelease(manifest, "6" * 64, launcher), image, config


def policy() -> ContainersPolicy:
    return ContainersPolicy(
        lifetime_seconds=300, queue_wait_seconds=1, queue_depth=1, start_timeout_seconds=30,
        ingress_bytes=100, outbound_bytes=100,
    )


def app_journal(installation: CloudflareInstallation, digest: str, now: datetime) -> CloudflareOwnedResource:
    return CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id, scope="containers",
        kind="container_application", target_name=f"bifrost-{installation.id.hex}-r1",
        desired_revision=1, artifact_digest=digest, effect_id=uuid4(),
        provider_id=APP_ID, provider_version=APP_VERSION, state="present", observed_at=now,
    )


def successful_probe(
    installation_id: UUID, revision: int, digest: str, origin: str, *, observed_at: datetime | None = None,
) -> ContainerEgressObservation:
    return ContainerEgressObservation(
        installation_id, revision, digest, origin, APP_ID, APP_VERSION, observed_at or datetime.now(UTC),
        ProbeReport(
            nonce="fixture", worker_id=uuid4(), application_id=APP_ID, provider_version=APP_VERSION,
            origin=origin, revision=revision, release_digest=digest,
        ),
        True, CONTAINER_EGRESS_DENIALS,
    )


async def verified_egress(
    installation_id: UUID, revision: int, digest: str, origin: str, *, observed_at: datetime | None = None,
) -> VerifiedEgress:
    observation = successful_probe(installation_id, revision, digest, origin, observed_at=observed_at)
    with patch(
        "src.services.worker_pool.cloudflare_containers_egress._probe_container_egress",
        new=AsyncMock(return_value=observation),
    ):
        proof = await verify_container_egress(
            None, None, installation_id=installation_id, revision=revision,
            release_digest=digest, origin=origin, probe_worker_id=observation.report.worker_id,
        )
    assert proof is not None
    return proof
