"""Synthetic Linux image and policy fixtures shared by unit and database tests."""

import json
from hashlib import sha256

from src.services.worker_pool.cloudflare_containers_policy import ContainersPolicy
from src.services.worker_pool.cloudflare_containers_release import (
    ENTRYPOINT,
    ContainersManifest,
    VerifiedContainerRelease,
)


def json_bytes(value) -> bytes:
    return json.dumps(value, sort_keys=True).encode()


def make_release() -> tuple[VerifiedContainerRelease, bytes, bytes]:
    config = json_bytes({
        "os": "linux", "architecture": "amd64",
        "config": {"Entrypoint": list(ENTRYPOINT), "User": "1000:1000", "Labels": {
            "org.opencontainers.image.revision": "1" * 40,
        }},
    })
    image = json_bytes({
        "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [{"digest": "sha256:" + "4" * 64}], "config": {"digest": "sha256:" + sha256(config).hexdigest()},
    })
    manifest = ContainersManifest.model_validate({
        "manifest_version": 1, "source_revision": "1" * 40, "repository": "ghcr.io/example/linux",
        "image_digest": "sha256:" + sha256(image).hexdigest(),
        "platform": "linux/amd64", "instance_type": "standard-1", "entrypoint": ENTRYPOINT,
        "capacity_protocol": 2, "sdk_protocol": 1, "ingress_contract": "worker-ingress-only-v1",
        "workers_release_digest": "2" * 64, "provider_modules": {"container-registry.mjs": "3" * 64},
        "wheels": {"example-1.0-py3-none-any.whl": "5" * 64},
        "rates": {
            "version": "standard-1-v1", "instance_second_usd": "0.00002056", "outbound_gb_usd": "0.05",
            "control_usd": "0.000001", "cleanup_seconds": 60,
        },
    })
    return VerifiedContainerRelease(manifest, "6" * 64), image, config


def policy() -> ContainersPolicy:
    return ContainersPolicy(
        lifetime_seconds=300, queue_wait_seconds=1, queue_depth=1, start_timeout_seconds=30,
        ingress_bytes=100, outbound_bytes=100,
    )
