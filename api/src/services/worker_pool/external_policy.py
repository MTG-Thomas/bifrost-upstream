"""The same provider limits apply at policy save and physical admission."""

from typing import TYPE_CHECKING

from shared.models import WorkerCapacityProfile
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    LINUX_RUNTIME_PROFILE,
    cloudflare_slots,
)

if TYPE_CHECKING:
    from src.services.kubernetes_execution import KubernetesPolicy


def validate_external_policy(
    policy: "KubernetesPolicy", profile: WorkerCapacityProfile, *, max_lifetime_seconds: int,
    peer: WorkerCapacityProfile | None = None,
    primary_only: bool = False,
) -> None:
    """Reject an incompatible policy without changing it or provisioning work."""
    if profile.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
        cloudflare_slots(profile.memory_limit_bytes)
    else:
        for field in ("worker_memory_mib", "largest_worker_mib"):
            if getattr(policy, field) * 2**20 != profile.memory_limit_bytes:
                raise ValueError(
                    f"{field} must match the capacity provider's fixed worker memory "
                    f"({profile.memory_limit_bytes // 2**20} MiB)"
                )
    limit = profile.max_workers + (peer.max_workers if peer else 0)
    if not primary_only and policy.max_workers > limit:
        raise ValueError(f"max_workers must not exceed the capacity provider limit ({limit})")
    if (
        (peer is not None or primary_only)
        and profile.runtime_profile == LINUX_RUNTIME_PROFILE and policy.warm_workers > profile.max_workers
    ):
        raise ValueError("warm_workers must fit within the primary Linux provider ceiling")
    if max_lifetime_seconds > profile.max_lifetime_seconds:
        raise ValueError(
            "BIFROST_POOL_MAX_LIFETIME_SECONDS exceeds the capacity provider limit "
            f"({profile.max_lifetime_seconds}); update the deployment configuration"
        )
    if policy.reserved_fast_path_workers:
        raise ValueError(
            "reserved_fast_path_workers requires managed Kubernetes or static Fast Path workers; "
            "the external capacity provider does not advertise lane isolation"
        )
