"""Verify the network boundary before admitting a managed Linux deployment."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

from src.services.worker_pool.cloudflare_tunnel_rest import dedicated_hostname

_MINT = object()
REQUIRED_DENIALS = frozenset({"alternate-origin", "wrong-host", "raw-tcp", "disallowed-route", "redirect"})


@dataclass(frozen=True, slots=True)
class ContainerEgressObservation:
    """Observations from the trusted qualification probe, outside the guest."""

    installation_id: UUID
    revision: int
    release_digest: str
    origin: str
    internet_enabled: bool
    intercepted_origins: frozenset[str]
    signed_control_round_trip: bool
    bounded_relay: bool
    denied: frozenset[str]


@dataclass(frozen=True, slots=True, init=False)
class VerifiedEgress:
    installation_id: UUID
    revision: int
    release_digest: str
    origin: str

    def __init__(self, observation: ContainerEgressObservation, *, _mint: object) -> None:
        if _mint is not _MINT:
            raise TypeError("Container egress must be verified by the network probe")
        object.__setattr__(self, "installation_id", observation.installation_id)
        object.__setattr__(self, "revision", observation.revision)
        object.__setattr__(self, "release_digest", observation.release_digest)
        object.__setattr__(self, "origin", observation.origin)


async def verify_container_egress(
    probe: Callable[[], Awaitable[ContainerEgressObservation]], *,
    installation_id: UUID, revision: int, release_digest: str, origin: str,
) -> VerifiedEgress | None:
    """Require matching deployed policy and an independently observed control round trip."""
    try:
        if origin != f"https://{dedicated_hostname(origin)}":
            return None
    except ValueError:
        return None
    observation = await probe()
    if (
        observation.installation_id != installation_id
        or observation.revision != revision
        or observation.release_digest != release_digest
        or observation.origin != origin
        or observation.internet_enabled
        or observation.intercepted_origins != {origin}
        or not observation.signed_control_round_trip
        or not observation.bounded_relay
        or not REQUIRED_DENIALS <= observation.denied
    ):
        return None
    return VerifiedEgress(observation, _mint=_MINT)
