from uuid import UUID

from shared.worker_control_auth import managed_cloudflare_control_key


def test_managed_capacity_key_wire_vector() -> None:
    actual = managed_cloudflare_control_key(
        "fixed-root", "workers", UUID("00000000-0000-0000-0000-000000000001"), "a" * 32,
    )
    assert actual == "5efdab323f726dee4997f15510b254e30dcf2a5d34512cd7a434b94a90c47fad"
