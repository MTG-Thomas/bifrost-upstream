from uuid import UUID

from shared.worker_control_auth import managed_cloudflare_control_key


def test_managed_capacity_key_wire_vector() -> None:
    actual = managed_cloudflare_control_key(
        "fixed-root", UUID(int=2), 3, "workers", UUID(int=1), "a" * 32,
    )
    assert actual == "e2f1ceb9355d8d4d264e5ab4d261c4f92f63c3fb01084674583d5771f6a357a8"
    assert managed_cloudflare_control_key("fixed-root", UUID(int=3), 3, "workers", UUID(int=1), "a" * 32) != actual
    assert managed_cloudflare_control_key("fixed-root", UUID(int=2), 4, "workers", UUID(int=1), "a" * 32) != actual
