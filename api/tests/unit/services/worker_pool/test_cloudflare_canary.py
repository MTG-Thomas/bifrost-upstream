"""Canary HMAC inputs are bound to a worker, run lease and session generation."""

from uuid import UUID

from src.services.worker_pool.cloudflare_canary import _proof


def test_capacity_canary_is_bound_to_worker_lease_and_generation() -> None:
    worker = UUID(int=1)
    lease = UUID(int=2)
    key = "ab" * 32
    challenge = _proof(key, worker, lease, 1)
    assert len(challenge) == 64
    assert challenge != _proof(key, UUID(int=3), lease, 1)
    assert challenge != _proof(key, worker, UUID(int=3), 1)
    assert challenge != _proof(key, worker, lease, 2)
