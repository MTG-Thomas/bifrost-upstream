"""Canary HMAC inputs are bound to a worker, run lease and session generation."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from src.services.worker_ingress_probes import CanaryFailure
from src.services.worker_pool import cloudflare_canary
from src.services.worker_pool.cloudflare_canary import _proof
from src.worker.remote.host import RemoteControlHost
from src.worker.remote.protocol import CredentialRouteChanged


def test_capacity_canary_is_bound_to_worker_lease_and_generation() -> None:
    worker = UUID(int=1)
    lease = UUID(int=2)
    key = "ab" * 32
    challenge = _proof(key, worker, lease, 1)
    assert len(challenge) == 64
    assert challenge != _proof(key, UUID(int=3), lease, 1)
    assert challenge != _proof(key, worker, UUID(int=3), 1)
    assert challenge != _proof(key, worker, lease, 2)


@pytest.mark.parametrize(
    ("submitted", "acknowledged", "expected"),
    [
        (None, None, CanaryFailure("proof", "unavailable")),
        (ConnectionError(), None, CanaryFailure("launcher", "connection_lost")),
        (CredentialRouteChanged(), None, CanaryFailure("launcher", "connection_lost")),
        (RuntimeError(), None, CanaryFailure("ack", "invalid_reply")),
        (TimeoutError(), None, CanaryFailure("ack", "timeout")),
        ({"failure": {"stage": "launcher", "error_class": "unavailable"}}, None, CanaryFailure("launcher", "unavailable")),
        ({"failure": {"stage": "sdk_fetch", "error_class": "fetch_error"}}, None, CanaryFailure("sdk_fetch", "fetch_error")),
        ({"failure": {"stage": "sdk_response", "error_class": "http_status", "status": 401}}, None,
         CanaryFailure("sdk_response", "http_status", 401)),
        ({"failure": {"stage": "sdk_response", "error_class": "sensitive message"}}, None,
         CanaryFailure("ack", "invalid_reply")),
        ({"failure": {"stage": "hello", "error_class": "timeout"}}, None, CanaryFailure("ack", "invalid_reply")),
        ({"failure": {"stage": "ingress_http", "error_class": "connect_error"}}, None, CanaryFailure("ack", "invalid_reply")),
        ({}, None, CanaryFailure("ack", "missing_sdk_receipt")),
        ({}, "a" * 64, None),
    ],
)
@pytest.mark.asyncio
async def test_canary_records_only_the_validated_failure(
    monkeypatch, submitted, acknowledged: str | None, expected: CanaryFailure | None,
) -> None:
    worker = UUID(int=1)
    proof = AsyncMock(side_effect=[None if submitted is None else "a" * 64, acknowledged])
    record = AsyncMock()
    monkeypatch.setattr(cloudflare_canary, "cloudflare_sdk_canary_proof", proof)
    monkeypatch.setattr(cloudflare_canary, "record_canary_failure", record)
    request = AsyncMock()
    if isinstance(submitted, Exception):
        request.side_effect = submitted
    else:
        request.return_value = submitted
    session = SimpleNamespace(lease=SimpleNamespace(worker_id=worker, session_generation=2),
                              bridge=SimpleNamespace(request=request, containers=False), reconciliation_tasks=set())

    RemoteControlHost()._start_capacity_canary(session)
    await asyncio.gather(*session.reconciliation_tasks)
    if expected is None:
        record.assert_not_awaited()
    else:
        record.assert_awaited_once_with(worker, 2, expected)
