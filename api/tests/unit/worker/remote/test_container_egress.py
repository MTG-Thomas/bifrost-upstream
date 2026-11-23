import errno
from contextlib import nullcontext
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest

from src.worker.remote import container_egress
from src.worker.remote.container_egress import (
    CONTAINER_EGRESS_DENIALS,
    CONTAINER_RELAY_REJECTION,
    run_container_egress_checks,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("failure", "redirect"), [
    (None, "relay"), (None, "ingress"), ("alternate-origin", "relay"), ("raw-tcp", "relay"),
    ("redirect", "unmarked"), ("redirect", "leaked-location"),
    ("rate-limited", "relay"), ("relay-unavailable", "relay"), ("socket-unavailable", "relay"),
    ("disallowed-route", "relay"), ("disallowed-route", "transient"),
])
async def test_egress_canary_only_records_observed_denials(monkeypatch, failure, redirect):
    origin, alternate = "https://ingress.example.test", "https://scope.example.test"
    address, worker, proof = "192.0.2.23", UUID(int=1), "a" * 64
    closed = AsyncMock()
    writer = Mock(wait_closed=closed)

    async def connect(host, port):
        assert host == address and port in {2053, 8443}
        if (port, failure) in {(2053, "alternate-origin"), (8443, "raw-tcp")}:
            return None, writer
        if failure == "socket-unavailable":
            raise OSError(errno.EMFILE, "fixture process limit")
        raise ConnectionRefusedError(errno.ECONNREFUSED, "blocked")

    def provider(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "ingress.example.test"
        if request.headers.get("host") == "wrong-host.invalid":
            assert request.headers.get("authorization") is None
            return httpx.Response(403, headers={CONTAINER_RELAY_REJECTION: "origin"})
        if request.url.path == "/__bifrost_canary_denied__":
            assert request.headers.get("authorization") is None
            if failure in ("rate-limited", "relay-unavailable"):
                return httpx.Response(429 if failure == "rate-limited" else 502)
            if failure == "disallowed-route":
                return httpx.Response(503, headers={"x-bifrost-container-relay-forwarded": "1"})
            return httpx.Response(403, headers={CONTAINER_RELAY_REJECTION: "route"})
        assert request.url.path == f"/api/sdk/capacity-canary/{worker}"
        assert request.headers["authorization"] == f"Bearer {proof}"
        assert request.headers["x-bifrost-canary-generation"] == "2"
        if "x-bifrost-canary-redirect" in request.headers:
            if redirect == "transient":
                return httpx.Response(502)
            if redirect == "unmarked":
                return httpx.Response(404)
            if redirect == "leaked-location":
                return httpx.Response(404, headers={"location": "/blocked", "x-bifrost-worker-ingress": "v1; denied"})
            if redirect == "ingress":
                return httpx.Response(404, headers={"x-bifrost-worker-ingress": "v1; denied"})
            return httpx.Response(403, headers={CONTAINER_RELAY_REJECTION: "redirect"})
        return httpx.Response(204, headers={"x-bifrost-worker-ingress": "v1; allowed"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(provider))
    monkeypatch.setattr(container_egress, "create_default_context", Mock())
    monkeypatch.setattr(container_egress.httpx, "AsyncClient", Mock(return_value=client))
    monkeypatch.setattr(container_egress.asyncio, "open_connection", connect)
    transient = {
        "rate-limited": httpx.HTTPStatusError, "relay-unavailable": httpx.HTTPStatusError, "socket-unavailable": OSError,
    }.get(failure)
    with pytest.raises(transient) if transient is not None else nullcontext():
        observed = await run_container_egress_checks(origin, alternate, address, worker, 2, proof)
        missing = {failure} | ({"redirect"} if redirect == "transient" else set())
        assert set(observed) == (CONTAINER_EGRESS_DENIALS - missing)
