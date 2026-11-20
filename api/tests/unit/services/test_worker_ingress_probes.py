import asyncio
import errno
import socket
import ssl
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services import worker_ingress_probes as probes
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

ORIGIN = "https://workers.example.test"
IPV4 = "1.1.1.1"
OTHER_IPV4 = "1.0.0.1"
IPV6 = "2606:4700:4700::1111"


@pytest.fixture
def ingress(monkeypatch):
    def configure(addresses, failure, cause=None, *, route_failure=None, failed_address=IPV6):
        resolver = AsyncMock(return_value=[
            (socket.AF_INET6 if ":" in ip else socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443))
            for ip in addresses
        ])
        monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolver)
        attempted: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            address = request.url.host
            if address not in attempted:
                attempted.append(address)
            if address == failed_address:
                raise failure from cause
            assert address == IPV4
            assert request.headers["Host"] == "workers.example.test"
            assert request.extensions["sni_hostname"] == "workers.example.test"
            if route_failure is not None and request.url.path == "/auth/login":
                raise route_failure
            allowed = (request.method, request.url.path) not in probes._REFUSALS
            return httpx.Response(401 if allowed else 404, headers={
                probes.INGRESS_HEADER.decode(): (probes.ALLOW_MARKER if allowed else probes.DENY_MARKER).decode(),
                probes.PROBE_HEADER.decode(): request.headers[probes.PROBE_HEADER.decode()],
            })

        client = httpx.AsyncClient
        monkeypatch.setattr(
            probes.httpx, "AsyncClient", lambda **kwargs: client(transport=httpx.MockTransport(respond), **kwargs),
        )
        control = AsyncMock(return_value=(True, None))
        monkeypatch.setattr(probes, "_check_control", control)
        canary = AsyncMock(return_value=probes.RuntimeCanary(signed_hello=True, scoped_sdk=True))
        installation = CloudflareInstallation(worker_ingress_origin=ORIGIN)
        return installation, attempted, control, canary

    return configure


@pytest.mark.asyncio
@pytest.mark.parametrize("addresses", [(IPV6, IPV4), (IPV4, IPV6)])
async def test_dual_stack_probe_uses_reachable_address(ingress, addresses) -> None:
    wrapped = RuntimeError("Transport wrapper")
    wrapped.__context__ = OSError(errno.ENETUNREACH, "Network is unreachable")
    wrapped.__suppress_context__ = True
    installation, attempted, control, canary = ingress(
        addresses, httpx.ConnectError("Connection failed"), wrapped,
    )

    result = await probes.probe_worker_ingress(installation, runtime_canary=canary)

    assert result.ready
    assert attempted == list(addresses)
    control.assert_awaited_once()
    assert control.await_args.args[2] == IPV4
    canary.assert_awaited_once_with(ORIGIN)


@pytest.mark.asyncio
async def test_unreachable_hostname_does_not_start_canary(ingress) -> None:
    installation, _, control, canary = ingress((IPV6,), httpx.ConnectTimeout("Connection timed out"))

    result = await probes.probe_worker_ingress(installation, runtime_canary=canary)
    assert not result.reachable
    assert result.failure == probes.CanaryFailure("ingress_http", "connect_timeout")
    control.assert_not_awaited()
    canary.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("error", "kind"), [
    (httpx.ReadTimeout, "route_timeout"),
    (httpx.RemoteProtocolError, "route_protocol_error"),
    (httpx.ReadError, "route_transport_error"),
])
async def test_route_transport_failure_reports_only_its_class(ingress, error, kind) -> None:
    installation, _, control, canary = ingress(
        (IPV4,), None, route_failure=error("Private diagnostic details"),
    )
    result = await probes.probe_worker_ingress(installation, runtime_canary=canary)

    assert result.tls and not result.reachable
    assert result.failure.as_result() == {"stage": "ingress_http", "error_class": kind}
    control.assert_not_awaited()
    canary.assert_not_awaited()


@pytest.mark.parametrize(("signed", "stage"), [(False, "hello"), (True, "ack")])
def test_incomplete_runtime_canary_reports_the_unobserved_stage(signed: bool, stage: str) -> None:
    result = probes.WorkerIngressProbeResult(
        ORIGIN, True, True, True, (probes.RouteProbe("GET", "/api/sdk/context", 401, True),),
        probes.RuntimeCanary(signed, False),
    )
    assert not result.ready
    assert result.failure == probes.CanaryFailure(stage, "timeout")


@pytest.mark.asyncio
async def test_dual_stack_probe_refuses_invalid_certificate_on_reachable_peer(ingress) -> None:
    installation, attempted, _, canary = ingress(
        (IPV4, IPV6), httpx.ConnectError("TLS handshake failed"), ssl.SSLCertVerificationError("Invalid certificate"),
    )

    result = await probes.probe_worker_ingress(installation, runtime_canary=canary)
    assert not result.reachable
    assert result.failure == probes.CanaryFailure("ingress_http", "tls_error")
    assert attempted == [IPV4, IPV6]
    canary.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("transport", "cause", "kind"), [
    (httpx.ConnectError, ConnectionRefusedError(errno.ECONNREFUSED, "Server refused"), "connect_error"),
    (httpx.ConnectError, ssl.SSLEOFError("Handshake ended"), "tls_error"),
    (httpx.ConnectError, EOFError("Handshake ended"), "connect_error"),
    (httpx.ConnectTimeout, None, "connect_timeout"),
    (httpx.ConnectError, OSError(errno.EHOSTUNREACH, "Host unreachable"), "connect_error"),
])
async def test_reachable_family_requires_all_addresses(ingress, transport, cause, kind) -> None:
    installation, attempted, _, canary = ingress(
        (IPV4, OTHER_IPV4), transport("Private diagnostic details"), cause, failed_address=OTHER_IPV4,
    )
    result = await probes.probe_worker_ingress(installation, runtime_canary=canary)

    assert not result.reachable
    assert attempted == [IPV4, OTHER_IPV4]
    assert result.failure.as_result() == {"stage": "ingress_http", "error_class": kind}
    canary.assert_not_awaited()


@pytest.mark.asyncio
async def test_dual_stack_probe_refuses_nonpublic_dns_answer(ingress) -> None:
    installation, attempted, control, canary = ingress((IPV4, "fd00::1"), httpx.ConnectError("Unused"))

    result = await probes.probe_worker_ingress(installation, runtime_canary=canary)
    assert not result.reachable
    assert result.failure == probes.CanaryFailure("ingress_dns", "invalid_public_address")
    assert attempted == []
    control.assert_not_awaited()
    canary.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "marker", "reason"), [
    (521, "v1; allowed", "unexpected_status"),
    (401, "v1; denied", "invalid_ingress_marker"),
    (401, "v1; allowed", "invalid_probe_nonce"),
])
async def test_http_probe_reports_failed_response_witness(status, marker, reason) -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _r: httpx.Response(status, headers={
        probes.INGRESS_HEADER.decode(): marker, probes.PROBE_HEADER.decode(): "stale-nonce",
    }))) as client:
        route = await probes._check_route(client, ORIGIN, "workers.example.test", "GET", "/api/sdk/context", allowed=True)
    result = probes.WorkerIngressProbeResult(ORIGIN, True, False, False, (route,))
    assert result.failure == probes.CanaryFailure("ingress_http", reason, status)


@pytest.mark.asyncio
async def test_control_refusal_reports_status_without_provider_message(monkeypatch) -> None:
    monkeypatch.setattr(asyncio.get_running_loop(), "sock_connect", AsyncMock())

    refused = MagicMock()
    refused.return_value.__aenter__.side_effect = InvalidStatus(Response(403, "Private diagnostic details", Headers()))
    monkeypatch.setattr(probes, "connect", refused)
    upgraded, failure = await probes._check_control(ORIGIN, "workers.example.test", IPV4, 443, ssl.create_default_context())
    assert not upgraded
    assert failure == probes.CanaryFailure("ingress_websocket", "unexpected_status", 403)
