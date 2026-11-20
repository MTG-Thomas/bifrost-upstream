import asyncio
import errno
import socket
import ssl
from unittest.mock import AsyncMock

import httpx
import pytest
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services import worker_ingress_probes as probes

ORIGIN = "https://workers.example.test"
IPV4 = "1.1.1.1"
IPV6 = "2606:4700:4700::1111"


@pytest.fixture
def ingress(monkeypatch):
    def configure(addresses, failure, cause=None):
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
            if address == IPV6:
                raise failure from cause
            assert address == IPV4
            assert request.headers["Host"] == "workers.example.test"
            assert request.extensions["sni_hostname"] == "workers.example.test"
            allowed = (request.method, request.url.path) not in probes._REFUSALS
            return httpx.Response(401 if allowed else 404, headers={
                probes.INGRESS_HEADER.decode(): (probes.ALLOW_MARKER if allowed else probes.DENY_MARKER).decode(),
                probes.PROBE_HEADER.decode(): request.headers[probes.PROBE_HEADER.decode()],
            })

        client = httpx.AsyncClient
        monkeypatch.setattr(
            probes.httpx, "AsyncClient", lambda **kwargs: client(transport=httpx.MockTransport(respond), **kwargs),
        )
        control = AsyncMock(return_value=(True, True))
        monkeypatch.setattr(probes, "_check_control", control)
        canary = AsyncMock(return_value=probes.RuntimeCanary(signed_hello=True, scoped_sdk=True))
        installation = CloudflareInstallation(worker_ingress_origin=ORIGIN)
        return installation, attempted, control, canary

    return configure


@pytest.mark.asyncio
@pytest.mark.parametrize("addresses", [(IPV6, IPV4), (IPV4, IPV6)])
async def test_dual_stack_probe_uses_reachable_address(ingress, addresses) -> None:
    installation, attempted, control, canary = ingress(
        addresses, httpx.ConnectError("Connection failed"), OSError(errno.ENETUNREACH, "Network is unreachable"),
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

    assert not (await probes.probe_worker_ingress(installation, runtime_canary=canary)).reachable
    control.assert_not_awaited()
    canary.assert_not_awaited()


@pytest.mark.asyncio
async def test_dual_stack_probe_refuses_invalid_certificate_on_reachable_peer(ingress) -> None:
    installation, attempted, _, canary = ingress(
        (IPV4, IPV6), httpx.ConnectError("TLS handshake failed"), ssl.SSLCertVerificationError("Invalid certificate"),
    )

    assert not (await probes.probe_worker_ingress(installation, runtime_canary=canary)).reachable
    assert attempted == [IPV4, IPV6]
    canary.assert_not_awaited()


@pytest.mark.asyncio
async def test_dual_stack_probe_refuses_nonpublic_dns_answer(ingress) -> None:
    installation, attempted, control, canary = ingress((IPV4, "fd00::1"), httpx.ConnectError("Unused"))

    assert not (await probes.probe_worker_ingress(installation, runtime_canary=canary)).reachable
    assert attempted == []
    control.assert_not_awaited()
    canary.assert_not_awaited()
