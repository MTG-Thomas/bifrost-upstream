import asyncio
import errno
import re
import socket
import ssl
import sys
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services import worker_ingress_probes as probes
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from tests.loopback_tls import loopback_tls

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
    assert result.failure.error_class == "connect_timeout"
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
    assert len(result.routes) == 2
    assert result.failure.as_result() == {
        "stage": "ingress_http",
        "error_class": kind,
        "address_family": "ipv4",
        "method": "POST",
        "route_template": "/auth/login",
        "handshake_completed": False,
        "connection_reused": False,
        "exception_class": error.__name__,
    }
    control.assert_not_awaited()
    canary.assert_not_awaited()


@pytest.mark.asyncio
async def test_allowed_route_failure_stops_after_its_batch(monkeypatch, ingress) -> None:
    installation, _, control, canary = ingress((IPV4,), None)
    template = "/api/forms/{form_id}"
    routes = {("GET", template), *(("GET", f"/probe/{index}") for index in range(8))}
    monkeypatch.setattr(probes, "ROUTES", routes)

    async def check(_client, _origin, _host, method, path, *, allowed, route_template, progress):
        return probes.RouteProbe(method, route_template or path, 404, route_template != template)

    monkeypatch.setattr(probes, "_check_route", check)
    result = await probes.probe_worker_ingress(installation, runtime_canary=canary)

    assert result.failure.route_template == template
    assert len(result.routes) == 1 + len(probes._REFUSALS) + 8
    control.assert_not_awaited()
    canary.assert_not_awaited()


@pytest.fixture(scope="module")
def tls_contexts(tmp_path_factory: pytest.TempPathFactory) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    return loopback_tls(tmp_path_factory.mktemp("ingress-tls"), "workers.example.test")


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "kind", "exception_class", "handshake"), [
    ("wrong_certificate", "tls_error", "SSLCertVerificationError", False),
    ("handshake_close", "tls_error", "SSLEOFError", False),
    ("handshake_timeout", "connect_timeout", "ConnectTimeout", False),
    ("read_timeout", "route_timeout", "ReadTimeout", True),
    ("fresh_close", "route_protocol_error", "RemoteProtocolError", True),
    ("reused_close", "route_protocol_error", "RemoteProtocolError", True),
])
async def test_classifies_failures_on_real_tls_sockets(
    tls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    mode: str, kind: str, exception_class: str, handshake: bool,
) -> None:
    server_tls, trust = tls_contexts
    reused = mode == "reused_close"
    before_tls = mode.startswith("handshake_")
    served = asyncio.Event()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            if before_tls:
                await reader.read(4096)  # Receive ClientHello without completing TLS.
            else:
                await reader.readuntil(b"\r\n\r\n")
                if reused:
                    writer.write(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")
                    await writer.drain()
                    await reader.readuntil(b"\r\n\r\n")
            if mode.endswith("_timeout"):
                await reader.read()  # Keep the socket open until the client times out.
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            finally:
                served.set()

    server = await asyncio.start_server(serve, "127.0.0.1", 0, ssl=None if before_tls else server_tls)
    async with server:
        origin = f"https://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        host = "wrong.example.test" if mode == "wrong_certificate" else "workers.example.test"
        async with httpx.AsyncClient(verify=trust, trust_env=False, timeout=0.5) as client:
            if reused:
                await client.get(origin + "/api/sdk/context", extensions={"sni_hostname": host})
            route = await probes._check_route(
                client, origin, host, "GET", "/api/forms/local-sample", allowed=True,
                route_template="/api/forms/{form_id}",
            )
        if mode != "wrong_certificate":
            await served.wait()

    assert not route.passed
    assert route.path == "/api/forms/{form_id}"
    assert route.failure is not None
    expected = {
        "stage": "ingress_http",
        "error_class": kind,
        "address_family": "ipv4",
        "method": "GET",
        "route_template": "/api/forms/{form_id}",
        "handshake_completed": handshake,
        "connection_reused": reused,
        "exception_class": exception_class,
        "phase": "response_headers" if handshake else "tls_handshake",
    }
    if mode == "wrong_certificate":
        expected.update(tls_reason="CERTIFICATE_VERIFY_FAILED", tls_verify_code=62)
    elif mode == "handshake_close":
        expected.update(tls_reason="UNEXPECTED_EOF_WHILE_READING")
    assert route.failure.as_result() == expected


@pytest.mark.asyncio
async def test_tcp_refusal_reports_socket_errno(tls_contexts) -> None:
    _, trust = tls_contexts
    # Reserve the port without listening, so the kernel refuses the TCP connect.
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        origin = f"https://127.0.0.1:{reserved.getsockname()[1]}"
        async with httpx.AsyncClient(verify=trust, trust_env=False) as client:
            route = await probes._check_route(
                client, origin, "workers.example.test", "GET", "/api/sdk/context", allowed=True,
            )

    assert route.failure is not None
    assert route.failure.phase == "tcp_connect"
    assert route.failure.error_class == "connect_error"
    expected = {"ECONNREFUSED"}
    if sys.platform == "win32":
        # Proactor can expose WinError 1225 as POSIX EINVAL.
        expected.update({"EINVAL", "WSAECONNREFUSED"})
    assert route.failure.socket_errno in expected
    assert route.failure.handshake_completed is False


@pytest.mark.asyncio
async def test_request_write_deadline_reports_phase_after_tls(tls_contexts) -> None:
    server_tls, trust = tls_contexts
    progress = probes._ProbeProgress()
    deadline = asyncio.timeout(None)

    async def hold(reader, writer):
        try:
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()

    async def expire_on_write(request: httpx.Request) -> None:
        trace = request.extensions["trace"]

        async def expire(event, info):
            await trace(event, info)
            if event == "http11.send_request_headers.started":
                deadline.reschedule(asyncio.get_running_loop().time())
                await asyncio.sleep(0)

        request.extensions["trace"] = expire

    server = await asyncio.start_server(hold, "127.0.0.1", 0, ssl=server_tls)
    async with server:
        origin = f"https://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        async with httpx.AsyncClient(
            verify=trust, trust_env=False, event_hooks={"request": [expire_on_write]},
        ) as client:
            with pytest.raises(TimeoutError):
                async with deadline:
                    await probes._check_route(
                        client, origin, "workers.example.test", "POST", "/api/sdk/context",
                        allowed=True, progress=progress,
                    )

    assert progress.latest_failure is not None
    assert progress.latest_failure.phase == "request_write"
    assert progress.latest_failure.handshake_completed is True


@pytest.mark.asyncio
async def test_public_probe_deadline_retains_inflight_route(tls_contexts, monkeypatch) -> None:
    server_tls, trust = tls_contexts
    served = asyncio.Event()

    async def hold(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()
            served.set()

    server = await asyncio.start_server(hold, "127.0.0.1", 0, ssl=server_tls)
    async with server:
        port = server.sockets[0].getsockname()[1]
        probe_address = probes._probe_address

        async def loopback(origin, host, address, *, progress):
            return await probe_address(origin, host, address, port, trust=trust, progress=progress)

        monkeypatch.setattr(probes, "_probe_address", loopback)
        monkeypatch.setattr(probes, "_public_addresses", AsyncMock(return_value=("127.0.0.1",)))
        monkeypatch.setattr(probes, "_PROBE_DEADLINE_SECONDS", 1)
        result = await probes.probe_public_worker_ingress(ORIGIN)
        await served.wait()

    assert result.tls and not result.reachable
    assert result.failure.as_result() == {
        "stage": "ingress_http",
        "error_class": "probe_deadline",
        "address_family": "ipv4",
        "method": "GET",
        "route_template": "/api/sdk/context",
        "handshake_completed": True,
        "connection_reused": False,
        "phase": "response_headers",
    }


@pytest.mark.asyncio
async def test_tls_record_error_after_handshake_keeps_route_diagnostics(tls_contexts) -> None:
    server_tls, trust = tls_contexts

    def send_invalid_record(listener: socket.socket) -> None:
        connection, _ = listener.accept()
        connection.settimeout(4)
        with connection, server_tls.wrap_socket(connection, server_side=True) as tls:
            request = tls.makefile("rb")
            with request:
                while request.readline() not in (b"\r\n", b""):
                    pass
            # Send a plaintext response to a client already expecting TLS records.
            with socket.socket(fileno=tls.detach()) as raw:
                raw.sendall(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")

    with socket.create_server(("127.0.0.1", 0)) as listener:
        listener.settimeout(4)
        origin = f"https://127.0.0.1:{listener.getsockname()[1]}"
        async with httpx.AsyncClient(verify=trust, trust_env=False, timeout=4) as client:
            serving = asyncio.create_task(asyncio.to_thread(send_invalid_record, listener))
            route = await probes._check_route(
                client, origin, "workers.example.test", "GET", "/api/sdk/context", allowed=True,
            )
            await serving

    assert route.failure is not None
    result = route.failure.as_result()
    reason = result.pop("tls_reason")
    assert isinstance(reason, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", reason)
    assert result == {
        "stage": "ingress_http",
        "error_class": "route_transport_error",
        "address_family": "ipv4",
        "method": "GET",
        "route_template": "/api/sdk/context",
        "handshake_completed": True,
        "connection_reused": False,
        "exception_class": "SSLError",
        "phase": "response_headers",
    }


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
    error = ssl.SSLCertVerificationError("Certificate rejected for private.example")
    error.reason = "CERTIFICATE_VERIFY_FAILED"
    error.verify_code = 62
    error.verify_message = "Host does not match private.example"
    installation, attempted, _, canary = ingress(
        (IPV4, IPV6), httpx.ConnectError("TLS handshake failed"), ExceptionGroup("Connection failed", [error]),
    )

    result = await probes.probe_worker_ingress(installation, runtime_canary=canary)
    assert not result.reachable
    assert result.failure.as_result() == {
        "stage": "ingress_http",
        "error_class": "tls_error",
        "address_family": "ipv6",
        "tls_reason": "CERTIFICATE_VERIFY_FAILED",
        "tls_verify_code": 62,
        "method": "GET",
        "route_template": "/api/sdk/context",
        "handshake_completed": False,
        "connection_reused": False,
        "exception_class": "SSLCertVerificationError",
    }
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
    assert result.failure.as_result() == {
        "stage": "ingress_http",
        "error_class": kind,
        "address_family": "ipv4",
        "method": "GET",
        "route_template": "/api/sdk/context",
        "handshake_completed": False,
        "connection_reused": False,
        "exception_class": "SSLEOFError" if kind == "tls_error" else transport.__name__,
        **({"socket_errno": errno.errorcode[cause.errno]} if isinstance(cause, OSError) and cause.errno else {}),
    }
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
    assert result.failure.as_result() == {
        "stage": "ingress_http",
        "error_class": reason,
        "status": status,
        "address_family": "ipv4",
        "method": "GET",
        "route_template": "/api/sdk/context",
        "handshake_completed": True,
        "connection_reused": False,
        "phase": "response_headers",
    }


@pytest.mark.asyncio
async def test_control_refusal_reports_status_without_provider_message(monkeypatch) -> None:
    monkeypatch.setattr(asyncio.get_running_loop(), "sock_connect", AsyncMock())

    refused = MagicMock()
    refused.return_value.__aenter__.side_effect = InvalidStatus(Response(403, "Private diagnostic details", Headers()))
    monkeypatch.setattr(probes, "connect", refused)
    upgraded, failure = await probes._check_control(ORIGIN, "workers.example.test", IPV4, 443, ssl.create_default_context())
    assert not upgraded
    assert failure == probes.CanaryFailure("ingress_websocket", "unexpected_status", 403)


@pytest.mark.asyncio
async def test_control_tls_failure_omits_non_symbolic_reason(monkeypatch) -> None:
    monkeypatch.setattr(asyncio.get_running_loop(), "sock_connect", AsyncMock())
    error = ssl.SSLError("Private diagnostic details")
    error.reason = "Certificate proxy: private.example"
    connection = MagicMock()
    connection.return_value.__aenter__.side_effect = error
    monkeypatch.setattr(probes, "connect", connection)

    upgraded, failure = await probes._check_control(ORIGIN, "workers.example.test", IPV4, 443, ssl.create_default_context())

    assert not upgraded
    assert failure is not None
    assert failure.as_result() == {
        "stage": "ingress_websocket", "error_class": "tls_error", "address_family": "ipv4",
    }


def test_anyio_connection_wrapper_classifies_first_errno() -> None:
    unreachable = OSError(errno.ENETUNREACH, "Network unavailable")
    refused = ConnectionRefusedError(errno.ECONNREFUSED, "Server refused")
    refused.__cause__ = unreachable
    wrapper = OSError("All connection attempts failed")
    wrapper.__cause__ = unreachable
    assert probes._missing_tcp_route(wrapper)
    wrapper.__cause__ = ExceptionGroup("connect", [unreachable, unreachable])
    assert probes._missing_tcp_route(wrapper)
    wrapper.__cause__ = ExceptionGroup("connect", [unreachable, refused])
    assert not probes._missing_tcp_route(wrapper)
