"""Enable-job contract for worker ingress reachability and signed canaries."""

import asyncio
import errno
import ipaddress
import json
import re
import secrets
import socket
import ssl
from collections.abc import Awaitable, Callable
from contextlib import aclosing
from dataclasses import asdict, dataclass, field, replace
from typing import TYPE_CHECKING, Literal, cast
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services.worker_ingress import (
    ALLOW_MARKER,
    CONTROL_PATH,
    DENY_MARKER,
    INGRESS_HEADER,
    PROBE_HEADER,
    ROUTES,
)

if TYPE_CHECKING:
    from httpcore import AsyncNetworkStream

_REFUSALS = (
    ("POST", "/auth/login"),
    ("POST", "/api/auth/login"),
    ("GET", "/admin/x"),
    ("GET", "/openapi.json"),
    ("GET", "/docs"),
    ("GET", "/api/packages"),
    ("GET", "/api/maintenance/authz/legacy-status"),
    ("GET", "/api/sdk/requirements"),
    ("POST", "/__fixture/v1/chat/completions"),
    ("GET", CONTROL_PATH),
    ("GET", "/api/sdk/context/"),
    ("POST", "/api/sdk/context"),
)
_MISSING_ROUTE_ERRNOS = {errno.ENETUNREACH, errno.EHOSTUNREACH, errno.EAFNOSUPPORT, errno.EADDRNOTAVAIL}
_PROBE_DEADLINE_SECONDS = 30
_MAX_PROBE_BODY_BYTES = 1024
_deadline = asyncio.timeout
ProbePhase = Literal["tcp_connect", "tls_handshake", "request_write", "response_headers", "response_body", "websocket"]
_PHASE_ORDER: tuple[ProbePhase | None, ...] = (
    None, "tcp_connect", "tls_handshake", "request_write", "response_headers", "response_body", "websocket",
)


@dataclass(frozen=True, slots=True)
class RouteProbe:
    method: str
    path: str
    status: int | None
    passed: bool
    connect_unavailable: bool = False
    error_class: str | None = None
    failure: "CanaryFailure | None" = None


@dataclass(frozen=True, slots=True)
class CanaryFailure:
    stage: str
    error_class: str
    status: int | None = None
    address_family: str | None = None
    tls_reason: str | None = None
    tls_verify_code: int | None = None
    method: str | None = None
    route_template: str | None = None
    handshake_completed: bool | None = None
    connection_reused: bool | None = None
    exception_class: str | None = None
    phase: ProbePhase | None = None
    socket_errno: str | None = None

    def as_result(self) -> dict[str, str | int | bool]:
        return {
            key: value for key, value in (
                ("stage", self.stage),
                ("error_class", self.error_class),
                ("status", self.status),
                ("address_family", self.address_family),
                ("tls_reason", self.tls_reason),
                ("tls_verify_code", self.tls_verify_code),
                ("method", self.method),
                ("route_template", self.route_template),
                ("handshake_completed", self.handshake_completed),
                ("connection_reused", self.connection_reused),
                ("exception_class", self.exception_class),
                ("phase", self.phase),
                ("socket_errno", self.socket_errno),
            ) if value is not None
        }

    @classmethod
    def from_result(cls, value: object) -> "CanaryFailure":
        allowed = {
            "launcher": ("unavailable", "unexpected", "connection_lost"),
            "proof": ("invalid_command", "unavailable"),
            "sdk_fetch": ("timeout", "fetch_error"),
            "sdk_response": ("http_status", "missing_ingress_marker"),
            "ack": ("timeout", "invalid_reply", "missing_sdk_receipt"),
        }
        if isinstance(value, dict) and set(value) <= {"stage", "error_class", "status"}:
            stage, error, status = value.get("stage"), value.get("error_class"), value.get("status")
            if (
                isinstance(stage, str) and isinstance(error, str) and error in allowed.get(stage, ())
                and (status is None or (type(status) is int and 100 <= status <= 599))
            ):
                return cls(stage, error, status)
        return cls("ack", "invalid_reply")


@dataclass(slots=True)
class _ProbeProgress:
    inflight: dict[object, CanaryFailure] = field(default_factory=dict)
    deadline_failure: CanaryFailure | None = None
    tls_verified: bool = False
    tcp_connects_started: int = 0
    tls_handshakes_started: int = 0
    tls_handshakes_completed: int = 0
    reused_requests: int = 0

    def capture_deadline(self) -> None:
        # A gather cancellation unwinds its children one by one. Snapshot
        # before the first child leaves so unwind order cannot choose the phase.
        if self.deadline_failure is None and self.inflight:
            self.deadline_failure = min(
                self.inflight.values(), key=lambda failure: _PHASE_ORDER.index(failure.phase),
            )


@dataclass(frozen=True, slots=True)
class RuntimeCanary:
    """Evidence from the enable job's reserved runtime and no-effect SDK check."""

    signed_hello: bool
    scoped_sdk: bool
    failure: CanaryFailure | None = None


@dataclass(frozen=True, slots=True)
class ProbeHttpConnections:
    address_family: str
    tcp_connects_started: int
    tls_handshakes_started: int
    tls_handshakes_completed: int
    reused_requests: int


@dataclass(frozen=True, slots=True)
class WorkerIngressProbeResult:
    origin: str
    tls: bool
    websocket: bool
    hello_rejected: bool
    routes: tuple[RouteProbe, ...]
    runtime_canary: RuntimeCanary | None = None
    probe_failure: CanaryFailure | None = None
    http_connections: tuple[ProbeHttpConnections, ...] = ()

    def http_connections_as_result(self) -> list[dict[str, str | int]]:
        """Return counts in address attempt order without exposing addresses."""
        return [asdict(counts) for counts in self.http_connections]

    @property
    def reachable(self) -> bool:
        return (
            self.tls and self.websocket and self.hello_rejected
            and bool(self.routes) and all(route.passed for route in self.routes)
        )

    @property
    def ready(self) -> bool:
        canary = self.runtime_canary
        return self.reachable and canary is not None and canary.signed_hello and canary.scoped_sdk

    @property
    def failure(self) -> CanaryFailure:
        if self.probe_failure is not None:
            return self.probe_failure
        if self.runtime_canary is not None and self.runtime_canary.failure is not None:
            return self.runtime_canary.failure
        if self.reachable:
            signed = self.runtime_canary is not None and self.runtime_canary.signed_hello
            return CanaryFailure("ack" if signed else "hello", "timeout")
        failed = next((route for route in self.routes if not route.passed), None)
        if failed is not None:
            return failed.failure or CanaryFailure(
                "ingress_http", failed.error_class or "route_refused", failed.status,
                method=failed.method, route_template=failed.path,
            )
        return CanaryFailure("ingress_websocket", "upgrade_unverified")


async def _public_addresses(host: str) -> tuple[str, ...]:
    addresses = tuple(dict.fromkeys(
        str(info[4][0]) for info in await asyncio.get_running_loop().getaddrinfo(
            host, 443, type=socket.SOCK_STREAM,
        )
    ))
    if not addresses or any(
        not ipaddress.ip_address(addr).is_global or ipaddress.ip_address(addr).is_multicast
        for addr in addresses
    ):
        raise ValueError("Worker ingress DNS must resolve exclusively to public unicast addresses")
    return addresses


def _find_tls_error(cause: BaseException | None) -> ssl.SSLError | None:
    while cause is not None:
        # TLSStream awaits the socket inside SSLWantRead/WriteError handlers.
        # Any timeout or socket error can carry one of these as its context.
        if isinstance(cause, ssl.SSLError) and not isinstance(cause, (ssl.SSLWantReadError, ssl.SSLWantWriteError)):
            return cause
        if isinstance(cause, BaseExceptionGroup):
            for member in cause.exceptions:
                if (error := _find_tls_error(member)) is not None:
                    return error
        cause = cause.__cause__ or cause.__context__
    return None


def _tls_failure(stage: str, error: ssl.SSLError, address: str, *, kind: str = "tls_error") -> CanaryFailure:
    # OpenSSL's symbolic reason and numeric verification code are safe to
    # publish. Certificate names and exception messages can disclose the origin.
    reason = getattr(error, "reason", None)
    code = getattr(error, "verify_code", None) if isinstance(error, ssl.SSLCertVerificationError) else None
    return CanaryFailure(
        stage, kind, address_family="ipv6" if ":" in address else "ipv4",
        tls_reason=reason if isinstance(reason, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", reason) else None,
        tls_verify_code=code if type(code) is int and 1 <= code <= 255 else None,
    )


def _socket_errno(cause: BaseException | None) -> str | None:
    if cause is None:
        return None
    # Prefer the root errno to a wrapper's, and never interpret OpenSSL's
    # separate error-number namespace as a socket errno.
    if name := _socket_errno(cause.__cause__ or cause.__context__):
        return name
    if isinstance(cause, BaseExceptionGroup):
        return next((name for member in cause.exceptions if (name := _socket_errno(member))), None)
    if isinstance(cause, OSError) and not isinstance(cause, ssl.SSLError) and cause.errno is not None:
        return errno.errorcode.get(cause.errno)
    return None


def _missing_tcp_route(cause: BaseException | None) -> bool:
    while cause is not None:
        if isinstance(cause, OSError) and cause.errno is not None:
            return cause.errno in _MISSING_ROUTE_ERRNOS
        if isinstance(cause, BaseExceptionGroup):
            return all(_missing_tcp_route(member) for member in cause.exceptions)
        # httpcore can suppress the display of the original OSError with
        # `raise ... from None`. The socket errno is still in __context__.
        cause = cause.__cause__ or cause.__context__
    return False


async def _check_route(
    client: httpx.AsyncClient, http_origin: str, host: str, method: str, path: str, *, allowed: bool,
    route_template: str | None = None, progress: _ProbeProgress | None = None,
) -> RouteProbe:
    route = route_template or path
    nonce = secrets.token_hex(16)
    address = httpx.URL(http_origin).host
    family = "ipv6" if ":" in address else "ipv4"
    tls_started = handshake_completed = connection_reused = False
    phase: ProbePhase | None = None
    raw_stream: AsyncNetworkStream | None = None
    tracked = object()
    if progress is not None:
        progress.inflight[tracked] = CanaryFailure(
            "ingress_http", "probe_deadline", address_family=family, method=method, route_template=route,
            handshake_completed=False, connection_reused=False,
        )

    async def trace(event: str, info: dict[str, object]) -> None:
        nonlocal phase, tls_started, handshake_completed, connection_reused, raw_stream
        if event == "connection.connect_tcp.started":
            phase = "tcp_connect"
            if progress is not None:
                progress.tcp_connects_started += 1
        elif event == "connection.connect_tcp.complete":
            raw_stream = cast("AsyncNetworkStream", info["return_value"])
        elif event == "connection.start_tls.started":
            phase = "tls_handshake"
            tls_started = True
            if progress is not None:
                progress.tls_handshakes_started += 1
        elif event == "connection.start_tls.complete":
            handshake_completed = True
            raw_stream = None  # The pool now owns the TLS connection.
            if progress is not None:
                progress.tls_handshakes_completed += 1
        elif event == "http11.send_request_headers.started":
            phase = "request_write"
            # A direct HTTPS request can send headers without its own TLS
            # trace only when httpcore has assigned an existing connection.
            handshake_completed = True
            connection_reused = not tls_started
            if connection_reused and progress is not None:
                progress.reused_requests += 1
        elif event == "http11.receive_response_headers.started":
            phase = "response_headers"
        else:
            return
        if progress is not None:
            progress.inflight[tracked] = describe(progress.inflight[tracked])

    def describe(failure: CanaryFailure) -> CanaryFailure:
        return replace(
            failure, method=method, route_template=route, phase=phase,
            handshake_completed=handshake_completed, connection_reused=connection_reused,
        )

    try:
        async with client.stream(
            method, http_origin + path, headers={"Host": host, PROBE_HEADER.decode(): nonce},
            extensions={"sni_hostname": host, "trace": trace}, json={} if method == "POST" else None,
        ) as response:
            phase = "response_headers"
            handshake_completed = True
            witness = response.headers.get(INGRESS_HEADER.decode())
            status = response.status_code
            if status not in ((401, 403, 422) if allowed else (404,)):
                kind = "unexpected_status"
            elif witness != (ALLOW_MARKER if allowed else DENY_MARKER).decode():
                kind = "invalid_ingress_marker"
            elif response.headers.get(PROBE_HEADER.decode()) != nonce:
                kind = "invalid_probe_nonce"
            else:
                phase = "response_body"
                if progress is not None:
                    progress.inflight[tracked] = describe(progress.inflight[tracked])
                # Consume HTTP framing to release the socket. Count raw bytes
                # without retaining chunks or invoking content decompression.
                size = 0
                async with aclosing(response.aiter_raw()) as chunks:
                    async for chunk in chunks:
                        size += len(chunk)
                        if size > _MAX_PROBE_BODY_BYTES:
                            break
                    else:
                        return RouteProbe(method, route, status, True)
                kind = "response_body_too_large"
            failure = describe(CanaryFailure("ingress_http", kind, status, address_family=family))
            return RouteProbe(method, route, status, False, error_class=kind, failure=failure)
    except asyncio.CancelledError:
        if progress is not None:
            progress.capture_deadline()
        # httpcore does not own the stream in its pool until TLS completes,
        # and its handshake cleans up Exception but not external cancellation.
        if raw_stream is not None and not handshake_completed:
            await raw_stream.aclose()
        raise
    except (httpx.HTTPError, ssl.SSLError) as exc:
        tls_error = _find_tls_error(exc)
        unavailable = False
        if tls_error is not None and not handshake_completed and isinstance(exc, httpx.ConnectError):
            kind = "tls_error"
        elif isinstance(exc, httpx.ConnectTimeout):
            kind = "connect_timeout"
        elif isinstance(exc, httpx.ConnectError):
            kind = "connect_error"
            unavailable = phase in (None, "tcp_connect") and _missing_tcp_route(exc)
        elif isinstance(exc, httpx.TimeoutException):
            kind = "route_timeout"
        elif isinstance(exc, httpx.ProtocolError):
            kind = "route_protocol_error"
        else:
            kind = "route_transport_error"
        failure = (
            _tls_failure("ingress_http", tls_error, address, kind=kind)
            if tls_error is not None
            else CanaryFailure("ingress_http", kind, address_family=family)
        )
        failure = describe(replace(
            failure, exception_class=type(tls_error or exc).__name__, socket_errno=_socket_errno(exc),
        ))
        return RouteProbe(method, route, None, False, unavailable, kind, failure)
    finally:
        if progress is not None:
            del progress.inflight[tracked]


async def _check_control(
    origin: str, host: str, address: str, port: int, trust: ssl.SSLContext,
) -> tuple[bool, CanaryFailure | None]:
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    nonce = secrets.token_hex(16)
    upgraded = False
    try:
        async with asyncio.timeout(5):
            with socket.socket(family, socket.SOCK_STREAM) as sock:
                sock.setblocking(False)
                await asyncio.get_running_loop().sock_connect(sock, (address, port))
                # Passing a connected socket pins DNS. websockets also refuses
                # redirects on a supplied socket, before any hello is sent.
                async with connect(
                    "wss://" + origin.removeprefix("https://") + CONTROL_PATH,
                    sock=sock, ssl=trust, server_hostname=host, proxy=None, max_size=4096, close_timeout=1,
                    additional_headers={PROBE_HEADER.decode(): nonce},
                ) as ws:
                    upgraded = True
                    if (
                        ws.response is None
                        or ws.response.headers.get(PROBE_HEADER.decode()) != nonce
                        or ws.response.headers.get(INGRESS_HEADER.decode()) != ALLOW_MARKER.decode()
                    ):
                        return upgraded, CanaryFailure("ingress_websocket", "invalid_upgrade_witness", 101)
                    await ws.send(json.dumps({"event": "probe"}))
                    await ws.recv()
    except ConnectionClosed as exc:
        if upgraded and exc.rcvd is not None and exc.rcvd.code == 1008:
            return True, None
        return upgraded, CanaryFailure("ingress_websocket", "unexpected_close", 101 if upgraded else None)
    except InvalidStatus as exc:
        return upgraded, CanaryFailure("ingress_websocket", "unexpected_status", exc.response.status_code)
    except TimeoutError:
        return upgraded, CanaryFailure("ingress_websocket", "timeout", 101 if upgraded else None)
    except ssl.SSLError as exc:
        return upgraded, _tls_failure("ingress_websocket", exc, address)
    except OSError:
        return upgraded, CanaryFailure("ingress_websocket", "transport_error", 101 if upgraded else None)
    except WebSocketException:
        return upgraded, CanaryFailure("ingress_websocket", "protocol_error", 101 if upgraded else None)
    return upgraded, CanaryFailure("ingress_websocket", "accepted_unsigned_hello", 101)


async def _probe_address(
    origin: str, host: str, address: str, port: int = 443, *, trust: ssl.SSLContext | None = None,
    progress: _ProbeProgress | None = None,
) -> WorkerIngressProbeResult:
    trust = trust or ssl.create_default_context()
    authority = f"[{address}]" if ":" in address else address
    http_origin = f"https://{authority}:{port}"
    results: list[RouteProbe] = []
    async with httpx.AsyncClient(timeout=4, follow_redirects=False, trust_env=False, verify=trust) as client:
        async def check(method: str, path: str, *, allowed: bool, template: str | None = None) -> RouteProbe:
            return await _check_route(
                client, http_origin, host, method, path, allowed=allowed, route_template=template, progress=progress,
            )

        first = await check("GET", "/api/sdk/context", allowed=True)
        results.append(first)
        if first.status is None:
            tls = first.failure is not None and first.failure.handshake_completed is True
            return WorkerIngressProbeResult(origin, tls, False, False, tuple(results))
        if progress is not None:
            progress.tls_verified = True
        if not first.passed:
            return WorkerIngressProbeResult(origin, True, False, False, tuple(results))
        for method, path in _REFUSALS:
            results.append(await check(method, path, allowed=False))
            if not results[-1].passed:
                return WorkerIngressProbeResult(origin, True, False, False, tuple(results))
        sample_id = str(uuid4())
        samples = [
            (method, template, re.sub(
                r"\{[^}]+\}", "probe.py" if "{path:path}" in template else sample_id, template,
            ))
            for method, template in sorted(ROUTES - {("GET", "/api/sdk/context")})
        ]
        samples.append(("GET", "/api/sdk/modules/{path:path}", "/api/sdk/modules/probe%2Fmodule.py"))
        for index in range(0, len(samples), 8):
            batch = await asyncio.gather(*(
                check(method, path, allowed=True, template=template)
                for method, template, path in samples[index:index + 8]
            ))
            results.extend(batch)
            if any(not route.passed for route in batch):
                return WorkerIngressProbeResult(origin, True, False, False, tuple(results))
    tracked = object()
    if progress is not None:
        progress.inflight[tracked] = CanaryFailure(
            "ingress_websocket", "probe_deadline", address_family="ipv6" if ":" in address else "ipv4",
            method="GET", route_template=CONTROL_PATH, connection_reused=False,
            phase="websocket",
        )
    try:
        upgraded, failure = await _check_control(origin, host, address, port, trust)
    except asyncio.CancelledError:
        if progress is not None:
            progress.capture_deadline()
        raise
    finally:
        if progress is not None:
            del progress.inflight[tracked]
    return WorkerIngressProbeResult(origin, True, upgraded, failure is None, tuple(results), probe_failure=failure)


async def probe_public_worker_ingress(origin: str) -> WorkerIngressProbeResult:
    """Run Enable's credential-free address, HTTP route and WebSocket checks."""
    host = urlsplit(origin).hostname
    if host is None or origin != f"https://{host}":
        raise ValueError("A dedicated HTTPS origin is required")
    attempts: list[tuple[int, _ProbeProgress]] = []
    result = await _probe_public_addresses(origin, host, attempts)
    return replace(result, http_connections=tuple(
        ProbeHttpConnections(
            f"ipv{family}", progress.tcp_connects_started, progress.tls_handshakes_started,
            progress.tls_handshakes_completed, progress.reused_requests,
        )
        for family, progress in attempts
    ))


async def _probe_public_addresses(
    origin: str, host: str, attempts: list[tuple[int, _ProbeProgress]],
) -> WorkerIngressProbeResult:
    unavailable = WorkerIngressProbeResult(origin, False, False, False, ())
    result = unavailable
    unavailable_families: dict[int, WorkerIngressProbeResult] = {}
    reachable_families: set[int] = set()
    stage = "ingress_dns"
    progress = _ProbeProgress()
    try:
        async with _deadline(_PROBE_DEADLINE_SECONDS):
            addresses = await _public_addresses(host)
            stage = "ingress_http"
            for address in addresses:
                progress = _ProbeProgress()
                family = ipaddress.ip_address(address).version
                attempts.append((family, progress))
                candidate = await _probe_address(origin, host, address, progress=progress)
                if candidate.routes and candidate.routes[0].connect_unavailable:
                    if family in reachable_families:
                        return candidate
                    unavailable_families[family] = candidate
                    if not result.reachable:
                        result = candidate
                    continue
                if not candidate.reachable:
                    return candidate
                if family in unavailable_families:
                    return unavailable_families[family]
                reachable_families.add(family)
                result = candidate
    except TimeoutError:
        failure = progress.deadline_failure
        if failure is not None:
            return replace(
                unavailable, tls=progress.tls_verified,
                probe_failure=failure,
            )
        return replace(unavailable, tls=progress.tls_verified, probe_failure=CanaryFailure(stage, "probe_deadline"))
    except OSError:
        reason = "resolution_error" if stage == "ingress_dns" else "transport_error"
        return replace(unavailable, probe_failure=CanaryFailure(stage, reason))
    except ValueError:
        return replace(unavailable, probe_failure=CanaryFailure(stage, "invalid_public_address"))
    return result


async def probe_worker_ingress(
    installation: CloudflareInstallation,
    *,
    runtime_canary: Callable[[str], Awaitable[RuntimeCanary]],
) -> WorkerIngressProbeResult:
    """Probe the persisted endpoint, then ask Enable to verify the signed runtime."""
    if installation.retired_at is not None:
        raise ValueError("A current installation with a dedicated HTTPS origin is required")
    origin = installation.worker_ingress_origin.rstrip("/")
    result = await probe_public_worker_ingress(origin)
    if not result.reachable:
        return result
    canary = await runtime_canary(origin)
    return replace(result, runtime_canary=canary)
