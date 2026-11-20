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
from dataclasses import dataclass, replace
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


@dataclass(frozen=True, slots=True)
class RouteProbe:
    method: str
    path: str
    status: int | None
    passed: bool
    connect_unavailable: bool = False
    error_class: str | None = None


@dataclass(frozen=True, slots=True)
class CanaryFailure:
    stage: str
    error_class: str
    status: int | None = None

    def as_result(self) -> dict[str, str | int]:
        result: dict[str, str | int] = {"stage": self.stage, "error_class": self.error_class}
        if self.status is not None:
            result["status"] = self.status
        return result

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


@dataclass(frozen=True, slots=True)
class RuntimeCanary:
    """Evidence from the enable job's reserved runtime and no-effect SDK check."""

    signed_hello: bool
    scoped_sdk: bool
    failure: CanaryFailure | None = None


@dataclass(frozen=True, slots=True)
class WorkerIngressProbeResult:
    origin: str
    tls: bool
    websocket: bool
    hello_rejected: bool
    routes: tuple[RouteProbe, ...]
    runtime_canary: RuntimeCanary | None = None
    probe_failure: CanaryFailure | None = None

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
            return CanaryFailure("ingress_http", failed.error_class or "route_refused", failed.status)
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


def _tls_error(cause: BaseException | None) -> bool:
    while cause is not None:
        if isinstance(cause, ssl.SSLError):
            return True
        if isinstance(cause, BaseExceptionGroup):
            return any(_tls_error(member) for member in cause.exceptions)
        cause = cause.__cause__ or cause.__context__
    return False


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
) -> RouteProbe:
    nonce = secrets.token_hex(16)
    try:
        async with client.stream(
            method, http_origin + path, headers={"Host": host, PROBE_HEADER.decode(): nonce},
            extensions={"sni_hostname": host}, json={} if method == "POST" else None,
        ) as response:
            witness = response.headers.get(INGRESS_HEADER.decode())
            status = response.status_code
            if status not in ((401, 403, 422) if allowed else (404,)):
                return RouteProbe(method, path, status, False, error_class="unexpected_status")
            if witness != (ALLOW_MARKER if allowed else DENY_MARKER).decode():
                return RouteProbe(method, path, status, False, error_class="invalid_ingress_marker")
            if response.headers.get(PROBE_HEADER.decode()) != nonce:
                return RouteProbe(method, path, status, False, error_class="invalid_probe_nonce")
            return RouteProbe(method, path, status, True)
    except httpx.HTTPError as exc:
        unavailable = False
        if _tls_error(exc):
            kind = "tls_error"
        elif isinstance(exc, httpx.ConnectTimeout):
            kind = "connect_timeout"
        elif isinstance(exc, httpx.ConnectError):
            kind = "connect_error"
            unavailable = _missing_tcp_route(exc)
        elif isinstance(exc, httpx.TimeoutException):
            kind = "route_timeout"
        elif isinstance(exc, httpx.ProtocolError):
            kind = "route_protocol_error"
        else:
            kind = "route_transport_error"
        return RouteProbe(method, path, None, False, unavailable, kind)


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
    except ssl.SSLError:
        return upgraded, CanaryFailure("ingress_websocket", "tls_error")
    except OSError:
        return upgraded, CanaryFailure("ingress_websocket", "transport_error", 101 if upgraded else None)
    except WebSocketException:
        return upgraded, CanaryFailure("ingress_websocket", "protocol_error", 101 if upgraded else None)
    return upgraded, CanaryFailure("ingress_websocket", "accepted_unsigned_hello", 101)


async def _probe_address(
    origin: str, host: str, address: str, port: int = 443, *, trust: ssl.SSLContext | None = None,
) -> WorkerIngressProbeResult:
    trust = trust or ssl.create_default_context()
    authority = f"[{address}]" if ":" in address else address
    http_origin = f"https://{authority}:{port}"
    results: list[RouteProbe] = []
    async with httpx.AsyncClient(timeout=4, follow_redirects=False, trust_env=False, verify=trust) as client:
        async def check(method: str, path: str, *, allowed: bool) -> RouteProbe:
            return await _check_route(client, http_origin, host, method, path, allowed=allowed)

        first = await check("GET", "/api/sdk/context", allowed=True)
        results.append(first)
        if first.status is None:
            return WorkerIngressProbeResult(origin, False, False, False, tuple(results))
        if not first.passed:
            return WorkerIngressProbeResult(origin, True, False, False, tuple(results))
        for method, path in _REFUSALS:
            results.append(await check(method, path, allowed=False))
        if any(not result.passed for result in results):
            return WorkerIngressProbeResult(origin, True, False, False, tuple(results))
        sample_id = str(uuid4())
        samples = [
            (method, re.sub(
                r"\{[^}]+\}", "probe.py" if "{path:path}" in template else sample_id, template,
            ))
            for method, template in sorted(ROUTES - {("GET", "/api/sdk/context")})
        ]
        samples.append(("GET", "/api/sdk/modules/probe%2Fmodule.py"))
        for index in range(0, len(samples), 8):
            results.extend(await asyncio.gather(*(
                check(method, path, allowed=True) for method, path in samples[index:index + 8]
            )))
    upgraded, failure = await _check_control(origin, host, address, port, trust)
    return WorkerIngressProbeResult(origin, True, upgraded, failure is None, tuple(results), probe_failure=failure)


async def probe_worker_ingress(
    installation: CloudflareInstallation,
    *,
    runtime_canary: Callable[[str], Awaitable[RuntimeCanary]],
) -> WorkerIngressProbeResult:
    """Probe the persisted HTTPS endpoint, then delegate signed canary ownership.

    The callback belongs to the enable job. It starts/probes only the reserved
    canary launcher and checks its server-observed signed admission and a
    no-effect SDK read from the runtime. Public probes send no credentials.
    """
    origin = installation.worker_ingress_origin.rstrip("/")
    host = urlsplit(origin).hostname
    if host is None or origin != f"https://{host}" or installation.retired_at is not None:
        raise ValueError("A current installation with a dedicated HTTPS origin is required")
    unavailable = WorkerIngressProbeResult(origin, False, False, False, ())
    result = unavailable
    unavailable_families: dict[int, WorkerIngressProbeResult] = {}
    reachable_families: set[int] = set()
    stage = "ingress_dns"
    try:
        async with asyncio.timeout(30):
            addresses = await _public_addresses(host)
            stage = "ingress_http"
            for address in addresses:
                candidate = await _probe_address(origin, host, address)
                family = ipaddress.ip_address(address).version
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
        return replace(unavailable, probe_failure=CanaryFailure(stage, "probe_deadline"))
    except OSError:
        reason = "resolution_error" if stage == "ingress_dns" else "transport_error"
        return replace(unavailable, probe_failure=CanaryFailure(stage, reason))
    except ValueError:
        return replace(unavailable, probe_failure=CanaryFailure(stage, "invalid_public_address"))
    if not result.reachable:
        return result
    canary = await runtime_canary(origin)
    return replace(result, runtime_canary=canary)
