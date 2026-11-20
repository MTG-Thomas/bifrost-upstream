"""Enable-job contract for worker ingress reachability and signed canaries."""

import asyncio
import ipaddress
import json
import re
import socket
import ssl
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services.worker_ingress import (
    ALLOW_MARKER,
    CONTROL_PATH,
    DENY_MARKER,
    INGRESS_HEADER,
    ROUTES,
)

_REFUSALS = (
    ("POST", "/auth/login"),
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


@dataclass(frozen=True, slots=True)
class RouteProbe:
    method: str
    path: str
    status: int | None
    passed: bool


@dataclass(frozen=True, slots=True)
class RuntimeCanary:
    """Evidence from the enable job's reserved runtime and no-effect SDK check."""

    signed_hello: bool
    scoped_sdk: bool


@dataclass(frozen=True, slots=True)
class WorkerIngressProbeResult:
    origin: str
    tls: bool
    websocket: bool
    hello_rejected: bool
    routes: tuple[RouteProbe, ...]
    runtime_canary: RuntimeCanary | None = None

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


async def _public_addresses(host: str) -> tuple[str, ...]:
    addresses = {
        str(info[4][0]) for info in await asyncio.get_running_loop().getaddrinfo(
            host, 443, type=socket.SOCK_STREAM,
        )
    }
    if not addresses or any(
        not ipaddress.ip_address(addr).is_global or ipaddress.ip_address(addr).is_multicast
        for addr in addresses
    ):
        raise ValueError("Worker ingress DNS must resolve exclusively to public unicast addresses")
    return tuple(sorted(addresses))


async def _check_route(
    client: httpx.AsyncClient, http_origin: str, host: str, method: str, path: str, *, allowed: bool,
) -> RouteProbe:
    try:
        async with client.stream(
            method, http_origin + path, headers={"Host": host},
            extensions={"sni_hostname": host}, json={} if method == "POST" else None,
        ) as response:
            witness = response.headers.get(INGRESS_HEADER.decode())
            passed = (
                response.status_code in (401, 403, 422) and witness == ALLOW_MARKER.decode()
                if allowed else response.status_code == 404 and witness == DENY_MARKER.decode()
            )
            return RouteProbe(method, path, response.status_code, passed)
    except httpx.HTTPError:
        return RouteProbe(method, path, None, False)


async def _check_control(
    origin: str, host: str, address: str, port: int, trust: ssl.SSLContext,
) -> tuple[bool, bool]:
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
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
                ) as ws:
                    upgraded = True
                    await ws.send(json.dumps({"event": "probe"}))
                    await ws.recv()
    except ConnectionClosed as exc:
        return upgraded, upgraded and exc.rcvd is not None and exc.rcvd.code == 1008
    except (OSError, TimeoutError, WebSocketException):
        return upgraded, False
    return upgraded, False


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
    upgraded, rejected = await _check_control(origin, host, address, port, trust)
    return WorkerIngressProbeResult(origin, True, upgraded, rejected, tuple(results))


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
    try:
        async with asyncio.timeout(30):
            addresses = await _public_addresses(host)
            for address in addresses:
                result = await _probe_address(origin, host, address)
                if not result.reachable:
                    return result
    except (OSError, TimeoutError, ValueError):
        return unavailable
    canary = await runtime_canary(origin)
    return WorkerIngressProbeResult(
        origin, result.tls, result.websocket, result.hello_rejected, result.routes, canary,
    )
