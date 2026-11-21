"""Restricted, proxy-facing ingress for external workflow runtimes."""

import asyncio
import hmac
import json
import logging
import re
import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager, suppress
from pathlib import Path
from time import monotonic
from urllib.parse import quote, unquote_to_bytes
from uuid import UUID

import h11
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.routing import APIRoute, APIWebSocketRoute
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from starlette.routing import Match
from starlette.types import Message, Receive, Scope, Send
from uvicorn.protocols.http.h11_impl import H11Protocol

from src.config import get_settings
from src.core.database import get_db_context, get_session_factory
from src.core.security import decode_expired_access_token, decode_token
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services.worker_pool.cloudflare_spend import record_sdk_gateway_request
from src.worker.remote.protocol import MAX_FRAME_BYTES

PORT = 8001
logger = logging.getLogger(__name__)
INGRESS_HEADER = b"x-bifrost-worker-ingress"
POLICY = json.loads(Path(__file__).with_name("worker_ingress_routes.json").read_text())
CONTROL_PATH: str = POLICY["control"]
_SDK_POLICY = json.loads(
    (Path(__file__).resolve().parents[2] / "bifrost" / "cloudflare_sdk_routes.json").read_text()
)
_SDK_ROUTE_METHODS: dict[str, list[str]] = _SDK_POLICY["routes"]
_INGRESS_ROUTE_METHODS: dict[str, list[str]] = POLICY["routes"]
_UUID = re.compile(_SDK_POLICY["identifiers"]["uuid"])
_EMAIL = re.compile(r"[A-Za-z0-9.!$&'*+=^_~-]+@[A-Za-z0-9.-]+")
# Exact canonical operations come directly from the runtime policy. Its
# prefix entries are an outer envelope; the matrix narrows those to handlers.
_EXACT_TEMPLATES = {
    "/api/roles/{uuid}": "/api/roles/{role_id}",
    "/api/roles/{uuid}/users": "/api/roles/{role_id}/users",
    "/api/roles/{uuid}/forms": "/api/roles/{role_id}/forms",
    "/api/users/{uuid}": "/api/users/{user_id}",
    "/api/users/{email}": "/api/users/{user_id}",
    "/api/platform-jobs/{uuid}": "/api/platform-jobs/{job_id}",
}
ROUTES: set[tuple[str, str]] = {
    (method, _EXACT_TEMPLATES.get(path, path))
    for path, methods in _SDK_ROUTE_METHODS.items() for method in methods
} | {
    (method, path) for path, methods in _INGRESS_ROUTE_METHODS.items() for method in methods
    if any(path == prefix or path.startswith(prefix.rstrip("/") + "/") for prefix in _SDK_POLICY["prefixes"])
}
ALLOW_MARKER = f'v{POLICY["version"]}; allowed'.encode()
DENY_MARKER = f'v{POLICY["version"]}; denied'.encode()
PROBE_HEADER = b"x-bifrost-ingress-probe"
_UUID_PARAMETERS = frozenset({"org_id", "role_id", "form_id", "agent_id", "run_id", "job_id", "execution_id", "worker_id"})
_CANARY_PREFIX = "/api/sdk/capacity-canary/"
_SOURCE_PREFIX = "/api/sdk/modules/"
_ENCODED_BYTE = re.compile(rb"%([0-9a-fA-F]{2})")
_UNRESERVED = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
_BODY_LIMIT = 4 * 1024 * 1024
_BODY_TIMEOUT = 10
_HEADER_TIMEOUT = 10
_INACTIVITY_TIMEOUT = 3
_HELLO_TIMEOUT = 5
_ORIGIN_TTL = 3
_REFRESH_PATH = "/auth/refresh"


def _marker_headers(scope: Scope, allowed: bool) -> list[tuple[bytes, bytes]]:
    headers = [(INGRESS_HEADER, ALLOW_MARKER if allowed else DENY_MARKER), (b"cache-control", b"no-store")]
    nonce = _header_values(scope, PROBE_HEADER)
    if len(nonce) == 1 and re.fullmatch(rb"[0-9a-f]{32}", nonce[0]):
        headers.append((PROBE_HEADER, nonce[0]))
    return headers


def _canonical_path(scope: Scope) -> bool:
    raw = scope.get("raw_path")
    path = scope["path"]
    if (
        not raw or len(raw) > 4096 or not path.startswith("/") or scope.get("root_path")
        or any(segment in {"", ".", ".."} for segment in path[1:].split("/"))
        or any(c in path for c in "\\%?#")
        or any(ord(c) < 32 or ord(c) == 127 or c == "\ufffd" for c in path)
    ):
        return False
    if unquote_to_bytes(raw) != path.encode():
        return False
    if path.startswith(_SOURCE_PREFIX) and not path.endswith(".py"):
        return False
    if re.search(rb"%2f", raw, re.IGNORECASE):
        # Tenant gateway encodes the complete module storage key as one
        # component. Permit that exact spelling only under the source router.
        prefix = _SOURCE_PREFIX.encode()
        return raw.startswith(prefix) and raw[len(prefix):] == quote(
            path[len(_SOURCE_PREFIX):], safe="~!*'()-._",
        ).encode()
    return not any(int(byte, 16) in _UNRESERVED for byte in _ENCODED_BYTE.findall(raw))


def _header_values(scope: Scope, name: bytes) -> list[bytes]:
    return [value for key, value in scope["headers"] if key.lower() == name]


def _upgrade(scope: Scope) -> bool:
    connections = b",".join(_header_values(scope, b"connection")).lower().split(b",")
    return (
        scope["type"] == "websocket"
        and [value.lower() for value in _header_values(scope, b"upgrade")] == [b"websocket"]
        and any(item.strip() == b"upgrade" for item in connections)
    )


def allows_request(app: FastAPI, scope: Scope) -> bool:
    if not _canonical_path(scope):
        return False
    websocket = scope["type"] == "websocket"
    if websocket:
        if scope["path"] != CONTROL_PATH or not _upgrade(scope):
            return False
    elif _header_values(scope, b"upgrade"):
        return False
    # Match the same first full route as the normal router. This prevents
    # a new static route from inheriting a parameterized route's allow entry.
    for route in app.router.routes:
        match, child = route.matches(scope)
        if match is not Match.FULL:
            continue
        if websocket:
            return isinstance(route, APIWebSocketRoute) and route.path == CONTROL_PATH
        if not isinstance(route, APIRoute) or (scope["method"], route.path) not in ROUTES:
            return False
        params: Mapping[str, str] = child.get("path_params", {})
        if any(not _UUID.fullmatch(params[key]) for key in _UUID_PARAMETERS & params.keys()):
            return False
        user = params.get("user_id")
        return user is None or bool(_UUID.fullmatch(user) or _EMAIL.fullmatch(user))
    return False


def _content_length(scope: Scope) -> int:
    lengths = _header_values(scope, b"content-length")
    if not lengths:
        return 0
    if len(lengths) != 1 or not re.fullmatch(rb"[0-9]{1,19}", lengths[0]):
        return -1
    return int(lengths[0])


async def _respond(scope: Scope, send: Send, status: int, *, allowed: bool = False) -> None:
    headers = _marker_headers(scope, allowed)
    if status == 429:
        headers.append((b"retry-after", b"1"))
    if status == 401:
        headers.append((b"www-authenticate", b"Bearer"))
    if scope["type"] == "http":
        headers.append((b"connection", b"close"))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": b""})
    elif "websocket.http.response" in scope.get("extensions", {}):
        await send({"type": "websocket.http.response.start", "status": status, "headers": headers})
        await send({"type": "websocket.http.response.body", "body": b""})
    else:
        await send({"type": "websocket.close", "code": 1008})


class _RequestBudget:
    def __init__(self, rate: int, concurrency: int) -> None:
        self.rate = rate
        self.concurrency = concurrency
        self.tokens = float(rate)
        self.updated = monotonic()
        self.active = 0

    def take(self) -> bool:
        now = monotonic()
        self.tokens = min(self.rate, self.tokens + (now - self.updated) * self.rate)
        self.updated = now
        if self.active >= self.concurrency or self.tokens < 1:
            return False
        self.tokens -= 1
        self.active += 1
        return True


class _IdentityBudgets:
    def __init__(self, rate: int, concurrency: int) -> None:
        self.rate = rate
        self.concurrency = concurrency
        self.budgets: dict[str, _RequestBudget] = {}
        self.sweep_at = 0.0

    def take(self, key: str) -> _RequestBudget | None:
        now = monotonic()
        if now >= self.sweep_at:
            self.budgets = {
                k: v for k, v in self.budgets.items() if v.active or now - v.updated < 60
            }
            self.sweep_at = now + 60
        budget = self.budgets.setdefault(key, _RequestBudget(self.rate, self.concurrency))
        return budget if budget.take() else None


async def _installation_origin() -> str | None:
    async with asyncio.timeout(3), get_session_factory()() as db:
        return await db.scalar(select(CloudflareInstallation.worker_ingress_origin).where(
            CloudflareInstallation.retired_at.is_(None)
        ))


async def _meter_gateway(origin: str, execution_id: UUID) -> None:
    async with asyncio.timeout(3), get_db_context() as db:
        await record_sdk_gateway_request(db, origin=origin, execution_id=execution_id)


def seal_capacity_canary(worker_id: UUID, generation: int, proof: str) -> str:
    """Bind a lease proof to its ingress identity before consulting the database."""
    subject = f"bifrost:worker-ingress:canary:v1:{worker_id}:{generation}:{proof}"
    signature = hmac.digest(get_settings().secret_key.encode(), subject.encode(), "sha256").hex()
    return proof + signature[:32]


def _canary_identity(scope: Scope) -> tuple[UUID, int, str] | None:
    authorization = _header_values(scope, b"authorization")
    generation_header = _header_values(scope, b"x-bifrost-canary-generation")
    if len(authorization) != 1 or len(generation_header) != 1:
        return None
    credential = re.fullmatch(rb"Bearer ([0-9a-f]{64})", authorization[0])
    if credential is None or not re.fullmatch(rb"[0-9]{1,19}", generation_header[0]):
        return None
    worker_id = UUID(scope["path"].removeprefix(_CANARY_PREFIX))
    generation = int(generation_header[0])
    proof = credential[1].decode()
    if not 0 < generation <= 2**63 - 1 or not hmac.compare_digest(
        proof, seal_capacity_canary(worker_id, generation, proof[:32]),
    ):
        return None
    return worker_id, generation, proof


def _access_claims(scope: Scope) -> dict | None:
    headers = _header_values(scope, b"authorization")
    if len(headers) != 1 or len(headers[0]) > 8192:
        return None
    scheme, _, token = headers[0].partition(b" ")
    if scheme.lower() != b"bearer" or not token.isascii():
        return None
    value = token.decode()
    # Preserve the normal API's expired-access challenge so the SDK can
    # refresh a live run. JWT verification still precedes body admission.
    return decode_token(value, expected_type="access") or decode_expired_access_token(value)


def _refresh_claims(body: bytes) -> dict | None:
    try:
        payload = json.loads(body)
    except (ValueError, RecursionError):
        return None
    token = payload.get("refresh_token") if isinstance(payload, dict) else None
    return decode_token(token, expected_type="run_refresh") if isinstance(token, str) else None


class WorkerIngress:
    def __init__(
        self, app: FastAPI, *, origin: Callable[[], Awaitable[str | None]] = _installation_origin,
    ) -> None:
        self.app = app
        self.origin = origin
        self.cached_origin: str | None = None
        self.origin_until = 0.0
        self.origin_refresh: asyncio.Task[str | None] | None = None
        self.refresh = _RequestBudget(16, 8)
        self.capacity_canary = _IdentityBudgets(4, 2)
        self.pending_hello = _RequestBudget(16, 8)
        self.http = _IdentityBudgets(40, 8)
        self.launchers = _IdentityBudgets(4, 2)

    async def _refresh_origin(self) -> str | None:
        async with asyncio.timeout(3):
            origin = await self.origin()
        self.cached_origin = origin
        self.origin_until = monotonic() + _ORIGIN_TTL
        return origin

    def _finish_origin_refresh(self, task: asyncio.Task[str | None]) -> None:
        self.origin_refresh = None
        if not task.cancelled():
            # Observe a failure even when every request that joined it disconnected.
            task.exception()

    async def _origin(self) -> str | None:
        if monotonic() < self.origin_until:
            return self.cached_origin
        if self.origin_refresh is None:
            self.origin_refresh = asyncio.create_task(self._refresh_origin())
            self.origin_refresh.add_done_callback(self._finish_origin_refresh)
        return await asyncio.shield(self.origin_refresh)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        host = _header_values(scope, b"host")
        if len(host) != 1 or not allows_request(self.app, scope):
            await _respond(scope, send, 404)
            return
        if scope["type"] == "http" and scope["path"].startswith(_CANARY_PREFIX):
            identity = _canary_identity(scope)
            if identity is None:
                await _respond(scope, send, 401, allowed=True)
                return
            worker_id, generation, proof = identity
            budget = self.capacity_canary.take(str(worker_id))
            if budget is None:
                await _respond(scope, send, 429)
                return
            try:
                state = {**scope.get("state", {}), "worker_ingress_capacity_canary": (generation, proof)}
                scope = {**scope, "state": state}
                await self._serve(scope, receive, send, host[0])
            finally:
                budget.active -= 1
            return
        await self._serve(scope, receive, send, host[0])

    async def _serve(self, scope: Scope, receive: Receive, send: Send, host: bytes) -> None:
        try:
            origin = await self._origin()
        except (OSError, SQLAlchemyError):
            await _respond(scope, send, 503)
            return
        if origin is None or b"https://" + host.lower() != origin.rstrip("/").encode():
            await _respond(scope, send, 404)
            return
        scope = {
            **scope,
            "scheme": "https" if scope["type"] == "http" else "wss",
            "headers": [
                (key, value) for key, value in scope["headers"]
                if key.lower() not in {b"cookie", b"forwarded", b"x-real-ip"}
                and not key.lower().startswith(b"x-forwarded-")
            ],
        }
        if scope["type"] == "websocket":
            await self._serve_control(scope, receive, send)
            return
        canary = "worker_ingress_capacity_canary" in scope.get("state", {})
        limit = 0 if canary else (8192 if scope["path"] == _REFRESH_PATH else _BODY_LIMIT)
        if not 0 <= _content_length(scope) <= limit:
            await _respond(scope, send, 413)
            return
        if canary:
            await self._serve_http(scope, receive, send, limit)
            return
        metered = False

        async def meter(execution_id: UUID) -> None:
            nonlocal metered
            if not metered:
                try:
                    await _meter_gateway(origin, execution_id)
                except (TimeoutError, SQLAlchemyError):
                    raise HTTPException(503, "Cloudflare Usage Ledger Unavailable") from None
                metered = True

        scope["worker_ingress_meter"] = meter
        refresh = scope["path"] == _REFRESH_PATH
        if refresh:
            budget = self.refresh if self.refresh.take() else None
        else:
            claims = _access_claims(scope)
            if not claims or not isinstance(claims.get("sub"), str):
                await _respond(scope, send, 401, allowed=True)
                return
            path = scope["path"]
            if scope["method"] != "GET" and any(
                path == root or path.startswith(root + "/")
                for root in ("/api/users", "/api/organizations", "/api/roles")
            ) and claims.get("credential_kind") != "workflow_execution":
                await _respond(scope, send, 403, allowed=True)
                return
            budget = self.http.take(claims["sub"])
        if budget is None:
            await _respond(scope, send, 429)
            return
        try:
            await self._serve_http(scope, receive, send, limit)
        finally:
            budget.active -= 1

    async def _serve_control(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not self.pending_hello.take():
            await _respond(scope, send, 429)
            return
        active: _RequestBudget | None = None
        deadline = asyncio.timeout(_HELLO_TIMEOUT)

        def authenticate(launcher: UUID) -> bool:
            nonlocal active
            active = self.launchers.take(str(launcher))
            if active is not None:
                deadline.reschedule(None)
                self.pending_hello.active -= 1
            return active is not None

        scope["worker_ingress_authenticate"] = authenticate

        async def send_marked(message: Message) -> None:
            if message["type"] == "websocket.accept":
                message = {**message, "headers": message.get("headers", []) + _marker_headers(scope, True)}
            await send(message)

        try:
            async with deadline:
                await self.app(scope, receive, send_marked)
        except TimeoutError:
            if not deadline.expired():
                raise
            await send({"type": "websocket.close", "code": 1008})
        finally:
            (active or self.pending_hello).active -= 1

    async def _serve_http(self, scope: Scope, receive: Receive, send: Send, limit: int) -> None:
        body = bytearray()
        try:
            async with asyncio.timeout(_BODY_TIMEOUT):
                while True:
                    event = await asyncio.wait_for(receive(), timeout=_INACTIVITY_TIMEOUT)
                    if event["type"] == "http.disconnect":
                        return
                    chunk = event.get("body", b"")
                    if len(body) + len(chunk) > limit:
                        await _respond(scope, send, 413)
                        return
                    body.extend(chunk)
                    if not event.get("more_body", False):
                        break
        except TimeoutError:
            await _respond(scope, send, 408)
            return
        replay = bytes(body)
        del body
        if scope["path"] == _REFRESH_PATH:
            claims = _refresh_claims(replay)
            if not claims or not isinstance(claims.get("sub"), str):
                await _respond(scope, send, 401, allowed=True)
                return
            # Switch to the same run quota before the normal handler redeems
            # the refresh credential. The body-reading budget stays bounded.
            budget = self.http.take(claims["sub"])
            if budget is None:
                await _respond(scope, send, 429)
                return
            self.refresh.active -= 1
            try:
                await self._forward(scope, receive, send, replay)
            finally:
                budget.active -= 1
                self.refresh.active += 1
            return
        await self._forward(scope, receive, send, replay)

    async def _forward(self, scope: Scope, receive: Receive, send: Send, body: bytes) -> None:
        replay: bytes | None = body

        async def replay_body() -> Message:
            nonlocal replay
            if replay is not None:
                event: Message = {"type": "http.request", "body": replay, "more_body": False}
                replay = None
                return event
            return await receive()

        redirected = False

        async def send_guarded(message: Message) -> None:
            nonlocal redirected
            if message["type"] == "http.response.start":
                if 300 <= message["status"] < 400 and message["status"] != 304:
                    redirected = True
                    await _respond(scope, send, 404)
                    return
                headers = [
                    (key, value) for key, value in message.get("headers", [])
                    if key.lower() not in {INGRESS_HEADER, PROBE_HEADER, b"cache-control"}
                ]
                message = {**message, "headers": headers + _marker_headers(scope, True)}
            if not redirected:
                await send(message)

        await self.app(scope, replay_body, send_guarded)


class _IngressHTTP(H11Protocol):
    """Close incomplete HTTP headers before an ASGI request exists."""

    header_timer: asyncio.TimerHandle | None = None
    header_started: float | None = None

    def connection_made(self, transport: asyncio.Transport) -> None:
        super().connection_made(transport)
        self._start_header_timer()

    def connection_lost(self, exc: Exception | None) -> None:
        self._stop_header_timer()
        super().connection_lost(exc)

    def handle_events(self) -> None:
        self._stop_header_timer()
        super().handle_events()
        # h11 is IDLE only before a complete request header or after
        # starting the next keep-alive cycle. The body has its own deadline.
        if (
            self.conn.their_state is h11.IDLE
            and self.transport.get_protocol() is self
            and not self.transport.is_closing()
        ):
            self._start_header_timer()
        else:
            self.header_started = None

    def _start_header_timer(self) -> None:
        now = self.loop.time()
        if self.header_started is None:
            self.header_started = now
        delay = min(_INACTIVITY_TIMEOUT, self.header_started + _HEADER_TIMEOUT - now)
        self.header_timer = self.loop.call_later(delay, self.transport.close)

    def _stop_header_timer(self) -> None:
        if self.header_timer is not None:
            self.header_timer.cancel()
            self.header_timer = None


class _IngressServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self) -> Iterator[None]:
        # The primary server owns this process's signal handlers.
        yield


@asynccontextmanager
async def serve_worker_ingress(app: FastAPI) -> AsyncIterator[None]:
    """Share the API lifespan, DB pools and remote-control host on another port."""
    server = _IngressServer(uvicorn.Config(
        WorkerIngress(app), host="0.0.0.0", port=PORT, lifespan="off",
        proxy_headers=False, log_config=None, access_log=False, http=_IngressHTTP, ws="websockets",
        ws_max_size=MAX_FRAME_BYTES, ws_max_queue=4, ws_per_message_deflate=False,
        timeout_graceful_shutdown=10,
    ))
    with socket.create_server(("0.0.0.0", PORT)) as listener:
        task = asyncio.create_task(server.serve(sockets=[listener]), name="worker-ingress")
        try:
            async with asyncio.timeout(10):
                while not server.started:
                    if task.done():
                        await task
                        raise RuntimeError("Worker ingress failed to start")
                    await asyncio.sleep(0.01)
            yield
        finally:
            server.should_exit = True
            await task


@asynccontextmanager
async def watch_worker_ingress(app: FastAPI) -> AsyncIterator[None]:
    """Open the dedicated socket when an installation is connected."""
    async def watch() -> None:
        while True:
            try:
                if await _installation_origin() is not None:
                    async with serve_worker_ingress(app):
                        absent = False
                        while True:
                            await asyncio.sleep(1)
                            try:
                                current_absent = await _installation_origin() is None
                            except (SQLAlchemyError, TimeoutError):
                                logger.warning("Could not refresh the worker ingress installation", exc_info=True)
                            else:
                                # A reconnect can occupy the singleton between
                                # polls. Keep its socket if the gap was brief.
                                if current_absent and absent:
                                    break
                                absent = current_absent
            except (SQLAlchemyError, OSError, TimeoutError, RuntimeError):
                logger.warning("Worker ingress is unavailable", exc_info=True)
            await asyncio.sleep(1)

    task = asyncio.create_task(watch(), name="worker-ingress-supervisor")
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
