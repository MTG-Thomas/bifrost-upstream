"""Restricted, proxy-facing ingress for external workflow runtimes."""

import asyncio
import json
import re
import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from time import monotonic
from urllib.parse import quote, unquote_to_bytes

import uvicorn
from fastapi import FastAPI
from fastapi.routing import APIRoute, APIWebSocketRoute
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from starlette.routing import Match
from starlette.types import Message, Receive, Scope, Send

from src.core.database import get_session_factory
from src.core.security import decode_token
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.worker.remote.protocol import MAX_FRAME_BYTES

PORT = 8001
INGRESS_HEADER = b"x-bifrost-worker-ingress"
POLICY = json.loads(Path(__file__).with_name("worker_ingress_routes.json").read_text())
CONTROL_PATH: str = POLICY["control"]
ROUTES = {(method, path) for path, methods in POLICY["routes"].items() for method in methods}
ALLOW_MARKER = f'v{POLICY["version"]}; allowed'.encode()
DENY_MARKER = f'v{POLICY["version"]}; denied'.encode()
_SDK_IDENTIFIERS = json.loads(
    (Path(__file__).resolve().parents[2] / "bifrost" / "cloudflare_sdk_routes.json").read_text()
)["identifiers"]
_UUID = re.compile(_SDK_IDENTIFIERS["uuid"])
_EMAIL = re.compile(_SDK_IDENTIFIERS["email"].replace("%5[Ee]", r"\^").replace("(?:@|%40)", "@"))
_UUID_PARAMETERS = frozenset({"org_id", "role_id", "form_id", "agent_id", "run_id", "job_id", "execution_id"})
_SOURCE_PREFIX = "/api/sdk/modules/"
_ENCODED_BYTE = re.compile(rb"%([0-9a-fA-F]{2})")
_UNRESERVED = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
_BODY_LIMIT = 4 * 1024 * 1024
_BODY_TIMEOUT = 10


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
    headers = [(INGRESS_HEADER, ALLOW_MARKER if allowed else DENY_MARKER), (b"cache-control", b"no-store")]
    if status == 429:
        headers.append((b"retry-after", b"1"))
    if scope["type"] == "http":
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


async def _installation_origin() -> str | None:
    async with get_session_factory()() as db:
        return await db.scalar(select(CloudflareInstallation.worker_ingress_origin).where(
            CloudflareInstallation.retired_at.is_(None)
        ))


def _is_run_refresh(body: bytes) -> bool:
    try:
        payload = json.loads(body)
    except (ValueError, RecursionError):
        return False
    token = payload.get("refresh_token") if isinstance(payload, dict) else None
    return isinstance(token, str) and decode_token(token, expected_type="run_refresh") is not None


class WorkerIngress:
    def __init__(
        self, app: FastAPI, *, origin: Callable[[], Awaitable[str | None]] = _installation_origin,
    ) -> None:
        self.app = app
        self.origin = origin
        # Bound aggregate load per API replica independently of proxy-supplied
        # identity or whether many workers share one egress address.
        self.http = _RequestBudget(200, 32)
        self.sockets = _RequestBudget(10, 64)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        budget = self.sockets if scope["type"] == "websocket" else self.http
        if not budget.take():
            await _respond(scope, send, 429)
            return
        try:
            await self._serve(scope, receive, send)
        finally:
            budget.active -= 1

    async def _serve(self, scope: Scope, receive: Receive, send: Send) -> None:
        host = _header_values(scope, b"host")
        if len(host) != 1 or not allows_request(self.app, scope):
            await _respond(scope, send, 404)
            return
        length = _content_length(scope)
        limit = 8192 if scope["path"] == "/auth/refresh" else _BODY_LIMIT
        if length < 0 or length > limit:
            await _respond(scope, send, 413)
            return
        try:
            async with asyncio.timeout(3):
                origin = await self.origin()
        except (TimeoutError, SQLAlchemyError):
            await _respond(scope, send, 503)
            return
        if origin is None or b"https://" + host[0].lower() != origin.rstrip("/").encode():
            await _respond(scope, send, 404)
            return
        # TLS terminates at the operator proxy. The separate listener ignores
        # forwarded headers and uses the verified Host with the public scheme.
        scope = {**scope, "scheme": "https" if scope["type"] == "http" else "wss"}
        if scope["type"] == "websocket":
            await self.app(scope, receive, send)
            return
        body = bytearray()
        try:
            async with asyncio.timeout(_BODY_TIMEOUT):
                while True:
                    event = await receive()
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
        replay: bytes | None = bytes(body)
        del body
        if scope["path"] == "/auth/refresh" and not _is_run_refresh(replay):
            await _respond(scope, send, 401, allowed=True)
            return

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
                    if key.lower() not in {INGRESS_HEADER, b"cache-control"}
                ]
                message = {**message, "headers": headers + [
                    (INGRESS_HEADER, ALLOW_MARKER), (b"cache-control", b"no-store"),
                ]}
            if not redirected:
                await send(message)

        await self.app(scope, replay_body, send_guarded)


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
        proxy_headers=False, log_config=None, access_log=False, ws="websockets",
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
