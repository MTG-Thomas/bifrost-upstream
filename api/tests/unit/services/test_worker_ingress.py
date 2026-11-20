"""Ingress policy and pre-dispatch boundaries."""

import asyncio
import inspect
import re
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
import yaml
from fastapi import FastAPI, Request, WebSocket
from fastapi.routing import APIRoute, APIWebSocketRoute
from starlette.responses import RedirectResponse

from bifrost.cloudflare_analysis import _api_path_allowed
from bifrost import _runtime_io, client as sdk_client
from bifrost.commands import agents as agent_commands
from bifrost.executions import executions
from src.services import worker_ingress as ingress


def test_versioned_allowlist_matches_sdk_and_registered_api_routes():
    from src.main import app

    expected = {(method, path) for path, methods in ingress.POLICY["routes"].items() for method in methods}
    assert ingress.ROUTES == expected
    sample = "12345678-1234-1234-1234-123456789abc"
    assert all(
        _api_path_allowed(method, re.sub(r"\{[^}]+\}", sample, path))
        for method, path in expected
    )
    registered = [
        (method, route.path)
        for route in app.router.routes if isinstance(route, APIRoute)
        for method in route.methods
    ]
    assert all(registered.count(route) == 1 for route in expected)
    assert [
        route.path for route in app.router.routes
        if isinstance(route, APIWebSocketRoute) and route.path == ingress.CONTROL_PATH
    ] == ["/api/worker-control"]


@pytest.mark.parametrize(("caller", "method", "template"), [
    (agent_commands.list_agents.callback, "GET", "/api/agents"),
    (agent_commands.get_agent.callback, "GET", "/api/agents/{agent_id}"),
    (_runtime_io.write_log, "POST", "/api/sdk/runtime/logs"),
    (executions.get_current_logs, "GET", "/api/sdk/runtime/logs/{execution_id}"),
    (sdk_client._refresh_locked, "POST", "/auth/refresh"),
])
def test_runtime_contracts_have_callers(caller, method, template):
    source = inspect.getsource(caller)
    call = rf'(?:"{method}",\s*|\.{method.lower()}\(\s*)'
    path = re.sub(r"\\\{[^}]+\\\}", r"\\{\\w+\\}", re.escape(template))
    assert re.search(call + r'\s*f?"' + path + r'"', source, re.IGNORECASE)
    assert (method, template) in ingress.ROUTES


@pytest.mark.parametrize("path", [
    "/app/docker-compose.yml", "/k8s/kustomization.yml", "/k8s/api/deployment.yaml", "/k8s/api/worker-ingress-service.yaml",
])
def test_ingress_manifest_yaml_has_no_duplicate_keys(path):
    def check(node):
        if isinstance(node, yaml.MappingNode):
            keys = [key.value for key, _ in node.value]
            assert len(keys) == len(set(keys))
            for _, value in node.value:
                check(value)
        elif isinstance(node, yaml.SequenceNode):
            for value in node.value:
                check(value)

    document = Path(path).read_text()
    check(yaml.compose(document))
    parsed = yaml.safe_load(document)
    if path.endswith("/deployment.yaml"):
        assert {entry["name"]: entry["containerPort"] for entry in parsed["spec"]["template"]["spec"]["containers"][0]["ports"]} == {
            "http": 8000, "worker-ingress": 8001,
        }


@pytest.fixture
def endpoint(monkeypatch):
    app = FastAPI()
    sdk = AsyncMock()
    origin = AsyncMock(return_value="https://workers.example.test")
    monkeypatch.setattr(ingress, "decode_token", lambda token, **_: {"sub": token} if token.startswith("valid-") else None)

    @app.get("/api/sdk/context")
    async def context():
        await sdk()
        return {"ok": True}

    @app.get("/api/agents")
    async def forwarded(request: Request):
        return dict(request.headers)

    @app.post("/api/sdk/config/get")
    async def config():
        return RedirectResponse("https://other.example.test")

    @app.post("/auth/refresh")
    async def refresh():
        return {"ok": True}

    @app.get("/api/sdk/modules/{path:path}")
    async def module(path: str):
        return {"path": path}

    @app.get("/api/users/{user_id}")
    async def user(user_id: str):
        return {"user": user_id}

    @app.post("/api/users")
    async def add_user():
        await sdk()
        return {"ok": True}

    @app.websocket(ingress.CONTROL_PATH)
    async def control(websocket: WebSocket):
        await websocket.accept()
        launcher = await websocket.receive_text()
        if websocket.scope["worker_ingress_authenticate"](launcher):
            await websocket.send_text("welcome")
            await websocket.receive_text()
        else:
            await websocket.close(code=1008)

    return ingress.WorkerIngress(app, origin=origin), sdk, origin


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "path", "status"), [
    ("GET", "/api/sdk/context", 200),
    ("HEAD", "/api/sdk/context", 404),
    ("POST", "/api/sdk/context", 404),
    ("GET", "/api/sdk/context/", 404),
    ("GET", "/api/sdk//context", 404),
    ("GET", "/api/sdk%2Fcontext", 404),
    ("GET", "/api/sdk/%63ontext", 404),
    ("GET", "/api/sdk/modules/pkg%2F..%2Fmodule.py", 404),
    ("GET", "/api/sdk/modules/pkg//module.py", 404),
    ("GET", "/api/sdk/modules/pkg%252Fmodule.py", 404),
    ("GET", "/api/sdk/modules/pkg/%2e%2e/module.py", 404),
    ("GET", "/api/sdk/modules/pkg%5Cmodule.py", 404),
    ("GET", "/api/sdk/modules/pkg/requirements.txt", 404),
    ("GET", "/api/sdk/modules/pkg%2Fmodule.py", 200),
    ("GET", "/api/sdk/modules/pkg/module.py", 200),
    ("GET", "/api/users/me", 404),
    ("GET", "/api/users/someone%40example.test", 200),
    ("POST", "/api/sdk/config/get", 404),
    ("GET", "/openapi.json", 404),
])
async def test_method_and_path(endpoint, method, path, status):
    app, _, _ = endpoint
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://workers.example.test",
        headers={"Authorization": "Bearer valid-one"},
    ) as client:
        response = await client.request(method, path)
    assert response.status_code == status
    assert "location" not in response.headers


def test_websocket_requires_exact_scope_and_upgrade(endpoint):
    app, _, _ = endpoint
    scope = {
        "type": "websocket", "path": ingress.CONTROL_PATH, "raw_path": ingress.CONTROL_PATH.encode(),
        "headers": [(b"connection", b"keep-alive, Upgrade"), (b"upgrade", b"WebSocket")],
    }
    assert ingress.allows_request(app.app, scope)
    assert not ingress.allows_request(app.app, {**scope, "headers": [(b"upgrade", b"websocket")]})
    assert not ingress.allows_request(app.app, {**scope, "headers": [(b"connection", b"upgrade")]})
    assert not ingress.allows_request(app.app, {**scope, "type": "http", "method": "GET"})
    assert not ingress.allows_request(app.app, {
        **scope, "path": "/api/sdk/context", "raw_path": b"/api/sdk/context",
    })


def test_literal_dot_segment_is_refused_before_dispatch(endpoint):
    app, _, _ = endpoint
    path = "/api/sdk/modules/pkg/../module.py"
    assert not ingress.allows_request(app.app, {
        "type": "http", "method": "GET", "path": path, "raw_path": path.encode(), "headers": [],
    })


@pytest.mark.asyncio
async def test_body_limits_prevent_dispatch(endpoint, monkeypatch):
    app, sdk, origin = endpoint
    monkeypatch.setattr(ingress, "_BODY_LIMIT", 4)

    async def chunks():
        yield b"12"
        yield b"345"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://workers.example.test",
        headers={"Authorization": "Bearer valid-one"},
    ) as client:
        assert (await client.request("GET", "/api/sdk/context", content=b"12345")).status_code == 413
        assert (await client.request("GET", "/api/sdk/context", content=chunks())).status_code == 413
    origin.assert_awaited_once()
    sdk.assert_not_awaited()


@pytest.mark.asyncio
async def test_wrong_host_and_saturated_budget_prevent_dispatch(endpoint):
    app, sdk, origin = endpoint
    app.http.concurrency = 0
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://workers.example.test",
        headers={"Authorization": "Bearer valid-one"},
    ) as client:
        wrong_host = await client.get("/api/sdk/context", headers={"Host": "api.example.test"})
        assert wrong_host.status_code == 404
        assert not app.http.budgets
        assert (await client.get("/openapi.json")).status_code == 404
        assert not app.http.budgets
        throttled = await client.get("/api/sdk/context")
        assert throttled.status_code == 429
        assert throttled.headers["retry-after"] == "1"
    sdk.assert_not_awaited()
    origin.assert_awaited_once()


@pytest.mark.asyncio
async def test_spoofable_forwarding_headers_are_removed(endpoint):
    app, _, _ = endpoint
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://workers.example.test",
    ) as client:
        response = await client.get("/api/agents", headers={
            "Authorization": "Bearer valid-one", "Forwarded": "host=spoof.invalid",
            "X-Forwarded-Host": "spoof.invalid", "X-Forwarded-For": "192.0.2.1",
            "X-Forwarded-Extra": "other", "X-Real-IP": "192.0.2.2",
        })
        assert response.status_code == 200
        assert not {"forwarded", "x-forwarded-host", "x-forwarded-for", "x-forwarded-extra", "x-real-ip"} & response.json().keys()


@pytest.mark.asyncio
async def test_sensitive_mutations_require_workflow_run_credentials(endpoint, monkeypatch):
    app, sdk, _ = endpoint
    monkeypatch.setattr(ingress, "decode_token", lambda token, **_: {
        "sub": token, "credential_kind": "workflow_execution" if token == "run" else "human",
    })
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://workers.example.test",
    ) as client:
        assert (await client.post("/api/users", headers={"Authorization": "Bearer human"})).status_code == 403
        sdk.assert_not_awaited()
        assert (await client.post("/api/users", headers={"Authorization": "Bearer run"})).status_code == 200
    sdk.assert_awaited_once()


@pytest.mark.asyncio
async def test_probe_requires_nonce_from_this_request():
    from src.services.worker_ingress_probes import _check_route

    observed = []

    def stale_marker(request):
        observed.append(request.headers[ingress.PROBE_HEADER.decode()])
        return httpx.Response(404, headers={
            ingress.INGRESS_HEADER: ingress.DENY_MARKER, ingress.PROBE_HEADER: observed[0],
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(stale_marker)) as client:
        first = await _check_route(client, "https://proxy.test", "workers.example.test", "GET", "/docs", allowed=False)
        second = await _check_route(client, "https://proxy.test", "workers.example.test", "GET", "/docs", allowed=False)
    assert first.passed and not second.passed
    assert observed[0] != observed[1]


@pytest.mark.asyncio
async def test_slow_requests_are_isolated_by_verified_subject(endpoint, monkeypatch):
    app, _, _ = endpoint
    app.http.concurrency = 1
    monkeypatch.setattr(ingress, "_INACTIVITY_TIMEOUT", 0.05)
    waiting = asyncio.Event()

    async def slow():
        yield b"a"
        waiting.set()
        await asyncio.Future()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://workers.example.test",
    ) as client:
        path = "/api/sdk/context"
        large = {"Content-Length": str(4 * 1024 * 1024)}
        assert (await client.post("/api/sdk/config/get", content=slow(), headers=large)).status_code == 401
        assert not waiting.is_set()
        first = asyncio.create_task(client.post("/api/sdk/config/get", content=slow(), headers={
            **large, "Authorization": "Bearer valid-one",
        }))
        await waiting.wait()
        assert (await client.get(path, headers={"Authorization": "Bearer valid-one"})).status_code == 429
        assert (await client.get(path, headers={"Authorization": "Bearer valid-two"})).status_code == 200
        assert (await first).status_code == 408
        waiting.clear()
        app.refresh.concurrency = 1
        refresh = asyncio.create_task(client.post("/auth/refresh", content=slow()))
        await waiting.wait()
        assert (await client.post("/auth/refresh", json={})).status_code == 429
        assert (await client.get(path, headers={"Authorization": "Bearer valid-one"})).status_code == 200
        assert (await refresh).status_code == 408


@pytest.mark.asyncio
async def test_origin_cache_expires_and_checks_new_host(endpoint, monkeypatch):
    app, _, origin = endpoint
    now = 10.0
    monkeypatch.setattr(ingress, "monotonic", lambda: now)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://workers.example.test",
    ) as client:
        path = "/api/sdk/context"
        assert (await client.get(path)).status_code == 401
        origin.return_value = "https://other.example.test"
        assert (await client.get(path)).status_code == 401
        origin.assert_awaited_once()
        now += ingress._ORIGIN_TTL
        assert (await client.get(path)).status_code == 404
    assert origin.await_count == 2


@pytest.mark.asyncio
async def test_signed_sockets_release_pending_slots_and_silent_hello_times_out(endpoint, monkeypatch):
    app, _, _ = endpoint
    app.pending_hello.concurrency = 1
    app.launchers.concurrency = 1
    monkeypatch.setattr(ingress, "_HELLO_TIMEOUT", 0.05)
    scope = {
        "type": "websocket", "path": ingress.CONTROL_PATH, "raw_path": ingress.CONTROL_PATH.encode(),
        "headers": [(b"host", b"workers.example.test"), (b"connection", b"upgrade"), (b"upgrade", b"websocket")],
        "extensions": {"websocket.http.response": {}}, "subprotocols": [], "query_string": b"",
    }

    def socket():
        incoming = asyncio.Queue()
        outgoing = asyncio.Queue()
        incoming.put_nowait({"type": "websocket.connect"})
        task = asyncio.create_task(app(scope, incoming.get, outgoing.put))
        async def next_event():
            try:
                return await asyncio.wait_for(outgoing.get(), 1)
            except TimeoutError:
                if task.done():
                    await task
                raise

        return task, incoming, next_event

    signed, frames, events = socket()
    assert (await events())["type"] == "websocket.accept"
    launcher = str(uuid4())
    frames.put_nowait({"type": "websocket.receive", "text": launcher})
    assert (await events())["text"] == "welcome"
    assert app.pending_hello.active == 0

    silent, _, stalled = socket()
    assert (await stalled())["type"] == "websocket.accept"
    rejected, _, refusal = socket()
    assert (await refusal())["status"] == 429
    await rejected
    assert (await stalled())["code"] == 1008
    await silent
    assert app.pending_hello.active == 0 and not signed.done()

    duplicate, duplicate_frames, duplicate_events = socket()
    assert (await duplicate_events())["type"] == "websocket.accept"
    duplicate_frames.put_nowait({"type": "websocket.receive", "text": launcher})
    assert (await duplicate_events())["code"] == 1008
    await duplicate

    independent, independent_frames, independent_events = socket()
    assert (await independent_events())["type"] == "websocket.accept"
    independent_frames.put_nowait({"type": "websocket.receive", "text": str(uuid4())})
    assert (await independent_events())["text"] == "welcome"
    assert app.pending_hello.active == 0
    independent_frames.put_nowait({"type": "websocket.receive", "text": ""})
    await independent
    frames.put_nowait({"type": "websocket.receive", "text": ""})
    await signed
