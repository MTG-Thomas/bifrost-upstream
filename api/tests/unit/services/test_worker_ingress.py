"""Ingress policy and pre-dispatch boundaries."""

import re
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute, APIWebSocketRoute
from starlette.responses import RedirectResponse

from bifrost.cloudflare_analysis import _api_path_allowed
from src.services import worker_ingress as ingress
from src.services.execution.worker_sdk_http import build_worker_sdk_app


def test_versioned_allowlist_matches_sdk_and_registered_api_routes():
    from src.main import app

    engine_routes = {
        (method, route.path)
        for route in build_worker_sdk_app().routes if isinstance(route, APIRoute)
        for method in route.methods
        if not route.path.startswith(("/api/workflows", "/api/executions", "/api/sdk/runtime/"))
    }
    # Engine-local service/cache commands aren't remote workflow contracts.
    expected = engine_routes | {
        ("GET", "/api/agents"),
        ("GET", "/api/agents/{agent_id}"),
        ("POST", "/api/sdk/runtime/logs"),
        ("GET", "/api/sdk/runtime/logs/{execution_id}"),
        ("POST", "/auth/refresh"),
    }
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


@pytest.fixture
def endpoint():
    app = FastAPI()
    sdk = AsyncMock()
    origin = AsyncMock(return_value="https://workers.example.test")

    @app.get("/api/sdk/context")
    async def context():
        await sdk()
        return {"ok": True}

    @app.post("/api/sdk/config/get")
    async def config():
        return RedirectResponse("https://other.example.test")

    @app.get("/api/sdk/modules/{path:path}")
    async def module(path: str):
        return {"path": path}

    @app.get("/api/users/{user_id}")
    async def user(user_id: str):
        return {"user": user_id}

    @app.websocket(ingress.CONTROL_PATH)
    async def control(websocket):
        raise AssertionError("Matcher test must not invoke handler")

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
    ) as client:
        assert (await client.request("GET", "/api/sdk/context", content=b"12345")).status_code == 413
        origin.assert_not_awaited()
        assert (await client.request("GET", "/api/sdk/context", content=chunks())).status_code == 413
    sdk.assert_not_awaited()


@pytest.mark.asyncio
async def test_wrong_host_and_saturated_budget_prevent_dispatch(endpoint):
    app, sdk, origin = endpoint
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://workers.example.test",
    ) as client:
        wrong_host = await client.get("/api/sdk/context", headers={"Host": "api.example.test"})
        assert wrong_host.status_code == 404
        app.http.active = app.http.concurrency
        throttled = await client.get("/api/sdk/context")
        assert throttled.status_code == 429
        assert throttled.headers["retry-after"] == "1"
    sdk.assert_not_awaited()
    origin.assert_awaited_once()
