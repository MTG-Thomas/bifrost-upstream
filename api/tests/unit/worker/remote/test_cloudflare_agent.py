"""Cloudflare agent control requests and launch supervision."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import OperationalError

from src.services.authorization.callers import CallerUnavailable
from src.services.authorization.enforce import PermissionDenied
from src.services.memory_requests import AGENT_RUN_REQUEST_BYTES
from src.services.worker_pool.claims import PoolClaim
from src.worker.remote import cloudflare_agent as routes


@pytest.fixture
def control():
    transport = AsyncMock()
    return routes.CloudflareAgentRoutes(transport, uuid4(), "https://model.example.test")


@pytest.fixture
def route(control):
    route = routes.CloudflareAgentRoute(uuid4(), uuid4(), datetime.now(UTC), asyncio.Event(), str(uuid4()), "live")
    control.routes[route.id] = route
    return route


def request(route, **overrides):
    return {
        "route_id": route.id, "runtime_handle": "live", "request_id": "request", "operation": "commit",
        "data": {"sequence": 1}, **overrides,
    }


@pytest.mark.parametrize(("error", "result"), [
    (HTTPException(403, "workflows.execute required"), {"refused": True}),
    (PermissionDenied("workflows.execute"), {"refused": True}),
    (CallerUnavailable("caller removed"), {"refused": True}),
    (ConnectionError("claim lost"), {}),
])
async def test_rejected_request_receives_a_negative_reply(control, route, monkeypatch, error, result):
    monkeypatch.setattr(routes.cloudflare_agent, "commit_event", AsyncMock(side_effect=error))
    await control.handle_request(request(route), 7)
    control.transport.request.assert_awaited_once_with("cloudflare_agent_reply", {
        "route_id": route.id, "request_id": "request", "ok": False, "result": result,
    }, expected_generation=7)


async def test_unknown_and_settled_routes_are_ignored(control, route, monkeypatch):
    commit = AsyncMock()
    monkeypatch.setattr(routes.cloudflare_agent, "commit_event", commit)
    await control.handle_request(request(route, route_id="unknown"), 7)
    route.finished.set()
    await control.handle_request(request(route), 7)
    commit.assert_not_awaited()
    control.transport.request.assert_not_awaited()


async def test_invalid_handle_is_rejected_without_writing(control, route, monkeypatch):
    commit = AsyncMock()
    monkeypatch.setattr(routes.cloudflare_agent, "commit_event", commit)
    await control.handle_request(request(route, runtime_handle="wrong"), 7)
    commit.assert_not_awaited()
    control.transport.request.assert_awaited_once_with("cloudflare_agent_reply", {
        "route_id": route.id, "request_id": "request", "ok": False, "result": {},
    }, expected_generation=7)
    assert route.handle == "live"


@pytest.fixture
def launch(monkeypatch):
    claim = PoolClaim(uuid4(), "agent.run", {"claim_id": str(uuid4())}, uuid4(),
                      AGENT_RUN_REQUEST_BYTES, datetime.now(UTC))
    prepare = AsyncMock(return_value=True)
    settle = AsyncMock()
    monkeypatch.setattr(routes, "run_timeout_seconds", AsyncMock(return_value=30))
    monkeypatch.setattr(routes.cloudflare_agent, "prepare_run", prepare)
    monkeypatch.setattr(routes.cloudflare_agent, "settle_run", settle)
    return claim, prepare, settle


@pytest.mark.parametrize("error", [ConnectionError("old owner"), OperationalError(None, None, OSError("db"))])
async def test_preparation_failure_hands_back_the_claim(control, launch, error):
    claim, prepare, settle = launch
    prepare.side_effect = error
    await control.run(claim, 7)
    settle.assert_awaited_once_with(
        claim.subject_id, UUID(claim.message["claim_id"]), "not_started", {}, dispatch_id=claim.dispatch_id,
    )
    control.transport.request.assert_not_awaited()
    assert not control.routes


async def test_invalid_launch_reply_can_still_deliver_a_terminal(control, launch, monkeypatch):
    claim, _, settle = launch
    control.transport.request.return_value = {"pid": 1}

    async def supervise(active):
        await control.handle_result({"route_id": active.id, "runtime_handle": "live",
                                     "attempt_state": "completed", "result": {"success": True}})
        assert active.finished.is_set()

    monkeypatch.setattr(control, "supervise", supervise)
    await control.run(claim, 7)
    settle.assert_awaited_once_with(claim.subject_id, UUID(claim.message["claim_id"]), "completed", {"success": True})
    assert not control.routes
