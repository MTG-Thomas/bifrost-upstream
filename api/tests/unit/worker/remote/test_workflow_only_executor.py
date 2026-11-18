"""A workflow-only launcher can start and reconnect through the shared executor."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from src.services.work_dispatch import WAKE_WORKFLOW
from src.worker import pool_main
from src.worker.remote import host
from src.worker.remote.ownership import RemoteOwnerLease
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    ControlEnvelope,
)
from src.worker.remote.remote_pool import ThinRemoteProcessPool


@pytest.mark.asyncio
async def test_cloudflare_executor_starts_and_reconnects_with_workflow_only(monkeypatch) -> None:
    lease = RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 1)
    socket = Mock()
    handlers = (("workflow.execute", 1),)
    bridge = host.Bridge(socket, {
        "current": 1, "limit": 256 * 2**20, "hostname": "cloudflare-test",
        "handlers": handlers, "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    }, lease)
    worker = host.ApiExecutor(bridge, lease.worker_id)
    commands: list[str] = []
    ready = asyncio.Event()
    heartbeat = asyncio.Event()
    claims: asyncio.Queue = asyncio.Queue()
    wake_events: asyncio.Queue = asyncio.Queue()

    async def send(frame: str) -> None:
        command = ControlEnvelope.from_json(frame).payload
        commands.append(command["action"])
        await bridge.receive({"reply": command["id"], "result": {}})
        if command["action"] == "ready":
            ready.set()

    async def beat(*args, **kwargs) -> str:
        heartbeat.set()
        return "ready"

    async def claim(_worker_id, claimable, **kwargs) -> None:
        claims.put_nowait(claimable)

    async def listen(_redis_url, wakes) -> None:
        wake_events.put_nowait(wakes)
        await asyncio.Future()

    socket.send_text = AsyncMock(side_effect=send)
    database = Mock(commit=AsyncMock(), scalar=AsyncMock(return_value=lease.worker_id))

    @asynccontextmanager
    async def db_context():
        yield database

    async def pending_message(**kwargs):
        await asyncio.Future()

    pubsub = Mock(
        subscribe=AsyncMock(), unsubscribe=AsyncMock(), aclose=AsyncMock(),
        get_message=AsyncMock(side_effect=pending_message),
    )
    redis = Mock(
        hset=AsyncMock(), expire=AsyncMock(), delete=AsyncMock(),
        pubsub=Mock(return_value=pubsub),
    )
    monkeypatch.setattr("src.core.database.get_db_context", db_context)
    monkeypatch.setattr("src.jobs.consumers.workflow_execution.get_redis_client", Mock(return_value=redis))
    monkeypatch.setattr(ThinRemoteProcessPool, "_get_redis", AsyncMock(return_value=redis))
    monkeypatch.setattr("src.core.pubsub.publish_worker_heartbeat", AsyncMock())
    monkeypatch.setattr("src.core.pubsub.publish_worker_event", AsyncMock())
    monkeypatch.setattr("src.services.work_dispatch.listen_for_pool_wake", listen)
    monkeypatch.setattr(host, "report_worker_handlers", AsyncMock(return_value=True))
    monkeypatch.setattr(host, "owns_remote_session", AsyncMock(return_value=True))
    monkeypatch.setattr("src.worker.remote.ownership.owns_remote_session", AsyncMock(return_value=True))
    monkeypatch.setattr(host, "release_remote_owner", AsyncMock())
    monkeypatch.setattr(worker, "_idle_seconds", AsyncMock(return_value=3600))
    monkeypatch.setattr(pool_main, "begin_drain", AsyncMock())
    monkeypatch.setattr(pool_main, "retire_worker", AsyncMock())
    monkeypatch.setattr(pool_main, "remove_scheduler_replica", AsyncMock())
    monkeypatch.setattr(pool_main, "claim_next", claim)
    beat_mock = AsyncMock(side_effect=beat)
    monkeypatch.setattr(pool_main, "heartbeat_worker", beat_mock)
    diagnostics = AsyncMock()
    monkeypatch.setattr(pool_main, "replica_heartbeat_loop", diagnostics)
    unsupported = [
        (pool_main, "current_package_generation"),
        (worker, "_make_package_consumer"),
        (worker, "_package_loop"),
        (worker, "_make_agent_consumer"),
        (worker, "_start_service_claim_loop"),
        (worker, "_platform_job_loop"),
        (bridge, "reject_missing_platform_routes"),
    ]
    for target, method in unsupported:
        monkeypatch.setattr(target, method, AsyncMock())
    requirements_status = Mock()
    monkeypatch.setattr(ThinRemoteProcessPool, "_apply_requirements_status", requirements_status)
    requirements_notification = AsyncMock()
    monkeypatch.setattr("src.services.execution.process_pool._notify_requirements_failures", requirements_notification)
    calibration = AsyncMock()
    monkeypatch.setattr("src.services.worker_pool.calibration.record_memory_calibration", calibration)

    inventory = {"event": "inventory", "inventory": {
        "version": 1, "baseline_bytes": 1, "routes": [], "terminal_routes": [],
    }}
    session = host.RemoteSession(bridge, worker, lease, owned_until=time.monotonic() + 20)
    try:
        await asyncio.wait_for(heartbeat.wait(), timeout=2)
        assert beat_mock.await_args.kwargs["package_generation"] is None
        assert commands == []
        await bridge.receive(inventory)
        await asyncio.wait_for(ready.wait(), timeout=2)
        assert await asyncio.wait_for(claims.get(), timeout=2) == handlers
        assert worker._claims_open()
        assert database.commit.await_count == 1
        assert bridge.pool.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE

        bridge.disconnect(socket)
        reconnect = Mock(send_text=AsyncMock(side_effect=send))
        lease = replace(lease, session_generation=2)
        session.lease = session.lease_ref.lease = lease
        bridge.replace_socket(reconnect, lease)
        assert not worker._claims_open()
        await bridge.receive(inventory)
        await asyncio.wait_for(session.reconcile_transport_inventory(reconnect), timeout=2)
        assert worker._claims_open()
        assert database.commit.await_count == 2
        wakes = await asyncio.wait_for(wake_events.get(), timeout=2)
        wakes[WAKE_WORKFLOW].set()
        assert await asyncio.wait_for(claims.get(), timeout=2) == handlers

        # Package refresh has an independent entry point on the thin pool.
        await bridge.pool.update_packages()
        assert commands == ["configure", "ready"]
        assert all("packages" not in call.kwargs["mapping"] for call in redis.hset.await_args_list)
        pool_heartbeat = bridge.pool._build_heartbeat()
        assert "requirements_installed" not in pool_heartbeat
        assert "requirements_total" not in pool_heartbeat
        assert diagnostics.call_args.kwargs["runs_platform_jobs"] is False
        assert bridge.agents is worker._package_consumer is worker._service_claim_loop is None
        for target, method in unsupported:
            getattr(target, method).assert_not_called()
        requirements_status.assert_not_called()
        requirements_notification.assert_not_called()
        calibration.assert_not_called()
    finally:
        if worker._claim_ready:
            await asyncio.wait_for(session.stop(), timeout=2)
        else:
            await session.detach()
    assert await session.task == 0
    assert commands == ["configure", "ready", "retired"]
