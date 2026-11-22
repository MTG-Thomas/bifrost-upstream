"""A physical route can survive an API process without becoming a new run."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import delete

from src.models.enums import ExecutionStatus
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.worker.remote.remote_pool import ThinRemoteProcessPool
from src.worker.remote.ownership import RemoteOwnerLease


@pytest.mark.asyncio
async def test_adopts_live_and_unacknowledged_workflow_routes_with_original_metadata(
    async_session_factory, monkeypatch,
) -> None:
    @asynccontextmanager
    async def context():
        async with async_session_factory() as session:
            yield session

    monkeypatch.setattr("src.core.database.get_db_context", context)
    worker_id = uuid4()
    executions = [uuid4(), uuid4()]
    async with async_session_factory() as session:
        session.add(ExecutionWorker(
            id=worker_id, form="static", state="ready", resource_class="standard",
        ))
        session.add_all([Execution(
            id=subject, workflow_name="recovery", executed_by_name="test",
            status=ExecutionStatus.RUNNING,
        ) for subject in executions])
        await session.flush()
        session.add_all([ExecutionWorkerClaim(
            worker_id=worker_id, family="workflow_execution", subject_id=subject,
        ) for subject in executions])
        await session.commit()
    transport = AsyncMock()
    pool = ThinRemoteProcessPool(transport, worker_id=str(worker_id))
    pool._write_active_execution_lease = AsyncMock()
    entries = [{
        "route_id": "workflow:" + str(subject), "kind": "workflow", "subject_id": str(subject),
        "pid": 12345, "memory_request_bytes": 128 * 2**20,
        "started_at": datetime.now(timezone.utc).isoformat(), "timeout_seconds": 300,
        "active_execution": {"execution_id": str(subject), "workflow_id": None,
                             "workflow_name": "recovery", "org_id": None, "user_id": None,
                             "user_name": "test", "user_email": None, "sync": True,
                             "event": {"delivery_id": "keep-original-event"}},
    } for subject in executions]
    try:
        snapshot = await pool.reconcile_inventory({
            "version": 1, "routes": entries[:1], "terminal_routes": entries[1:],
        })
        assert snapshot["workflow_requests"] == {str(subject): 128 * 2**20 for subject in executions}
        assert len(pool.processes) == 2
        for handle in pool.processes.values():
            assert handle.pid is None
            assert handle.process.remote_pid == 12345
            assert handle.current_execution.active_execution["sync"] is True
            assert handle.current_execution.active_execution["event"] == {"delivery_id": "keep-original-event"}
        transport.request.assert_not_awaited()
    finally:
        async with async_session_factory() as session:
            await session.execute(delete(ExecutionWorker).where(ExecutionWorker.id == worker_id))
            await session.execute(delete(Execution).where(Execution.id.in_(executions)))
            await session.commit()


@pytest.mark.asyncio
async def test_inventory_cannot_adopt_a_workflow_claimed_by_another_worker(
    async_session_factory, monkeypatch,
) -> None:
    @asynccontextmanager
    async def context():
        async with async_session_factory() as session:
            yield session

    monkeypatch.setattr("src.core.database.get_db_context", context)
    transport = AsyncMock()
    pool = ThinRemoteProcessPool(transport, worker_id=str(uuid4()))
    route_id = "stale-route"
    snapshot = await pool.reconcile_inventory({"version": 1, "routes": [{
        "route_id": route_id, "kind": "workflow", "subject_id": str(uuid4()),
        "memory_request_bytes": 128 * 2**20,
    }], "terminal_routes": []})
    assert snapshot["workflow_requests"] == {}
    assert pool.processes == {}
    transport.request.assert_awaited_once()
    assert transport.request.await_args.args[0] == "terminate"
    assert transport.request.await_args.args[1]["route_id"] == route_id


@pytest.mark.parametrize("closed_terminal", [False, True])
async def test_managed_inventory_settles_slot_without_replaying_closed_results(
    async_session_factory, monkeypatch, closed_terminal: bool,
) -> None:
    worker_id, subject = uuid4(), uuid4()
    lease = RemoteOwnerLease(worker_id, uuid4(), uuid4(), 1)
    route_id = f"workflow:{subject}:{uuid4()}"
    entry = {
        "route_id": route_id, "kind": "workflow", "subject_id": str(subject),
        "memory_request_bytes": 1024, "started_at": datetime.now(timezone.utc).isoformat(),
        "timeout_seconds": 0, "workflow_deadline": None,
        "active_execution": {"execution_id": str(subject), "org_id": None, "sync": False},
        "runtime_handle": "fixture", "attempt_state": "stopped", "event_id": "terminal",
    }
    event = {**entry, "_event_id": "terminal", "result": {"success": False}}
    transport = SimpleNamespace(
        lease=lease, managed_cloudflare=True, admission=None,
        wait_for_opaque_terminals=AsyncMock(return_value={"terminal": event}), acknowledge=AsyncMock(),
    )
    callback = AsyncMock()
    pool = ThinRemoteProcessPool(
        transport, worker_id=str(worker_id), runtime_profile="cloudflare-python-v1", on_result=callback,
    )
    settle = AsyncMock()
    monkeypatch.setattr(pool, "_settle_workflow_slot", settle)
    monkeypatch.setattr(pool, "_owns_session", AsyncMock(return_value=True))
    monkeypatch.setattr(pool, "ownership_lost", AsyncMock())
    monkeypatch.setattr("src.worker.remote.fencing.fence_remote_owner", AsyncMock(return_value=True))
    monkeypatch.setattr("src.services.worker_pool.recovery.announce_lost_runs", AsyncMock())
    execution, _ = pool._opaque_inventory_execution(entry, subject)
    handle = pool._adopt_handle(route_id, entry, execution.started_at)
    handle.current_execution = execution
    pool._routes[route_id] = handle
    if not closed_terminal:
        handle.process.runtime_handle = None

    async with async_session_factory() as db:
        db.add(ExecutionWorker(id=worker_id, form="external", state="ready", runtime_profile="cloudflare-python-v1"))
        db.add(Execution(id=subject, workflow_name="fixture", executed_by_name="fixture",
                         status=ExecutionStatus.FAILED if closed_terminal else ExecutionStatus.RUNNING))
        await db.flush()
        db.add(ExecutionWorkerClaim(worker_id=worker_id, family="workflow_execution", subject_id=subject))
        await db.commit()
    try:
        snapshot = await pool._reconcile_opaque_inventory({
            "version": 1, "routes": [], "terminal_routes": [entry] if closed_terminal else [],
            "workflow_slots": 2 if closed_terminal else None,
        }, lease, SimpleNamespace(declared_slots=1, release=lambda _: None))
        if closed_terminal:
            assert snapshot is None
            transport.acknowledge.assert_awaited_once_with("terminal", reconciliation_lease=lease)
            callback.assert_not_awaited()
            settle.assert_awaited_once_with(route_id, "stopped", None)
        else:
            assert snapshot == {"workflow_requests": {}, "opaque_live_workflows": {}}
            assert callback.await_args.args[0]["error_type"] == "WorkflowAttemptUncertain"
            settle.assert_awaited_once_with(route_id, "uncertain")
    finally:
        async with async_session_factory() as db:
            await db.execute(delete(ExecutionWorker).where(ExecutionWorker.id == worker_id))
            await db.execute(delete(Execution).where(Execution.id == subject))
            await db.commit()
