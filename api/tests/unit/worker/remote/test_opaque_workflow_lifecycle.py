"""Cloudflare workflows retain their canonical claim until fenced settlement."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, select, update
from src.models.enums import ExecutionStatus
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.models.orm.work_dispatches import WorkDispatch
from src.routers.platform import workers
from src.services.work_dispatch import WAKE_WORKFLOW
from src.worker.admission import Admission
from src.worker.remote import remote_pool
from src.worker.remote.host import ApiExecutor, Bridge
from src.worker.remote.ownership import RemoteOwnerLease
from src.worker.remote.protocol import CLOUDFLARE_PYTHON_RUNTIME_PROFILE, ProtocolError

PROFILE = CLOUDFLARE_PYTHON_RUNTIME_PROFILE
RESERVATION = 32 * 2**20


@pytest.fixture
async def opaque_executor(async_session_factory, monkeypatch):
    lease = RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 2)
    bridge = Bridge(Mock(close=AsyncMock()), {
        "hostname": "opaque", "current": 1, "limit": 256 * 2**20,
        "runtime_profile": PROFILE, "handlers": (("workflow.execute", 1),),
    }, lease)
    worker = ApiExecutor(bridge, lease.worker_id)
    pool = worker._make_workflow_consumer()._pool
    worker.admission = bridge.admission = Admission(
        budget_bytes=128 * 2**20, under_pressure=lambda: False,
        running_workflow_ids=pool.active_execution_ids, workflow_growth=pool.workflow_growth,
        claims_open=worker._claims_open,
    )
    monkeypatch.setattr(bridge, "request", AsyncMock())
    monkeypatch.setattr(bridge, "acknowledge", AsyncMock())
    monkeypatch.setattr(pool, "_write_context_to_redis", AsyncMock())
    monkeypatch.setattr(pool, "_write_active_execution_lease", AsyncMock())
    pool.on_result = AsyncMock()

    @asynccontextmanager
    async def db_context():
        async with async_session_factory() as db:
            yield db

    monkeypatch.setattr("src.core.database.get_db_context", db_context)
    monkeypatch.setattr("src.worker.remote.ownership.get_liveness_db_context", db_context)
    monkeypatch.setattr("src.services.worker_pool.recovery.announce_lost_runs", AsyncMock())
    subjects = [uuid4(), uuid4()]
    async with async_session_factory() as db:
        db.add(ExecutionWorker(
            id=lease.worker_id, form="external", state="ready", runtime_profile=PROFILE,
            remote_launcher_id=lease.launcher_id, remote_owner_id=lease.owner_id,
            remote_session_generation=lease.session_generation,
            remote_owner_lease_expires_at=datetime.now(UTC) + timedelta(minutes=10),
        ))
        for subject in subjects:
            db.add(Execution(
                id=subject, workflow_name="opaque", executed_by_name="test",
                status=ExecutionStatus.RUNNING,
            ))
            db.add(ExecutionWorkerClaim(worker_id=lease.worker_id, subject_id=subject, family=WAKE_WORKFLOW))
        await db.commit()

    try:
        yield worker, pool, subjects
    finally:
        await pool.ownership_lost()
        async with async_session_factory() as db:
            await db.execute(delete(ExecutionWorker).where(ExecutionWorker.id == lease.worker_id))
            await db.execute(delete(Execution).where(Execution.id.in_(subjects)))
            await db.commit()


def workflow_entry(subject, *, expired=False):
    started_at = datetime.now(UTC) - timedelta(seconds=301 if expired else 0)
    return {
        "kind": "workflow", "route_id": f"workflow:{subject}:{uuid4()}", "subject_id": str(subject),
        "runtime_handle": f"invocation:{uuid4()}", "memory_request_bytes": RESERVATION,
        "started_at": started_at.isoformat(), "timeout_seconds": 300,
        "workflow_deadline": (started_at + timedelta(seconds=300)).isoformat(),
        "active_execution": {
            "execution_id": str(subject), "sync": True, "event": None,
            "workflow_name": "opaque", "workflow_id": None, "org_id": None,
            "user_id": None, "user_name": "test", "user_email": None,
        },
    }


def inventory(live=(), terminals=()):
    return {"event": "inventory", "inventory": {
        "version": 1, "baseline_bytes": 1, "routes": list(live), "terminal_routes": list(terminals),
    }}


def terminal_event(entry, *, success=True):
    return {
        "event": "result", "route_id": entry["route_id"], "runtime_handle": entry["runtime_handle"],
        "_event_id": entry["event_id"], "attempt_state": entry["attempt_state"],
        "result": {"success": success, "result": "output"} if success else {
            "success": False, "error_type": "CancelledError", "error": "Stop requested",
        },
    }


@pytest.mark.asyncio
async def test_lost_launch_ack_recovers_the_same_opaque_route(opaque_executor):
    worker, pool, (subject, other_subject) = opaque_executor
    bridge = worker.bridge
    bridge.request.side_effect = TimeoutError
    worker.admission.hold(str(subject), RESERVATION, workflow=True)
    entry = workflow_entry(subject)
    await pool.route_execution(str(subject), {"timeout_seconds": 300}, entry["active_execution"])
    launch = bridge.request.await_args
    dispatched = launch.args[1]
    assert launch.kwargs["expected_generation"] == bridge.lease.session_generation
    assert dispatched["workflow_deadline"] == dispatched["context"]["workflow_deadline"]
    pending = pool._build_heartbeat()
    assert pending["processes"] == []
    assert pending["busy_count"] == 1
    assert pending["runtime_invocations"][0]["runtime_handle"] is None

    entry.update({key: dispatched[key] for key in ("route_id", "started_at", "workflow_deadline")})
    await bridge.receive(inventory([entry, workflow_entry(other_subject)]))
    await worker._prepare_admission()
    handle = pool.processes[entry["route_id"]]
    assert handle.process.runtime_handle == entry["runtime_handle"]
    assert handle.pid is handle.process.remote_pid is None
    assert worker._claims_open()
    bridge.request.assert_awaited_once()
    pool.on_result.assert_not_awaited()


@pytest.mark.asyncio
async def test_takeover_adopts_live_and_replays_retained_terminal(opaque_executor):
    worker, pool, subjects = opaque_executor
    live, terminal = map(workflow_entry, subjects)
    terminal.update(event_id="retained-result", attempt_state="completed")
    await worker.bridge.receive(terminal_event(terminal))
    pool.on_result.assert_not_awaited()
    assert not worker._claims_open()

    await worker.bridge.receive(inventory([live], [terminal]))
    await worker._prepare_admission()
    await asyncio.gather(*worker.bridge.results)

    assert worker._claims_open()
    assert worker.admission.held_bytes() == RESERVATION
    assert list(pool.processes) == [live["route_id"]]
    handle = pool.processes[live["route_id"]]
    assert pool._opaque_deadline(handle.current_execution).isoformat() == live["workflow_deadline"]
    pool.on_result.assert_awaited_once_with({
        "execution_id": str(subjects[1]), "sync": True, "success": True, "result": "output",
    })
    worker.bridge.acknowledge.assert_awaited_once_with(terminal["event_id"])
    worker.bridge.request.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("reconnect", [False, True], ids=["takeover", "reconnect"])
async def test_omitted_claim_fails_fenced_worker_without_redispatch(
    opaque_executor, async_session_factory, reconnect,
):
    worker, pool, subjects = opaque_executor
    bridge = worker.bridge
    live, other = map(workflow_entry, subjects)
    if reconnect:
        await bridge.receive(inventory([live, other]))
        await worker._prepare_admission()
        lease = replace(bridge.lease, session_generation=bridge.lease.session_generation + 1)
        async with async_session_factory() as db:
            await db.execute(update(ExecutionWorker).where(ExecutionWorker.id == lease.worker_id).values(
                remote_session_generation=lease.session_generation,
            ))
            await db.commit()
        bridge.replace_socket(Mock(), lease)
        # Both subjects appear, but the original in-memory route is missing.
        await bridge.receive(inventory([live, workflow_entry(subjects[1])]))
        await worker.reconcile_transport_inventory(expected_generation=lease.session_generation)
    else:
        await bridge.receive(inventory([live]))
        await worker._prepare_admission()

    assert not worker._claims_open()
    assert pool.active_execution_ids() == set()
    bridge.request.assert_not_awaited()
    pool.on_result.assert_not_awaited()
    async with async_session_factory() as db:
        row = await db.get(ExecutionWorker, bridge.lease.worker_id)
        assert row.state == "lost"
        assert row.end_reason == "opaque_inventory_mismatch"
        assert set(await db.scalars(select(Execution.status).where(Execution.id.in_(subjects)))) == {
            ExecutionStatus.FAILED,
        }
        assert not (await db.scalars(select(WorkDispatch.id).where(WorkDispatch.subject_id.in_(subjects)))).all()


@pytest.mark.asyncio
@pytest.mark.parametrize(("reason", "reply"), [
    ("cancel", "missing"), ("deadline", "missing"), ("cancel", "received"), ("cancel", "ambiguous"),
])
async def test_cooperative_stop_waits_for_terminal(
    opaque_executor, async_session_factory, monkeypatch, reason, reply,
):
    worker, pool, (subject, other_subject) = opaque_executor
    bridge = worker.bridge
    if reason == "cancel":
        async with async_session_factory() as db:
            await db.execute(update(Execution).where(Execution.id == subject).values(
                status=ExecutionStatus.CANCELLING,
            ))
            await db.commit()
    entry = workflow_entry(subject, expired=reason == "deadline")

    async def no_reply(*args, **kwargs):
        await asyncio.Future()

    owner_check = AsyncMock(return_value=True)
    monkeypatch.setattr("src.worker.remote.ownership.owns_remote_session", owner_check)
    if reply == "missing":
        monkeypatch.setattr(remote_pool, "OPAQUE_STOP_TIMEOUT_SECONDS", 0)
        bridge.request.side_effect = no_reply
    else:
        bridge.request.return_value = {"received": reply == "received"}

    await bridge.receive(inventory([entry, workflow_entry(other_subject)]))
    await worker._prepare_admission()
    await pool.processes[entry["route_id"]].process.stop_task

    error_type = "CancelledError" if reason == "cancel" else "TimeoutError"
    stop = bridge.request.await_args
    assert stop.args[0] == "terminate"
    assert stop.args[1]["route_id"] == entry["route_id"]
    assert stop.args[1]["error_type"] == error_type
    assert stop.kwargs["expected_generation"] == bridge.lease.session_generation
    owner_check.assert_awaited_once_with(bridge.lease)
    await pool._handle_cancel_request(str(subject))
    bridge.request.assert_awaited_once()
    assert worker.admission.held_bytes() == 2 * RESERVATION
    pool.on_result.assert_not_awaited()

    entry.update(event_id="settled-stop", attempt_state="uncertain")
    await bridge.receive(terminal_event(entry, success=False))
    await asyncio.gather(*bridge.results)
    assert worker.admission.held_bytes() == RESERVATION
    assert pool.on_result.await_args.args[0]["error_type"] == "WorkflowAttemptUncertain"
    bridge.acknowledge.assert_awaited_once_with(entry["event_id"])


@pytest.mark.asyncio
async def test_late_terminal_cannot_commit_after_owner_replaced(opaque_executor, async_session_factory):
    worker, pool, subjects = opaque_executor
    live, other = map(workflow_entry, subjects)
    await worker.bridge.receive(inventory([live, other]))
    await worker._prepare_admission()
    async with async_session_factory() as db:
        await db.execute(update(ExecutionWorker).where(ExecutionWorker.id == worker.bridge.lease.worker_id).values(
            remote_owner_id=uuid4(), remote_session_generation=worker.bridge.lease.session_generation + 1,
        ))
        await db.commit()

    live.update(event_id="late-result", attempt_state="completed")
    await worker.bridge.receive(terminal_event(live))
    with pytest.raises(ConnectionError, match="ownership was replaced"):
        await asyncio.gather(*worker.bridge.results)
    pool.on_result.assert_not_awaited()
    worker.bridge.acknowledge.assert_not_awaited()


@pytest.mark.asyncio
async def test_opaque_bridge_requires_inventory_v1():
    bridge = Bridge(Mock(), {"hostname": "opaque", "current": 1, "limit": 2, "runtime_profile": PROFILE},
                    RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 1))
    message = inventory()
    message["inventory"]["version"] = 2
    with pytest.raises(ProtocolError, match="inventory v1"):
        await bridge.receive(message)
    assert not bridge.inventory_ready.is_set()


@pytest.mark.asyncio
async def test_pid_recycle_rejects_opaque_pool(monkeypatch):
    redis = Mock(exists=AsyncMock(return_value=True), hget=AsyncMock(return_value=PROFILE), publish=AsyncMock())
    monkeypatch.setattr(workers, "_get_redis", AsyncMock(return_value=redis))

    with pytest.raises(HTTPException, match="no OS processes") as exc:
        await workers.recycle_process("opaque", 1234, Mock())
    assert exc.value.status_code == 404
    redis.publish.assert_not_awaited()
