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
from src.repositories.executions import ExecutionRepository
from src.routers.platform import workers
from src.services.work_dispatch import WAKE_WORKFLOW
from src.services.worker_pool import recovery
from src.worker.admission import Admission
from src.worker.remote import remote_pool
from src.worker.remote.host import ApiExecutor, Bridge
from src.worker.remote.ownership import RemoteOwnerLease
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    MAX_FRAME_BYTES,
    ControlEnvelope,
    CredentialRouteChanged,
    ProtocolError,
)

PROFILE = CLOUDFLARE_PYTHON_RUNTIME_PROFILE
RESERVATION = 32 * 2**20


def opaque_bridge(socket, lease):
    return Bridge(socket, {
        "hostname": "opaque", "current": 1, "limit": 256 * 2**20,
        "runtime_profile": PROFILE, "handlers": (("workflow.execute", 1),),
    }, lease)


@pytest.fixture
async def opaque_executor(async_session_factory, monkeypatch):
    lease = RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 2)
    bridge = opaque_bridge(Mock(close=AsyncMock()), lease)
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
    monkeypatch.setattr(pool, "_get_redis", AsyncMock(return_value=Mock(delete=AsyncMock())))
    pool.on_result = AsyncMock()

    @asynccontextmanager
    async def db_context():
        async with async_session_factory() as db:
            yield db

    monkeypatch.setattr("src.core.database.get_db_context", db_context)
    monkeypatch.setattr("src.worker.remote.ownership.get_liveness_db_context", db_context)
    monkeypatch.setattr(recovery, "announce_lost_runs", AsyncMock())
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


async def receive_reconnect(bridge, async_session_factory, snapshot, *, socket=None):
    lease = replace(bridge.lease, session_generation=bridge.lease.session_generation + 1)
    async with async_session_factory() as db:
        await db.execute(update(ExecutionWorker).where(ExecutionWorker.id == lease.worker_id).values(
            remote_session_generation=lease.session_generation,
        ))
        await db.commit()
    bridge.replace_socket(socket if socket is not None else Mock(), lease)
    await bridge.receive(snapshot)
    return lease.session_generation


async def prepare_sibling_then_claim(worker, subject, sibling, async_session_factory):
    async with async_session_factory() as db:
        await db.execute(delete(ExecutionWorkerClaim).where(ExecutionWorkerClaim.subject_id == subject))
        await db.commit()
    await worker.bridge.receive(inventory([sibling]))
    await worker._prepare_admission()
    assert worker._claims_open()
    worker.admission.hold(str(subject), RESERVATION, workflow=True)
    async with async_session_factory() as db:
        db.add(ExecutionWorkerClaim(worker_id=worker.incarnation_id, subject_id=subject, family=WAKE_WORKFLOW))
        await db.commit()


@pytest.mark.asyncio
async def test_lost_launch_ack_recovers_the_same_opaque_route(opaque_executor, async_session_factory):
    worker, pool, (subject, other_subject) = opaque_executor
    bridge = worker.bridge
    bridge.request.side_effect = TimeoutError
    sibling = workflow_entry(other_subject)
    await prepare_sibling_then_claim(worker, subject, sibling, async_session_factory)
    entry = workflow_entry(subject)
    await pool.route_execution(
        str(subject), {"timeout_seconds": 300}, entry["active_execution"],
        credential_route=pool.credential_route(),
    )
    worker.admission.started(str(subject))
    launch = bridge.request.await_args
    dispatched = launch.args[1]
    assert launch.kwargs["expected_generation"] == bridge.lease.session_generation
    assert dispatched["workflow_deadline"] == dispatched["context"]["workflow_deadline"]
    pending = pool._build_heartbeat()
    assert pending["processes"] == []
    assert pending["busy_count"] == 2
    assert next(invocation for invocation in pending["runtime_invocations"]
                if invocation["execution_id"] == str(subject))["runtime_handle"] is None

    entry.update({key: dispatched[key] for key in ("route_id", "started_at", "workflow_deadline")})
    generation = await receive_reconnect(bridge, async_session_factory, inventory([entry, sibling]))
    await worker.reconcile_transport_inventory(expected_generation=generation)
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
        # Both subjects appear, but the original in-memory route is missing.
        generation = await receive_reconnect(
            bridge, async_session_factory, inventory([live, workflow_entry(subjects[1])]),
        )
        await worker.reconcile_transport_inventory(expected_generation=generation)
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
@pytest.mark.parametrize("queued_send", [False, True], ids=["claimed", "waiting-for-socket-lock"])
async def test_reconnect_excludes_workflow_still_entering_dispatch(
    opaque_executor, async_session_factory, monkeypatch, queued_send,
):
    worker, pool, (subject, sibling_subject) = opaque_executor
    bridge = worker.bridge
    sibling, entry = workflow_entry(sibling_subject), workflow_entry(subject)
    await prepare_sibling_then_claim(worker, subject, sibling, async_session_factory)
    monkeypatch.setattr(bridge, "request", Bridge.request.__get__(bridge))
    credential_route = pool.credential_route()
    replacement = Mock(send_text=AsyncMock())

    async def dispatch():
        with pytest.raises(CredentialRouteChanged, match="before workflow dispatch"):
            await pool.route_execution(
                str(subject), {"timeout_seconds": 300}, entry["active_execution"],
                credential_route=credential_route,
            )

    if queued_send:
        entering_socket = asyncio.Event()
        monkeypatch.setattr(pool, "_write_active_execution_lease", AsyncMock(
            side_effect=lambda execution: entering_socket.set(),
        ))
        reconcile_waiting = asyncio.Event()
        acquire = pool._restart_lock.acquire

        async def observe_acquire():
            if pool._restart_lock.locked():
                reconcile_waiting.set()
            return await acquire()

        monkeypatch.setattr(pool._restart_lock, "acquire", observe_acquire)
        async with bridge.lock:
            dispatch_task = asyncio.create_task(dispatch())
            await entering_socket.wait()
            generation = await receive_reconnect(
                bridge, async_session_factory, inventory([sibling]), socket=replacement,
            )
            reconcile_task = asyncio.create_task(
                worker.reconcile_transport_inventory(expected_generation=generation),
            )
            lock_waiter = asyncio.create_task(reconcile_waiting.wait())
            await asyncio.wait({reconcile_task, lock_waiter}, return_when=asyncio.FIRST_COMPLETED)
            waited_for_dispatch = reconcile_waiting.is_set()
            lock_waiter.cancel()
            await asyncio.gather(lock_waiter, return_exceptions=True)
        await asyncio.gather(dispatch_task, reconcile_task)
        assert waited_for_dispatch
    else:
        generation = await receive_reconnect(
            bridge, async_session_factory, inventory([sibling]), socket=replacement,
        )
        await worker.reconcile_transport_inventory(expected_generation=generation)
        await dispatch()

    assert worker._claims_open()
    assert list(pool.processes) == [sibling["route_id"]]
    replacement.send_text.assert_not_awaited()
    pool.on_result.assert_not_awaited()
    async with async_session_factory() as db:
        assert (await db.get(Execution, sibling_subject)).status == ExecutionStatus.RUNNING
        assert (await db.get(ExecutionWorker, worker.incarnation_id)).state == "ready"


@pytest.mark.asyncio
async def test_interrupted_send_omission_settles_only_its_workflow(
    opaque_executor, async_session_factory, monkeypatch,
):
    worker, pool, (subject, sibling_subject) = opaque_executor
    bridge = worker.bridge
    sibling, entry = workflow_entry(sibling_subject), workflow_entry(subject)
    await prepare_sibling_then_claim(worker, subject, sibling, async_session_factory)
    monkeypatch.setattr(bridge, "request", Bridge.request.__get__(bridge))
    sending, release_send = asyncio.Event(), asyncio.Event()

    async def send(frame):
        assert ControlEnvelope.from_json(frame).payload["action"] == "workflow"
        sending.set()
        await release_send.wait()

    bridge.socket.send_text = AsyncMock(side_effect=send)
    dispatch = asyncio.create_task(pool.route_execution(
        str(subject), {"timeout_seconds": 300}, entry["active_execution"],
        credential_route=pool.credential_route(),
    ))
    await sending.wait()
    replacement = Mock(send_text=AsyncMock())
    generation = await receive_reconnect(
        bridge, async_session_factory, inventory([sibling]), socket=replacement,
    )
    release_send.set()
    await dispatch
    await worker.reconcile_transport_inventory(expected_generation=generation)

    assert worker._claims_open()
    assert list(pool.processes) == [sibling["route_id"]]
    pool.on_result.assert_awaited_once()
    result = pool.on_result.await_args.args[0]
    assert result["error_type"] == "WorkflowAttemptUncertain"
    assert result["execution_id"] == str(subject)
    replacement.send_text.assert_not_awaited()
    async with async_session_factory() as db:
        assert (await db.get(Execution, sibling_subject)).status == ExecutionStatus.RUNNING


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [
    "disconnected", "inventory-gated", "generation-changed", "RuntimeError", "MemoryError",
])
async def test_opaque_workflow_refusal_cleans_route_with_real_bridge(monkeypatch, refusal):
    socket = Mock(send_text=AsyncMock())
    bridge = opaque_bridge(socket, RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 1))
    pool = remote_pool.ThinRemoteProcessPool(bridge, runtime_profile=PROFILE, on_result=AsyncMock())
    redis = Mock(delete=AsyncMock())
    monkeypatch.setattr(pool, "_get_redis", AsyncMock(return_value=redis))
    monkeypatch.setattr(pool, "_write_context_to_redis", AsyncMock())
    monkeypatch.setattr(pool, "_write_active_execution_lease", AsyncMock())
    credential_route = pool.credential_route()
    error = CredentialRouteChanged
    if refusal == "disconnected":
        pool._write_active_execution_lease.side_effect = lambda execution: bridge.disconnect()
    elif refusal == "generation-changed":
        bridge.replace_socket(socket, replace(bridge.lease, session_generation=2))
        bridge.reconciliation_ready.set()
    elif refusal != "inventory-gated":
        bridge.reconciliation_ready.set()
        error = MemoryError if refusal == "MemoryError" else RuntimeError

        async def refuse(frame):
            command = ControlEnvelope.from_json(frame)
            await bridge.receive({
                "reply": command.payload["id"], "error": "Launcher refused launch", "error_type": refusal,
            })

        socket.send_text.side_effect = refuse
    entry = workflow_entry(uuid4())
    subject = entry["subject_id"]
    try:
        with pytest.raises(error):
            await pool.route_execution(
                subject, {"timeout_seconds": 300}, entry["active_execution"],
                credential_route=credential_route,
            )
        assert not pool.processes and not pool._routes and not bridge.pending
        redis.delete.assert_awaited_once_with(f"bifrost:exec:{subject}:context",
                                              remote_pool.active_execution_key(subject))
        pool.on_result.assert_not_awaited()
        if error is CredentialRouteChanged:
            socket.send_text.assert_not_awaited()
        else:
            socket.send_text.assert_awaited_once()
    finally:
        await pool.ownership_lost()


@pytest.mark.asyncio
async def test_opaque_dispatch_requires_credential_route(monkeypatch):
    bridge = opaque_bridge(Mock(send_text=AsyncMock()), RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 1))
    pool = remote_pool.ThinRemoteProcessPool(bridge, runtime_profile=PROFILE)
    monkeypatch.setattr(pool, "_write_context_to_redis", AsyncMock())
    entry = workflow_entry(uuid4())
    with pytest.raises(CredentialRouteChanged, match="requires a credential route generation"):
        await pool.route_execution(entry["subject_id"], {}, entry["active_execution"])
    pool._write_context_to_redis.assert_not_awaited()
    bridge.socket.send_text.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True], ids=["transport-error", "send-timeout"])
async def test_opaque_bridge_closes_interrupted_fragment_stream(monkeypatch, timeout):
    closed = asyncio.Event()
    socket = Mock(send_text=AsyncMock(side_effect=[None, RuntimeError("link closed")]),
                  close=AsyncMock(side_effect=lambda **kwargs: closed.set()))
    if timeout:
        monkeypatch.setattr("src.worker.remote.host.COMMAND_REPLY_SECONDS", 0)

        async def stall_after_first_frame(frame):
            if socket.send_text.await_count == 2:
                await asyncio.Future()

        socket.send_text.side_effect = stall_after_first_frame
    bridge = opaque_bridge(socket, RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 1))
    bridge.reconciliation_ready.set()
    with pytest.raises(ConnectionError, match="command send was interrupted"):
        await bridge.request("workflow", {"context": "x" * MAX_FRAME_BYTES}, expected_generation=1)
    await closed.wait()
    assert socket.send_text.await_count == 2
    assert ControlEnvelope.from_json(socket.send_text.await_args_list[0].args[0]).kind == "fragment"
    socket.close.assert_awaited_once_with(code=1011, reason="interrupted command")
    assert not bridge.connected and not bridge.reconciliation_ready.is_set() and not bridge.pending


@pytest.mark.asyncio
async def test_opaque_bridge_does_not_continue_fragment_stream_on_replacement():
    old = Mock(send_text=AsyncMock(), close=AsyncMock())
    replacement = Mock(send_text=AsyncMock(), close=AsyncMock())
    bridge = opaque_bridge(old, RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 1))
    bridge.reconciliation_ready.set()

    async def replace_during_send(frame):
        assert ControlEnvelope.from_json(frame).kind == "fragment"
        bridge.replace_socket(replacement, replace(bridge.lease, session_generation=2))

    old.send_text.side_effect = replace_during_send
    with pytest.raises(ConnectionError, match="socket was replaced during dispatch"):
        await bridge.request("workflow", {"context": "x" * MAX_FRAME_BYTES}, expected_generation=1)
    old.send_text.assert_awaited_once()
    replacement.send_text.assert_not_awaited()
    replacement.close.assert_not_awaited()
    assert bridge.connected and not bridge.pending


@pytest.mark.asyncio
async def test_inventory_loss_preserves_retained_completion_and_classifies_live_invocation(
    opaque_executor, async_session_factory, monkeypatch,
):
    worker, pool, subjects = opaque_executor
    terminal, live = map(workflow_entry, subjects)
    terminal.update(event_id="retained-completion", attempt_state="completed")
    omitted = uuid4()
    subjects.append(omitted)
    async with async_session_factory() as db:
        db.add(Execution(id=omitted, workflow_name="opaque", executed_by_name="test", status=ExecutionStatus.RUNNING))
        db.add(ExecutionWorkerClaim(worker_id=worker.incarnation_id, subject_id=omitted, family=WAKE_WORKFLOW))
        await db.commit()

    async def persist(result):
        async with async_session_factory() as db:
            assert await ExecutionRepository(db).update_execution(
                execution_id=result["execution_id"], owner_worker_id=worker.incarnation_id,
                status=ExecutionStatus.SUCCESS, result=result["result"], duration_ms=1,
            )
            await db.commit()

    pool.on_result.side_effect = persist
    waiting = asyncio.Event()
    bridge = worker.bridge
    wait_for_replay = bridge.wait_for_opaque_terminals

    async def observe_replay_wait(*args):
        waiting.set()
        return await wait_for_replay(*args)

    monkeypatch.setattr(bridge, "wait_for_opaque_terminals", observe_replay_wait)
    await bridge.receive(inventory([live], [terminal]))
    reconcile = asyncio.create_task(worker._prepare_admission())
    waiter = asyncio.create_task(waiting.wait())
    await asyncio.wait({reconcile, waiter}, return_when=asyncio.FIRST_COMPLETED)
    woke_for_replay = waiting.is_set()
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)
    await bridge.receive(terminal_event(terminal))
    await reconcile

    assert woke_for_replay
    pool.on_result.assert_awaited_once_with({
        "execution_id": terminal["subject_id"], "sync": True, "success": True, "result": "output",
    })
    assert not worker._claims_open()
    assert not pool.processes
    bridge.request.assert_not_awaited()
    lost = recovery.announce_lost_runs.await_args.args[0]
    assert lost.uncertain_workflow_runs == [subjects[1]]
    assert lost.workflow_runs == [omitted]
    async with async_session_factory() as db:
        completed = await db.get(Execution, subjects[0])
        uncertain = await db.get(Execution, subjects[1])
        missing = await db.get(Execution, omitted)
        assert completed.status == ExecutionStatus.SUCCESS and completed.result == "output"
        assert uncertain.status == ExecutionStatus.FAILED
        assert uncertain.error_message == recovery.WORKFLOW_ATTEMPT_UNCERTAIN
        assert missing.status == ExecutionStatus.FAILED and missing.error_message == recovery.EXECUTOR_LOST


@pytest.mark.asyncio
@pytest.mark.parametrize(("start_format", "deadline_format"), [
    ("microseconds", "microseconds"),
    ("microseconds", "milliseconds"),
    ("milliseconds", "milliseconds"),
])
async def test_opaque_reconnect_accepts_equivalent_deadline_serialization(
    opaque_executor, async_session_factory, start_format, deadline_format,
):
    worker, pool, subjects = opaque_executor
    entry, sibling = map(workflow_entry, subjects)
    original_start = datetime.now(UTC).replace(microsecond=123456)
    entry["started_at"] = original_start.isoformat()
    original_deadline = original_start + timedelta(seconds=entry["timeout_seconds"])
    entry["workflow_deadline"] = original_deadline.isoformat()
    await worker.bridge.receive(inventory([entry, sibling]))
    await worker._prepare_admission()
    handle = pool.processes[entry["route_id"]]
    reserialized = {
        **entry,
        "started_at": original_start.isoformat(timespec=start_format).replace("+00:00", "Z"),
        "workflow_deadline": original_deadline.isoformat(timespec=deadline_format).replace("+00:00", "Z"),
    }
    generation = await receive_reconnect(worker.bridge, async_session_factory, inventory([reserialized, sibling]))
    await worker.reconcile_transport_inventory(expected_generation=generation)
    assert worker._claims_open()
    assert pool.processes[entry["route_id"]] is handle
    assert handle.current_execution.started_at == original_start
    assert pool._opaque_deadline(handle.current_execution) == original_deadline
    pool.on_result.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("takeover", [True, False], ids=["takeover", "reconnect"])
async def test_changed_opaque_deadline_settles_instead_of_retrying(
    opaque_executor, async_session_factory, takeover,
):
    worker, pool, subjects = opaque_executor
    bridge = worker.bridge
    entry, sibling = map(workflow_entry, subjects)
    deadline = datetime.fromisoformat(entry["workflow_deadline"]) + timedelta(seconds=1)
    changed = {**entry, "workflow_deadline": deadline.isoformat()}
    if takeover:
        await bridge.receive(inventory([changed, sibling]))
        await worker._prepare_admission()
    else:
        await bridge.receive(inventory([entry, sibling]))
        await worker._prepare_admission()
        generation = await receive_reconnect(bridge, async_session_factory, inventory([changed, sibling]))
        await worker.reconcile_transport_inventory(expected_generation=generation)
    assert not worker._claims_open()
    assert not pool.processes
    bridge.request.assert_not_awaited()
    async with async_session_factory() as db:
        assert (await db.get(ExecutionWorker, worker.incarnation_id)).state == "lost"
        row = await db.get(Execution, subjects[0])
        assert row.status == ExecutionStatus.FAILED
        assert row.error_message == recovery.WORKFLOW_ATTEMPT_UNCERTAIN


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmed", [False, True], ids=["lost-receipt", "confirmed"])
async def test_same_owner_reconnect_reissues_only_unconfirmed_stop(
    opaque_executor, async_session_factory, confirmed,
):
    worker, pool, subjects = opaque_executor
    bridge = worker.bridge
    entry, sibling = map(workflow_entry, subjects)
    async with async_session_factory() as db:
        await db.execute(update(Execution).where(Execution.id == subjects[0]).values(
            status=ExecutionStatus.CANCELLING,
        ))
        await db.commit()
    receipt = {"received": True}
    bridge.request.side_effect = [receipt if confirmed else TimeoutError(), receipt]
    await bridge.receive(inventory([entry, sibling]))
    await worker._prepare_admission()
    handle = pool.processes[entry["route_id"]]
    await handle.process.stop_task
    first = bridge.request.await_args
    generation = await receive_reconnect(bridge, async_session_factory, inventory([entry, sibling]))
    await worker.reconcile_transport_inventory(expected_generation=generation)
    await handle.process.stop_task

    assert bridge.request.await_count == (1 if confirmed else 2)
    if not confirmed:
        retry = bridge.request.await_args
        assert retry.args == first.args
        assert retry.kwargs["expected_generation"] == generation
    assert worker.admission.held_bytes() == 2 * RESERVATION
    assert pool.processes[entry["route_id"]] is handle
    pool.on_result.assert_not_awaited()


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
