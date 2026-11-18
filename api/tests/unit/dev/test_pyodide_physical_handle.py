"""Experimental opaque physical adapter; not public runtime admission."""
import json
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from src.models.enums import ExecutionStatus
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.executions import Execution
from src.services.execution.process_pool import ProcessState
from src.worker.remote.host import RemoteControlHost
from src.worker.remote.protocol import Hello, ProtocolError, sign_hello
from src.worker.remote.remote_pool import ThinRemoteProcessPool
from tests.unit.dev.test_thin_remote_pool import _active, _Transport
from tests.unit.worker.remote.test_opaque_workflow_lifecycle import (
    inventory,
    workflow_entry,
)
from tests.unit.worker.remote.test_opaque_workflow_lifecycle import (
    opaque_executor as _opaque_executor,
)

PROFILE = "cloudflare-python-v1"
opaque_executor = _opaque_executor


@pytest.fixture
async def opaque_dispatch(monkeypatch):
    transport = _Transport()
    transport.request = AsyncMock()
    callback = AsyncMock()
    pool = ThinRemoteProcessPool(transport, runtime_profile=PROFILE, on_result=callback)
    monkeypatch.setattr(pool, "_owns_session", AsyncMock(return_value=True))
    monkeypatch.setattr(pool, "_write_context_to_redis", AsyncMock())
    monkeypatch.setattr(pool, "_write_active_execution_lease", AsyncMock())

    async def dispatch(ack):
        transport.request.return_value = ack
        await pool.route_execution(
            'opaque-run', {'timeout_seconds': 30}, _active('opaque-run'),
            credential_route=pool.credential_route(),
        )
        route = transport.request.await_args.args[1]['route_id']
        return pool, transport, callback, route

    try:
        yield dispatch
    finally:
        await pool.ownership_lost()


@pytest.mark.asyncio
async def test_opaque_ack_is_counted_without_inventing_a_pid_or_measurements(opaque_dispatch):
    pool, _, callback, route = await opaque_dispatch({'runtime_handle': 'invocation:opaque-1'})
    handle = pool.processes[route]
    assert handle.pid is None and handle.process.remote_pid is None
    assert handle.process.runtime_handle == 'invocation:opaque-1'
    heartbeat = pool._build_heartbeat()
    assert heartbeat['processes'] == []
    assert heartbeat['busy_count'] == heartbeat['pool_size'] == 1
    assert heartbeat['runtime_invocations'] == [{
        'runtime_profile': PROFILE, 'runtime_handle': 'invocation:opaque-1',
        'route_id': route, 'execution_id': 'opaque-run', 'state': 'busy',
        'memory_bytes': None, 'cpu_seconds': None}]
    event = {'event': 'result', 'route_id': route, 'runtime_handle': 'invocation:opaque-1',
             '_event_id': 'completed-opaque-1', 'attempt_state': 'completed',
             'result': {'success': True, 'execution_id': 'other-run'}}
    await pool.handle_event(event)
    await pool.handle_event(event)
    callback.assert_awaited_once()
    assert callback.await_args.args[0]['execution_id'] == 'opaque-run'
    assert pool._build_heartbeat()['busy_count'] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('ack', [None, [], {}, {'pid': 123}, {'runtime_handle': True},
    {'runtime_handle': 'x' * 256}, {'runtime_handle': 'https://not-a-handle.invalid/'},
    {'runtime_handle': 'opaque', 'pid': None}])
async def test_invalid_opaque_ack_retains_uncertain_invocation(opaque_dispatch, ack):
    pool, transport, callback, route = await opaque_dispatch(ack)
    transport.request.assert_awaited_once()
    assert pool.processes[route].process.runtime_handle is None
    assert pool._build_heartbeat()['busy_count'] == 1
    await pool.handle_event({'event': 'result', 'route_id': route,
                             'runtime_handle': 'invocation-1', '_event_id': 'completed-opaque-1',
                             'attempt_state': 'completed',
                             'result': {'success': False, 'execution_id': 'opaque-run', 'error': 'fixed failure'}})
    callback.assert_awaited_once()
    assert not pool.processes


@pytest.mark.asyncio
async def test_unqualified_cancellation_does_not_claim_stop_or_release_route(opaque_dispatch):
    pool, transport, callback, route = await opaque_dispatch({'runtime_handle': 'opaque'})
    transport.request.return_value = {"received": True}
    await pool._handle_cancel_request('opaque-run')
    action, payload = transport.request.await_args.args
    assert action == "terminate" and payload["route_id"] == route
    assert "pid" not in payload and "grace_seconds" not in payload
    callback.assert_not_awaited()
    assert pool.processes[route].state == ProcessState.BUSY
    assert pool.processes[route].killed_at is None
    assert pool._build_heartbeat()['busy_count'] == 1


@pytest.mark.asyncio
async def test_persistent_service_rejected_before_any_effect():
    transport = _Transport()
    transport.request = AsyncMock()
    pool = ThinRemoteProcessPool(transport, runtime_profile=PROFILE)
    with pytest.raises(RuntimeError, match='persistent services'):
        await pool.route_service(service_id='service', attempt_id='attempt', lease_token='lease', context={})
    transport.request.assert_not_awaited()
    assert pool.service_processes == {}


def test_unknown_profile_rejected_instead_of_falling_back_to_linux():
    with pytest.raises(ValueError, match='Unsupported physical runtime profile'):
        ThinRemoteProcessPool(_Transport(), runtime_profile='unknown-runtime')


def test_opaque_inventory_adoption_binds_handle_and_refuses_replacement():
    pool = ThinRemoteProcessPool(_Transport(), runtime_profile=PROFILE)
    handle = pool._adopt_handle('route', {'runtime_handle': 'opaque-original'}, datetime.now(UTC))
    assert handle.process.runtime_handle == 'opaque-original'
    assert handle.process.remote_pid is None
    with pytest.raises(ConnectionError, match='handle changed'):
        pool._bind_workflow_handle(handle, {'runtime_handle': 'opaque-replacement'})
    assert handle.process.runtime_handle == 'opaque-original'


@pytest.mark.asyncio
async def test_experimental_pool_profile_is_still_rejected_by_public_control_host():
    declared = Hello(launcher_id=uuid4(), incarnation_id=uuid4(), job_uid=None, pod_uid=None,
                     hostname='opaque-test', form='static', issued_at=int(time.time()),
                     nonce='n' * 32, hello_sequence=1, runtime_profile=PROFILE)
    frame = {'event': 'hello', 'hello': sign_hello('test-secret', declared).to_dict(),
             'sample': {'current': 1, 'limit': 100}, 'requirements': {}}
    socket = Mock(accept=AsyncMock(), receive_text=AsyncMock(return_value=json.dumps(frame)), close=AsyncMock())
    with (
        patch('src.worker.remote.host.get_settings', return_value=Mock(secret_key='test-secret')),
        patch('src.worker.remote.host.acquire_remote_owner', new_callable=AsyncMock) as acquire,
        patch('src.worker.remote.host.get_liveness_db_context') as db,
        pytest.raises(ProtocolError, match='unsupported physical runtime'),
    ):
        await RemoteControlHost().connect(socket)
    acquire.assert_not_awaited()
    db.assert_not_called()
    socket.close.assert_awaited_once_with(code=1008)


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["timing", "handle"])
async def test_opaque_reconnect_keeps_original_expired_timing_and_canonical_claim(
    opaque_executor, async_session_factory, conflict,
):
    worker, pool, (run, other_run) = opaque_executor
    entry = workflow_entry(run, expired=True)
    other = workflow_entry(other_run)
    first = await pool.reconcile_inventory(inventory([entry, other])["inventory"])
    assert first['workflow_requests'][str(run)] == entry["memory_request_bytes"]

    entry.update(event_id="retained", attempt_state="completed")
    same = await pool.reconcile_inventory(inventory([other], [entry])["inventory"])
    assert same['workflow_requests'] == first['workflow_requests']
    handle = pool.processes[entry["route_id"]]
    info = handle.current_execution
    assert info.started_at == datetime.fromisoformat(entry["started_at"])
    assert info.timeout_seconds == entry["timeout_seconds"] and info.is_timed_out
    assert pool._build_heartbeat()['busy_count'] == 2

    if conflict == "timing":
        later = info.started_at + timedelta(seconds=1)
        changed = {**entry, "started_at": later.isoformat(),
                   "workflow_deadline": (later + timedelta(seconds=info.timeout_seconds)).isoformat()}
    else:
        changed = {**entry, "runtime_handle": "replacement"}
    assert await pool.reconcile_inventory(inventory([other], [changed])["inventory"]) is None
    assert handle.current_execution is info
    assert handle.process.runtime_handle == entry["runtime_handle"]
    assert not pool.processes
    async with async_session_factory() as session:
        assert (await session.get(ExecutionWorker, worker.incarnation_id)).state == "lost"
        assert (await session.get(Execution, run)).status == ExecutionStatus.FAILED
