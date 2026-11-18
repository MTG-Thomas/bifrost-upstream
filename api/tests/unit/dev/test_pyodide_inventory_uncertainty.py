"""Opaque inventory cannot certify physical settlement by Linux conventions."""
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import delete
from src.models.enums import ExecutionStatus
from src.models.orm.execution_workers import ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.services.worker_pool.recovery import WORKFLOW_ATTEMPT_UNCERTAIN
from src.worker.remote.remote_pool import ThinRemoteProcessPool
from tests.unit.dev.test_thin_remote_pool import _active, _Transport
from tests.unit.worker.remote.test_opaque_workflow_lifecycle import (
    inventory,
    workflow_entry,
)
from tests.unit.worker.remote.test_opaque_workflow_lifecycle import (
    opaque_executor as _opaque_executor,
)

opaque_executor = _opaque_executor


@pytest.mark.asyncio
@pytest.mark.parametrize('opaque', [True, False])
async def test_omitted_inventory_only_settles_the_linux_process_contract(
    opaque, opaque_executor, async_session_factory,
):
    if opaque:
        worker, pool, subjects = opaque_executor
        await pool.reconcile_inventory(inventory(list(map(workflow_entry, subjects)))["inventory"])
        assert await pool.reconcile_inventory(inventory()["inventory"]) is None
        pool.on_result.assert_not_awaited()
        worker.bridge.request.assert_not_awaited()
        async with async_session_factory() as session:
            for subject in subjects:
                run = await session.get(Execution, subject)
                assert run.status == ExecutionStatus.FAILED and run.error_message == WORKFLOW_ATTEMPT_UNCERTAIN
        return

    transport = _Transport()
    transport.request = AsyncMock(return_value={'pid': 123})
    callback = AsyncMock()
    pool = ThinRemoteProcessPool(transport, on_result=callback)
    with (patch.object(pool, '_write_context_to_redis', new_callable=AsyncMock),
          patch.object(pool, '_write_active_execution_lease', new_callable=AsyncMock)):
        await pool.route_execution('inventory-run', {'timeout_seconds': 30}, _active('inventory-run'))
    await pool.reconcile_inventory(inventory()["inventory"])
    callback.assert_awaited_once()
    assert callback.await_args.args[0]['error_type'] == 'ProcessCrashError'
    assert pool._build_heartbeat()['busy_count'] == 0
    transport.request.assert_awaited_once()  # Dispatch only; no guessed termination.


@pytest.mark.asyncio
@pytest.mark.parametrize('terminal', [False, True])
async def test_rejected_opaque_inventory_never_issues_a_linux_terminate(
    opaque_executor, async_session_factory, terminal,
):
    worker, pool, _ = opaque_executor
    async with async_session_factory() as session:
        await session.execute(delete(ExecutionWorkerClaim).where(
            ExecutionWorkerClaim.worker_id == worker.incarnation_id,
        ))
        await session.commit()
    entry = workflow_entry(uuid4())
    if terminal:
        # Already-collected terminal events must remain replayable after the
        # canonical claim ended, including a commit whose transport ACK was lost.
        entry.update(event_id="already-committed", attempt_state="completed")
        snapshot = await pool.reconcile_inventory(inventory(terminals=[entry])["inventory"])
        assert snapshot['workflow_requests'] == {}
    else:
        assert await pool.reconcile_inventory(inventory([entry])["inventory"]) is None
    worker.bridge.request.assert_not_awaited()
    assert pool.processes == {}


@pytest.mark.asyncio
async def test_opaque_omission_preserves_owner_fencing_priority(opaque_executor):
    _, pool, subjects = opaque_executor
    await pool.reconcile_inventory(inventory(list(map(workflow_entry, subjects)))["inventory"])
    with (
        patch('src.worker.remote.fencing.fence_remote_owner', new_callable=AsyncMock, return_value=False),
        pytest.raises(ConnectionError, match='Remote API ownership was replaced'),
    ):
        await pool.reconcile_inventory(inventory()["inventory"])
    pool.on_result.assert_not_awaited()
    assert pool._build_heartbeat()['busy_count'] == 2
