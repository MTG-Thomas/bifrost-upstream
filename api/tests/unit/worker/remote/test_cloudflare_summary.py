from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from sqlalchemy import delete

from src.models.orm.agent_runs import AgentRun
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.work_dispatches import WorkDispatch
from src.services.execution import cloudflare_summary
from src.services.memory_requests import SUMMARY_REQUEST_BYTES
from src.services.worker_pool.claims import PoolClaim
from src.worker.remote import cloudflare_summary as routes_module


@pytest.mark.parametrize("endpoint", ["http://stub.invalid", "https://a:b@stub.invalid", "https://stub.invalid?token=1"])
def test_summary_endpoint_refuses_unpinned_addresses(endpoint):
    assert cloudflare_summary.summary_endpoint(endpoint) is None


async def test_summary_request_only_selects_public_model_profile_fields():
    profile = SimpleNamespace(
        model="stub", default_max_tokens=50, openai_transport="chat_completions",
        failover_profile_id=None, reasoning_effort=None,
        provider="openai_compatible", endpoint="https://stub.invalid/v1/", catalog_provider_id=None,
    )
    db = SimpleNamespace(execute=AsyncMock(side_effect=[
        SimpleNamespace(one_or_none=lambda: profile), SimpleNamespace(one_or_none=lambda: None),
    ]))
    run = SimpleNamespace(agent_id=None, input={"message": "ask"}, output={"message": "answer"})

    result = await cloudflare_summary.prepare_completion(db, run, "https://stub.invalid/v1")

    query = str(db.execute.await_args_list[0].args[0])
    assert "encrypted_api_key" not in query
    assert result["endpoint"] == "https://stub.invalid/v1"
    assert result["body"]["max_completion_tokens"] == 50
    assert '"message": "answer"' in result["body"]["messages"][1]["content"]


async def test_success_cannot_settle_an_uncertain_attempt():
    import asyncio

    run_id, claim_id = uuid4(), uuid4()
    route = routes_module.SummaryRoute(run_id, claim_id, 1, datetime.now(UTC),
                                       asyncio.get_running_loop().create_future(), "hold")
    consumer = routes_module.CloudflareSummaryRoutes(AsyncMock(), uuid4(), "https://stub.invalid")
    consumer.routes[route.id] = route

    with pytest.raises(ValueError, match="Invalid opaque summary result"):
        await consumer.handle_result({
            "route_id": route.id, "runtime_handle": "opaque", "_event_id": "receipt",
            "attempt_state": "uncertain", "result": {"success": True},
        })
    assert not route.finished.done()


async def test_summary_preparation_locks_only_its_agent_run(async_session_factory, monkeypatch):
    worker_id, run_id, claim_id, dispatch_id = uuid4(), uuid4(), uuid4(), uuid4()

    @asynccontextmanager
    async def context():
        async with async_session_factory() as db:
            yield db

    monkeypatch.setattr(routes_module, "get_db_context", context)
    monkeypatch.setattr(routes_module, "fence_remote_owner", AsyncMock(return_value=True))
    prepare = AsyncMock(return_value={"endpoint": "https://stub.invalid", "body": {}})
    monkeypatch.setattr(cloudflare_summary, "prepare_completion", prepare)
    transport = SimpleNamespace(request=AsyncMock(side_effect=MemoryError()))
    report_failure = AsyncMock()
    monkeypatch.setattr(routes_module, "fail_remote_summary", report_failure)
    async with context() as db:
        db.add(ExecutionWorker(id=worker_id, state="ready", form="external", resource_class="standard"))
        db.add(AgentRun(id=run_id, status="completed", trigger_type="sdk", summary_status="pending"))
        await db.flush()
        db.add(ExecutionWorkerClaim(id=claim_id, worker_id=worker_id, family="agent_summary", subject_id=run_id))
        db.add(WorkDispatch(
            id=dispatch_id, family="agent_summary", subject_id=run_id,
            handler="agent.summarize", handler_version=1, message={}, payload={},
            memory_request_bytes=SUMMARY_REQUEST_BYTES, assigned_worker_id=worker_id,
        ))
        await db.commit()
    claim = PoolClaim(
        dispatch_id, "agent.summarize", {"claim_id": str(claim_id)}, run_id,
        SUMMARY_REQUEST_BYTES, datetime.now(UTC),
    )
    try:
        consumer = routes_module.CloudflareSummaryRoutes(transport, worker_id, "https://stub.invalid")
        await consumer.run(claim, 1)
        prepare.assert_awaited_once()
        transport.request.assert_awaited_once()
        report_failure.assert_awaited_once()
        async with context() as db:
            assert (await db.get(AgentRun, run_id)).summary_status == "generating"
            assert await db.get(WorkDispatch, dispatch_id) is None
    finally:
        async with context() as db:
            await db.execute(delete(WorkDispatch).where(WorkDispatch.id == dispatch_id))
            await db.execute(delete(ExecutionWorker).where(ExecutionWorker.id == worker_id))
            await db.execute(delete(AgentRun).where(AgentRun.id == run_id))
            await db.commit()
