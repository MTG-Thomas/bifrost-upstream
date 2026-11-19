from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from sqlalchemy import delete, select, text

from src.models.orm.agent_runs import AgentRun
from src.models.orm.ai_models import AIModelAssignment, AIModelProfile, AIProviderConnection
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.work_dispatches import WorkDispatch
from src.services.ai_model_service import AIModelService
from src.services.execution import cloudflare_summary
from src.services.memory_requests import SUMMARY_REQUEST_BYTES
from src.services.worker_pool.claims import PoolClaim
from src.worker.remote import cloudflare_summary as routes_module


@pytest.mark.parametrize("endpoint", ["http://stub.invalid", "https://a:b@stub.invalid", "https://stub.invalid?token=1"])
def test_summary_endpoint_refuses_unpinned_addresses(endpoint):
    assert cloudflare_summary.summary_endpoint(endpoint) is None


async def test_cloudflare_summary_uses_current_managed_connection(async_session_factory):
    async with async_session_factory() as db:
        service = AIModelService(db)
        endpoint = "https://stub.invalid/v1"
        connection = AIProviderConnection(
            name=str(uuid4()), provider="openai_compatible", endpoint=endpoint,
            encrypted_api_key=service.encrypt_api_key("first-stub-key"),
        )
        db.add(connection)
        await db.flush()
        profile = AIModelProfile(name=str(uuid4()), connection_id=connection.id,
                                 model="deepseek-chat", openai_transport="chat_completions")
        db.add(profile)
        await db.flush()
        await service.set_assignment("summarization", profile.id)
        await db.flush()
        run = AgentRun()
        try:
            for key in ("first-stub-key", "rotated-stub-key"):
                connection.encrypted_api_key = service.encrypt_api_key(key)
                await db.flush()
                linux, _ = await cloudflare_summary.get_summarization_client(db)
                cloud = await cloudflare_summary.prepare_completion(db, run, endpoint)
                assert cloud["api_key"] == linux.config.api_key == key
                assert cloud["body"]["max_tokens"] == 8000
            connection.encrypted_api_key = service._get_fernet().encrypt(b"").decode()
            await db.flush()
            with pytest.raises(RuntimeError):
                await cloudflare_summary.get_summarization_client(db)
            with pytest.raises(ValueError, match="could not be resolved"):
                await cloudflare_summary.prepare_completion(db, run, endpoint)
            await db.execute(delete(AIModelAssignment).where(
                AIModelAssignment.assignment_key == "summarization",
            ))
            with pytest.raises(RuntimeError):
                await cloudflare_summary.get_summarization_client(db)
            with pytest.raises(ValueError, match="could not be resolved"):
                await cloudflare_summary.prepare_completion(db, run, endpoint)
        finally:
            await db.rollback()


async def test_cloudflare_completion_uses_linux_summary_writer(monkeypatch):
    writer = AsyncMock()
    monkeypatch.setattr(cloudflare_summary, "write_summary", writer)
    db, run = AsyncMock(), AgentRun()
    assert await cloudflare_summary._persist_completion(db, run, {
        "content": '{"asked":"example"}', "model": "stub", "provider": "fixture-provider",
        "input_tokens": 3, "output_tokens": 1,
    }) is None
    args, kwargs = writer.await_args
    assert args[:3] == (db, run, {"asked": "example"})
    assert args[3].cache_read_tokens is None
    assert args[3].reasoning_tokens is None
    assert kwargs["provider"] == "fixture-provider"


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
    async def check_lock(db, _run, endpoint):
        locked = set((await db.scalars(text("""
            SELECT relation::regclass::text FROM pg_locks
            WHERE pid = pg_backend_pid() AND mode = 'RowShareLock'
        """))).all())
        assert "agent_runs" in locked
        assert "agents" not in locked
        return {"endpoint": endpoint, "body": {}}

    prepare = AsyncMock(side_effect=check_lock)
    monkeypatch.setattr(cloudflare_summary, "prepare_completion", prepare)
    transport = SimpleNamespace(request=AsyncMock(side_effect=MemoryError()))
    monkeypatch.setattr(routes_module, "hand_off_summary", AsyncMock())
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
        async with context() as db:
            assert (await db.get(AgentRun, run_id)).summary_status == "pending"
            retry = await db.scalar(select(WorkDispatch).where(WorkDispatch.subject_id == run_id))
            assert retry is not None and retry.id != dispatch_id and retry.assigned_worker_id is None
        assert not consumer.routes
    finally:
        async with context() as db:
            await db.execute(delete(WorkDispatch).where(WorkDispatch.subject_id == run_id))
            await db.execute(delete(ExecutionWorker).where(ExecutionWorker.id == worker_id))
            await db.execute(delete(AgentRun).where(AgentRun.id == run_id))
            await db.commit()


async def test_reconcile_stops_superseded_live_summary_and_waits_for_terminal(monkeypatch):
    run_id, claim_id = uuid4(), uuid4()
    db = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [])))

    @asynccontextmanager
    async def context():
        yield db

    monkeypatch.setattr(routes_module, "get_db_context", context)
    monkeypatch.setattr(routes_module, "fence_remote_owner", AsyncMock(return_value=True))
    transport = SimpleNamespace(request=AsyncMock(return_value={"received": True}))
    consumer = routes_module.CloudflareSummaryRoutes(transport, uuid4(), "https://stub.invalid")
    monkeypatch.setattr(consumer, "_current_run", AsyncMock(return_value=None))
    monkeypatch.setattr("src.services.execution.summary_dispatch.report_unclaimed_summary", AsyncMock())
    route_id = f"summary:{run_id}:{claim_id}"
    [route] = await consumer.reconcile([{
        "route_id": route_id, "subject_id": str(run_id), "claim_id": str(claim_id),
        "deadline": "2040-01-01T00:00:00Z", "memory_request_bytes": SUMMARY_REQUEST_BYTES,
        "runtime_handle": "summary-handle",
    }], [])
    assert transport.request.await_args.args[0] == "terminate"
    assert not route.finished.done()
    await consumer.handle_result({
        "route_id": route_id, "runtime_handle": "summary-handle", "_event_id": "receipt",
        "attempt_state": "uncertain", "result": {"success": False},
    })
    assert route.finished.done()
