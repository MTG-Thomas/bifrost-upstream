"""Executor runtime profiles and the Cloudflare eligibility marker are opt-in.

Every existing executor is a Linux process and every ordinarily staged
dispatch is Linux-only; only the API's explicit approval marks a dispatch.
"""

from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.work_dispatches import WorkDispatch
from src.services import work_dispatch
from tests.unit.services.worker_pool.test_worker_pool import _clean as _clean, db as db_fixture

db = db_fixture
DIGEST = "ab" * 32


async def _stage(db, **marker):
    async with db.context() as session:
        row = work_dispatch.stage_dispatch(
            session, family="workflow_run", subject_id=uuid4(), handler="workflow.execute",
            handler_version=1, message={}, payload={}, memory_request_bytes=64 << 20, **marker,
        )
        await session.flush()
        return row.id


async def _marker(db, dispatch_id):
    async with db.context() as session:
        try:
            return await session.scalar(
                select(WorkDispatch.cloudflare_revision).where(WorkDispatch.id == dispatch_id))
        finally:
            await session.execute(delete(WorkDispatch).where(WorkDispatch.id == dispatch_id))


async def test_ordinary_dispatches_carry_no_cloudflare_marker(db) -> None:
    assert await _marker(db, await _stage(db)) is None


async def test_an_explicit_marker_round_trips(db) -> None:
    assert await _marker(db, await _stage(db, cloudflare_revision=DIGEST)) == DIGEST


@pytest.mark.parametrize("bad", ["", "AB" * 32, "ab" * 31, "ab" * 33, "zz" * 32])
async def test_a_marker_must_be_a_sha256_digest(db, bad) -> None:
    with pytest.raises(ValueError, match="sha256"):
        await _stage(db, cloudflare_revision=bad)


async def test_executors_default_to_the_linux_runtime(db) -> None:
    worker_id = uuid4()
    async with db.context() as session:
        session.add(ExecutionWorker(id=worker_id, form="static", state="ready"))
    async with db.context() as session:
        try:
            assert await session.scalar(
                select(ExecutionWorker.runtime_profile).where(ExecutionWorker.id == worker_id)
            ) == "linux-process-v1"
        finally:
            await session.execute(delete(ExecutionWorker).where(ExecutionWorker.id == worker_id))
