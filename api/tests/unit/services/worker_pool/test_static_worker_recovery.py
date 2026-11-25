"""A swept static incarnation cannot stall queued cleanup after reconnect."""

import json
import time
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, update

from src.config import get_settings
from src.jobs.schedulers import platform_jobs
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.platform_jobs import PlatformJob
from src.services.worker_pool.claims import mark_static_ready
from src.services.worker_pool.liveness import HEARTBEAT_LOST_AFTER, end_silent_static_executors
from src.worker.remote import host
from src.worker.remote.fencing import LeaseRef, remote_owner_context
from src.worker.remote.identity import LauncherIdentity
from src.worker.remote.ownership import renew_remote_owner
from src.worker.remote.protocol import STATIC_INCARNATION_LOST, AuthenticationError, Hello, new_nonce, sign_hello


@pytest.mark.asyncio
async def test_lost_static_reconnect_fences_old_claims_and_allows_cleanup(async_session_factory, monkeypatch):
    control = host.RemoteControlHost()
    original, replacement = (LauncherIdentity.from_environment({"HOSTNAME": "static"}) for _ in range(2))
    # Keep enrollment and the claim transaction real, leaving worker task
    # startup to the launcher lifecycle tests.
    monkeypatch.setattr(host, "ApiExecutor", Mock())
    monkeypatch.setattr(host, "RemoteSession", lambda _bridge, _worker, lease, **_: Mock(lease=lease))
    monkeypatch.setattr(platform_jobs, "publish_platform_job_update", AsyncMock())

    def socket(identity, sequence, *, launcher_id=None):
        hello = Hello(
            launcher_id=launcher_id or identity.launcher_id,
            incarnation_id=identity.incarnation_id,
            job_uid=None, pod_uid=None, hostname=identity.hostname, form="static",
            issued_at=int(time.time()), nonce=new_nonce(), hello_sequence=sequence,
        )
        return Mock(scope={}, accept=AsyncMock(), send_text=AsyncMock(), close=AsyncMock(),
                    receive_text=AsyncMock(return_value=json.dumps({
                        "event": "hello", "hello": sign_hello(get_settings().secret_key, hello).to_dict(),
                        "sample": {"current": 1, "limit": 1024**3},
                    })))

    job_type = "capacity.cloudflare_disable"
    job_id = uuid4()
    try:
        first, _ = await control.connect(socket(original, 1))
        with remote_owner_context(LeaseRef(first.lease)):
            assert await mark_static_ready(original.incarnation_id, memory_budget_bytes=1024**3)
        async with async_session_factory() as db:
            db.add(PlatformJob(
                id=job_id, job_type=job_type, payload={}, title="Disconnect",
                requested_by_user_id=str(uuid4()), requested_by_email="operator@example.test",
                requested_by_name="Operator", resource_lock_key=f"cloudflare_installation:{uuid4()}",
            ))
            await db.execute(update(ExecutionWorker).where(ExecutionWorker.id == original.incarnation_id).values(
                heartbeat_at=func.now() - 2 * HEARTBEAT_LOST_AFTER,
            ))
            await db.commit()
        await end_silent_static_executors()
        async with async_session_factory() as db:
            row = await db.get(ExecutionWorker, original.incarnation_id)
            assert row is not None and row.state == "lost" and row.end_reason == "heartbeat_lost"
        assert await renew_remote_owner(first.lease) is None
        for launcher, expected_code in [(uuid4(), 1008), (original.launcher_id, STATIC_INCARNATION_LOST)]:
            reconnect = socket(original, 2, launcher_id=launcher)
            with pytest.raises(AuthenticationError):
                await control.connect(reconnect)
            assert reconnect.close.await_args.kwargs["code"] == expected_code

        claim_args = {"fits_bytes": 1024**3, "job_types": frozenset({job_type})}
        with remote_owner_context(LeaseRef(first.lease)):
            assert await platform_jobs.claim_platform_job(**claim_args) is None
        successor, _ = await control.connect(socket(replacement, 1))
        with remote_owner_context(LeaseRef(successor.lease)):
            assert await mark_static_ready(replacement.incarnation_id, memory_budget_bytes=1024**3)
            claimed = await platform_jobs.claim_platform_job(**claim_args)
            assert claimed is not None and claimed.id == job_id
            assert await platform_jobs.claim_platform_job(**claim_args) is None
        async with async_session_factory() as db:
            job = await db.get(PlatformJob, job_id)
            assert job is not None and job.status == "running" and job.attempt == 1
    finally:
        async with async_session_factory() as db:
            await db.execute(delete(PlatformJob).where(PlatformJob.id == job_id))
            await db.execute(delete(ExecutionWorker).where(ExecutionWorker.id.in_(
                (original.incarnation_id, replacement.incarnation_id),
            )))
            await db.commit()
