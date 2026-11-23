"""Audit identity propagation for durable platform jobs."""

import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from pydantic import BaseModel
from src.jobs.platform.base import (
    PlatformJobDefinition,
    PlatformJobFailure,
    PlatformJobPolicy,
    PlatformJobRequiresAction,
)
from src.jobs.platform.policy import PlatformJobExecutor
from src.jobs.platform.runner import run_claimed_platform_job
from src.models.orm.platform_jobs import PlatformJob
from src.services.audit_context import current_actor


class _Payload(BaseModel):
    value: str


def _running_job(*, payload_version: int, payload: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        lease_token=uuid4(),
        status="running",
        job_type="application.sdk_update",
        payload_version=payload_version,
        payload=payload,
        encrypted_payload=None,
        organization_id=None,
        requested_by_user_id=str(uuid4()),
        requested_by_email="operator@example.com",
        requested_by_name="Operator",
        impersonated_by_user_id=None,
        impersonation_session_id=None,
        result=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", list(PlatformJobExecutor))
@pytest.mark.parametrize("failure_code", [None, "CF_WORKER_INGRESS_UNVERIFIED", "CF_OWNERSHIP_CONFLICT"])
async def test_enable_receipt_identifies_actual_executor(monkeypatch, executor, failure_code) -> None:
    from src.jobs.platform import runner
    from src.jobs.platform.catalog import get_platform_job_metadata

    job = _running_job(payload_version=1, payload={"value": "ok"})
    job.job_type = "capacity.cloudflare_enable"
    db = AsyncMock()
    db.execute.return_value = MagicMock(scalar_one_or_none=lambda: job)

    @asynccontextmanager
    async def db_context():
        yield db

    result: dict[str, str | dict[str, str]] = {"code": failure_code} if failure_code else {"outcome": "enabled"}
    if failure_code == "CF_OWNERSHIP_CONFLICT":
        result["details"] = {
            "resource_class": "worker_script", "check": "foreign_occupant",
            "observed_category": "owner_missing_or_different", "expected_category": "exact_owner",
        }

    async def handle(_context, _payload):
        if failure_code:
            raise PlatformJobRequiresAction("Verify Cloudflare Deployment", result)
        return result

    definition = PlatformJobDefinition(
        job_type=job.job_type, payload_version=1, payload_model=_Payload, handler=handle,
        policy=get_platform_job_metadata(job.job_type).policy,
    )
    finish = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "get_db_context", db_context)
    monkeypatch.setattr(runner, "get_platform_job_definition", lambda _: definition)
    monkeypatch.setattr(runner, "finish_platform_job", finish)

    assert await run_claimed_platform_job(job.id, job.lease_token, executor=executor)
    assert finish.await_args.kwargs["status"] == ("requires_action" if failure_code else "succeeded")
    assert finish.await_args.kwargs["result"] == {**result, "execution_class": executor.value}


@pytest.mark.asyncio
@pytest.mark.parametrize(("attempt", "delay_seconds", "job_type", "failure_code"), [
    (1, 15, "capacity.cloudflare_enable", "CF_DEPLOYMENT_UNVERIFIED"),
    (2, 45, "capacity.cloudflare_enable", "CF_DEPLOYMENT_UNVERIFIED"),
    (3, None, "capacity.cloudflare_enable", "CF_DEPLOYMENT_UNVERIFIED"),
    (1, 0, None, "CF_DEPLOYMENT_UNVERIFIED"),
    (1, 15, "capacity.cloudflare_enable", "CF_PROVIDER_UNAVAILABLE"),
    (2, 45, "capacity.cloudflare_enable", "CF_PROVIDER_UNAVAILABLE"),
    (1, 15, "capacity.cloudflare_rotate_worker_credential", "CF_PROVIDER_UNAVAILABLE"),
    (2, 45, "capacity.cloudflare_rotate_worker_credential", "CF_PROVIDER_UNAVAILABLE"),
    (1, 15, "capacity.cloudflare_reconcile", "CF_PROVIDER_UNAVAILABLE"),
    (2, 45, "capacity.cloudflare_reconcile", "CF_PROVIDER_UNAVAILABLE"),
])
async def test_runner_schedules_handler_failure_retries_and_fails_at_the_limit(
    monkeypatch, attempt, delay_seconds, job_type, failure_code,
) -> None:
    from src.jobs.platform import runner
    from src.jobs.platform.catalog import get_platform_job_metadata
    from src.services import platform_jobs

    job = _running_job(payload_version=1, payload={"value": "ok"})
    policy = PlatformJobPolicy(timeout_seconds=30, retry_on_failure=True)
    if job_type is not None:
        job.job_type = job_type
        metadata = get_platform_job_metadata(job.job_type)
        assert metadata is not None
        policy = metadata.policy
    now = datetime(2026, 11, 22, tzinfo=UTC)
    job.attempt = attempt
    job.max_attempts = policy.max_attempts
    job.available_at = now - timedelta(minutes=1)
    job.completed_at = None
    job.revision = 1
    db = AsyncMock()
    db.execute.return_value = MagicMock(scalar_one_or_none=lambda: job)

    @asynccontextmanager
    async def db_context():
        yield db

    result = {"code": failure_code}
    handler = AsyncMock(side_effect=PlatformJobFailure(
        failure_code, "Waiting for provider readback", retryable=True, result=result,
    ))
    definition = PlatformJobDefinition(
        job_type=job.job_type, payload_version=1, payload_model=_Payload, handler=handler, policy=policy,
    )
    publish = AsyncMock()
    monkeypatch.setattr(runner, "get_db_context", db_context)
    monkeypatch.setattr(runner, "get_platform_job_definition", lambda _: definition)
    monkeypatch.setattr(platform_jobs, "get_db_context", db_context)
    monkeypatch.setattr(platform_jobs, "_now", lambda: now)
    monkeypatch.setattr(platform_jobs, "publish_platform_job_update", publish)

    requeued = await run_claimed_platform_job(job.id, job.lease_token)

    assert job.lease_token is None
    assert job.result == result and job.error_code == failure_code
    if delay_seconds is None:
        assert not requeued and job.status == "failed"
        assert job.completed_at == now and not job.error_retryable
        assert job.available_at == now - timedelta(minutes=1)
    else:
        assert requeued and job.status == "queued"
        assert job.completed_at is None and job.error_retryable
        assert job.available_at == now + timedelta(seconds=delay_seconds)
    publish.assert_awaited_once_with(job)


@pytest.mark.asyncio
async def test_local_platform_job_spawn_passes_worker_pool_executor(monkeypatch) -> None:
    from src.jobs.schedulers import platform_jobs

    claim = SimpleNamespace(id=uuid4(), lease_token=uuid4())
    spawn = AsyncMock()
    monkeypatch.setattr(platform_jobs.asyncio, "create_subprocess_exec", spawn)

    backend = platform_jobs._LocalPlatformJobExecutionBackend(PlatformJobExecutor.WORKER_POOL)
    await backend.spawn(claim)

    spawn.assert_awaited_once_with(
        sys.executable, "-m", "src.jobs.platform.runner", str(claim.id), str(claim.lease_token),
        PlatformJobExecutor.WORKER_POOL.value, start_new_session=True,
    )


@pytest.mark.asyncio
async def test_kubernetes_platform_job_passes_pod_executor(monkeypatch) -> None:
    from src.jobs.platform import kubernetes_runner

    claim = SimpleNamespace(id=uuid4(), lease_token=uuid4())
    monkeypatch.setenv("BIFROST_KUBERNETES_JOB_UID", "test-job")
    monkeypatch.setenv("BIFROST_KUBERNETES_POD_UID", "test-pod")
    monkeypatch.setattr(kubernetes_runner, "admit_kubernetes_platform_job", AsyncMock(return_value=claim))
    run = AsyncMock(return_value=True)
    monkeypatch.setattr(kubernetes_runner, "run_claimed_platform_job", run)
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", MagicMock())
    monkeypatch.setattr(loop, "remove_signal_handler", MagicMock())

    assert await kubernetes_runner.run_kubernetes_platform_job(claim.id, claim.lease_token)
    run.assert_awaited_once_with(claim, executor=PlatformJobExecutor.KUBERNETES_PLATFORM_JOB)


@pytest.mark.asyncio
async def test_remote_launcher_platform_job_passes_launcher_executor(monkeypatch) -> None:
    from src.worker.remote import launcher as launcher_module
    from src.worker.remote.identity import LauncherIdentity

    job_id, lease_token = str(uuid4()), str(uuid4())
    identity = LauncherIdentity(uuid4(), uuid4(), None, None, "test-launcher", "external")
    launcher = launcher_module.Launcher(AsyncMock(), identity=identity, secret="test")
    launcher.session_generation = 1
    launcher.pressure_threshold = 0.9
    monkeypatch.setattr(launcher_module, "memory_pressure_ratio", lambda: 0.0)
    monkeypatch.setattr(launcher, "wait_job", AsyncMock())
    spawn = AsyncMock()
    monkeypatch.setattr(launcher_module.asyncio, "create_subprocess_exec", spawn)

    await launcher.command({
        "id": "test-command", "action": "platform_job",
        "payload": {
            "route_id": "test-route", "job_id": job_id, "lease_token": lease_token,
            "memory_request_bytes": 1,
        },
    }, generation=1)
    await asyncio.gather(*launcher._tasks)

    spawn.assert_awaited_once_with(
        sys.executable, "-m", "src.jobs.platform.runner", job_id, lease_token,
        PlatformJobExecutor.REMOTE_LAUNCHER.value, start_new_session=True,
    )


def test_platform_runner_cli_parses_executor_from_argv(monkeypatch) -> None:
    from src.jobs.platform import runner
    from src.services.execution import oom_priority

    job_id, lease_token = str(uuid4()), str(uuid4())
    monkeypatch.setattr(sys, "argv", ["runner", job_id, lease_token, "remote_launcher"])
    monkeypatch.setattr(oom_priority, "volunteer_for_oom_kill", MagicMock())
    run = AsyncMock(return_value=0)
    monkeypatch.setattr(runner, "_main", run)

    assert runner._run_cli() == 0
    run.assert_awaited_once_with(job_id, lease_token, PlatformJobExecutor.REMOTE_LAUNCHER)


@pytest.mark.asyncio
async def test_runner_restores_requesting_actor_for_handler(monkeypatch) -> None:
    job_id = uuid4()
    lease_token = uuid4()
    organization_id = uuid4()
    user_id = uuid4()
    operator_id = uuid4()
    impersonation_session_id = uuid4()
    job = SimpleNamespace(
        id=job_id,
        lease_token=lease_token,
        status="running",
        job_type="test.actor",
        payload_version=1,
        payload={"value": "ok"},
        encrypted_payload=None,
        organization_id=organization_id,
        requested_by_user_id=str(user_id),
        requested_by_email="operator@example.com",
        requested_by_name="Operator",
        impersonated_by_user_id=operator_id,
        impersonation_session_id=impersonation_session_id,
        result=None,
    )
    db = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = job
    db.execute.return_value = result

    @asynccontextmanager
    async def db_context():
        yield db

    async def handler(_context, _payload):
        actor = current_actor()
        assert actor is not None
        assert actor.user_id == user_id
        assert actor.organization_id == organization_id
        assert actor.email == "operator@example.com"
        assert actor.source == "platform_job"
        assert actor.impersonated_by_user_id == operator_id
        assert actor.impersonation_session_id == impersonation_session_id
        return {"ok": True}

    definition = PlatformJobDefinition(
        job_type="test.actor",
        payload_version=1,
        payload_model=_Payload,
        handler=handler,
        policy=PlatformJobPolicy(timeout_seconds=30),
    )
    finish = AsyncMock(return_value=True)
    monkeypatch.setattr("src.jobs.platform.runner.get_db_context", db_context)
    monkeypatch.setattr(
        "src.jobs.platform.runner.get_platform_job_definition",
        lambda _job_type: definition,
    )
    monkeypatch.setattr("src.jobs.platform.runner.finish_platform_job", finish)

    assert await run_claimed_platform_job(job_id, lease_token, executor=PlatformJobExecutor.WORKER_POOL)
    assert current_actor() is None
    finish.assert_awaited_once_with(
        job_id, lease_token, status="succeeded", result={"ok": True}
    )


@pytest.mark.asyncio
async def test_old_sdk_update_runner_refuses_pinned_upgrade_payload(monkeypatch) -> None:
    """A pre-pinning worker must never silently ignore the new target fields."""
    from src.jobs.platform import runner

    job_id, lease_token = uuid4(), uuid4()
    job = SimpleNamespace(
        id=job_id, status="running", job_type="application.sdk_update",
        payload_version=2,
    )
    db = AsyncMock()
    db.execute.return_value = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = job

    @asynccontextmanager
    async def context():
        yield db

    handler = AsyncMock(side_effect=AssertionError("old SDK handler executed"))
    monkeypatch.setattr(runner, "get_db_context", context)
    monkeypatch.setattr(runner, "get_platform_job_definition", lambda _kind: SimpleNamespace(
        handler=handler,
        payload_model_for_version=lambda _version: None,
    ))
    finish = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "finish_platform_job", finish)

    assert not await runner.run_claimed_platform_job(job_id, lease_token)
    handler.assert_not_awaited()
    assert finish.await_args.kwargs["error_code"] == "unsupported_payload_version"


@pytest.mark.asyncio
async def test_runner_dispatches_persisted_legacy_sdk_update_payload(
    db_session, monkeypatch
) -> None:
    from shared.models import ApplicationSdkUpdatePayloadV1
    from src.jobs.platform.application_sdk_update import ApplicationSdkUpdatePayload

    job_id, lease_token, application_id = uuid4(), uuid4(), uuid4()
    job = PlatformJob(
        id=job_id,
        job_type="application.sdk_update",
        payload_version=1,
        payload={"application_id": str(application_id)},
        requested_by_user_id=str(uuid4()),
        requested_by_email="operator@example.com",
        requested_by_name="Operator",
        title="Updating App SDK",
        status="running",
        lease_token=lease_token,
    )
    db_session.add(job)
    await db_session.flush()

    @asynccontextmanager
    async def db_context():
        yield db_session

    handled: list[ApplicationSdkUpdatePayloadV1] = []

    async def handler(_context, payload):
        handled.append(payload)
        return {"application_id": str(payload.application_id)}

    definition = PlatformJobDefinition(
        job_type="application.sdk_update",
        payload_version=2,
        payload_model=ApplicationSdkUpdatePayload,
        handler=handler,
        policy=PlatformJobPolicy(timeout_seconds=30),
        historical_payload_models={1: ApplicationSdkUpdatePayloadV1},
    )
    finish = AsyncMock(return_value=True)
    monkeypatch.setattr("src.jobs.platform.runner.get_db_context", db_context)
    monkeypatch.setattr(
        "src.jobs.platform.runner.get_platform_job_definition",
        lambda _job_type: definition,
    )
    monkeypatch.setattr("src.jobs.platform.runner.finish_platform_job", finish)

    assert await run_claimed_platform_job(job_id, lease_token)
    assert len(handled) == 1
    assert isinstance(handled[0], ApplicationSdkUpdatePayloadV1)
    assert handled[0].application_id == application_id
    finish.assert_awaited_once_with(
        job_id,
        lease_token,
        status="succeeded",
        result={"application_id": str(application_id)},
    )


@pytest.mark.asyncio
async def test_runner_rejects_automatic_fields_on_legacy_sdk_update_payload(
    monkeypatch,
) -> None:
    from shared.models import ApplicationSdkUpdatePayloadV1
    from src.jobs.platform import runner
    from src.jobs.platform.application_sdk_update import ApplicationSdkUpdatePayload

    job = _running_job(
        payload_version=1,
        payload={"application_id": str(uuid4()), "automatic_upgrade": True},
    )
    db = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = job
    db.execute.return_value = result

    @asynccontextmanager
    async def db_context():
        yield db

    handler = AsyncMock()
    definition = PlatformJobDefinition(
        job_type="application.sdk_update",
        payload_version=2,
        payload_model=ApplicationSdkUpdatePayload,
        handler=handler,
        policy=PlatformJobPolicy(timeout_seconds=30),
        historical_payload_models={1: ApplicationSdkUpdatePayloadV1},
    )
    finish = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "get_db_context", db_context)
    monkeypatch.setattr(runner, "get_platform_job_definition", lambda _kind: definition)
    monkeypatch.setattr(runner, "finish_platform_job", finish)

    assert await runner.run_claimed_platform_job(job.id, job.lease_token)
    handler.assert_not_awaited()
    assert finish.await_args.kwargs["error_code"] == "invalid_payload"


@pytest.mark.asyncio
async def test_runner_uses_legacy_model_for_encrypted_sdk_update_payload(
    monkeypatch,
) -> None:
    from shared.models import ApplicationSdkUpdatePayloadV1
    from src.jobs.platform import runner
    from src.jobs.platform.application_sdk_update import ApplicationSdkUpdatePayload

    application_id = uuid4()
    job = _running_job(payload_version=1, payload={"protected": True})
    job.encrypted_payload = "encrypted"
    db = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = job
    db.execute.return_value = result

    @asynccontextmanager
    async def db_context():
        yield db

    handled: list[ApplicationSdkUpdatePayloadV1] = []

    async def handler(_context, payload):
        handled.append(payload)
        return {}

    definition = PlatformJobDefinition(
        job_type="application.sdk_update",
        payload_version=2,
        payload_model=ApplicationSdkUpdatePayload,
        handler=handler,
        policy=PlatformJobPolicy(timeout_seconds=30),
        historical_payload_models={1: ApplicationSdkUpdatePayloadV1},
    )
    monkeypatch.setattr(runner, "get_db_context", db_context)
    monkeypatch.setattr(runner, "get_platform_job_definition", lambda _kind: definition)
    monkeypatch.setattr(
        "src.core.security.decrypt_secret",
        lambda _value: f'{{"application_id": "{application_id}"}}',
    )
    monkeypatch.setattr(runner, "finish_platform_job", AsyncMock(return_value=True))

    assert await runner.run_claimed_platform_job(job.id, job.lease_token)
    assert len(handled) == 1
    assert isinstance(handled[0], ApplicationSdkUpdatePayloadV1)
    assert handled[0].application_id == application_id


@pytest.mark.asyncio
async def test_runner_refuses_unknown_payload_version(monkeypatch) -> None:
    from shared.models import ApplicationSdkUpdatePayloadV1
    from src.jobs.platform import runner
    from src.jobs.platform.application_sdk_update import ApplicationSdkUpdatePayload

    job = _running_job(payload_version=99, payload={"application_id": str(uuid4())})
    db = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = job
    db.execute.return_value = result

    @asynccontextmanager
    async def db_context():
        yield db

    handler = AsyncMock(side_effect=AssertionError("handler executed"))
    definition = PlatformJobDefinition(
        job_type="application.sdk_update",
        payload_version=2,
        payload_model=ApplicationSdkUpdatePayload,
        handler=handler,
        policy=PlatformJobPolicy(timeout_seconds=30),
        historical_payload_models={1: ApplicationSdkUpdatePayloadV1},
    )
    finish = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "get_db_context", db_context)
    monkeypatch.setattr(runner, "get_platform_job_definition", lambda _kind: definition)
    monkeypatch.setattr(runner, "finish_platform_job", finish)

    assert not await runner.run_claimed_platform_job(job.id, job.lease_token)
    handler.assert_not_awaited()
    assert finish.await_args.kwargs["error_code"] == "unsupported_payload_version"
