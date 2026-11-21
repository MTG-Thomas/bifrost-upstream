"""Exercise the Cloudflare journal against the migrated platform database."""

import asyncio
import json
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from fastapi import HTTPException
from pydantic import SecretStr
from shared.models import (
    CloudflareCapacityPolicy,
    CloudflareLauncherUsage,
    CloudflareWorkersPolicy,
)
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from src.config import Settings
from src.core.principal import UserPrincipal
from src.jobs.platform import cloudflare_capacity as lifecycle_job
from src.jobs.platform import cloudflare_enable as enable_job
from src.jobs.platform.base import (
    PlatformJobContext,
    PlatformJobFailure,
    PlatformJobRequiresAction,
)
from src.models.orm.audit import AuditLog
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareStagedToken,
    CloudflareUsageActivity,
    CloudflareUsageIncarnation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.platform_jobs import PlatformJob
from src.models.orm.work_dispatches import WorkDispatch
from src.services import worker_ingress_probes as ingress_probes
from src.services.audit_context import ActorContext, clear_actor, set_actor
from src.services.authorization.context import (
    AuthorizationContext,
    Boundary,
    BoundaryKind,
    RoleGrant,
)
from src.services.authorization.enforce import Caller
from src.services.kubernetes_execution import KubernetesExecutionService
from src.services.work_dispatch import WAKE_WORKFLOW
from src.services.worker_pool import (
    cloudflare_canary,
    cloudflare_spend,
    cloudflare_supervisor,
    controller,
    external_controller,
)
from src.services.worker_pool.cloudflare_admission import (
    ManagedCloudflareScope,
    cloudflare_claim_allowed,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    consume_staged_token,
    open_credential,
    seal_credential,
    stage_account_token,
)
from src.services.worker_pool.cloudflare_policy import (
    CONSENT_TEXT_VERSION,
    CloudflareBudgetTooLow,
    CloudflareInstallationExists,
    CloudflarePolicyConflict,
    WorkersReleaseMeterBounds,
    create_cloudflare_installation,
    get_cloudflare_capacity_status,
    retire_cloudflare_installation,
    save_cloudflare_policy,
)
from src.services.worker_pool.cloudflare_provisioning import (
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
)
from src.services.worker_pool.cloudflare_spend import (
    WORKERS_CALIBRATED_RATE_TABLE_VERSION,
    WORKERS_INVENTORY_RATE_TABLE_VERSION,
    WORKERS_LEGACY_RATE_TABLE_VERSION,
    WORKERS_RATE_TABLE_VERSION,
    WORKERS_RATES,
    cloudflare_estimated_usage,
    read_workers_usage,
    reserve_tenant_host_usage,
    usage_period,
)
from src.services.worker_pool.cloudflare_workers_rest import (
    OWNER_BINDING,
    ConfiguredWorkersScript,
    WorkersRestCleanup,
    worker_owner_value,
)

OPERATOR_ID = UUID("20cb7fe5-78e2-48d7-8f1e-8edcac69ffab")
TEST_LAUNCHER_CAPS = {"sqlite_rows_written": 100, "do_requests": 20, "log_events": 5}
TEST_CPU_LIMITS = {
    "capacity": 87, "gateway": 50, "workflow": 30_000, "boot": 2000, "warm": 200, "exercise": 100, "startup": 1000,
}


def _platform_operator() -> Caller:
    role = RoleGrant(uuid4(), frozenset({"platform.readwrite"}), (Boundary(BoundaryKind.PLATFORM),))
    return Caller(
        UserPrincipal(OPERATOR_ID, "operator@example.test", None),
        AuthorizationContext(OPERATOR_ID, None, uuid4(), False, frozenset(), (role,)),
    )


@pytest.fixture
def lifecycle_audit_actor(platform_admin):
    token = set_actor(ActorContext(platform_admin.user_id, platform_admin.organization_id))
    try:
        yield
    finally:
        clear_actor(token)


def test_cloudflare_routes_refuse_provider_org_membership(e2e_client, provider_org_user, platform_admin) -> None:
    path = "/api/capacity/cloudflare"
    assert e2e_client.get(path, headers=platform_admin.headers).status_code == 200
    assert e2e_client.get(path, headers=provider_org_user.headers).status_code == 403
    assert e2e_client.post(f"{path}/credential", headers=provider_org_user.headers, json={
        "account_id": "a" * 32, "purpose": "connect", "api_token": "synthetic-management-token",
    }).status_code == 403
    body = {"installation_id": str(uuid4()), "expected_revision": 1}
    assert e2e_client.patch(path, headers=provider_org_user.headers, json={
        **body, "action": "policy", "policy": {
            "workers": {"launcher_cap": 1, "slots_per_launcher": 2, "max_lifetime_seconds": 3600, "work_kinds": ["workflow"]},
            "monthly_limit_usd": "10.00",
        },
    }).status_code == 403
    for action in ("scale-in", "disconnect", "enable", "reconcile"):
        assert e2e_client.post(f"{path}/{action}", headers=provider_org_user.headers, json=body).status_code == 403
    assert e2e_client.post(
        f"{path}/rotate", headers=provider_org_user.headers,
        json={**body, "api_token": "synthetic-replacement-token"},
    ).status_code == 403


@pytest.mark.asyncio
async def test_staged_credential_is_encrypted_bound_expiring_and_single_use(db_session: AsyncSession) -> None:
    token = SecretStr("synthetic-staged-account-token")
    account = "a" * 32
    ref, expires_at = await stage_account_token(
        db_session, actor_id=OPERATOR_ID, account_id=account, purpose="connect", token=token,
    )
    await db_session.flush()
    staged = await db_session.get(CloudflareStagedToken, ref)
    assert staged is not None and token.get_secret_value() not in staged.ciphertext
    assert expires_at > datetime.now(UTC)
    for bad_actor, bad_account, bad_purpose in (
        (uuid4(), account, "connect"), (OPERATOR_ID, "b" * 32, "connect"), (OPERATOR_ID, account, "rotate"),
    ):
        with pytest.raises(ValueError, match="Invalid or expired"):
            await consume_staged_token(
                db_session, ref, actor_id=bad_actor, account_id=bad_account, purpose=bad_purpose,
            )
    assert await consume_staged_token(
        db_session, ref, actor_id=OPERATOR_ID, account_id=account, purpose="connect",
    ) == token
    with pytest.raises(ValueError, match="Invalid or expired"):
        await consume_staged_token(db_session, ref, actor_id=OPERATOR_ID, account_id=account, purpose="connect")
    expired, _ = await stage_account_token(
        db_session, actor_id=OPERATOR_ID, account_id=account, purpose="rotate", token=token,
    )
    await db_session.flush()
    await db_session.execute(update(CloudflareStagedToken).where(
        CloudflareStagedToken.id == expired,
    ).values(expires_at=datetime.now(UTC) - timedelta(seconds=1)))
    with pytest.raises(ValueError, match="Invalid or expired"):
        await consume_staged_token(db_session, expired, actor_id=OPERATOR_ID, account_id=account, purpose="rotate")


@pytest.mark.asyncio
async def test_connect_uses_opaque_credential_from_protected_rest_entry(e2e_client, platform_admin, async_session_factory) -> None:
    path = "/api/capacity/cloudflare"
    headers = platform_admin.headers
    account = "f" * 32
    token = "browser-fixture-cloudflare-token"
    staged = e2e_client.post(f"{path}/credential", headers=headers, json={
        "account_id": account, "purpose": "connect", "api_token": token,
    })
    assert staged.status_code == 201 and token not in staged.text
    reference = staged.json()["credential_ref"]
    request = {
        "action": "connect", "account_id": account, "worker_ingress_origin": "https://workers.browser-fixture.test",
        "consent_version": CONSENT_TEXT_VERSION, "consent": True, "credential_ref": reference,
        "policy": {"workers": {
            "launcher_cap": 1, "slots_per_launcher": 1, "max_lifetime_seconds": 60, "work_kinds": ["workflow"],
        }, "monthly_limit_usd": "15.00"},
    }
    installation_id = None
    try:
        connected = e2e_client.patch(path, headers=headers, json=request)
        assert connected.status_code == 200, connected.text
        installation = connected.json()["installation"]
        installation_id = UUID(installation["installation_id"])
        assert installation["fleet_envelope_usd"] == "12.85"
        assert installation["spend"]["estimated_usd"] == "0.00"
        assert installation["spend"]["workers_paid_subscription_usd"] == "5.00"
        assert token not in connected.text and reference not in connected.text
        assert e2e_client.patch(path, headers=headers, json=request).status_code == 422
    finally:
        async with async_session_factory() as db:
            await db.execute(delete(CloudflareStagedToken).where(CloudflareStagedToken.id == UUID(reference)))
            if installation_id is not None:
                await db.execute(delete(CloudflareCredential).where(CloudflareCredential.installation_id == installation_id))
                await db.execute(delete(CloudflareInstallation).where(CloudflareInstallation.id == installation_id))
            await db.commit()


@pytest.mark.asyncio
async def test_canary_proof_requires_current_lease_worker_generation_and_revision(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    job, _ = await enable_job.enqueue_cloudflare_enable(
        db_session, _platform_operator(), installation_id=installation.id, expected_revision=1,
    )
    now = datetime.now(UTC)
    job.status = "running"
    job.lease_token = uuid4()
    job.lease_expires_at = now + timedelta(minutes=2)
    worker = ExecutionWorker(
        id=uuid4(), form="external", state="ready", capacity_scope="workers",
        runtime_profile="cloudflare-python-v1", memory_limit_bytes=128 * 2**20,
        capacity_resource_uid=uuid4().hex, remote_hello_sequence=1, remote_session_generation=7,
        remote_owner_lease_expires_at=now + timedelta(minutes=2),
    )
    db_session.add(worker)
    await db_session.flush()
    pin = ManagedCloudflareScope(installation.id, "workers", 1).pin(worker.id, 1)
    pin.canary_job_id, pin.canary_lease_token = job.id, job.lease_token
    db_session.add(pin)
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(cloudflare_canary, "get_db_context", context)
    proof_for = cloudflare_canary.cloudflare_sdk_canary_proof
    proof = await proof_for(worker.id, 7)
    assert proof is not None and await proof_for(worker.id, 8, received=proof) is None
    assert await proof_for(worker.id, 7, acknowledged=True) is None
    failure = ingress_probes.CanaryFailure("sdk_response", "http_status", 404)
    await cloudflare_canary.record_canary_failure(worker.id, 8, failure)
    await db_session.refresh(job)
    assert job.result is None
    job.result = {"prior_checkpoint": "retain"}
    await db_session.flush()
    await cloudflare_canary.record_canary_failure(worker.id, 7, failure)
    await db_session.refresh(job)
    assert job.result == {
        "prior_checkpoint": "retain",
        "canary_failure": {"worker_id": str(worker.id), "generation": 7, "reason": failure.as_result()},
    }
    assert await proof_for(worker.id, 7, received="a" * 64) is None
    assert await proof_for(worker.id, 7, received=proof) == proof
    assert await proof_for(worker.id, 7, acknowledged=True) == proof
    await db_session.refresh(pin)
    assert pin.canary_sdk_at is not None and pin.canary_ack_at is not None and pin.canary_sdk_generation == 7
    worker.remote_session_generation = 8
    await db_session.flush()
    assert await proof_for(worker.id, 8, acknowledged=True) is None
    proof8 = await proof_for(worker.id, 8)
    assert proof8 is not None and proof8 != proof
    assert await proof_for(worker.id, 8, received=proof8) == proof8
    await db_session.refresh(pin)
    assert pin.canary_ack_at is None and pin.canary_sdk_generation == 8
    assert await proof_for(worker.id, 8, acknowledged=True) == proof8
    worker.remote_session_generation = 7
    worker.state = "starting"
    await db_session.flush()
    assert await proof_for(worker.id, 7) is None
    worker.state = "ready"
    installation.desired_revision = 2
    await db_session.flush()
    assert await proof_for(worker.id, 7) is None
    installation.desired_revision = 1
    job.lease_expires_at = now - timedelta(seconds=1)
    await db_session.flush()
    assert await proof_for(worker.id, 7) is None
    await cloudflare_canary.record_canary_failure(worker.id, 7, ingress_probes.CanaryFailure("ack", "timeout"))
    await db_session.refresh(job)
    assert job.result is not None and job.result["canary_failure"]["reason"] == failure.as_result()


@pytest.mark.asyncio
async def test_enable_requires_signed_sdk_evidence_and_resumes_owned_uploads(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    installation.policy = {**installation.policy, "bounds_release": "d" * 64}
    job, reused = await enable_job.enqueue_cloudflare_enable(
        db_session, _platform_operator(), installation_id=installation.id, expected_revision=1,
    )
    assert not reused and job.payload == {"installation_id": str(installation.id), "revision": 1}
    lease_token = uuid4()
    job.status = "running"
    job.lease_token = lease_token
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(enable_job, "get_db_context", context)
    monkeypatch.setattr(cloudflare_spend, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    observed = {}
    writes = []

    class Provider:
        async def observe(self, *, api_token, intent):
            assert api_token == SecretStr("synthetic-cloudflare-management-token")
            return observed.get(intent.target.name)

        async def create(self, *, api_token, intent, scope_bearer):
            assert (scope_bearer is not None) == intent.target.name.endswith("-capacity")
            writes.append(intent.target.name)
            observed[intent.target.name] = ResourceObservation(intent, str(uuid4()), str(uuid4()))

        async def configure_endpoint(self, *, api_token, expected):
            name = expected.intent.target.name
            return ConfiguredWorkersScript(
                "https://capacity.example.test" if name.endswith("-capacity") else None,
                {} if name.endswith("-gateway") else {"OwnedNamespace": "a" * 32},
            )

    monkeypatch.setattr(enable_job, "WorkersRestProvisioner", lambda *_: Provider())
    monkeypatch.setattr(ingress_probes, "_public_addresses", AsyncMock(return_value=("203.0.113.10",)))
    reachable = ingress_probes.WorkerIngressProbeResult(
        installation.worker_ingress_origin, True, True, True,
        (ingress_probes.RouteProbe("GET", "/api/sdk/context", 401, True),),
    )
    refused = ingress_probes.WorkerIngressProbeResult(
        installation.worker_ingress_origin, True, False, False,
        (ingress_probes.RouteProbe("GET", "/api/sdk/context", 521, False, error_class="unexpected_status"),),
    )
    monkeypatch.setattr(ingress_probes, "_probe_address", AsyncMock(side_effect=[refused, reachable, reachable]))
    canary = AsyncMock(side_effect=[
        ingress_probes.RuntimeCanary(True, False, ingress_probes.CanaryFailure("sdk_response", "http_status", 404)),
        ingress_probes.RuntimeCanary(True, True),
    ])
    monkeypatch.setattr(enable_job, "run_workers_canary", canary)
    paid_reads = []

    def provider_read(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET" and request.url.host == "api.cloudflare.com"
        assert request.headers["Authorization"] == "Bearer synthetic-cloudflare-management-token"
        account_path = f"/client/v4/accounts/{installation.account_id}"
        paid_reads.append(request.url.path)
        if request.url.path == account_path:
            return httpx.Response(200, json={"success": True, "result": {"id": installation.account_id}})
        assert request.url.path == account_path + "/entitlements"
        return httpx.Response(200, json={"success": True, "result": [
            {"id": feature, "deleted_date": "", "allocation": {"type": "bool", "value": True}}
            for feature in ("workers.enabled", "containers.enabled")
        ]})

    running = PlatformJobContext(job.id, lease_token, None, str(OPERATOR_ID), "operator@example.test", "")
    payload = enable_job.CloudflareEnablePayload.model_validate(job.payload)
    release = SimpleNamespace(
        digest="d" * 64,
        manifest=SimpleNamespace(
            meter_bounds=SimpleNamespace(qualification=SimpleNamespace(expected_cpu_ms={"capacity": 9})),
        ),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider_read)) as http:
        with pytest.raises(PlatformJobRequiresAction) as ingress_denied:
            await enable_job._settle_enable(running, payload, http, release)
        assert ingress_denied.value.result == {
            "code": "CF_WORKER_INGRESS_UNVERIFIED",
            "canary_failure": {"stage": "ingress_http", "error_class": "unexpected_status", "status": 521},
        }
        assert ingress_denied.value.phase == "Worker Ingress Probe Failed (Ingress HTTP, Unexpected Status, HTTP 521)"
        canary.assert_not_awaited()
        with pytest.raises(PlatformJobRequiresAction) as denied:
            await enable_job._settle_enable(running, payload, http, release)
        assert denied.value.result == {
            "code": "CF_WORKER_INGRESS_UNVERIFIED",
            "canary_failure": {"stage": "sdk_response", "error_class": "http_status", "status": 404},
        }
        assert denied.value.phase == "Cloudflare Canary Failed (SDK Response, HTTP Status, HTTP 404)"
        await db_session.refresh(installation)
        assert installation.admitted_revision is None
        assert await enable_job._settle_enable(
            running, payload, http, release,
        ) == {"outcome": "enabled"}
    assert [name.rsplit("-", 1)[-1] for name in writes] == ["gateway", "launcher", "capacity"]
    assert canary.await_count == 2
    assert sum(path.endswith("/entitlements") for path in paid_reads) == 3
    await db_session.refresh(installation)
    assert installation.admitted_revision == 1


@pytest.mark.asyncio
async def test_account_rotation_switches_only_a_valid_same_account_version(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    admin = _platform_operator()
    candidate = SecretStr("test-rotated-account-token")
    receipt = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="rotation-capacity", desired_revision=1,
        artifact_digest="c" * 64, effect_id=uuid4(), state="present",
        provider_id=str(uuid4()), provider_version=str(uuid4()),
    )
    db_session.add(receipt)
    await db_session.flush()
    connection = await db_session.connection()
    locked = False

    @asynccontextmanager
    async def context():
        nonlocal locked
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            locked = True
            try:
                yield session
                await session.commit()
            finally:
                locked = False

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    replacement_version = 2
    subdomain_writes = 0

    def remote(request: httpx.Request) -> httpx.Response:
        nonlocal subdomain_writes
        assert not locked
        assert request.headers["Authorization"] == "Bearer " + candidate.get_secret_value()
        path = request.url.path
        if path.endswith("/subdomain"):
            result = {"enabled": True, "previews_enabled": False}
            if request.method == "POST":
                subdomain_writes += 1
                assert json.loads(request.content) == result
                if replacement_version == 3:
                    return httpx.Response(403)
            else:
                assert request.method == "GET"
        elif path.endswith("/entitlements"):
            result = [
                {"id": feature, "deleted_date": "", "allocation": {"type": "bool", "value": True}}
                for feature in ("workers.enabled", "containers.enabled", "containers.enabled")
            ]
        elif path.endswith("/settings"):
            result = {"bindings": [{
                "name": OWNER_BINDING, "type": "plain_text",
                "text": worker_owner_value(lifecycle_job._resource_intent(receipt)),
            }]}
        elif path.endswith("/deployments"):
            result = {"deployments": [{
                "id": receipt.provider_id,
                "versions": [{"percentage": 100, "version_id": receipt.provider_version}],
            }]}
        else:
            assert path == f"/client/v4/accounts/{installation.account_id}"
            result = {"id": "b" * 32 if replacement_version == 2 else installation.account_id}
        return httpx.Response(200, json={
            "success": True, "result": result,
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        adapter = WorkersRestCleanup(http)
        monkeypatch.setattr(lifecycle_job, "WorkersRestCleanup", lambda _http: adapter)
        for replacement_version in (2, 3, 4):
            ref, _ = await stage_account_token(
                db_session, actor_id=OPERATOR_ID, account_id=installation.account_id, purpose="rotate", token=candidate,
            )
            await db_session.flush()
            job, reused = await lifecycle_job.enqueue_cloudflare_account_token_rotation(
                db_session, admin, installation_id=installation.id, expected_revision=1, replacement=ref,
            )
            assert not reused and job.payload["replacement_version"] == replacement_version
            assert candidate.get_secret_value() not in json.dumps(job.payload)
            assert str(ref) not in json.dumps(job.payload)
            assert installation.api_token_version == 1
            assert (await get_cloudflare_capacity_status(db_session, admin)).latest_job_id == job.id
            token = uuid4()
            job.lease_token = token
            job.status = "running"
            await db_session.flush()
            payload = lifecycle_job.CloudflareRotateAccountPayload.model_validate(job.payload)
            running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")
            handler = lifecycle_job.CLOUDFLARE_ROTATE_ACCOUNT_DEFINITION.handler
            if replacement_version in (2, 3):
                with pytest.raises(PlatformJobRequiresAction) as denied:
                    await handler(running, payload)
                assert denied.value.result == {"code": {
                    2: "CF_MANAGEMENT_CREDENTIAL",
                    3: "CF_WORKERS_WRITE_UNVERIFIED",
                }[replacement_version]}
                job.status = "requires_action"
                rejected = await db_session.get(
                    CloudflareCredential, (installation.id, "account_token", "", replacement_version),
                )
                assert rejected is not None and rejected.ciphertext is None
                previous = await db_session.get(CloudflareCredential, (installation.id, "account_token", "", 1))
                assert previous is not None and previous.ciphertext is not None
            else:
                assert await handler(running, payload) == {"outcome": "rotated", "credential_version": 4}
            await db_session.refresh(installation)
    assert subdomain_writes == 2
    assert installation.api_token_version == 4 and installation.control_root_version == 1
    previous = await db_session.get(
        CloudflareCredential, (installation.id, "account_token", "", 1), populate_existing=True,
    )
    assert previous is not None and previous.ciphertext is None
    audit = await db_session.scalar(select(AuditLog).where(
        AuditLog.resource_id == installation.id, AuditLog.action == "capacity.cloudflare_account_token_rotated",
    ))
    assert audit is not None
    assert candidate.get_secret_value() not in json.dumps(audit.details)


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["control_root", "scope_bearer"])
async def test_rotate_worker_credential_is_drain_first(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, purpose,
) -> None:
    admin = _platform_operator()
    scope = "workers" if purpose == "scope_bearer" else ""
    previous_key = (installation.id, purpose, scope, 1)
    if scope:
        db_session.add(seal_credential(
            CredentialRef(installation.id, installation.account_id, purpose, scope, 1),
            SecretStr("previous-scope-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
        ))
    installation.desired_state = "enabled"
    with pytest.raises(CloudflarePolicyConflict, match="Scale in"):
        await lifecycle_job.enqueue_cloudflare_worker_credential_rotation(
            db_session, admin, installation_id=installation.id, expected_revision=1, credential=purpose,
        )
    installation.desired_state = "scaled_in"
    await db_session.flush()
    job, reused = await lifecycle_job.enqueue_cloudflare_worker_credential_rotation(
        db_session, admin, installation_id=installation.id, expected_revision=1, credential=purpose,
    )
    assert not reused and job.payload["replacement_version"] == 2
    token = uuid4()
    job.lease_token = token
    job.status = "running"
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    payload = lifecycle_job.CloudflareRotateWorkerPayload.model_validate(job.payload)
    running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")
    assert await lifecycle_job.CLOUDFLARE_ROTATE_WORKER_DEFINITION.handler(running, payload) == {
        "outcome": "rotated", "credential_version": 2,
    }
    previous = await db_session.get(CloudflareCredential, previous_key, populate_existing=True)
    active = await db_session.get(CloudflareCredential, (*previous_key[:3], 2))
    await db_session.refresh(installation)
    assert previous is not None and previous.ciphertext is None
    assert active is not None and active.ciphertext is not None
    assert installation.control_root_version == (1 if scope else 2)
    assert installation.desired_revision == (2 if scope else 1)


@pytest.mark.asyncio
async def test_scale_in_job_uses_platform_authority_and_current_lease(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, lifecycle_audit_actor,
) -> None:
    organization_id = uuid4()
    provider_member = Caller(
        UserPrincipal(OPERATOR_ID, "member@example.test", organization_id, is_provider_org=True),
        AuthorizationContext(OPERATOR_ID, organization_id, uuid4(), False, frozenset()),
    )
    with pytest.raises(HTTPException) as denied:
        await lifecycle_job.enqueue_cloudflare_disable(
            db_session, provider_member, installation_id=installation.id, expected_revision=1, action="scale_in",
        )
    assert denied.value.status_code == 403
    assert installation.desired_revision == 1

    admin = _platform_operator()
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    job, reused = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="scale_in",
    )
    assert not reused and job.payload_version == 1
    assert installation.desired_state == "scaled_in" and installation.admitted_revision is None
    duplicate, reused = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="scale_in",
    )
    assert reused and duplicate.id == job.id
    token = uuid4()
    job.lease_token = token
    job.status = "running"
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
    running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")
    assert await lifecycle_job.CLOUDFLARE_DISABLE_DEFINITION.handler(running, payload) == {"outcome": "scaled_in"}
    assert installation.api_token_version == 1
    actions = set(await db_session.scalars(select(AuditLog.action).where(AuditLog.resource_id == installation.id)))
    assert {"capacity.cloudflare_scale_in", "capacity.cloudflare_scaled_in"} <= actions


@pytest.mark.asyncio
async def test_disconnect_resumes_owned_script_delete_until_namespace_absence(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, lifecycle_audit_actor,
) -> None:
    admin = _platform_operator()
    resource = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="test-capacity",
        desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="present",
        provider_id=str(uuid4()), provider_version=str(uuid4()),
    )
    db_session.add(resource)
    job, _ = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="disconnect",
    )
    token = uuid4()
    job.lease_token = token
    job.status = "running"
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    intent = ResourceIntent(
        ResourceTarget(installation.id, installation.account_id, "workers", "worker_script", resource.target_name),
        resource.effect_id, 1, resource.artifact_digest,
    )
    script_exists = True
    namespace_exists = True
    mutations = []

    async def remote(request: httpx.Request) -> httpx.Response:
        nonlocal script_exists
        assert request.url.host == "api.cloudflare.com"
        assert request.headers["Authorization"] == "Bearer synthetic-cloudflare-management-token"
        if request.url.path.endswith("/settings"):
            if not script_exists:
                return httpx.Response(404)
            result = {"bindings": [{
                "name": OWNER_BINDING, "type": "plain_text", "text": worker_owner_value(intent),
            }]}
        elif request.url.path.endswith("/deployments"):
            result = {"deployments": [{
                "id": resource.provider_id,
                "versions": [{"percentage": 100, "version_id": resource.provider_version}],
            }]}
        elif request.url.path.endswith("/namespaces"):
            page = int(request.url.params["page"])
            assert page in (1, 2)
            result = [{"id": str(page), "script": resource.target_name if page == 2 and namespace_exists else "other"}]
            return httpx.Response(200, json={
                "success": True, "result": result, "result_info": {"page": page, "total_pages": 2, "total_count": 2},
            })
        else:
            assert request.method == "DELETE" and request.url.path.endswith("/test-capacity")
            if request.url.params.get("force") != "true":
                return httpx.Response(400, json={"success": False, "errors": [{"message": "Worker owns a namespace"}]})
            await db_session.refresh(resource)
            assert resource.state == "deleting"
            script_exists = False
            mutations.append("DELETE")
            return httpx.Response(204)
        return httpx.Response(200, json={"success": True, "result": result})

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
    running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")

    pauses = []

    async def propagate(delay):
        nonlocal namespace_exists
        pauses.append(delay)
        await db_session.refresh(installation)
        assert installation.retired_at is None and installation.api_token_version == 1
        if len(pauses) == 2:
            namespace_exists = False

    monkeypatch.setattr(lifecycle_job, "asyncio", SimpleNamespace(sleep=propagate))
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        adapter = WorkersRestCleanup(http)
        monkeypatch.setattr(lifecycle_job, "WorkersRestCleanup", lambda _http: adapter)
        assert await lifecycle_job.CLOUDFLARE_DISABLE_DEFINITION.handler(running, payload) == {"outcome": "disconnected"}
    await db_session.refresh(resource)
    await db_session.refresh(installation)
    assert mutations == ["DELETE"]
    assert pauses == [2, 4]
    assert resource.state == "absent" and resource.observed_at is not None
    assert installation.retired_at is not None and installation.api_token_version is None
    actions = set(await db_session.scalars(select(AuditLog.action).where(AuditLog.resource_id == installation.id)))
    assert {"capacity.cloudflare_disconnect", "capacity.cloudflare_retired"} <= actions


@pytest.mark.asyncio
async def test_cleanup_waits_for_an_unsettled_create_before_confirming_absence(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    receipt = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="settlement-capacity",
        desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="creating",
    )
    db_session.add(receipt)
    db_session.add(CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="current-capacity",
        desired_revision=2, artifact_digest="d" * 64, effect_id=uuid4(), state="creating",
    ))
    await db_session.flush()
    adapter = SimpleNamespace(observe=AsyncMock(return_value=None), verify_absent=AsyncMock())
    assert await lifecycle_job._cleanup_one(db_session, installation, adapter, before_revision=2) == "progressed"
    assert receipt.observed_at is not None and receipt.state == "creating"
    assert await lifecycle_job._cleanup_one(db_session, installation, adapter, before_revision=2) == "settling"
    adapter.verify_absent.assert_not_awaited()
    receipt.observed_at -= lifecycle_job.CREATE_SETTLEMENT_WINDOW
    assert await lifecycle_job._cleanup_one(db_session, installation, adapter, before_revision=2) == "progressed"
    adapter.verify_absent.assert_awaited_once()
    assert receipt.state == "absent"
    assert await lifecycle_job._cleanup_one(db_session, installation, adapter, before_revision=2) == "complete"


@pytest.mark.asyncio
async def test_cleanup_deadline_excludes_queue_time(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    admin = _platform_operator()
    job, _ = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="scale_in",
    )
    token = uuid4()
    job.status = "running"
    job.lease_token = token
    job.created_at = datetime.now(UTC) - timedelta(days=1)
    job.started_at = datetime.now(UTC)
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    # A worker remains in the drain phase. Observe the decision to wait before
    # moving the attempt timestamp past its deadline.
    monkeypatch.setattr(lifecycle_job, "cloudflare_workers_remain", AsyncMock(return_value=True))

    class DrainWaiting(Exception):
        pass

    monkeypatch.setattr(lifecycle_job, "asyncio", SimpleNamespace(sleep=AsyncMock(side_effect=DrainWaiting)))
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
    running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")
    with pytest.raises(DrainWaiting):
        await lifecycle_job._settle_disable(running, payload, SimpleNamespace())
    job.started_at -= lifecycle_job.DRAIN_DEADLINE
    await db_session.flush()
    with pytest.raises(PlatformJobRequiresAction) as expired:
        await lifecycle_job._settle_disable(running, payload, SimpleNamespace())
    assert expired.value.result == {"code": "CF_DRAIN_TIMEOUT"}


@pytest.mark.asyncio
async def test_deletion_deadline_requires_action_across_attempts(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    admin = _platform_operator()
    job, _ = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="disconnect",
    )
    job.status = "running"
    job.started_at = datetime.now(UTC) - lifecycle_job.DRAIN_DEADLINE
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(lifecycle_job, "cloudflare_workers_remain", AsyncMock(return_value=False))
    monkeypatch.setattr(lifecycle_job, "_cleanup_one", AsyncMock(side_effect=lifecycle_job.DeletionSettling()))
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
    for attempt in (1, 2, 3):
        job.attempt = attempt
        job.lease_token = uuid4()
        await db_session.flush()
        running = PlatformJobContext(job.id, job.lease_token, None, str(OPERATOR_ID), admin.principal.email, "")
        with pytest.raises(PlatformJobRequiresAction) as expired:
            await lifecycle_job._settle_disable(running, payload, SimpleNamespace())
        assert expired.value.result == {"code": "CF_CLEANUP_PENDING"}


@pytest.mark.asyncio
async def test_reconcile_dedupes_schedule_and_closes_admission_on_missing_deployment(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    resources = {}
    for role in ("launcher", "capacity"):
        resource = CloudflareOwnedResource(
            id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
            scope="workers", kind="worker_script", target_name=f"reconcile-{role}",
            desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="present",
            provider_id=str(uuid4()), provider_version=str(uuid4()), namespace_ids={"OwnedNamespace": str(uuid4())},
        )
        resources[resource.target_name] = resource
        db_session.add(resource)
    await db_session.flush()
    now = datetime.now(UTC)
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now)
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now)
    jobs = (await db_session.scalars(select(PlatformJob).where(
        PlatformJob.job_type == lifecycle_job.RECONCILE_JOB,
        PlatformJob.resource_id == str(installation.id),
    ))).all()
    assert len(jobs) == 1 and jobs[0].payload_version == 1
    job = jobs[0]
    token = uuid4()
    job.lease_token = token
    job.status = "running"
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    status_code = 503

    def remote(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/graphql"):
            body = json.loads(request.content)
            assert body["variables"]["scripts"] == sorted(resources)
            return httpx.Response(200, json={"data": {"viewer": {"accounts": [{
                "workersInvocationsAdaptive": [
                    {"dimensions": {"scriptName": name}, "sum": {"requests": 3}} for name in resources
                ],
            }]}}})
        assert request.method == "GET"
        if status_code != 200:
            assert request.url.path.endswith("/settings")
            return httpx.Response(status_code)
        target, operation = request.url.path.split("/")[-2:]
        resource = resources[target]
        if operation == "settings":
            result = {"bindings": [{
                "name": OWNER_BINDING, "type": "plain_text",
                "text": worker_owner_value(lifecycle_job._resource_intent(resource)),
            }]}
        else:
            assert operation == "deployments"
            result = {"deployments": [{
                "id": resource.provider_id,
                "versions": [{"percentage": 100, "version_id": resource.provider_version}],
            }]}
        return httpx.Response(200, json={"success": True, "result": result})

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    running = PlatformJobContext(job.id, token, None, "system", "", "Bifrost")
    payload = lifecycle_job.CloudflareReconcilePayload.model_validate(job.payload)
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        adapter = WorkersRestCleanup(http)
        monkeypatch.setattr(lifecycle_job, "WorkersRestCleanup", lambda _http: adapter)
        with pytest.raises(PlatformJobFailure) as unavailable:
            await lifecycle_job.CLOUDFLARE_RECONCILE_DEFINITION.handler(running, payload)
        assert unavailable.value.retryable and lifecycle_job.CLOUDFLARE_RECONCILE_DEFINITION.policy.retry_on_failure
        status_code = 200
        assert await lifecycle_job.CLOUDFLARE_RECONCILE_DEFINITION.handler(running, payload) == {"outcome": "verified"}
        status = await get_cloudflare_capacity_status(db_session, _platform_operator())
        assert status.installation is not None and status.installation.admission_open
        assert status.installation.spend is not None and status.installation.spend.analytics is not None
        assert status.installation.spend.analytics.request_difference == Decimal(6)
        status_code = 404
        with pytest.raises(PlatformJobRequiresAction) as missing:
            await lifecycle_job.CLOUDFLARE_RECONCILE_DEFINITION.handler(running, payload)
        assert missing.value.result == {"code": "CF_DEPLOYMENT_UNVERIFIED"}
    await db_session.refresh(installation)
    assert installation.admitted_revision is None


@pytest.mark.asyncio
@pytest.mark.parametrize("primary", ["static", "job", "external"])
async def test_supervisor_reserves_and_retires_without_restarting(
    db_session: AsyncSession, installation: CloudflareInstallation, primary: str, monkeypatch,
) -> None:
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    for module in (controller, external_controller, cloudflare_supervisor, cloudflare_spend):
        monkeypatch.setattr(module, "get_db_context", context)
    monkeypatch.setattr(controller, "_publish_live", AsyncMock(return_value=controller.LiveControllers(
        ids=frozenset(), versions=frozenset({"test"}),
    )))
    await KubernetesExecutionService(db_session).update_policy({"max_workers": 1, "warm_workers": 0}, updated_by="test")
    # Each case supplies the entire primary cohort. The test stack's own
    # in-process workers would otherwise fill demand in the Job and VM cases.
    await db_session.execute(update(ExecutionWorker).values(state="retired"))
    bearer = "test-scope-bearer-" + "a" * 30
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr(bearer), actor=str(OPERATOR_ID), now=datetime.now(UTC),
    ))
    primary_worker = ExecutionWorker(
        id=uuid4(), form=primary, state="ready",
        capacity_scope="vm" if primary == "external" else None,
        heartbeat_at=datetime.now(UTC), supported_handlers=["workflow.execute"],
        memory_budget_bytes=128 * 2**20, memory_reserved_bytes=128 * 2**20,
    )
    db_session.add(primary_worker)
    db_session.add(WorkDispatch(
        family="workflow", handler="workflow.execute", handler_version=1, subject_id=uuid4(),
        message={}, payload={}, cloudflare_revision="c" * 64, memory_request_bytes=64 * 2**20,
        created_at=datetime.now(UTC) - timedelta(minutes=1),
    ))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    await db_session.flush()

    resources = {}
    writes = []

    async def remote(request: httpx.Request) -> httpx.Response:
        if request.url.host == "vm.example.test":
            assert request.headers["Authorization"] == "Bearer " + "p" * 40
            return httpx.Response(200, json={
                "protocol_version": 1, "scope": "vm", "worker_version": "vm-build",
                "memory_limit_bytes": 128 * 2**20, "max_workers": 1, "max_lifetime_seconds": 3600,
            })
        assert request.headers["Authorization"] == f"Bearer {bearer}"
        if request.url.path == "/v1/capabilities":
            return httpx.Response(200, json={
                "protocol_version": 2, "scope": "workers", "worker_version": "test",
                "runtime_profile": "cloudflare-python-v1", "memory_limit_bytes": 256 * 2**20,
                "max_workers": 1, "max_lifetime_seconds": 3600,
            })
        worker_id = request.url.path.split("/")[3]
        if request.method == "GET":
            resource = resources.get(worker_id)
            return httpx.Response(200, json=resource) if resource else httpx.Response(404)
        writes.append(request.method)
        body = json.loads(request.content)
        if request.method == "PUT":
            assert await db_session.get(CloudflareWorkerReservation, UUID(worker_id)) is not None
            resources[worker_id] = {
                "scope": "workers", "worker_id": worker_id, "worker_version": "test",
                "resource_uid": body["resource_uid"], "state": "prepared",
            }
        else:
            assert body["resource_uid"] == resources[worker_id]["resource_uid"]
            resources[worker_id]["state"] = "starting" if request.method == "POST" else "deleted"
        return httpx.Response(200, json=resources[worker_id])

    settings = Settings(
        _env_file=None, secret_key="synthetic-supervisor-test-key-" + "x" * 32,
        worker_capacity_url="https://vm.example.test" if primary == "external" else None,
        worker_capacity_token="p" * 40 if primary == "external" else None,
    )
    controllers = {}
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        async def sweep():
            await cloudflare_supervisor._reconcile_scope(
                ManagedCloudflareScope(installation.id, "workers", 1),
                settings, http, controllers, kubernetes=primary == "job",
            )

        await sweep()
        assert not writes
        installation.desired_state = "enabled"
        installation.admitted_revision = 1
        if primary == "static":
            primary_worker.memory_reserved_bytes = 0
            installation.policy = {
                **installation.policy,
                "limits": {**installation.policy["limits"], "shared_managed_worker_cap": None},
            }
        await db_session.flush()
        if primary == "static":
            await sweep()
            assert not writes
            primary_worker.memory_reserved_bytes = 128 * 2**20
            await db_session.flush()
        await sweep()
        assert writes == ["PUT", "POST"]
        owned = tuple(controllers.values())
        installation.desired_state = "scaled_in"
        installation.admitted_revision = None
        installation.desired_revision = 2
        db_session.add(CloudflareCapacityScope(
            installation_id=installation.id, scope="workers", endpoint="https://new-capacity.example.test",
            deployment_revision=2, bearer_version=1,
        ))
        await db_session.flush()
        assert set(await cloudflare_supervisor._installations()) == {
            ManagedCloudflareScope(installation.id, "workers", revision) for revision in (1, 2)
        }
        await sweep()
        assert tuple(controllers.values()) == owned
        assert writes == ["PUT", "POST", "DELETE"]
        worker_id = UUID(next(iter(resources)))
        worker = await db_session.get(ExecutionWorker, worker_id)
        assert worker is not None
        await db_session.refresh(worker)
        assert worker.capacity_resource_deleted_at is not None


@pytest.fixture
async def installation(db_session: AsyncSession) -> CloudflareInstallation:
    installation_id = await create_installation(db_session, "a" * 32)
    row = await db_session.get(CloudflareInstallation, installation_id)
    assert row is not None
    return row


async def create_installation(
    db_session: AsyncSession, account_id: str, *, consent_version: str = CONSENT_TEXT_VERSION,
    rated_slots: int = 2,
) -> UUID:
    zero = dict.fromkeys(WORKERS_RATES, Decimal(0))
    return await create_cloudflare_installation(
        db_session,
        account_id=account_id,
        worker_ingress_origin="https://workers.example.test",
        consent_version=consent_version,
        actor_id=OPERATOR_ID,
        verified_api_token=SecretStr("synthetic-cloudflare-management-token"),
        policy=CloudflareCapacityPolicy(
            workers=CloudflareWorkersPolicy(
                launcher_cap=1, slots_per_launcher=2, max_lifetime_seconds=3600, work_kinds=frozenset({"workflow"}),
            ),
            monthly_limit_usd=Decimal("10.00"),
            shared_managed_worker_cap=8,
        ),
        bounds=WorkersReleaseMeterBounds(
            60, zero, zero, "fixture-release", rated_slots, TEST_LAUNCHER_CAPS, TEST_CPU_LIMITS, 500_000,
        ),
    )


@pytest.mark.asyncio
async def test_usage_envelope_migration_preserves_saved_policy(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    scripts = ScriptDirectory(str(Path(__file__).resolve().parents[3] / "alembic"))
    migration = scripts.get_revision("20261120_cf_usage_only_envelope")
    assert migration is not None
    module = migration.module
    original = {
        **installation.policy,
        "rate_table_version": WORKERS_LEGACY_RATE_TABLE_VERSION,
        "fleet_envelope_usd": "13.31",
    }
    installation.policy = original
    await db_session.flush()
    connection = await db_session.connection()

    def migrate(sync_connection: Connection, operation: Callable[[], None]) -> None:
        with Operations.context(MigrationContext.configure(sync_connection)):
            operation()

    await connection.run_sync(migrate, module.downgrade)
    await db_session.refresh(installation)
    assert installation.policy == {**original, "fleet_envelope_usd": "18.31"}
    await connection.run_sync(migrate, module.upgrade)
    await db_session.refresh(installation)
    assert installation.policy == original


@pytest.mark.asyncio
async def test_scope_writer_waits_for_installation_admission_lock(async_session_factory) -> None:
    async with async_session_factory() as db:
        installation_id = await create_installation(db, "a" * 32)
        db.add(seal_credential(
            CredentialRef(installation_id, "a" * 32, "scope_bearer", "workers", 1),
            SecretStr("synthetic-scope-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
        ))
        await db.flush()
        db.add(CloudflareCapacityScope(
            installation_id=installation_id, scope="workers", endpoint="https://capacity.example.test",
            deployment_revision=1, bearer_version=1,
        ))
        await db.commit()
    statement = update(CloudflareCapacityScope).where(
        CloudflareCapacityScope.installation_id == installation_id,
    ).values(bearer_version=1)
    try:
        async with async_session_factory() as reader, async_session_factory() as writer:
            await reader.scalar(select(CloudflareInstallation.id).where(
                CloudflareInstallation.id == installation_id,
            ).with_for_update(key_share=True))
            await writer.execute(text("SET LOCAL lock_timeout = '100ms'"))
            with pytest.raises(DBAPIError, match="lock timeout"):
                await writer.execute(statement)
            await writer.rollback()
            await reader.rollback()
            await writer.execute(statement)
            await writer.commit()
    finally:
        async with async_session_factory() as db:
            await db.execute(delete(CloudflareCapacityScope).where(
                CloudflareCapacityScope.installation_id == installation_id,
            ))
            row = await db.get(CloudflareInstallation, installation_id)
            assert row is not None
            row.desired_state = "disconnected"
            await db.flush()
            await retire_cloudflare_installation(db, installation_id=installation_id, expected_revision=1)
            await db.execute(delete(CloudflareCredential).where(CloudflareCredential.installation_id == installation_id))
            await db.delete(row)
            await db.commit()


@pytest.mark.asyncio
async def test_connection_persists_consent_and_account_token_without_admitting_work(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    assert (installation.consent_actor, installation.consent_version) == (str(OPERATOR_ID), CONSENT_TEXT_VERSION)
    assert installation.desired_state == "scaled_in" and installation.admitted_revision is None
    assert installation.api_token_version == 1
    credential = await db_session.get(CloudflareCredential, (
        installation.id, "account_token", "", installation.api_token_version,
    ))
    assert credential is not None
    ref = CredentialRef(installation.id, installation.account_id, "account_token", "", 1)
    assert open_credential(ref, credential) == SecretStr("synthetic-cloudflare-management-token")

    with pytest.raises(CloudflareInstallationExists, match="already exists"):
        await create_installation(db_session, "b" * 32)


@pytest.mark.asyncio
async def test_disconnect_retires_with_receipts_and_allows_reconnect(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    installation_id = installation.id
    installation.desired_state = "disconnected"
    installation.desired_revision += 1
    await db_session.flush()
    resource = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation_id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="disconnect-receipt",
        desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="creating",
    )
    db_session.add(resource)
    await db_session.flush()
    with pytest.raises(CloudflarePolicyConflict, match="cleanup is pending"):
        await retire_cloudflare_installation(db_session, installation_id=installation_id, expected_revision=2)
    with pytest.raises(CloudflareInstallationExists):
        await create_installation(db_session, "b" * 32)

    resource.state = "absent"
    resource.observed_at = datetime.now(UTC)
    resource.provider_id = str(uuid4())
    resource.provider_version = str(uuid4())
    await db_session.flush()
    await retire_cloudflare_installation(db_session, installation_id=installation_id, expected_revision=2)
    assert installation.retired_at is not None and installation.api_token_version is None
    credential = await db_session.get(CloudflareCredential, (installation_id, "account_token", "", 1))
    assert credential is not None and credential.ciphertext is None
    new_id = await create_installation(db_session, installation.account_id)
    assert new_id != installation_id
    assert await db_session.get(CloudflareOwnedResource, resource.id) is not None
    assert await lifecycle_job.usage_script_identities(db_session, new_id) == {resource.target_name: {}}
    db_session.add(CloudflareOwnedResource(
        id=uuid4(), installation_id=new_id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name=resource.target_name,
        desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="creating",
    ))
    await db_session.flush()

    with pytest.raises(IntegrityError, match="tombstone cannot authorize"):
        async with db_session.begin_nested():
            resource.state = "creating"
            await db_session.flush()
    with pytest.raises(IntegrityError, match="tombstone must be retained"):
        async with db_session.begin_nested():
            await db_session.delete(resource)
            await db_session.flush()


@pytest.mark.asyncio
async def test_connection_requires_current_consent_and_slot_rated_bounds(db_session: AsyncSession) -> None:
    with pytest.raises(ValueError, match="current text"):
        await create_installation(db_session, "a" * 32, consent_version="obsolete")
    with pytest.raises(ValueError, match="slot count"):
        await create_installation(db_session, "a" * 32, rated_slots=1)


@pytest.mark.asyncio
async def test_active_token_reference_requires_matching_version(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            installation.api_token_version = 2
            await db_session.flush()
            await db_session.execute(text("SET CONSTRAINTS fk_cf_active_account_token IMMEDIATE"))


@pytest.mark.asyncio
async def test_scope_bearer_reference_is_versioned_and_scope_bound(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    now = datetime.now(UTC)
    ref = CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1)
    value = SecretStr("workers-scope-test-bearer")
    db_session.add(seal_credential(ref, value, actor="operator", now=now))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    await db_session.flush()
    db_session.expunge_all()

    scope = await db_session.get(CloudflareCapacityScope, (ref.installation_id, ref.scope, 1))
    assert scope is not None and scope.bearer_version == ref.version
    credential = await db_session.get(CloudflareCredential, (ref.installation_id, ref.purpose, ref.scope, ref.version))
    assert credential is not None and open_credential(ref, credential) == value

    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            db_session.add(CloudflareCapacityScope(
                installation_id=ref.installation_id, scope="other-scope",
                endpoint="https://other.example.test", deployment_revision=1, bearer_version=1,
            ))
            await db_session.flush()


@pytest.mark.asyncio
async def test_exact_budget_boundary_closes_reservations(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    installation.policy = {
        **installation.policy, "limits": {**installation.policy["limits"], "monthly_limit_usd": "120.09"},
    }
    installation.desired_state, installation.admitted_revision = "enabled", 1
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr("synthetic-boundary-test-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
    ))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    start, _ = usage_period(datetime.now(UTC))
    db_session.add(CloudflareUsageActivity(
        installation_id=installation.id, period_start=start, sdk_requests=200_000,
    ))
    await db_session.flush()
    assert await db_session.scalar(select(cloudflare_estimated_usage()).where(
        CloudflareInstallation.id == installation.id,
    )) == Decimal("120.09")
    assert await ManagedCloudflareScope(installation.id, "workers", 1).lock_admission(db_session) is None


@pytest.mark.asyncio
async def test_ledger_splits_open_incarnation_at_utc_rollover(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    boundary = datetime(2027, 1, 1, tzinfo=UTC)
    db_session.add(CloudflareUsageIncarnation(
        worker_id=uuid4(), installation_id=installation.id, started_at=boundary - timedelta(seconds=10), slots=3,
    ))
    db_session.add(CloudflareUsageActivity(
        installation_id=installation.id, period_start=datetime(2026, 12, 1, tzinfo=UTC),
        dispatched_runs=5, sdk_requests=9,
    ))
    await db_session.flush()
    dec = await read_workers_usage(db_session, installation.id, boundary - timedelta(seconds=2))
    jan = await read_workers_usage(db_session, installation.id, boundary + timedelta(seconds=4))
    assert dec is not None and (dec.launcher_seconds, dec.slot_seconds, dec.dispatched_runs, dec.sdk_requests) == (8, 24, 5, 9)
    assert jan is not None and (jan.launcher_seconds, jan.slot_seconds, jan.dispatched_runs, jan.sdk_requests) == (4, 12, 0, 0)


@pytest.mark.asyncio
async def test_admission_prices_the_qualified_ledger_bounds(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    start, _ = usage_period(datetime.now(UTC))
    elapsed = min(1000, int((datetime.now(UTC) - start).total_seconds()))
    current = CloudflareUsageIncarnation(
        worker_id=uuid4(), installation_id=installation.id, started_at=start,
        deleted_at=start + timedelta(seconds=elapsed), slots=2,
        meter_release_digest="a" * 64, meter_rate_version=WORKERS_CALIBRATED_RATE_TABLE_VERSION,
    )
    db_session.add_all([
        current,
        CloudflareUsageIncarnation(
            worker_id=uuid4(), installation_id=installation.id, started_at=start - timedelta(hours=1),
            deleted_at=start - timedelta(seconds=1), slots=4,
        ),
        CloudflareUsageActivity(
            installation_id=installation.id, period_start=start, dispatched_runs=11, sdk_requests=17,
        ),
    ])
    await db_session.flush()
    price = select(cloudflare_estimated_usage()).where(CloudflareInstallation.id == installation.id)
    # Per-second launcher and slot charges from the qualified bounds and
    # published rates. The out-of-period closed incarnation contributes zero.
    assert await db_session.scalar(price) == (
        Decimal("0.01681260") + elapsed * Decimal("0.000855853")
    )
    current.meter_release_digest = None
    await db_session.flush()
    assert await db_session.scalar(price) == (
        Decimal("0.01681260") + elapsed * Decimal("0.001447353")
    )
    receipt = CloudflareLauncherUsage(sqlite_rows_written=120, do_requests=22, log_events=8, exhausted=True)
    current.meter_release_digest = "b" * 64
    current.meter_rate_version = WORKERS_INVENTORY_RATE_TABLE_VERSION
    current.meter_bounds = {
        "caps": TEST_LAUNCHER_CAPS,
        "second_usd": "0.000681378",
    }
    # The production guard charges a floor per meter and retains amounts
    # above the cap even for a regressing or uncertain final receipt.
    # The launcher, two control objects and two slots each reserve 0.128 GB.
    lifetime_base = Decimal("0.01681260") + elapsed * Decimal("0.00068457800") + Decimal("0.00004280")
    month_seconds = Decimal((usage_period(start)[1] - start).total_seconds())
    lifetime_base += Decimal("0.022") * elapsed / month_seconds
    fallback = lifetime_base + Decimal("0.000106")
    await db_session.flush()
    assert abs((await db_session.scalar(price)) - (fallback)) < Decimal("1e-12")
    current.meter_final = receipt.model_dump()
    await db_session.flush()
    assert abs((await db_session.scalar(price)) - (lifetime_base + Decimal("0.00012810"))) < Decimal("1e-12")
    current.meter_terminal = {"sqlite_rows_written": 150, "do_requests": 18, "log_events": 1}
    current.meter_uncertain = True
    await db_session.flush()
    assert abs((await db_session.scalar(price)) - (lifetime_base + Decimal("0.00015810"))) < Decimal("1e-12")
    saved_bounds = current.meter_bounds
    current.meter_bounds = {"caps": {"do_requests": 1}, "second_usd": "0.00068126550"}
    await db_session.flush()
    assert await db_session.scalar(price) is None
    current.meter_bounds = saved_bounds
    installation.policy = {**installation.policy, "rate_table_version": "previous-rate-table"}
    await db_session.flush()
    assert await db_session.scalar(price) is None
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None
    assert status.installation.spend.state == "ledger_unavailable"
    assert status.installation.spend.rate_table_version == "unavailable"


@pytest.mark.asyncio
async def test_idle_capacity_profile_reserves_request_and_cpu_and_reports_mixed_rate_coverage(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    period, _ = usage_period(now)
    price = select(cloudflare_estimated_usage()).where(CloudflareInstallation.id == installation.id)
    assert await db_session.scalar(price) == 0
    async def charge() -> None:
        await cloudflare_spend.record_capacity_request(db_session, installation.id, 400)

    reserve = AsyncMock(side_effect=charge)

    def provider(request: httpx.Request) -> httpx.Response:
        reserve.assert_awaited_once()
        assert request.url.path == "/v1/capabilities"
        return httpx.Response(200, json={
            "protocol_version": 2, "scope": "workers", "worker_version": "fixture-release",
            "runtime_profile": "cloudflare-python-v1", "memory_limit_bytes": 128 * 1024**2,
            "max_workers": 1, "max_lifetime_seconds": 120,
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        await external_controller.ExternalCapacityClient(
            "https://capacity.example.test", "x" * 32, http=http, reserve_request=reserve,
        ).profile()
    assert await db_session.scalar(price) == Decimal("0.00000830")
    activity = await db_session.get(CloudflareUsageActivity, (installation.id, period))
    assert activity is not None
    activity.tenant_dispatches = 1
    await db_session.flush()
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None and status.installation.spend is not None
    rows = status.installation.spend.coverage
    assert any("control-cadence" in line for line in rows)
    assert any("Managed capacity calls" in line for line in rows)
    assert not any("Dynamic Worker creation" in line for line in rows)


@pytest.mark.asyncio
async def test_supervisor_waits_for_its_cadence_after_a_timeout(monkeypatch) -> None:
    stop = asyncio.Event()

    async def wait(*_args, **_kwargs):
        stop.set()

    cadence = AsyncMock(side_effect=wait)
    monkeypatch.setattr(cloudflare_supervisor, "listen_for_pool_wake", AsyncMock())
    monkeypatch.setattr(cloudflare_supervisor, "_installations", AsyncMock(side_effect=TimeoutError))
    monkeypatch.setattr(cloudflare_supervisor, "wait_for_next_pass", cadence)
    await cloudflare_supervisor.cloudflare_supervisor_loop(
        stop, SimpleNamespace(redis_url="redis://fixture"), lease=SimpleNamespace(
            holds_lease=lambda: True, seconds_remaining=lambda: 1,
        ),
    )
    cadence.assert_awaited_once()
    assert cadence.await_args.kwargs["spacing"] == 2.0


@pytest.mark.asyncio
async def test_tenant_residency_reserves_route_and_warm_tail_in_the_spend_query(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    worker_id = uuid4()
    incarnation = CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, started_at=now - timedelta(minutes=1),
        slots=1, meter_release_digest="f" * 64, meter_rate_version=WORKERS_INVENTORY_RATE_TABLE_VERSION,
        meter_bounds={"caps": TEST_LAUNCHER_CAPS, "second_usd": "0.00068126550"},
    )
    db_session.add(incarnation)
    await db_session.flush()
    price = select(cloudflare_estimated_usage()).where(CloudflareInstallation.id == installation.id)
    assert not await reserve_tenant_host_usage(db_session, uuid4(), now)
    assert not await reserve_tenant_host_usage(db_session, worker_id, None)
    assert await reserve_tenant_host_usage(db_session, worker_id, now + timedelta(seconds=120))
    row = await db_session.get(CloudflareUsageActivity, (installation.id, usage_period(now)[0]))
    assert row is not None
    assert 119 <= row.inventory_tenant_seconds - 1282 <= 120
    assert row.inventory_dispatches == 1 and row.inventory_warm_pings == 32
    incarnation.deleted_at = now
    await db_session.flush()
    with_host = await db_session.scalar(price)
    expected = (Decimal("0.040541628") + row.inventory_tenant_seconds * Decimal("0.0000016")
                + 32 * Decimal("0.002627408"))
    row.inventory_tenant_seconds = row.inventory_dispatches = row.inventory_warm_pings = 0
    await db_session.flush()
    without_host = await db_session.scalar(price)
    assert with_host is not None and without_host is not None
    assert abs(with_host - without_host - expected) < Decimal("0.000000000001")
    # A route booked across rollover leaves a residency reservation in
    # the next UTC month even though its request starts in this month.
    next_period = usage_period(now)[1]
    assert not await reserve_tenant_host_usage(db_session, worker_id, next_period)
    incarnation.deleted_at = None
    await db_session.flush()
    assert await reserve_tenant_host_usage(db_session, worker_id, next_period + timedelta(seconds=10))
    rollover = await db_session.get(CloudflareUsageActivity, (installation.id, next_period))
    assert rollover is not None
    assert (rollover.inventory_tenant_seconds, rollover.inventory_dispatches, rollover.inventory_warm_pings) == (1292, 0, 29)
    installation.policy = {**installation.policy, "meter_launcher": {}}
    await db_session.flush()
    assert await db_session.scalar(price) is None


@pytest.mark.asyncio
async def test_qualified_dispatch_prices_pinned_cpu_and_keeps_sdk_events_attributed(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    worker_id, execution_id = uuid4(), uuid4()
    db_session.add(ExecutionWorker(
        id=worker_id, form="external", state="ready", capacity_scope="workers",
        runtime_profile="cloudflare-python-v1", memory_limit_bytes=128 * 2**20, version="fixture-release",
    ))
    await db_session.flush()
    db_session.add(CloudflareWorkerReservation(
        worker_id=worker_id, installation_id=installation.id, scope="workers", deployment_revision=1,
        control_root_version=1,
    ))
    await db_session.flush()
    db_session.add(ExecutionWorkerClaim(worker_id=worker_id, family=WAKE_WORKFLOW, subject_id=execution_id))
    await db_session.flush()
    await cloudflare_spend.record_sdk_gateway_request(
        db_session, origin=installation.worker_ingress_origin, execution_id=execution_id,
    )

    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    assert not await reserve_tenant_host_usage(db_session, worker_id, now)
    assert await reserve_tenant_host_usage(
        db_session, worker_id, now + timedelta(seconds=120), workflow_cpu_ms=123, subrequests=12,
        tenant_id="f" * 32, route_id="fixture-1",
    )
    identity = f"tenant-eng-tenant-py-{'f' * 32}-fixture"
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, f"{identity}-g0")
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, f"{identity}-g0")
    row = await db_session.get(CloudflareUsageActivity, (installation.id, usage_period(now)[0]))
    assert row is not None
    assert (row.dispatched_runs, row.sdk_requests, row.qualified_legacy_events) == (1, 1, 2)
    assert (row.qualified_dispatches, row.qualified_warm_pings, row.qualified_cpu_ms) == (1, 32, 1_367_523)
    assert row.qualified_creations == 1
    assert row.generation_control_requests == 4
    incarnation = await db_session.get(CloudflareUsageIncarnation, worker_id)
    assert incarnation is not None
    incarnation.deleted_at = now
    await db_session.flush()
    price = select(cloudflare_estimated_usage()).where(CloudflareInstallation.id == installation.id)
    qualified_price = await db_session.scalar(price)
    row.qualified_legacy_events = 0
    await db_session.flush()
    duplicate_proxy = await db_session.scalar(price)
    assert duplicate_proxy is not None and qualified_price is not None
    assert duplicate_proxy - qualified_price == Decimal("0.0012")

    row.qualified_cpu_ms = 0
    await db_session.flush()
    without_reserved_cpu = await db_session.scalar(price)
    assert without_reserved_cpu is not None
    assert duplicate_proxy - without_reserved_cpu == Decimal("0.02735046")

    incarnation.deleted_at = None
    await db_session.flush()
    assert await reserve_tenant_host_usage(
        db_session, worker_id, now + timedelta(seconds=120), workflow_cpu_ms=123, subrequests=12,
        tenant_id="f" * 32, route_id="fixture-2",
    )
    await db_session.refresh(row)
    assert row.qualified_creations == 1
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, f"{identity}-g1")
    await db_session.refresh(row)
    assert row.qualified_creations == 2
    # A different launcher package can carry the same loader code and identity.
    incarnation.meter_release_digest = "a" * 64
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, f"{identity}-g1")
    await db_session.refresh(row)
    assert row.qualified_creations == 2

    tomorrow = now + timedelta(days=1)
    await cloudflare_spend._book_daily_engine(db_session, incarnation, f"{identity}-g1", tomorrow)
    await cloudflare_spend._book_daily_engine(db_session, incarnation, f"{identity}-g1", tomorrow)
    next_day = await db_session.get(CloudflareUsageActivity, (installation.id, usage_period(tomorrow)[0]))
    assert next_day is not None
    await db_session.refresh(next_day)
    assert next_day.qualified_creations == (3 if usage_period(tomorrow)[0] == usage_period(now)[0] else 1)

    next_period = usage_period(now)[1]
    previous_rollover = await db_session.get(CloudflareUsageActivity, (installation.id, next_period))
    previous_pings = previous_rollover.qualified_warm_pings if previous_rollover is not None else 0
    assert await reserve_tenant_host_usage(
        db_session, worker_id, next_period + timedelta(seconds=10), workflow_cpu_ms=123, subrequests=12,
        tenant_id="f" * 32, route_id="fixture-rollover",
    )
    rollover = await db_session.get(CloudflareUsageActivity, (installation.id, next_period))
    assert rollover is not None and rollover.qualified_dispatches == 0
    await db_session.refresh(rollover)
    assert rollover.qualified_warm_pings == previous_pings + 29 and rollover.qualified_creations == 1
    assert rollover.generation_control_requests == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence", ["complete", "reloaded", "missing", "uncertain", "incomplete", "regressing", "drift"])
async def test_foreground_pricing_keeps_reserves_until_a_confirmed_seal(
    db_session: AsyncSession, installation: CloudflareInstallation, evidence: str,
) -> None:
    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    period, _ = usage_period(now)
    worker_id = uuid4()
    tenant_id = "e" * 32
    identity = f"tenant-eng-tenant-py-{tenant_id}-fixture"
    row = CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, started_at=now - timedelta(seconds=75), slots=1,
        meter_rate_version=WORKERS_RATE_TABLE_VERSION, meter_release_digest="f" * 64,
        meter_bounds={**installation.policy["meter_launcher"], "second_usd": "0.0013", "root_second_usd": "0.0012"},
    )
    db_session.add(row)
    await db_session.flush()
    assert await reserve_tenant_host_usage(
        db_session, worker_id, now + timedelta(seconds=10), workflow_cpu_ms=500, subrequests=10,
        tenant_id=tenant_id, route_id="fixture",
    )
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, identity)
    event = {"sqlite_rows_written": 12, "do_requests": 3, "log_events": 1}
    await cloudflare_spend.record_launcher_terminal_meter(
        db_session, worker_id, {
            **event, "sqlite_rows_written": 13 if evidence == "regressing" else 12,
            "do_requests": 0 if evidence == "drift" else event["do_requests"],
        },
        uncertain=evidence == "uncertain",
    )
    price = select(cloudflare_estimated_usage()).where(CloudflareInstallation.id == installation.id)
    payload = CloudflareLauncherUsage.model_validate({
        **event, "do_requests": 0 if evidence == "drift" else event["do_requests"],
        "routes": {} if evidence == "incomplete" else {"fixture": [
            {"identity": identity, "kind": "boot", "cold": True, "started_at": now},
            {"identity": identity, "kind": "workflow", "cold": evidence == "reloaded", "started_at": now},
        ]},
    })
    await cloudflare_spend.seal_launcher_meter(db_session, worker_id, payload)
    assert row.meter_final is None
    row.deleted_at = now
    await db_session.flush()
    reserved = await db_session.scalar(price)
    assert reserved is not None
    await cloudflare_spend.seal_launcher_meter(db_session, worker_id, None if evidence == "missing" else payload)
    await cloudflare_spend.seal_launcher_meter(db_session, worker_id, payload if evidence != "missing" else None)
    await db_session.flush()
    activity = await db_session.get(CloudflareUsageActivity, (installation.id, period))
    assert activity is not None
    settled = await db_session.scalar(price)
    assert settled is not None
    lifetime = Decimal(str((now - max(row.started_at, period)).total_seconds()))
    counter_and_root_release = Decimal("0.00009295") + lifetime * Decimal("0.0012")
    if evidence in ("complete", "reloaded"):
        extra_load_us = 500_000 if evidence == "reloaded" else 0
        assert activity.released_cpu_us == 7_500_000 - extra_load_us
        assert activity.released_engine_requests == 2
        assert activity.qualified_creations == 1
        assert reserved - settled == counter_and_root_release + Decimal("0.00015060") - extra_load_us * Decimal("2e-11")
    elif evidence == "incomplete":
        assert activity.released_cpu_us == activity.released_engine_requests == 0
        assert reserved - settled == counter_and_root_release
    else:
        assert activity.released_cpu_us == activity.released_engine_requests == 0
        assert reserved == settled
    if evidence == "drift":
        coverage = await cloudflare_spend.spend_coverage(db_session, installation.account_id, period, WORKERS_RATE_TABLE_VERSION)
        assert any("counts diverged" in line for line in coverage)
    if evidence != "missing":
        assert await db_session.get(cloudflare_spend.CloudflareDispatchMeter, (worker_id, "fixture")) is None


@pytest.mark.asyncio
async def test_launcher_sqlite_high_water_accrues_until_namespace_deletion(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    start, end = usage_period(now)
    worker_id = uuid4()
    row = CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, started_at=start, slots=1,
        meter_rate_version=WORKERS_RATE_TABLE_VERSION, meter_release_digest="f" * 64,
    )
    namespace = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="fixture-launcher", desired_revision=1,
        artifact_digest=row.meter_release_digest, effect_id=uuid4(), state="present",
        provider_id="fixture", provider_version="1", observed_at=start,
    )
    db_session.add_all([row, namespace])
    await db_session.flush()
    estimate = select(cloudflare_spend._retained_storage_cost(CloudflareInstallation)).where(
        CloudflareInstallation.id == installation.id,
    )
    event = {"sqlite_rows_written": 1, "do_requests": 1, "log_events": 1, "exhausted": False}
    await cloudflare_spend.record_launcher_terminal_meter(
        db_session, worker_id, {**event, "sqlite_database_bytes": 12_288}, uncertain=False,
    )
    await db_session.flush()
    assert (await db_session.scalar(estimate)) > 0

    await cloudflare_spend.record_launcher_terminal_meter(
        db_session, worker_id, {**event, "sqlite_database_bytes": 9_000_000_000}, uncertain=False,
    )
    await cloudflare_spend.record_launcher_terminal_meter(db_session, worker_id, event, uncertain=False)
    assert row.meter_terminal is not None and row.meter_terminal["sqlite_database_bytes"] == 9_000_000_000
    just_after_start = min(start + timedelta(seconds=1), now)
    row.deleted_at = just_after_start
    await db_session.flush()
    still_accruing = await db_session.scalar(estimate)
    namespace.state = "absent"
    namespace.observed_at = min(start + timedelta(seconds=2), now)
    await db_session.flush()
    month_fraction = Decimal(str((namespace.observed_at - start).total_seconds())) / Decimal(str((end - start).total_seconds()))
    accrued = Decimal("90.01") * month_fraction * Decimal("0.20")
    price = await db_session.scalar(estimate)
    assert price is not None and abs(price - accrued) < Decimal("0.00000000001")
    assert still_accruing is not None and still_accruing >= price


@pytest.mark.asyncio
@pytest.mark.parametrize("delete_uid,stored,expected", [("wrong", False, False), ("owned", False, False), ("owned", True, True)])
async def test_canary_cannot_seal_without_a_matching_delete_receipt_and_row(
    monkeypatch, delete_uid: str, stored: bool, expected: bool,
) -> None:
    worker_id = uuid4()
    db = SimpleNamespace(scalar=AsyncMock(side_effect=[worker_id, None, worker_id if stored else None]))

    @asynccontextmanager
    async def context():
        yield db

    seal = AsyncMock()
    monkeypatch.setattr(cloudflare_canary, "get_db_context", context)
    monkeypatch.setattr(cloudflare_canary, "seal_launcher_meter", seal)
    client = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(resource_uid="owned", state="running")),
        remove=AsyncMock(return_value=SimpleNamespace(resource_uid=delete_uid, state="deleted", usage={})),
    )
    assert await cloudflare_canary._retire_canary(client, worker_id, "owned") is expected
    seal.assert_not_awaited()


@pytest.mark.asyncio
async def test_claim_gate_tracks_scale_in_and_deployment_changes_without_restart(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    worker_id = uuid4()
    db_session.add(ExecutionWorker(
        id=worker_id, form="external", state="ready", capacity_scope="workers",
        runtime_profile="cloudflare-python-v1", memory_limit_bytes=256 * 2**20,
    ))
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr("synthetic-backend-test-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
    ))
    await db_session.flush()
    db_session.add(CloudflareWorkerReservation(
        worker_id=worker_id, installation_id=installation.id, scope="workers", deployment_revision=1,
        control_root_version=1,
    ))
    scope = CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    )
    db_session.add(scope)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)

    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    await db_session.flush()
    assert await cloudflare_claim_allowed(db_session, worker_id)
    db_session.add(ExecutionWorkerClaim(worker_id=worker_id, family=WAKE_WORKFLOW, subject_id=uuid4()))
    await db_session.flush()
    start, _ = usage_period(datetime.now(UTC))
    activity = await db_session.get(CloudflareUsageActivity, (installation.id, start))
    assert activity is not None and activity.dispatched_runs == 1
    activity.sdk_requests = 100_000
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None and status.installation.spend is not None
    assert status.installation.spend.state == "budget_reached"
    activity.sdk_requests = 0
    installation.usage_incomplete_until = datetime.now(UTC) + timedelta(days=1)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    installation.usage_incomplete_until = None
    await db_session.flush()
    assert await cloudflare_claim_allowed(db_session, worker_id)
    ledger = await read_workers_usage(db_session, installation.id, datetime.now(UTC))
    assert ledger is not None and ledger.launcher_seconds > 0
    assert ledger.slot_seconds == ledger.launcher_seconds * 2

    policy = installation.policy
    limits = policy["limits"]
    installation.policy = {
        **policy,
        "limits": {**limits, "workers": {**limits["workers"], "work_kinds": []}},
    }
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    installation.policy = policy

    installation.admitted_revision = None
    installation.desired_state = "scaled_in"
    installation.desired_revision = 2
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)

    installation.admitted_revision = 2
    installation.desired_state = "enabled"
    replacement_scope = CloudflareCapacityScope(
        installation_id=scope.installation_id, scope=scope.scope, endpoint="https://new-capacity.example.test",
        deployment_revision=2, bearer_version=scope.bearer_version,
    )
    db_session.add(replacement_scope)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    await db_session.delete(scope)
    await db_session.delete(replacement_scope)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    assert await cloudflare_claim_allowed(db_session, uuid4())
    orphan = ExecutionWorker(
        id=uuid4(), form="external", state="retired", capacity_scope="old-workers",
        runtime_profile="cloudflare-python-v1",
    )
    db_session.add(orphan)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, orphan.id)

    installation.admitted_revision = None
    installation.desired_state = "disconnected"
    installation.desired_revision = 3
    await db_session.flush()
    with pytest.raises(CloudflarePolicyConflict, match="cleanup is pending"):
        await retire_cloudflare_installation(db_session, installation_id=installation.id, expected_revision=3)
    worker = await db_session.get(ExecutionWorker, worker_id)
    assert worker is not None
    worker.state = "retired"
    worker.capacity_resource_deleted_at = datetime.now(UTC)
    await db_session.flush()
    with pytest.raises(CloudflarePolicyConflict, match="cleanup is pending"):
        await retire_cloudflare_installation(db_session, installation_id=installation.id, expected_revision=3)
    orphan.capacity_resource_deleted_at = datetime.now(UTC)
    await db_session.flush()
    await retire_cloudflare_installation(db_session, installation_id=installation.id, expected_revision=3)
    assert installation.retired_at is not None


@pytest.mark.asyncio
async def test_resource_journal_cannot_bind_another_account(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            db_session.add(CloudflareOwnedResource(
                id=uuid4(), installation_id=installation.id, account_id="b" * 32,
                scope="workers", kind="worker_script", target_name="bifrost-resource",
                desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="creating",
            ))
            await db_session.flush()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("collision", "constraint"),
    [
        ("target_name", "uq_cf_resource_target"),
        ("provider_id", "uq_cf_resource_provider"),
        ("effect_id", "uq_cf_resource_effect"),
    ],
)
async def test_resource_journal_rejects_duplicate_identity(
    db_session: AsyncSession, installation: CloudflareInstallation, collision: str, constraint: str,
) -> None:
    identity = {"target_name": "worker-a", "provider_id": "opaque-id-a", "effect_id": uuid4()}
    common = {
        "installation_id": installation.id, "account_id": installation.account_id,
        "scope": "workers", "kind": "worker_script", "desired_revision": 1,
        "artifact_digest": "c" * 64, "state": "present", "provider_version": "etag-a",
        "observed_at": datetime.now(UTC),
    }
    db_session.add(CloudflareOwnedResource(id=uuid4(), **common, **identity))
    await db_session.flush()
    next_identity = {"target_name": "worker-b", "provider_id": "opaque-id-b", "effect_id": uuid4()}
    next_identity[collision] = identity[collision]
    with pytest.raises(IntegrityError, match=constraint):
        async with db_session.begin_nested():
            db_session.add(CloudflareOwnedResource(id=uuid4(), **common, **next_identity))
            await db_session.flush()


@pytest.mark.asyncio
async def test_scaled_in_installation_cannot_admit_its_revision(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            installation.admitted_revision = installation.desired_revision
            await db_session.flush()


@pytest.mark.asyncio
async def test_policy_save_fences_old_admission_and_rejects_an_unsafe_budget(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    await db_session.flush()
    policy = CloudflareCapacityPolicy(
        workers=CloudflareWorkersPolicy(
            launcher_cap=1, slots_per_launcher=2, max_lifetime_seconds=3600, work_kinds=frozenset({"workflow"}),
        ),
        monthly_limit_usd=Decimal("4.00"),
    )
    zero = dict.fromkeys(WORKERS_RATES, Decimal(0))
    fixed = {**zero, "sqlite_rows_written": Decimal(5_000_000)}
    bounds = WorkersReleaseMeterBounds(60, zero, fixed, "fixture-release", 2, TEST_LAUNCHER_CAPS, TEST_CPU_LIMITS, 500_000)
    with pytest.raises(CloudflareBudgetTooLow):
        await save_cloudflare_policy(
            db_session, installation_id=installation.id, expected_revision=1, policy=policy, bounds=bounds,
        )
    assert installation.admitted_revision == 1

    policy.monthly_limit_usd = Decimal("11.00")
    assert await save_cloudflare_policy(
        db_session, installation_id=installation.id, expected_revision=1, policy=policy, bounds=bounds,
    ) == 2
    assert installation.admitted_revision is None
    assert installation.policy["rate_table_version"] == WORKERS_RATE_TABLE_VERSION
    assert installation.policy["fleet_envelope_usd"] == "5.00"
    assert installation.policy["bounds_release"] == "fixture-release"
    assert len(installation.policy["bounds_digest"]) == 64
    assert await save_cloudflare_policy(
        db_session, installation_id=installation.id, expected_revision=2, policy=policy, bounds=bounds,
    ) == 2
    with pytest.raises(CloudflarePolicyConflict):
        await save_cloudflare_policy(
            db_session, installation_id=installation.id, expected_revision=1, policy=policy, bounds=bounds,
        )
