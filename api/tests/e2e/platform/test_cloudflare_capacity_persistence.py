"""Exercise the Cloudflare journal against the migrated platform database."""

import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from shared.models import CloudflareCapacityPolicy, CloudflareWorkersPolicy
from sqlalchemy import delete, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from src.config import Settings
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.work_dispatches import WorkDispatch
from src.services.kubernetes_execution import KubernetesExecutionService
from src.services.worker_pool import (
    cloudflare_supervisor,
    controller,
    external_controller,
)
from src.services.worker_pool.cloudflare_admission import cloudflare_claim_allowed
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    open_credential,
    seal_credential,
)
from src.services.worker_pool.cloudflare_policy import (
    CONSENT_TEXT_VERSION,
    CloudflareBudgetTooLow,
    CloudflareInstallationExists,
    CloudflarePolicyConflict,
    WorkersReleaseMeterBounds,
    create_cloudflare_installation,
    retire_cloudflare_installation,
    save_cloudflare_policy,
)
from src.services.worker_pool.cloudflare_spend import (
    WORKERS_RATE_TABLE_VERSION,
    WORKERS_RATES,
)

OPERATOR_ID = UUID("20cb7fe5-78e2-48d7-8f1e-8edcac69ffab")

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

    for module in (controller, external_controller, cloudflare_supervisor):
        monkeypatch.setattr(module, "get_db_context", context)
    monkeypatch.setattr(controller, "_publish_live", AsyncMock(return_value=controller.LiveControllers(
        ids=frozenset(), versions=frozenset({"test"}),
    )))
    await KubernetesExecutionService(db_session).update_policy({"max_workers": 1, "warm_workers": 0}, updated_by="test")
    await db_session.execute(update(ExecutionWorker).where(
        ExecutionWorker.form == primary,
    ).values(state="retired"))
    bearer = "test-scope-bearer-" + "a" * 30
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr(bearer), actor=str(OPERATOR_ID), now=datetime.now(UTC),
    ))
    db_session.add(ExecutionWorker(
        id=uuid4(), form=primary, state="ready",
        capacity_scope="vm" if primary == "external" else None,
        heartbeat_at=datetime.now(UTC), supported_handlers=["workflow.execute"],
        memory_budget_bytes=128 * 2**20, memory_reserved_bytes=128 * 2**20,
    ))
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
        _env_file=None, secret_key="synthetic-supervisor-test-key",
        worker_capacity_url="https://vm.example.test" if primary == "external" else None,
        worker_capacity_token="p" * 40 if primary == "external" else None,
    )
    controllers = {}
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        async def sweep():
            await cloudflare_supervisor._reconcile_scope(
                installation.id, "workers", settings, http, controllers, kubernetes=primary == "job",
            )

        await sweep()
        assert not writes
        installation.desired_state = "enabled"
        installation.admitted_revision = 1
        await db_session.flush()
        await sweep()
        assert writes == ["PUT", "POST"]
        owned = tuple(controllers.values())
        installation.desired_state = "scaled_in"
        installation.admitted_revision = None
        await db_session.flush()
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
        bounds=WorkersReleaseMeterBounds(60, zero, zero, "fixture-release", rated_slots),
    )


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
    ).values(deployment_revision=2)
    try:
        async with async_session_factory() as reader, async_session_factory() as writer:
            await reader.scalar(select(CloudflareInstallation.id).where(
                CloudflareInstallation.id == installation_id,
            ).with_for_update(read=True))
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
    await db_session.flush()
    await retire_cloudflare_installation(db_session, installation_id=installation_id, expected_revision=2)
    assert installation.retired_at is not None and installation.api_token_version is None
    credential = await db_session.get(CloudflareCredential, (installation_id, "account_token", "", 1))
    assert credential is not None and credential.ciphertext is None
    new_id = await create_installation(db_session, installation.account_id)
    assert new_id != installation_id
    assert await db_session.get(CloudflareOwnedResource, resource.id) is not None
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

    scope = await db_session.get(CloudflareCapacityScope, (ref.installation_id, ref.scope))
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
async def test_claim_gate_tracks_scale_in_and_deployment_changes_without_restart(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    worker_id = uuid4()
    db_session.add(ExecutionWorker(
        id=worker_id, form="external", state="ready", capacity_scope="workers",
        runtime_profile="cloudflare-python-v1",
    ))
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr("synthetic-backend-test-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
    ))
    await db_session.flush()
    db_session.add(CloudflareWorkerReservation(
        worker_id=worker_id, installation_id=installation.id, scope="workers", deployment_revision=1,
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
    scope.deployment_revision = 2
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    await db_session.delete(scope)
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
    # The minimum plan fee alone exceeds the proposed reduced budget.
    bounds = WorkersReleaseMeterBounds(60, zero, zero, "fixture-release", 2)
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
