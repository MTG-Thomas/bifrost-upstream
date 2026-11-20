"""Exercise the Cloudflare journal against the migrated platform database."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr
from shared.models import CloudflareCapacityPolicy, CloudflareWorkersPolicy
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    open_credential,
    seal_credential,
)
from src.services.worker_pool.cloudflare_policy import (
    CloudflareBudgetTooLow,
    CloudflarePolicyConflict,
    WorkersReleaseMeterBounds,
    create_cloudflare_installation,
    save_cloudflare_policy,
)
from src.services.worker_pool.cloudflare_spend import (
    WORKERS_RATE_TABLE_VERSION,
    WORKERS_RATES,
)


@pytest.fixture
async def installation(db_session: AsyncSession) -> CloudflareInstallation:
    installation_id = await create_installation(db_session, "a" * 32)
    row = await db_session.get(CloudflareInstallation, installation_id)
    assert row is not None
    return row


async def create_installation(db_session: AsyncSession, account_id: str) -> UUID:
    zero = dict.fromkeys(WORKERS_RATES, Decimal(0))
    return await create_cloudflare_installation(
        db_session,
        account_id=account_id,
        worker_ingress_origin="https://workers.example.test",
        consent_version="existing-https-v1",
        actor="operator",
        verified_api_token=SecretStr("synthetic-cloudflare-management-token"),
        policy=CloudflareCapacityPolicy(
            workers=CloudflareWorkersPolicy(
                launcher_cap=1, slots_per_launcher=2, max_lifetime_seconds=3600, work_kinds=frozenset({"workflow"}),
            ),
            monthly_limit_usd=Decimal("10.00"),
        ),
        bounds=WorkersReleaseMeterBounds(60, zero, zero),
    )


@pytest.mark.asyncio
async def test_connection_persists_consent_and_account_token_without_admitting_work(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    assert (installation.consent_actor, installation.consent_version) == ("operator", "existing-https-v1")
    assert installation.desired_state == "scaled_in" and installation.admitted_revision is None
    credential = await db_session.get(CloudflareCredential, (installation.id, "account_token", "", 1))
    assert credential is not None
    ref = CredentialRef(installation.id, installation.account_id, "account_token", "", 1)
    assert open_credential(ref, credential) == SecretStr("synthetic-cloudflare-management-token")

    with pytest.raises(CloudflarePolicyConflict, match="already exists"):
        await create_installation(db_session, "b" * 32)


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
    # The minimum plan fee alone exceeds the initial budget.
    bounds = WorkersReleaseMeterBounds(60, zero, zero)
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
    with pytest.raises(CloudflarePolicyConflict):
        await save_cloudflare_policy(
            db_session, installation_id=installation.id, expected_revision=1, policy=policy, bounds=bounds,
        )
