"""Exercise data-preserving Cloudflare downgrades at their exact revision."""

import asyncio
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.e2e.platform.test_r2b_roles_migration_rehearsal import (
    _alembic_config,
    _create_database,
    _direct_database_url,
    _drop_database,
    _run_in_database,
    _temporary_migration_database_url,
    _upgrade,
)

pytestmark = pytest.mark.e2e

_ACTIVITY = "cloudflare_usage_activity"
_INCARNATIONS = "cloudflare_usage_incarnations"
_INSTALLATIONS = "cloudflare_installations"
_RESIDUE = "cloudflare_storage_residue"
_KEYS = "cloudflare_engine_usage_keys"
_PINS = "cloudflare_worker_reservations"
_LEGACY = "cloudflare_capacity_usage"
_DISPATCH = "cloudflare_dispatch_meters"
_STAGED = "cloudflare_staged_tokens"
_SLOTS = "cloudflare_workflow_slots"
_RESOURCES = "cloudflare_owned_resources"
_CREDENTIALS = "cloudflare_capacity_credentials"
_SCOPES = "cloudflare_capacity_scopes"
_APPROVALS = "cloudflare_workflow_approvals"
_ATTEMPTS = "cloudflare_agent_attempts"
_OUTBOX = "cloudflare_chat_outbox"
_CAPACITY_TABLES = {
    _INSTALLATIONS, _ACTIVITY, _INCARNATIONS, _RESIDUE, _KEYS, _PINS, _LEGACY, _DISPATCH,
    _STAGED, _SLOTS, _RESOURCES, _CREDENTIALS, _SCOPES,
}
_CREDENTIAL_SEED = """
    INSERT INTO cloudflare_capacity_credentials (
        installation_id, purpose, scope, version, account_id, recorded_by, recorded_at
    ) VALUES (:id, :purpose, :scope, 1, :account, 'test', now())
"""
_USER_SEED = """
    INSERT INTO users (
        id, email, is_active, is_superuser, is_verified, is_registered, is_system, mfa_enabled, base_role_id
    ) VALUES (:user, 'rehearsal@example.test', true, true, true, true, false, false,
              '00000000-0000-0000-0000-000000000006')
"""
_ATTEMPT_SEED = ("""
    INSERT INTO agent_runs (id, trigger_type, status, caller_user_id)
    VALUES (:run, 'api', 'completed', :user)
""", """
    INSERT INTO cloudflare_agent_attempts (claim_id, run_id, data)
    VALUES (:claim, :run, '{}')
""")
_SEEDS = {
    _INSTALLATIONS: (),
    _CREDENTIALS: (_CREDENTIAL_SEED,),
    _SCOPES: (_CREDENTIAL_SEED, """
        INSERT INTO cloudflare_capacity_scopes (installation_id, scope, endpoint, deployment_revision, bearer_version)
        VALUES (:id, :scope, 'https://worker.example.test', 1, 1)
    """),
    _APPROVALS: ("""
        INSERT INTO workflows (id, name, function_name, path, endpoint_enabled)
        VALUES (:workflow, 'test', 'test', 'test.py', true)
    """, """
        INSERT INTO cloudflare_workflow_approvals (
            revision, workflow_id, policy_version, declaration, is_current, accepted_by
        ) VALUES ('test', :workflow, 'test', '{}', false, 'reviewer')
    """),
    _ATTEMPTS: (_USER_SEED, *_ATTEMPT_SEED),
    _OUTBOX: (_USER_SEED, *_ATTEMPT_SEED, """
        INSERT INTO conversations (id, user_id, channel, extra_data, is_active)
        VALUES (:conversation, :user, 'chat', '{}', true)
    """, """
        INSERT INTO cloudflare_chat_outbox (claim_id, conversation_id, chunk_sequence, event_id, envelope)
        VALUES (:claim, :conversation, 1, gen_random_uuid(), '{}')
    """),
    "executions": (_USER_SEED, """
        INSERT INTO executions (
            id, workflow_name, status, executed_by, executed_by_name, cloudflare_timing
        ) VALUES (:run, 'test', 'Success', :user, 'test', '{}')
    """),
    _ACTIVITY: (),
    _INCARNATIONS: ("""
        INSERT INTO cloudflare_usage_incarnations (worker_id, installation_id, started_at, slots)
        VALUES (:worker, :id, now(), 1)
    """,),
    _RESIDUE: ("""
        INSERT INTO cloudflare_storage_residue (
            installation_id, release_digest, deployment_revision, launcher_bytes, ledger_bytes
        ) VALUES (:id, 'test', 1, 10, 20)
    """,),
    _KEYS: ("""
        INSERT INTO cloudflare_engine_usage_keys (installation_id, usage_key)
        VALUES (:id, 'test')
    """,),
    _LEGACY: ("""
        INSERT INTO cloudflare_capacity_usage (
            installation_id, account_id, deployment_revision, rate_table_version, bounds_digest,
            period_start, period_end, measured_at, attributed_usd
        ) VALUES (:id, :account, 1, 'test', 'test', now(), now() + interval '1 day', now(), 1)
    """,),
    _DISPATCH: ("""
        INSERT INTO cloudflare_usage_incarnations (worker_id, installation_id, started_at, slots)
        VALUES (:worker, :id, now(), 1)
    """, """
        INSERT INTO cloudflare_dispatch_meters (worker_id, route_id, period_start, tenant_id)
        VALUES (:worker, 'test', date_trunc('month', now()), 'test')
    """),
    _STAGED: ("""
        INSERT INTO cloudflare_staged_tokens (id, actor_id, account_id, purpose, expires_at, ciphertext)
        VALUES (gen_random_uuid(), gen_random_uuid(), :account, 'connect', now() + interval '1 day', 'test')
    """,),
    _PINS: (),
    _SLOTS: ("""
        INSERT INTO cloudflare_workflow_slots (
            installation_id, tenant, ordinal, worker_id, execution_id, dispatch_id, fence
        ) VALUES (:id, 'test', 0, :worker, gen_random_uuid(), gen_random_uuid(), gen_random_uuid())
    """,),
    _RESOURCES: ("""
        INSERT INTO cloudflare_owned_resources (
            id, installation_id, account_id, scope, kind, target_name, desired_revision,
            artifact_digest, effect_id, state, provider_id, provider_version
        ) VALUES (gen_random_uuid(), :id, :account, 'test', 'script', 'test', 1,
            'test', gen_random_uuid(), 'present', 'test', 'test')
    """,),
}


@pytest.fixture
def migration_database():
    name = f"bifrost_r2b_rehearsal_{uuid4().hex[:12]}"
    asyncio.run(_create_database(name))
    try:
        yield _direct_database_url(name)
    finally:
        asyncio.run(_drop_database(name))


async def _seed(
    connection: AsyncConnection, table: str, column: str | None,
) -> None:
    params: dict[str, str | UUID] = {key: uuid4() for key in ("id", "worker", "workflow", "user", "run", "claim", "conversation")}
    params.update(account="a" * 32, purpose="scope_bearer", scope="test")
    statements = []
    if table in _CAPACITY_TABLES - {_STAGED}:
        has_retirement = await connection.scalar(sa.text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'cloudflare_installations' AND column_name = 'retired_at'
            )
        """))
        column_suffix = ", retired_at" if has_retirement else ""
        value_suffix = ", now()" if has_retirement else ""
        statements.append(f"""
            INSERT INTO cloudflare_installations (
                id, account_id, worker_ingress_origin, consent_version, consent_actor, consent_at,
                policy, desired_revision, desired_state{column_suffix}
            ) VALUES (:id, :account, 'https://worker.example.test', 'test', 'test', now(),
                '{{}}', 1, 'disconnected'{value_suffix})
        """)
    if table in (_PINS, _SLOTS):
        statements.append("""
            INSERT INTO execution_workers (id, form, state, resource_class, slots)
            VALUES (:worker, 'external', 'ready', 'test', 1)
        """)
    if table == _PINS:
        has_root = await connection.scalar(sa.text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'cloudflare_worker_reservations' AND column_name = 'control_root_version'
            )
        """))
        if has_root:
            params.update(purpose="control_root", scope="")
            statements.append(_CREDENTIAL_SEED)
        root_column = ", control_root_version" if has_root else ""
        root_version = ", 1" if has_root else ""
        statements.append(f"""
            INSERT INTO cloudflare_worker_reservations (worker_id, installation_id, scope, deployment_revision{root_column})
            VALUES (:worker, :id, 'test', 1{root_version})
        """)
    statements.extend(_SEEDS[table])
    if table == _ACTIVITY:
        statements.append(f"""
            INSERT INTO cloudflare_usage_activity (installation_id, period_start, {column})
            VALUES (:id, date_trunc('month', now()), 1)
        """)
    if column == "usage_advisory":
        statements.append("UPDATE cloudflare_installations SET usage_advisory = '{}' WHERE id = :id")
    elif column == "usage_incomplete_until":
        statements.append("UPDATE cloudflare_installations SET usage_incomplete_until = now() WHERE id = :id")
    elif column == "is_review":
        statements.append("UPDATE cloudflare_workflow_approvals SET is_review = true, accepted_by = NULL")
    elif column == "meter_terminal":
        statements.append("UPDATE cloudflare_usage_incarnations SET meter_terminal = '{}' WHERE worker_id = :worker")
    for statement in statements:
        await connection.execute(sa.text(statement), params)


@pytest.mark.parametrize(("revision", "table", "column"), [
    ("20261120_cf_capacity", _INSTALLATIONS, None),
    ("20261118_cloudflare_approvals", _APPROVALS, None),
    ("20261118_cloudflare_reapproval", _APPROVALS, None),
    ("20261119_cf_agent_attempt", _ATTEMPTS, None),
    ("20261119_cf_chat_outbox", _OUTBOX, None),
    ("20261120_cf_capacity", _CREDENTIALS, None),
    ("20261120_cf_capacity", _SCOPES, None),
    ("20261120_cf_installation_retire", _INSTALLATIONS, "retired_at"),
    ("20261120_cf_control_root", _PINS, "control_root_version"),
    ("20261120_cf_control_root", _INSTALLATIONS, None),
    ("20261120_cf_runtime_canary", _PINS, None),
    ("20261120_cf_runtime_canary", _RESOURCES, "namespace_ids"),
    ("20261120_cf_self_meter", _INSTALLATIONS, "usage_incomplete_until"),
    ("20261120_cf_canary_gen", _PINS, None),
    ("20261120_cf_meter_inventory", _SCOPES, "capacity_cpu_ms"),
    ("20261120_cf_gateway_isolation", _ACTIVITY, "generation_control_requests"),
    ("20261120_cf_gateway_isolation", _SCOPES, "gateway_required"),
    ("20261120_cf_settle_receipts", _KEYS, None),
    ("20261120_cf_settle_receipts", _ACTIVITY, "dispatched_runs"),
    ("20261120_cf_spend_observations", _DISPATCH, None),
    ("20261120_cf_spend_observations", _INCARNATIONS, None),
    ("20261121_cf_cleanup_backoff", _PINS, None),
    ("20261121_cf_cleanup_window", _PINS, None),
    ("20261121_cf_managed_tunnel", _RESOURCES, "provider_details"),
    ("20261121_cf_managed_tunnel", _INSTALLATIONS, None),
    ("20261121_cf_workers_upgrade", _INSTALLATIONS, None),
    ("20261121_cf_containers", _PINS, None),
    ("20261121_cf_containers", _INSTALLATIONS, None),
    ("20261121_cf_pending_reviews", _APPROVALS, "is_review"),
    ("20261121_cf_pending_reviews", _APPROVALS, None),
    ("20261121_cf_queue_prewarm", _SLOTS, None),
    ("20261122_cf_status_evidence", _INSTALLATIONS, None),
    ("20261122_cf_workers_deadline", _PINS, None),
    ("20261122_cf_overflow_timing", _PINS, None),
    ("20261122_cf_overflow_timing", "executions", "cloudflare_timing"),
    ("20261122_cf_route_uncertainty", _DISPATCH, "attempt_uncertain"),
    ("20261120_cf_capacity", _RESOURCES, None),
    ("20261120_cf_usage_receipts", _LEGACY, None),
    ("20261120_cf_reservation_pin", _PINS, None),
    ("20261120_cf_self_meter", _ACTIVITY, "dispatched_runs"),
    ("20261120_cf_self_meter", _INCARNATIONS, None),
    ("20261120_cf_self_meter", _INSTALLATIONS, "usage_advisory"),
    ("20261120_cf_staged_token", _STAGED, None),
    ("20261120_cf_launcher_meter", _INCARNATIONS, "meter_terminal"),
    ("20261120_cf_meter_control", _ACTIVITY, "tenant_dispatches"),
    ("20261120_cf_meter_inventory", _ACTIVITY, "capacity_requests"),
    ("20261120_cf_qualified_cpu", _ACTIVITY, "qualified_cpu_ms"),
    ("20261120_cf_gateway_isolation", _KEYS, None),
    ("20261120_cf_settle_receipts", _DISPATCH, None),
    ("20261120_cf_prepare_usage", _ACTIVITY, "qualified_preparations"),
    ("20261120_cf_spend_observations", _RESIDUE, None),
    ("20261120_cf_residue_rates", _RESIDUE, None),
    ("20261120_cf_workflow_slots", _SLOTS, None),
    ("20261121_cf_cleanup_backoff", _ACTIVITY, "capacity_cleanup_do_requests"),
])
def test_cloudflare_downgrade_refuses_to_discard_rows(
    migration_database: str, revision: str, table: str, column: str | None,
) -> None:
    # Start exactly here so a guard on a newer migration cannot hide a hole.
    _upgrade(migration_database, revision)

    async def seed(connection: AsyncConnection) -> None:
        await _seed(connection, table, column)

    asyncio.run(_run_in_database(migration_database, seed))
    config = _alembic_config()
    script = ScriptDirectory.from_config(config).get_revision(revision)
    assert script is not None and isinstance(script.down_revision, str)
    with _temporary_migration_database_url(migration_database):
        with pytest.raises(DBAPIError, match=f"Cloudflare downgrade would discard data from {table}") as failure:
            command.downgrade(config, script.down_revision)
    assert getattr(failure.value.orig, "sqlstate", None) == "55000"

    async def readback(connection: AsyncConnection) -> None:
        predicate = f" WHERE {column} IS NOT NULL" if column else ""
        assert await connection.scalar(sa.text(f"SELECT count(*) FROM {table}{predicate}")) == 1
        assert await connection.scalar(sa.text("SELECT version_num FROM alembic_version")) == revision

    asyncio.run(_run_in_database(migration_database, readback))


def test_empty_cloudflare_schema_can_reverse_and_reupgrade_across_merge(migration_database: str) -> None:
    _upgrade(migration_database, "head")
    config = _alembic_config()
    with _temporary_migration_database_url(migration_database):
        command.downgrade(config, "20261119_cf_chat_outbox")

    async def readback(connection: AsyncConnection) -> None:
        heads = (await connection.scalars(sa.text("SELECT version_num FROM alembic_version"))).all()
        assert len(heads) == 2 and "20261119_cf_chat_outbox" in heads
        for table in sorted(_CAPACITY_TABLES):
            assert await connection.scalar(sa.text("SELECT to_regclass(:name)"), {"name": table}) is None
        assert await connection.scalar(sa.text("SELECT to_regprocedure('cloudflare_usage_totals(uuid,timestamptz)')")) is None

    asyncio.run(_run_in_database(migration_database, readback))
    _upgrade(migration_database, "head")

    async def restored(connection: AsyncConnection) -> None:
        assert await connection.scalar(sa.text("SELECT version_num FROM alembic_version")) == (
            ScriptDirectory.from_config(config).get_current_head()
        )
        for table in sorted(_CAPACITY_TABLES):
            assert await connection.scalar(sa.text("SELECT to_regclass(:name)"), {"name": table}) == table

    asyncio.run(_run_in_database(migration_database, restored))


def test_populated_cloudflare_downgrade_from_head_rolls_back_every_revision(migration_database: str) -> None:
    _upgrade(migration_database, "head")
    config = _alembic_config()

    async def seed(connection: AsyncConnection) -> None:
        # A standalone staged token allows earlier downgrades to execute
        # before the old token-table revision refuses to discard the row.
        await _seed(connection, _STAGED, None)

    async def snapshot(connection: AsyncConnection) -> tuple:
        revisions = (await connection.scalars(sa.text("SELECT version_num FROM alembic_version ORDER BY version_num"))).all()
        schema = (await connection.execute(sa.text("""
            SELECT table_name, column_name, column_default, is_nullable
            FROM information_schema.columns WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """))).all()
        token_rows = await connection.scalar(sa.text("SELECT count(*) FROM cloudflare_staged_tokens"))
        return revisions, schema, token_rows

    asyncio.run(_run_in_database(migration_database, seed))
    before = asyncio.run(_run_in_database(migration_database, snapshot))
    assert before[2] == 1
    with _temporary_migration_database_url(migration_database), pytest.raises(
        DBAPIError, match=f"Cloudflare downgrade would discard data from {_STAGED}",
    ) as failure:
        command.downgrade(config, "20261119_cf_chat_outbox")
    assert getattr(failure.value.orig, "sqlstate", None) == "55000"
    assert asyncio.run(_run_in_database(migration_database, snapshot)) == before
