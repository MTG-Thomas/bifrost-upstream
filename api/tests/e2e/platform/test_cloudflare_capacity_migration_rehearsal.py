"""Exercise data-preserving Cloudflare downgrades at their exact revision."""

import asyncio
from uuid import uuid4

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
_TABLES = (
    _INSTALLATIONS, _ACTIVITY, _INCARNATIONS, _RESIDUE, _KEYS, _PINS, _LEGACY, _DISPATCH,
    _STAGED, _SLOTS, _RESOURCES, "cloudflare_capacity_credentials", "cloudflare_capacity_scopes",
)

_SEEDS = {
    _INSTALLATIONS: (),
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
    _PINS: ("""
        INSERT INTO cloudflare_worker_reservations (worker_id, installation_id, scope, deployment_revision)
        VALUES (:worker, :id, 'test', 1)
    """,),
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
    params = {"id": uuid4(), "account": "a" * 32, "worker": uuid4()}
    statements = []
    if table != _STAGED:
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
    statements.extend(_SEEDS[table])
    if table == _ACTIVITY:
        statements.append(f"""
            INSERT INTO cloudflare_usage_activity (installation_id, period_start, {column})
            VALUES (:id, date_trunc('month', now()), 1)
        """)
    if column == "usage_advisory":
        statements.append("UPDATE cloudflare_installations SET usage_advisory = '{}' WHERE id = :id")
    elif column == "meter_terminal":
        statements.append("UPDATE cloudflare_usage_incarnations SET meter_terminal = '{}' WHERE worker_id = :worker")
    for statement in statements:
        await connection.execute(sa.text(statement), params)


@pytest.mark.parametrize(("revision", "table", "column"), [
    ("20261120_cf_capacity", _INSTALLATIONS, None),
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
        for table in _TABLES:
            assert await connection.scalar(sa.text("SELECT to_regclass(:name)"), {"name": table}) is None
        assert await connection.scalar(sa.text("SELECT to_regprocedure('cloudflare_usage_totals(uuid,timestamptz)')")) is None

    asyncio.run(_run_in_database(migration_database, readback))
    _upgrade(migration_database, "head")

    async def restored(connection: AsyncConnection) -> None:
        assert await connection.scalar(sa.text("SELECT version_num FROM alembic_version")) == (
            ScriptDirectory.from_config(config).get_current_head()
        )
        for table in _TABLES:
            assert await connection.scalar(sa.text("SELECT to_regclass(:name)"), {"name": table}) == table

    asyncio.run(_run_in_database(migration_database, restored))
