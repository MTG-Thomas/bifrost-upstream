"""Exercise a guarded Cloudflare rollback across the release migration chain."""

import asyncio
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
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
TARGET = "20261120_cf_control_root"


def test_cloudflare_meter_downgrade_preserves_receipts_and_reverses_empty_schema() -> None:
    database_name = f"bifrost_r2b_rehearsal_{uuid4().hex[:12]}"
    url = _direct_database_url(database_name)
    installation = uuid4()
    account = "a" * 32

    async def execute(sql: str):
        async def run(connection: AsyncConnection):
            result = await connection.execute(sa.text(sql), {"id": installation, "account": account})
            return result.scalar() if result.returns_rows else None

        return await _run_in_database(url, run)

    def downgrade() -> None:
        with _temporary_migration_database_url(url):
            command.downgrade(_alembic_config(), TARGET)

    try:
        asyncio.run(_create_database(database_name))
        _upgrade(url, "head")
        asyncio.run(execute("""
            INSERT INTO cloudflare_installations (
                id, account_id, worker_ingress_origin, consent_version, consent_actor, consent_at,
                policy, desired_revision, desired_state, retired_at
            ) VALUES (:id, :account, 'https://worker.example.test', 'test', 'test', now(),
                '{}', 1, 'disconnected', now())
        """))
        for table, seed, message in (
            ("cloudflare_usage_activity", """
                INSERT INTO cloudflare_usage_activity (installation_id, period_start, dispatched_runs)
                VALUES (:id, date_trunc('month', now()), 1)
            """, "Self-metering receipts must survive rollback"),
            ("cloudflare_usage_incarnations", """
                INSERT INTO cloudflare_usage_incarnations (worker_id, installation_id, started_at, slots)
                VALUES (gen_random_uuid(), :id, now(), 1)
            """, "Self-metering receipts must survive rollback"),
            ("cloudflare_capacity_usage", """
                INSERT INTO cloudflare_capacity_usage (
                    installation_id, account_id, deployment_revision, rate_table_version, bounds_digest,
                    period_start, period_end, measured_at, attributed_usd
                ) VALUES (:id, :account, 1, 'test', 'test', now(), now() + interval '1 day', now(), 1)
            """, "Legacy capacity receipts must survive rollback"),
        ):
            asyncio.run(execute(seed))
            with pytest.raises(DBAPIError, match=message):
                downgrade()
            assert asyncio.run(execute(f"SELECT count(*) FROM {table}")) == 1
            asyncio.run(execute(f"DELETE FROM {table}"))

        downgrade()
        for table in ("cloudflare_usage_activity", "cloudflare_usage_incarnations", "cloudflare_capacity_usage"):
            assert asyncio.run(execute(f"SELECT to_regclass('{table}')")) is None
        assert asyncio.run(execute("SELECT to_regprocedure('cloudflare_usage_totals(uuid,timestamptz)')")) is None
        _upgrade(url, "head")
    finally:
        asyncio.run(_drop_database(database_name))
