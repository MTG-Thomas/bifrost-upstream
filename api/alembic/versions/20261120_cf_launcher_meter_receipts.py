"""Retain launcher usage across worker and claim retention.

workers-paid-2026-11-20-launcher-receipts prices launcher lifetime at 128 MiB
and caps receipt meters from the incarnation's deployed release. Older
incarnations have no final receipt and retain conservative pricing.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

from shared.migration_policy import refuse_populated_downgrade

revision = "20261120_cf_launcher_meter"
down_revision = "20261120_cf_canary_gen"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = "cloudflare_usage_incarnations"
    op.add_column(table, sa.Column("meter_terminal", JSONB()))
    op.add_column(table, sa.Column("meter_final", JSONB()))
    op.add_column(table, sa.Column("meter_uncertain", sa.Boolean(), server_default=sa.false(), nullable=False))
    op.add_column(table, sa.Column("meter_release_digest", sa.String(100)))
    op.add_column(table, sa.Column("meter_rate_version", sa.String(64)))
    op.add_column(table, sa.Column("meter_bounds", JSONB()))
    op.execute("""
        UPDATE cloudflare_usage_incarnations m
        SET meter_release_digest = w.version,
            meter_rate_version = CASE WHEN w.version = i.policy->>'bounds_release'
              THEN i.policy->>'rate_table_version' END
        FROM execution_workers w, cloudflare_installations i
        WHERE w.id = m.worker_id AND i.id = m.installation_id
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION record_cf_usage_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO cloudflare_usage_incarnations
            (worker_id, installation_id, started_at, deleted_at, slots, meter_release_digest, meter_rate_version, meter_bounds)
          SELECT w.id, NEW.installation_id, w.created_at, w.capacity_resource_deleted_at,
            CASE WHEN w.memory_limit_bytes IN (134217728, 268435456, 402653184, 536870912)
              THEN w.memory_limit_bytes / 134217728 END, w.version,
            CASE WHEN w.version = i.policy->>'bounds_release' THEN i.policy->>'rate_table_version' END,
            CASE WHEN w.version = i.policy->>'bounds_release' THEN i.policy->'meter_launcher' END
          FROM execution_workers w JOIN cloudflare_installations i ON i.id = NEW.installation_id
          WHERE w.id = NEW.worker_id;
          RETURN NEW;
        END $$;
    """)


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_usage_incarnations")
    op.execute("""
        CREATE OR REPLACE FUNCTION record_cf_usage_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO cloudflare_usage_incarnations (worker_id, installation_id, started_at, deleted_at, slots)
          SELECT w.id, NEW.installation_id, w.created_at, w.capacity_resource_deleted_at,
            CASE WHEN w.memory_limit_bytes IN (134217728, 268435456, 402653184, 536870912)
              THEN w.memory_limit_bytes / 134217728 END
          FROM execution_workers w WHERE w.id = NEW.worker_id;
          RETURN NEW;
        END $$;
    """)
    for name in ("meter_bounds", "meter_rate_version", "meter_release_digest", "meter_uncertain", "meter_final", "meter_terminal"):
        op.drop_column("cloudflare_usage_incarnations", name)
