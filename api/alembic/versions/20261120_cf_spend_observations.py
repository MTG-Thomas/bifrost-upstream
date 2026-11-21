"""Retain workflow CPU bookings and roll up expired launcher storage detail."""

import sqlalchemy as sa
from alembic import op

revision = "20261120_cf_spend_observations"
down_revision = "20261120_cf_settle_receipts"
branch_labels = None
depends_on = None


def _incarnation_trigger(*, pin_deployment: bool) -> None:
    column = ", deployment_revision" if pin_deployment else ""
    value = ", NEW.deployment_revision" if pin_deployment else ""
    op.execute(f"""
        CREATE OR REPLACE FUNCTION record_cf_usage_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO cloudflare_usage_incarnations
            (worker_id, installation_id, started_at, deleted_at, slots,
             meter_release_digest, meter_rate_version, meter_bounds{column})
          SELECT w.id, NEW.installation_id, w.created_at, w.capacity_resource_deleted_at,
            CASE WHEN w.memory_limit_bytes IN (134217728, 268435456, 402653184, 536870912)
              THEN w.memory_limit_bytes / 134217728 END, w.version,
            CASE WHEN w.version = i.policy->>'bounds_release' THEN i.policy->>'rate_table_version' END,
            CASE WHEN w.version = i.policy->>'bounds_release' THEN i.policy->'meter_launcher' END{value}
          FROM execution_workers w JOIN cloudflare_installations i ON i.id = NEW.installation_id
          WHERE w.id = NEW.worker_id;
          RETURN NEW;
        END $$;
    """)


def upgrade() -> None:
    op.add_column("cloudflare_dispatch_meters", sa.Column("workflow_cpu_ms", sa.Integer()))
    op.add_column("cloudflare_usage_incarnations", sa.Column("deployment_revision", sa.Integer()))
    op.execute("""
        UPDATE cloudflare_usage_incarnations m SET deployment_revision = r.deployment_revision
        FROM cloudflare_worker_reservations r WHERE r.worker_id = m.worker_id
    """)
    _incarnation_trigger(pin_deployment=True)
    op.create_index("ix_cf_usage_incarnation_deleted", "cloudflare_usage_incarnations", ["deleted_at"])
    op.create_table(
        "cloudflare_storage_residue",
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey("cloudflare_installations.id"), primary_key=True),
        sa.Column("release_digest", sa.String(100), primary_key=True),
        sa.Column("deployment_revision", sa.Integer(), primary_key=True),
        sa.Column("launcher_bytes", sa.BigInteger(), nullable=False),
        sa.Column("ledger_bytes", sa.BigInteger(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("cloudflare_storage_residue")
    op.drop_index("ix_cf_usage_incarnation_deleted", table_name="cloudflare_usage_incarnations")
    _incarnation_trigger(pin_deployment=False)
    op.drop_column("cloudflare_usage_incarnations", "deployment_revision")
    op.drop_column("cloudflare_dispatch_meters", "workflow_cpu_ms")
