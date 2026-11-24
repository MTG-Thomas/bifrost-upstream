"""Pin whether the installed revision includes the private gateway script."""

import sqlalchemy as sa
from alembic import op

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261120_cf_gateway_isolation"
down_revision = "20261120_cf_qualified_cpu"
branch_labels = None
depends_on = None


def _dispatch_trigger(*, lifecycle: bool) -> None:
    rates = "'workers-paid-2026-11-20-qualified-cpu'"
    if lifecycle:
        rates += ", 'workers-paid-2026-11-20-meter-lifecycle'"
    op.execute(f"""
        CREATE OR REPLACE FUNCTION record_cf_usage_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO cloudflare_usage_activity (installation_id, period_start, dispatched_runs, qualified_legacy_events)
          SELECT installation_id, date_trunc('month', clock_timestamp(), 'UTC'), 1, CASE WHEN EXISTS (
            SELECT 1 FROM cloudflare_usage_incarnations
            WHERE worker_id = NEW.worker_id AND meter_rate_version IN ({rates})
          ) THEN 1 ELSE 0 END FROM cloudflare_worker_reservations WHERE worker_id = NEW.worker_id
          ON CONFLICT (installation_id, period_start) DO UPDATE
            SET dispatched_runs = cloudflare_usage_activity.dispatched_runs + 1,
                qualified_legacy_events = cloudflare_usage_activity.qualified_legacy_events + excluded.qualified_legacy_events;
          RETURN NEW;
        END $$;
    """)


def upgrade() -> None:
    op.add_column(
        "cloudflare_capacity_scopes",
        sa.Column("gateway_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.create_table(
        "cloudflare_engine_usage_keys",
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey("cloudflare_installations.id"), primary_key=True),
        sa.Column("usage_key", sa.String(256), primary_key=True),
    )
    op.add_column("cloudflare_usage_activity", sa.Column(
        "generation_control_requests", sa.BigInteger(), nullable=False, server_default="0",
    ))
    _dispatch_trigger(lifecycle=True)


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_engine_usage_keys", "cloudflare_usage_activity", "cloudflare_capacity_scopes")
    _dispatch_trigger(lifecycle=False)
    op.drop_column("cloudflare_usage_activity", "generation_control_requests")
    op.drop_table("cloudflare_engine_usage_keys")
    op.drop_column("cloudflare_capacity_scopes", "gateway_required")
