"""Keep foreground reservations until a verified launcher seal supplies usage."""

import sqlalchemy as sa
from alembic import op

from shared.migration_policy import refuse_populated_downgrade

revision = "20261120_cf_settle_receipts"
down_revision = "20261120_cf_gateway_isolation"
branch_labels = None
depends_on = None


def _dispatch_trigger(*, settle: bool) -> None:
    rates = "'workers-paid-2026-11-20-qualified-cpu', 'workers-paid-2026-11-20-meter-lifecycle'"
    if settle:
        rates += ", 'workers-paid-2026-11-20-settled-usage'"
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
    op.create_table(
        "cloudflare_dispatch_meters",
        sa.Column("worker_id", sa.Uuid(), sa.ForeignKey("cloudflare_usage_incarnations.worker_id"), primary_key=True),
        sa.Column("route_id", sa.String(128), primary_key=True),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("tenant_id", sa.String(32), nullable=False),
    )
    op.add_column("cloudflare_engine_usage_keys", sa.Column("booked_by", sa.Uuid()))
    op.add_column("cloudflare_engine_usage_keys", sa.Column("booked_at", sa.DateTime(timezone=True)))
    for name in ("released_cpu_us", "released_engine_requests"):
        op.add_column("cloudflare_usage_activity", sa.Column(name, sa.BigInteger(), nullable=False, server_default="0"))
    _dispatch_trigger(settle=True)


def downgrade() -> None:
    refuse_populated_downgrade(
        "cloudflare_dispatch_meters", "cloudflare_engine_usage_keys", "cloudflare_usage_activity",
    )
    _dispatch_trigger(settle=False)
    for name in ("released_engine_requests", "released_cpu_us"):
        op.drop_column("cloudflare_usage_activity", name)
    op.drop_column("cloudflare_engine_usage_keys", "booked_at")
    op.drop_column("cloudflare_engine_usage_keys", "booked_by")
    op.drop_table("cloudflare_dispatch_meters")
