"""Keep measured CPU reservations separate from already priced monthly usage."""

import sqlalchemy as sa
from alembic import op

revision = "20261120_cf_qualified_cpu"
down_revision = "20261120_cf_meter_inventory"
branch_labels = None
depends_on = None

_COLUMNS = (
    "qualified_cpu_ms", "qualified_dispatches", "qualified_warm_pings", "qualified_creations", "qualified_legacy_events",
)


def _dispatch_trigger(*, qualified: bool) -> None:
    column = ", qualified_legacy_events" if qualified else ""
    value = """, CASE WHEN EXISTS (
        SELECT 1 FROM cloudflare_usage_incarnations
        WHERE worker_id = NEW.worker_id AND meter_rate_version = 'workers-paid-2026-11-20-qualified-cpu'
    ) THEN 1 ELSE 0 END""" if qualified else ""
    update = """, qualified_legacy_events = cloudflare_usage_activity.qualified_legacy_events
        + excluded.qualified_legacy_events""" if qualified else ""
    op.execute(f"""
        CREATE OR REPLACE FUNCTION record_cf_usage_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO cloudflare_usage_activity (installation_id, period_start, dispatched_runs{column})
          SELECT installation_id, date_trunc('month', clock_timestamp(), 'UTC'), 1{value}
          FROM cloudflare_worker_reservations WHERE worker_id = NEW.worker_id
          ON CONFLICT (installation_id, period_start) DO UPDATE
            SET dispatched_runs = cloudflare_usage_activity.dispatched_runs + 1{update};
          RETURN NEW;
        END $$;
    """)


def upgrade() -> None:
    for name in _COLUMNS:
        op.add_column("cloudflare_usage_activity", sa.Column(name, sa.BigInteger(), nullable=False, server_default="0"))
    _dispatch_trigger(qualified=True)


def downgrade() -> None:
    _dispatch_trigger(qualified=False)
    for name in reversed(_COLUMNS):
        op.drop_column("cloudflare_usage_activity", name)
