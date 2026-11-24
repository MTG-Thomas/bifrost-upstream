"""Pin the lifetime of each managed Workers incarnation."""

import sqlalchemy as sa
from alembic import op

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261122_cf_workers_deadline"
down_revision = "20261122_cf_overflow_timing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = "cloudflare_worker_reservations"
    op.add_column(table, sa.Column("workers_lifetime_seconds", sa.Integer()))
    op.add_column(table, sa.Column("workers_deadline_at", sa.DateTime(timezone=True)))
    op.add_column(table, sa.Column("workers_deadline_wire", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.execute("""
        WITH pinned AS (
            SELECT p.worker_id, w.created_at, CASE WHEN p.canary_job_id IS NOT NULL
                THEN least(120, (i.policy #>> '{limits,workers,max_lifetime_seconds}')::integer)
                ELSE (i.policy #>> '{limits,workers,max_lifetime_seconds}')::integer END AS lifetime
            FROM cloudflare_worker_reservations p
            JOIN execution_workers w ON w.id = p.worker_id
            JOIN cloudflare_installations i ON i.id = p.installation_id
            WHERE p.container_reserved_at IS NULL AND w.capacity_resource_deleted_at IS NULL
        )
        UPDATE cloudflare_worker_reservations p
        SET workers_lifetime_seconds = pinned.lifetime,
            workers_deadline_at = pinned.created_at + make_interval(secs => pinned.lifetime)
        FROM pinned
        WHERE p.worker_id = pinned.worker_id AND pinned.lifetime IS NOT NULL
    """)


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_worker_reservations")
    op.drop_column("cloudflare_worker_reservations", "workers_deadline_wire")
    op.drop_column("cloudflare_worker_reservations", "workers_deadline_at")
    op.drop_column("cloudflare_worker_reservations", "workers_lifetime_seconds")
