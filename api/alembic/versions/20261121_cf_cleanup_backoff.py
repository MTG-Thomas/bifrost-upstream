"""Persist Cloudflare provider cleanup backoff and DO usage."""

import sqlalchemy as sa
from alembic import op

from shared.migration_policy import refuse_populated_downgrade

revision = "20261121_cf_cleanup_backoff"
down_revision = "20261120_cf_workflow_slots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_worker_reservations", sa.Column(
        "cleanup_attempts", sa.SmallInteger(), server_default="0", nullable=False,
    ))
    op.add_column("cloudflare_worker_reservations", sa.Column("cleanup_retry_at", sa.DateTime(timezone=True)))
    op.add_column("cloudflare_usage_activity", sa.Column(
        "capacity_cleanup_do_requests", sa.BigInteger(), server_default="0", nullable=False,
    ))


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_usage_activity", "cloudflare_worker_reservations")
    op.drop_column("cloudflare_usage_activity", "capacity_cleanup_do_requests")
    op.drop_column("cloudflare_worker_reservations", "cleanup_retry_at")
    op.drop_column("cloudflare_worker_reservations", "cleanup_attempts")
