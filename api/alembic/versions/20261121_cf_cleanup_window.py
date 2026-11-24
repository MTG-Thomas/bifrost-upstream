"""Track the managed cleanup retry window."""

import sqlalchemy as sa
from alembic import op

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261121_cf_cleanup_window"
down_revision = "20261121_cf_cleanup_backoff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_worker_reservations", sa.Column("cleanup_started_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_worker_reservations")
    op.drop_column("cloudflare_worker_reservations", "cleanup_started_at")
