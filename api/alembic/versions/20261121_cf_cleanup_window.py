"""Track the cleanup retry window and observed launcher quiescence."""

import sqlalchemy as sa
from alembic import op

revision = "20261121_cf_cleanup_window"
down_revision = "20261121_cf_cleanup_backoff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_worker_reservations", sa.Column("cleanup_started_at", sa.DateTime(timezone=True)))
    op.add_column("cloudflare_usage_incarnations", sa.Column("launcher_stopped_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    op.drop_column("cloudflare_usage_incarnations", "launcher_stopped_at")
    op.drop_column("cloudflare_worker_reservations", "cleanup_started_at")
