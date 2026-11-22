"""Store sanitized stage timings for managed Workers overflow."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261122_cf_overflow_timing"
down_revision = "20261121_cf_containers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("executions", sa.Column("cloudflare_timing", JSONB))
    op.add_column("cloudflare_worker_reservations", sa.Column("launch_timing", JSONB))


def downgrade() -> None:
    op.drop_column("cloudflare_worker_reservations", "launch_timing")
    op.drop_column("executions", "cloudflare_timing")
