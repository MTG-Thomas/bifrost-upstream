"""Store sanitized stage timings for managed Workers overflow."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261122_cf_overflow_timing"
down_revision = "20261121_cf_review_source_scrub"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("executions", sa.Column("cloudflare_timing", JSONB))
    op.add_column("cloudflare_worker_reservations", sa.Column("launch_timing", JSONB))


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_worker_reservations", non_null=("executions", "cloudflare_timing"))
    op.drop_column("cloudflare_worker_reservations", "launch_timing")
    op.drop_column("executions", "cloudflare_timing")
