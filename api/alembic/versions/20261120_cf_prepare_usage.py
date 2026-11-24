"""Count speculative Cloudflare engine entries separately from workflows."""

import sqlalchemy as sa
from alembic import op

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261120_cf_prepare_usage"
down_revision = "20261120_cf_settle_receipts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "cloudflare_usage_activity",
        sa.Column("qualified_preparations", sa.BigInteger(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    refuse_populated_downgrade("cloudflare_usage_activity")
    op.drop_column("cloudflare_usage_activity", "qualified_preparations")
