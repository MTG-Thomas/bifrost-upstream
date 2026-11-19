"""Add the claim-bound Cloudflare autonomous agent journal."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20261119_cf_agent_attempt"
down_revision = "20261118_cf_solution_source"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloudflare_agent_attempts",
        sa.Column("claim_id", sa.Uuid(), primary_key=True),
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey("agent_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("data", postgresql.JSONB(), nullable=False),
    )
    op.create_index("ix_cloudflare_agent_attempts_run_id", "cloudflare_agent_attempts", ["run_id"])


def downgrade() -> None:
    op.drop_table("cloudflare_agent_attempts")
