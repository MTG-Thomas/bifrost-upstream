"""Retain the last fenced remote compute assignment for a platform job."""

import sqlalchemy as sa
from alembic import op

revision = "20261119_pjob_remote_stage"
down_revision = "20261119_cf_agent_attempt"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("platform_jobs", sa.Column("remote_stage", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("platform_jobs", "remote_stage")
