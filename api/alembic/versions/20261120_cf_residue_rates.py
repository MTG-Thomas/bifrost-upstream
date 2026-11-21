"""Retain rate coverage when old incarnation storage is rolled up."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261120_cf_residue_rates"
down_revision = "20261120_cf_spend_observations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_storage_residue", sa.Column(
        "rate_versions", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb"),
    ))


def downgrade() -> None:
    op.drop_column("cloudflare_storage_residue", "rate_versions")
