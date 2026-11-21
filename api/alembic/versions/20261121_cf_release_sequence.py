"""Persist the Workers release ordering fence."""

import sqlalchemy as sa
from alembic import op

revision = "20261121_cf_release_sequence"
down_revision = "20261121_cf_upgrade_checkpoint"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "cloudflare_installations",
        sa.Column("workers_release_sequence", sa.BigInteger(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("cloudflare_installations", "workers_release_sequence")
