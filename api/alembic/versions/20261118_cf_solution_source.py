"""Witness the published runtime source of a Solution install."""

import sqlalchemy as sa
from alembic import op

revision = "20261118_cf_solution_source"
down_revision = "20261118_cloudflare_reapproval"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("solutions", sa.Column("runtime_source_generation", sa.Uuid(), nullable=True))


def downgrade() -> None:
    op.drop_column("solutions", "runtime_source_generation")
