"""Retain workflow CPU bookings and roll up expired launcher storage detail."""

import sqlalchemy as sa
from alembic import op

revision = "20261120_cf_spend_observations"
down_revision = "20261120_cf_settle_receipts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_dispatch_meters", sa.Column("workflow_cpu_ms", sa.Integer()))
    op.create_index("ix_cf_usage_incarnation_deleted", "cloudflare_usage_incarnations", ["deleted_at"])
    op.create_table(
        "cloudflare_storage_residue",
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey("cloudflare_installations.id"), primary_key=True),
        sa.Column("release_digest", sa.String(100), primary_key=True),
        sa.Column("launcher_bytes", sa.BigInteger(), nullable=False),
        sa.Column("ledger_bytes", sa.BigInteger(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("cloudflare_storage_residue")
    op.drop_index("ix_cf_usage_incarnation_deleted", table_name="cloudflare_usage_incarnations")
    op.drop_column("cloudflare_dispatch_meters", "workflow_cpu_ms")
