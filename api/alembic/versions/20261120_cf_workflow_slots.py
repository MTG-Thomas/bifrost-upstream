"""Persist installation-wide Cloudflare workflow slot occupancy."""

import sqlalchemy as sa
from alembic import op

revision = "20261120_cf_workflow_slots"
down_revision = "20261120_cf_residue_rates"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloudflare_workflow_slots",
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey("cloudflare_installations.id", ondelete="RESTRICT"), primary_key=True),
        sa.Column("tenant", sa.String(36), primary_key=True),
        sa.Column("ordinal", sa.SmallInteger(), primary_key=True),
        sa.Column("worker_id", sa.Uuid(), sa.ForeignKey("execution_workers.id", ondelete="CASCADE"), nullable=False),
        sa.Column("execution_id", sa.Uuid(), nullable=False),
        sa.Column("dispatch_id", sa.Uuid(), nullable=False),
        sa.Column("fence", sa.Uuid(), nullable=False),
        sa.Column("sent", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("released_at", sa.DateTime(timezone=True)),
        sa.Column("warm_until", sa.DateTime(timezone=True)),
        sa.CheckConstraint("ordinal >= 0 AND ordinal < 32", name="ck_cf_workflow_slot_ordinal"),
        sa.UniqueConstraint("fence", name="uq_cf_workflow_slot_fence"),
    )
    op.create_index("ix_cf_workflow_slot_worker", "cloudflare_workflow_slots", ["worker_id"])


def downgrade() -> None:
    op.drop_table("cloudflare_workflow_slots")
