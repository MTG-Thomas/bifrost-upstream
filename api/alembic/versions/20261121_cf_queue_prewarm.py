"""Fence speculative Cloudflare preparations independently of workflow claims."""

import sqlalchemy as sa
from alembic import op

revision = "20261121_cf_queue_prewarm"
down_revision = "20261121_cf_managed_tunnel"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_workflow_slots", sa.Column("prewarm_fence", sa.Uuid()))
    op.create_unique_constraint("uq_cf_workflow_slot_prewarm", "cloudflare_workflow_slots", ["prewarm_fence"])


def downgrade() -> None:
    op.drop_constraint("uq_cf_workflow_slot_prewarm", "cloudflare_workflow_slots", type_="unique")
    op.drop_column("cloudflare_workflow_slots", "prewarm_fence")
