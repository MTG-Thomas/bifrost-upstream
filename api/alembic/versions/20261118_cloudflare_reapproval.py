"""Record carry-forward provenance for Cloudflare workflow approvals.

Revision ID: 20261118_cloudflare_reapproval
Revises: 20261118_cloudflare_approvals
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261118_cloudflare_reapproval"
down_revision = "20261118_cloudflare_approvals"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_workflow_approvals", sa.Column("derived_from", sa.String(64)))
    op.add_column("cloudflare_workflow_approvals", sa.Column("pin_diff", JSONB()))
    op.create_foreign_key(
        "fk_cloudflare_approval_parent", "cloudflare_workflow_approvals",
        "cloudflare_workflow_approvals", ["derived_from"], ["revision"],
    )


def downgrade() -> None:
    op.drop_constraint("fk_cloudflare_approval_parent", "cloudflare_workflow_approvals", type_="foreignkey")
    op.drop_column("cloudflare_workflow_approvals", "pin_diff")
    op.drop_column("cloudflare_workflow_approvals", "derived_from")
