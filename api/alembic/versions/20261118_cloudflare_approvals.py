"""Store reviewed Cloudflare workflow declarations.

Revision ID: 20261118_cloudflare_approvals
Revises: 20261118_cloudflare_profile
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261118_cloudflare_approvals"
down_revision = "20261118_cloudflare_profile"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloudflare_workflow_approvals",
        sa.Column("revision", sa.String(64), primary_key=True),
        sa.Column("workflow_id", sa.Uuid(), sa.ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False),
        sa.Column("organization_id", sa.Uuid()),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("declaration", JSONB(), nullable=False),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("accepted_by", sa.String(255), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_cloudflare_workflow_approvals_workflow_id", "cloudflare_workflow_approvals", ["workflow_id"])
    op.create_index(
        "uq_cloudflare_approval_current", "cloudflare_workflow_approvals", ["workflow_id"],
        unique=True, postgresql_where=sa.text("is_current"),
    )


def downgrade() -> None:
    op.drop_table("cloudflare_workflow_approvals")
