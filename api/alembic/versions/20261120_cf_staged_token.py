"""Store short-lived, operator-bound Cloudflare credential references."""

import sqlalchemy as sa
from alembic import op

revision = "20261120_cf_staged_token"
down_revision = "20261120_cf_self_meter"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloudflare_staged_tokens",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("actor_id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.String(32), nullable=False),
        sa.Column("purpose", sa.String(16), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ciphertext", sa.Text(), nullable=False),
        sa.CheckConstraint("purpose IN ('connect', 'rotate')", name="ck_cf_staged_token_purpose"),
    )
    op.create_index("ix_cf_staged_token_expiry", "cloudflare_staged_tokens", ["expires_at"])


def downgrade() -> None:
    op.drop_table("cloudflare_staged_tokens")
