"""Persist the billing evidence used by Cloudflare admission."""

import sqlalchemy as sa
from alembic import op

revision = "20261120_cf_usage_receipts"
down_revision = "20261120_cf_control_root"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloudflare_capacity_usage",
        sa.Column("installation_id", sa.Uuid(), primary_key=True),
        sa.Column("account_id", sa.String(32), nullable=False),
        sa.Column("deployment_revision", sa.Integer(), nullable=False),
        sa.Column("rate_table_version", sa.String(64), nullable=False),
        sa.Column("bounds_digest", sa.String(64), nullable=False),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("measured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attributed_usd", sa.Numeric(20, 6), nullable=False),
        sa.CheckConstraint("deployment_revision > 0", name="ck_cf_usage_revision"),
        sa.CheckConstraint("period_start <= measured_at AND measured_at < period_end", name="ck_cf_usage_period"),
        sa.CheckConstraint("attributed_usd >= 0 AND attributed_usd < 'Infinity'::numeric", name="ck_cf_usage_amount"),
        sa.ForeignKeyConstraint(
            ["installation_id", "account_id"],
            ["cloudflare_installations.id", "cloudflare_installations.account_id"],
            ondelete="RESTRICT",
        ),
    )


def downgrade() -> None:
    op.execute("LOCK TABLE cloudflare_capacity_usage IN ACCESS EXCLUSIVE MODE")
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM cloudflare_capacity_usage) THEN
                RAISE EXCEPTION 'Legacy capacity receipts must survive rollback'
                    USING ERRCODE = 'object_not_in_prerequisite_state';
            END IF;
        END $$;
    """)
    op.drop_table("cloudflare_capacity_usage")
