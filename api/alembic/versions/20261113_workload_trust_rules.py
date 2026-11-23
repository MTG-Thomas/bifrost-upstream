"""Trust rules for inbound OAuth 2.0 token exchange."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "20261113_workload_trust_rules"
down_revision = "20261110_form_layout"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workload_trust_rules",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("issuer", sa.String(length=512), nullable=False),
        sa.Column("subject_token_type", sa.String(length=128), nullable=False),
        sa.Column("audiences", JSONB(), nullable=False),
        sa.Column("jwks_uri", sa.String(length=512), nullable=True),
        sa.Column("pinned_jwks", JSONB(), nullable=True),
        sa.Column("claim_conditions", JSONB(), nullable=False),
        sa.Column("identity_id", UUID(as_uuid=True), nullable=False),
        sa.Column("token_lifetime_seconds", sa.Integer(), nullable=False),
        sa.Column("created_by", UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "(jwks_uri IS NOT NULL) <> (pinned_jwks IS NOT NULL)",
            name="ck_workload_trust_rules_one_key_source",
        ),
        sa.CheckConstraint(
            "token_lifetime_seconds BETWEEN 60 AND 3600",
            name="ck_workload_trust_rules_lifetime",
        ),
        sa.ForeignKeyConstraint(["identity_id"], ["users.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_workload_trust_rules_name"),
    )
    op.create_index(
        "ix_workload_trust_rules_lookup",
        "workload_trust_rules",
        ["enabled", "subject_token_type"],
    )


def downgrade() -> None:
    op.drop_index("ix_workload_trust_rules_lookup", table_name="workload_trust_rules")
    op.drop_table("workload_trust_rules")
