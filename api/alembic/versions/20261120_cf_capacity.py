"""Persist the Cloudflare capacity installation and resource journal."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

from migration_policy_v1 import refuse_populated_downgrade

revision = "20261120_cf_capacity"
down_revision = "20261119_cf_chat_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloudflare_installations",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("singleton", sa.Boolean(), unique=True, server_default=sa.true(), nullable=False),
        sa.Column("account_id", sa.String(32), nullable=False),
        sa.Column("worker_ingress_origin", sa.String(255), nullable=False),
        sa.Column("consent_version", sa.String(64), nullable=False),
        sa.Column("consent_actor", sa.String(255), nullable=False),
        sa.Column("consent_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("policy", JSONB(), nullable=False),
        sa.Column("desired_revision", sa.Integer(), nullable=False),
        sa.Column("desired_state", sa.String(16), nullable=False),
        sa.Column("admitted_revision", sa.Integer()),
        sa.CheckConstraint("singleton", name="ck_cf_installation_singleton"),
        sa.CheckConstraint("desired_revision > 0", name="ck_cf_installation_revision"),
        sa.CheckConstraint("desired_state IN ('scaled_in', 'enabled', 'disconnected')", name="ck_cf_desired_state"),
        sa.CheckConstraint(
            "admitted_revision IS NULL OR (desired_state = 'enabled' AND admitted_revision = desired_revision)",
            name="ck_cf_admitted_revision",
        ),
        sa.UniqueConstraint("id", "account_id", name="uq_cf_installation_account"),
    )
    op.create_table(
        "cloudflare_capacity_credentials",
        sa.Column("installation_id", sa.Uuid(), primary_key=True),
        sa.Column("purpose", sa.String(16), primary_key=True),
        sa.Column("scope", sa.String(64), primary_key=True),
        sa.Column("version", sa.Integer(), primary_key=True),
        sa.Column("account_id", sa.String(32), nullable=False),
        sa.Column("ciphertext", sa.Text()),
        sa.Column("recorded_by", sa.String(255), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["installation_id", "account_id"],
            ["cloudflare_installations.id", "cloudflare_installations.account_id"],
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("version > 0", name="ck_cf_credential_version"),
        sa.CheckConstraint(
            "(purpose IN ('account_token', 'control_root') AND scope = '') OR "
            "(purpose = 'scope_bearer' AND scope <> '')",
            name="ck_cf_credential_scope",
        ),
    )
    op.create_table(
        "cloudflare_capacity_scopes",
        sa.Column(
            "installation_id", sa.Uuid(), sa.ForeignKey("cloudflare_installations.id", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("scope", sa.String(64), primary_key=True),
        sa.Column("endpoint", sa.String(255), nullable=False),
        sa.Column("deployment_revision", sa.Integer(), nullable=False),
        sa.Column("bearer_purpose", sa.String(16), server_default="scope_bearer", nullable=False),
        sa.Column("bearer_version", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["installation_id", "bearer_purpose", "scope", "bearer_version"],
            [
                "cloudflare_capacity_credentials.installation_id",
                "cloudflare_capacity_credentials.purpose",
                "cloudflare_capacity_credentials.scope",
                "cloudflare_capacity_credentials.version",
            ],
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("bearer_purpose = 'scope_bearer'", name="ck_cf_scope_bearer_purpose"),
        sa.CheckConstraint("deployment_revision > 0", name="ck_cf_scope_deployment_revision"),
    )
    op.create_table(
        "cloudflare_owned_resources",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("installation_id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.String(32), nullable=False),
        sa.Column("scope", sa.String(64), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("target_name", sa.String(255), nullable=False),
        sa.Column("desired_revision", sa.Integer(), nullable=False),
        sa.Column("artifact_digest", sa.String(64), nullable=False),
        sa.Column("effect_id", sa.Uuid(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("provider_id", sa.String(255)),
        sa.Column("provider_version", sa.String(255)),
        sa.Column("observed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(
            ["installation_id", "account_id"],
            ["cloudflare_installations.id", "cloudflare_installations.account_id"],
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("account_id", "kind", "target_name", name="uq_cf_resource_target"),
        sa.UniqueConstraint("account_id", "kind", "provider_id", name="uq_cf_resource_provider"),
        sa.CheckConstraint("desired_revision > 0", name="ck_cf_resource_revision"),
        sa.UniqueConstraint("effect_id", name="uq_cf_resource_effect"),
        sa.CheckConstraint(
            "state IN ('creating', 'present', 'deleting', 'absent') AND "
            "(state NOT IN ('present', 'deleting') OR "
            "(provider_id IS NOT NULL AND provider_version IS NOT NULL))",
            name="ck_cf_resource_observation",
        ),
    )
    op.create_index("ix_cloudflare_owned_resources_installation_id", "cloudflare_owned_resources", ["installation_id"])


def downgrade() -> None:
    refuse_populated_downgrade(
        "cloudflare_owned_resources", "cloudflare_capacity_scopes",
        "cloudflare_capacity_credentials", "cloudflare_installations",
    )
    op.drop_table("cloudflare_owned_resources")
    op.drop_table("cloudflare_capacity_scopes")
    op.drop_table("cloudflare_capacity_credentials")
    op.drop_table("cloudflare_installations")
