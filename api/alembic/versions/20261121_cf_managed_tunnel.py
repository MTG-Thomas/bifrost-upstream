"""Store Cloudflare installation reachability and the selected DNS zone."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261121_cf_managed_tunnel"
down_revision = "20261121_cf_cleanup_window"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "cloudflare_installations",
        sa.Column("reachability_mode", sa.String(32), nullable=False, server_default="existing_https_endpoint"),
    )
    op.add_column("cloudflare_installations", sa.Column("zone_id", sa.String(32)))
    op.add_column("cloudflare_installations", sa.Column("tunnel_token_version", sa.Integer()))
    op.add_column("cloudflare_installations", sa.Column("tunnel_token_projected_version", sa.Integer()))
    op.add_column(
        "cloudflare_owned_resources",
        sa.Column("provider_details", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    )
    op.create_check_constraint(
        "ck_cf_installation_reachability",
        "cloudflare_installations",
        "(reachability_mode = 'existing_https_endpoint' AND zone_id IS NULL) OR "
        "(reachability_mode = 'managed_tunnel' AND zone_id IS NOT NULL AND zone_id ~ '^[a-f0-9]{32}$')",
    )
    op.drop_constraint("ck_cf_credential_scope", "cloudflare_capacity_credentials", type_="check")
    op.create_check_constraint(
        "ck_cf_credential_scope", "cloudflare_capacity_credentials",
        "(purpose IN ('account_token', 'control_root', 'tunnel_secret', 'connector_token') AND scope = '') OR "
        "(purpose = 'scope_bearer' AND scope <> '')",
    )


def downgrade() -> None:
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM cloudflare_installations WHERE reachability_mode = 'managed_tunnel') THEN
                RAISE EXCEPTION 'Managed Tunnel inventory requires this schema';
            END IF;
        END $$
    """)
    op.drop_constraint("ck_cf_credential_scope", "cloudflare_capacity_credentials", type_="check")
    op.create_check_constraint(
        "ck_cf_credential_scope", "cloudflare_capacity_credentials",
        "(purpose IN ('account_token', 'control_root') AND scope = '') OR "
        "(purpose = 'scope_bearer' AND scope <> '')",
    )
    op.drop_column("cloudflare_installations", "tunnel_token_version")
    op.drop_column("cloudflare_installations", "tunnel_token_projected_version")
    op.drop_column("cloudflare_owned_resources", "provider_details")
    op.drop_constraint("ck_cf_installation_reachability", "cloudflare_installations", type_="check")
    op.drop_column("cloudflare_installations", "zone_id")
    op.drop_column("cloudflare_installations", "reachability_mode")
