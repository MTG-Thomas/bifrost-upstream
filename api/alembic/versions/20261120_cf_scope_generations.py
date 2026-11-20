"""Retain an exact cleanup endpoint for each Cloudflare deployment."""

from alembic import op

revision = "20261120_cf_scope_generations"
down_revision = "20261120_cf_usage_receipts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("cloudflare_capacity_scopes_pkey", "cloudflare_capacity_scopes", type_="primary")
    op.create_primary_key(
        "cloudflare_capacity_scopes_pkey", "cloudflare_capacity_scopes",
        ["installation_id", "scope", "deployment_revision"],
    )
    op.execute("""
        CREATE FUNCTION cf_keep_scope_target() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.scope IS DISTINCT FROM OLD.scope
               OR NEW.deployment_revision IS DISTINCT FROM OLD.deployment_revision
               OR NEW.endpoint IS DISTINCT FROM OLD.endpoint THEN
                RAISE EXCEPTION 'Cloudflare scope deployment target is immutable' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$;
    """)
    op.execute("""
        CREATE TRIGGER cf_keep_scope_target
        BEFORE UPDATE ON cloudflare_capacity_scopes
        FOR EACH ROW EXECUTE FUNCTION cf_keep_scope_target();
    """)


def downgrade() -> None:
    """Downgrade only when no scope has more than one deployment receipt."""
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM cloudflare_capacity_scopes
                GROUP BY installation_id, scope HAVING COUNT(*) > 1
            ) THEN
                RAISE EXCEPTION 'Cannot downgrade Cloudflare scopes with multiple retained generations';
            END IF;
        END;
        $$;
    """)
    op.execute("DROP TRIGGER cf_keep_scope_target ON cloudflare_capacity_scopes")
    op.execute("DROP FUNCTION cf_keep_scope_target()")
    op.drop_constraint("cloudflare_capacity_scopes_pkey", "cloudflare_capacity_scopes", type_="primary")
    op.create_primary_key(
        "cloudflare_capacity_scopes_pkey", "cloudflare_capacity_scopes", ["installation_id", "scope"],
    )
