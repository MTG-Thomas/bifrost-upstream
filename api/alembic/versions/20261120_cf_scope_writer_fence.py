"""Enforce Cloudflare scope writers and retain installation-local receipts."""

from alembic import op

revision = "20261120_cf_scope_writer_fence"
down_revision = "20261120_cf_reservation_pin"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_cf_active_has_account_token", "cloudflare_installations",
        "retired_at IS NOT NULL OR api_token_version IS NOT NULL",
    )
    op.drop_constraint("uq_cf_resource_target", "cloudflare_owned_resources", type_="unique")
    op.create_unique_constraint(
        "uq_cf_resource_target", "cloudflare_owned_resources",
        ["installation_id", "account_id", "kind", "target_name"],
    )
    op.execute("""
        CREATE FUNCTION cf_lock_scope_installation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                PERFORM id FROM cloudflare_installations WHERE id = OLD.installation_id FOR UPDATE;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' AND NEW.installation_id IS DISTINCT FROM OLD.installation_id THEN
                RAISE EXCEPTION 'Cloudflare scope installation is immutable' USING ERRCODE = '23514';
            END IF;
            PERFORM id FROM cloudflare_installations WHERE id = NEW.installation_id FOR UPDATE;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER cf_lock_scope_installation
        BEFORE INSERT OR UPDATE OR DELETE ON cloudflare_capacity_scopes
        FOR EACH ROW EXECUTE FUNCTION cf_lock_scope_installation()
    """)
    op.execute("""
        CREATE FUNCTION cf_retain_resource_tombstone() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.state = 'absent' THEN
                RAISE EXCEPTION 'Cloudflare resource tombstone must be retained' USING ERRCODE = '23514';
            END IF;
            RETURN OLD;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER cf_retain_resource_tombstone
        BEFORE DELETE ON cloudflare_owned_resources
        FOR EACH ROW EXECUTE FUNCTION cf_retain_resource_tombstone()
    """)


def downgrade() -> None:
    """Downgrade only while the old account-wide uniqueness still holds."""
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM cloudflare_owned_resources
                GROUP BY account_id, kind, target_name HAVING COUNT(*) > 1
            ) THEN
                RAISE EXCEPTION 'Cannot downgrade Cloudflare receipt keys with same-account tombstones';
            END IF;
        END;
        $$;
    """)
    op.execute("DROP TRIGGER cf_retain_resource_tombstone ON cloudflare_owned_resources")
    op.execute("DROP FUNCTION cf_retain_resource_tombstone()")
    op.execute("DROP TRIGGER cf_lock_scope_installation ON cloudflare_capacity_scopes")
    op.execute("DROP FUNCTION cf_lock_scope_installation()")
    op.drop_constraint("ck_cf_active_has_account_token", "cloudflare_installations", type_="check")
    op.drop_constraint("uq_cf_resource_target", "cloudflare_owned_resources", type_="unique")
    op.create_unique_constraint(
        "uq_cf_resource_target", "cloudflare_owned_resources", ["account_id", "kind", "target_name"],
    )
