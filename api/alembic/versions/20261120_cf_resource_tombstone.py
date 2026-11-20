"""Prevent a verified Cloudflare tombstone from authorizing another effect."""

from alembic import op

revision = "20261120_cf_resource_tombstone"
down_revision = "20261120_cf_installation_retire"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE FUNCTION cf_guard_resource_tombstone() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.state = 'absent' AND (
                NEW.state IS DISTINCT FROM OLD.state OR NEW.effect_id IS DISTINCT FROM OLD.effect_id
            ) THEN
                RAISE EXCEPTION 'Cloudflare resource tombstone cannot authorize another effect'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER cf_guard_resource_tombstone
        BEFORE UPDATE OF state, effect_id ON cloudflare_owned_resources
        FOR EACH ROW EXECUTE FUNCTION cf_guard_resource_tombstone()
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER cf_guard_resource_tombstone ON cloudflare_owned_resources")
    op.execute("DROP FUNCTION cf_guard_resource_tombstone()")
