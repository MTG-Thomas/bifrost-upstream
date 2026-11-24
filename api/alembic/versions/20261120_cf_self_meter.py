"""Retain durable Bifrost usage independently of worker and claim retention."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261120_cf_self_meter"
down_revision = "20261120_cf_runtime_canary"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cloudflare_installations", sa.Column("usage_advisory", JSONB()))
    op.add_column("cloudflare_installations", sa.Column("usage_incomplete_until", sa.DateTime(timezone=True)))
    op.execute("""
        UPDATE cloudflare_installations i
        SET usage_incomplete_until = date_trunc('month', statement_timestamp(), 'UTC') + interval '1 month'
        WHERE EXISTS (SELECT 1 FROM cloudflare_owned_resources o WHERE o.installation_id = i.id)
           OR EXISTS (SELECT 1 FROM cloudflare_worker_reservations r WHERE r.installation_id = i.id)
    """)
    op.create_table(
        "cloudflare_usage_incarnations",
        sa.Column("worker_id", sa.Uuid(), primary_key=True),
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey("cloudflare_installations.id"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        sa.Column("slots", sa.SmallInteger()),
        sa.CheckConstraint("slots BETWEEN 1 AND 4", name="ck_cf_usage_slots"),
        sa.CheckConstraint("deleted_at >= started_at", name="ck_cf_usage_lifetime"),
    )
    op.create_index("ix_cf_usage_incarnation_installation", "cloudflare_usage_incarnations", ["installation_id", "deleted_at"])
    op.create_table(
        "cloudflare_usage_activity",
        sa.Column("installation_id", sa.Uuid(), sa.ForeignKey("cloudflare_installations.id"), primary_key=True),
        sa.Column("period_start", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("dispatched_runs", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("sdk_requests", sa.BigInteger(), nullable=False, server_default="0"),
        sa.CheckConstraint("dispatched_runs >= 0 AND sdk_requests >= 0", name="ck_cf_usage_activity_nonnegative"),
    )

    op.execute("""
        INSERT INTO cloudflare_usage_incarnations
        SELECT w.id, p.installation_id, w.created_at, w.capacity_resource_deleted_at,
        CASE WHEN w.memory_limit_bytes IN (134217728, 268435456, 402653184, 536870912)
          THEN w.memory_limit_bytes / 134217728 END
        FROM cloudflare_worker_reservations p JOIN execution_workers w ON w.id = p.worker_id
    """)
    op.execute("""
        CREATE FUNCTION record_cf_usage_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO cloudflare_usage_incarnations
          SELECT w.id, NEW.installation_id, w.created_at, w.capacity_resource_deleted_at,
          CASE WHEN w.memory_limit_bytes IN (134217728, 268435456, 402653184, 536870912)
            THEN w.memory_limit_bytes / 134217728 END
          FROM execution_workers w WHERE w.id = NEW.worker_id;
          RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER record_cf_usage_incarnation AFTER INSERT ON cloudflare_worker_reservations
          FOR EACH ROW EXECUTE FUNCTION record_cf_usage_incarnation()
    """)
    op.execute("""
        CREATE FUNCTION settle_cf_usage_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          UPDATE cloudflare_usage_incarnations SET deleted_at = NEW.capacity_resource_deleted_at
          WHERE worker_id = NEW.id AND deleted_at IS NULL;
          RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER settle_cf_usage_incarnation AFTER UPDATE OF capacity_resource_deleted_at ON execution_workers
          FOR EACH ROW WHEN (NEW.capacity_resource_deleted_at IS NOT NULL)
          EXECUTE FUNCTION settle_cf_usage_incarnation()
    """)
    op.execute("""
        CREATE FUNCTION record_cf_usage_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO cloudflare_usage_activity (installation_id, period_start, dispatched_runs)
          SELECT installation_id, date_trunc('month', clock_timestamp(), 'UTC'), 1
          FROM cloudflare_worker_reservations WHERE worker_id = NEW.worker_id
          ON CONFLICT (installation_id, period_start) DO UPDATE
            SET dispatched_runs = cloudflare_usage_activity.dispatched_runs + 1;
          RETURN NEW;
        END $$
    """)
    op.execute("""
        CREATE TRIGGER record_cf_usage_dispatch AFTER INSERT ON execution_worker_claims
          FOR EACH ROW EXECUTE FUNCTION record_cf_usage_dispatch()
    """)
    op.execute("""
        CREATE FUNCTION cloudflare_usage_totals(target uuid, as_of timestamptz)
        RETURNS TABLE (launcher_seconds numeric, slot_seconds numeric, dispatched_runs numeric,
                       sdk_requests numeric, complete boolean) LANGUAGE sql STABLE AS $$
          WITH period AS (SELECT date_trunc('month', as_of, 'UTC') AS start_at),
          installations AS (
            SELECT i.id, i.usage_incomplete_until
            FROM cloudflare_installations i
            JOIN cloudflare_installations current ON current.id = target AND i.account_id = current.account_id
          ), lifetimes AS (
            SELECT m.slots, m.started_at, m.deleted_at,
              extract(epoch FROM least(coalesce(m.deleted_at, as_of), as_of)
                - greatest(m.started_at, period.start_at)) AS seconds
            FROM cloudflare_usage_incarnations m JOIN installations i ON i.id = m.installation_id CROSS JOIN period
            WHERE coalesce(m.deleted_at, as_of) >= period.start_at
          )
          SELECT coalesce((SELECT sum(seconds) FROM lifetimes), 0),
            coalesce((SELECT sum(seconds * slots) FROM lifetimes), 0),
            coalesce((SELECT sum(a.dispatched_runs) FROM cloudflare_usage_activity a
                       JOIN installations i ON i.id = a.installation_id CROSS JOIN period
                       WHERE a.period_start = period.start_at), 0),
            coalesce((SELECT sum(a.sdk_requests) FROM cloudflare_usage_activity a
                       JOIN installations i ON i.id = a.installation_id CROSS JOIN period
                       WHERE a.period_start = period.start_at), 0),
            EXISTS (SELECT 1 FROM installations)
              AND NOT EXISTS (SELECT 1 FROM installations WHERE usage_incomplete_until > as_of)
              AND NOT EXISTS (SELECT 1 FROM lifetimes WHERE slots IS NULL OR seconds < 0
                              OR started_at > as_of OR deleted_at > as_of)
              AND NOT EXISTS (
                  SELECT 1 FROM cloudflare_worker_reservations p JOIN installations i ON i.id = p.installation_id
                  LEFT JOIN cloudflare_usage_incarnations m ON m.worker_id = p.worker_id
                    AND m.installation_id = p.installation_id
                  WHERE m.worker_id IS NULL
              )
        $$;
    """)


def downgrade() -> None:
    op.execute(
        "LOCK TABLE cloudflare_installations, cloudflare_usage_incarnations, cloudflare_usage_activity "
        "IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM cloudflare_usage_incarnations)
               OR EXISTS (SELECT 1 FROM cloudflare_usage_activity)
               OR EXISTS (SELECT 1 FROM cloudflare_installations WHERE usage_advisory IS NOT NULL)
            THEN
                RAISE EXCEPTION 'Self-metering receipts must survive rollback'
                    USING ERRCODE = 'object_not_in_prerequisite_state';
            END IF;
        END $$;
    """)
    op.execute("DROP FUNCTION cloudflare_usage_totals(uuid, timestamptz)")
    for trigger, table in (
        ("record_cf_usage_dispatch", "execution_worker_claims"),
        ("settle_cf_usage_incarnation", "execution_workers"),
        ("record_cf_usage_incarnation", "cloudflare_worker_reservations"),
    ):
        op.execute(f"DROP TRIGGER {trigger} ON {table}")
        op.execute(f"DROP FUNCTION {trigger}()")
    op.drop_table("cloudflare_usage_activity")
    op.drop_table("cloudflare_usage_incarnations")
    op.drop_column("cloudflare_installations", "usage_incomplete_until")
    op.drop_column("cloudflare_installations", "usage_advisory")
