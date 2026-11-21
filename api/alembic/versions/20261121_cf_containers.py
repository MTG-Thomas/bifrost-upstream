"""Persist disabled Containers policy and jointly fenced Linux reservations."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261121_cf_containers"
down_revision = "20261121_cf_workers_upgrade"
branch_labels = None
depends_on = None


def _workers_usage(*, contain_linux: bool) -> None:
    guard = "AND p.container_reserved_at IS NULL" if contain_linux else ""
    when = "WHEN (NEW.container_reserved_at IS NULL)" if contain_linux else ""
    # Container reservations already carry their own interval and rate pins.
    # The legacy lifetime recorder has no slot price for a 4-GiB process.
    op.execute("DROP TRIGGER record_cf_usage_incarnation ON cloudflare_worker_reservations")
    op.execute(f"""
        CREATE TRIGGER record_cf_usage_incarnation AFTER INSERT ON cloudflare_worker_reservations
          FOR EACH ROW {when} EXECUTE FUNCTION record_cf_usage_incarnation()
    """)
    op.execute(f"""
        CREATE OR REPLACE FUNCTION cloudflare_usage_totals(target uuid, as_of timestamptz)
        RETURNS TABLE (launcher_seconds numeric, slot_seconds numeric, dispatched_runs numeric,
                       sdk_requests numeric, complete boolean) LANGUAGE sql STABLE AS $$
          WITH period AS (SELECT date_trunc('month', as_of, 'UTC') AS start_at),
          installations AS (
            SELECT i.id, i.usage_incomplete_until
            FROM cloudflare_installations i
            JOIN cloudflare_installations current ON current.id = target AND i.account_id = current.account_id
          ), retained AS (
            SELECT m.slots, m.started_at, m.deleted_at
            FROM installations i JOIN cloudflare_usage_incarnations m ON m.installation_id = i.id
            WHERE m.deleted_at IS NULL
            UNION ALL
            SELECT m.slots, m.started_at, m.deleted_at
            FROM installations i JOIN cloudflare_usage_incarnations m ON m.installation_id = i.id CROSS JOIN period
            WHERE m.deleted_at >= period.start_at
          ), lifetimes AS (
            SELECT m.slots, m.started_at, m.deleted_at,
              extract(epoch FROM least(coalesce(m.deleted_at, as_of), as_of)
                - greatest(m.started_at, period.start_at)) AS seconds
            FROM retained m CROSS JOIN period
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
                  WHERE m.worker_id IS NULL {guard}
              )
        $$;
    """)


def upgrade() -> None:
    op.add_column("cloudflare_installations", sa.Column(
        "containers_policy", JSONB(), nullable=False, server_default=sa.literal_column(
            """'{"enabled":false,"instance_type":"standard-1","installation_instances":2,
                 "tenant_instances":1,"workflow_slots":1,"idle_seconds":120}'::jsonb"""
        ),
    ))
    op.add_column("cloudflare_installations", sa.Column("containers_ready_revision", sa.Integer()))
    op.add_column("cloudflare_installations", sa.Column("containers_release_digest", sa.String(64)))
    op.add_column("cloudflare_worker_reservations", sa.Column("container_tenant", sa.String(36)))
    op.add_column("cloudflare_worker_reservations", sa.Column("container_release", sa.String(64)))
    op.add_column("cloudflare_worker_reservations", sa.Column("container_envelope", JSONB()))
    op.add_column("cloudflare_worker_reservations", sa.Column(
        "container_reserved_at", sa.DateTime(timezone=True),
    ))
    op.add_column("cloudflare_worker_reservations", sa.Column(
        "container_stopped_at", sa.DateTime(timezone=True),
    ))
    op.create_check_constraint(
        "ck_cf_containers_readiness", "cloudflare_installations",
        "num_nonnulls(containers_ready_revision, containers_release_digest) IN (0, 2) "
        "AND (containers_ready_revision IS NULL OR containers_ready_revision > 0)",
    )
    op.create_check_constraint(
        "ck_cf_container_reservation", "cloudflare_worker_reservations",
        "num_nonnulls(container_tenant, container_release, container_envelope, container_reserved_at) IN (0, 4) "
        "AND (container_stopped_at IS NULL OR "
        "(container_reserved_at IS NOT NULL AND container_stopped_at >= container_reserved_at))",
    )
    _workers_usage(contain_linux=True)


def downgrade() -> None:
    # Dropping this revision with reserved instances would lose their physical and spend fences.
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM cloudflare_worker_reservations WHERE container_reserved_at IS NOT NULL) THEN
                RAISE EXCEPTION 'Container reservation history must be retained';
            END IF;
        END $$
    """)
    _workers_usage(contain_linux=False)
    op.drop_constraint("ck_cf_container_reservation", "cloudflare_worker_reservations", type_="check")
    op.drop_constraint("ck_cf_containers_readiness", "cloudflare_installations", type_="check")
    for name in (
        "container_stopped_at", "container_reserved_at", "container_envelope", "container_release", "container_tenant",
    ):
        op.drop_column("cloudflare_worker_reservations", name)
    for name in ("containers_release_digest", "containers_ready_revision", "containers_policy"):
        op.drop_column("cloudflare_installations", name)
