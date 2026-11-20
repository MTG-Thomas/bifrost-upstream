"""Use bounded lifetime and run-attribution lookups on the admission path."""

from alembic import op

revision = "20261120_cf_usage_lookup"
down_revision = "20261120_cf_staged_token"
branch_labels = None
depends_on = None


def _usage_totals(lifetime_rows: str) -> None:
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
            {lifetime_rows}
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
                  WHERE m.worker_id IS NULL
              )
        $$;
    """)


def upgrade() -> None:
    # Separate the NULL and date-range lookups so each can use both columns
    # of ix_cf_usage_incarnation_installation.
    _usage_totals("""
        SELECT m.slots, m.started_at, m.deleted_at
        FROM installations i JOIN cloudflare_usage_incarnations m ON m.installation_id = i.id
        WHERE m.deleted_at IS NULL
        UNION ALL
        SELECT m.slots, m.started_at, m.deleted_at
        FROM installations i JOIN cloudflare_usage_incarnations m ON m.installation_id = i.id CROSS JOIN period
        WHERE m.deleted_at >= period.start_at
    """)
    op.create_index(
        "ix_execution_worker_claims_run_assignment", "execution_worker_claims",
        ["family", "subject_id", "claimed_at", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_execution_worker_claims_run_assignment", table_name="execution_worker_claims")
    _usage_totals("""
        SELECT m.slots, m.started_at, m.deleted_at
        FROM cloudflare_usage_incarnations m JOIN installations i ON i.id = m.installation_id CROSS JOIN period
        WHERE coalesce(m.deleted_at, as_of) >= period.start_at
    """)
