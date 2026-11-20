"""Index events by source and receive time for per-source windows.

Revision ID: 20261118_events_source_received
Revises: 20261117_integ_secret_encrypt

``EventRepository.count_by_source`` (event-source cards: last-24h count; the
source event list: paginated total) and ``get_by_source`` filter on
``event_source_id`` and bound or order by ``received_at``. With only the
single-column indexes, a 30-day count on a production copy switched from
``ix_events_received_at`` plus a source filter to a parallel sequential scan
of the 1.1 GB table (12.2 s cold). The composite index serves the equality
and the range together, and the ``count(*)`` form can use an index-only scan.

``ix_events_event_source_id`` is a left prefix of this index and therefore
redundant once it is valid; it is intentionally kept here and can be dropped
in a separate change after production index usage confirms it.

The index is created concurrently so event ingestion keeps writing while
PostgreSQL scans the table. Lock acquisition is bounded to 5s and the build to
30min; if a failed concurrent build leaves an invalid index behind, inspect it
and clean it up before retrying:

    DROP INDEX CONCURRENTLY IF EXISTS ix_events_event_source_id_received_at;
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20261118_events_source_received"
down_revision: str | None = "20261117_integ_secret_encrypt"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        try:
            op.execute("SET lock_timeout = '5s'")
            op.execute("SET statement_timeout = '30min'")
            op.execute(
                "CREATE INDEX CONCURRENTLY ix_events_event_source_id_received_at "
                "ON events (event_source_id, received_at)"
            )
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        try:
            op.execute("SET lock_timeout = '5s'")
            op.execute("SET statement_timeout = '30min'")
            op.execute(
                "DROP INDEX CONCURRENTLY IF EXISTS ix_events_event_source_id_received_at"
            )
        finally:
            op.execute("RESET statement_timeout")
            op.execute("RESET lock_timeout")
