"""Per-source event counts must stay answerable from the composite index."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects.postgresql import asyncpg as pg_asyncpg

from src.repositories.events import EventRepository


@pytest.mark.asyncio
async def test_count_by_source_counts_rows_not_ids() -> None:
    session = AsyncMock()
    result = MagicMock()
    result.scalar.return_value = 7
    session.execute.return_value = result
    since = datetime(2026, 10, 19, tzinfo=timezone.utc)

    count = await EventRepository(session).count_by_source(uuid4(), since=since)

    assert count == 7
    (statement,), _ = session.execute.await_args
    sql = str(statement.compile(dialect=pg_asyncpg.dialect()))
    # count(*) references no column outside ix_events_event_source_id_received_at,
    # so a source/time-window count can be an index-only scan.
    assert sql == (
        "SELECT count(*) AS count_1 \nFROM events \n"
        "WHERE events.event_source_id = $1::UUID "
        "AND events.received_at >= $2::TIMESTAMP WITH TIME ZONE"
    )
