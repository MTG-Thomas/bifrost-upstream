"""Real pooled client reuse/replacement, without changing shared application pools."""
import logging
import os

import pytest
from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker

from shared.runtime_database_identity import IDENTITY_QUERY, RuntimeDatabaseIdentityObserver
from src.core.database import create_pool_engine
from src.routers import health

pytestmark = pytest.mark.e2e


async def test_readiness_reports_only_after_reusing_a_real_pool_connection(monkeypatch, caplog):
    url = make_url(os.environ["BIFROST_DATABASE_URL"]).set(
        drivername="postgresql+asyncpg", host="postgres", port=5432,
    ).render_as_string(hide_password=False)
    engine = create_pool_engine(url, pool_size=1, max_overflow=0, pool_timeout=2)
    factory = async_sessionmaker(engine)
    connections = []
    statements = []
    event.listen(engine.sync_engine, "connect", lambda connection, record: connections.append(connection))
    event.listen(engine.sync_engine, "before_cursor_execute",
                 lambda connection, cursor, statement, parameters, context, many: statements.append(statement))
    monkeypatch.setattr(health, "_database_identity_observer", RuntimeDatabaseIdentityObserver())
    try:
        with caplog.at_level(logging.INFO, logger="shared.runtime_database_identity"):
            # The first connection is fresh; its replacement must not inherit its marker.
            async with factory() as db:
                assert await health.check_database(db) == ("database", {"status": "healthy", "type": "postgresql"})
                await (await db.connection()).invalidate()
            assert not caplog.records
            async with factory() as db:
                assert await health.check_database(db) == ("database", {"status": "healthy", "type": "postgresql"})
            assert not caplog.records
            async with factory() as db:
                assert await health.check_database(db) == ("database", {"status": "healthy", "type": "postgresql"})
        assert len(connections) == 2
        assert statements == [IDENTITY_QUERY] * 3
        assert len(caplog.records) == 1
        assert "client_connection_previously_observed=true" in caplog.text
    finally:
        await engine.dispose()
