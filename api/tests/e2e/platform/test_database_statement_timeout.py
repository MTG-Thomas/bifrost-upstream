"""Statement bounds run through the production engine and transaction pooler."""

import os

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from src.core.database import create_pool_engine


@pytest.mark.asyncio
async def test_statement_timeout_cancels_and_recovers_after_rollback():
    engine = create_pool_engine(
        os.environ["BIFROST_DATABASE_URL"],
        pool_size=1, max_overflow=0, pool_timeout=10,
        statement_timeout_ms=100,
    )
    try:
        async with engine.connect() as connection:
            assert (await connection.execute(text("SHOW statement_timeout"))).scalar_one() == "100ms"
            with pytest.raises(DBAPIError, match="statement timeout") as error:
                await connection.execute(text("SELECT pg_sleep(1)"))
            assert error.value.orig.sqlstate == "57014"
            await connection.rollback()
            assert (await connection.execute(text("SELECT 1"))).scalar_one() == 1
            # A transaction-local override must not survive rollback or checkout.
            await connection.execute(text("SET LOCAL statement_timeout = 0"))
            await connection.rollback()

        async with engine.connect() as connection:
            assert (await connection.execute(text("SHOW statement_timeout"))).scalar_one() == "100ms"
            with pytest.raises(DBAPIError, match="statement timeout"):
                await connection.execute(text("SELECT pg_sleep(1)"))
            await connection.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_zero_disables_statement_timeout():
    engine = create_pool_engine(
        os.environ["BIFROST_DATABASE_URL"],
        pool_size=1, max_overflow=0, pool_timeout=10,
        statement_timeout_ms=0,
    )
    try:
        async with engine.connect() as connection:
            assert (await connection.execute(text("SHOW statement_timeout"))).scalar_one() == "0"
            # Longer than the enabled bound in the companion test.
            await connection.execute(text("SELECT pg_sleep(0.2)"))
            await connection.commit()
            assert (await connection.execute(text("SHOW statement_timeout"))).scalar_one() == "0"
    finally:
        await engine.dispose()
