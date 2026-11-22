"""Storage isolation primitives for tests."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol
from uuid import uuid4

from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool


@asynccontextmanager
async def isolated_test_database(url: URL) -> AsyncIterator[AsyncEngine]:
    """Clone the migrated test template so live executors cannot see committed test rows."""
    if url.host == "pgbouncer":
        # The transaction pool exposes the application database only.
        url = url.set(host="postgres")
    name = f"capacity_test_{uuid4().hex}"
    admin = create_async_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool)
    engine = create_async_engine(url.set(database=name), poolclass=NullPool)
    try:
        async with admin.connect() as connection:
            await connection.execute(text(f"CREATE DATABASE {name} TEMPLATE bifrost_test_template"))
            try:
                yield engine
            finally:
                await engine.dispose()
                await connection.execute(text(f"DROP DATABASE {name} WITH (FORCE)"))
    finally:
        await admin.dispose()


class ScannableRedis(Protocol):
    async def scan(
        self, cursor: int, *, match: str, count: int
    ) -> tuple[int, list[str]]:
        raise NotImplementedError

    async def delete(self, key: str) -> int:
        raise NotImplementedError


async def clear_redis_module_cache(redis: ScannableRedis) -> None:
    """Delete every cached virtual module without assuming multi-key delete support."""
    cursor = 0
    while True:
        cursor, keys = await redis.scan(
            cursor,
            match="bifrost:module:*",
            count=100,
        )
        for key in keys:
            await redis.delete(key)
        if cursor == 0:
            return
