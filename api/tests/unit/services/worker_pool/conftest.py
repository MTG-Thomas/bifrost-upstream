"""Keep the worker-pool tests' committed state away from live Compose executors."""

from urllib.parse import urlsplit, urlunsplit

import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker
from src.core import database
from src.core.cache import redis_client
from tests.helpers.isolation import isolated_test_database


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def worker_pool_database(async_engine):
    async with isolated_test_database(async_engine.url) as engine:
        yield engine


@pytest_asyncio.fixture
async def async_session_factory(worker_pool_database, monkeypatch):
    test_sessions = async_sessionmaker(worker_pool_database, expire_on_commit=False)
    service_sessions = async_sessionmaker(worker_pool_database, expire_on_commit=False, autoflush=False)
    # Keep controller telemetry in a separate Redis database from the stack.
    redis_url = urlunsplit(urlsplit(redis_client._get_redis_url())._replace(path="/14"))
    with monkeypatch.context() as patch:
        patch.setattr(database, "_async_session_factory", service_sessions)
        patch.setattr(database, "_dispatch_session_factory", service_sessions)
        patch.setattr(database, "_liveness_session_factory", service_sessions)
        patch.setattr(redis_client, "_get_redis_url", lambda: redis_url)
        yield test_sessions
