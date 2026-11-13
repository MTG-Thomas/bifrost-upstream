"""
Per-user role cache (write-through).

Backs `has_role` lookups in the table-policy evaluator. Replaces the
per-request `select(Role.id, Role.name) JOIN UserRole WHERE user_id=...`
queries previously fired from `get_execution_context` (HTTP) and
`_populate_user_roles` (WebSocket).

Cache shape
-----------
Key: ``bifrost:role_cache:user:{user_id}``
Value: JSON ``{"role_ids": [...], "role_names": [...], "v": 1}``
TTL:   1 hour (defense-in-depth; correctness comes from write-through
       invalidation at every Role / UserRole mutation site).

Invalidation
------------
- ``invalidate_user(user_id)``: drop one user's entry.
- ``invalidate_role(role_id)``: scan every cached user, drop entries that
  contain this role_id. Acceptable at <10k cached users.

Mutation sites call ``invalidate_users_after_commit`` /
``invalidate_role_after_commit`` instead. Evicting before the transaction
commits lets a concurrent ``get_user_roles`` re-cache the still-committed old
rows for the full TTL, so a removed role would keep authorizing requests.

Empty role lists are a valid cached value, never treated as a miss.
Redis failures fall back to DB (read) or are logged (invalidation), per
the broader cache pattern in ``src/core/cache/invalidation.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import event

from src.core.cache.redis_client import get_shared_redis
from src.core.log_safety import log_safe

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Schema version for the cached payload. Bump if the structure changes so
# stale entries are treated as a miss instead of being mis-parsed.
_CACHE_SCHEMA_VERSION = 1
_ROLE_CACHE_KEY_PREFIX = "bifrost:role_cache:user:"
_ROLE_CACHE_TTL = 3600  # 1 hour


def _key(user_id: UUID) -> str:
    return f"{_ROLE_CACHE_KEY_PREFIX}{user_id}"


async def get_user_roles(
    user_id: UUID, db: "AsyncSession"
) -> tuple[list[UUID], list[str]]:
    """Return ``(role_ids, role_names)`` for a user. Cache-first, DB on miss.

    On miss, queries the DB, populates the cache with TTL, and returns.
    On Redis failure, falls back to DB without caching (logged as warning).
    Empty role lists are a valid cache value and are NOT re-queried.
    """
    key = _key(user_id)

    # Read path
    try:
        r = await get_shared_redis()
        raw = await r.get(key)
    except Exception as e:
        logger.warning(f"Role cache read failed, falling back to DB: {e}")
        raw = None

    if raw is not None:
        try:
            payload = json.loads(raw)
            if (
                isinstance(payload, dict)
                and payload.get("v") == _CACHE_SCHEMA_VERSION
                and isinstance(payload.get("role_ids"), list)
                and isinstance(payload.get("role_names"), list)
            ):
                role_ids = [UUID(s) for s in payload["role_ids"]]
                role_names = list(payload["role_names"])
                return role_ids, role_names
            # Wrong shape / stale schema: treat as miss, fall through to DB.
            logger.debug("Role cache entry for %s has unexpected shape; refetching", log_safe(user_id))
        except (ValueError, TypeError) as e:
            logger.warning("Role cache entry for %s unparseable; refetching: %s", log_safe(user_id), log_safe(e))

    # Miss -> DB
    from sqlalchemy import select

    from src.models.orm.users import Role, UserRole

    result = await db.execute(
        select(Role.id, Role.name)
        .join(UserRole, UserRole.role_id == Role.id)
        .where(UserRole.user_id == user_id)
    )
    rows = result.all()
    role_ids = [r.id for r in rows]
    role_names = [r.name for r in rows]

    # Populate cache (best-effort)
    payload = {
        "role_ids": [str(rid) for rid in role_ids],
        "role_names": role_names,
        "v": _CACHE_SCHEMA_VERSION,
    }
    try:
        r = await get_shared_redis()
        await r.set(key, json.dumps(payload), ex=_ROLE_CACHE_TTL)
    except Exception as e:
        logger.warning("Role cache populate failed for user %s: %s", log_safe(user_id), log_safe(e))

    return role_ids, role_names


async def invalidate_user(user_id: UUID) -> None:
    """Drop the cache entry for one user.

    Mutation sites use ``invalidate_users_after_commit``.
    Redis failures are logged; the next read will see stale data until TTL.
    """
    try:
        r = await get_shared_redis()
        await r.delete(_key(user_id))
        logger.debug("Invalidated role cache for user %s", log_safe(user_id))
    except Exception as e:
        logger.warning(
            "Failed to invalidate role cache for user %s: %s",
            log_safe(user_id),
            log_safe(e),
        )


async def invalidate_role(role_id: UUID) -> None:
    """Drop cache entries for every user who has this role.

    Mutation sites use ``invalidate_role_after_commit``. Scans
    ``bifrost:role_cache:user:*`` and inspects each entry's ``role_ids``
    list. Acceptable at <10k cached users; if the cache grows substantially
    larger consider a reverse index.
    """
    role_id_str = str(role_id)
    try:
        r = await get_shared_redis()
        async for key in r.scan_iter(match=f"{_ROLE_CACHE_KEY_PREFIX}*"):
            try:
                raw = await r.get(key)
                if raw is None:
                    continue
                payload = json.loads(raw)
                if role_id_str in payload.get("role_ids", []):
                    await r.delete(key)
            except (ValueError, TypeError) as e:
                # Unparseable entry — drop it so we don't keep hitting the same
                # bad value on every invalidation sweep.
                logger.debug(f"Dropping unparseable role cache entry {key}: {e}")
                await r.delete(key)
        logger.debug(f"Invalidated role cache for role {role_id}")
    except Exception as e:
        logger.warning(f"Failed to invalidate role cache for role {role_id}: {e}")


_after_commit_invalidations: set[asyncio.Task[None]] = set()


def _after_commit(session: "AsyncSession", invalidate: Callable[[], Awaitable[None]]) -> None:
    def run(_session: Any) -> None:
        task = asyncio.get_running_loop().create_task(invalidate())
        _after_commit_invalidations.add(task)
        task.add_done_callback(_after_commit_invalidations.discard)

    event.listen(session.sync_session, "after_commit", run, once=True)


def invalidate_users_after_commit(session: "AsyncSession", user_ids: Iterable[UUID]) -> None:
    """Drop these users' entries once ``session`` commits its role changes."""
    pending = list(user_ids)

    async def invalidate() -> None:
        for user_id in pending:
            await invalidate_user(user_id)

    _after_commit(session, invalidate)


def invalidate_role_after_commit(session: "AsyncSession", role_id: UUID) -> None:
    """Drop every entry holding ``role_id`` once ``session`` commits."""
    _after_commit(session, lambda: invalidate_role(role_id))
