"""Project committed Cloudflare chat chunks onto the existing Redis stream."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cache.keys import chat_run_events_stream_key, chat_run_events_version_key
from src.core.cache.redis_client import CacheError, get_redis
from src.core.database import get_db_context
from src.core.pubsub import chat_run_event_envelope, publish_agent_run_update
from src.models.contracts.agents import ChatStreamChunk
from src.models.orm.agent_runs import AgentRun, CloudflareAgentAttempt, CloudflareChatEvent
from src.models.orm.agents import Conversation
from src.services.work_dispatch import (
    listen_for_pool_wake,
    wait_for_next_pass,
    wake_pool_workers_after_commit,
)

logger = logging.getLogger(__name__)
_WAKE_CHAT_OUTBOX = "cloudflare_chat_outbox"
_RECEIPT_TTL_SECONDS = 172800
_OUTBOX_RETENTION = timedelta(days=2)

# Arm a per-event receipt before publication and commit the observed counter
# in PostgreSQL. Once armed, a missing receipt is evidence of projection loss,
# even when the event was the first Cloudflare event in the conversation.
_PROJECT = """
local stream_type = redis.call('TYPE', KEYS[1]).ok
local counter_type = redis.call('TYPE', KEYS[2]).ok
local receipt_type = redis.call('TYPE', KEYS[3]).ok
if (stream_type ~= 'stream' and stream_type ~= 'none') or
   (counter_type ~= 'string' and counter_type ~= 'none') or
   (receipt_type ~= 'string' and receipt_type ~= 'none') or
   (stream_type == 'none') ~= (counter_type == 'none') then
    return -1
end
local raw_counter = redis.call('GET', KEYS[2])
local counter = tonumber(raw_counter or '0')
local floor = tonumber(ARGV[2])
if not counter or (raw_counter and raw_counter ~= '0' and not string.match(raw_counter, '^[1-9]%d*$')) or
   counter < floor or counter >= 9007199254740991 then
    return -1
end
if stream_type == 'stream' then
    local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
    if #last == 0 then return -1 end
    local fields = last[1][2]
    local sequence
    for i = 1, #fields, 2 do
        if fields[i] == 'sequence' then sequence = tonumber(fields[i+1]) end
    end
    if sequence ~= counter then return -1 end
end
local receipt = redis.call('GET', KEYS[3])
if ARGV[1] == 'arm' then
    if not receipt then redis.call('SET', KEYS[3], 'armed', 'EX', ARGV[5]) end
    return counter
end
if not receipt then return -1 end
if receipt ~= 'armed' then
    local committed = tonumber(receipt)
    if not committed or committed <= floor or committed > counter then return -1 end
    return committed
end
local envelope = cjson.decode(ARGV[3])
local sequence = redis.call('INCR', KEYS[2])
envelope['sequence'] = sequence
local encoded = cjson.encode(envelope)
redis.call(
    'XADD', KEYS[1], 'MAXLEN', '~', 20000, '*',
    'event', encoded, 'sequence', tostring(sequence), 'kind', envelope['kind'],
    'status', envelope['status'], 'occurred_at', envelope['occurred_at']
)
redis.call('EXPIRE', KEYS[1], 86400)
redis.call('EXPIRE', KEYS[2], 86400)
redis.call('SET', KEYS[3], tostring(sequence), 'EX', ARGV[5])
redis.call('PUBLISH', ARGV[4], encoded)
return sequence
"""


def stage_chat_chunk(
    db: AsyncSession,
    conversation_id: UUID,
    run_id: UUID,
    claim_id: UUID,
    sequence: int,
    status: str,
    chunk: ChatStreamChunk,
) -> None:
    """Call under the attempt lock after the unseen sequence has been accepted."""
    event_id = uuid4()
    db.add(CloudflareChatEvent(
        claim_id=claim_id, conversation_id=conversation_id, chunk_sequence=sequence, event_id=event_id,
        envelope=chat_run_event_envelope(conversation_id, run_id, chunk.type, status, chunk, event_id=event_id),
    ))
    wake_pool_workers_after_commit(db, _WAKE_CHAT_OUTBOX)


def receipt_key(row: CloudflareChatEvent) -> str:
    return f"bifrost:chat:{row.conversation_id}:receipt:{row.event_id}"


async def project(row: CloudflareChatEvent, floor: int) -> int:
    conversation_id = str(row.conversation_id)
    async with asyncio.timeout(2), get_redis() as redis:
        result = await redis.eval(  # type: ignore[misc]
            _PROJECT, 3, chat_run_events_stream_key(conversation_id),
            chat_run_events_version_key(conversation_id), receipt_key(row),
            "arm" if row.redis_floor is None else "publish", str(floor),
            json.dumps(row.envelope), f"bifrost:chat:{conversation_id}", str(_RECEIPT_TTL_SECONDS),
        )
    return int(result)


async def publish_one(db: AsyncSession) -> bool:
    """Advance one stage; the caller commits before starting another stage."""
    pending = (CloudflareChatEvent.redis_sequence.is_(None), CloudflareChatEvent.projection_lost.is_(False))
    conversation_id = await db.scalar(
        select(Conversation.id)
        .where(select(CloudflareChatEvent.id).where(
            CloudflareChatEvent.conversation_id == Conversation.id, *pending,
        ).exists())
        .order_by(Conversation.id)
        .limit(1)
        .with_for_update(of=Conversation, skip_locked=True)
    )
    if conversation_id is None:
        return False
    row = await db.scalar(select(CloudflareChatEvent).where(
        CloudflareChatEvent.conversation_id == conversation_id, *pending,
    ).order_by(CloudflareChatEvent.id).limit(1))
    if row is None:
        return False
    # A previous turn may belong to a Redis stream that expired normally.
    lost = await has_projection_loss(db, row.claim_id)
    high_water = await db.scalar(select(func.max(CloudflareChatEvent.redis_sequence)).where(
        CloudflareChatEvent.claim_id == row.claim_id,
    ))
    queued = await db.scalar(select(CloudflareAgentAttempt.data["chat"]["queued_sequence"].as_integer()).where(
        CloudflareAgentAttempt.claim_id == row.claim_id,
    ))
    result = -1 if lost else await project(row, max(high_water or 0, queued or 0, row.redis_floor or 0))
    if result < 0:
        await db.execute(update(CloudflareChatEvent).where(
            CloudflareChatEvent.claim_id == row.claim_id, *pending,
        ).values(projection_lost=True))
        logger.warning("Cloudflare chat Redis projection lost for claim %s", row.claim_id)
    elif row.redis_floor is None:
        row.redis_floor = result
    else:
        row.redis_sequence = result
    return True


async def has_projection_loss(db: AsyncSession, claim_id: UUID) -> bool:
    return bool(await db.scalar(select(CloudflareChatEvent.id).where(
        CloudflareChatEvent.claim_id == claim_id, CloudflareChatEvent.projection_lost.is_(True),
    ).limit(1)))


async def discard_receipts(db: AsyncSession) -> None:
    """Remove receipts only after the projection result survives commit."""
    rows = (await db.scalars(select(CloudflareChatEvent).where(
        CloudflareChatEvent.redis_floor.is_not(None),
        CloudflareChatEvent.redis_sequence.is_not(None) | CloudflareChatEvent.projection_lost.is_(True),
    ).limit(100).with_for_update(skip_locked=True))).all()
    if not rows:
        return
    async with asyncio.timeout(2), get_redis() as redis:
        await redis.delete(*(receipt_key(row) for row in rows))
    for row in rows:
        row.redis_floor = None


async def prune_settled_events(db: AsyncSession) -> None:
    expired = select(CloudflareChatEvent.id).where(
        CloudflareChatEvent.created_at < datetime.now(UTC) - _OUTBOX_RETENTION,
        CloudflareChatEvent.redis_floor.is_(None),
        CloudflareChatEvent.redis_sequence.is_not(None) | CloudflareChatEvent.projection_lost.is_(True),
        select(CloudflareAgentAttempt.claim_id).where(
            CloudflareAgentAttempt.claim_id == CloudflareChatEvent.claim_id,
            CloudflareAgentAttempt.data.contains({"settled": True}),
        ).exists(),
    ).order_by(CloudflareChatEvent.created_at).limit(500).with_for_update(skip_locked=True)
    await db.execute(delete(CloudflareChatEvent).where(CloudflareChatEvent.id.in_(expired)))


async def fail_settled_runs_with_projection_loss() -> None:
    """Catch loss detected after the terminal and its outbox row committed."""
    async with get_db_context() as db:
        runs = (await db.scalars(
            select(AgentRun).join(CloudflareAgentAttempt, CloudflareAgentAttempt.run_id == AgentRun.id)
            .where(AgentRun.status == "completed", select(CloudflareChatEvent.id).where(
                CloudflareChatEvent.claim_id == CloudflareAgentAttempt.claim_id,
                CloudflareChatEvent.projection_lost.is_(True),
            ).exists())
            .limit(20).with_for_update(of=AgentRun, skip_locked=True)
        )).all()
        updates = []
        for run in runs:
            attempt = (await db.scalars(select(CloudflareAgentAttempt).where(
                CloudflareAgentAttempt.run_id == run.id,
            ))).one()
            attempt.data = {**attempt.data, "attempt_state": "uncertain"}
            run.status = "failed"
            run.error = "The Cloudflare chat event projection was lost. The saved messages may be incomplete."
            run.output = {"text": (run.output or {}).get("text"), "partial": True}
            updates.append((run, run.agent.name if run.agent else "Chat"))
    for run, name in updates:
        await publish_agent_run_update(run, name)


async def run_publisher(stop: asyncio.Event) -> None:
    from src.config import get_settings
    from src.scheduler.control import wait_or_stop

    wake = asyncio.Event()
    listener = asyncio.create_task(listen_for_pool_wake(get_settings().redis_url, {_WAKE_CHAT_OUTBOX: wake}))
    maintenance_due = 0.0
    try:
        while not stop.is_set():
            started = time.monotonic()
            try:
                for _ in range(100):
                    async with get_db_context() as db:
                        if not await publish_one(db):
                            break
                else:
                    wake.set()
                # A preceding publisher may have crashed after marking loss.
                await fail_settled_runs_with_projection_loss()
                async with get_db_context() as db:
                    await discard_receipts(db)
                    if started >= maintenance_due:
                        await prune_settled_events(db)
                if started >= maintenance_due:
                    maintenance_due = started + 60
            except (TimeoutError, CacheError, SQLAlchemyError):
                logger.warning("Cloudflare chat event publishing will retry", exc_info=True)
                await wait_or_stop(stop, 5)
                continue
            await wait_for_next_pass(stop, wake, since=started, spacing=0.1, sweep=15)
    finally:
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)
