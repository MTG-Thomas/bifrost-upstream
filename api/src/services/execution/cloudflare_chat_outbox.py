"""Project committed Cloudflare chat chunks onto the existing Redis stream."""

from __future__ import annotations

import asyncio
import json
import logging
from uuid import UUID, uuid4

from redis.exceptions import RedisError
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cache.keys import chat_run_events_stream_key, chat_run_events_version_key
from src.core.cache.redis_client import get_redis
from src.core.database import get_db_context
from src.core.pubsub import chat_run_event_envelope
from src.models.contracts.agents import ChatStreamChunk
from src.models.orm.agent_runs import CloudflareChatEvent
from src.models.orm.agents import Conversation

logger = logging.getLogger(__name__)

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
if not counter or (raw_counter and not string.match(raw_counter, '^%d+$')) or
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
    if not receipt then redis.call('SET', KEYS[3], 'armed') end
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
redis.call('SET', KEYS[3], tostring(sequence))
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


def receipt_key(row: CloudflareChatEvent) -> str:
    return f"bifrost:chat:{row.conversation_id}:receipt:{row.event_id}"


async def project(row: CloudflareChatEvent, floor: int) -> int:
    conversation_id = str(row.conversation_id)
    async with asyncio.timeout(2), get_redis() as redis:
        result = await redis.eval(  # type: ignore[misc]
            _PROJECT, 3, chat_run_events_stream_key(conversation_id),
            chat_run_events_version_key(conversation_id), receipt_key(row),
            "arm" if row.redis_floor is None else "publish", str(floor),
            json.dumps(row.envelope), f"bifrost:chat:{conversation_id}",
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
        .with_for_update(of=Conversation, skip_locked=True, key_share=True)
    )
    if conversation_id is None:
        return False
    row = await db.scalar(select(CloudflareChatEvent).where(
        CloudflareChatEvent.conversation_id == conversation_id, *pending,
    ).order_by(CloudflareChatEvent.id).limit(1))
    if row is None:
        return False
    high_water = await db.scalar(select(func.max(CloudflareChatEvent.redis_sequence)).where(
        CloudflareChatEvent.conversation_id == conversation_id,
    ))
    result = await project(row, max(high_water or 0, row.redis_floor or 0))
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


async def run_publisher(stop: asyncio.Event) -> None:
    from src.scheduler.control import wait_or_stop

    while not stop.is_set():
        try:
            for _ in range(100):
                async with get_db_context() as db:
                    if not await publish_one(db):
                        break
            async with get_db_context() as db:
                await discard_receipts(db)
        except (TimeoutError, RedisError):
            logger.warning("Cloudflare chat event publishing will retry", exc_info=True)
        await wait_or_stop(stop, 0.2)
