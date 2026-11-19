"""Exercise PostgreSQL and Redis at the outbox transaction boundaries."""

from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from src.core.cache.keys import chat_run_events_stream_key, chat_run_events_version_key
from src.core.cache.redis_client import get_redis
from src.core.pubsub import publish_chat_run_event, replay_chat_run_events
from src.models.contracts.agents import ChatStreamChunk
from src.models.orm.agent_runs import AgentRun, CloudflareAgentAttempt, CloudflareChatEvent
from src.models.orm.agents import Conversation
from src.models.orm.users import User
from src.services.execution.cloudflare_chat_outbox import (
    discard_receipts,
    publish_one,
    receipt_key,
    stage_chat_chunk,
)

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def attempt(db_session):
    user = User(email=f"{uuid4()}@example.invalid", is_superuser=True)
    db_session.add(user)
    await db_session.flush()
    conversation = Conversation(user_id=user.id)
    db_session.add(conversation)
    await db_session.flush()
    run = AgentRun(trigger_type="chat", conversation_id=conversation.id)
    db_session.add(run)
    await db_session.flush()
    attempt = CloudflareAgentAttempt(claim_id=uuid4(), run_id=run.id, data={"events": []})
    db_session.add(attempt)
    await db_session.commit()
    user_id, conversation_id, run_id, claim_id = user.id, conversation.id, run.id, attempt.claim_id
    try:
        yield conversation_id, run_id, claim_id
    finally:
        await db_session.rollback()
        rows = (await db_session.scalars(select(CloudflareChatEvent).where(
            CloudflareChatEvent.claim_id == claim_id,
        ))).all()
        async with get_redis() as redis:
            await redis.delete(
                chat_run_events_stream_key(str(conversation_id)),
                chat_run_events_version_key(str(conversation_id)),
                *(receipt_key(row) for row in rows),
            )
        await db_session.execute(delete(AgentRun).where(AgentRun.id == run_id))
        await db_session.execute(delete(User).where(User.id == user_id))
        await db_session.commit()


async def _stage_delta(db, attempt, sequence, content):
    conversation_id, run_id, claim_id = attempt
    journal = await db.get(CloudflareAgentAttempt, claim_id)
    journal.data = {"events": [*journal.data["events"], content]}
    stage_chat_chunk(db, conversation_id, run_id, claim_id, sequence, "running",
                     ChatStreamChunk(type="delta", content=content))


@pytest.mark.asyncio
async def test_outbox_recovers_commits_and_reuses_redis_sequence(attempt, async_session_factory):
    conversation_id, run_id, claim_id = attempt
    async with async_session_factory() as db:
        await _stage_delta(db, attempt, 1, "one")
        await db.rollback()
    async with async_session_factory() as db:
        journal = await db.get(CloudflareAgentAttempt, claim_id)
        assert journal.data == {"events": []}
        assert not await publish_one(db)
        await _stage_delta(db, attempt, 1, "one")
        await _stage_delta(db, attempt, 2, "two")
        await db.commit()

    # A new publisher observes the durable rows after the producer stopped.
    await publish_chat_run_event(conversation_id, run_id, "run_status", "running",
                                 ChatStreamChunk(type="run_status", run_status="running"))
    async with async_session_factory() as db:
        assert await publish_one(db)  # Arm the receipt.
        await db.commit()
    async with async_session_factory() as db:
        assert await publish_one(db)  # Publish, then lose the PostgreSQL marking transaction.
        await db.rollback()
    await publish_chat_run_event(conversation_id, run_id, "run_status", "running",
                                 ChatStreamChunk(type="run_status", run_status="running"))
    async with async_session_factory() as db:
        for _ in range(3):
            assert await publish_one(db)
            await db.commit()
        assert not await publish_one(db)
        await discard_receipts(db)
        await db.commit()
        rows = (await db.scalars(select(CloudflareChatEvent).where(
            CloudflareChatEvent.claim_id == claim_id,
        ).order_by(CloudflareChatEvent.chunk_sequence))).all()
        assert [row.redis_sequence for row in rows] == [2, 4]
        async with get_redis() as redis:
            assert await redis.exists(*(receipt_key(row) for row in rows)) == 0

    events = await replay_chat_run_events(conversation_id)
    assert [(event["sequence"], event["payload"].get("content")) for event in events] == [
        (1, None), (2, "one"), (3, None), (4, "two"),
    ]
    assert [events[index]["event_id"] for index in (1, 3)] == [str(row.event_id) for row in rows]


@pytest.mark.asyncio
async def test_outbox_refuses_to_guess_after_redis_receipt_loss(attempt, db_session):
    conversation_id, _, claim_id = attempt
    await _stage_delta(db_session, attempt, 1, "one")
    await db_session.commit()
    assert await publish_one(db_session)
    await db_session.commit()
    assert await publish_one(db_session)
    await db_session.rollback()

    row = await db_session.scalar(select(CloudflareChatEvent).where(CloudflareChatEvent.claim_id == claim_id))
    assert row is not None
    async with get_redis() as redis:
        assert await redis.delete(receipt_key(row)) == 1
    assert await publish_one(db_session)
    await db_session.commit()
    assert row.projection_lost and row.redis_sequence is None
    assert not await publish_one(db_session)
    events = await replay_chat_run_events(conversation_id)
    assert [event["sequence"] for event in events] == [1]
