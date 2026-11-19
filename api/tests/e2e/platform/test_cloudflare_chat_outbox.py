"""Exercise PostgreSQL and Redis at the outbox transaction boundaries."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from src.core.cache.keys import chat_run_events_stream_key, chat_run_events_version_key
from src.core.cache.redis_client import get_redis
from src.core.pubsub import publish_chat_run_event, replay_chat_run_events
from src.models.contracts.agents import ChatStreamChunk
from src.models.orm.agent_runs import AgentRun, CloudflareAgentAttempt, CloudflareChatEvent
from src.models.orm.agents import Conversation, Message
from src.models.orm.users import User
from src.services.execution.agent_run_service import announce_ended_agent_run, request_agent_run_cancel
from src.services.execution.cloudflare_chat import CHAT_UNCERTAIN
from src.services.execution.cloudflare_chat_outbox import (
    discard_receipts,
    fail_settled_runs_with_projection_loss,
    prune_settled_events,
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
    queued = await publish_chat_run_event(conversation.id, run.id, "run_status", "queued",
                                          ChatStreamChunk(type="run_status", run_status="queued"))
    attempt = CloudflareAgentAttempt(
        claim_id=uuid4(), run_id=run.id, data={"events": [], "chat": {"queued_sequence": queued["sequence"]}},
    )
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
    journal.data = {**journal.data, "events": [*journal.data["events"], content]}
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
        assert journal.data["events"] == []
        assert not await publish_one(db)
        await _stage_delta(db, attempt, 1, "one")
        await _stage_delta(db, attempt, 2, "two")
        await db.commit()

    # A new publisher observes the durable rows after the producer stopped.
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
    assert [event["sequence"] for event in events] == [1, 2]


@pytest.mark.asyncio
async def test_new_claim_can_publish_after_normal_expiry(attempt, db_session):
    conversation_id, _, _ = attempt
    await _stage_delta(db_session, attempt, 1, "old")
    await db_session.commit()
    for _ in range(2):
        assert await publish_one(db_session)
        await db_session.commit()
    await discard_receipts(db_session)
    await db_session.commit()
    async with get_redis() as redis:
        for key in (chat_run_events_stream_key(str(conversation_id)), chat_run_events_version_key(str(conversation_id))):
            assert await redis.expire(key, 0)
    run = AgentRun(trigger_type="chat", conversation_id=conversation_id)
    db_session.add(run)
    await db_session.flush()
    run_id, claim_id = run.id, uuid4()
    try:
        queued = await publish_chat_run_event(conversation_id, run_id, "run_status", "queued",
                                              ChatStreamChunk(type="run_status", run_status="queued"))
        assert queued["sequence"] == 1
        db_session.add(CloudflareAgentAttempt(
            run_id=run_id, claim_id=claim_id, data={"events": [], "chat": {"queued_sequence": 1}},
        ))
        await db_session.flush()
        await _stage_delta(db_session, (conversation_id, run_id, claim_id), 1, "new")
        await db_session.commit()
        for _ in range(2):
            assert await publish_one(db_session)
            await db_session.commit()
        await discard_receipts(db_session)
        await db_session.commit()
        events = await replay_chat_run_events(conversation_id)
        assert [(event["sequence"], event["payload"].get("content")) for event in events] == [
            (1, None), (2, "new"),
        ]
    finally:
        await db_session.rollback()
        await db_session.execute(delete(AgentRun).where(AgentRun.id == run_id))
        await db_session.commit()


@pytest.mark.asyncio
async def test_publishers_exclude_each_other_until_receipt_commits(attempt, async_session_factory):
    conversation_id, _, _ = attempt
    async with async_session_factory() as first, async_session_factory() as second:
        await _stage_delta(first, attempt, 1, "one")
        await first.commit()
        assert await publish_one(first)
        await first.commit()
        assert await publish_one(first)
        # Redis has the event, but the first transaction still holds the
        # conversation lock and its publish receipt is not committed.
        assert not await publish_one(second)
        await second.commit()
        await first.commit()
        await discard_receipts(second)
        await second.commit()
        assert not await publish_one(second)
    assert [event["sequence"] for event in await replay_chat_run_events(conversation_id)] == [1, 2]


@pytest.mark.asyncio
async def test_settled_projection_loss_fails_run_and_discards_late_rows(attempt, db_session):
    conversation_id, run_id, claim_id = attempt
    await _stage_delta(db_session, attempt, 1, "one")
    await db_session.commit()
    assert await publish_one(db_session)
    await db_session.commit()
    row = await db_session.scalar(select(CloudflareChatEvent).where(CloudflareChatEvent.claim_id == claim_id))
    async with get_redis() as redis:
        assert await redis.delete(receipt_key(row))
    assert await publish_one(db_session)
    await db_session.commit()
    await _stage_delta(db_session, attempt, 2, "staged during loss detection")
    run = await db_session.get(AgentRun, run_id)
    run.status = "completed"
    run.output = {"text": "one"}
    journal = await db_session.get(CloudflareAgentAttempt, claim_id)
    journal.data = {**journal.data, "settled": True}
    await db_session.commit()
    assert await publish_one(db_session)
    await db_session.commit()
    await discard_receipts(db_session)
    rows = (await db_session.scalars(select(CloudflareChatEvent).where(
        CloudflareChatEvent.claim_id == claim_id,
    ))).all()
    assert len(rows) == 2 and all(row.projection_lost for row in rows)
    for row in rows:
        row.created_at = datetime.now(UTC) - timedelta(days=3)
    await db_session.flush()
    await prune_settled_events(db_session)
    await db_session.commit()
    assert await db_session.scalar(select(CloudflareChatEvent.id).where(
        CloudflareChatEvent.claim_id == claim_id,
    ).limit(1))
    await fail_settled_runs_with_projection_loss()
    await db_session.refresh(run)
    await db_session.refresh(journal)
    assert run.status == "failed"
    assert run.output == {"text": "one", "partial": True}
    assert journal.data["attempt_state"] == "uncertain"
    assert [event["sequence"] for event in await replay_chat_run_events(conversation_id)] == [1]
    await prune_settled_events(db_session)
    await db_session.commit()
    assert not await db_session.scalar(select(CloudflareChatEvent.id).where(CloudflareChatEvent.claim_id == claim_id))


@pytest.mark.asyncio
async def test_executor_loss_keeps_partial_and_repeated_cancel_uses_outbox(attempt, db_session):
    conversation_id, run_id, claim_id = attempt
    run = await db_session.get(AgentRun, run_id)
    run.status = "running"
    journal = await db_session.get(CloudflareAgentAttempt, claim_id)
    assistant_id = uuid4()
    journal.data = {
        **journal.data, "text": "partial", "requests": {"1": "grant"},
        "chat": {**journal.data["chat"], "assistant_id": str(assistant_id), "sequence": 1},
    }
    await _stage_delta(db_session, attempt, 1, "partial")
    await db_session.commit()
    assert (await request_agent_run_cancel(db_session, run_id)).chat_outbox
    assert (await request_agent_run_cancel(db_session, run_id)).chat_outbox
    assert await announce_ended_agent_run(run_id, "worker stopped")
    await db_session.refresh(run)
    assert run.status == "cancelled" and run.error == CHAT_UNCERTAIN
    assert (await db_session.get(Message, assistant_id)).content == "partial"
    assert [event["kind"] for event in await replay_chat_run_events(conversation_id)] == ["run_status"]
    for _ in range(6):
        assert await publish_one(db_session)
        await db_session.commit()
    assert [event["kind"] for event in await replay_chat_run_events(conversation_id)] == [
        "run_status", "delta", "run_status", "cancelled",
    ]
