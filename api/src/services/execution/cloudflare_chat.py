"""Eligibility and Linux-compatible transcript projection for text-only Workers chat."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from shared.chat_context import store_chat_context_usage
from src.core.cache import get_shared_redis
from src.core.database import get_db_context
from src.models.contracts.agents import ChatContextUsage, ChatStreamChunk
from src.models.enums import MessageRole
from src.models.orm.agent_runs import AgentRun, CloudflareAgentAttempt
from src.models.orm.agents import Agent, Conversation, Message, MessageAttachment
from src.models.orm.ai_models import AIModelProfile
from src.models.orm.work_dispatches import WorkDispatch
from src.services.ai_model_service import AIModelService
from src.services.ai_usage_service import record_ai_usage
from src.services.agent_router import MENTION_PATTERN
from src.services.conversation_messages import add_conversation_message
from src.services.execution.agent_helpers import build_agent_system_prompt
from src.services.execution.cloudflare_chat_outbox import has_projection_loss, stage_chat_chunk
from src.services.execution.cloudflare_summary import _eligible_profile
from src.services.model_capabilities import should_offer_tool_calling
from src.services.model_catalog import get_model_catalog
from src.services.model_pricing import config_context_window

CHAT_UNCERTAIN = "The model may already have run. Check the saved conversation before sending again."


async def find_chat_attempt(db: AsyncSession, run: AgentRun) -> CloudflareAgentAttempt | None:
    if run.trigger_type != "chat" or run.conversation_id is None:
        return None
    return await db.scalar(select(CloudflareAgentAttempt).where(
        CloudflareAgentAttempt.run_id == run.id,
        CloudflareAgentAttempt.data.has_key("chat"),  # noqa: W601 - PostgreSQL JSONB membership
    ))


async def stage_interrupted_chat(db: AsyncSession, run: AgentRun) -> bool:
    """Preserve committed text and order the terminal after all pending deltas."""
    attempt = await find_chat_attempt(db, run)
    if attempt is None:
        return False
    data = dict(attempt.data)
    assert run.completed_at is not None
    started = run.started_at or run.completed_at
    run.duration_ms = int((run.completed_at - started).total_seconds() * 1000)
    run.llm_model = data.get("model")
    if data.get("requests"):
        run.error = CHAT_UNCERTAIN
    await stage_chat_terminal(
        db, run, attempt.claim_id, data,
        lost=await has_projection_loss(db, attempt.claim_id), uncertain=bool(data.get("requests")),
    )
    attempt.data = {**data, "settled": True, "attempt_state": "uncertain" if data.get("requests") else "not_started"}
    return True


@dataclass(frozen=True)
class ChatInput:
    conversation: Conversation
    user_message: Message
    profile_id: UUID
    messages: list[dict[str, Any]]
    context_window: int | None


async def mark_chat_queued(run_id: UUID, sequence: int) -> None:
    """Workers may claim only after the canonical queued event was published."""
    async with get_db_context() as db:
        await db.execute(update(WorkDispatch).where(
            WorkDispatch.subject_id == run_id, WorkDispatch.family == "agent_run",
        ).values(payload=WorkDispatch.payload.op("||")({"cloudflare_chat_queued_sequence": sequence})))


async def load_chat_input(db: AsyncSession, run: AgentRun, agent: Agent, endpoint: str) -> ChatInput:
    """Refuse turns whose Linux path can switch agents, offer tools or read files."""
    if run.conversation_id is None:
        raise ValueError("Cloudflare chat requires a conversation")
    conversation = await db.get(Conversation, run.conversation_id)
    inputs = run.input or {}
    if (
        conversation is None or conversation.agent_id != agent.id or not conversation.title
        or inputs.get("attachment_ids") or not inputs.get("user_message_id")
        or MENTION_PATTERN.search(str(inputs.get("content") or ""))
    ):
        raise ValueError("Cloudflare chat requires a titled, fixed-agent, attachment-free turn")
    profile_id = UUID(inputs["model_profile_id"]) if inputs.get("model_profile_id") else None
    profile, config, _ = await _eligible_profile(db, endpoint, assignment_key="chat_default", profile_id=profile_id)
    stored = await db.get(AIModelProfile, profile.id)
    if stored is None or not stored.enabled_for_chat or should_offer_tool_calling(
        await AIModelService(db).profile_capabilities(stored),
    ):
        raise ValueError("Cloudflare chat does not offer built-in artifact tools")
    if await db.scalar(select(MessageAttachment.artifact_id).where(
        MessageAttachment.conversation_id == conversation.id, MessageAttachment.message_id.is_not(None),
    ).limit(1)) is not None:
        raise ValueError("Cloudflare chat cannot load attachments from conversation history")
    history = (await db.scalars(select(Message).where(
        Message.conversation_id == conversation.id,
    ).order_by(Message.sequence).limit(33))).all()
    if (
        not history or len(history) > 32 or str(history[-1].id) != inputs["user_message_id"]
        or history[-1].role != MessageRole.USER
        or any(
            message.role not in (MessageRole.USER, MessageRole.ASSISTANT) or message.tool_calls
            for message in history
        )
    ):
        raise ValueError("Cloudflare chat requires bounded text-only conversation history")
    messages = [
        {"role": "system", "content": build_agent_system_prompt(agent, execution_context={"mode": "chat"})},
        *[{"role": message.role.value, "content": message.content or ""} for message in history],
    ]
    window = config_context_window(await get_model_catalog(db), config)
    if len(json.dumps(messages).encode()) > min(8192, (window or 16384) // 4):
        raise ValueError("Cloudflare chat history exceeds the text-only fixture limit")
    return ChatInput(conversation, history[-1], profile.id, messages, window)


def stage_chunk(
    db: AsyncSession, run: AgentRun, claim_id: UUID, data: dict, status: str, chunk: ChatStreamChunk,
) -> None:
    conversation_id = run.conversation_id
    assert conversation_id is not None
    chat = dict(data["chat"])
    chat["sequence"] += 1
    data["chat"] = chat
    stage_chat_chunk(db, conversation_id, run.id, claim_id, chat["sequence"], status, chunk)


def stage_chat_start(db: AsyncSession, run: AgentRun, claim_id: UUID, data: dict) -> None:
    chat = {**data["chat"], "assistant_id": str(uuid4())}
    data["chat"] = chat
    stage_chunk(db, run, claim_id, data, "running", ChatStreamChunk(
        type="run_status", conversation_id=str(run.conversation_id), run_status="running",
    ))
    stage_chunk(db, run, claim_id, data, "running", ChatStreamChunk(
        type="message_start", user_message_id=chat["user_message_id"],
        assistant_message_id=chat["assistant_id"], local_id=chat["user_message_id"],
    ))


async def stage_chat_usage(
    db: AsyncSession, run: AgentRun, claim_id: UUID, data: dict, usage: dict[str, int],
) -> None:
    conversation_id = run.conversation_id
    assert conversation_id is not None
    chat = data["chat"]
    measurement = ChatContextUsage(
        input_tokens=usage["input_tokens"], cache_read_tokens=usage["cache_read_tokens"],
        model_profile_id=UUID(data["profile_id"]), context_window=chat["context_window"],
    )
    await store_chat_context_usage(db, conversation_id, measurement)
    # Linux uses the generic failed envelope status for this chunk type.
    stage_chunk(db, run, claim_id, data, "failed", ChatStreamChunk(
        type="context_usage", context_usage=measurement,
    ))
    data["chat"] = {**data["chat"], "usage": usage}


async def stage_chat_terminal(
    db: AsyncSession, run: AgentRun, claim_id: UUID, data: dict, *, lost: bool, uncertain: bool,
) -> None:
    conversation_id = run.conversation_id
    assert conversation_id is not None and run.duration_ms is not None
    chat = data["chat"]
    content = data.get("text") or None
    assistant_id = chat.get("assistant_id")
    usage = chat.get("usage")
    success = run.status == "completed"
    if assistant_id is not None and (success or content):
        await add_conversation_message(db, Message(
            id=UUID(assistant_id), conversation_id=conversation_id, role=MessageRole.ASSISTANT,
            content=content, token_count_input=usage["input_tokens"] if usage else None,
            token_count_output=usage["output_tokens"] if usage else None,
            model=data["model"] if usage else None, duration_ms=run.duration_ms if usage else None,
        ))
        if usage:
            await db.flush()
            await record_ai_usage(
                session=db, redis_client=await get_shared_redis(), provider=data["provider"], model=data["model"],
                conversation_id=conversation_id, message_id=UUID(assistant_id), user_id=UUID(chat["user_id"]),
                organization_id=run.agent.organization_id if run.agent else None, duration_ms=run.duration_ms, **usage,
            )
    if success:
        reason = data["finish_reason"]
        run.output = {"text": content, "finish_reason": reason, "incomplete": reason != "stop"}
        assert usage is not None
        chunk = ChatStreamChunk(
            type="done", content=content, message_id=assistant_id, run_status="completed",
            token_count_input=usage["input_tokens"], token_count_output=usage["output_tokens"],
            duration_ms=run.duration_ms, finish_reason=reason, incomplete=reason != "stop",
        )
    else:
        cancelled = run.status == "cancelled"
        run.output = {"text": content, "partial": True} if cancelled or uncertain else None
        chunk = ChatStreamChunk(
            type="cancelled" if cancelled else "error", content=content,
            message_id=assistant_id, duration_ms=run.duration_ms, error=run.error, run_status=run.status,
        )
    if not lost:
        stage_chunk(db, run, claim_id, data, run.status, chunk)
