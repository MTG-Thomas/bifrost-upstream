"""Claim-bound completion requests for trusted Cloudflare summary Workers."""

import json
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.agent_runs import AgentRun
from src.models.orm.agents import Agent
from src.models.orm.ai_models import (
    AIModelAssignment,
    AIModelProfile,
    AIProviderConnection,
)
from src.services.execution.run_summarizer import (
    _broadcast_run,
    _extract_json_object,
    summary_messages,
    write_summary,
)
from src.services.llm.base import LLMResponse

REQUEST_BYTES = 256 * 1024
RESPONSE_BYTES = 64 * 1024
TIMEOUT_SECONDS = 30
UNCERTAIN_ERROR = "The summary provider may already have received the request. Regenerate to try again."


def summary_endpoint(value: str | None) -> str | None:
    if not value or not value.isascii() or any(c.isspace() or c == "\\" for c in value):
        return None
    try:
        url = urlsplit(value)
    except ValueError:
        return None
    if url.scheme != "https" or not url.hostname or url.username is not None or url.query or url.fragment:
        return None
    return value.rstrip("/")


async def prepare_completion(db: AsyncSession, run: AgentRun, endpoint: str) -> dict[str, Any]:
    # Read profile metadata only. The generic resolver can decrypt credentials
    # and probe an unclassified endpoint, including failover.
    profile = (await db.execute(select(
        AIModelProfile.model, AIModelProfile.default_max_tokens, AIModelProfile.openai_transport,
        AIModelProfile.failover_profile_id, AIModelProfile.reasoning_effort,
        AIProviderConnection.provider, AIProviderConnection.endpoint, AIProviderConnection.catalog_provider_id,
    ).join(AIModelAssignment, AIModelAssignment.profile_id == AIModelProfile.id)
      .join(AIProviderConnection, AIProviderConnection.id == AIModelProfile.connection_id)
      .where(AIModelAssignment.assignment_key == "summarization"))).one_or_none()
    if (
        profile is None or profile.provider != "openai_compatible" or profile.catalog_provider_id is not None
        or profile.failover_profile_id is not None or profile.reasoning_effort is not None
        or profile.openai_transport != "chat_completions" or summary_endpoint(profile.endpoint) != endpoint
    ):
        raise ValueError("Summary profile is outside the Cloudflare chat-completion spike")
    agent = (await db.execute(select(Agent.name, Agent.system_prompt).where(
        Agent.id == run.agent_id,
    ))).one_or_none()
    messages = summary_messages(
        (agent.name or "") if agent else "",
        ((agent.system_prompt or "")[:2000]) if agent else "",
        run.input, run.output,
    )
    body: dict[str, Any] = {
        "model": profile.model,
        "messages": [{"role": msg.role, "content": msg.content} for msg in messages],
        "stream": False,
        "store": False,
    }
    if (max_tokens := profile.default_max_tokens) is not None:
        body["max_completion_tokens"] = max_tokens
    if len(json.dumps(body).encode()) > REQUEST_BYTES:
        raise ValueError("Cloudflare summary exceeds the request size limit")
    return {"endpoint": endpoint, "body": body}


async def settle_completion(db: AsyncSession, run: AgentRun, result: dict[str, Any], state: str) -> None:
    if state == "uncertain":
        error = UNCERTAIN_ERROR
    elif result.get("success") is not True:
        error = "The Cloudflare summary request failed" if state == "completed" else "The summary request did not start"
    else:
        error = await _persist_completion(db, run, result)
    if error is not None:
        run.summary_status = "failed"
        run.summary_error = error
    await db.commit()
    await _broadcast_run(run, db)


async def _persist_completion(db: AsyncSession, run: AgentRun, result: dict[str, Any]) -> str | None:
    content, model = result.get("content"), result.get("model")
    if not isinstance(content, str) or not isinstance(model, str) or not model or len(model) > 100:
        return "Invalid Cloudflare summary result"
    if len(content.encode()) > RESPONSE_BYTES:
        return "Cloudflare summary exceeds the response size limit"
    if not content.strip():
        return "Summarization model returned empty content"
    try:
        parsed = json.loads(_extract_json_object(content))
    except (ValueError, RecursionError):
        return "Invalid JSON from summarization model"
    if not isinstance(parsed, dict):
        return "Summarization model did not return a JSON object"
    usage = [result.get(name) for name in ("input_tokens", "output_tokens", "cache_read_tokens", "reasoning_tokens")]
    if any(type(tokens) is not int or not 0 <= tokens <= 2**31 - 1 for tokens in usage):
        return "Invalid Cloudflare summary token usage"
    response = LLMResponse(
        input_tokens=usage[0], output_tokens=usage[1],
        cache_read_tokens=usage[2], reasoning_tokens=usage[3],
    )
    await write_summary(db, run, parsed, response, provider="openai", model=model, organization_id=run.org_id)
    return None
