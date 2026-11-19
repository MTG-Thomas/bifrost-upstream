"""Claim-bound completion requests for trusted Cloudflare summary Workers."""

import json
from typing import Any
from urllib.parse import urlsplit

from cryptography.fernet import InvalidToken
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.agent_runs import AgentRun
from src.models.orm.agents import Agent
from src.models.orm.ai_models import AIModelAssignment, AIModelProfile, AIProviderConnection
from src.services.ai_model_service import AIModelService
from src.services.execution.run_summarizer import (
    _broadcast_run,
    _extract_json_object,
    summary_messages,
    write_summary,
)
from src.services.llm.base import LLMConfig, LLMResponse, is_deepseek_family, request_max_tokens

REQUEST_BYTES = 256 * 1024
RESPONSE_BYTES = 64 * 1024
TIMEOUT_SECONDS = 30
UNCERTAIN_ERROR = "The summary provider may already have received the request. Regenerate to try again."


def summary_endpoint(value: str | None) -> str | None:
    if not value or not value.isascii() or any(c.isspace() or c in "\\?#" for c in value):
        return None
    try:
        url = urlsplit(value)
    except ValueError:
        return None
    if url.scheme != "https" or not url.hostname or url.username is not None or url.query or url.fragment:
        return None
    return value.rstrip("/")


def _profile_query(assignment_key: str, profile_id=None):
    query = (
        select(
            AIModelProfile.id, AIModelProfile.model, AIModelProfile.default_max_tokens,
            AIModelProfile.openai_transport, AIModelProfile.failover_profile_id,
            AIModelProfile.reasoning_effort, AIModelProfile.updated_at,
            AIProviderConnection.id.label("connection_id"), AIProviderConnection.provider,
            AIProviderConnection.endpoint, AIProviderConnection.catalog_provider_id,
            AIProviderConnection.updated_at.label("connection_updated_at"),
        )
        .select_from(AIModelProfile)
        .join(AIProviderConnection, AIProviderConnection.id == AIModelProfile.connection_id)
    )
    if profile_id is not None:
        return query.where(AIModelProfile.id == profile_id)
    return query.join(AIModelAssignment, AIModelAssignment.profile_id == AIModelProfile.id).where(
        AIModelAssignment.assignment_key == assignment_key,
    )


async def _eligible_profile(db: AsyncSession, endpoint: str, *, assignment_key: str = "summarization", profile_id=None):
    from src.services.agent_runtime.model_factory import provider_name_for_config

    profile = (await db.execute(_profile_query(assignment_key, profile_id))).one_or_none()
    if (
        profile is None or profile.failover_profile_id is not None
        or profile.openai_transport != "chat_completions"
        or profile.provider not in {"openai", "openai_compatible"}
        # Catalog-linked models can select a different endpoint or wire API.
        or profile.catalog_provider_id is not None or profile.reasoning_effort is not None
        or summary_endpoint(profile.endpoint) != endpoint
    ):
        raise ValueError("Summary profile is outside the Cloudflare chat-completion spike")
    config = LLMConfig(
        provider="openai", model=profile.model, api_key="", endpoint=profile.endpoint,
        openai_transport="chat_completions", default_max_tokens=profile.default_max_tokens,
    )
    provider = provider_name_for_config(config)
    if provider != "openai":
        raise ValueError("Summary provider is outside the Cloudflare chat-completion spike")
    return profile, config, provider


async def summary_provider(db: AsyncSession, endpoint: str) -> str:
    _, _, provider = await _eligible_profile(db, endpoint)
    return provider


async def prepare_completion(db: AsyncSession, run: AgentRun, endpoint: str) -> tuple[dict[str, Any], str]:
    """Read the primary configuration without row locks or transport discovery."""
    profile, config, provider = await _eligible_profile(db, endpoint)
    key = await resolve_provider_key(db, profile, assignment_key="summarization")
    agent = (await db.execute(select(Agent.name, Agent.system_prompt).where(
        Agent.id == run.agent_id,
    ))).one_or_none()
    messages = summary_messages(
        (agent.name or "") if agent else "",
        ((agent.system_prompt or "")[:2000]) if agent else "",
        run.input, run.output,
    )
    body: dict[str, Any] = {
        "model": config.model,
        "messages": [{"role": msg.role, "content": msg.content} for msg in messages],
        "stream": False,
        "store": False,
    }
    if (max_tokens := request_max_tokens(config, None)) is not None:
        token_field = "max_tokens" if is_deepseek_family(config) else "max_completion_tokens"
        body[token_field] = max_tokens
    if len(json.dumps(body).encode()) > REQUEST_BYTES:
        raise ValueError("Cloudflare summary exceeds the request size limit")
    return {"endpoint": endpoint, "body": body, "api_key": key}, provider


async def resolve_provider_key(db: AsyncSession, profile, *, assignment_key: str, profile_id=None) -> str:
    credential = (await db.execute(_profile_query(assignment_key, profile_id).add_columns(
        AIProviderConnection.encrypted_api_key,
    ).where(
        AIModelProfile.id == profile.id, AIModelProfile.updated_at == profile.updated_at,
        AIProviderConnection.id == profile.connection_id,
        AIProviderConnection.updated_at == profile.connection_updated_at,
    ))).one_or_none()
    if credential is None or tuple(credential[:-1]) != tuple(profile):
        raise ValueError("The summary connection changed during preparation")
    try:
        key = AIModelService(db).decrypt_api_key(credential.encrypted_api_key)
    except InvalidToken:
        raise ValueError("The summary connection could not be resolved") from None
    if not key:
        raise ValueError("The summary connection could not be resolved")
    return key


async def settle_completion(db: AsyncSession, run: AgentRun, result: dict[str, Any], state: str, provider: str | None) -> None:
    if state == "uncertain":
        error = UNCERTAIN_ERROR
    elif result.get("success") is not True:
        error = "The Cloudflare summary request failed"
    else:
        error = await _persist_completion(db, run, result, provider)
    if error is not None:
        run.summary_status = "failed"
        run.summary_error = error
    await db.commit()
    await _broadcast_run(run, db)


async def _persist_completion(db: AsyncSession, run: AgentRun, result: dict[str, Any], provider: str | None) -> str | None:
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
    usage = [
        result.get("input_tokens"), result.get("output_tokens"),
        result.get("cache_read_tokens", 0), result.get("reasoning_tokens", 0),
    ]
    if any(type(tokens) is not int or not 0 <= tokens <= 2**31 - 1 for tokens in usage):
        return "Invalid Cloudflare summary token usage"
    response = LLMResponse(
        input_tokens=usage[0], output_tokens=usage[1],
        cache_read_tokens=usage[2], reasoning_tokens=usage[3],
    )
    if not isinstance(provider, str) or not 0 < len(provider) <= 50:
        return "Invalid Cloudflare summary provider attribution"
    await write_summary(db, run, parsed, response, provider=provider, model=model, organization_id=run.org_id)
    return None
