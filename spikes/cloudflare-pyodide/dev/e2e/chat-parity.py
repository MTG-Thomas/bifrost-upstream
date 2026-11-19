"""Run a queued chat through the Linux writer with a transport-local provider."""

import asyncio
import json
import logging
import sys
from unittest.mock import patch
from uuid import UUID

import httpx2
from sqlalchemy import select

from src.core.database import get_db_context
from src.jobs.consumers.agent_run import AgentRunConsumer
from src.models.orm.work_dispatches import WorkDispatch


def completion(request):
    assert str(request.url) == "https://phase5-fixture.invalid/v1/chat/completions"
    body = json.loads(request.content)
    assert body["stream"]
    assert not body.get("tools")
    scenario = json.loads(body["messages"][-1]["content"])["scenario"]
    assert scenario in {"chat-prior", "chat-text"}
    chunks = [
        {"choices": [{"index": 0, "delta": {"role": "assistant", "content": "fixture "}}]},
        {"choices": [{"index": 0, "delta": {"content": scenario}}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
        {"choices": [], "usage": {"prompt_tokens": 9, "completion_tokens": 3, "total_tokens": 12}},
    ]
    base = {"id": "fixture", "object": "chat.completion.chunk", "created": 1, "model": "gpt-4o-mini"}
    stream = "".join("data: " + json.dumps({**base, **chunk}) + "\n\n" for chunk in chunks)
    return httpx2.Response(200, headers={"content-type": "text/event-stream"}, content=stream + "data: [DONE]\n\n")


async def main():
    run_id = UUID(sys.argv[1])
    async with get_db_context() as db:
        body = await db.scalar(select(WorkDispatch.message).where(WorkDispatch.subject_id == run_id))
    assert body is not None
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(completion)) as http:
        with patch("src.services.agent_runtime.model_factory.get_ai_retry_http_client", return_value=http):
            await AgentRunConsumer().process_message(body)


logging.disable(logging.INFO)
asyncio.run(main())
