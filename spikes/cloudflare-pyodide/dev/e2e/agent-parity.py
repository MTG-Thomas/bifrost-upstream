"""Run the Linux writer against a transport-local stub inside the lab API container."""

import asyncio
import json
import logging
import sys
from unittest.mock import patch
from uuid import UUID

import httpx2
from sqlalchemy import select

from shared.run_lineage import person_lineage
from src.core.database import get_db_context
from src.jobs.consumers.agent_run import AgentRunConsumer
from src.models.orm.agent_runs import AgentRun, AgentRunStep
from src.models.orm.ai_usage import AIUsage
from src.models.orm.executions import Execution
from src.models.orm.work_dispatches import WorkDispatch
from src.services.execution.agent_run_service import stage_agent_run_request


def completion(request):
    assert str(request.url) == "https://phase5-fixture.invalid/v1/chat/completions"
    body = json.loads(request.content)
    scenario = json.loads(next(item["content"] for item in body["messages"] if item["role"] == "user"))["scenario"]
    assert scenario in {"text", "tool"}
    is_tool_turn = scenario == "tool" and not any(item["role"] == "tool" for item in body["messages"])
    if is_tool_turn:
        name = body["tools"][0]["function"]["name"]
        message = {"role": "assistant", "content": None, "tool_calls": [{
            "id": "fixture-call", "type": "function", "function": {"name": name, "arguments": "{}"},
        }]}
    else:
        message = {"role": "assistant", "content": "fixture " + scenario}
    return httpx2.Response(200, json={
        "id": "completion-fixture", "object": "chat.completion", "created": 1,
        "model": "gpt-4o-mini", "choices": [{
            "index": 0, "message": message, "finish_reason": "tool_calls" if is_tool_turn else "stop",
        }],
        "usage": {"prompt_tokens": 9, "completion_tokens": 3, "total_tokens": 12},
    })


async def signature(run_id):
    async with get_db_context() as db:
        run = await db.get(AgentRun, run_id)
        rows = (await db.scalars(
            select(AgentRunStep).where(AgentRunStep.run_id == run_id).order_by(AgentRunStep.step_number),
        )).all()
        steps = []
        for row in rows:
            content = dict(row.content or {})
            for field in ("context_breakdown", "messages_count", "tools_count"):
                content.pop(field, None)
            if execution_id := content.pop("execution_id", None):
                execution = await db.get(Execution, UUID(execution_id))
                assert execution is not None
                content["execution"] = {
                    "workflow_id": str(execution.workflow_id), "status": execution.status.value,
                    "run_user": str(execution.run_user_id), "actor": str(execution.executed_by),
                }
            steps.append({"type": row.type, "content": content, "tokens_used": row.tokens_used})
        usage = (await db.scalars(
            select(AIUsage).where(AIUsage.agent_run_id == run_id).order_by(AIUsage.timestamp),
        )).all()
        return {
            "status": run.status, "output": run.output, "model": run.llm_model,
            "iterations": run.iterations_used, "tokens": run.tokens_used, "steps": steps,
            "usage": [[row.provider, row.model, row.input_tokens, row.output_tokens,
                       row.cache_read_tokens, row.cache_write_tokens, row.reasoning_tokens] for row in usage],
        }


async def main():
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(completion)) as http:
        with patch("src.services.agent_runtime.model_factory.get_ai_retry_http_client", return_value=http):
            for cloudflare_id in map(UUID, sys.argv[1:]):
                async with get_db_context() as db:
                    origin = await db.get(AgentRun, cloudflare_id)
                    assert origin is not None and origin.run_user_id is not None
                    run = await stage_agent_run_request(
                        db, str(origin.agent_id), "api", origin.input,
                        org_id=str(origin.org_id) if origin.org_id else None,
                        caller_user_id=origin.caller_user_id, caller_email=origin.caller_email,
                        caller_name=origin.caller_name, lineage=person_lineage(origin.run_user_id),
                    )
                    await db.flush()
                    body = await db.scalar(select(WorkDispatch.message).where(WorkDispatch.subject_id == UUID(run.run_id)))
                assert body is not None
                await AgentRunConsumer().process_message(body)
                linux = await signature(UUID(run.run_id))
                workers = await signature(cloudflare_id)
                assert linux == workers, {"linux": linux, "workers": workers}
                print(json.dumps({"label": "autonomous-linux-writer-parity", "linux": run.run_id,
                                  "workers": str(cloudflare_id), "steps": len(linux["steps"]),
                                  "usage": len(linux["usage"])}), flush=True)


logging.disable(logging.INFO)
asyncio.run(main())
