"""Fenced API operations for the autonomous Workers agent runtime."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.run_lineage import principal_lineage
from src.core.database import get_db_context
from src.models.orm.agent_runs import AgentRun, AgentRunStep, CloudflareAgentAttempt
from src.models.orm.execution_workers import ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.models.orm.work_dispatches import WorkDispatch
from src.services.authorization.callers import CallerUnavailable
from src.services.authorization.enforce import load_caller
from src.services.authorization.runtime_auth import load_runtime_principal
from src.services.durable_impersonation import principal_durable_impersonation
from src.services.execution.agent_workflow_tools import authorize_bound_workflow_tool
from src.services.execution.async_executor import _pending_run
from src.services.execution.workflow_dispatch import (
    hand_off_workflow_run,
    stage_workflow_run,
)
from src.services.tool_schema import (
    parameters_json_schema,
    validate_arguments_against_schema,
)
from src.services.worker_pool.claims import PoolClaim
from src.worker.remote.fencing import current_remote_lease_ref, fence_remote_owner

ATTEMPT_SECONDS = 60
PROVIDER_REQUEST_BYTES = 256 * 1024
UNCERTAIN = "The model or workflow tool may already have run. Check the recorded agent steps before running again."


async def prepare_run(claim: PoolClaim, deadline: datetime, endpoint: str) -> bool:
    """Prepare an immutable, secret-free snapshot under the current claim."""
    from src.services.ai_behavior_service import AIBehaviorService
    from src.services.authorization.agent_runtime import authorize_agent_execution
    from src.services.execution.agent_helpers import (
        build_agent_system_prompt,
        resolve_agent_tools,
    )
    from src.services.execution.cloudflare_summary import _eligible_profile
    from src.services.llm.base import request_max_tokens

    claim_id = UUID(claim.message["claim_id"])
    async with get_db_context() as db:
        run = await lock_attempt_run(db, claim.subject_id, claim_id)
        dispatch = await db.get(WorkDispatch, claim.dispatch_id)
        if run.status != "queued" or run.agent_id is None or dispatch is None:
            return False
        if (
            run.trigger_type == "chat" or run.conversation_id is not None or run.output_schema is not None
            or run.parent_run_id is not None or run.event_delivery_id is not None
            or dispatch.payload.get("trigger_type") == "chat"
            or dispatch.payload.get("attachments")
            or await AIBehaviorService(db).get_agent_execution_mode() != "delegated"
        ):
            return False
        run.status = "running"
        run.started_at = datetime.now(UTC)
        await db.flush()
        principal = await load_runtime_principal(db, {"credential_kind": "agent_run", "sub": str(run.id)})
        if principal is None:
            raise ValueError("Agent has no current runtime authority")
        caller, agent = await authorize_agent_execution(db, await load_caller(db, principal), run.agent_id)
        if agent.system_tools or agent.knowledge_sources or agent.delegated_agents or agent.mcp_connections:
            raise ValueError("Unsupported autonomous agent capability")
        definitions, tool_ids = await resolve_agent_tools(agent, db, caller_user_id=caller.principal.user_id)
        if len(agent.tools) != len(definitions) or len(definitions) > 1:
            raise ValueError("Unsupported workflow tool set")
        tool = None
        if definitions:
            definition = definitions[0]
            tool = {
                "name": definition.name, "description": definition.description, "parameters": definition.parameters,
                "workflow_id": str(tool_ids[definition.name]),
            }
        profile, config, provider = await _eligible_profile(
            db, endpoint, assignment_key="primary", profile_id=agent.llm_profile_id,
        )
        snapshot = {
            "dispatch_id": str(claim.dispatch_id),
            "deadline": deadline.isoformat(),
            "profile_id": str(agent.llm_profile_id) if agent.llm_profile_id else None,
            "profile_pin": list(map(str, profile)),
            "provider": provider,
            "model": config.model,
            "max_output": min(request_max_tokens(config, agent.llm_max_tokens) or 4096, 4096),
            "max_requests": min(agent.max_iterations or 2, 2),
            "max_tokens": agent.max_token_budget or 16_384,
            "tool": tool,
            "messages": [
                {"role": "system", "content": build_agent_system_prompt(agent, execution_context={"mode": "autonomous"})},
                {"role": "user", "content": json.dumps(run.input) if run.input else "Run your task."},
            ],
            "requests": {},
            "events": [],
            "text": "",
            "step": 0,
        }
        if len(json.dumps(snapshot).encode()) > 128 * 1024:
            raise ValueError("Cloudflare autonomous run exceeds snapshot limit")
        run.budget_max_iterations = agent.max_iterations
        run.budget_max_tokens = agent.max_token_budget
        db.add(CloudflareAgentAttempt(claim_id=claim_id, run_id=run.id, data=snapshot))
        return True


def _require_attempt(attempt: CloudflareAgentAttempt | None, run_id: UUID) -> CloudflareAgentAttempt:
    if attempt is None or attempt.run_id != run_id:
        raise ConnectionError("Cloudflare agent attempt is missing")
    return attempt


def _step(db: AsyncSession, run: AgentRun, data: dict, kind: str, content: dict, tokens: int | None = None) -> None:
    data["step"] += 1
    db.add(AgentRunStep(
        run_id=run.id, step_number=data["step"], type=kind, content=content, tokens_used=tokens,
    ))


async def grant_turn(
    run_id: UUID, claim_id: UUID, request_id: str, turn: int, execution_id: str | None, endpoint: str,
) -> dict[str, Any]:
    """Resolve the primary key without the owner or run locks, then record a single-use grant."""
    from src.services.execution.cloudflare_summary import (
        _eligible_profile,
        resolve_provider_key,
    )

    async with get_db_context() as db:
        attempt = _require_attempt(await db.get(CloudflareAgentAttempt, claim_id), run_id)
        snapshot = attempt.data
        profile_id = UUID(snapshot["profile_id"]) if snapshot["profile_id"] else None
        profile, _config, _provider = await _eligible_profile(db, endpoint, assignment_key="primary", profile_id=profile_id)
        if list(map(str, profile)) != snapshot["profile_pin"]:
            return {"granted": False}
        key = await resolve_provider_key(db, profile, assignment_key="primary", profile_id=profile_id)
    async with get_db_context() as db:
        run = await lock_attempt_run(db, run_id, claim_id)
        attempt = _require_attempt(await db.get(CloudflareAgentAttempt, claim_id), run_id)
        data = dict(attempt.data)
        remaining = int((datetime.fromisoformat(data["deadline"]) - datetime.now(UTC)).total_seconds() * 1000)
        if (
            run.status != "running" or remaining <= 0 or turn not in (1, 2)
        ):
            return {"granted": False}
        if turn > data["max_requests"] or run.tokens_used >= data["max_tokens"]:
            return {"granted": False, "error_type": "AgentBudgetExceeded"}
        requests = dict(data["requests"])
        previous = requests.get(str(turn))
        if previous is not None and previous != request_id:
            raise ValueError("Conflicting Cloudflare provider grant")
        if turn == 1 and execution_id is not None:
            raise ValueError("Unexpected workflow receipt")
        messages = list(data["messages"])
        if turn == 2:
            if not data.get("tool_result") or execution_id != data["tool_receipt"]["execution_id"]:
                raise ValueError("Workflow tool has no canonical result")
            principal = await load_runtime_principal(db, {"credential_kind": "agent_run", "sub": str(run_id)})
            if run.agent_id is None or principal is None:
                raise CallerUnavailable("Cloudflare agent authority is unavailable")
            await authorize_bound_workflow_tool(
                db, run.agent_id, UUID(data["tool"]["workflow_id"]), await load_caller(db, principal),
            )
            call = data["model_tool"]
            messages += [
                {"role": "assistant", "content": data["tool_text"] or None, "tool_calls": [{
                    "id": call["id"], "type": "function",
                    "function": {"name": call["name"], "arguments": json.dumps(call["arguments"])},
                }]},
                {"role": "tool", "content": data["tool_result"], "tool_call_id": call["id"]},
            ]
        body: dict[str, Any] = {
            "model": data["model"], "messages": messages, "stream": True,
            "stream_options": {"include_usage": True}, "store": False,
            "max_completion_tokens": min(data["max_output"], data["max_tokens"] - run.tokens_used),
        }
        if data["tool"] is not None and turn == 1:
            body["tools"] = [{"type": "function", "function": {
                k: data["tool"][k] for k in ("name", "description", "parameters")
            }}]
            body["parallel_tool_calls"] = False
        body_bytes = json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        if len(body_bytes) > PROVIDER_REQUEST_BYTES:
            return {"granted": False, "error_type": "AgentProviderRequestTooLarge"}
        if previous is None:
            requests[str(turn)] = request_id
            data["requests"] = requests
            data["text"] = ""
            _step(db, run, data, "llm_request", {
                "messages_count": len(messages), "tools_count": len(body.get("tools", [])),
                "model": data["model"], "context_breakdown": None,
            })
            run.iterations_used += 1
            attempt.data = data
        return {"granted": True, "endpoint": endpoint, "api_key": key, "body": body, "remaining_ms": remaining}


async def lock_attempt_run(db: AsyncSession, run_id: UUID, claim_id: UUID) -> AgentRun:
    """Lock in owner, run order. Cancellation takes the same run lock."""
    owner = current_remote_lease_ref()
    if owner is None or not await fence_remote_owner(db):
        raise ConnectionError("Cloudflare agent owner is no longer current")
    run = await db.get(AgentRun, run_id, with_for_update={"of": AgentRun})
    current = await db.scalar(
        select(ExecutionWorkerClaim.id).where(
            ExecutionWorkerClaim.id == claim_id,
            ExecutionWorkerClaim.worker_id == owner.lease.worker_id,
            ExecutionWorkerClaim.family == "agent_run",
            ExecutionWorkerClaim.subject_id == run_id,
        )
    )
    if run is None or current is None:
        raise ConnectionError("Cloudflare agent claim is no longer current")
    return run


async def accept_workflow_tool(
    *, run_id: UUID, claim_id: UUID, call_id: str, name: str, arguments: dict[str, Any],
) -> UUID:
    """Return the canonical execution ID, including on exact replay."""
    encoded = json.dumps(arguments, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if not 0 < len(call_id) <= 255 or len(encoded) > 64 * 1024:
        raise ValueError("Invalid Cloudflare workflow-tool request")
    digest = sha256(encoded).hexdigest()
    async with get_db_context() as db:
        run = await lock_attempt_run(db, run_id, claim_id)
        attempt = await db.get(CloudflareAgentAttempt, claim_id)
        if attempt is None or attempt.run_id != run.id:
            raise ConnectionError("Cloudflare agent attempt is missing")
        tool = attempt.data.get("tool")
        if not isinstance(tool, dict) or tool["name"] != name or run.agent_id is None:
            raise ValueError("Unexpected Cloudflare agent tool")
        if (receipt := attempt.data.get("tool_receipt")) is not None:
            if (receipt["call_id"], receipt["arguments_sha256"]) != (call_id, digest):
                raise ValueError("Conflicting Cloudflare agent tool replay")
            return UUID(receipt["execution_id"])
        if run.status != "running" or datetime.now(UTC) >= datetime.fromisoformat(attempt.data["deadline"]):
            raise ValueError("Cloudflare agent cannot start a tool")
        principal = await load_runtime_principal(db, {"credential_kind": "agent_run", "sub": str(run_id)})
        if principal is None:
            raise ValueError("Cloudflare agent authority is unavailable")
        caller = await load_caller(db, principal)
        agent, workflow, caller, org_id = await authorize_bound_workflow_tool(
            db, run.agent_id, UUID(tool["workflow_id"]), caller,
        )
        schema = parameters_json_schema(workflow.parameters_schema, allow_unknown_when_empty=True)
        issues, schema_error = validate_arguments_against_schema(schema, arguments)
        if issues or schema_error:
            raise ValueError("Cloudflare workflow-tool arguments did not validate")

        execution_id = uuid4()
        principal = caller.principal
        lineage = await principal_lineage(db, principal)
        pending, message = _pending_run(
            execution_id=str(execution_id),
            workflow_id=str(workflow.id),
            parameters=arguments,
            org_id=str(org_id) if org_id else None,
            user_id=str(principal.user_id),
            user_name=principal.name,
            user_email=principal.email,
            form_id=None,
            startup=None,
            form_inputs={},
            embed={},
            api_key_id=None,
            sync=False,
            is_platform_admin=principal.is_superuser,
            file_path=None,
            artifact_workspace_id=str(run_id),
            is_agent=True,
            execution_origin="agent",
            agent_id=str(agent.id),
            lineage=lineage.bound(str(execution_id)) if lineage else None,
            impersonation_provenance=principal_durable_impersonation(principal).pending_value(),
        )
        accepted = await stage_workflow_run(db, pending, message, record_exists=False)
        data = dict(attempt.data)
        _step(db, run, data, "tool_call", {"tool_name": name, "arguments": arguments})
        attempt.data = {**data, "tool_receipt": {
            "call_id": call_id, "arguments_sha256": digest, "execution_id": str(execution_id),
        }}
        await db.commit()
    await hand_off_workflow_run(accepted)
    return execution_id


def _event_receipt(data: dict, event: dict[str, Any]) -> tuple[int, str, bool]:
    sequence = event.get("sequence")
    if type(sequence) is not int or not 1 <= sequence <= 1024:
        raise ValueError("Invalid Cloudflare agent sequence")
    digest = sha256(json.dumps(event, sort_keys=True, allow_nan=False).encode()).hexdigest()
    cursor = len(data["events"])
    if sequence <= cursor:
        if data["events"][sequence - 1] != digest:
            raise ValueError("Conflicting Cloudflare agent event replay")
        return sequence, digest, True
    if sequence != cursor + 1:
        raise ValueError("Cloudflare agent event sequence gap")
    return sequence, digest, False


async def _workflow_result(execution_id: UUID, deadline: datetime) -> tuple[str, bool, int | None]:
    from src.models.contracts.executions import WorkflowExecutionResponse
    from src.services.execution.agent_workflow_tools import workflow_failure_receipt

    while datetime.now(UTC) < deadline:
        async with get_db_context() as db:
            execution = await db.get(Execution, execution_id)
            if execution is None:
                raise ValueError("Workflow execution receipt no longer exists")
            if execution.completed_at is not None:
                result = execution.result
                success = execution.status.value == "Success"
                if not success:
                    response = WorkflowExecutionResponse(
                        execution_id=str(execution_id),
                        workflow_id=str(execution.workflow_id),
                        status=execution.status,
                        duration_ms=execution.duration_ms,
                        error=execution.error_message,
                        result=result,
                    )
                    text = "Error: " + json.dumps(workflow_failure_receipt(response), default=str)
                elif not result:
                    text = "Tool executed successfully"
                else:
                    text = json.dumps(result, default=str) if isinstance(result, (dict, list)) else str(result)
                if len(text.encode()) > 64 * 1024:
                    raise ValueError("Workflow tool result exceeds the agent budget")
                return text, not success, execution.duration_ms
        await asyncio.sleep(0.25)
    raise TimeoutError("Workflow tool did not finish within the agent deadline")


async def commit_event(run_id: UUID, claim_id: UUID, event: dict[str, Any]) -> dict[str, Any]:
    from src.core.cache import get_shared_redis
    from src.services.ai_usage_service import record_ai_usage
    from src.services.execution.autonomous_agent_executor import (
        _capture_tool_result_for_timeout_evidence,
    )

    child: tuple[UUID, str, bool, int | None] | None = None
    if event.get("kind") == "workflow_tool":
        async with get_db_context() as db:
            run = await lock_attempt_run(db, run_id, claim_id)
            attempt = _require_attempt(await db.get(CloudflareAgentAttempt, claim_id), run_id)
            data = attempt.data
            sequence, _, replay = _event_receipt(data, event)
            if replay:
                return {"sequence": sequence, "execution_id": data["tool_receipt"]["execution_id"]}
            if "tool_result" in data:
                raise ValueError("Workflow tool result was already committed")
            if run.status != "running" or event.get("turn") != 1 or data.get("model_tool") is None:
                raise ValueError("No workflow tool turn is pending")
            call = event["toolCall"]
            if {"id": call["id"], "name": call["name"], "arguments": json.loads(call["arguments"])} != data["model_tool"]:
                raise ValueError("Tool call differs from the committed model response")
            deadline = datetime.fromisoformat(data["deadline"])
        execution_id = await accept_workflow_tool(
            run_id=run_id, claim_id=claim_id, call_id=call["id"], name=call["name"],
            arguments=json.loads(call["arguments"]),
        )
        text, is_error, duration = await _workflow_result(execution_id, deadline)
        child = (execution_id, text, is_error, duration)

    async with get_db_context() as db:
        run = await lock_attempt_run(db, run_id, claim_id)
        attempt = _require_attempt(await db.get(CloudflareAgentAttempt, claim_id), run_id)
        data = dict(attempt.data)
        sequence, digest, replay = _event_receipt(data, event)
        if replay:
            receipt: dict[str, Any] = {"sequence": sequence}
            if child is not None:
                receipt["execution_id"] = str(child[0])
            return receipt
        turn = event.get("turn")
        if (
            run.status != "running" or type(turn) is not int or turn != len(data["requests"])
            or turn not in (1, 2)
        ):
            raise ValueError("Cloudflare agent turn is no longer active")
        reply: dict[str, Any] = {"sequence": sequence}
        kind = event.get("kind")
        if child is not None:
            if "tool_result" in data:
                raise ValueError("Workflow tool result was already committed")
            execution_id, text, is_error, duration = child
            result, truncated = _capture_tool_result_for_timeout_evidence(text)
            data["tool_result"] = text
            _step(db, run, data, "tool_result", {
                "tool_name": data["model_tool"]["name"], "is_error": is_error,
                "execution_id": str(execution_id), "result": result,
                **({"result_truncated": True} if truncated else {}),
            })
            reply["execution_id"] = str(execution_id)
        elif kind == "delta":
            text = event.get("text")
            if not isinstance(text, str) or data.get("finished_turn") == turn:
                raise ValueError("Invalid agent text delta")
            data["text"] += text
            if len(data["text"].encode()) > 64 * 1024:
                raise ValueError("Agent output exceeds byte budget")
        elif kind == "finish" and data.get("finished_turn") != turn:
            reason = event.get("reason")
            if reason not in {"error", "length", "stop", "tool_calls"}:
                raise ValueError("Invalid agent finish reason")
            data["finished_turn"] = turn
            data["finish_reason"] = reason
            if reason == "error":
                _step(db, run, data, "error", {"error": "Model request failed", "phase": "llm_call"})
            else:
                fields = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "reasoning_tokens")
                usage = {key: event.get("usage", {}).get(key) for key in fields}
                if any(type(value) is not int or not 0 <= value <= 1_000_000 for value in usage.values()):
                    raise ValueError("Invalid agent provider usage")
                call = event.get("toolCall")
                tool_calls: list[dict[str, Any]] = []
                if reason == "tool_calls":
                    if (
                        turn != 1 or not data.get("tool") or not isinstance(call, dict)
                        or call.get("name") != data["tool"]["name"]
                    ):
                        raise ValueError("Unexpected workflow tool call")
                    arguments = json.loads(call["arguments"])
                    if not isinstance(arguments, dict):
                        raise ValueError("Tool arguments must be a JSON object")
                    data["model_tool"] = {"id": call["id"], "name": call["name"], "arguments": arguments}
                    data["tool_text"] = data["text"]
                    tool_calls.append({"name": call["name"], "arguments": arguments})
                tokens = usage["input_tokens"] + usage["output_tokens"]
                run.tokens_used += tokens
                _step(db, run, data, "llm_response", {
                    "content": data["text"][:20000],
                    "tool_calls": tool_calls,
                    "finish_reason": {"tool_calls": "tool_call"}.get(reason, reason),
                    "usage": {**usage, "provider_cost": None},
                }, tokens)
                await record_ai_usage(
                    session=db, redis_client=await get_shared_redis(), provider=data["provider"], model=data["model"],
                    agent_run_id=run.id, organization_id=run.agent.organization_id if run.agent else run.org_id,
                    duration_ms=0, **usage,
                )
        else:
            raise ValueError("Unknown or repeated agent event")
        attempt.data = {**data, "events": [*data["events"], digest]}
        return reply


async def settle_run(
    run_id: UUID, claim_id: UUID, state: str, result: dict[str, Any], *, dispatch_id: UUID | None = None,
) -> None:
    """Close a run or hand it back using the durable grant boundary."""
    from src.core.pubsub import publish_agent_run_update
    from src.jobs.consumers.agent_run import _publish_sync_result
    from src.services.execution.agent_run_dispatch import hand_off_agent_run
    from src.services.execution.summary_dispatch import hand_off_summary, stage_summary

    async with get_db_context() as db:
        run = await lock_attempt_run(db, run_id, claim_id)
        attempt = await db.get(CloudflareAgentAttempt, claim_id)
        data = attempt.data if attempt is not None else {}
        if data.get("settled"):
            return
        if state == "not_started" and data.get("requests"):
            state = "uncertain"
        dispatch_id = dispatch_id or (UUID(data["dispatch_id"]) if data else None)
        dispatch = await db.get(WorkDispatch, dispatch_id) if dispatch_id else None
        if dispatch is None or run.status not in {"queued", "running", "cancelling", "cancelled"}:
            return
        if not data.get("requests") and state != "completed" and run.status in {"queued", "running"}:
            run.status = "queued"
            run.started_at = None
            dispatch.assigned_worker_id = None
            dispatch.payload = {**dispatch.payload, "cloudflare_agent_linux_only": True}
            await db.execute(delete(ExecutionWorkerClaim).where(ExecutionWorkerClaim.id == claim_id))
            if attempt is not None:
                await db.delete(attempt)
            await db.commit()
            await hand_off_agent_run()
            return

        summary = None
        if run.status != "cancelled":
            final_response_committed = (
                data.get("finish_reason") in {"stop", "length"}
                and data.get("finished_turn") == len(data["requests"])
            )
            success = (
                state == "completed" and result.get("success") is True
                and final_response_committed and result.get("through_sequence") == len(data["events"])
            )
            cancelled = run.status == "cancelling"
            error_type = result.get("error_type") if isinstance(result.get("error_type"), str) else None
            if cancelled:
                if final_response_committed:
                    state = "completed"
                run.status = "cancelled"
                run.error = "Cancelled by user" if state != "uncertain" else UNCERTAIN
            elif success:
                run.status = "completed"
                run.error = None
            elif state == "completed" and error_type == "AgentBudgetExceeded":
                run.status = "budget_exceeded"
                run.error = "The autonomous agent reached its model budget"
            else:
                run.status = "failed"
                refusal = {
                    "AgentTurnRefused": "The next model turn was refused",
                    "AgentRequestRefused": "The agent request was refused",
                    "AgentProviderRequestTooLarge": "The next model request exceeds the provider size limit",
                }
                run.error = (
                    UNCERTAIN if state != "completed" or result.get("success")
                    else refusal.get(error_type, "Cloudflare model request failed")
                )
            run.output = {"text": data.get("text") or None}
            run.llm_model = data.get("model")
            now = datetime.now(UTC)
            run.completed_at = now
            start = run.started_at or now
            run.duration_ms = int((now - start).total_seconds() * 1000)
            if cancelled and attempt is not None:
                _step(db, run, data, "cancelled", {"reason": run.error, "iterations_used": run.iterations_used})
            if run.status == "completed":
                summary = await stage_summary(db, run.id)
        sync = dispatch.payload.get("sync")
        await db.delete(dispatch)
        if attempt is not None:
            attempt.data = {**data, "settled": True, "attempt_state": state}
        await db.commit()
        agent_name = run.agent.name if run.agent is not None else "Unknown"
    await hand_off_summary(summary)
    await publish_agent_run_update(run, agent_name)
    if sync:
        await _publish_sync_result(str(run.id), {
            "output": run.output, "status": run.status, "error": run.error,
            "iterations_used": run.iterations_used, "tokens_used": run.tokens_used, "llm_model": run.llm_model,
        })
