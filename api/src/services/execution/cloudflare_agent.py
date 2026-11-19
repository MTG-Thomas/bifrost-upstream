"""Fenced API operations for the autonomous Workers agent runtime."""

from __future__ import annotations

import json
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.run_lineage import principal_lineage
from src.core.database import get_db_context
from src.models.orm.agent_runs import AgentRun, CloudflareAgentAttempt
from src.models.orm.execution_workers import ExecutionWorkerClaim
from src.services.authorization.enforce import load_caller
from src.services.authorization.runtime_auth import load_runtime_principal
from src.services.durable_impersonation import principal_durable_impersonation
from src.services.execution.agent_workflow_tools import authorize_bound_workflow_tool
from src.services.execution.async_executor import _pending_run
from src.services.execution.workflow_dispatch import hand_off_workflow_run, stage_workflow_run
from src.services.tool_schema import parameters_json_schema, validate_arguments_against_schema
from src.worker.remote.fencing import current_remote_lease_ref, fence_remote_owner


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
        if run.status != "running":
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
        attempt.data = {**attempt.data, "tool_receipt": {
            "call_id": call_id, "arguments_sha256": digest, "execution_id": str(execution_id),
        }}
        await db.commit()
    await hand_off_workflow_run(accepted)
    return execution_id
