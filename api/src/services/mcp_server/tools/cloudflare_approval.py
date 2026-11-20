"""Thin REST tools for Cloudflare workflow approval."""

from typing import Any
from uuid import UUID

from src.services.mcp_server.tool_result import ToolResult, error_result, success_result
from src.services.mcp_server.tools._http_bridge import call_rest


def _result(code: int, result: Any) -> ToolResult:
    if code != 200:
        return error_result(f"Cloudflare workflow approval returned HTTP {code}", {"body": result})
    return success_result("Cloudflare workflow approval", result)


async def bifrost_workflow_cloudflare_approval_status(context: Any, workflow_id: UUID) -> ToolResult:
    return _result(*await call_rest(context, "GET", f"/api/workflows/{workflow_id}/cloudflare-approval"))


async def bifrost_workflow_cloudflare_approval_propose(
    context: Any, workflow_id: UUID, subrequest_allowance: int, cpu_budget_ms: int,
    https_hosts: list[str] | None = None,
) -> ToolResult:
    return _result(*await call_rest(
        context, "POST", f"/api/workflows/{workflow_id}/cloudflare-approval/propose",
        json_body={
            "subrequest_allowance": subrequest_allowance, "cpu_budget_ms": cpu_budget_ms,
            "https_hosts": https_hosts or [],
        },
    ))


async def bifrost_workflow_cloudflare_approval_accept(
    context: Any, workflow_id: UUID, subrequest_allowance: int, cpu_budget_ms: int, review_sha256: str,
    https_hosts: list[str] | None = None,
) -> ToolResult:
    return _result(*await call_rest(
        context, "POST", f"/api/workflows/{workflow_id}/cloudflare-approval/accept",
        json_body={
            "subrequest_allowance": subrequest_allowance, "cpu_budget_ms": cpu_budget_ms,
            "https_hosts": https_hosts or [], "review_sha256": review_sha256,
        },
    ))


async def bifrost_workflow_cloudflare_approval_revoke(context: Any, workflow_id: UUID) -> ToolResult:
    return _result(*await call_rest(context, "DELETE", f"/api/workflows/{workflow_id}/cloudflare-approval"))


TOOLS = [
    ("bifrost_workflow_cloudflare_approval_status", "Cloudflare Approval Status", "Read a workflow's Cloudflare approval status by workflow UUID."),
    ("bifrost_workflow_cloudflare_approval_propose", "Propose Cloudflare Approval", "Generate an approval proposal and diagnostics by workflow UUID."),
    ("bifrost_workflow_cloudflare_approval_accept", "Accept Cloudflare Approval", "Accept the reviewed digest and its options as a Global operator."),
    ("bifrost_workflow_cloudflare_approval_revoke", "Revoke Cloudflare Approval", "Revoke the current approval as a Global operator."),
]


def register_tools(mcp: Any, get_context_fn: Any) -> None:
    from src.services.mcp_server.generators.fastmcp_generator import (
        register_tool_with_context,
    )

    functions = {
        "bifrost_workflow_cloudflare_approval_status": bifrost_workflow_cloudflare_approval_status,
        "bifrost_workflow_cloudflare_approval_propose": bifrost_workflow_cloudflare_approval_propose,
        "bifrost_workflow_cloudflare_approval_accept": bifrost_workflow_cloudflare_approval_accept,
        "bifrost_workflow_cloudflare_approval_revoke": bifrost_workflow_cloudflare_approval_revoke,
    }
    for tool_id, _name, description in TOOLS:
        register_tool_with_context(mcp, functions[tool_id], tool_id, description, get_context_fn)
