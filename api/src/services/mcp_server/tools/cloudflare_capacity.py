"""Thin REST bridge for the Cloudflare capacity operator actions."""

from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from shared.models import (
    CloudflareAccountTokenReferenceRotation,
    CloudflareActivateRequest,
    CloudflareCapacityAction,
    CloudflareCapacityPolicy,
    CloudflarePolicyUpdate,
    CloudflarePreflightRequest,
    CloudflareReachabilityMode,
    CloudflareReconcileAction,
    CloudflareWorkerCredentialRotation,
)

from src.services.mcp_server.tool_result import ToolResult, error_result, success_result
from src.services.mcp_server.tools._http_bridge import call_rest

_PATH = "/api/capacity/cloudflare"


async def bifrost_cloudflare_capacity_get(context: Any) -> ToolResult:
    status, body = await call_rest(context, "GET", _PATH)
    if status != 200:
        return error_result(f"Cloudflare capacity request failed (HTTP {status})", {"body": body})
    return success_result("Cloudflare Capacity", body)


async def bifrost_cloudflare_preflight_validate(
    context: Any, credential_ref: UUID, account_id: str | None = None, zone_id: str | None = None,
    reachability_mode: CloudflareReachabilityMode | None = None, worker_ingress_origin: str | None = None,
    policy: CloudflareCapacityPolicy | None = None, containers: bool = False,
    container_instance_hours: int | None = None,
    container_qualification_canaries: int | None = None,
) -> ToolResult:
    request = CloudflarePreflightRequest(
        credential_ref=credential_ref, account_id=account_id, zone_id=zone_id,
        reachability_mode=reachability_mode, worker_ingress_origin=worker_ingress_origin, policy=policy,
        containers=containers, container_instance_hours=container_instance_hours,
        container_qualification_canaries=container_qualification_canaries,
    )
    status, body = await call_rest(
        context, "POST", f"{_PATH}/preflight", json_body=request.model_dump(mode="json", exclude_none=True),
    )
    if status != 200:
        return error_result(f"Cloudflare preflight failed (HTTP {status})", {"body": body})
    return success_result("Cloudflare Preflight", body)


async def bifrost_cloudflare_activate_execute(
    context: Any, credential_ref: UUID, preflight_receipt: str, consent_version: str, consent: Literal[True],
    monthly_limit_usd: Decimal,
) -> ToolResult:
    request = CloudflareActivateRequest(
        credential_ref=credential_ref, preflight_receipt=preflight_receipt, consent_version=consent_version,
        consent=consent, monthly_limit_usd=monthly_limit_usd,
    )
    status, body = await call_rest(
        context, "POST", f"{_PATH}/activate", json_body=request.model_dump(mode="json", exclude_none=True),
    )
    if status != 202:
        return error_result(f"Cloudflare activation failed (HTTP {status})", {"body": body})
    return success_result("Cloudflare Job Queued", body)


async def _submit(
    context: Any, operation: str, installation_id: UUID, expected_revision: int, *,
    reset_release_fence: bool | None = None,
) -> ToolResult:
    request = (
        CloudflareCapacityAction(installation_id=installation_id, expected_revision=expected_revision)
        if reset_release_fence is None else CloudflareReconcileAction(
            installation_id=installation_id, expected_revision=expected_revision, reset_release_fence=reset_release_fence,
        )
    )
    status, body = await call_rest(
        context, "POST", f"{_PATH}/{operation}", json_body=request.model_dump(mode="json", exclude_defaults=True),
    )
    if status != 202:
        return error_result(f"Cloudflare action failed (HTTP {status})", {"body": body})
    return success_result("Cloudflare Job Queued", body)


async def bifrost_cloudflare_scale_in_execute(context: Any, installation_id: UUID, expected_revision: int) -> ToolResult:
    return await _submit(context, "scale-in", installation_id, expected_revision)


async def bifrost_cloudflare_disconnect_execute(context: Any, installation_id: UUID, expected_revision: int) -> ToolResult:
    return await _submit(context, "disconnect", installation_id, expected_revision)


async def bifrost_cloudflare_enable_execute(context: Any, installation_id: UUID, expected_revision: int) -> ToolResult:
    return await _submit(context, "enable", installation_id, expected_revision)


async def bifrost_cloudflare_reconcile_execute(
    context: Any, installation_id: UUID, expected_revision: int, reset_release_fence: bool = False,
) -> ToolResult:
    return await _submit(context, "reconcile", installation_id, expected_revision, reset_release_fence=reset_release_fence)


async def bifrost_cloudflare_upgrade_execute(
    context: Any, installation_id: UUID, expected_revision: int, reset_release_fence: bool = False,
) -> ToolResult:
    return await _submit(context, "upgrade", installation_id, expected_revision, reset_release_fence=reset_release_fence)


async def bifrost_cloudflare_worker_credential_replace(
    context: Any, installation_id: UUID, expected_revision: int, credential: Literal["control_root", "scope_bearer", "connector_token"],
) -> ToolResult:
    request = CloudflareWorkerCredentialRotation(
        installation_id=installation_id, expected_revision=expected_revision, credential=credential,
    )
    status, body = await call_rest(context, "POST", f"{_PATH}/rotate", json_body=request.model_dump(mode="json"))
    if status != 202:
        return error_result(f"Cloudflare action failed (HTTP {status})", {"body": body})
    return success_result("Cloudflare Job Queued", body)




async def bifrost_cloudflare_account_token_replace(
    context: Any, installation_id: UUID, expected_revision: int, credential_ref: UUID,
) -> ToolResult:
    request = CloudflareAccountTokenReferenceRotation(
        installation_id=installation_id, expected_revision=expected_revision, credential_ref=credential_ref,
    )
    status, body = await call_rest(context, "POST", f"{_PATH}/rotate", json_body=request.model_dump(mode="json"))
    if status != 202:
        return error_result(f"Cloudflare rotation failed (HTTP {status})", {"body": body})
    return success_result("Cloudflare Job Queued", body)


async def bifrost_cloudflare_policy_update(
    context: Any, installation_id: UUID, expected_revision: int, policy: CloudflareCapacityPolicy,
) -> ToolResult:
    request = CloudflarePolicyUpdate(
        action="policy", installation_id=installation_id, expected_revision=expected_revision, policy=policy,
    )
    status, body = await call_rest(context, "PATCH", _PATH, json_body=request.model_dump(mode="json"))
    if status != 200:
        return error_result(f"Cloudflare policy request failed (HTTP {status})", {"body": body})
    return success_result("Cloudflare Capacity", body)


_TOOLS = (
    (bifrost_cloudflare_capacity_get, "Cloudflare Capacity Status", "Read the Global Cloudflare connection and policy."),
    (bifrost_cloudflare_preflight_validate, "Preview Cloudflare Capacity", "Use a reference from capacity cloudflare stage-credential --purpose connect. Present the resolved account, zone, hostname, usage forecast, limit and consent before activation."),
    (bifrost_cloudflare_activate_execute, "Scale Out with Cloudflare", "Requires operator acceptance of the preflight and explicit confirmation of the monthly limit. Use the same staged credential reference."),
    (bifrost_cloudflare_scale_in_execute, "Scale In Cloudflare", "Drain Cloudflare workers and retain the connection."),
    (bifrost_cloudflare_disconnect_execute, "Disconnect Cloudflare", "Drain capacity and remove owned resources."),
    (bifrost_cloudflare_enable_execute, "Enable Cloudflare", "Provision and verify Cloudflare Workers capacity."),
    (bifrost_cloudflare_reconcile_execute, "Reconcile Cloudflare", "Verify or repair the current Cloudflare Workers deployment."),
    (bifrost_cloudflare_upgrade_execute, "Upgrade Cloudflare", "Apply the Workers release packaged with Bifrost."),
    (bifrost_cloudflare_worker_credential_replace, "Rotate Cloudflare Worker Credential", "After Scale In, rotate the internal control root, Workers scope bearer, or Managed Tunnel connector token."),
    (bifrost_cloudflare_account_token_replace, "Rotate Cloudflare API Token", "Use a one-time reference from capacity cloudflare stage-credential --purpose rotate --installation-id."),
    (bifrost_cloudflare_policy_update, "Update Cloudflare Policy", "Update the current Cloudflare capacity policy."),
)
TOOLS = [(tool.__name__, title, description) for tool, title, description in _TOOLS]


def register_tools(mcp: Any, get_context_fn: Any) -> None:
    from src.services.mcp_server.generators.fastmcp_generator import (
        register_tool_with_context,
    )

    for tool, _, description in _TOOLS:
        register_tool_with_context(mcp, tool, tool.__name__, description, get_context_fn)
