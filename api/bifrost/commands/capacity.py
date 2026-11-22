"""Operate platform capacity through the authenticated REST API."""

import json
from decimal import Decimal
from typing import Any, TextIO
from uuid import UUID

import click
from pydantic import ValidationError

from bifrost.client import BifrostClient
from bifrost.contracts.cloudflare_capacity import (
    CloudflareAccountTokenRotation,
    CloudflareActivateRequest,
    CloudflareCapacityAction,
    CloudflarePreflightRequest,
    CloudflareReconcileAction,
)
from bifrost.dto_flags import build_cli_flags
from bifrost.platform_jobs import poll_platform_job
from bifrost.refs import RefResolver

from .base import _apply_flags, entity_group, output_result, pass_resolver, run_async

capacity_group = entity_group("capacity", "Manage platform worker capacity.")
cloudflare_group = entity_group("cloudflare", "Read and manage Cloudflare capacity.")
capacity_group.add_command(cloudflare_group)

_PATH = "/api/capacity/cloudflare"
_ACTION_FLAGS = build_cli_flags(CloudflareCapacityAction, exclude=set(), verb_ref_lookups={})


@cloudflare_group.command("status")
@click.pass_context
@pass_resolver
@run_async
async def cloudflare_status(ctx: click.Context, *, client: BifrostClient, resolver: RefResolver) -> None:
    """Show the current Cloudflare connection and capacity policy."""
    response = await client.get(_PATH)
    response.raise_for_status()
    output_result(response.json(), ctx=ctx)


def _register_action(name: str, help_text: str, *, credential: str | None = None) -> None:
    model = CloudflareReconcileAction if name in ("reconcile", "upgrade") else CloudflareCapacityAction
    flags = build_cli_flags(model, exclude=set(), verb_ref_lookups={})
    @cloudflare_group.command(name, help=help_text)
    @click.option("--wait", is_flag=True, help="Wait for completion through the platform job.")
    @_apply_flags(flags)
    @click.pass_context
    @pass_resolver
    @run_async
    async def action(
        ctx: click.Context, wait: bool, *, client: BifrostClient, resolver: RefResolver, **fields: Any,
    ) -> None:
        body = model.model_validate({key: value for key, value in fields.items() if value is not None})
        data = body.model_dump(mode="json", exclude_defaults=True)
        if credential is not None:
            data["credential"] = credential
        response = await client.post(f"{_PATH}/{'rotate' if credential else name}", json=data)
        response.raise_for_status()
        accepted = response.json()
        result = await poll_platform_job(client, accepted["job_id"], label=help_text) if wait else accepted
        output_result(result, ctx=ctx)


_register_action("scale-in", "Drain Cloudflare workers and retain the connection.")
_register_action("disconnect", "Drain Cloudflare workers and remove verified owned capacity.")
_register_action("enable", "Provision and verify Cloudflare Workers capacity.")
_register_action("reconcile", "Verify or repair the current Cloudflare Workers deployment.")
_register_action("upgrade", "Apply the Workers release packaged with Bifrost.")
_register_action("rotate-root", "Rotate the derivation root after Scale In.", credential="control_root")
_register_action("rotate-bearer", "Retire the previous Workers deployment and stage a new bearer.", credential="scope_bearer")
_register_action("rotate-connector", "Rotate the Managed Tunnel connector token after Scale In.", credential="connector_token")


def _read_policy(policy: TextIO) -> dict[str, Any]:
    try:
        value = json.load(policy)
    except ValueError:
        raise click.ClickException("The Cloudflare policy file must contain a JSON object.") from None
    if not isinstance(value, dict):
        raise click.ClickException("The Cloudflare policy file must contain a JSON object.")
    return value


def _preflight_options(command):
    for option in reversed((
        click.option("--account-id"),
        click.option("--zone-id"),
        click.option("--reachability-mode", type=click.Choice(["existing_https_endpoint", "managed_tunnel"])),
        click.option("--worker-ingress-origin"),
        click.option("--credential-ref", type=click.UUID),
        click.option("--policy", type=click.File("r", encoding="utf-8"), help="Advanced capacity policy JSON file."),
    )):
        command = option(command)
    return command


def _preflight_body(policy: TextIO | None, credential_ref: UUID | None, fields: dict[str, Any]) -> CloudflarePreflightRequest:
    return CloudflarePreflightRequest(
        **fields, credential_ref=credential_ref, policy=_read_policy(policy) if policy else None,
        api_token=None if credential_ref else click.prompt("Cloudflare API Token", hide_input=True),
    )


@cloudflare_group.command("preflight")
@_preflight_options
@click.pass_context
@pass_resolver
@run_async
async def preflight(
    ctx: click.Context, policy: TextIO | None, credential_ref: UUID | None,
    *, client: BifrostClient, resolver: RefResolver, **fields: Any,
) -> None:
    """Discover Cloudflare prerequisites and price the capacity policy."""
    body = _preflight_body(policy, credential_ref, fields)
    response = await client.post(f"{_PATH}/preflight", json=body.request_body())
    response.raise_for_status()
    output_result(response.json(), ctx=ctx)


@cloudflare_group.command("activate")
@_preflight_options
@click.option("--monthly-limit-usd", required=True, help="Confirmed monthly estimated usage limit in USD.")
@click.option("--wait", is_flag=True, help="Wait for completion through the platform job.")
@click.pass_context
@pass_resolver
@run_async
async def activate(
    ctx: click.Context, policy: TextIO | None, credential_ref: UUID | None, monthly_limit_usd: str,
    wait: bool, *, client: BifrostClient, resolver: RefResolver, **fields: Any,
) -> None:
    """Run preflight, review the route, and provision Cloudflare Workers."""
    request = _preflight_body(policy, credential_ref, fields)
    response = await client.post(f"{_PATH}/preflight", json=request.request_body())
    response.raise_for_status()
    plan = response.json()
    if not plan["preflight_receipt"]:
        output_result(plan, ctx=ctx)
        raise click.ClickException("Preflight needs input. Review the account, reachability, and zone shown above.")
    try:
        body = CloudflareActivateRequest.model_validate({
            "preflight_receipt": plan["preflight_receipt"], "credential_ref": credential_ref,
            "monthly_limit_usd": monthly_limit_usd, "consent": True, "consent_version": plan["consent_version"],
        })
    except ValidationError:
        raise click.UsageError("Enter a positive monthly limit in USD with up to two decimal places.") from None
    envelope = Decimal(plan["preview"]["fleet_envelope_usd"])
    if body.monthly_limit_usd < envelope:
        raise click.UsageError(f"The monthly limit must be at least {envelope} USD for this fleet.")
    account = next(
        (item["name"] for item in plan["accounts"] or [] if item["id"] == plan["account_id"]),
        plan["account_id"],
    )
    mode = "Managed Tunnel" if plan["reachability_mode"] == "managed_tunnel" else "Existing HTTPS Endpoint"
    click.echo(f"Account: {account} ({plan['account_id']})", err=True)
    if plan["zone_id"]:
        click.echo(f"DNS Zone: {plan['zone_name']} ({plan['zone_id']})", err=True)
    click.echo(f"Reachability: {mode}", err=True)
    click.echo(f"HTTPS Origin: {plan['worker_ingress_origin']}", err=True)
    click.echo(f"Monthly Usage Limit: {body.monthly_limit_usd:.2f} USD", err=True)
    click.echo(plan["consent_text"], err=True)
    click.confirm("Accept the reviewed Cloudflare route and scale out", abort=True)
    data = body.model_dump(mode="json", exclude_none=True)
    if request.api_token is not None:
        data["api_token"] = request.api_token.get_secret_value()
    response = await client.post(f"{_PATH}/activate", json=data)
    response.raise_for_status()
    accepted = response.json()
    result = await poll_platform_job(client, accepted["job_id"], label="Scale Out with Cloudflare") if wait else accepted
    output_result(result, ctx=ctx)


@cloudflare_group.command("stage-credential")
@click.option("--account-id", help="Account ID for connecting Cloudflare.")
@click.option("--installation-id", type=click.UUID, help="Installation ID for rotating a connected token.")
@click.option("--purpose", type=click.Choice(["connect", "rotate"]), required=True)
@click.pass_context
@pass_resolver
@run_async
async def stage_credential(
    ctx: click.Context, account_id: str | None, installation_id: UUID | None, purpose: str,
    *, client: BifrostClient, resolver: RefResolver,
) -> None:
    """Enter a Cloudflare token for a one-time MCP credential reference."""
    if (account_id is None) == (installation_id is None) or (purpose == "connect" and account_id is None):
        raise click.UsageError("Provide --account-id for connect or --installation-id for rotate.")
    target = {"installation_id": str(installation_id)} if installation_id is not None else {"account_id": account_id}
    response = await client.post(f"{_PATH}/credential", json={
        **target, "purpose": purpose,
        "api_token": click.prompt("Cloudflare API Token", hide_input=True),
    })
    response.raise_for_status()
    output_result(response.json(), ctx=ctx)


@cloudflare_group.command("policy")
@_apply_flags(_ACTION_FLAGS)
@click.option("--policy", type=click.File("r", encoding="utf-8"), required=True, help="Cloudflare capacity policy JSON file.")
@click.pass_context
@pass_resolver
@run_async
async def policy(
    ctx: click.Context, policy: TextIO, *, client: BifrostClient, resolver: RefResolver, **fields: Any,
) -> None:
    """Update the current Cloudflare capacity policy."""
    current = CloudflareCapacityAction.model_validate(fields)
    response = await client.patch(_PATH, json={
        **current.model_dump(mode="json"), "action": "policy", "policy": _read_policy(policy),
    })
    response.raise_for_status()
    output_result(response.json(), ctx=ctx)


@cloudflare_group.command("rotate", help="Validate and replace the Cloudflare account API token.")
@click.option("--wait", is_flag=True, help="Wait for completion through the platform job.")
@_apply_flags(_ACTION_FLAGS)
@click.pass_context
@pass_resolver
@run_async
async def rotate(
    ctx: click.Context, wait: bool, *, client: BifrostClient, resolver: RefResolver, **fields: Any,
) -> None:
    try:
        body = CloudflareAccountTokenRotation.model_validate({
            **fields, "api_token": click.prompt("Replacement Cloudflare API Token", hide_input=True),
        })
    except ValidationError:
        raise click.ClickException("Enter a valid Cloudflare API token and installation revision.") from None
    response = await client.post(f"{_PATH}/rotate", json={
        **body.model_dump(mode="json", exclude={"api_token"}),
        "api_token": body.api_token.get_secret_value(),
    })
    response.raise_for_status()
    accepted = response.json()
    result = await poll_platform_job(client, accepted["job_id"], label="Rotate Cloudflare API Token") if wait else accepted
    output_result(result, ctx=ctx)
