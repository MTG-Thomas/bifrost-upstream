"""Operate platform capacity through the authenticated REST API."""

import json
from typing import Any, TextIO

import click
from pydantic import ValidationError

from bifrost.client import BifrostClient
from bifrost.contracts.cloudflare_capacity import (
    CloudflareAccountTokenRotation,
    CloudflareCapacityAction,
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
    @cloudflare_group.command(name, help=help_text)
    @click.option("--wait", is_flag=True, help="Wait for completion through the platform job.")
    @_apply_flags(_ACTION_FLAGS)
    @click.pass_context
    @pass_resolver
    @run_async
    async def action(
        ctx: click.Context, wait: bool, *, client: BifrostClient, resolver: RefResolver, **fields: Any,
    ) -> None:
        body = CloudflareCapacityAction.model_validate(fields)
        data = body.model_dump(mode="json")
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


def _read_policy(policy: TextIO) -> dict[str, Any]:
    try:
        value = json.load(policy)
    except ValueError:
        raise click.ClickException("The Cloudflare policy file must contain a JSON object.") from None
    if not isinstance(value, dict):
        raise click.ClickException("The Cloudflare policy file must contain a JSON object.")
    return value


@cloudflare_group.command("stage-credential")
@click.option("--account-id", required=True)
@click.option("--purpose", type=click.Choice(["connect", "rotate"]), required=True)
@click.pass_context
@pass_resolver
@run_async
async def stage_credential(
    ctx: click.Context, account_id: str, purpose: str, *, client: BifrostClient, resolver: RefResolver,
) -> None:
    """Enter a Cloudflare token for a one-time MCP credential reference."""
    response = await client.post(f"{_PATH}/credential", json={
        "account_id": account_id, "purpose": purpose,
        "api_token": click.prompt("Cloudflare API Token", hide_input=True),
    })
    response.raise_for_status()
    output_result(response.json(), ctx=ctx)


@cloudflare_group.command("connect")
@click.option("--account-id", required=True)
@click.option("--worker-ingress-origin", required=True)
@click.option("--policy", type=click.File("r", encoding="utf-8"), required=True, help="Cloudflare capacity policy JSON file.")
@click.pass_context
@pass_resolver
@run_async
async def connect(
    ctx: click.Context, account_id: str, worker_ingress_origin: str, policy: TextIO, *,
    client: BifrostClient, resolver: RefResolver,
) -> None:
    """Validate an account token and connect Cloudflare."""
    limits = _read_policy(policy)
    current = await client.get(_PATH)
    current.raise_for_status()
    consent = current.json()
    click.echo(consent["consent_text"], err=True)
    click.confirm("Accept the current Cloudflare consent", abort=True)
    response = await client.patch(_PATH, json={
        "action": "connect", "account_id": account_id, "worker_ingress_origin": worker_ingress_origin,
        "consent_version": consent["consent_text_version"], "consent": True,
        "api_token": click.prompt("Cloudflare API Token", hide_input=True), "policy": limits,
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
