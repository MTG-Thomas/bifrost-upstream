"""Operate platform capacity through the authenticated REST API."""

from typing import Any

import click

from bifrost.client import BifrostClient
from bifrost.contracts.cloudflare_capacity import CloudflareCapacityAction
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


def _register_action(name: str, help_text: str) -> None:
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
        response = await client.post(f"{_PATH}/{name}", json=body.model_dump(mode="json"))
        response.raise_for_status()
        accepted = response.json()
        result = await poll_platform_job(client, accepted["job_id"], label=help_text) if wait else accepted
        output_result(result, ctx=ctx)


_register_action("scale-in", "Drain Cloudflare workers and retain the connection.")
_register_action("disconnect", "Drain Cloudflare workers and remove verified owned capacity.")
