"""Review Cloudflare workflow approval through the public API."""

from typing import Any

import click

from bifrost.client import BifrostClient
from bifrost.contracts.cloudflare_approval import (
    CloudflareApprovalAccept,
    CloudflareApprovalOptions,
)
from bifrost.dto_flags import assemble_body, build_cli_flags
from bifrost.refs import RefResolver

from .base import _apply_flags, entity_group, output_result, pass_resolver, run_async

approval_group = entity_group("cloudflare-approval", "Review Cloudflare workflow eligibility and approval.")


@approval_group.command("status")
@click.argument("ref")
@click.pass_context
@pass_resolver
@run_async
async def status(ctx: click.Context, ref: str, *, client: BifrostClient, resolver: RefResolver) -> None:
    """Read approval status for a workflow."""
    workflow_id = await resolver.resolve("workflow", ref)
    response = await client.get(f"/api/workflows/{workflow_id}/cloudflare-approval")
    response.raise_for_status()
    output_result(response.json(), ctx=ctx)


@approval_group.command("propose")
@click.argument("ref")
@_apply_flags(build_cli_flags(CloudflareApprovalOptions, exclude=set(), verb_ref_lookups={}))
@click.pass_context
@pass_resolver
@run_async
async def propose(ctx: click.Context, ref: str, *, client: BifrostClient, resolver: RefResolver, **fields: Any) -> None:
    """Generate a review with source pins, budgets and diagnostics."""
    workflow_id = await resolver.resolve("workflow", ref)
    body = await assemble_body(CloudflareApprovalOptions, fields, resolver=resolver)
    response = await client.post(f"/api/workflows/{workflow_id}/cloudflare-approval/propose", json=body)
    response.raise_for_status()
    output_result(response.json(), ctx=ctx)


@approval_group.command("accept")
@click.argument("ref")
@_apply_flags(build_cli_flags(CloudflareApprovalAccept, exclude=set(), verb_ref_lookups={}))
@click.pass_context
@pass_resolver
@run_async
async def accept(ctx: click.Context, ref: str, *, client: BifrostClient, resolver: RefResolver, **fields: Any) -> None:
    """Accept only the supplied reviewed digest. Repeat the reviewed options."""
    workflow_id = await resolver.resolve("workflow", ref)
    body = await assemble_body(CloudflareApprovalAccept, fields, resolver=resolver)
    response = await client.post(f"/api/workflows/{workflow_id}/cloudflare-approval/accept", json=body)
    response.raise_for_status()
    output_result(response.json(), ctx=ctx)


@approval_group.command("revoke")
@click.argument("ref")
@click.pass_context
@pass_resolver
@run_async
async def revoke(ctx: click.Context, ref: str, *, client: BifrostClient, resolver: RefResolver) -> None:
    """Revoke the current approval."""
    workflow_id = await resolver.resolve("workflow", ref)
    response = await client.delete(f"/api/workflows/{workflow_id}/cloudflare-approval")
    response.raise_for_status()
    output_result(response.json(), ctx=ctx)
