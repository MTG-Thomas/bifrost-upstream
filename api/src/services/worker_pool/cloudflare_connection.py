"""Verify and configure a platform Cloudflare connection."""

import httpx
from shared.models import (
    CloudflareCapacityStatus,
    CloudflareConnectRequest,
    CloudflarePolicyUpdate,
)
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.audit import emit_audit
from src.services.authorization.enforce import Caller
from src.services.worker_pool.cloudflare_credentials import consume_staged_token
from src.services.worker_pool.cloudflare_policy import (
    CONSENT_TEXT_VERSION,
    CloudflarePolicyConflict,
    create_cloudflare_installation,
    get_cloudflare_capacity_status,
    release_meter_bounds,
    save_cloudflare_policy,
)
from src.services.worker_pool.cloudflare_provisioning import OwnershipConflict
from src.services.worker_pool.cloudflare_release import load_workers_release
from src.services.worker_pool.cloudflare_tunnel import tunnel_origin
from src.services.worker_pool.cloudflare_tunnel_rest import (
    TunnelRest,
    dedicated_hostname,
)
from src.services.worker_pool.cloudflare_workers_rest import WorkersRestCleanup


async def configure_cloudflare(
    db: AsyncSession, caller: Caller, body: CloudflareConnectRequest | CloudflarePolicyUpdate,
) -> CloudflareCapacityStatus:
    bounds = release_meter_bounds(load_workers_release())
    if isinstance(body, CloudflareConnectRequest):
        if body.consent_version != CONSENT_TEXT_VERSION:
            raise CloudflarePolicyConflict("Review the current Cloudflare consent text.")
        actor_id = caller.principal.user_id
        token = body.api_token
        if body.credential_ref is not None:
            token = await consume_staged_token(
                db, body.credential_ref, actor_id=actor_id, account_id=body.account_id, purpose="connect",
            )
        if token is None:
            raise ValueError("Cloudflare credential is unavailable")
        hostname = dedicated_hostname(body.worker_ingress_origin)
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            await WorkersRestCleanup(http).verify_workers_paid(body.account_id, token)
            if body.zone_id is not None:
                tunnel_origin()
                try:
                    await TunnelRest(
                        http, account_id=body.account_id, zone_id=body.zone_id, hostname=hostname, token=token,
                    ).verify_zone()
                except OwnershipConflict:
                    raise ValueError("Managed Tunnel requires a DNS zone owned by the selected Cloudflare account") from None
        installation_id = await create_cloudflare_installation(
            db, account_id=body.account_id, worker_ingress_origin=body.worker_ingress_origin,
            consent_version=body.consent_version, actor_id=actor_id,
            verified_api_token=token, policy=body.policy, bounds=bounds,
            reachability_mode=body.reachability_mode, zone_id=body.zone_id,
        )
        await emit_audit(
            db, "capacity.cloudflare_connect", resource_type="cloudflare_installation", resource_id=installation_id,
            details={"account_id": body.account_id, "consent_version": body.consent_version,
                     "reachability_mode": body.reachability_mode, "zone_id": body.zone_id},
        )
    else:
        revision = await save_cloudflare_policy(
            db, installation_id=body.installation_id, expected_revision=body.expected_revision,
            policy=body.policy, bounds=bounds,
        )
        if revision != body.expected_revision:
            await emit_audit(db, "capacity.cloudflare_policy_updated", resource_type="cloudflare_installation",
                             resource_id=body.installation_id, details={"revision": revision})
    return await get_cloudflare_capacity_status(db, caller)
