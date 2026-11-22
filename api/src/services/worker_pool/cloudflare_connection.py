"""Update policy for an existing Cloudflare installation."""

from shared.models import CloudflareCapacityStatus, CloudflarePolicyUpdate
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.audit import emit_audit
from src.services.authorization.enforce import Caller
from src.services.worker_pool.cloudflare_policy import (
    get_cloudflare_capacity_status,
    release_meter_bounds,
    save_cloudflare_policy,
)
from src.services.worker_pool.cloudflare_release import load_workers_release


async def configure_cloudflare(
    db: AsyncSession, caller: Caller, body: CloudflarePolicyUpdate,
) -> CloudflareCapacityStatus:
    bounds = release_meter_bounds(load_workers_release())
    revision = await save_cloudflare_policy(
        db, installation_id=body.installation_id, expected_revision=body.expected_revision,
        policy=body.policy, bounds=bounds,
    )
    if revision != body.expected_revision:
        await emit_audit(db, "capacity.cloudflare_policy_updated", resource_type="cloudflare_installation",
                         resource_id=body.installation_id, details={"revision": revision})
    return await get_cloudflare_capacity_status(db, caller)
