"""Update policy for an existing Cloudflare installation."""

import httpx
from shared.models import CloudflareCapacityStatus, CloudflarePolicyUpdate
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.services.authorization.enforce import Caller
from src.services.worker_pool.cloudflare_containers_release import load_containers_release
from src.services.worker_pool.cloudflare_policy import (
    get_cloudflare_capacity_status,
    release_meter_bounds,
    save_cloudflare_policy,
)
from src.services.worker_pool.cloudflare_release import load_workers_release


async def configure_cloudflare(
    db: AsyncSession, caller: Caller, body: CloudflarePolicyUpdate,
) -> CloudflareCapacityStatus:
    workers_release = load_workers_release()
    bounds = release_meter_bounds(workers_release)
    installation = await db.get(CloudflareInstallation, body.installation_id)
    containers_release = None
    if installation is not None and installation.containers_policy.get("enabled") is True:
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            containers_release = await load_containers_release(http, workers_release)
    await save_cloudflare_policy(
        db, installation_id=body.installation_id, expected_revision=body.expected_revision,
        expected_policy_version=body.expected_policy_version,
        policy=body.policy, bounds=bounds, containers_release=containers_release,
    )
    return await get_cloudflare_capacity_status(db, caller)
