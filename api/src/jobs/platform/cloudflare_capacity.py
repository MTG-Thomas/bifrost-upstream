"""Lease-fenced scale-in and disconnect settlement."""

import asyncio
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobRequiresAction,
    define_platform_job,
)
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.models.orm.platform_jobs import PlatformJob
from src.services.authorization.enforce import Caller, require_permission
from src.services.platform_job_lease import LeaseLost, hold_lease
from src.services.platform_jobs import enqueue_platform_job, find_active_platform_job
from src.services.worker_pool.cloudflare_policy import (
    CloudflarePolicyConflict,
    cloudflare_workers_remain,
    retire_cloudflare_installation,
)

DISABLE_JOB = "capacity.cloudflare_disable"


class CloudflareDisablePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    revision: int = Field(gt=0)
    action: Literal["scale_in", "disconnect"]

    @property
    def desired_state(self) -> Literal["scaled_in", "disconnected"]:
        return "scaled_in" if self.action == "scale_in" else "disconnected"

    @property
    def key(self) -> str:
        return f"{self.installation_id}:{self.revision}:{self.action}"


async def _disable(context: PlatformJobContext, parsed: BaseModel) -> dict[str, str]:
    payload = CloudflareDisablePayload.model_validate(parsed)
    await context.report("Draining Cloudflare Capacity")
    while True:
        async with get_db_context() as db:
            try:
                await hold_lease(db, context.job_id, context.lease_token)
            except LeaseLost as exc:
                raise PlatformJobCancelled from exc
            row = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
            if row is None or row.desired_revision != payload.revision or row.desired_state != payload.desired_state:
                return {"outcome": "superseded"}
            if row.admitted_revision is not None:
                raise PlatformJobRequiresAction("Cloudflare Admission Still Open", {"code": "CF_ADMISSION_OPEN"})
            if not await cloudflare_workers_remain(db, row.id):
                if payload.action == "disconnect":
                    try:
                        await retire_cloudflare_installation(db, installation_id=row.id, expected_revision=payload.revision)
                    except CloudflarePolicyConflict as exc:
                        raise PlatformJobRequiresAction(
                            "Cloudflare Cleanup Pending", {"code": "CF_CLEANUP_PENDING"},
                        ) from exc
                return {"outcome": payload.desired_state}
        await asyncio.sleep(2)


CLOUDFLARE_DISABLE_DEFINITION = define_platform_job(DISABLE_JOB, CloudflareDisablePayload, _disable)


async def enqueue_cloudflare_disable(
    db: AsyncSession, caller: Caller, *, installation_id: UUID, expected_revision: int,
    action: Literal["scale_in", "disconnect"],
) -> tuple[PlatformJob, bool]:
    require_permission(caller, "platform.readwrite", None)
    actor = caller.principal
    payload = CloudflareDisablePayload(installation_id=installation_id, revision=expected_revision + 1, action=action)
    row = await db.get(CloudflareInstallation, installation_id, with_for_update=True, populate_existing=True)
    if row is None or row.retired_at is not None:
        raise CloudflarePolicyConflict("Cloudflare installation is unavailable")
    if row.desired_revision != expected_revision:
        if row.desired_revision == payload.revision and row.desired_state == payload.desired_state:
            existing = await find_active_platform_job(
                db, DISABLE_JOB, payload.key, reuse_when=PlatformJob.requested_by_user_id == str(actor.user_id),
            )
            if existing is not None:
                return existing, True
        raise CloudflarePolicyConflict("Cloudflare installation changed. Reload its current policy.")
    if row.desired_state == "disconnected" and action == "scale_in":
        raise CloudflarePolicyConflict("Cloudflare disconnect is already pending")
    row.admitted_revision = None
    row.desired_state = payload.desired_state
    row.desired_revision = payload.revision
    return await enqueue_platform_job(
        db, CLOUDFLARE_DISABLE_DEFINITION, payload, dedupe_key=payload.key,
        resource_lock_key=f"cloudflare-installation:{installation_id}",
        organization_id=None, requested_by_user_id=actor.user_id, requested_by_email=actor.email,
        requested_by_name=actor.name, resource_type="cloudflare_installation", resource_id=str(installation_id),
        title="Scale In Cloudflare" if action == "scale_in" else "Disconnect Cloudflare",
        action_url=None,
    )
