"""Lease-fenced scale-in and disconnect settlement."""

import asyncio
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import case, delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobRequiresAction,
    define_platform_job,
)
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareInstallation,
    CloudflareOwnedResource,
)
from src.models.orm.platform_jobs import PlatformJob
from src.services.authorization.enforce import Caller, require_operation
from src.services.authorization.evaluator import GLOBAL
from src.services.platform_job_lease import LeaseLost, hold_lease
from src.services.platform_jobs import enqueue_platform_job, find_active_platform_job
from src.services.work_dispatch import (
    WAKE_CLOUDFLARE_CAPACITY,
    wake_pool_workers_after_commit,
)
from src.services.worker_pool.cloudflare_credentials import load_account_token
from src.services.worker_pool.cloudflare_policy import (
    CloudflarePolicyConflict,
    cloudflare_workers_remain,
    retire_cloudflare_installation,
)
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    OwnedResourceCleanup,
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
    observe_owned,
    resume_deletion,
)
from src.services.worker_pool.cloudflare_workers_rest import (
    CloudflareManagementDenied,
    WorkersRestCleanup,
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


async def _hold_installation_lease(
    db: AsyncSession, context: PlatformJobContext, installation_id: UUID,
) -> None:
    try:
        await hold_lease(db, context.job_id, context.lease_token)
    except LeaseLost as exc:
        raise PlatformJobCancelled from exc
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"bifrost:cloudflare:provisioning:{installation_id}"},
    )


async def _cleanup_one(
    db: AsyncSession, row: CloudflareInstallation, adapter: OwnedResourceCleanup,
) -> bool:
    # Newer generations first, then withdraw the capacity entry point before
    # retiring launcher and runtime dependencies. Workers scripts implicitly
    # own their namespaces. The adapter checks for those before recording absence.
    resource = await db.scalar(select(CloudflareOwnedResource).where(
        CloudflareOwnedResource.installation_id == row.id,
        CloudflareOwnedResource.state != "absent",
    ).order_by(
        CloudflareOwnedResource.desired_revision.desc(),
        case(
            (CloudflareOwnedResource.target_name.endswith("-capacity"), 0),
            (CloudflareOwnedResource.target_name.endswith("-launcher"), 1),
            else_=2,
        ),
        CloudflareOwnedResource.target_name,
    ).limit(1).with_for_update())
    if resource is None:
        return False
    intent = ResourceIntent(
        ResourceTarget(resource.installation_id, resource.account_id, resource.scope, resource.kind, resource.target_name),
        resource.effect_id, resource.desired_revision, resource.artifact_digest,
    )
    token = await load_account_token(db, row)
    if resource.state == "creating":
        observed = await observe_owned(adapter, api_token=token, intent=intent, recorded=None)
        if observed is None:
            raise ObservationPending("A previous Cloudflare creation could still be in flight")
        resource.provider_id = observed.provider_id
        resource.provider_version = observed.provider_version
    if resource.provider_id is None or resource.provider_version is None:
        raise OwnershipConflict("Cloudflare cleanup has no exact provider receipt")
    if resource.state != "deleting":
        resource.state = "deleting"
        return True  # Commit the deletion intent before issuing DELETE.
    await resume_deletion(
        adapter, api_token=token,
        committed_deletion=ResourceObservation(intent, resource.provider_id, resource.provider_version),
    )
    resource.state = "absent"
    resource.observed_at = datetime.now(UTC)
    return True


async def _settle_disable(
    context: PlatformJobContext, payload: CloudflareDisablePayload, adapter: OwnedResourceCleanup,
) -> dict[str, str]:
    while True:
        async with get_db_context() as db:
            await _hold_installation_lease(db, context, payload.installation_id)
            row = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
            if row is None or row.desired_revision != payload.revision or row.desired_state != payload.desired_state:
                return {"outcome": "superseded"}
            if row.admitted_revision is not None:
                raise PlatformJobRequiresAction("Cloudflare Admission Still Open", {"code": "CF_ADMISSION_OPEN"})
            if not await cloudflare_workers_remain(db, row.id):
                if payload.action == "disconnect":
                    if await _cleanup_one(db, row, adapter):
                        continue
                    unverified_scope = await db.scalar(select(CloudflareCapacityScope.scope).where(
                        CloudflareCapacityScope.installation_id == row.id,
                        ~select(CloudflareOwnedResource.id).where(
                            CloudflareOwnedResource.installation_id == row.id,
                            CloudflareOwnedResource.scope == CloudflareCapacityScope.scope,
                            CloudflareOwnedResource.desired_revision == CloudflareCapacityScope.deployment_revision,
                            CloudflareOwnedResource.kind == "worker_script",
                            CloudflareOwnedResource.target_name.endswith("-capacity"),
                            CloudflareOwnedResource.state == "absent",
                            CloudflareOwnedResource.observed_at.is_not(None),
                        ).correlate(CloudflareCapacityScope).exists(),
                    ).limit(1))
                    if unverified_scope is not None:
                        raise ObservationPending("Cloudflare scope absence is unverified")
                    await db.execute(delete(CloudflareCapacityScope).where(
                        CloudflareCapacityScope.installation_id == row.id,
                    ))
                    await retire_cloudflare_installation(db, installation_id=row.id, expected_revision=payload.revision)
                return {"outcome": payload.desired_state}
        await asyncio.sleep(2)


async def _disable(context: PlatformJobContext, parsed: BaseModel) -> dict[str, str]:
    payload = CloudflareDisablePayload.model_validate(parsed)
    await context.report("Draining Cloudflare Capacity")
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            return await _settle_disable(context, payload, WorkersRestCleanup(http))
    except OwnershipConflict:
        raise PlatformJobRequiresAction("Verify Cloudflare Ownership", {"code": "CF_OWNERSHIP_CONFLICT"}) from None
    except (ObservationPending, CloudflarePolicyConflict):
        raise PlatformJobRequiresAction("Cloudflare Cleanup Pending", {"code": "CF_CLEANUP_PENDING"}) from None
    except (CloudflareManagementDenied, ValueError):
        raise PlatformJobRequiresAction("Reconnect Cloudflare", {"code": "CF_MANAGEMENT_CREDENTIAL"}) from None


CLOUDFLARE_DISABLE_DEFINITION = define_platform_job(DISABLE_JOB, CloudflareDisablePayload, _disable)


async def enqueue_cloudflare_disable(
    db: AsyncSession, caller: Caller, *, installation_id: UUID, expected_revision: int,
    action: Literal["scale_in", "disconnect"],
) -> tuple[PlatformJob, bool]:
    if action == "scale_in":
        require_operation(caller, "capacity.cloudflare_scale_in", GLOBAL)
    else:
        require_operation(caller, "capacity.cloudflare_disconnect", GLOBAL)
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
    wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
    return await enqueue_platform_job(
        db, CLOUDFLARE_DISABLE_DEFINITION, payload, dedupe_key=payload.key,
        resource_lock_key=f"cloudflare-installation:{installation_id}",
        organization_id=None, requested_by_user_id=actor.user_id, requested_by_email=actor.email,
        requested_by_name=actor.name, resource_type="cloudflare_installation", resource_id=str(installation_id),
        title="Scale In Cloudflare" if action == "scale_in" else "Disconnect Cloudflare",
        action_url=None,
    )
