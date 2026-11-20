"""Platform operator controls for Cloudflare capacity."""

from typing import Literal

from fastapi import APIRouter, HTTPException, Response, status
from shared.models import CloudflareCapacityAction, CloudflareCapacityStatus

from src.core.auth import CurrentActiveUser
from src.core.db_deps import DbSession
from src.jobs.platform.cloudflare_capacity import enqueue_cloudflare_disable
from src.models.contracts.platform_jobs import PlatformJobAccepted
from src.services.authorization.enforce import authorize_operation, load_caller
from src.services.authorization.evaluator import GLOBAL
from src.services.worker_pool.cloudflare_policy import (
    CloudflarePolicyConflict,
    get_cloudflare_capacity_status,
)

router = APIRouter(prefix="/api/capacity/cloudflare", tags=["Capacity"])


@router.get("", response_model=CloudflareCapacityStatus, operation_id="capacity.cloudflare_status")
async def get_cloudflare_capacity(db: DbSession, user: CurrentActiveUser) -> CloudflareCapacityStatus:
    await authorize_operation(db, user, "capacity.cloudflare_status", GLOBAL)
    return await get_cloudflare_capacity_status(db)


async def _disable(
    body: CloudflareCapacityAction, response: Response, db: DbSession,
    user: CurrentActiveUser, action: Literal["scale_in", "disconnect"],
) -> PlatformJobAccepted:
    caller = await load_caller(db, user)
    try:
        job, reused = await enqueue_cloudflare_disable(
            db, caller, installation_id=body.installation_id, expected_revision=body.expected_revision, action=action,
        )
    except CloudflarePolicyConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    response.headers["Location"] = f"/api/platform-jobs/{job.id}"
    return PlatformJobAccepted(job_id=job.id, status=job.status, notification_id=job.notification_id, reused=reused)


@router.post(
    "/scale-in", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    operation_id="capacity.cloudflare_scale_in",
)
async def scale_in_cloudflare_capacity(
    body: CloudflareCapacityAction, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    return await _disable(body, response, db, user, "scale_in")


@router.post(
    "/disconnect", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    operation_id="capacity.cloudflare_disconnect",
)
async def disconnect_cloudflare_capacity(
    body: CloudflareCapacityAction, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    return await _disable(body, response, db, user, "disconnect")
