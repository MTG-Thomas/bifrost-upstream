"""Platform operator controls for Cloudflare capacity."""

from typing import Annotated, Literal

import httpx
from fastapi import APIRouter, Body, HTTPException, Response, status
from shared.models import (
    CloudflareAccountTokenRotation,
    CloudflareCapacityAction,
    CloudflareCapacityStatus,
    CloudflareConnectRequest,
    CloudflarePolicyUpdate,
)

from src.core.auth import CurrentActiveUser
from src.core.db_deps import DbSession
from src.jobs.platform.cloudflare_capacity import (
    enqueue_cloudflare_account_token_rotation,
    enqueue_cloudflare_disable,
    enqueue_cloudflare_reconcile,
)
from src.jobs.platform.cloudflare_enable import enqueue_cloudflare_enable
from src.models.contracts.platform_jobs import PlatformJobAccepted
from src.services.audit import emit_audit
from src.services.authorization.enforce import load_caller, require_operation
from src.services.authorization.evaluator import GLOBAL
from src.services.worker_pool.cloudflare_enable_dependencies import (
    WorkersPaidUnverified,
    require_workers_paid,
)
from src.services.worker_pool.cloudflare_policy import (
    CONSENT_TEXT_VERSION,
    CloudflareInstallationExists,
    CloudflarePolicyConflict,
    create_cloudflare_installation,
    get_cloudflare_capacity_status,
    release_meter_bounds,
    save_cloudflare_policy,
)
from src.services.worker_pool.cloudflare_provisioning import ObservationPending
from src.services.worker_pool.cloudflare_release import (
    WorkersReleaseUnavailable,
    load_workers_release,
)
from src.services.worker_pool.cloudflare_workers_rest import (
    CloudflareManagementDenied,
    CloudflareRestUnavailable,
    WorkersRestCleanup,
)

router = APIRouter(prefix="/api/capacity/cloudflare", tags=["Capacity"])


@router.get("", response_model=CloudflareCapacityStatus, operation_id="capacity.cloudflare_status")
async def get_cloudflare_capacity(db: DbSession, user: CurrentActiveUser) -> CloudflareCapacityStatus:
    caller = await load_caller(db, user)
    require_operation(caller, "capacity.cloudflare_status", GLOBAL)
    return await get_cloudflare_capacity_status(db, caller)


@router.patch("", response_model=CloudflareCapacityStatus, operation_id="capacity.cloudflare_configure")
async def configure_cloudflare_capacity(
    body: Annotated[CloudflareConnectRequest | CloudflarePolicyUpdate, Body(discriminator="action")],
    db: DbSession,
    user: CurrentActiveUser,
) -> CloudflareCapacityStatus:
    caller = await load_caller(db, user)
    require_operation(caller, "capacity.cloudflare_configure", GLOBAL)
    try:
        bounds = release_meter_bounds(load_workers_release())
        if isinstance(body, CloudflareConnectRequest):
            if body.consent_version != CONSENT_TEXT_VERSION:
                raise CloudflarePolicyConflict("Review the current Cloudflare consent text.")
            async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
                await WorkersRestCleanup(http).verify_workers_entitlement(body.account_id, body.api_token)
            await require_workers_paid(body.account_id)
            installation_id = await create_cloudflare_installation(
                db, account_id=body.account_id, worker_ingress_origin=body.worker_ingress_origin,
                consent_version=body.consent_version, actor_id=caller.principal.user_id,
                verified_api_token=body.api_token, policy=body.policy, bounds=bounds,
            )
            await emit_audit(
                db, "capacity.cloudflare_connect", resource_type="cloudflare_installation", resource_id=installation_id,
                details={"account_id": body.account_id, "consent_version": body.consent_version},
            )
        else:
            revision = await save_cloudflare_policy(
                db, installation_id=body.installation_id, expected_revision=body.expected_revision,
                policy=body.policy, bounds=bounds,
            )
            if revision != body.expected_revision:
                await emit_audit(db, "capacity.cloudflare_policy_updated", resource_type="cloudflare_installation",
                                 resource_id=body.installation_id, details={"revision": revision})
    except WorkersReleaseUnavailable:
        raise HTTPException(503, "Install a qualified Workers release with metering ceilings.") from None
    except (WorkersPaidUnverified, CloudflareManagementDenied):
        raise HTTPException(422, "Workers Paid and Workers entitlement must be verified for the selected account.") from None
    except (CloudflareRestUnavailable, ObservationPending):
        raise HTTPException(503, "Cloudflare account verification is temporarily unavailable.") from None
    except (CloudflareInstallationExists, CloudflarePolicyConflict) as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return await get_cloudflare_capacity_status(db, caller)


@router.post(
    "/enable", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    operation_id="capacity.cloudflare_enable",
)
async def enable_cloudflare_capacity(
    body: CloudflareCapacityAction, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    caller = await load_caller(db, user)
    try:
        job, reused = await enqueue_cloudflare_enable(
            db, caller, installation_id=body.installation_id, expected_revision=body.expected_revision,
        )
    except CloudflarePolicyConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    response.headers["Location"] = f"/api/platform-jobs/{job.id}"
    return PlatformJobAccepted(job_id=job.id, status=job.status, notification_id=job.notification_id, reused=reused)


@router.post(
    "/reconcile", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    operation_id="capacity.cloudflare_reconcile",
)
async def reconcile_cloudflare_capacity(
    body: CloudflareCapacityAction, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    caller = await load_caller(db, user)
    try:
        job, reused = await enqueue_cloudflare_reconcile(
            db, caller, installation_id=body.installation_id, expected_revision=body.expected_revision,
        )
    except CloudflarePolicyConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    response.headers["Location"] = f"/api/platform-jobs/{job.id}"
    return PlatformJobAccepted(job_id=job.id, status=job.status, notification_id=job.notification_id, reused=reused)


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


@router.post(
    "/rotate", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    operation_id="capacity.cloudflare_rotate_account_token",
)
async def rotate_cloudflare_account_token(
    body: CloudflareAccountTokenRotation, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    caller = await load_caller(db, user)
    try:
        job, reused = await enqueue_cloudflare_account_token_rotation(
            db, caller, installation_id=body.installation_id, expected_revision=body.expected_revision,
            replacement=body.api_token,
        )
    except CloudflarePolicyConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    response.headers["Location"] = f"/api/platform-jobs/{job.id}"
    return PlatformJobAccepted(job_id=job.id, status=job.status, notification_id=job.notification_id, reused=reused)
