"""Platform operator controls for Cloudflare capacity."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Literal

from fastapi import APIRouter, HTTPException, Response, status
from sqlalchemy import select
from shared.models import (
    CloudflareAccountTokenRotation,
    CloudflareAccountTokenReferenceRotation,
    CloudflareActivateRequest,
    CloudflareCapacityAction,
    CloudflareCapacityStatus,
    CloudflarePolicyUpdate,
    CloudflarePreflight,
    CloudflarePreflightRequest,
    CloudflareReconcileAction,
    CloudflareStageCredentialRequest,
    CloudflareStagedCredential,
    CloudflareWorkerCredentialRotation,
)

from src.core.auth import CurrentActiveUser
from src.core.db_deps import DbSession
from src.core.exceptions import StructuredRequestRefusal
from src.core.rate_limit import RateLimiter
from src.jobs.platform.cloudflare_capacity import (
    enqueue_cloudflare_account_token_rotation,
    enqueue_cloudflare_disable,
    enqueue_cloudflare_reconcile,
    enqueue_cloudflare_worker_credential_rotation,
)
from src.jobs.platform.cloudflare_enable import enqueue_cloudflare_enable
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.models.contracts.platform_jobs import PlatformJobAccepted
from src.services.audit import emit_audit
from src.services.authorization.enforce import load_caller, require_operation
from src.services.authorization.evaluator import GLOBAL
from src.services.operation_catalog import operation_route
from src.services.worker_pool.cloudflare_activation import CloudflarePreflightFieldError, activate_cloudflare, preflight_cloudflare
from src.services.worker_pool.cloudflare_connection import configure_cloudflare
from src.services.worker_pool.cloudflare_credentials import StagedCloudflareCredentialUnavailable, stage_account_token
from src.services.worker_pool.cloudflare_policy import (
    CloudflareBudgetTooLow,
    CloudflareInstallationExists,
    CloudflarePolicyConflict,
    get_cloudflare_capacity_status,
)
from src.services.worker_pool.cloudflare_provisioning import ObservationPending, OwnershipConflict
from src.services.worker_pool.cloudflare_containers_release import ContainersReleaseFetchError, ContainersReleaseUnavailable
from src.services.worker_pool.cloudflare_release import WorkersReleaseUnavailable
from src.services.worker_pool.cloudflare_workers_rest import (
    CloudflareManagementDenied,
    CloudflareRestUnavailable,
    WorkersPaidUnverified,
)

router = APIRouter(prefix="/api/capacity/cloudflare", tags=["Capacity"])
preflight_limiter = RateLimiter(max_requests=10, window_seconds=60)


def _configuration_refusal(
    code: str, message: str, field: str, remedy: str, *, status_code: int = 422,
) -> StructuredRequestRefusal:
    return StructuredRequestRefusal("cloudflare_configuration_refused", message, {
        "code": code, "fields": {field: message}, "remedy": remedy,
    }, status_code=status_code)


@router.get("", response_model=CloudflareCapacityStatus, **operation_route("capacity.cloudflare_status"))
async def get_cloudflare_capacity(db: DbSession, user: CurrentActiveUser) -> CloudflareCapacityStatus:
    caller = await load_caller(db, user)
    require_operation(caller, "capacity.cloudflare_status", GLOBAL)
    return await get_cloudflare_capacity_status(db, caller)


@router.post(
    "/credential", response_model=CloudflareStagedCredential, status_code=status.HTTP_201_CREATED,
    **operation_route("capacity.cloudflare_credential_stage"),
)
async def stage_cloudflare_credential(
    body: CloudflareStageCredentialRequest, db: DbSession, user: CurrentActiveUser,
) -> CloudflareStagedCredential:
    caller = await load_caller(db, user)
    require_operation(caller, "capacity.cloudflare_credential_stage", GLOBAL)
    account_id = body.account_id
    installation_id = None
    if body.purpose == "rotate":
        target = CloudflareInstallation
        match = target.id == body.installation_id if body.installation_id is not None else target.account_id == account_id
        installation = await db.scalar(select(target).where(match, target.retired_at.is_(None)))
        if installation is None:
            raise HTTPException(404, "Active Cloudflare installation not found.")
        installation_id = installation.id
        account_id = installation.account_id
    assert account_id is not None
    ref, expires_at = await stage_account_token(
        db, actor_id=caller.principal.user_id, account_id=account_id, purpose=body.purpose, token=body.api_token,
        installation_id=installation_id,
    )
    await emit_audit(
        db, "capacity.cloudflare_credential_stage", resource_id=installation_id,
        details={"account_id": account_id, "purpose": body.purpose},
    )
    return CloudflareStagedCredential(credential_ref=ref, expires_at=expires_at)


@router.patch("", response_model=CloudflareCapacityStatus, **operation_route("capacity.cloudflare_configure"))
async def configure_cloudflare_capacity(
    body: CloudflarePolicyUpdate,
    db: DbSession,
    user: CurrentActiveUser,
) -> CloudflareCapacityStatus:
    caller = await load_caller(db, user)
    require_operation(caller, "capacity.cloudflare_configure", GLOBAL)
    with _configuration_errors(str(body.policy.monthly_limit_usd)):
        return await configure_cloudflare(db, caller, body)


@contextmanager
def _configuration_errors(
    monthly_limit: str | None, *, limit_field: str = "policy.monthly_limit_usd",
) -> Iterator[None]:
    try:
        yield
    except CloudflareBudgetTooLow as exc:
        message = f"The fleet envelope is {exc.envelope} USD/month. The submitted limit was {monthly_limit} USD/month."
        remedy = "Raise the limit or reduce the fleet size or lifetime."
        raise StructuredRequestRefusal("cloudflare_budget_refused", message, {
            "code": "CF_BUDGET_REFUSED",
            "fields": {limit_field: message},
            "fleet_envelope_usd": str(exc.envelope),
            "monthly_limit_usd": monthly_limit,
            "remedy": remedy,
        }) from None
    except WorkersReleaseUnavailable:
        raise HTTPException(503, "Install a qualified Workers release with metering ceilings.") from None
    except ContainersReleaseFetchError:
        raise HTTPException(503, "The signed Cloudflare Containers release is temporarily unavailable.") from None
    except ContainersReleaseUnavailable:
        raise HTTPException(503, "Install the matched, signed Cloudflare Containers release.") from None
    except WorkersPaidUnverified as exc:
        raise _configuration_refusal(
            "CF_WORKERS_PAID_UNVERIFIED", str(exc), "account_id",
            "Verify Workers Paid and confirm that the token can read the account entitlements.",
        ) from None
    except CloudflareManagementDenied:
        raise _configuration_refusal(
            "CF_MANAGEMENT_CREDENTIAL", "Cloudflare refused account or entitlement read access.", "api_token",
            "Grant Account Settings Read for the selected account.",
        ) from None
    except (CloudflareRestUnavailable, ObservationPending):
        raise HTTPException(503, "Cloudflare account verification is temporarily unavailable.") from None
    except StagedCloudflareCredentialUnavailable as exc:
        raise _configuration_refusal(
            "CF_STAGED_CREDENTIAL_UNAVAILABLE", str(exc), "credential_ref",
            "Stage the Cloudflare credential again, then run preflight.", status_code=409,
        ) from None
    except CloudflareInstallationExists as exc:
        raise _configuration_refusal(
            "CF_INSTALLATION_EXISTS", str(exc), "preflight_receipt",
            "Refresh and review the current Cloudflare installation.", status_code=409,
        ) from None
    except CloudflarePolicyConflict as exc:
        raise _configuration_refusal(
            "CF_REVIEW_REQUIRED", str(exc), "preflight_receipt",
            "Refresh capacity and review the current Cloudflare setup or policy.", status_code=409,
        ) from None
    except OwnershipConflict as exc:
        raise _configuration_refusal(
            "CF_HOSTNAME_OWNERSHIP_CONFLICT", str(exc), "zone_id",
            "Review the selected zone and any leftover DNS record from a previous installation, or choose another route.",
            status_code=409,
        ) from None
    except CloudflarePreflightFieldError as exc:
        raise _configuration_refusal(
            "CF_PREFLIGHT_FIELD_INVALID", str(exc), exc.field,
            "Review the highlighted Cloudflare setup field.",
        ) from None
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.post("/preflight", response_model=CloudflarePreflight, **operation_route("capacity.cloudflare_preflight"))
async def run_cloudflare_preflight(
    body: CloudflarePreflightRequest, db: DbSession, user: CurrentActiveUser,
) -> CloudflarePreflight:
    caller = await load_caller(db, user)
    require_operation(caller, "capacity.cloudflare_preflight", GLOBAL)
    await preflight_limiter.check("capacity.cloudflare_preflight", str(caller.principal.user_id))
    with _configuration_errors(str(body.policy.monthly_limit_usd) if body.policy else None):
        return await preflight_cloudflare(db, caller, body)


@router.post(
    "/activate", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    **operation_route("capacity.cloudflare_activate"),
)
async def activate_cloudflare_capacity(
    body: CloudflareActivateRequest, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    caller = await load_caller(db, user)
    require_operation(caller, "capacity.cloudflare_activate", GLOBAL)
    with _configuration_errors(str(body.monthly_limit_usd), limit_field="monthly_limit_usd"):
        job, reused = await activate_cloudflare(db, caller, body)
    response.headers["Location"] = f"/api/platform-jobs/{job.id}"
    return PlatformJobAccepted(job_id=job.id, status=job.status, notification_id=job.notification_id, reused=reused)


@router.post(
    "/enable", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    **operation_route("capacity.cloudflare_enable"),
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
    **operation_route("capacity.cloudflare_reconcile"),
)
async def reconcile_cloudflare_capacity(
    body: CloudflareReconcileAction, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    return await _reconcile(body, response, db, user, upgrade=False)


@router.post(
    "/upgrade", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    **operation_route("capacity.cloudflare_upgrade"),
)
async def upgrade_cloudflare_capacity(
    body: CloudflareReconcileAction, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    return await _reconcile(body, response, db, user, upgrade=True)


async def _reconcile(
    body: CloudflareReconcileAction, response: Response, db: DbSession, user: CurrentActiveUser,
    *, upgrade: bool,
) -> PlatformJobAccepted:
    caller = await load_caller(db, user)
    try:
        job, reused = await enqueue_cloudflare_reconcile(
            db, caller, installation_id=body.installation_id, expected_revision=body.expected_revision, upgrade=upgrade,
            reset_release_fence=body.reset_release_fence,
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
    **operation_route("capacity.cloudflare_scale_in"),
)
async def scale_in_cloudflare_capacity(
    body: CloudflareCapacityAction, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    return await _disable(body, response, db, user, "scale_in")


@router.post(
    "/disconnect", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    **operation_route("capacity.cloudflare_disconnect"),
)
async def disconnect_cloudflare_capacity(
    body: CloudflareCapacityAction, response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    return await _disable(body, response, db, user, "disconnect")


@router.post(
    "/rotate", response_model=PlatformJobAccepted, status_code=status.HTTP_202_ACCEPTED,
    **operation_route("capacity.cloudflare_rotate_account_token"),
)
async def rotate_cloudflare_account_token(
    body: CloudflareAccountTokenRotation | CloudflareAccountTokenReferenceRotation | CloudflareWorkerCredentialRotation,
    response: Response, db: DbSession, user: CurrentActiveUser,
) -> PlatformJobAccepted:
    caller = await load_caller(db, user)
    try:
        if isinstance(body, (CloudflareAccountTokenRotation, CloudflareAccountTokenReferenceRotation)):
            job, reused = await enqueue_cloudflare_account_token_rotation(
                db, caller, installation_id=body.installation_id, expected_revision=body.expected_revision,
                replacement=body.api_token if isinstance(body, CloudflareAccountTokenRotation) else body.credential_ref,
            )
        else:
            job, reused = await enqueue_cloudflare_worker_credential_rotation(
                db, caller, installation_id=body.installation_id, expected_revision=body.expected_revision,
                credential=body.credential,
            )
    except CloudflarePolicyConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc
    response.headers["Location"] = f"/api/platform-jobs/{job.id}"
    return PlatformJobAccepted(job_id=job.id, status=job.status, notification_id=job.notification_id, reused=reused)
