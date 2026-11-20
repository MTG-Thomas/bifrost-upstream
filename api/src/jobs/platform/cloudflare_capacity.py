"""Lease-fenced Cloudflare inventory and retirement jobs."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import case, delete, func, select, text, true
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobFailure,
    PlatformJobRequiresAction,
    define_platform_job,
)
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.platform_jobs import PlatformJob
from src.services.audit import emit_audit
from src.services.audit_context import ActorContext
from src.services.authorization.enforce import Caller, require_operation
from src.services.authorization.evaluator import GLOBAL
from src.services.platform_job_lease import LeaseLost, hold_lease
from src.services.platform_jobs import (
    ACTIVE_PLATFORM_JOB_STATUSES,
    enqueue_platform_job,
    find_active_platform_job,
)
from src.services.work_dispatch import (
    WAKE_CLOUDFLARE_CAPACITY,
    wake_pool_workers_after_commit,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    load_account_token,
    open_credential,
    seal_credential,
)
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
    CloudflareRestUnavailable,
    WorkersRestCleanup,
)

DISABLE_JOB = "capacity.cloudflare_disable"
RECONCILE_JOB = "capacity.cloudflare_reconcile"
ROTATE_JOB = "capacity.cloudflare_rotate_account_token"
RECONCILE_INTERVAL_SECONDS = 5 * 60
DRAIN_DEADLINE = timedelta(minutes=10)
CREATE_SETTLEMENT_WINDOW = timedelta(minutes=1)


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


class CloudflareReconcilePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    revision: int = Field(gt=0)


class CloudflareRotateAccountPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    installation_id: UUID
    previous_version: int = Field(gt=0)
    replacement_version: int = Field(gt=0)


def _resource_intent(resource: CloudflareOwnedResource) -> ResourceIntent:
    return ResourceIntent(
        ResourceTarget(resource.installation_id, resource.account_id, resource.scope, resource.kind, resource.target_name),
        resource.effect_id, resource.desired_revision, resource.artifact_digest,
    )


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


async def _management_token(db: AsyncSession, installation: CloudflareInstallation) -> SecretStr:
    try:
        return await load_account_token(db, installation)
    except ValueError:
        raise CloudflareManagementDenied from None


async def _cleanup_one(
    db: AsyncSession, row: CloudflareInstallation, adapter: OwnedResourceCleanup,
) -> Literal["complete", "progressed", "settling"]:
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
        return "complete"
    intent = _resource_intent(resource)
    token = await _management_token(db, row)
    if resource.state == "creating":
        observed = await observe_owned(adapter, api_token=token, intent=intent, recorded=None)
        if observed is None:
            now = datetime.now(UTC)
            if resource.observed_at is None:
                resource.observed_at = now
                return "progressed"
            if now - resource.observed_at < CREATE_SETTLEMENT_WINDOW:
                return "settling"
            await adapter.verify_absent(api_token=token, target=intent.target)
            resource.state = "absent"
            resource.observed_at = now
            return "progressed"
        resource.provider_id = observed.provider_id
        resource.provider_version = observed.provider_version
    if resource.provider_id is None or resource.provider_version is None:
        raise OwnershipConflict("Cloudflare cleanup has no exact provider receipt")
    if resource.state != "deleting":
        resource.state = "deleting"
        return "progressed"  # Commit the deletion intent before issuing DELETE.
    await resume_deletion(
        adapter, api_token=token,
        committed_deletion=ResourceObservation(intent, resource.provider_id, resource.provider_version),
        namespace_ids=frozenset(resource.namespace_ids.values()),
    )
    resource.state = "absent"
    resource.observed_at = datetime.now(UTC)
    return "progressed"


async def _settle_disable(
    context: PlatformJobContext, payload: CloudflareDisablePayload, adapter: OwnedResourceCleanup,
) -> dict[str, str]:
    while True:
        async with get_db_context() as db:
            await _hold_installation_lease(db, context, payload.installation_id)
            row = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
            if row is None or row.desired_revision != payload.revision or row.desired_state != payload.desired_state:
                return {"outcome": "superseded"}
            workers_remain = await cloudflare_workers_remain(db, row.id)
            if workers_remain:
                await _require_legacy_cleanup(db)
            if not workers_remain:
                cleanup = await _cleanup_one(db, row, adapter) if payload.action == "disconnect" else "complete"
                if cleanup == "progressed":
                    continue
                if cleanup == "complete":
                    if payload.action == "disconnect":
                        await _retire(db, row, payload)
                    await emit_audit(
                        db, "capacity.cloudflare_retired" if payload.action == "disconnect" else "capacity.cloudflare_scaled_in",
                        resource_type="cloudflare_installation", resource_id=row.id,
                        details={"revision": payload.revision, "job_id": str(context.job_id)},
                        actor_override=ActorContext(None, None, source="scheduler"),
                    )
                    return {"outcome": payload.desired_state}
            started_at = await db.scalar(select(PlatformJob.started_at).where(PlatformJob.id == context.job_id))
            if started_at is not None and datetime.now(UTC) >= started_at + DRAIN_DEADLINE:
                phase = "Wait for Cloudflare Creation to Settle" if not workers_remain else "Verify Cloudflare Worker Retirement"
                raise PlatformJobRequiresAction(phase, {"code": "CF_DRAIN_TIMEOUT"})
        await asyncio.sleep(2)


async def _require_legacy_cleanup(db: AsyncSession) -> None:
    workers = (await db.execute(select(
        ExecutionWorker.id, ExecutionWorker.capacity_scope, ExecutionWorker.capacity_resource_uid,
    ).where(
        ExecutionWorker.runtime_profile == "cloudflare-python-v1",
        ExecutionWorker.capacity_scope.is_not(None),
        ExecutionWorker.capacity_resource_deleted_at.is_(None),
        ~select(CloudflareWorkerReservation.worker_id).where(
            CloudflareWorkerReservation.worker_id == ExecutionWorker.id,
        ).exists(),
    ).order_by(ExecutionWorker.id).limit(21))).all()
    if workers:
        raise PlatformJobRequiresAction("Retire Legacy Cloudflare Workers", {
            "code": "CF_LEGACY_WORKERS_UNVERIFIED",
            "workers": [
                {"worker_id": str(worker.id), "scope": worker.capacity_scope, "resource_uid": worker.capacity_resource_uid}
                for worker in workers[:20]
            ],
            "more_workers": len(workers) > 20,
            "instructions": [
                "Find the original capacity endpoint in the previous Bifrost deployment configuration.",
                "Look up each worker by its exact worker ID and scope. Match its resource UID before releasing it.",
                "Use that provider's release operation and retain its exact deletion receipt. If the UID is missing or differs, stop and recover the original enrollment record.",
                "Have a platform operator reconcile physical deletion using these receipts, then retry the capacity action.",
            ],
        })


async def _retire(db: AsyncSession, row: CloudflareInstallation, payload: CloudflareDisablePayload) -> None:
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
    await db.execute(delete(CloudflareCapacityScope).where(CloudflareCapacityScope.installation_id == row.id))
    await retire_cloudflare_installation(db, installation_id=row.id, expected_revision=payload.revision)


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
    except CloudflareManagementDenied:
        raise PlatformJobRequiresAction("Reconnect Cloudflare", {"code": "CF_MANAGEMENT_CREDENTIAL"}) from None
    except CloudflareRestUnavailable:
        raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare REST Is Temporarily Unavailable", retryable=True) from None


CLOUDFLARE_DISABLE_DEFINITION = define_platform_job(DISABLE_JOB, CloudflareDisablePayload, _disable)


@dataclass(frozen=True, slots=True)
class _InventoryReceipt:
    intent: ResourceIntent
    provider_id: str | None
    provider_version: str | None
    state: str


async def _inventory_snapshot(
    db: AsyncSession, installation_id: UUID, *, revision: int | None = None,
) -> tuple[_InventoryReceipt, ...]:
    query = select(CloudflareOwnedResource).where(CloudflareOwnedResource.installation_id == installation_id)
    if revision is None:
        query = query.where(CloudflareOwnedResource.state.in_(("present", "deleting")))
    else:
        query = query.where(
            CloudflareOwnedResource.desired_revision == revision,
            CloudflareOwnedResource.scope == "workers",
            CloudflareOwnedResource.kind == "worker_script",
            CloudflareOwnedResource.state == "present",
        )
    rows = (await db.scalars(query.order_by(CloudflareOwnedResource.id))).all()
    return tuple(_InventoryReceipt(_resource_intent(row), row.provider_id, row.provider_version, row.state) for row in rows)


async def _observe_inventory(
    adapter: OwnedResourceCleanup, token: SecretStr, inventory: tuple[_InventoryReceipt, ...], *, require_present: bool,
) -> None:
    for receipt in inventory:
        if receipt.provider_id is None or receipt.provider_version is None:
            raise OwnershipConflict("Cloudflare Worker has no exact deployment receipt")
        observation = ResourceObservation(receipt.intent, receipt.provider_id, receipt.provider_version)
        remote = await observe_owned(adapter, api_token=token, intent=receipt.intent, recorded=observation)
        if remote is None and require_present:
            raise ObservationPending("An owned Cloudflare Worker has disappeared")


def _current_reconcile(installation: CloudflareInstallation | None, revision: int) -> bool:
    return (
        installation is not None and installation.retired_at is None
        and installation.desired_revision == revision and installation.desired_state == "enabled"
    )


async def _settle_reconcile(
    context: PlatformJobContext, payload: CloudflareReconcilePayload, adapter: OwnedResourceCleanup,
) -> dict[str, str]:
    issue = None
    token = None
    async with get_db_context() as db:
        await _hold_installation_lease(db, context, payload.installation_id)
        installation = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
        if installation is None or not _current_reconcile(installation, payload.revision):
            return {"outcome": "superseded"}
        token_version = installation.api_token_version
        admitted_revision = installation.admitted_revision
        inventory = await _inventory_snapshot(db, installation.id, revision=payload.revision)
        try:
            token = await _management_token(db, installation)
        except CloudflareManagementDenied:
            issue = ("Reconnect Cloudflare", "CF_MANAGEMENT_CREDENTIAL")

    if token is not None:
        try:
            roles = {receipt.intent.target.name.rsplit("-", 1)[-1] for receipt in inventory}
            if len(inventory) != 2 or roles != {"launcher", "capacity"}:
                raise ObservationPending("The admitted revision lacks a complete Workers deployment receipt")
            await _observe_inventory(adapter, token, inventory, require_present=True)
        except OwnershipConflict:
            issue = ("Verify Cloudflare Ownership", "CF_OWNERSHIP_CONFLICT")
        except ObservationPending:
            issue = ("Verify Cloudflare Deployment", "CF_DEPLOYMENT_UNVERIFIED")
        except CloudflareManagementDenied:
            issue = ("Reconnect Cloudflare", "CF_MANAGEMENT_CREDENTIAL")

    async with get_db_context() as db:
        await _hold_installation_lease(db, context, payload.installation_id)
        installation = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
        if (
            installation is None or not _current_reconcile(installation, payload.revision)
            or installation.api_token_version != token_version
            or installation.admitted_revision != admitted_revision
            or await _inventory_snapshot(db, installation.id, revision=payload.revision) != inventory
        ):
            return {"outcome": "superseded"}
        if issue is not None:
            installation.admitted_revision = None
            wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
    if issue is not None:
        raise PlatformJobRequiresAction(issue[0], {"code": issue[1]})
    return {"outcome": "verified"}


async def _reconcile(context: PlatformJobContext, parsed: BaseModel) -> dict[str, str]:
    payload = CloudflareReconcilePayload.model_validate(parsed)
    await context.report("Verifying Cloudflare Deployment")
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            return await _settle_reconcile(context, payload, WorkersRestCleanup(http))
    except CloudflareRestUnavailable:
        raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare REST Is Temporarily Unavailable", retryable=True) from None


CLOUDFLARE_RECONCILE_DEFINITION = define_platform_job(RECONCILE_JOB, CloudflareReconcilePayload, _reconcile)


async def enqueue_cloudflare_reconcile(
    db: AsyncSession, caller: Caller, *, installation_id: UUID, expected_revision: int,
) -> tuple[PlatformJob, bool]:
    require_operation(caller, "capacity.cloudflare_reconcile", GLOBAL)
    actor = caller.principal
    installation = await db.get(CloudflareInstallation, installation_id, with_for_update=True)
    if not _current_reconcile(installation, expected_revision):
        raise CloudflarePolicyConflict("Cloudflare reconciliation requires the current enabled deployment")
    payload = CloudflareReconcilePayload(installation_id=installation_id, revision=expected_revision)
    job, reused = await enqueue_platform_job(
        db, CLOUDFLARE_RECONCILE_DEFINITION, payload,
        dedupe_key=f"manual:{installation_id}:{expected_revision}:{actor.user_id}",
        resource_lock_key=f"cloudflare-installation:{installation_id}",
        organization_id=None, requested_by_user_id=actor.user_id, requested_by_email=actor.email,
        requested_by_name=actor.name, resource_type="cloudflare_installation", resource_id=str(installation_id),
        title="Verify Cloudflare Deployment", action_url=None,
    )
    if not reused:
        await emit_audit(
            db, "capacity.cloudflare_reconcile", resource_type="cloudflare_installation", resource_id=installation_id,
            details={"revision": expected_revision, "job_id": str(job.id)},
        )
    return job, reused


async def _rotate_account_token(context: PlatformJobContext, parsed: BaseModel) -> dict[str, str | int]:
    payload = CloudflareRotateAccountPayload.model_validate(parsed)
    await context.report("Validating Replacement Cloudflare API Token")
    denial = None
    credential_key = (payload.installation_id, "account_token", "", payload.replacement_version)
    async with get_db_context() as db:
        await _hold_installation_lease(db, context, payload.installation_id)
        installation = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
        if installation is None or installation.retired_at is not None:
            return {"outcome": "superseded"}
        if installation.api_token_version == payload.replacement_version:
            return {"outcome": "rotated", "credential_version": payload.replacement_version}
        candidate = await db.get(CloudflareCredential, credential_key)
        if candidate is None or candidate.ciphertext is None:
            raise PlatformJobRequiresAction("Submit a Replacement Cloudflare API Token", {"code": "CF_REPLACEMENT_MISSING"})
        if installation.api_token_version != payload.previous_version:
            candidate.ciphertext = None
            return {"outcome": "superseded"}
        account_id = installation.account_id
        desired_revision = installation.desired_revision
        ciphertext = candidate.ciphertext
        inventory = await _inventory_snapshot(db, installation.id)
        ref = CredentialRef(installation.id, account_id, "account_token", "", payload.replacement_version)
        bearer = open_credential(ref, candidate)

    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            adapter = WorkersRestCleanup(http)
            await adapter.verify_account(account_id, bearer)
            await _observe_inventory(adapter, bearer, inventory, require_present=False)
    except CloudflareManagementDenied:
        denial = ("Submit a Replacement Cloudflare API Token", "CF_MANAGEMENT_CREDENTIAL")
    except (ObservationPending, OwnershipConflict):
        denial = ("Verify Cloudflare Ownership", "CF_DEPLOYMENT_UNVERIFIED")
    except CloudflareRestUnavailable:
        raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare REST Is Temporarily Unavailable", retryable=True) from None

    async with get_db_context() as db:
        await _hold_installation_lease(db, context, payload.installation_id)
        installation = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
        candidate = await db.get(CloudflareCredential, credential_key, with_for_update=True)
        if (
            installation is None or installation.retired_at is not None
            or candidate is None or candidate.ciphertext != ciphertext
        ):
            return {"outcome": "superseded"}
        if (
            installation.api_token_version != payload.previous_version or installation.desired_revision != desired_revision
            or installation.account_id != account_id or await _inventory_snapshot(db, installation.id) != inventory
        ):
            candidate.ciphertext = None
            return {"outcome": "superseded"}
        if denial is not None:
            candidate.ciphertext = None
        else:
            installation.api_token_version = payload.replacement_version
            await emit_audit(
                db, "capacity.cloudflare_account_token_rotated", resource_type="cloudflare_installation",
                resource_id=installation.id, details={
                    "credential_version": payload.replacement_version, "job_id": str(context.job_id),
                }, actor_override=ActorContext(None, None, source="scheduler"),
            )
    if denial is not None:
        raise PlatformJobRequiresAction(denial[0], {"code": denial[1]})
    return {"outcome": "rotated", "credential_version": payload.replacement_version}


CLOUDFLARE_ROTATE_ACCOUNT_DEFINITION = define_platform_job(
    ROTATE_JOB, CloudflareRotateAccountPayload, _rotate_account_token,
)


async def enqueue_cloudflare_account_token_rotation(
    db: AsyncSession, caller: Caller, *, installation_id: UUID, expected_revision: int, replacement: SecretStr,
) -> tuple[PlatformJob, bool]:
    require_operation(caller, "capacity.cloudflare_rotate_account_token", GLOBAL)
    actor = caller.principal
    row = await db.get(CloudflareInstallation, installation_id, with_for_update=True, populate_existing=True)
    if row is None or row.retired_at is not None or row.desired_revision != expected_revision or row.api_token_version is None:
        raise CloudflarePolicyConflict("Cloudflare installation changed. Reload its current policy.")
    key = str(row.id)
    if await find_active_platform_job(db, ROTATE_JOB, key) is not None:
        raise CloudflarePolicyConflict("A Cloudflare API token rotation is already pending")
    versions = (
        CloudflareCredential.installation_id == row.id,
        CloudflareCredential.purpose == "account_token",
        CloudflareCredential.scope == "",
    )
    next_version = (await db.scalar(select(func.max(CloudflareCredential.version)).where(*versions)) or 0) + 1
    ref = CredentialRef(row.id, row.account_id, "account_token", "", next_version)
    db.add(seal_credential(ref, replacement, actor=str(actor.user_id), now=datetime.now(UTC)))
    payload = CloudflareRotateAccountPayload(
        installation_id=row.id, previous_version=row.api_token_version, replacement_version=next_version,
    )
    job, reused = await enqueue_platform_job(
        db, CLOUDFLARE_ROTATE_ACCOUNT_DEFINITION, payload, dedupe_key=key,
        resource_lock_key=f"cloudflare-installation:{row.id}",
        organization_id=None, requested_by_user_id=actor.user_id, requested_by_email=actor.email,
        requested_by_name=actor.name, resource_type="cloudflare_installation", resource_id=str(row.id),
        title="Rotate Cloudflare API Token", action_url=None,
    )
    await emit_audit(
        db, "capacity.cloudflare_rotate_account_token", resource_type="cloudflare_installation", resource_id=row.id,
        details={"credential_version": next_version, "job_id": str(job.id)},
    )
    return job, reused


async def schedule_cloudflare_reconcile(db: AsyncSession, *, now: datetime) -> None:
    """Schedule one durable inventory observation per admitted revision and interval."""
    installation = await db.scalar(select(CloudflareInstallation).where(
        CloudflareInstallation.retired_at.is_(None),
        CloudflareInstallation.admitted_revision.is_not(None),
    ).with_for_update(skip_locked=True))
    if installation is None:
        return
    active = await db.scalar(select(PlatformJob.id).where(
        PlatformJob.job_type == RECONCILE_JOB,
        PlatformJob.resource_id == str(installation.id),
        PlatformJob.status.in_(ACTIVE_PLATFORM_JOB_STATUSES),
    ).limit(1))
    if active is not None:
        return
    interval = int(now.timestamp()) // RECONCILE_INTERVAL_SECONDS
    payload = CloudflareReconcilePayload(installation_id=installation.id, revision=installation.desired_revision)
    await enqueue_platform_job(
        db, CLOUDFLARE_RECONCILE_DEFINITION, payload,
        dedupe_key=f"{installation.id}:{payload.revision}:{interval}", reuse_when=true(),
        resource_lock_key=f"cloudflare-installation:{installation.id}",
        organization_id=None, requested_by_user_id="system", requested_by_email="",
        requested_by_name="Bifrost", resource_type="cloudflare_installation", resource_id=str(installation.id),
        title="Verify Cloudflare Deployment", action_url=None,
    )


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
    job, reused = await enqueue_platform_job(
        db, CLOUDFLARE_DISABLE_DEFINITION, payload, dedupe_key=payload.key,
        resource_lock_key=f"cloudflare-installation:{installation_id}",
        organization_id=None, requested_by_user_id=actor.user_id, requested_by_email=actor.email,
        requested_by_name=actor.name, resource_type="cloudflare_installation", resource_id=str(installation_id),
        title="Scale In Cloudflare" if action == "scale_in" else "Disconnect Cloudflare",
        action_url=None,
    )
    if not reused:
        await emit_audit(
            db, f"capacity.cloudflare_{action}", resource_type="cloudflare_installation",
            resource_id=row.id, details={"revision": payload.revision, "job_id": str(job.id)},
        )
    return job, reused
