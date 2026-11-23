"""Lease-fenced Cloudflare inventory and retirement jobs."""

import asyncio
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import case, delete, func, select, text, true, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobFailure,
    PlatformJobRequiresAction,
    define_platform_job,
)
from src.models.enums import ExecutionStatus
from src.models.orm.cloudflare_approvals import CloudflareApproval
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
    CloudflareWorkflowSlot,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.executions import Execution
from src.models.orm.platform_jobs import PlatformJob
from src.models.orm.work_dispatches import WorkDispatch
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
from src.services.worker_pool import cloudflare_tunnel
from src.services.worker_pool.cloudflare_containers_admission import LINUX_REVIEW_VERSION
from src.services.worker_pool.cloudflare_containers_egress import read_verified_egress
from src.services.worker_pool.cloudflare_containers_release import ContainersReleaseUnavailable
from src.services.worker_pool.cloudflare_containers_rest import ContainerRegistry, ContainersRest
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    consume_staged_token,
    load_account_token,
    open_credential,
    purge_expired_staged_tokens,
    seal_credential,
)
from src.services.worker_pool.cloudflare_policy import (
    CloudflarePolicyConflict,
    cloudflare_workers_remain,
    retire_cloudflare_installation,
    set_workers_release_floor,
    workers_release_error,
    workers_release_floor,
    workers_upgrade_available,
)
from src.services.worker_pool.cloudflare_provisioning import (
    DeletionSettling,
    ObservationCheck,
    ObservationPending,
    ObservationSettling,
    OwnedResourceCleanup,
    OwnershipCheck,
    OwnershipConflict,
    OwnershipResource,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
    observe_owned,
    resume_deletion,
)
from src.services.worker_pool.cloudflare_release import (
    WorkersReleaseOrder,
    installed_workers_release_digest,
    installed_workers_release_order,
)
from src.services.worker_pool.cloudflare_slots import delete_workflow_slots
from src.services.worker_pool.cloudflare_spend import (
    clear_destroyed_ledger_receipts,
    cloudflare_estimated_usage,
    finalize_destroyed_launcher_meter,
    managed_worker_stop_unverified,
    read_workers_usage,
    usage_period,
)
from src.services.worker_pool.cloudflare_tunnel_rest import IngressMismatch
from src.services.worker_pool.cloudflare_workers_rest import (
    CloudflareManagementDenied,
    CloudflareRestUnavailable,
    WorkersPaidUnverified,
    WorkersRestCleanup,
    WorkersWriteUnverified,
)

DISABLE_JOB = "capacity.cloudflare_disable"
RECONCILE_JOB = "capacity.cloudflare_reconcile"
ROTATE_JOB = "capacity.cloudflare_rotate_account_token"
ROTATE_WORKER_JOB = "capacity.cloudflare_rotate_worker_credential"
RECONCILE_INTERVAL_SECONDS = 5 * 60
RECONCILE_MAX_BACKOFF_SECONDS = 60 * 60
SELF_HEALING_RECONCILE_CODES = frozenset({
    "CF_DRAIN_TIMEOUT", "CF_CLEANUP_PENDING", "CF_RELEASE_MISMATCH",
    "CF_PROVIDER_UNAVAILABLE", "CF_SPEND_UNVERIFIED", "CF_CONNECTOR_UNAVAILABLE",
})
PINNED_UPGRADE_RUNNER_CODES = frozenset({
    "unsupported_payload_version", "runner_shutdown", "runner_lost", "runner_exited", "timeout",
})
RELEASE_FENCE_REMEDY = "Use Reconcile with Reset Release Fence to accept this package."
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
    # Remove the v1 payload bridge one release after v2 ships.
    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    revision: int = Field(gt=0)
    operation: Literal["reconcile", "upgrade"] = "reconcile"


class CloudflareReconcilePayloadV2(CloudflareReconcilePayload):
    reset_release_fence: bool = False


class CloudflareRotateAccountPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    installation_id: UUID
    previous_version: int = Field(gt=0)
    replacement_version: int = Field(gt=0)


class CloudflareRotateWorkerPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    installation_id: UUID
    revision: int = Field(gt=0)
    credential: Literal["control_root", "scope_bearer", "connector_token"]
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


def _next_cleanup_resource(
    installation_id: UUID, *, before_revision: int | None = None,
    stranded_scope: tuple[str, int] | None = None,
    workers_only: bool = False,
):
    # Newer generations first, then withdraw the capacity entry point before
    # retiring launcher and runtime dependencies. Workers scripts implicitly
    # own their namespaces. The adapter checks for those before recording absence.
    containers = (CloudflareOwnedResource.scope == "containers") & (
        CloudflareOwnedResource.kind.in_(("container_application", "worker_script", "container_image"))
    )
    query = select(CloudflareOwnedResource).where(
        CloudflareOwnedResource.installation_id == installation_id,
        CloudflareOwnedResource.state != "absent",
        (CloudflareOwnedResource.scope == "workers") | containers,
    )
    if workers_only:
        query = query.where(CloudflareOwnedResource.scope == "workers")
    if before_revision is not None:
        query = query.where(CloudflareOwnedResource.desired_revision < before_revision)
    if stranded_scope is not None:
        scope, revision = stranded_scope
        query = query.where(CloudflareOwnedResource.scope == scope, CloudflareOwnedResource.desired_revision == revision)
    return query.order_by(
        CloudflareOwnedResource.desired_revision.desc(),
        case(
            (CloudflareOwnedResource.kind == "container_application", 0),
            (CloudflareOwnedResource.target_name.endswith("-capacity"), 1),
            (CloudflareOwnedResource.target_name.endswith("-launcher"), 2),
            (CloudflareOwnedResource.kind == "container_image", 4),
            else_=3,
        ),
        CloudflareOwnedResource.target_name,
    ).limit(1)


async def _next_cleanup_is_image(db: AsyncSession, installation_id: UUID, *, before_revision: int | None = None) -> bool:
    return await db.scalar(_next_cleanup_resource(installation_id, before_revision=before_revision)
                           .with_only_columns(CloudflareOwnedResource.kind)) == "container_image"


async def _cleanup_one(
    db: AsyncSession, row: CloudflareInstallation, adapter: OwnedResourceCleanup, *, before_revision: int | None = None,
    stranded_scope: tuple[str, int] | None = None,
    workers_only: bool = False,
    container_adapter: ContainersRest | None = None,
) -> Literal["complete", "progressed", "settling"]:
    resource = await db.scalar(_next_cleanup_resource(
        row.id, before_revision=before_revision, stranded_scope=stranded_scope, workers_only=workers_only,
    ).with_for_update())
    if resource is None:
        return "complete"
    intent = _resource_intent(resource)
    token = await _management_token(db, row)
    if resource.kind == "container_application":
        if container_adapter is None:
            raise ObservationPending("Container cleanup requires the Container inventory adapter",
                                     OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_INVENTORY)
        return await _cleanup_container_application(container_adapter, token, resource, intent)
    if resource.kind == "container_image":
        if container_adapter is None:
            raise ObservationPending("Container cleanup requires the registry adapter",
                                     OwnershipResource.CONTAINER_IMAGE, ObservationCheck.CONTAINER_INVENTORY)
        if resource.state != "deleting" and _container_create_may_exist(resource):
            resource.state = "deleting"
            return "progressed"
        return await _cleanup_container_image(ContainerRegistry(container_adapter), token, resource, intent)
    if resource.state == "creating":
        recorded = (
            ResourceObservation(intent, resource.provider_id, resource.provider_version)
            if resource.provider_id and resource.provider_version else None
        )
        observed = await observe_owned(adapter, api_token=token, intent=intent, recorded=recorded)
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
        raise OwnershipConflict(
            "Cloudflare cleanup has no exact provider receipt",
            OwnershipResource.WORKER_SCRIPT, OwnershipCheck.RECEIPT_MISSING,
        )
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
    if resource.target_name.endswith("-capacity") and "WorkerLedger" in resource.namespace_ids:
        await clear_destroyed_ledger_receipts(db, row.id, resource.scope, resource.desired_revision)
    return "progressed"


def _container_create_may_exist(resource: CloudflareOwnedResource) -> bool:
    return (
        resource.provider_id is not None
        or resource.provider_details.get("creation_sent") is True
        or resource.provider_details.get("creation_ambiguous") is True
    )


def _container_create_absence_settled(resource: CloudflareOwnedResource, now: datetime) -> bool:
    # The first complete empty read after a possible send starts the clock.
    # A preflight read or the age of the send cannot substitute for it.
    if resource.observed_at is None or resource.provider_details.get("creation_empty_observed") is not True:
        resource.observed_at = now
        resource.provider_details = {**resource.provider_details, "creation_empty_observed": True}
        return False
    return now - resource.observed_at >= CREATE_SETTLEMENT_WINDOW


async def _cleanup_container_application(
    adapter: ContainersRest, token: SecretStr, resource: CloudflareOwnedResource, intent: ResourceIntent,
) -> Literal["progressed", "settling"]:
    namespace = resource.namespace_ids.get("ContainerLauncher")
    if not namespace or (resource.provider_version is not None and resource.provider_version != namespace):
        raise OwnershipConflict("Container namespace does not match the journal",
                                OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISMATCH)
    observed = await adapter.observe_application(
        token, intent, namespace, recorded_id=resource.provider_id, for_deletion=True,
    )
    now = datetime.now(UTC)
    if resource.state == "creating":
        may_exist = _container_create_may_exist(resource)
        if observed is None:
            if may_exist and not _container_create_absence_settled(resource, now):
                return "settling"
        elif not may_exist:
            raise OwnershipConflict("Container name is occupied before its creation effect",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.TARGET_OCCUPIED)
        else:
            resource.provider_id = observed.provider_id
            resource.provider_version = observed.provider_version
    elif not resource.provider_id or not resource.provider_version:
        raise OwnershipConflict("Container deletion requires its provider receipt",
                                OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RECEIPT_MISSING)
    if observed is not None:
        if resource.state != "deleting":
            resource.state = "deleting"
            return "progressed"
        await adapter.delete_application(token, observed)
    resource.state = "absent"
    resource.observed_at = now
    return "progressed"


async def _cleanup_container_image(
    registry: ContainerRegistry, token: SecretStr, resource: CloudflareOwnedResource, intent: ResourceIntent,
) -> Literal["progressed", "settling"]:
    now = datetime.now(UTC)
    if _container_create_may_exist(resource):
        digest = resource.provider_details.get("image_digest")
        if not isinstance(digest, str) or resource.provider_version not in (None, digest):
            raise OwnershipConflict("Container image journal lacks its exact digest",
                                    OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.RECEIPT_MISSING)
        try:
            if await registry.delete(token, intent, digest):
                resource.provider_details = {
                    key: value for key, value in resource.provider_details.items() if key != "creation_empty_observed"
                }
                return "settling"
        except ContainersReleaseUnavailable:
            raise ObservationPending("The Container registry cleanup could not be confirmed",
                                     OwnershipResource.CONTAINER_IMAGE, ObservationCheck.CONTAINER_INVENTORY) from None
        if not _container_create_absence_settled(resource, now):
            return "settling"
    resource.state = "absent"
    resource.observed_at = now
    return "progressed"


def _stranded_cleanup_action(
    revision: int, worker_ids: list[UUID], resources: list[CloudflareOwnedResource], *, scale_in: bool = False,
) -> PlatformJobRequiresAction:
    instructions = [
        "Keep Cloudflare admission closed while you verify the retired revision.",
        "Check the exact owned script and namespace identities in the deployment inventory. Restore management access if Cloudflare refused verification.",
        "Retry the capacity action once the exact ownership or absence evidence is available.",
    ]
    if scale_in:
        instructions.insert(1, "Use Disconnect to remove the owned deployment if the worker cannot supply a stop receipt.")
    return PlatformJobRequiresAction(
        "Verify Cloudflare Worker Retirement", {
            "code": "CF_SLOT_STOP_UNVERIFIED", "revision": revision,
            "worker_ids": [str(worker_id) for worker_id in worker_ids],
            "scripts": {resource.target_name: resource.namespace_ids for resource in resources},
            "instructions": instructions,
        },
    )


async def _owned_revision_scripts(
    db: AsyncSession, installation_id: UUID, scope: str, revision: int,
) -> list[CloudflareOwnedResource]:
    resource = CloudflareOwnedResource
    return list(await db.scalars(select(resource).where(
        resource.installation_id == installation_id, resource.scope == scope,
        resource.desired_revision == revision, resource.kind == "worker_script",
    ).order_by(resource.target_name)))


async def _stranded_timeout_action(
    db: AsyncSession, installation_id: UUID, *, scale_in: bool,
) -> PlatformJobRequiresAction | None:
    worker, pin = ExecutionWorker, CloudflareWorkerReservation
    rows = (await db.execute(select(worker.id, pin.scope, pin.deployment_revision).join(
        pin, pin.worker_id == worker.id,
    ).where(
        pin.installation_id == installation_id, worker.capacity_resource_deleted_at.is_(None),
        worker.state.in_(("lost", "retired")), managed_worker_stop_unverified(),
    ).order_by(pin.deployment_revision, pin.scope, worker.id))).all()
    if not rows:
        return None
    _, scope, revision = rows[0]
    resources = await _owned_revision_scripts(db, installation_id, scope, revision)
    return _stranded_cleanup_action(
        revision, [worker_id for worker_id, name, rev in rows if (name, rev) == (scope, revision)],
        resources, scale_in=scale_in,
    )


async def _finalize_destroyed_worker(db: AsyncSession, worker_id: UUID, *, ledger_destroyed: bool) -> None:
    worker = ExecutionWorker
    matched = await db.scalar(update(worker).where(
        worker.id == worker_id, worker.state.in_(("lost", "retired")),
        worker.capacity_resource_deleted_at.is_(None),
    ).values(capacity_resource_deleted_at=func.clock_timestamp()).returning(worker.id))
    if matched is not None:
        await finalize_destroyed_launcher_meter(db, worker_id, ledger_destroyed=ledger_destroyed)
        await delete_workflow_slots(db, CloudflareWorkflowSlot.worker_id == worker_id)


async def _cleanup_stranded_workflow_slots(
    db: AsyncSession, row: CloudflareInstallation, adapter: OwnedResourceCleanup,
    *, before_revision: int | None = None,
) -> Literal["blocked", "progressed", "settling"]:
    """Retire one closed revision only after every remaining incarnation is stranded."""
    worker, pin = ExecutionWorker, CloudflareWorkerReservation
    revision_filter = () if before_revision is None else (pin.deployment_revision < before_revision,)
    remaining = (await db.execute(select(
        worker.id, worker.state, pin.scope, pin.deployment_revision, managed_worker_stop_unverified(),
    ).join(pin, pin.worker_id == worker.id).where(
        pin.installation_id == row.id, *revision_filter,
        worker.capacity_resource_deleted_at.is_(None) | worker.state.not_in(("lost", "retired")),
    ).order_by(worker.id))).all()
    # The installation lease serializes revision changes. Terminal workers
    # cannot return to service. Keep worker rows unlocked during provider I/O.
    for _, _, scope, revision, _ in remaining:
        if row.admitted_revision == revision:
            continue
        cohort = [(worker_id, state, held) for worker_id, state, name, rev, held in remaining
                  if (name, rev) == (scope, revision)]
        if any(state not in ("lost", "retired") or not held for _, state, held in cohort):
            continue
        resources = await _owned_revision_scripts(db, row.id, scope, revision)
        worker_ids = [worker_id for worker_id, _, _ in cohort]
        for suffix, namespaces in (
            ("-launcher", {"CloudflareLauncher", "TenantHost"}),
            ("-capacity", {"WorkerLedger", "ScopeRegistry"}),
        ):
            matches = [resource for resource in resources if resource.target_name.endswith(suffix)]
            if (
                len(matches) != 1 or matches[0].provider_id is None or not namespaces <= matches[0].namespace_ids.keys()
                or (matches[0].state == "absent" and matches[0].observed_at is None)
            ):
                raise _stranded_cleanup_action(revision, worker_ids, resources)
        try:
            cleanup = await _cleanup_one(db, row, adapter, stranded_scope=(scope, revision))
        except DeletionSettling:
            return "settling"
        except (ObservationPending, OwnershipConflict, CloudflareManagementDenied) as exc:
            if isinstance(exc, ObservationSettling) and before_revision is not None:
                raise
            raise _stranded_cleanup_action(revision, worker_ids, resources) from None
        if cleanup != "complete":
            return cleanup
        for worker_id in worker_ids:
            await _finalize_destroyed_worker(db, worker_id, ledger_destroyed=True)
        await clear_destroyed_ledger_receipts(db, row.id, scope, revision)
        return "progressed"
    return "blocked"


async def _verify_stuck_workflow_slots(
    db: AsyncSession, row: CloudflareInstallation, adapter: OwnedResourceCleanup,
) -> bool:
    """Recover terminal send fences using independent script and namespace absence."""
    worker, pin, resource, slot = ExecutionWorker, CloudflareWorkerReservation, CloudflareOwnedResource, CloudflareWorkflowSlot
    held = await db.execute(select(worker.id, resource).select_from(worker).join(pin, pin.worker_id == worker.id).outerjoin(
        resource, (resource.installation_id == pin.installation_id) & (resource.scope == pin.scope)
        & (resource.desired_revision == pin.deployment_revision) & (resource.kind == "worker_script")
        & resource.target_name.endswith("-launcher"),
    ).where(
        pin.installation_id == row.id, worker.capacity_resource_deleted_at.is_(None),
        worker.state.in_(("lost", "retired")),
        select(slot.fence).where(slot.worker_id == worker.id, slot.sent, slot.released_at.is_(None)).exists(),
    ).order_by(worker.id))
    entries = held.all()
    checked: set[UUID] = set()
    confirmed: list[UUID] = []
    for worker_id, launcher in entries:
        if (
            launcher is None or launcher.provider_id is None
            or not {"CloudflareLauncher", "TenantHost"} <= launcher.namespace_ids.keys()
        ):
            continue
        if launcher.id not in checked:
            try:
                await adapter.verify_absent(
                    api_token=await _management_token(db, row), target=_resource_intent(launcher).target,
                    namespace_ids=frozenset(launcher.namespace_ids.values()),
                )
            except (DeletionSettling, ObservationPending):
                continue
            except (OwnershipConflict, CloudflareManagementDenied):
                resources = await _owned_revision_scripts(db, row.id, launcher.scope, launcher.desired_revision)
                raise _stranded_cleanup_action(
                    launcher.desired_revision, [wid for wid, script in entries if script == launcher],
                    resources, scale_in=True,
                ) from None
            checked.add(launcher.id)
        confirmed.append(worker_id)
    for worker_id in confirmed:
        await _finalize_destroyed_worker(db, worker_id, ledger_destroyed=False)
    return bool(confirmed)


async def _settle_disable(
    context: PlatformJobContext, payload: CloudflareDisablePayload, adapter: OwnedResourceCleanup,
    *, tunnel_http: httpx.AsyncClient | None = None,
) -> dict[str, str]:
    delay = 2
    container_adapter = ContainersRest(tunnel_http) if tunnel_http is not None else None
    while True:
        waiting_for_deletion = False
        async with get_db_context() as db:
            await _hold_installation_lease(db, context, payload.installation_id)
            row = await db.get(CloudflareInstallation, payload.installation_id)
            if row is None or row.desired_revision != payload.revision or row.desired_state != payload.desired_state:
                return {"outcome": "superseded"}
            if row.retired_at is not None:
                # Retirement already verified cleanup and removed the management token.
                return {"outcome": "disconnected"}
            workers_remain = await cloudflare_workers_remain(db, row.id)
            # The provisioning lease and owned image row fence registry I/O.
            # Keep the installation available while waiting for CLI effects.
            if workers_remain or payload.action != "disconnect" or not await _next_cleanup_is_image(db, row.id):
                row = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True, populate_existing=True)
                if row is None or row.desired_revision != payload.revision or row.desired_state != payload.desired_state:
                    return {"outcome": "superseded"}
            if workers_remain:
                await _require_legacy_cleanup(db)
                if payload.action == "disconnect":
                    recovery = await _cleanup_stranded_workflow_slots(db, row, adapter)
                    if recovery == "progressed":
                        delay = 2
                        continue
                    waiting_for_deletion = recovery == "settling"
            if not workers_remain:
                cleanup = "complete"
                try:
                    if payload.action == "disconnect":
                        cleanup = await _cleanup_one(db, row, adapter, container_adapter=container_adapter)
                        if cleanup == "complete" and row.reachability_mode == "managed_tunnel":
                            if tunnel_http is None:
                                raise RuntimeError("Managed Tunnel requires the Cloudflare REST client")
                            tunnel = cloudflare_tunnel.adapter(tunnel_http, row, await _management_token(db, row))
                            if not await cloudflare_tunnel.cleanup(db, row, tunnel):
                                cleanup = "progressed"
                except DeletionSettling:
                    # The journaled deletion intent survives the wait and any
                    # runner loss. The next pass verifies ownership again.
                    cleanup = "settling"
                    waiting_for_deletion = True
                if cleanup == "progressed":
                    delay = 2
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
                if waiting_for_deletion:
                    raise PlatformJobRequiresAction("Cloudflare Cleanup Pending", {"code": "CF_CLEANUP_PENDING"})
                if workers_remain and payload.action == "scale_in" and await _verify_stuck_workflow_slots(db, row, adapter):
                    continue
                if workers_remain:
                    stranded = await _stranded_timeout_action(db, row.id, scale_in=payload.action == "scale_in")
                    if stranded is not None:
                        raise stranded
                raise await _drain_timeout_action(db, row.id, workers_remain=workers_remain)
        if waiting_for_deletion:
            await context.report("Verifying Cloudflare Deletion")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)
        else:
            await asyncio.sleep(2)


async def _drain_timeout_action(
    db: AsyncSession, installation_id: UUID, *, workers_remain: bool,
) -> PlatformJobRequiresAction:
    if not workers_remain:
        return PlatformJobRequiresAction("Wait for Cloudflare Creation to Settle", {"code": "CF_DRAIN_TIMEOUT"})
    worker, pin = ExecutionWorker, CloudflareWorkerReservation
    workers = (await db.execute(select(
        worker.id, worker.capacity_resource_uid, pin.scope, pin.deployment_revision,
    ).join(pin, pin.worker_id == worker.id).where(
        pin.installation_id == installation_id,
        worker.capacity_resource_deleted_at.is_(None) | worker.state.not_in(("lost", "retired")),
    ).order_by(pin.deployment_revision, pin.scope, worker.id))).all()
    return PlatformJobRequiresAction("Verify Cloudflare Worker Retirement", {
        "code": "CF_DRAIN_TIMEOUT",
        "workers": [{
            "worker_id": str(worker_id), "resource_uid": uid, "scope": scope, "revision": revision,
        } for worker_id, uid, scope, revision in workers],
    })


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


async def _remove_verified_scopes(
    db: AsyncSession, row: CloudflareInstallation, *, before_revision: int | None = None, scope: str | None = None,
) -> None:
    scopes = [CloudflareCapacityScope.installation_id == row.id]
    if before_revision is not None:
        scopes.append(CloudflareCapacityScope.deployment_revision < before_revision)
    if scope is not None:
        scopes.append(CloudflareCapacityScope.scope == scope)
    unverified_scope = await db.scalar(select(CloudflareCapacityScope.scope).where(
        *scopes,
        ~select(CloudflareOwnedResource.id).where(
            CloudflareOwnedResource.installation_id == row.id,
            CloudflareOwnedResource.scope == CloudflareCapacityScope.scope,
            CloudflareOwnedResource.desired_revision == CloudflareCapacityScope.deployment_revision,
            CloudflareOwnedResource.kind == "worker_script",
            CloudflareOwnedResource.target_name.endswith(case(
                (CloudflareCapacityScope.scope == "containers", "-containers"),
                else_="-capacity",
            )),
            CloudflareOwnedResource.state == "absent",
            CloudflareOwnedResource.observed_at.is_not(None),
        ).correlate(CloudflareCapacityScope).exists(),
    ).limit(1))
    if unverified_scope is not None:
        raise ObservationPending(
            "Cloudflare scope absence is unverified", OwnershipResource.WORKERS_SCOPE, ObservationCheck.SCOPE_ABSENCE,
        )
    await db.execute(delete(CloudflareCapacityScope).where(*scopes))


async def _retire(db: AsyncSession, row: CloudflareInstallation, payload: CloudflareDisablePayload) -> None:
    await _remove_verified_scopes(db, row)
    await retire_cloudflare_installation(db, installation_id=row.id, expected_revision=payload.revision)


async def _disable(context: PlatformJobContext, parsed: BaseModel) -> dict[str, str]:
    payload = CloudflareDisablePayload.model_validate(parsed)
    await context.report("Draining Cloudflare Capacity")
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            return await _settle_disable(context, payload, WorkersRestCleanup(http), tunnel_http=http)
    except OwnershipConflict as exc:
        raise PlatformJobRequiresAction("Verify Cloudflare Ownership", {
            "code": "CF_OWNERSHIP_CONFLICT", "details": exc.as_result(),
        }) from None
    except ObservationPending as exc:
        raise PlatformJobRequiresAction("Cloudflare Cleanup Pending", {
            "code": "CF_CLEANUP_PENDING", "details": exc.as_result(),
        }) from None
    except CloudflarePolicyConflict:
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
    namespaces: tuple[tuple[str, str], ...]


async def _inventory_snapshot(
    db: AsyncSession, installation_id: UUID, *, revision: int | None = None,
) -> tuple[_InventoryReceipt, ...]:
    query = select(CloudflareOwnedResource).where(
        CloudflareOwnedResource.installation_id == installation_id,
    )
    if revision is None:
        query = query.where(CloudflareOwnedResource.state.in_(("creating", "present", "deleting")))
    else:
        query = query.where(
            CloudflareOwnedResource.desired_revision == revision,
            CloudflareOwnedResource.scope == "workers",
            CloudflareOwnedResource.kind == "worker_script",
            CloudflareOwnedResource.state == "present",
        )
    rows = (await db.scalars(query.order_by(CloudflareOwnedResource.id))).all()
    return tuple(_InventoryReceipt(
        _resource_intent(row), row.provider_id, row.provider_version, row.state,
        tuple(sorted(row.namespace_ids.items())),
    ) for row in rows)


async def _observe_inventory(
    adapter: OwnedResourceCleanup, token: SecretStr, inventory: tuple[_InventoryReceipt, ...],
) -> None:
    for receipt in inventory:
        if receipt.provider_id is None or receipt.provider_version is None:
            raise OwnershipConflict(
                "Cloudflare Worker has no exact deployment receipt",
                OwnershipResource.WORKER_SCRIPT, OwnershipCheck.RECEIPT_MISSING,
            )
        observation = ResourceObservation(receipt.intent, receipt.provider_id, receipt.provider_version)
        remote = await observe_owned(adapter, api_token=token, intent=receipt.intent, recorded=observation)
        if remote is None:
            raise ObservationPending(
                "An owned Cloudflare Worker has disappeared",
                OwnershipResource.WORKER_SCRIPT, ObservationCheck.SCRIPT_RECORDED_ABSENT,
            )


async def usage_script_identities(db: AsyncSession, installation_id: UUID) -> dict[str, dict[str, str]]:
    """Compare the ledger against retained Workers receipts for the same account."""
    account = select(CloudflareInstallation.account_id).where(
        CloudflareInstallation.id == installation_id,
    ).scalar_subquery()
    identities = await db.execute(select(
        CloudflareOwnedResource.target_name, CloudflareOwnedResource.namespace_ids,
    ).where(
        CloudflareOwnedResource.account_id == account,
        CloudflareOwnedResource.scope == "workers",
        CloudflareOwnedResource.kind == "worker_script",
        CloudflareOwnedResource.provider_id.is_not(None),
    ))
    return {name: namespaces for name, namespaces in identities}


def _current_reconcile(installation: CloudflareInstallation | None, revision: int) -> bool:
    return (
        installation is not None and installation.retired_at is None
        and installation.desired_revision == revision and installation.desired_state == "enabled"
    )


def _replacement_revision(
    row: CloudflareInstallation, release: str, order: WorkersReleaseOrder,
) -> int | None:
    if workers_release_error(row, release, order):
        return None
    target = row.upgrade_release or row.policy.get("bounds_release")
    if release != target or (row.upgrade_release is None and row.drain_required):
        return row.desired_revision + 1
    return None


async def _stage_reconcile(
    db: AsyncSession, row: CloudflareInstallation, *, release: str | None, order: WorkersReleaseOrder | None,
    force: bool = False,
) -> None:
    """Close claims and pin the target in the same transaction as enqueue."""
    if release is None or order is None or workers_release_error(row, release, order):
        return
    revision = row.desired_revision + 1 if force else _replacement_revision(row, release, order)
    if revision is not None:
        row.desired_revision = revision
        row.upgrade_release = release
        row.upgrade_drained_at = None
        row.admitted_revision = None
        wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
    floor = workers_release_floor(row)
    if floor is None or order.key > floor.key:
        set_workers_release_floor(row, release, order)


async def _settle_reconcile(
    context: PlatformJobContext, payload: CloudflareReconcilePayload, adapter: WorkersRestCleanup,
    *, tunnel_http: httpx.AsyncClient | None = None,
) -> dict[str, str]:
    issue = None
    token = None
    advisory = None
    repair_tunnel_ingress = False
    async with get_db_context() as db:
        await _hold_installation_lease(db, context, payload.installation_id)
        installation = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
        if installation is None or not _current_reconcile(installation, payload.revision):
            return {"outcome": "superseded"}
        token_version = installation.api_token_version
        admitted_revision = installation.admitted_revision
        scope = await db.get(CloudflareCapacityScope, (installation.id, "workers", payload.revision))
        inventory = await _inventory_snapshot(db, installation.id, revision=payload.revision)
        usage_identities = await usage_script_identities(db, installation.id)
        try:
            token = await _management_token(db, installation)
            if installation.reachability_mode == "managed_tunnel":
                if tunnel_http is None:
                    raise RuntimeError("Managed Tunnel requires the Cloudflare REST client")
                tunnel = cloudflare_tunnel.adapter(tunnel_http, installation, token)
                try:
                    await cloudflare_tunnel.verify(db, installation, tunnel)
                except IngressMismatch:
                    issue = ("Verify Cloudflare Tunnel Ingress", "CF_TUNNEL_INGRESS_MISMATCH")
                    # Commit closure before asking the provider to change its route.
                    installation.admitted_revision = admitted_revision = None
                    wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
                    repair_tunnel_ingress = True
        except CloudflareManagementDenied:
            issue = ("Reconnect Cloudflare", "CF_MANAGEMENT_CREDENTIAL")
        except OwnershipConflict:
            issue = ("Verify Cloudflare Tunnel Ownership", "CF_OWNERSHIP_CONFLICT")
        except (cloudflare_tunnel.TunnelUnavailable, ValueError):
            issue = ("Verify Managed Tunnel Connector and DNS", "CF_CONNECTOR_UNAVAILABLE")
        except ObservationPending:
            raise CloudflareRestUnavailable("Managed Tunnel inventory could not be confirmed") from None

    if token is not None and issue is None:
        try:
            roles = {receipt.intent.target.name.rsplit("-", 1)[-1] for receipt in inventory}
            expected_roles = {"launcher", "capacity"}
            if scope is not None and scope.gateway_required:
                expected_roles.add("gateway")
            if len(inventory) != len(expected_roles) or roles != expected_roles:
                raise ObservationPending("The admitted revision lacks a complete Workers deployment receipt")
            await _observe_inventory(adapter, token, inventory)
            if not all(receipt.namespaces for receipt in inventory if not receipt.intent.target.name.endswith("-gateway")):
                raise ObservationPending("The admitted revision lacks exact namespace receipts")
            checked_at = datetime.now(UTC)
            start, _ = usage_period(checked_at)
            sampled = await adapter.advisory_workers_requests(
                installation.account_id, token, set(usage_identities), start, checked_at,
            )
            advisory = {"checked_at": checked_at.isoformat(), "period_start": start.isoformat()}
            if sampled is not None:
                async with get_db_context() as db:
                    ledger = await read_workers_usage(db, installation.id, checked_at)
                if ledger is not None:
                    local = ledger.dispatched_runs + ledger.sdk_requests
                    advisory.update(worker_requests=str(sampled), ledger_request_proxy=str(local),
                                    request_difference=str(sampled - local))
        except OwnershipConflict:
            issue = ("Verify Cloudflare Ownership", "CF_OWNERSHIP_CONFLICT")
        except ObservationPending:
            issue = ("Verify Cloudflare Deployment", "CF_DEPLOYMENT_UNVERIFIED")
        except CloudflareManagementDenied:
            issue = ("Reconnect Cloudflare", "CF_MANAGEMENT_CREDENTIAL")

    estimated_usage = None
    async with get_db_context() as db:
        await _hold_installation_lease(db, context, payload.installation_id)
        installation = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
        if (
            installation is None or not _current_reconcile(installation, payload.revision)
            or installation.api_token_version != token_version
            or installation.admitted_revision != admitted_revision
            or await _inventory_snapshot(db, installation.id, revision=payload.revision) != inventory
            or await usage_script_identities(db, installation.id) != usage_identities
        ):
            return {"outcome": "superseded"}
        if issue is not None:
            installation.admitted_revision = None
            if repair_tunnel_ingress and tunnel_http is not None and token is not None:
                try:
                    tunnel = cloudflare_tunnel.adapter(tunnel_http, installation, token)
                    await cloudflare_tunnel.repair_ingress(db, installation, tunnel)
                except (CloudflareManagementDenied, CloudflareRestUnavailable, ObservationPending, OwnershipConflict):
                    issue = ("Repair Cloudflare Tunnel Ingress", "CF_TUNNEL_INGRESS_MISMATCH")
        else:
            installation.usage_advisory = advisory
            estimated_usage = await db.scalar(
                select(cloudflare_estimated_usage()).where(CloudflareInstallation.id == installation.id),
            )
        wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
    if issue is not None:
        raise PlatformJobRequiresAction(issue[0], {"code": issue[1]})
    if estimated_usage is None:
        raise PlatformJobRequiresAction("Verify Cloudflare Spend", {"code": "CF_SPEND_UNVERIFIED"})
    return {"outcome": "verified"}


async def _reconcile(context: PlatformJobContext, parsed: BaseModel) -> dict[str, Any]:
    payload = CloudflareReconcilePayloadV2.model_validate(parsed.model_dump())
    async with get_db_context() as db:
        await _hold_installation_lease(db, context, payload.installation_id)
        row = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
        if row is None or row.retired_at is not None or row.desired_revision != payload.revision:
            return {"outcome": "superseded"}
        if payload.reset_release_fence and row.desired_state == "scaled_in":
            return {"outcome": "fence_reset"}
        if not _current_reconcile(row, payload.revision):
            return {"outcome": "superseded"}
        target = row.upgrade_release or row.policy.get("bounds_release")
        release, order = installed_workers_release_digest(), installed_workers_release_order()
        if release is not None and order is not None and (code := workers_release_error(row, release, order)):
            if code == "CF_RELEASE_DOWNGRADE" and row.upgrade_release is not None:
                # A newer target is already pinned. A matched runner can resume it.
                raise PlatformJobRequiresAction("Install the Matched Workers Release", {
                    "code": "CF_RELEASE_MISMATCH", "instructions": [RELEASE_FENCE_REMEDY],
                })
            raise PlatformJobRequiresAction("Review the Workers Release Fence", {
                "code": code, "instructions": [RELEASE_FENCE_REMEDY],
            })
        if release is None or order is None or release != target:
            raise PlatformJobRequiresAction("Install the Matched Workers Release", {"code": "CF_RELEASE_MISMATCH"})
        provision = row.admitted_revision is None
        refresh = (
            not provision and row.containers_policy.get("enabled") is True
            and read_verified_egress(row, datetime.now(UTC)) is None
            and await db.scalar(select(WorkDispatch.id).join(
                Execution, Execution.id == WorkDispatch.subject_id,
            ).join(CloudflareApproval, CloudflareApproval.workflow_id == Execution.workflow_id).where(
                WorkDispatch.assigned_worker_id.is_(None), WorkDispatch.handler == "workflow.execute",
                Execution.status == ExecutionStatus.PENDING,
                CloudflareApproval.is_current, CloudflareApproval.policy_version == LINUX_REVIEW_VERSION,
                CloudflareApproval.declaration["installation_id"].as_string() == str(row.id),
                CloudflareApproval.declaration["deployment_revision"].as_integer() == row.desired_revision,
            ).limit(1)) is not None
        )
    if provision:
        # Reuse the admission canary and write fences used by Enable.
        from src.jobs.platform.cloudflare_enable import CloudflareEnablePayload, _enable

        return await _enable(
            context, CloudflareEnablePayload(installation_id=payload.installation_id, revision=payload.revision),
            repair_tunnel_ingress=True,
        )

    await context.report("Verifying Cloudflare Deployment")
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            verified = await _settle_reconcile(context, payload, WorkersRestCleanup(http), tunnel_http=http)
    except CloudflareRestUnavailable:
        raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare REST Is Temporarily Unavailable", retryable=True) from None
    if refresh and verified["outcome"] == "verified":
        from src.jobs.platform.cloudflare_enable import CloudflareEnablePayload, _enable

        return await _enable(
            context, CloudflareEnablePayload(installation_id=payload.installation_id, revision=payload.revision),
            refresh_container_egress=True,
        )
    return verified


CLOUDFLARE_RECONCILE_DEFINITION = define_platform_job(
    RECONCILE_JOB, CloudflareReconcilePayloadV2, _reconcile,
    historical_payload_models={1: CloudflareReconcilePayload},
)


async def enqueue_cloudflare_reconcile(
    db: AsyncSession, caller: Caller, *, installation_id: UUID, expected_revision: int,
    upgrade: bool = False, reset_release_fence: bool = False,
) -> tuple[PlatformJob, bool]:
    if upgrade:
        operation = "capacity.cloudflare_upgrade"
    else:
        operation = "capacity.cloudflare_reconcile"
    require_operation(caller, operation, GLOBAL)
    actor = caller.principal
    installation = await db.get(CloudflareInstallation, installation_id, with_for_update=True)
    if (
        installation is None or installation.retired_at is not None
        or installation.desired_revision != expected_revision
        or installation.desired_state not in (("enabled", "scaled_in") if reset_release_fence else ("enabled",))
    ):
        raise CloudflarePolicyConflict("Cloudflare reconciliation requires the current installation revision.")
    release, order = installed_workers_release_digest(), installed_workers_release_order()
    if reset_release_fence:
        if release is None or order is None or not workers_release_error(installation, release, order):
            raise CloudflarePolicyConflict("A conflicting or older packaged Workers release is required to reset the fence.")
        await emit_audit(
            db, operation, resource_type="cloudflare_installation", resource_id=installation_id,
            details={
                "reset_release_fence": True,
                "previous_version": installation.workers_release_version,
                "previous_sequence": installation.workers_release_sequence,
                "previous_digest": installation.workers_release_digest,
                "release_version": order.version,
                "release_sequence": order.sequence,
                "release_digest": release,
            },
        )
        set_workers_release_floor(installation, release, order)
    if upgrade and not reset_release_fence and not workers_upgrade_available(installation, release, order):
        code = workers_release_error(installation, release, order) if release is not None and order is not None else None
        raise CloudflarePolicyConflict(
            f"{code}. {RELEASE_FENCE_REMEDY}" if code else "There is no matching Cloudflare upgrade available."
        )
    if installation.desired_state == "enabled":
        await _stage_reconcile(db, installation, release=release, order=order, force=reset_release_fence)
    payload = CloudflareReconcilePayloadV2(
        installation_id=installation_id, revision=installation.desired_revision,
        operation="upgrade" if upgrade or installation.upgrade_release else "reconcile",
        reset_release_fence=reset_release_fence,
    )
    job, reused = await enqueue_platform_job(
        db, CLOUDFLARE_RECONCILE_DEFINITION, payload,
        dedupe_key=f"manual:{operation}:{reset_release_fence}:{installation_id}:{payload.revision}:{actor.user_id}",
        resource_lock_key=f"cloudflare-installation:{installation_id}",
        organization_id=None, requested_by_user_id=actor.user_id, requested_by_email=actor.email,
        requested_by_name=actor.name, resource_type="cloudflare_installation", resource_id=str(installation_id),
        title="Upgrade Cloudflare" if payload.operation == "upgrade" else "Reconcile Cloudflare", action_url=None,
    )
    if not reused:
        await emit_audit(
            db, operation, resource_type="cloudflare_installation", resource_id=installation_id,
            details={"revision": payload.revision, "job_id": str(job.id)},
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
        # A lost upload response can leave a live Worker with only a creation
        # intent. Observe it with the new token before activating that token.
        inventory = await _inventory_snapshot(db, installation.id)
        ref = CredentialRef(installation.id, account_id, "account_token", "", payload.replacement_version)
        bearer = open_credential(ref, candidate)

    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            adapter = WorkersRestCleanup(http)
            await adapter.verify_workers_paid(account_id, bearer)
            workers = [receipt for receipt in inventory if receipt.intent.target.scope == "workers"]
            for receipt in workers:
                recorded = None
                if receipt.provider_id is not None and receipt.provider_version is not None:
                    recorded = ResourceObservation(receipt.intent, receipt.provider_id, receipt.provider_version)
                elif receipt.state != "creating":
                    raise OwnershipConflict("Cloudflare Worker has no exact deployment receipt")
                observed = await observe_owned(adapter, api_token=bearer, intent=receipt.intent, recorded=recorded)
                if observed is None:
                    if receipt.state == "present":
                        raise OwnershipConflict("The recorded Cloudflare Worker is absent")
                    continue
                await adapter.verify_workers_write(bearer, observed)
    except CloudflareManagementDenied:
        denial = ("Submit a Replacement Cloudflare API Token", "CF_MANAGEMENT_CREDENTIAL")
    except WorkersPaidUnverified as exc:
        denial = (str(exc), "CF_WORKERS_PAID_UNVERIFIED")
    except WorkersWriteUnverified as exc:
        denial = (str(exc), "CF_WORKERS_WRITE_UNVERIFIED")
    except (ObservationPending, OwnershipConflict, ValueError):
        denial = ("Verify Cloudflare Ownership", "CF_DEPLOYMENT_UNVERIFIED")
    except CloudflareRestUnavailable:
        raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare REST Is Temporarily Unavailable", retryable=True) from None

    for _ in range(2):
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
            if denial is None and installation.reachability_mode == "managed_tunnel":
                try:
                    async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
                        tunnel = cloudflare_tunnel.adapter(http, installation, bearer)
                        await cloudflare_tunnel.verify_management_writes(
                            db, installation, tunnel, repair=installation.admitted_revision is None,
                        )
                except IngressMismatch:
                    if installation.admitted_revision is not None:
                        installation.admitted_revision = None
                        wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
                        # Commit closure, then reacquire the lease and compare the same receipts.
                        continue
                    denial = ("Verify Cloudflare Tunnel and DNS Ownership", "CF_DEPLOYMENT_UNVERIFIED")
                except CloudflareManagementDenied:
                    denial = ("Check Cloudflare Tunnel and DNS Write Permissions", "CF_MANAGEMENT_CREDENTIAL")
                except (ObservationPending, OwnershipConflict, ValueError):
                    denial = ("Verify Cloudflare Tunnel and DNS Ownership", "CF_DEPLOYMENT_UNVERIFIED")
                except CloudflareRestUnavailable:
                    raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare REST Is Temporarily Unavailable", retryable=True) from None
            if denial is not None:
                candidate.ciphertext = None
            else:
                installation.api_token_version = payload.replacement_version
                # Replacing a token revokes Bifrost's access to earlier versions.
                # Repair a narrower or revoked replacement with another explicit rotation.
                await db.execute(update(CloudflareCredential).where(
                    CloudflareCredential.installation_id == installation.id,
                    CloudflareCredential.purpose == "account_token",
                    CloudflareCredential.version < payload.replacement_version,
                ).values(ciphertext=None))
                await emit_audit(
                    db, "capacity.cloudflare_account_token_rotated", resource_type="cloudflare_installation",
                    resource_id=installation.id, details={
                        "credential_version": payload.replacement_version, "job_id": str(context.job_id),
                    }, actor_override=ActorContext(None, None, source="scheduler"),
                )
        break
    else:
        return {"outcome": "superseded"}
    if denial is not None:
        raise PlatformJobRequiresAction(denial[0], {"code": denial[1]})
    return {"outcome": "rotated", "credential_version": payload.replacement_version}


CLOUDFLARE_ROTATE_ACCOUNT_DEFINITION = define_platform_job(
    ROTATE_JOB, CloudflareRotateAccountPayload, _rotate_account_token,
)


async def _rotate_worker_credential(context: PlatformJobContext, parsed: BaseModel) -> dict[str, str | int]:
    payload = CloudflareRotateWorkerPayload.model_validate(parsed)
    await context.report("Rotating Cloudflare Worker Credential")
    scope = "workers" if payload.credential == "scope_bearer" else ""
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            if payload.credential == "connector_token":
                return await _rotate_connector(context, payload, http)
            adapter = WorkersRestCleanup(http)
            while True:
                async with get_db_context() as db:
                    await _hold_installation_lease(db, context, payload.installation_id)
                    row = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
                    if (
                        row is None or row.retired_at is not None or row.desired_revision != payload.revision
                        or row.desired_state != "scaled_in"
                    ):
                        return {"outcome": "superseded"}
                    key = (row.id, payload.credential, scope, payload.replacement_version)
                    current = await db.get(CloudflareCredential, key)
                    if current is not None and current.ciphertext is not None:
                        break
                    if await cloudflare_workers_remain(db, row.id):
                        await _require_legacy_cleanup(db)
                        raise PlatformJobRequiresAction("Wait for Cloudflare Workers to Retire", {"code": "CF_PRIOR_WORKERS_LIVE"})
                    if scope:
                        # Keep the revision fence through the exact delete
                        # and absence receipt, even if the job lease expires.
                        progress = await _cleanup_one(db, row, adapter, workers_only=True)
                        if progress == "progressed":
                            continue
                        if progress == "settling":
                            raise PlatformJobRequiresAction("Wait for Cloudflare Creation to Settle", {"code": "CF_CLEANUP_PENDING"})
                        await _remove_verified_scopes(db, row, scope="workers")
                    ref = CredentialRef(row.id, row.account_id, payload.credential, scope, payload.replacement_version)
                    db.add(seal_credential(
                        ref, SecretStr(secrets.token_urlsafe(48)), actor=context.requested_by_user_id, now=datetime.now(UTC),
                    ))
                    if payload.credential == "control_root":
                        row.control_root_version = ref.version
                    await db.execute(update(CloudflareCredential).where(
                        CloudflareCredential.installation_id == row.id,
                        CloudflareCredential.purpose == payload.credential,
                        CloudflareCredential.scope == scope,
                        CloudflareCredential.version < ref.version,
                    ).values(ciphertext=None))
                    await emit_audit(
                        db, "capacity.cloudflare_worker_credential_rotated", resource_type="cloudflare_installation",
                        resource_id=row.id, details={"credential": ref.purpose, "credential_version": ref.version},
                        actor_override=ActorContext(None, None, source="scheduler"),
                    )
                    break
    except OwnershipConflict as exc:
        raise PlatformJobRequiresAction("Verify Cloudflare Ownership", {
            "code": "CF_OWNERSHIP_CONFLICT", "details": exc.as_result(),
        }) from None
    except ObservationPending as exc:
        raise PlatformJobRequiresAction("Cloudflare Cleanup Pending", {
            "code": "CF_CLEANUP_PENDING", "details": exc.as_result(),
        }) from None
    except CloudflareManagementDenied:
        raise PlatformJobRequiresAction("Reconnect Cloudflare", {"code": "CF_MANAGEMENT_CREDENTIAL"}) from None
    except CloudflareRestUnavailable:
        raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare REST Is Temporarily Unavailable", retryable=True) from None
    return {"outcome": "rotated", "credential_version": payload.replacement_version}


async def _rotate_connector(
    context: PlatformJobContext, payload: CloudflareRotateWorkerPayload, http: httpx.AsyncClient,
) -> dict[str, str | int]:
    async def current(db: AsyncSession) -> CloudflareInstallation | None:
        await _hold_installation_lease(db, context, payload.installation_id)
        row = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
        if row is None or row.retired_at is not None or row.desired_revision != payload.revision or row.desired_state != "scaled_in":
            return None
        return row

    async with get_db_context() as db:
        row = await current(db)
        if row is None:
            return {"outcome": "superseded"}
        if await cloudflare_workers_remain(db, row.id):
            raise PlatformJobRequiresAction("Wait for Cloudflare Workers to Retire", {"code": "CF_PRIOR_WORKERS_LIVE"})
        tunnel = cloudflare_tunnel.adapter(http, row, await _management_token(db, row))
        if row.tunnel_token_version != payload.replacement_version:
            await cloudflare_tunnel.record_rotation_intent(db, row, tunnel, version=payload.replacement_version)
    async with get_db_context() as db:
        row = await current(db)
        if row is None:
            return {"outcome": "superseded"}
        tunnel = cloudflare_tunnel.adapter(http, row, await _management_token(db, row))
        if row.tunnel_token_version != payload.replacement_version:
            await cloudflare_tunnel.rotate(
                db, row, tunnel, version=payload.replacement_version, actor=context.requested_by_user_id,
            )
            await emit_audit(
                db, "capacity.cloudflare_worker_credential_rotated", resource_type="cloudflare_installation",
                resource_id=row.id, details={"credential": "connector_token", "credential_version": payload.replacement_version},
                actor_override=ActorContext(None, None, source="scheduler"),
            )
    await context.report("Verifying Managed Tunnel Connector Rollout")
    async with get_db_context() as db:
        row = await current(db)
        if row is None:
            return {"outcome": "superseded"}
        tunnel = cloudflare_tunnel.adapter(http, row, await _management_token(db, row))
        ready = await cloudflare_tunnel.connector_ready(db, row, tunnel)
    if not ready:
        raise PlatformJobFailure("CF_CONNECTOR_UNAVAILABLE", "Waiting for Managed Tunnel Connector Rollout", retryable=True)
    return {"outcome": "rotated", "credential_version": payload.replacement_version}


CLOUDFLARE_ROTATE_WORKER_DEFINITION = define_platform_job(
    ROTATE_WORKER_JOB, CloudflareRotateWorkerPayload, _rotate_worker_credential,
)


async def enqueue_cloudflare_worker_credential_rotation(
    db: AsyncSession, caller: Caller, *, installation_id: UUID, expected_revision: int,
    credential: Literal["control_root", "scope_bearer", "connector_token"],
) -> tuple[PlatformJob, bool]:
    require_operation(caller, "capacity.cloudflare_rotate_account_token", GLOBAL)
    actor = caller.principal
    row = await db.get(CloudflareInstallation, installation_id, with_for_update=True, populate_existing=True)
    if row is None or row.retired_at is not None or row.desired_revision != expected_revision or row.desired_state != "scaled_in":
        raise CloudflarePolicyConflict("Scale in the current Cloudflare deployment before rotating worker credentials.")
    existing = await find_active_platform_job(db, ROTATE_WORKER_JOB, str(row.id))
    if existing is not None:
        raise CloudflarePolicyConflict("A worker credential rotation is already pending")
    if credential == "connector_token":
        if row.reachability_mode != "managed_tunnel" or row.tunnel_token_version is None:
            raise CloudflarePolicyConflict("Provision Managed Tunnel before rotating its connector token.")
        version = row.tunnel_token_version + 1
        await cloudflare_tunnel.stage_rotation(db, row, version=version, actor=str(actor.user_id))
    elif credential == "scope_bearer":
        row.desired_revision += 1
        version = row.desired_revision
    else:
        version = (row.control_root_version or 0) + 1
    payload = CloudflareRotateWorkerPayload(
        installation_id=row.id, revision=row.desired_revision, credential=credential,
        replacement_version=version,
    )
    job, reused = await enqueue_platform_job(
        db, CLOUDFLARE_ROTATE_WORKER_DEFINITION, payload, dedupe_key=str(row.id),
        resource_lock_key=f"cloudflare-installation:{row.id}",
        organization_id=None, requested_by_user_id=actor.user_id, requested_by_email=actor.email,
        requested_by_name=actor.name, resource_type="cloudflare_installation", resource_id=str(row.id),
        title="Rotate Cloudflare Worker Credential", action_url=None,
    )
    if not reused:
        await emit_audit(
            db, "capacity.cloudflare_rotate_worker_credential", resource_type="cloudflare_installation",
            resource_id=row.id, details={"credential": credential, "job_id": str(job.id)},
        )
    return job, reused


async def enqueue_cloudflare_account_token_rotation(
    db: AsyncSession, caller: Caller, *, installation_id: UUID, expected_revision: int, replacement: SecretStr | UUID,
) -> tuple[PlatformJob, bool]:
    require_operation(caller, "capacity.cloudflare_rotate_account_token", GLOBAL)
    actor = caller.principal
    row = await db.get(CloudflareInstallation, installation_id, with_for_update=True, populate_existing=True)
    if row is None or row.retired_at is not None or row.desired_revision != expected_revision or row.api_token_version is None:
        raise CloudflarePolicyConflict("Cloudflare installation changed. Reload its current policy.")
    key = str(row.id)
    if await find_active_platform_job(db, ROTATE_JOB, key) is not None:
        raise CloudflarePolicyConflict("A Cloudflare API token rotation is already pending")
    if isinstance(replacement, UUID):
        replacement = await consume_staged_token(
            db, replacement, actor_id=actor.user_id, account_id=row.account_id, purpose="rotate", installation_id=row.id,
        )
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
    """Fence an outdated release and schedule inventory verification or repair."""
    await purge_expired_staged_tokens(db)
    installation = await db.scalar(select(CloudflareInstallation).where(
        CloudflareInstallation.retired_at.is_(None),
        CloudflareInstallation.desired_state == "enabled",
    ).with_for_update(skip_locked=True))
    if installation is None:
        return
    was_admitted = installation.admitted_revision is not None
    if not was_admitted and installation.upgrade_release is None and not installation.drain_required:
        return
    active_enable = await db.scalar(select(PlatformJob.id).where(
        PlatformJob.job_type == "capacity.cloudflare_enable",
        PlatformJob.resource_id == str(installation.id),
        PlatformJob.status.in_(ACTIVE_PLATFORM_JOB_STATUSES),
    ).limit(1))
    if active_enable is not None:
        return
    release, order = installed_workers_release_digest(), installed_workers_release_order()
    await _stage_reconcile(db, installation, release=release, order=order)
    active = await db.scalar(select(PlatformJob.id).where(
        PlatformJob.job_type == RECONCILE_JOB,
        PlatformJob.resource_id == str(installation.id),
        PlatformJob.status.in_(ACTIVE_PLATFORM_JOB_STATUSES),
    ).limit(1))
    if active is not None:
        return
    history = select(PlatformJob).where(
        PlatformJob.job_type == RECONCILE_JOB,
        PlatformJob.resource_id == str(installation.id),
        PlatformJob.payload["revision"].as_integer() == installation.desired_revision,
    )
    latest = await db.scalar(history.order_by(PlatformJob.created_at.desc(), PlatformJob.id.desc()).limit(1))
    if latest is not None and latest.status != "succeeded":
        code = (latest.result or {}).get("code") if latest.status == "requires_action" else latest.error_code
        if (
            installation.admitted_revision is None and code not in SELF_HEALING_RECONCILE_CODES
            and not (installation.upgrade_release is not None and code in PINNED_UPGRADE_RUNNER_CODES)
        ):
            return
        last_success = await db.scalar(history.where(PlatformJob.status == "succeeded")
                                       .order_by(PlatformJob.created_at.desc(), PlatformJob.id.desc()).limit(1))
        failures_query = history.where(PlatformJob.status.in_(("requires_action", "failed", "cancelled")))
        if last_success is not None:
            failures_query = failures_query.where(
                tuple_(PlatformJob.created_at, PlatformJob.id) > (last_success.created_at, last_success.id),
            )
        failures = await db.scalar(select(func.count()).select_from(failures_query.subquery()))
        delay = min(RECONCILE_INTERVAL_SECONDS * 2 ** min(max((failures or 1) - 1, 0), 4),
                    RECONCILE_MAX_BACKOFF_SECONDS)
        if now < (latest.completed_at or latest.created_at) + timedelta(seconds=delay):
            return
    interval = int(now.timestamp()) // RECONCILE_INTERVAL_SECONDS
    payload = CloudflareReconcilePayloadV2(
        installation_id=installation.id, revision=installation.desired_revision,
        operation="upgrade" if installation.upgrade_release else "reconcile",
    )
    await enqueue_platform_job(
        db, CLOUDFLARE_RECONCILE_DEFINITION, payload,
        dedupe_key=f"{installation.id}:{payload.revision}:{interval}", reuse_when=true(),
        resource_lock_key=f"cloudflare-installation:{installation.id}",
        organization_id=None, requested_by_user_id="system", requested_by_email="",
        requested_by_name="Bifrost", resource_type="cloudflare_installation", resource_id=str(installation.id),
        title="Upgrade Cloudflare" if installation.upgrade_release else "Reconcile Cloudflare",
        action_url=None,
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
    row = await db.get(CloudflareInstallation, installation_id, with_for_update=True, populate_existing=True)
    if row is None or row.retired_at is not None:
        raise CloudflarePolicyConflict("Cloudflare installation is unavailable")
    retain_revision = (
        action == "scale_in" and row.deployed_revision == row.desired_revision == expected_revision
        and row.upgrade_release is None and not row.drain_required
        and (row.admitted_revision == expected_revision or row.desired_state == "scaled_in")
    )
    # Keep the deployment identity when pausing a fully admitted revision.
    # Admission and the Scale In job lease fence the drain. A cutover or
    # Disconnect still advances the revision to invalidate outstanding work.
    payload = CloudflareDisablePayload(
        installation_id=installation_id, revision=expected_revision if retain_revision else expected_revision + 1,
        action=action,
    )
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
    row.containers_ready_revision = None
    row.containers_release_digest = None
    row.upgrade_release = None
    row.upgrade_drained_at = None
    row.drain_required = False
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
