"""Provision and admit one exact, verified Workers deployment."""

import secrets
from asyncio import sleep
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from shared.models import CloudflareCapacityPolicy
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.jobs.platform.base import (
    PlatformJobContext,
    PlatformJobFailure,
    PlatformJobRequiresAction,
    define_platform_job,
)
from src.jobs.platform.cloudflare_capacity import (
    DRAIN_DEADLINE,
    _cleanup_one,
    _cleanup_stranded_workflow_slots,
    _drain_timeout_action,
    _hold_installation_lease,
    _inventory_snapshot,
    _management_token,
    _observe_inventory,
    _remove_verified_scopes,
    _require_legacy_cleanup,
    _resource_intent,
)
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
)
from src.models.orm.platform_jobs import PlatformJob
from src.services.audit import emit_audit
from src.services.authorization.enforce import Caller, require_operation
from src.services.authorization.evaluator import GLOBAL
from src.services.platform_jobs import enqueue_platform_job
from src.services.work_dispatch import (
    WAKE_CLOUDFLARE_CAPACITY,
    wake_pool_workers_after_commit,
)
from src.services.worker_ingress_probes import probe_worker_ingress
from src.services.worker_pool.cloudflare_canary import run_workers_canary
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    open_credential,
    seal_credential,
)
from src.services.worker_pool.cloudflare_policy import (
    CloudflarePolicyConflict,
    _price_policy,
    cloudflare_workers_remain,
    release_meter_bounds,
)
from src.services.worker_pool.cloudflare_provisioning import (
    DeletionSettling,
    ObservationPending,
    OwnershipConflict,
    ProvisioningAdapter,
    ResourceObservation,
    observe_owned,
    resume_creation,
)
from src.services.worker_pool.cloudflare_release import (
    WorkersRelease,
    WorkersReleaseUnavailable,
    load_workers_release,
)
from src.services.worker_pool.cloudflare_spend import (
    capacity_request_reservation,
    cloudflare_estimated_usage,
)
from src.services.worker_pool.cloudflare_workers_rest import (
    CloudflareManagementDenied,
    CloudflareRestUnavailable,
    ConfiguredWorkersScript,
    WorkersDeployment,
    WorkersPaidUnverified,
    WorkersRestCleanup,
    WorkersRestProvisioner,
)
from src.services.worker_pool.external_client import (
    CapacityProtocolError,
    ExternalCapacityClient,
)

ENABLE_JOB = "capacity.cloudflare_enable"


class CloudflareEnablePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    installation_id: UUID
    revision: int = Field(gt=0)


class WorkersProvisioner(ProvisioningAdapter, Protocol):
    async def configure_endpoint(
        self, *, api_token: SecretStr, expected: ResourceObservation,
    ) -> ConfiguredWorkersScript: ...


async def _locked_current(db: AsyncSession, context: PlatformJobContext, payload: CloudflareEnablePayload) -> CloudflareInstallation | None:
    await _hold_installation_lease(db, context, payload.installation_id)
    row = await db.get(CloudflareInstallation, payload.installation_id, with_for_update=True)
    if row is None or row.retired_at is not None or row.desired_revision != payload.revision or row.desired_state != "enabled":
        return None
    return row


async def _journal(
    db: AsyncSession, installation: CloudflareInstallation, deployment: WorkersDeployment, *, actor: str,
) -> tuple[SecretStr, tuple[UUID, ...]]:
    revision = installation.desired_revision
    bearer_ref = CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", revision)
    bearer_row = await db.get(CloudflareCredential, (installation.id, "scope_bearer", "workers", revision))
    if bearer_row is None:
        bearer = SecretStr(secrets.token_urlsafe(48))
        db.add(seal_credential(bearer_ref, bearer, actor=actor, now=datetime.now(UTC)))
    else:
        bearer = open_credential(bearer_ref, bearer_row)
    identifiers = []
    for name in (deployment.gateway_name, deployment.launcher_name, deployment.capacity_name):
        resource = await db.scalar(select(CloudflareOwnedResource).where(
            CloudflareOwnedResource.installation_id == installation.id,
            CloudflareOwnedResource.kind == "worker_script",
            CloudflareOwnedResource.target_name == name,
        ))
        if resource is None:
            resource = CloudflareOwnedResource(
                id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
                scope="workers", kind="worker_script", target_name=name, desired_revision=revision,
                artifact_digest=deployment.release.digest, effect_id=uuid4(), state="creating",
            )
            db.add(resource)
        elif resource.state not in ("creating", "present") or resource.artifact_digest != deployment.release.digest:
            raise OwnershipConflict("The current deployment intent is incompatible with the release")
        identifiers.append(resource.id)
    return bearer, tuple(identifiers)


async def _install_script(
    adapter: WorkersProvisioner, resource: CloudflareOwnedResource, *,
    token: SecretStr, bearer: SecretStr | None,
) -> ConfiguredWorkersScript:
    intent = _resource_intent(resource)
    if resource.state == "creating":
        observed = await resume_creation(adapter, api_token=token, scope_bearer=bearer, committed_intent=intent)
    elif resource.state == "present" and resource.provider_id and resource.provider_version:
        recorded = ResourceObservation(intent, resource.provider_id, resource.provider_version)
        observed = await observe_owned(adapter, api_token=token, intent=intent, recorded=recorded)
        if observed is None:
            raise ObservationPending("The recorded Worker is absent")
    else:
        raise OwnershipConflict("The Worker journal cannot be created in its current state")
    endpoint = await adapter.configure_endpoint(api_token=token, expected=observed)
    if resource.namespace_ids and resource.namespace_ids != endpoint.namespaces:
        raise OwnershipConflict("Cloudflare namespace identity changed")
    resource.state = "present"
    resource.provider_id = observed.provider_id
    resource.provider_version = observed.provider_version
    resource.namespace_ids = endpoint.namespaces
    resource.observed_at = datetime.now(UTC)
    return endpoint


async def _settle_enable(
    context: PlatformJobContext, payload: CloudflareEnablePayload, http: httpx.AsyncClient,
    release: WorkersRelease,
) -> dict[str, str]:
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return {"outcome": "superseded"}
        upgrading = row.upgrade_release is not None
        if (row.upgrade_release if upgrading else row.policy.get("bounds_release")) != release.digest:
            raise PlatformJobRequiresAction("Review Policy for the Packaged Workers Release", {"code": "CF_POLICY_RELEASE_CHANGED"})
        replacement_policy = None
        if upgrading:
            try:
                replacement_policy = _price_policy(
                    CloudflareCapacityPolicy.model_validate(row.policy["limits"]), release_meter_bounds(release),
                )
            except ValueError as exc:
                raise PlatformJobRequiresAction(
                    "Review Policy for the Packaged Workers Release",
                    {"code": "CF_POLICY_RELEASE_CHANGED", "reason": str(exc)},
                ) from None
        installation = row
        management_token = await _management_token(db, row)
    cleanup_adapter = WorkersRestCleanup(http)
    await cleanup_adapter.verify_workers_paid(installation.account_id, management_token)
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None or row.api_token_version != installation.api_token_version:
            return {"outcome": "superseded"}
        if not upgrading:
            await _require_budget(db, row)
        if row.admitted_revision == payload.revision:
            return {"outcome": "enabled"}
        await _require_legacy_cleanup(db)
    if upgrading:
        await context.report("Draining Cloudflare Capacity")
    if not await _drain_previous_revision(context, payload, cleanup_adapter, upgrading=upgrading):
        return {"outcome": "superseded"}

    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return {"outcome": "superseded"}
        await db.execute(update(CloudflareCredential).where(
            CloudflareCredential.installation_id == row.id,
            CloudflareCredential.purpose == "scope_bearer",
            CloudflareCredential.scope == "workers",
            CloudflareCredential.version < payload.revision,
            CloudflareCredential.ciphertext.is_not(None),
        ).values(ciphertext=None))
        if replacement_policy is not None:
            row.policy = replacement_policy
            await db.flush()
        await _require_budget(db, row)
        prefix = f"bifrost-{row.id.hex}-{payload.revision:x}"
        deployment = WorkersDeployment(
            release, row.worker_ingress_origin, f"{prefix}-launcher", f"{prefix}-capacity",
            CloudflareCapacityPolicy.model_validate(row.policy["limits"]).workers,
            gateway_name=f"{prefix}-gateway",
        )
        bearer, identifiers = await _journal(db, row, deployment, actor=context.requested_by_user_id)

    await context.report("Deploying Cloudflare Workers")
    adapter = WorkersRestProvisioner(http, deployment)
    capacity_endpoint = None
    for identifier in identifiers:
        # An upload is an unconditional PUT. Keep the revision/effect fence
        # until its bounded response and receipt commit, so Disconnect cannot
        # verify absence and tombstone an intent that can still be uploaded.
        async with get_db_context() as db:
            row = await _locked_current(db, context, payload)
            if row is None:
                return {"outcome": "superseded"}
            resource = await db.get(CloudflareOwnedResource, identifier, with_for_update=True)
            if resource is None:
                raise OwnershipConflict("The Worker intent disappeared")
            is_capacity = resource.target_name == deployment.capacity_name
            result = await _install_script(
                adapter, resource, token=await _management_token(db, row),
                bearer=bearer if is_capacity else None,
            )
            if is_capacity:
                capacity_endpoint = result.endpoint
    if capacity_endpoint is None:
        raise ObservationPending("Cloudflare capacity has no confirmed public endpoint")

    await context.report("Checking Worker Ingress")
    capacity_cpu_ms = release.manifest.meter_bounds.qualification.expected_cpu_ms["capacity"]
    client = ExternalCapacityClient(
        capacity_endpoint, bearer.get_secret_value(), http=http,
        reserve_request=capacity_request_reservation(payload.installation_id, capacity_cpu_ms),
    )
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return {"outcome": "superseded"}
        scope = await db.get(CloudflareCapacityScope, (row.id, "workers", payload.revision))
        if scope is None:
            if await cloudflare_workers_remain(db, row.id, before_revision=payload.revision):
                raise PlatformJobRequiresAction("Wait for Cloudflare Workers to Retire", {"code": "CF_PRIOR_WORKERS_LIVE"})
            db.add(CloudflareCapacityScope(
                installation_id=row.id, scope="workers", deployment_revision=payload.revision,
                endpoint=capacity_endpoint, bearer_version=payload.revision,
                capacity_cpu_ms=capacity_cpu_ms,
                gateway_required=True,
            ))
        elif scope.endpoint != capacity_endpoint or scope.bearer_version != payload.revision:
            raise OwnershipConflict("Cloudflare scope differs from the verified deployment")

    async def canary(origin: str):
        return await run_workers_canary(context, payload.installation_id, payload.revision, origin, client, deployment)

    ingress = await probe_worker_ingress(installation, runtime_canary=canary)
    if not ingress.ready:
        failure = ingress.failure
        abbreviations = {"ack", "dns", "http", "sdk", "tls"}
        stage, reason = (
            " ".join(word.upper() if word in abbreviations else word.title() for word in code.split("_"))
            for code in (failure.stage, failure.error_class)
        )
        if failure.status is not None:
            reason += f", HTTP {failure.status}"
        label = "Worker Ingress Probe Failed" if failure.stage.startswith("ingress_") else "Cloudflare Canary Failed"
        raise PlatformJobRequiresAction(
            f"{label} ({stage}, {reason})",
            {"code": "CF_WORKER_INGRESS_UNVERIFIED", "canary_failure": failure.as_result()},
        )
    await context.report("Checking Cloudflare Spend")
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return {"outcome": "superseded"}
        if upgrading:
            await _observe_inventory(
                cleanup_adapter, await _management_token(db, row),
                await _inventory_snapshot(db, row.id, revision=payload.revision),
            )
        await _require_budget(db, row)
        row.admitted_revision = payload.revision
        row.deployed_revision = payload.revision
        row.upgrade_release = None
        row.upgrade_drained_at = None
        wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
    return {"outcome": "enabled"}


async def _drain_previous_revision(
    context: PlatformJobContext, payload: CloudflareEnablePayload, adapter: WorkersRestCleanup,
    *, upgrading: bool,
) -> bool:
    while True:
        async with get_db_context() as db:
            row = await _locked_current(db, context, payload)
            if row is None:
                return False
            if upgrading and (row.upgrade_release is None or row.admitted_revision is not None):
                raise OwnershipConflict("Cloudflare upgrade no longer owns the paused revision")
            if row.upgrade_drained_at is not None:
                return True
            workers_remain = await cloudflare_workers_remain(db, row.id, before_revision=payload.revision)
            if workers_remain:
                result = await _cleanup_stranded_workflow_slots(db, row, adapter, before_revision=payload.revision)
            else:
                try:
                    result = await _cleanup_one(db, row, adapter, before_revision=payload.revision)
                except DeletionSettling:
                    result = "settling"
                if result == "complete":
                    await _remove_verified_scopes(db, row, before_revision=payload.revision)
                    if upgrading:
                        row.upgrade_drained_at = datetime.now(UTC)
                    if row.deployed_revision != payload.revision:
                        row.deployed_revision = None
                    return True
            if result == "progressed":
                continue
            if not upgrading:
                if workers_remain and result == "settling":
                    raise PlatformJobFailure("CF_CLEANUP_SETTLING", "Verifying Prior Cloudflare Deletion", retryable=True)
                if workers_remain:
                    raise PlatformJobRequiresAction("Wait for Cloudflare Workers to Retire", {"code": "CF_PRIOR_WORKERS_LIVE"})
                raise PlatformJobRequiresAction("Wait for Cloudflare Creation to Settle", {"code": "CF_CLEANUP_PENDING"})
            started_at = await db.scalar(select(PlatformJob.started_at).where(PlatformJob.id == context.job_id))
            if started_at is not None and datetime.now(UTC) >= started_at + DRAIN_DEADLINE:
                if result == "settling":
                    raise PlatformJobRequiresAction("Cloudflare Cleanup Pending", {"code": "CF_CLEANUP_PENDING"})
                raise await _drain_timeout_action(db, row.id, workers_remain=workers_remain)
        await sleep(2)


async def _require_budget(db: AsyncSession, row: CloudflareInstallation) -> None:
    cost = await db.scalar(select(cloudflare_estimated_usage()).where(CloudflareInstallation.id == row.id))
    if cost is None:
        raise PlatformJobRequiresAction("Verify Cloudflare Spend", {"code": "CF_SPEND_UNVERIFIED"})
    if cost >= CloudflareCapacityPolicy.model_validate(row.policy["limits"]).monthly_limit_usd:
        raise PlatformJobRequiresAction("Cloudflare Budget Reached", {"code": "CF_BUDGET_REACHED"})


async def _enable(context: PlatformJobContext, parsed: BaseModel) -> dict[str, str]:
    payload = CloudflareEnablePayload.model_validate(parsed)
    await context.report("Checking Cloudflare Release and Entitlement")
    try:
        release = load_workers_release()
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            return await _settle_enable(context, payload, http, release)
    except WorkersReleaseUnavailable:
        raise PlatformJobRequiresAction("Install a Qualified Workers Release", {"code": "CF_RELEASE_UNAVAILABLE"}) from None
    except WorkersPaidUnverified as exc:
        raise PlatformJobRequiresAction(str(exc), {"code": "CF_WORKERS_PAID_UNVERIFIED"}) from None
    except OwnershipConflict:
        raise PlatformJobRequiresAction("Verify Cloudflare Ownership", {"code": "CF_OWNERSHIP_CONFLICT"}) from None
    except (ObservationPending, CapacityProtocolError):
        raise PlatformJobRequiresAction("Verify Cloudflare Deployment", {"code": "CF_DEPLOYMENT_UNVERIFIED"}) from None
    except CloudflareManagementDenied:
        raise PlatformJobRequiresAction("Reconnect Cloudflare", {"code": "CF_MANAGEMENT_CREDENTIAL"}) from None
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status in (408, 429) or status >= 500:
            raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare Is Temporarily Unavailable", retryable=True) from None
        raise PlatformJobRequiresAction("Verify Cloudflare Deployment", {"code": "CF_DEPLOYMENT_UNVERIFIED"}) from None
    except (CloudflareRestUnavailable, httpx.TransportError):
        raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare Is Temporarily Unavailable", retryable=True) from None


CLOUDFLARE_ENABLE_DEFINITION = define_platform_job(ENABLE_JOB, CloudflareEnablePayload, _enable)


async def enqueue_cloudflare_enable(
    db: AsyncSession, caller: Caller, *, installation_id: UUID, expected_revision: int,
) -> tuple[PlatformJob, bool]:
    require_operation(caller, "capacity.cloudflare_enable", GLOBAL)
    actor = caller.principal
    row = await db.get(CloudflareInstallation, installation_id, with_for_update=True, populate_existing=True)
    if row is None or row.retired_at is not None or row.desired_revision != expected_revision or row.desired_state == "disconnected":
        raise CloudflarePolicyConflict("Cloudflare installation changed. Reload its current policy.")
    if row.upgrade_release is not None:
        raise CloudflarePolicyConflict("Reconcile the pending Cloudflare upgrade.")
    row.desired_state = "enabled"
    payload = CloudflareEnablePayload(installation_id=row.id, revision=row.desired_revision)
    job, reused = await enqueue_platform_job(
        db, CLOUDFLARE_ENABLE_DEFINITION, payload, dedupe_key=f"{row.id}:{payload.revision}",
        resource_lock_key=f"cloudflare-installation:{row.id}",
        organization_id=None, requested_by_user_id=actor.user_id, requested_by_email=actor.email,
        requested_by_name=actor.name, resource_type="cloudflare_installation", resource_id=str(row.id),
        title="Enable Cloudflare", action_url=None,
    )
    if not reused:
        await emit_audit(
            db, "capacity.cloudflare_enable", resource_type="cloudflare_installation", resource_id=row.id,
            details={"revision": payload.revision, "job_id": str(job.id)},
        )
    return job, reused
