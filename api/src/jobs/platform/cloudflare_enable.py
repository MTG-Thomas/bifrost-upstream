"""Provision and admit one exact, verified Workers deployment."""

import secrets
from asyncio import sleep, timeout
from datetime import UTC, datetime
from decimal import Decimal, ROUND_CEILING
from time import monotonic
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from shared.models import CloudflareCapacityPolicy
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.core.database import get_db_context
from src.jobs.platform.base import (
    PlatformJobContext,
    PlatformJobFailure,
    PlatformJobRequiresAction,
    define_platform_job,
)
from src.jobs.platform.cloudflare_capacity import (
    CREATE_SETTLEMENT_WINDOW,
    DISABLE_JOB,
    DRAIN_DEADLINE,
    RELEASE_FENCE_REMEDY,
    CloudflareDisablePayload,
    _cleanup_one,
    _cleanup_stranded_workflow_slots,
    _container_create_absence_settled,
    _container_create_may_exist,
    _drain_timeout_action,
    _hold_installation_lease,
    _inventory_snapshot,
    _management_token,
    _next_cleanup_is_image,
    _observe_inventory,
    _remove_verified_scopes,
    _require_legacy_cleanup,
    _resource_intent,
    _wait_for_connector,
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
from src.services.authorization.enforce import Caller, require_operation
from src.services.authorization.evaluator import GLOBAL
from src.services.kubernetes_control import may_manage_jobs
from src.services.platform_jobs import enqueue_platform_job, find_active_platform_job
from src.services.work_dispatch import (
    WAKE_CLOUDFLARE_CAPACITY,
    wake_pool_workers_after_commit,
)
from src.services.worker_ingress_probes import CanaryFailure, probe_worker_ingress
from src.services.worker_pool import cloudflare_tunnel
from src.services.worker_pool.cloudflare_admission import PrimaryPool, joint_container_readiness
from src.services.worker_pool.cloudflare_canary import CANARY_DEADLINE_SECONDS, canary_session_failure, run_workers_canary
from src.services.worker_pool.cloudflare_containers_admission import ContainerAdmissionRefused, reserve_container_canary
from src.services.worker_pool.cloudflare_containers_control import reconcile_container_stop
from src.services.worker_pool.cloudflare_containers_egress import (
    EGRESS_PROOF_MAX_AGE,
    EGRESS_REFRESH_KEY,
    PENDING_QUALIFICATION_KEY,
    QUALIFICATION_BACKOFF_CODE,
    QUALIFICATION_BACKOFF_KEY,
    ContainerEgressMismatch,
    VerifiedEgress,
    current_container_application,
    read_egress_refresh,
    read_pending_qualification,
    read_verified_egress,
    record_egress_refresh_failure,
    verify_container_egress,
)
from src.services.worker_pool.cloudflare_containers_policy import (
    CONTAINERS_QUALIFICATION_DEFERRED,
    CONTAINERS_SCOPE,
    QUALIFICATION_DEFERRED_KEY,
    QUALIFICATION_DEFERRED_RELEASE_KEY,
    ContainersPolicy,
    container_qualification_evidence,
    fleet_envelope,
    qualification_deferred,
)
from src.services.worker_pool.cloudflare_containers_release import (
    ContainersReleaseFetchError,
    ContainersReleaseUnavailable,
    VerifiedContainerRelease,
    load_containers_release,
)
from src.services.worker_pool.cloudflare_containers_rest import ContainerLauncherRest, ContainerRegistry, ContainersRest
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
    set_workers_release_floor,
    workers_release_error,
    workers_release_floor,
)
from src.services.worker_pool.cloudflare_provisioning import (
    DeletionSettling,
    ObservationCheck,
    ObservationPending,
    ObservationSettling,
    OwnershipCheck,
    OwnershipConflict,
    OwnershipResource,
    ProvisioningAdapter,
    ResourceObservation,
    observe_owned,
    request_creation,
)
from src.services.worker_pool.cloudflare_release import (
    WorkersRelease,
    WorkersReleaseOrder,
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
    CapacityProtocolCheck,
    CapacityProtocolError,
    ExternalCapacityClient,
)

ENABLE_JOB = "capacity.cloudflare_enable"


class CloudflareEnablePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    installation_id: UUID
    revision: int = Field(gt=0)


class _EarlierContainerCreate(ObservationSettling):
    """A committed empty read has started the previous attempt's absence window."""


class WorkersProvisioner(ProvisioningAdapter, Protocol):
    async def configure_endpoint(
        self, *, api_token: SecretStr, expected: ResourceObservation,
    ) -> ConfiguredWorkersScript: ...


async def _locked_current(
    db: AsyncSession, context: PlatformJobContext, payload: CloudflareEnablePayload, *, lock_row: bool = True,
) -> CloudflareInstallation | None:
    await _hold_installation_lease(db, context, payload.installation_id)
    row = await db.get(
        CloudflareInstallation, payload.installation_id, with_for_update=lock_row, populate_existing=True,
    )
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
            raise OwnershipConflict(
                "The current deployment intent is incompatible with the release",
                OwnershipResource.WORKER_SCRIPT, OwnershipCheck.RELEASE_INTENT,
            )
        identifiers.append(resource.id)
    return bearer, tuple(identifiers)


async def _install_script(
    adapter: WorkersProvisioner, resource: CloudflareOwnedResource, *,
    token: SecretStr,
) -> ConfiguredWorkersScript:
    intent = _resource_intent(resource)
    recorded = None
    if resource.provider_id and resource.provider_version:
        recorded = ResourceObservation(intent, resource.provider_id, resource.provider_version)
    if resource.state != "creating" and (resource.state != "present" or recorded is None):
        raise OwnershipConflict(
            "The Worker journal cannot be created in its current state",
            OwnershipResource.WORKER_SCRIPT, OwnershipCheck.JOURNAL_STATE,
        )
    observed = await observe_owned(adapter, api_token=token, intent=intent, recorded=recorded)
    if observed is None:
        if recorded is None:
            raise ObservationSettling(
                "The acknowledged Worker upload is not yet visible",
                OwnershipResource.WORKER_SCRIPT, ObservationCheck.SCRIPT_UPLOAD_PENDING,
            )
        raise ObservationPending(
            "The recorded Worker is absent",
            OwnershipResource.WORKER_SCRIPT, ObservationCheck.SCRIPT_RECORDED_ABSENT,
        )
    # Keep the exact version for a successor attempt even if namespaces have not settled.
    resource.provider_id = observed.provider_id
    resource.provider_version = observed.provider_version
    endpoint = await adapter.configure_endpoint(api_token=token, expected=observed)
    if resource.namespace_ids and resource.namespace_ids != endpoint.namespaces:
        raise OwnershipConflict(
            "Cloudflare namespace identity changed",
            OwnershipResource.DURABLE_OBJECT_NAMESPACE, OwnershipCheck.RECEIPT_MISMATCH,
        )
    resource.state = "present"
    resource.namespace_ids = endpoint.namespaces
    resource.observed_at = datetime.now(UTC)
    return endpoint


async def _settle_enable(
    context: PlatformJobContext, payload: CloudflareEnablePayload, http: httpx.AsyncClient,
    release: WorkersRelease, *, repair_tunnel_ingress: bool = False, refresh_container_egress: bool = False,
) -> dict[str, Any]:
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return {"outcome": "superseded"}
        joint_ready = await db.scalar(select(joint_container_readiness(payload.revision)).where(
            CloudflareInstallation.id == row.id,
        ))
        containers_enabled = row.containers_policy.get("enabled") is True
        deferred = qualification_deferred(row) and row.admitted_revision == payload.revision
        if joint_ready is not True and row.admitted_revision is not None and not deferred:
            launcher = await _container_launcher_namespace(db, row) if containers_enabled else None
            row.admitted_revision = None
            wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
            # Commit the closure before returning the operator action.
            await db.commit()
            if launcher is None:
                raise PlatformJobRequiresAction("Verify Cloudflare Containers Readiness", {
                    "code": "CF_CONTAINERS_UNVERIFIED",
                })
        upgrading = row.upgrade_release is not None
        order = WorkersReleaseOrder(release.manifest.release_version, release.manifest.release_sequence)
        if code := workers_release_error(row, release.digest, order):
            raise PlatformJobRequiresAction("Review the Workers Release Fence", {
                "code": code, "instructions": [RELEASE_FENCE_REMEDY],
            })
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
        container_digest = row.containers_release_digest if row.containers_ready_revision == payload.revision else None
        management_token = await _management_token(db, row)
    container_release = None
    if containers_enabled:
        try:
            container_release = await load_containers_release(http, release)
            matched_release = container_digest is None or container_release.digest == container_digest
        except ContainersReleaseFetchError:
            raise PlatformJobFailure(
                "CF_CONTAINERS_RELEASE_FETCH", "Waiting for the Cloudflare Linux Release", retryable=True,
            ) from None
        except ContainersReleaseUnavailable:
            matched_release = False
        if not matched_release:
            async with get_db_context() as db:
                row = await _locked_current(db, context, payload)
                if row is None:
                    return {"outcome": "superseded"}
                row.admitted_revision = None
                evidence = dict(row.deployment_evidence or {})
                evidence.pop(QUALIFICATION_DEFERRED_KEY, None)
                evidence.pop(QUALIFICATION_DEFERRED_RELEASE_KEY, None)
                row.deployment_evidence = evidence
                wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
            raise PlatformJobRequiresAction("Install the Matched Cloudflare Linux Release", {
                "code": "CF_CONTAINERS_RELEASE_UNAVAILABLE",
            })
    cleanup_adapter = WorkersRestCleanup(http)
    container_adapter = ContainersRest(http)
    await cleanup_adapter.verify_workers_paid(installation.account_id, management_token)
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None or row.api_token_version != installation.api_token_version:
            return {"outcome": "superseded"}
        if not upgrading:
            await _require_budget(db, row)
        already_admitted = row.admitted_revision == payload.revision
        refresh = (
            already_admitted and container_release is not None and refresh_container_egress
            and (deferred or read_verified_egress(row, datetime.now(UTC)) is None)
        )
        if already_admitted and not refresh:
            return {"outcome": "enabled"}
        if not already_admitted:
            await _require_legacy_cleanup(db)
    if refresh and container_release is not None:
        if deferred:
            await context.report("Qualifying Deferred Cloudflare Container Ingress")
            result = await _qualify_container_revision(
                context, payload, http, container_adapter, management_token, container_release,
                recovering_deferred=True,
            )
            return {"outcome": "container_qualification_deferred" if result == "deferred" else (
                "enabled" if result is True else "superseded"
            )}
        from src.services.worker_pool.cloudflare_supervisor import read_primary_pool

        settings = get_settings()
        try:
            primary, _ = await read_primary_pool(settings, http, kubernetes=await may_manage_jobs(settings))
        except (CapacityProtocolError, httpx.HTTPError):
            primary = None
        if primary is None:
            return {"outcome": "container_egress_refresh_pending"}
        await context.report("Checking Cloudflare Container Ingress")
        outcome = await _qualify_container_revision(
            context, payload, http, container_adapter, management_token, container_release, refresh_primary=primary,
        )
        if outcome is None:
            return {"outcome": "container_egress_refresh_pending"}
        if outcome in ("failed", "backoff"):
            return {"outcome": "container_egress_refresh_" + outcome}
        return {"outcome": "enabled" if outcome else "superseded"}
    if installation.reachability_mode == "managed_tunnel":
        await context.report("Provisioning Managed Tunnel")
        async with get_db_context() as db:
            row = await _locked_current(db, context, payload)
            if row is None:
                return {"outcome": "superseded"}
            await cloudflare_tunnel.prepare(db, row, actor=context.requested_by_user_id)
        async with get_db_context() as db:
            row = await _locked_current(db, context, payload)
            if row is None:
                return {"outcome": "superseded"}
            tunnel = cloudflare_tunnel.adapter(http, row, await _management_token(db, row))
            await cloudflare_tunnel.provision_tunnel(db, row, tunnel, repair_ingress=repair_tunnel_ingress)
        async with get_db_context() as db:
            row = await _locked_current(db, context, payload)
            if row is None:
                return {"outcome": "superseded"}
            tunnel = cloudflare_tunnel.adapter(http, row, await _management_token(db, row))
            await cloudflare_tunnel.provision_dns(db, row, tunnel, actor=context.requested_by_user_id)
        await context.report("Checking Managed Tunnel Connector")

        async def current_connector_revision(db: AsyncSession) -> CloudflareInstallation | None:
            return await _locked_current(db, context, payload)

        if not await _wait_for_connector(current_connector_revision, http):
            return {"outcome": "superseded"}

    if upgrading:
        await context.report("Draining Cloudflare Capacity")
    if not await _drain_previous_revision(
        context, payload, cleanup_adapter, upgrading=upgrading, container_adapter=container_adapter,
    ):
        return {"outcome": "superseded"}

    if container_release is not None:
        await context.report("Deploying Cloudflare Container Launcher")
        if not await _deploy_container_launcher(context, payload, http, container_adapter, container_release):
            return {"outcome": "superseded"}
        await context.report("Provisioning Cloudflare Container Application")
        if not await _wait_for_container_application(context, payload, container_adapter, container_release):
            return {"outcome": "superseded"}
        await context.report("Qualifying Cloudflare Container Ingress")
        qualification = await _qualify_container_revision(
            context, payload, http, container_adapter, management_token, container_release,
        )
        if not qualification:
            return {"outcome": "superseded"}
        deferred = qualification == "deferred"

    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return {"outcome": "superseded"}
        if container_release is not None and not deferred and (
            row.containers_release_digest != container_release.digest
            or await db.scalar(select(joint_container_readiness(payload.revision)).where(
                CloudflareInstallation.id == row.id,
            )) is not True
        ):
            raise PlatformJobRequiresAction("Verify Cloudflare Containers Readiness", {
                "code": "CF_CONTAINERS_UNVERIFIED",
            })
        await db.execute(update(CloudflareCredential).where(
            CloudflareCredential.installation_id == row.id,
            CloudflareCredential.purpose == "scope_bearer",
            CloudflareCredential.scope == "workers",
            CloudflareCredential.version < payload.revision,
            CloudflareCredential.ciphertext.is_not(None),
        ).values(ciphertext=None))
        if replacement_policy is not None:
            row.policy = {**row.policy, **replacement_policy}
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
        # until its bounded response commits, so Disconnect cannot
        # verify absence and tombstone an intent that can still be uploaded.
        async with get_db_context() as db:
            row = await _locked_current(db, context, payload)
            if row is None:
                return {"outcome": "superseded"}
            resource = await db.get(CloudflareOwnedResource, identifier, with_for_update=True)
            if resource is None:
                raise OwnershipConflict(
                    "The Worker intent disappeared",
                    OwnershipResource.WORKER_SCRIPT, OwnershipCheck.JOURNAL_INTENT,
                )
            is_capacity = resource.target_name == deployment.capacity_name
            if resource.state == "creating" and resource.provider_details.get("upload_acknowledged") is not True:
                await request_creation(
                    adapter, api_token=await _management_token(db, row),
                    scope_bearer=bearer if is_capacity else None, committed_intent=_resource_intent(resource),
                )
                resource.provider_details = {**resource.provider_details, "upload_acknowledged": True}

        pending_readback = None
        async with get_db_context() as db:
            row = await _locked_current(db, context, payload)
            if row is None:
                return {"outcome": "superseded"}
            resource = await db.get(CloudflareOwnedResource, identifier, with_for_update=True)
            if resource is None:
                raise OwnershipConflict(
                    "The Worker intent disappeared",
                    OwnershipResource.WORKER_SCRIPT, OwnershipCheck.JOURNAL_INTENT,
                )
            try:
                result = await _install_script(adapter, resource, token=await _management_token(db, row))
            except ObservationSettling as exc:
                pending_readback = exc
            else:
                if is_capacity:
                    capacity_endpoint = result.endpoint
        if pending_readback is not None:
            raise pending_readback
    if capacity_endpoint is None:
        raise ObservationPending(
            "Cloudflare capacity has no confirmed public endpoint",
            OwnershipResource.WORKERS_SCOPE, ObservationCheck.SCOPE_ENDPOINT,
        )

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
            raise OwnershipConflict(
                "Cloudflare scope differs from the verified deployment",
                OwnershipResource.WORKERS_SCOPE, OwnershipCheck.RECEIPT_MISMATCH,
            )

    async def canary(origin: str):
        return await run_workers_canary(context, payload.installation_id, payload.revision, origin, client, deployment)

    ingress = await probe_worker_ingress(installation, runtime_canary=canary)
    probe_completed_at = datetime.now(UTC)
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
            {
                "code": "CF_WORKER_INGRESS_UNVERIFIED",
                "canary_failure": failure.as_result(),
                "http_connections": ingress.http_connections_as_result(),
            },
        )
    await context.report("Checking Cloudflare Spend")
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return {"outcome": "superseded"}
        joint_ready = await db.scalar(select(joint_container_readiness(payload.revision)).where(
            CloudflareInstallation.id == row.id,
        ))
        if not deferred and (joint_ready is not True or (
            row.containers_policy.get("enabled")
            and (container_release is None or row.containers_release_digest != container_release.digest)
        )):
            raise PlatformJobRequiresAction("Verify Cloudflare Containers Readiness", {
                "code": "CF_CONTAINERS_UNVERIFIED",
            })
        if upgrading:
            await _observe_inventory(
                cleanup_adapter, await _management_token(db, row),
                await _inventory_snapshot(db, row.id, revision=payload.revision),
            )
        await _require_budget(db, row)
        if container_release is not None:
            floor = fleet_envelope(
                ContainersPolicy.model_validate(row.containers_policy), container_release.manifest.rates, 31 * 86400,
            ).quantize(Decimal("0.01"), rounding=ROUND_CEILING)
            row.policy = {**row.policy, "containers_fleet_envelope_usd": str(floor)}
        evidence = {
            **(row.deployment_evidence or {}),
            "revision": payload.revision,
            "ingress_verified_at": (ingress.verified_at or probe_completed_at).isoformat(),
            "signed_canary_at": (
                (ingress.runtime_canary.verified_at if ingress.runtime_canary else None) or probe_completed_at
            ).isoformat(),
        }
        if deferred:
            assert container_release is not None
            row.containers_ready_revision = None
            row.containers_release_digest = None
            evidence[QUALIFICATION_DEFERRED_KEY] = payload.revision
            evidence[QUALIFICATION_DEFERRED_RELEASE_KEY] = container_release.digest
        row.deployment_evidence = evidence
        row.admitted_revision = payload.revision
        row.deployed_revision = payload.revision
        floor = workers_release_floor(row)
        if floor is None or order.key > floor.key:
            set_workers_release_floor(row, release.digest, order)
        row.drain_required = False
        row.upgrade_release = None
        row.upgrade_drained_at = None
        wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
    return {
        "outcome": "enabled", "http_connections": ingress.http_connections_as_result(),
        **({"containers_qualification_deferred": True} if deferred else {}),
    }


async def _qualify_container_revision(
    context: PlatformJobContext, payload: CloudflareEnablePayload, http: httpx.AsyncClient,
    adapter: ContainersRest, management_token: SecretStr, release: VerifiedContainerRelease,
    *, refresh_primary: PrimaryPool | None = None, recovering_deferred: bool = False,
) -> bool | Literal["failed", "backoff", "deferred"] | None:
    refresh = refresh_primary is not None
    async with get_db_context() as db:
        installation = await _locked_current(db, context, payload)
        if installation is None:
            return False
        if refresh and installation.admitted_revision != payload.revision:
            return False
        if recovering_deferred and (
            installation.admitted_revision != payload.revision or not qualification_deferred(installation)
        ):
            return False
        if not refresh and installation.containers_release_digest == release.digest and await db.scalar(
            select(joint_container_readiness(payload.revision)).where(CloudflareInstallation.id == installation.id),
        ):
            return True
        now = datetime.now(UTC)
        pending = read_pending_qualification(installation, now) if not refresh else None
        if pending is not None and pending.release_digest != release.digest:
            pending = None
        retry = read_egress_refresh(installation, qualification=not refresh)
        origin = installation.worker_ingress_origin
        pin = CloudflareWorkerReservation
        prior = tuple(await db.scalars(select(pin.worker_id).join(
            ExecutionWorker, ExecutionWorker.id == pin.worker_id,
        ).where(
            pin.installation_id == installation.id, pin.scope == CONTAINERS_SCOPE,
            pin.deployment_revision == payload.revision, pin.canary_job_id.is_not(None),
            ExecutionWorker.capacity_resource_deleted_at.is_(None),
        )))

    async def retire(worker_id: UUID) -> None:
        async with get_db_context() as db:
            await db.execute(update(ExecutionWorker).where(
                ExecutionWorker.id == worker_id, ExecutionWorker.state.in_(("starting", "ready")),
            ).values(state="draining"))
        # A declined start may have a 15 s start window and a 60 s independent
        # boot observation window. No DB locks span a placement settling wait.
        try:
            async with timeout(80):
                while True:
                    try:
                        if await reconcile_container_stop(http, adapter, management_token, worker_id):
                            return
                    except (ObservationPending, CloudflareRestUnavailable, httpx.TransportError):
                        pass
                    await sleep(1)
        except TimeoutError:
            raise PlatformJobFailure(
                "CF_CONTAINERS_CLEANUP_PENDING", "Waiting for the Container Canary to Retire", retryable=True,
            ) from None

    for prior_worker in prior:
        await retire(prior_worker)
    if pending is not None and read_pending_qualification(installation, datetime.now(UTC)) is not None:
        return await _commit_container_egress(context, payload, release, pending, refresh=False)
    if retry is not None and retry.retry_at > datetime.now(UTC):
        if recovering_deferred:
            return "deferred"
        if refresh:
            return "backoff"
        retry_at = retry.retry_at.isoformat()
        raise PlatformJobRequiresAction("Cloudflare Container Qualification Is Cooling Down", {
            "code": QUALIFICATION_BACKOFF_CODE, "retry_at": retry_at,
            "instructions": [f"Retry this capacity action at or after {retry_at}."],
        })
    async with get_db_context() as db:
        reserved = await reserve_container_canary(
            db, installation_id=payload.installation_id, controller_id=context.job_id,
            job_id=context.job_id, lease_token=context.lease_token, release=release, refresh_primary=refresh_primary,
        )
    if not isinstance(reserved, CloudflareWorkerReservation):
        if not refresh and isinstance(reserved, ContainerAdmissionRefused) and reserved.code == CONTAINERS_QUALIFICATION_DEFERRED:
            return "deferred"
        if recovering_deferred:
            return "deferred"
        if refresh:
            return None
        raise PlatformJobRequiresAction("Verify Cloudflare Containers Readiness", {"code": "CF_CONTAINERS_UNVERIFIED"})
    verified = None
    failure = None
    mismatch = False
    retirement_failure = None
    try:
        async with timeout(CANARY_DEADLINE_SECONDS):
            while verified is None:
                async with get_db_context() as db:
                    verified = await verify_container_egress(
                        db, http, installation_id=payload.installation_id, revision=payload.revision,
                        release_digest=release.digest, origin=origin, probe_worker_id=reserved.worker_id,
                    )
                    if verified is None:
                        witness = (await db.execute(select(
                            ExecutionWorker.remote_session_generation, PlatformJob.result,
                        ).select_from(ExecutionWorker).join(PlatformJob, PlatformJob.id == context.job_id).where(
                            ExecutionWorker.id == reserved.worker_id,
                            PlatformJob.lease_token == context.lease_token,
                        ))).one_or_none()
                        if witness is not None:
                            failure = canary_session_failure(witness.result or {}, reserved.worker_id,
                                                             witness.remote_session_generation)
                            if failure is not None and (failure.stage, failure.error_class) not in {
                                ("launcher", "connection_lost"), ("proof", "unavailable"),
                            }:
                                break
                if verified is None:
                    await sleep(0.5)
    except TimeoutError:
        failure = failure or CanaryFailure("ack", "timeout")
    except ContainerEgressMismatch:
        mismatch = True
    finally:
        if (mismatch and refresh) or (recovering_deferred and verified is None):
            async with get_db_context() as db:
                installation = await _locked_current(db, context, payload)
                if installation is not None:
                    installation.admitted_revision = installation.containers_ready_revision = None
                    installation.containers_release_digest = None
                    evidence = dict(installation.deployment_evidence or {})
                    evidence.pop(QUALIFICATION_DEFERRED_KEY, None)
                    evidence.pop(QUALIFICATION_DEFERRED_RELEASE_KEY, None)
                    installation.deployment_evidence = evidence
                    wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
        try:
            await retire(reserved.worker_id)
        except PlatformJobFailure as exc:
            if not refresh and not mismatch:
                if verified is None:
                    raise
                retirement_failure = exc
    if verified is None:
        if refresh and not mismatch:
            async with get_db_context() as db:
                installation = await _locked_current(db, context, payload)
                if installation is None or installation.admitted_revision != payload.revision:
                    return False
                record_egress_refresh_failure(installation, datetime.now(UTC))
            return "failed"
        raise PlatformJobRequiresAction("Verify Cloudflare Containers Readiness", {
            "code": "CF_CONTAINERS_UNVERIFIED",
            **({"canary_failure": failure.as_result()} if failure is not None else {}),
        })

    committed = await _commit_container_egress(
        context, payload, release, verified, refresh=refresh, retirement_pending=retirement_failure is not None,
    )
    if committed and retirement_failure is not None:
        raise retirement_failure
    return committed


async def _commit_container_egress(
    context: PlatformJobContext, payload: CloudflareEnablePayload, release: VerifiedContainerRelease,
    verified: VerifiedEgress, *, refresh: bool, retirement_pending: bool = False,
) -> bool:
    async with get_db_context() as db:
        installation = await _locked_current(db, context, payload)
        if installation is None:
            return False
        application = (await db.execute(current_container_application(
            installation, payload.revision, release.digest,
        ))).one_or_none()
        if (
            application != (verified.provider_id, verified.provider_version)
            or installation.worker_ingress_origin != verified.origin
            or verified.observed_at < datetime.now(UTC) - EGRESS_PROOF_MAX_AGE
        ):
            if not refresh and not qualification_deferred(installation):
                raise ObservationPending("Cloudflare Container application changed after ingress qualification")
            installation.admitted_revision = installation.containers_ready_revision = None
            installation.containers_release_digest = None
            evidence = dict(installation.deployment_evidence or {})
            evidence.pop(QUALIFICATION_DEFERRED_KEY, None)
            evidence.pop(QUALIFICATION_DEFERRED_RELEASE_KEY, None)
            installation.deployment_evidence = evidence
            wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
        else:
            evidence = dict(installation.deployment_evidence or {})
            if retirement_pending:
                evidence[PENDING_QUALIFICATION_KEY] = verified.as_record()
            else:
                installation.containers_ready_revision = payload.revision
                installation.containers_release_digest = release.digest
                for key in (
                    EGRESS_REFRESH_KEY, QUALIFICATION_BACKOFF_KEY, PENDING_QUALIFICATION_KEY,
                    QUALIFICATION_DEFERRED_KEY, QUALIFICATION_DEFERRED_RELEASE_KEY,
                ):
                    evidence.pop(key, None)
                evidence["container_egress"] = verified.as_record()
            installation.deployment_evidence = evidence
            return True
    raise PlatformJobRequiresAction("Verify Cloudflare Containers Readiness", {"code": "CF_CONTAINERS_UNVERIFIED"})


async def _container_launcher_namespace(
    db: AsyncSession, row: CloudflareInstallation,
) -> tuple[str, ResourceObservation] | None:
    resource = CloudflareOwnedResource
    launcher = await db.scalar(select(resource).where(
        resource.installation_id == row.id,
        resource.account_id == row.account_id,
        resource.scope == "containers",
        resource.kind == "worker_script",
        resource.desired_revision == row.desired_revision,
        resource.target_name == f"bifrost-{row.id.hex}-r{row.desired_revision}-containers",
        resource.state == "present",
        resource.provider_id.is_not(None),
        resource.provider_version.is_not(None),
        resource.observed_at.is_not(None),
    ))
    if launcher is None or launcher.provider_id is None or launcher.provider_version is None:
        return None
    namespace = launcher.namespace_ids.get("ContainerLauncher")
    if namespace is None:
        return None
    return namespace, ResourceObservation(_resource_intent(launcher), launcher.provider_id, launcher.provider_version)


async def _container_resource(
    db: AsyncSession, row: CloudflareInstallation, release: VerifiedContainerRelease,
    *, kind: str, name: str,
) -> CloudflareOwnedResource:
    resource = await db.scalar(select(CloudflareOwnedResource).where(
        CloudflareOwnedResource.installation_id == row.id,
        CloudflareOwnedResource.account_id == row.account_id,
        CloudflareOwnedResource.scope == "containers",
        CloudflareOwnedResource.kind == kind,
        CloudflareOwnedResource.target_name == name,
    ).with_for_update())
    if resource is None:
        resource = CloudflareOwnedResource(
            id=uuid4(), installation_id=row.id, account_id=row.account_id, scope="containers", kind=kind,
            target_name=name, desired_revision=row.desired_revision, artifact_digest=release.digest,
            effect_id=uuid4(), state="creating", provider_details={},
        )
        db.add(resource)
    elif (
        resource.state not in ("creating", "present") or resource.desired_revision != row.desired_revision
        or resource.artifact_digest != release.digest
    ):
        raise OwnershipConflict("Container resource differs from its release intent",
                                OwnershipResource(kind), OwnershipCheck.RELEASE_INTENT)
    return resource


async def _deploy_container_launcher(
    context: PlatformJobContext, payload: CloudflareEnablePayload, http: httpx.AsyncClient,
    adapter: ContainersRest, release: VerifiedContainerRelease,
) -> bool:
    registry = ContainerRegistry(adapter)
    name = f"bifrost-{payload.installation_id.hex}-r{payload.revision}"
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload, lock_row=False)
        if row is None:
            return False
        image = await _container_resource(db, row, release, kind="container_image", name=name)
        intent = _resource_intent(image)
        previously_sent = image.provider_details.get("creation_sent") is True
        if not previously_sent:
            await registry.require_empty(await _management_token(db, row), intent)
            image.provider_details = {"creation_sent": True, "image_digest": release.manifest.image_digest}
        image_id = image.id

    async with get_db_context() as db:
        row = await _locked_current(db, context, payload, lock_row=False)
        if row is None:
            return False
        image = await db.get(CloudflareOwnedResource, image_id, with_for_update=True)
        if (
            image is None or _resource_intent(image) != intent or image.state not in ("creating", "present")
            or image.provider_details.get("image_digest") != release.manifest.image_digest
        ):
            raise OwnershipConflict("Container image creation fence changed",
                                    OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.JOURNAL_STATE)
        token = await _management_token(db, row)
        prepared = None
        if image.state == "present":
            assert image.provider_id is not None and image.provider_version is not None
            prepared = ResourceObservation(intent, image.provider_id, image.provider_version)

    # The PlatformJob resource key serializes provisioning and cleanup.
    # No database transaction spans the copy or the cancellable settling wait.
    created = prepared is None
    if prepared is None:
        async def hold_image_lease() -> None:
            async with get_db_context() as db:
                await _hold_installation_lease(db, context, payload.installation_id)

        prepared = await registry.copy(
            token, intent, release, previously_sent=previously_sent, hold_lease=hold_image_lease,
        )

    async with get_db_context() as db:
        await _hold_installation_lease(db, context, payload.installation_id)
        image = await db.get(CloudflareOwnedResource, image_id, with_for_update=True)
        if (
            image is None or _resource_intent(image) != intent
            or image.state != ("creating" if created else "present")
            or image.provider_details.get("image_digest") != release.manifest.image_digest
        ):
            raise OwnershipConflict("Container image creation fence changed",
                                    OwnershipResource.CONTAINER_IMAGE, OwnershipCheck.JOURNAL_STATE)
        if created:
            image.provider_id = prepared.provider_id
            image.provider_version = prepared.provider_version
            image.state = "present"
            image.observed_at = datetime.now(UTC)
        # Commit any exact image receipt even if the desired state changed.
        # There is no authority to journal a launcher for a superseded job.
        row = await _locked_current(db, context, payload)
        if row is None:
            return False
        launcher = await _container_resource(db, row, release, kind="worker_script", name=f"{name}-containers")
        launcher_id = launcher.id
        origin = row.worker_ingress_origin
        policy = ContainersPolicy.model_validate(row.containers_policy)
        ref = CredentialRef(row.id, row.account_id, "scope_bearer", "containers", payload.revision)
        scope_credential = await db.get(CloudflareCredential, (row.id, ref.purpose, ref.scope, ref.version))
        if scope_credential is None:
            signing_secret = SecretStr(secrets.token_urlsafe(48))
            db.add(seal_credential(ref, signing_secret, actor=context.requested_by_user_id, now=datetime.now(UTC)))
        else:
            signing_secret = open_credential(ref, scope_credential)

    provisioner = ContainerLauncherRest(http, release, prepared, origin=origin, policy=policy, signing_secret=signing_secret)
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return False
        launcher = await db.get(CloudflareOwnedResource, launcher_id, with_for_update=True)
        if launcher is None:
            raise OwnershipConflict("Container launcher intent disappeared",
                                    OwnershipResource.WORKER_SCRIPT, OwnershipCheck.JOURNAL_INTENT)
        if launcher.state == "creating" and launcher.provider_details.get("upload_acknowledged") is not True:
            await request_creation(
                provisioner, api_token=await _management_token(db, row), scope_bearer=None,
                committed_intent=_resource_intent(launcher),
            )
            launcher.provider_details = {"upload_acknowledged": True}

    pending_readback = None
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return False
        launcher = await db.get(CloudflareOwnedResource, launcher_id, with_for_update=True)
        if launcher is None:
            raise OwnershipConflict("Container launcher intent disappeared",
                                    OwnershipResource.WORKER_SCRIPT, OwnershipCheck.JOURNAL_INTENT)
        try:
            result = await _install_script(provisioner, launcher, token=await _management_token(db, row))
            prior_endpoint = launcher.provider_details.get("endpoint")
            if result.endpoint is None or prior_endpoint not in (None, result.endpoint):
                raise OwnershipConflict("Container launcher endpoint differs from its receipt",
                                        OwnershipResource.WORKERS_ENDPOINT, OwnershipCheck.RECEIPT_MISMATCH)
            launcher.provider_details = {**launcher.provider_details, "endpoint": result.endpoint}
            # Qualification owns this route before joint readiness opens
            # the revision to the scheduler.
            scope = await db.get(CloudflareCapacityScope, (row.id, "containers", payload.revision))
            if scope is None:
                db.add(CloudflareCapacityScope(
                    installation_id=row.id, scope="containers", deployment_revision=payload.revision,
                    bearer_version=ref.version, endpoint=result.endpoint, gateway_required=True,
                ))
            elif scope.endpoint != result.endpoint or scope.bearer_version != ref.version:
                raise OwnershipConflict("Container scope differs from its launcher receipt",
                                        OwnershipResource.WORKERS_SCOPE, OwnershipCheck.RECEIPT_MISMATCH)
        except ObservationSettling as exc:
            pending_readback = exc
    if pending_readback is not None:
        raise pending_readback
    return True


async def _provision_container_application(
    context: PlatformJobContext, payload: CloudflareEnablePayload,
    adapter: ContainersRest, release: VerifiedContainerRelease,
) -> bool:
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None:
            return False
        binding = await _container_launcher_namespace(db, row)
        if binding is None or binding[1].intent.artifact_digest != release.digest:
            raise OwnershipConflict("Container launcher differs from the verified release",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.RELEASE_INTENT)
        namespace, launcher = binding
        token = await _management_token(db, row)
        await adapter.verify_launcher_namespace(token, launcher, namespace)
        resource = CloudflareOwnedResource
        name = f"bifrost-{row.id.hex}-r{row.desired_revision}"
        application = await db.scalar(select(resource).where(
            resource.installation_id == row.id, resource.account_id == row.account_id,
            resource.scope == "containers", resource.kind == "container_application", resource.target_name == name,
        ).with_for_update())
        if application is None:
            application = CloudflareOwnedResource(
                id=uuid4(), installation_id=row.id, account_id=row.account_id,
                scope="containers", kind="container_application", target_name=name,
                desired_revision=payload.revision, artifact_digest=release.digest, effect_id=uuid4(),
                namespace_ids={"ContainerLauncher": namespace}, state="creating", provider_details={},
            )
            db.add(application)
        if (
            application.desired_revision != payload.revision or application.artifact_digest != release.digest
            or application.namespace_ids != {"ContainerLauncher": namespace}
            or application.state not in ("creating", "present")
        ):
            raise OwnershipConflict("Container application intent differs from the verified revision",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.JOURNAL_STATE)
        intent = _resource_intent(application)
        observed = await adapter.observe_application(token, intent, namespace, recorded_id=application.provider_id)
        may_exist = _container_create_may_exist(application)
        if observed is not None:
            if application.state == "creating" and not may_exist:
                raise OwnershipConflict("Container application was occupied before creation",
                                        OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.TARGET_OCCUPIED)
            _record_container_application(application, observed)
            return True
        if application.state == "present" or application.provider_id is not None:
            # An acknowledged ID permanently binds this effect. Disconnect
            # can prove its absence and retire it before a new deployment.
            raise ObservationPending("The recorded Container application is absent",
                                     OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_CREATE)
        now = datetime.now(UTC)
        if may_exist and not _container_create_absence_settled(application, now):
            await db.commit()
            raise _EarlierContainerCreate("Waiting for the earlier Container application create",
                                          OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_CREATE)
        # Provider inventory and the launcher fence have both been checked.
        # An expired attempt may retry only at the same account-unique name.
        application_id = application.id
        api_token_version = row.api_token_version
        attempt = uuid4().hex
        details = {key: value for key, value in application.provider_details.items() if key != "creation_empty_observed"}
        if may_exist:
            # A definitive rejection of this attempt cannot resolve a
            # previous send that may still surface in the exact inventory.
            details["creation_ambiguous"] = True
        application.provider_details = {**details, "creation_sent": True, "creation_attempt": attempt}
        application.observed_at = now

    failure: Exception | None = None
    async with get_db_context() as db:
        row = await _locked_current(db, context, payload)
        if row is None or row.api_token_version != api_token_version:
            return False
        application = await db.get(CloudflareOwnedResource, application_id, with_for_update=True)
        if (
            application is None or _resource_intent(application) != intent
            or application.state != "creating" or application.provider_id is not None
            or application.provider_details.get("creation_attempt") != attempt
            or application.observed_at != now or await _container_launcher_namespace(db, row) != binding
        ):
            raise OwnershipConflict("Container application creation fence changed",
                                    OwnershipResource.CONTAINER_APPLICATION, OwnershipCheck.JOURNAL_STATE)
        try:
            receipt = await adapter.create_application(token, intent, namespace)
        except CloudflareManagementDenied as exc:
            application.provider_details = {
                key: value for key, value in application.provider_details.items()
                if key not in ("creation_sent", "creation_attempt")
            }
            application.observed_at = None
            failure = exc
        except (CloudflareRestUnavailable, ObservationPending, OwnershipConflict) as exc:
            application.provider_details = {**application.provider_details, "creation_ambiguous": True}
            failure = exc
        else:
            # Keep the acknowledged ID even if its independent readback lags.
            # An API retry must observe this ID and cannot re-arm the POST.
            application.provider_id = receipt.provider_id
            application.provider_version = receipt.provider_version
            try:
                observed = await adapter.observe_application(token, intent, namespace, recorded_id=receipt.provider_id)
                if observed is None:
                    raise ObservationSettling("Waiting to read back the acknowledged Container application",
                                              OwnershipResource.CONTAINER_APPLICATION, ObservationCheck.CONTAINER_CREATE)
            except (CloudflareManagementDenied, CloudflareRestUnavailable, ObservationPending, OwnershipConflict) as exc:
                failure = exc
            else:
                _record_container_application(application, observed)
    if failure is not None:
        raise failure
    return True


def _record_container_application(resource: CloudflareOwnedResource, receipt: ResourceObservation) -> None:
    resource.provider_id = receipt.provider_id
    resource.provider_version = receipt.provider_version
    resource.state = "present"
    resource.observed_at = datetime.now(UTC)


async def _wait_for_container_application(
    context: PlatformJobContext, payload: CloudflareEnablePayload,
    adapter: ContainersRest, release: VerifiedContainerRelease,
) -> bool:
    # Allow two empty observations a full settlement window apart in one job
    # attempt. The first empty observation commits before each wait. A late
    # exact-name app can be adopted on any probe.
    deadline = monotonic() + CREATE_SETTLEMENT_WINDOW.total_seconds() + 5
    while True:
        try:
            return await _provision_container_application(context, payload, adapter, release)
        except _EarlierContainerCreate:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise
        await context.report("Waiting for Earlier Cloudflare Container Creation")
        await sleep(min(2, remaining))


async def _drain_previous_revision(
    context: PlatformJobContext, payload: CloudflareEnablePayload, adapter: WorkersRestCleanup,
    *, upgrading: bool, container_adapter: ContainersRest | None = None,
) -> bool:
    deadline = monotonic() + DRAIN_DEADLINE.total_seconds()
    while True:
        async with get_db_context() as db:
            row = await _locked_current(db, context, payload, lock_row=False)
            if row is None:
                return False
            if upgrading and (row.upgrade_release is None or row.admitted_revision is not None):
                raise OwnershipConflict(
                    "Cloudflare upgrade no longer owns the paused revision",
                    OwnershipResource.UPGRADE_REVISION, OwnershipCheck.UPGRADE_FENCE,
                )
            if row.upgrade_drained_at is not None:
                return True
            workers_remain = await cloudflare_workers_remain(db, row.id, before_revision=payload.revision)
            if workers_remain or not await _next_cleanup_is_image(db, row.id, before_revision=payload.revision):
                row = await _locked_current(db, context, payload)
                if row is None:
                    return False
            if workers_remain:
                result = await _cleanup_stranded_workflow_slots(db, row, adapter, before_revision=payload.revision)
            else:
                try:
                    result = await _cleanup_one(
                        db, row, adapter, before_revision=payload.revision, container_adapter=container_adapter,
                    )
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
                if result == "settling":
                    raise PlatformJobFailure("CF_CLEANUP_SETTLING", "Verifying Prior Cloudflare Deletion", retryable=True)
                if workers_remain:
                    raise PlatformJobRequiresAction("Wait for Cloudflare Workers to Retire", {"code": "CF_PRIOR_WORKERS_LIVE"})
                raise PlatformJobRequiresAction("Wait for Cloudflare Creation to Settle", {"code": "CF_CLEANUP_PENDING"})
            if monotonic() >= deadline:
                if result == "settling":
                    raise PlatformJobRequiresAction("Cloudflare Cleanup Pending", {"code": "CF_CLEANUP_PENDING"})
                raise await _drain_timeout_action(db, row.id, workers_remain=workers_remain)
        await sleep(2)


async def _require_budget(db: AsyncSession, row: CloudflareInstallation) -> None:
    cost = await db.scalar(select(cloudflare_estimated_usage()).where(CloudflareInstallation.id == row.id))
    if cost is None:
        raise PlatformJobRequiresAction("Verify Cloudflare Spend", {
            "code": "CF_SPEND_UNVERIFIED",
            "details": {"resource_class": "spend_guard", "check": "monthly_usage",
                        "observed_category": "ledger_unavailable", "expected_category": "under_limit"},
        })
    if cost >= CloudflareCapacityPolicy.model_validate(row.policy["limits"]).monthly_limit_usd:
        raise PlatformJobRequiresAction("Cloudflare Budget Reached", {
            "code": "CF_BUDGET_REACHED",
            "details": {"resource_class": "spend_guard", "check": "monthly_usage",
                        "observed_category": "limit_reached", "expected_category": "under_limit"},
        })


async def _enable(
    context: PlatformJobContext, parsed: BaseModel, *, repair_tunnel_ingress: bool = False,
    refresh_container_egress: bool = False,
) -> dict[str, Any]:
    payload = CloudflareEnablePayload.model_validate(parsed)
    await context.report("Checking Cloudflare Release and Entitlement")

    async def qualification_evidence() -> dict[str, Any]:
        async with get_db_context() as db:
            evidence = await container_qualification_evidence(
                db, payload.installation_id, job_id=context.job_id, lease_token=context.lease_token,
            )
        return {"container_qualification": evidence} if evidence is not None else {}

    try:
        release = load_workers_release()
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            result = await _settle_enable(
                context, payload, http, release, repair_tunnel_ingress=repair_tunnel_ingress,
                refresh_container_egress=refresh_container_egress,
            )
        return {**result, **await qualification_evidence()}
    except (PlatformJobFailure, PlatformJobRequiresAction) as exc:
        exc.result = {**(exc.result or {}), **await qualification_evidence()}
        raise
    except WorkersReleaseUnavailable:
        raise PlatformJobRequiresAction("Install a Qualified Workers Release", {"code": "CF_RELEASE_UNAVAILABLE"}) from None
    except ContainersReleaseUnavailable:
        raise PlatformJobRequiresAction("Install the Matched Cloudflare Linux Release", {
            "code": "CF_CONTAINERS_RELEASE_UNAVAILABLE",
        }) from None
    except WorkersPaidUnverified as exc:
        raise PlatformJobRequiresAction(str(exc), {"code": "CF_WORKERS_PAID_UNVERIFIED"}) from None
    except ValueError as exc:
        raise PlatformJobRequiresAction("Verify Cloudflare Connection Configuration", {"code": "CF_CONNECTION_INVALID"}) from exc
    except OwnershipConflict as exc:
        raise PlatformJobRequiresAction(
            "Verify Cloudflare Ownership", {"code": "CF_OWNERSHIP_CONFLICT", "details": exc.as_result()},
        ) from None
    except ObservationSettling as exc:
        code = "CF_DEPLOYMENT_UNVERIFIED"
        raise PlatformJobFailure(
            code, "Waiting for Cloudflare Deployment Readback", retryable=True,
            result={"code": code, "details": exc.as_result()},
        ) from None
    except (ObservationPending, CapacityProtocolError) as exc:
        raise PlatformJobRequiresAction(
            "Verify Cloudflare Deployment", {"code": "CF_DEPLOYMENT_UNVERIFIED", "details": exc.as_result()},
        ) from None
    except CloudflareManagementDenied:
        raise PlatformJobRequiresAction("Reconnect Cloudflare", {"code": "CF_MANAGEMENT_CREDENTIAL"}) from None
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status in (408, 429) or status >= 500:
            raise PlatformJobFailure("CF_PROVIDER_UNAVAILABLE", "Cloudflare Is Temporarily Unavailable", retryable=True) from None
        if exc.request.url.path.endswith("/capabilities"):
            check = CapacityProtocolCheck.PROFILE_HTTP
        elif exc.request.url.path.endswith("/start"):
            check = CapacityProtocolCheck.START_HTTP
        elif exc.request.url.path.endswith("/prepare"):
            check = CapacityProtocolCheck.PREPARE_HTTP
        else:
            check = CapacityProtocolCheck.UNCLASSIFIED
        raise PlatformJobRequiresAction("Verify Cloudflare Deployment", {
            "code": "CF_DEPLOYMENT_UNVERIFIED",
            "details": CapacityProtocolError("Capacity endpoint rejected the request", check).as_result(),
        }) from None
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
    if row.desired_state == "scaled_in" and row.deployed_revision == expected_revision:
        scale_in = CloudflareDisablePayload(installation_id=row.id, revision=expected_revision, action="scale_in")
        if (
            await find_active_platform_job(db, DISABLE_JOB, scale_in.key) is not None
            or await cloudflare_workers_remain(db, row.id)
        ):
            raise CloudflarePolicyConflict("Wait for Cloudflare Scale In to finish draining.")
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
