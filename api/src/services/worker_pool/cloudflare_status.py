"""Read-only Cloudflare placement and recovery explanations."""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

from shared.models import (
    CloudflareCapacityPolicy,
    CloudflareDeploymentEvidence,
    CloudflareLaneStatus,
    CloudflarePlacementCode,
    CloudflarePlacementReason,
    CloudflareRecoveryDetails,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.core.cache.redis_client import CacheConnectionError, CacheOperationError, get_redis
from src.models.enums import LIVE_EXECUTION_STATUSES, ExecutionStatus
from src.models.orm.cloudflare_approvals import CloudflareApproval
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.models.orm.platform_jobs import PlatformJob
from src.models.orm.work_dispatches import WorkDispatch
from src.models.orm.workflows import Workflow
from src.services.authorization.enforce import Caller
from src.services.authorization.execution_access import execution_list_filter
from src.services.kubernetes_execution import load_kubernetes_policy
from src.services.pool_cadence import CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS, POOL_RECONCILE_SPACING_SECONDS
from src.services.work_dispatch import WAKE_WORKFLOW
from src.services.worker_pool.cloudflare_admission import ManagedCloudflareScope, PrimaryPool
from src.services.worker_pool.cloudflare_containers_admission import LINUX_REVIEW_VERSION, linux_source_current_sql
from src.services.worker_pool.cloudflare_containers_egress import read_verified_egress
from src.services.worker_pool.cloudflare_containers_policy import ContainersPolicy
from src.services.worker_pool.cloudflare_provisioning import ObservationCheck, OwnershipCheck, OwnershipResource
from src.services.worker_pool.cloudflare_slots import read_workflow_slot_admission, tenant_key
from src.services.worker_pool.controller import Request, linux_capacity_budgets, take_first_budget, waiting_capacity_rows
from src.services.worker_pool.demand import pending_local_platform_job_ids
from src.services.worker_pool.eligibility import cloudflare_dispatch_sql
from src.services.worker_pool.external_client import CapacityProtocolCheck
from src.services.worker_pool.liveness import HEARTBEAT_LOST_AFTER
from src.services.worker_pool.sizing import LIVE_STATES, JobSizing, job_baseline
from src.worker.remote.protocol import CLOUDFLARE_WORKFLOW_HANDLER, LINUX_RUNTIME_PROFILE

logger = logging.getLogger(__name__)

PrimaryCause = Literal["below_ceiling", "unhealthy", "profile_mismatch", "not_admitting", "unreachable"]
PRIMARY_REMEDIES: dict[PrimaryCause, str] = {
    "below_ceiling": "The primary can add workers before Cloudflare overflow is needed.",
    "unhealthy": "Check that Normal primary workers are ready and reporting heartbeats.",
    "profile_mismatch": "Verify the primary runtime profile and the deployed Workers capacity policy.",
    "not_admitting": "Resolve the installation admission blockers and reconcile Cloudflare.",
    "unreachable": "Restore capacity controller connectivity and check the next observation.",
}

PLACEMENT: dict[CloudflarePlacementCode, tuple[str, str]] = {
    "setup_required": ("Waiting for Setup", "Connect Cloudflare and enable capacity to provision and verify overflow."),
    "workflow_review_required": ("Workflow Review Required", "Review and accept an eligible workflow revision in Workflows."),
    "waiting_for_demand": ("Waiting for Demand", "Run approved workflows when the primary is full."),
    "primary_has_capacity": ("Primary Has Capacity", "The primary can serve this demand. No action is needed."),
    "primary_capacity_unverified": (
        "Primary Capacity Unverified", "Check primary worker readiness and wait for the controller to observe the pool ceiling.",
    ),
    "waiting_for_launch_gate": (
        "Waiting for Launch Gate", "Wait for the queue age and demand threshold. A ready Workers launcher can claim sooner.",
    ),
    "tenant_limit_reached": ("Tenant Limit Reached", "Wait for tenant slots to release or review the per-tenant limit."),
    "installation_slots_full": ("Installation Slots Full", "Wait for capacity to retire or review the installation limits."),
    "spend_guard_closed": ("Spend Guard Closed", "Verify the ledger and review the monthly limit or wait for the UTC reset."),
    "cleanup_pending": ("Cleanup Pending", "Run Scale In to verify retirement or finish an outstanding Disconnect."),
    "cloudflare_capacity_unavailable": (
        "Cloudflare Capacity Unavailable", "Wait for the controller to observe provider capacity again, then check deployment status.",
    ),
    "release_mismatch": (
        "Release Mismatch / Upgrade Pending", "Install a qualified release and run Upgrade Cloudflare when available.",
    ),
    "connector_unavailable": (
        "Connector Unavailable", "Check the Managed Tunnel connector and token projection, then reconcile Cloudflare.",
    ),
    "eligible_for_overflow": (
        "Eligible for Overflow", "Wait for a controller pass and a ready slot. Check the queue and claim counts to verify placement.",
    ),
    "container_readiness_unverified": (
        "Container Readiness Unverified", "Qualify the exact Linux release and worker ingress through Enable or Reconcile.",
    ),
    "container_egress_stale": (
        "Container Egress Proof Expired", "Wait for Reconcile to verify the Container worker ingress again.",
    ),
    "container_allocation_cooldown": (
        "Container Allocation Cooling Down", "The primary can claim handed-back work while the Container lane settles.",
    ),
    "container_receipt_limit": (
        "Container Receipt Limit Reached", "Wait for expired registry receipts to clear and verify any outstanding cleanup.",
    ),
}


def reason(code: CloudflarePlacementCode, at: datetime | None = None) -> CloudflarePlacementReason:
    label, remedy = PLACEMENT[code]
    return CloudflarePlacementReason(code=code, label=label, remedy=remedy, next_eligible_at=at)


def recovery_for_job(job: PlatformJob | None) -> CloudflareRecoveryDetails | None:
    if job is None or job.status not in ("failed", "requires_action"):
        return None
    result = job.result or {}
    code = result.get("code", job.error_code)
    remedies = {
        "CF_OWNERSHIP_CONFLICT": "Verify the exact resource owner. Preserve resources whose ownership is uncertain.",
        "CF_DEPLOYMENT_UNVERIFIED": "Check the deployment and installed release, then reconcile to verify the exact revision.",
        "CF_WORKER_INGRESS_UNVERIFIED": "Check DNS, TLS, and the dedicated worker ingress route before reconciling.",
        "CF_WORKERS_PAID_UNVERIFIED": "Verify Workers Paid and the account token's read grants in Cloudflare.",
        "CF_MANAGEMENT_CREDENTIAL": "Rotate the API token with the required grants for the connected account.",
        "CF_RELEASE_UNAVAILABLE": PLACEMENT["release_mismatch"][1],
        "CF_CONNECTOR_UNAVAILABLE": PLACEMENT["connector_unavailable"][1],
        "CF_BUDGET_REACHED": PLACEMENT["spend_guard_closed"][1],
        "CF_SPEND_UNVERIFIED": PLACEMENT["spend_guard_closed"][1],
        "CF_CLEANUP_PENDING": PLACEMENT["cleanup_pending"][1],
        "CF_CLEANUP_SETTLING": PLACEMENT["cleanup_pending"][1],
        "CF_PRIOR_WORKERS_LIVE": PLACEMENT["cleanup_pending"][1],
        "CF_PROVIDER_UNAVAILABLE": PLACEMENT["cloudflare_capacity_unavailable"][1],
        "CF_CONTAINERS_UNVERIFIED": PLACEMENT["container_readiness_unverified"][1],
        "CF_CONTAINERS_CLEANUP_PENDING": PLACEMENT["cleanup_pending"][1],
        "CF_CONTAINERS_RELEASE_UNAVAILABLE": PLACEMENT["release_mismatch"][1],
    }
    if not isinstance(code, str) or code not in remedies:
        return None
    details = result.get("details")
    sanitized = {}
    if isinstance(details, dict):
        values = tuple(details.get(key) for key in ("resource_class", "check", "observed_category", "expected_category"))
        resource, *check = values
        valid = all(isinstance(value, str) for value in values) and (
            (resource in {item.value for item in OwnershipResource}
             and tuple(check) in {item.value for item in (*ObservationCheck, *OwnershipCheck)})
            or values in [item.value for item in CapacityProtocolCheck]
            or values in (
                ("spend_guard", "monthly_usage", "ledger_unavailable", "under_limit"),
                ("spend_guard", "monthly_usage", "limit_reached", "under_limit"),
            )
        )
        if valid:
            sanitized = dict(zip(("resource_class", "check", "observed_category", "expected_category"), values, strict=True))
    return CloudflareRecoveryDetails(code=code, remedy=remedies[code], **sanitized)


@dataclass(frozen=True)
class PlacementObservation:
    queued: int = 0
    oldest: datetime | None = None
    primary_fit: int | None = None
    primary_saturated: bool | None = None
    primary_cause: PrimaryCause | None = None
    tenant_blocked: bool = False
    installation_full: bool = False
    minimum_demand: int = 1
    aged_demand: int = 0
    cooldown_until: datetime | None = None
    provider_unavailable: bool = False


def lane_reasons(
    observation: PlacementObservation, *, now: datetime, gate_seconds: float, approvals: int,
    blockers: list[CloudflarePlacementReason],
) -> list[CloudflarePlacementReason]:
    reasons = list(blockers)
    if observation.provider_unavailable:
        reasons.append(reason("cloudflare_capacity_unavailable", observation.cooldown_until))
    if approvals == 0 and observation.queued == 0:
        reasons.append(reason("workflow_review_required"))
    if observation.queued == 0:
        if approvals:
            reasons.append(reason("waiting_for_demand"))
        return reasons
    if observation.primary_fit or observation.primary_cause == "below_ceiling":
        primary_reason = reason("primary_has_capacity")
        if observation.primary_cause == "below_ceiling":
            primary_reason.remedy = PRIMARY_REMEDIES["below_ceiling"]
        reasons.append(primary_reason)
    if observation.primary_fit == observation.queued:
        return reasons
    if observation.primary_saturated is not True and observation.primary_cause != "below_ceiling":
        unverified = reason("primary_capacity_unverified")
        if observation.primary_cause is not None:
            unverified.remedy = PRIMARY_REMEDIES[observation.primary_cause]
        reasons.append(unverified)
    if observation.installation_full:
        reasons.append(reason("installation_slots_full"))
    if observation.tenant_blocked:
        reasons.append(reason("tenant_limit_reached"))
    if observation.aged_demand < observation.minimum_demand and observation.oldest is not None:
        gate = observation.oldest + timedelta(seconds=gate_seconds)
        if gate > now:
            reasons.append(reason("waiting_for_launch_gate", gate))
        elif observation.minimum_demand > 1:
            reasons.append(reason("waiting_for_demand"))
    if not reasons or (observation.primary_saturated and len(reasons) == 1 and reasons[0].code == "primary_has_capacity"):
        reasons.append(reason("eligible_for_overflow"))
    return reasons


def _controller_key(binding: ManagedCloudflareScope) -> str:
    return f"cloudflare:placement:{binding.installation_id}:{binding.revision}:{binding.name}"


async def record_controller_observation(
    binding: ManagedCloudflareScope, *, primary: PrimaryPool | None = None, unavailable: bool = False,
    cause: PrimaryCause | None = None,
    placement_code: CloudflarePlacementCode | None = None, next_eligible_at: datetime | None = None,
) -> None:
    try:
        async with get_redis() as redis:
            await redis.set(_controller_key(binding), json.dumps({
                "primary": asdict(primary) if primary is not None else None,
                "cause": cause,
                "cooldown": (datetime.now(UTC) + timedelta(seconds=POOL_RECONCILE_SPACING_SECONDS)).isoformat()
                if unavailable else None,
                "placement_code": placement_code,
                "next_eligible_at": next_eligible_at.isoformat() if next_eligible_at else None,
            }), ex=int(HEARTBEAT_LOST_AFTER.total_seconds()))
    except (CacheConnectionError, CacheOperationError):
        logger.debug("Cloudflare controller observation unavailable")


async def _controller_observation(
    db: AsyncSession, installation: CloudflareInstallation,
) -> tuple[PrimaryPool | None, datetime | None, PrimaryCause | None]:
    scope = CloudflareCapacityScope
    revision = await db.scalar(select(scope.deployment_revision).where(
        scope.installation_id == installation.id, scope.scope == "workers",
    ).order_by((scope.deployment_revision == installation.admitted_revision).desc().nulls_last(),
               scope.deployment_revision.desc()).limit(1))
    if revision is None:
        return None, None, None
    binding = ManagedCloudflareScope(installation.id, "workers", revision)
    try:
        async with get_redis() as redis:
            value = await redis.get(_controller_key(binding))
    except (CacheConnectionError, CacheOperationError):
        return None, None, "unreachable"
    if value is None:
        return None, None, None
    data = json.loads(value)
    return (PrimaryPool(**data["primary"]) if data["primary"] else None,
            datetime.fromisoformat(data["cooldown"]) if data["cooldown"] else None, data.get("cause"))


async def _container_controller_reason(installation: CloudflareInstallation) -> CloudflarePlacementReason | None:
    binding = ManagedCloudflareScope(installation.id, "containers", installation.desired_revision)
    try:
        async with get_redis() as redis:
            value = await redis.get(_controller_key(binding))
    except (CacheConnectionError, CacheOperationError):
        return None
    if value is None:
        return None
    data = json.loads(value)
    code = data.get("placement_code")
    if code not in ("container_allocation_cooldown", "container_receipt_limit", "cleanup_pending"):
        return None
    retry = data.get("next_eligible_at")
    return reason(code, datetime.fromisoformat(retry) if retry else None)


async def deployment_evidence(db: AsyncSession, installation: CloudflareInstallation) -> CloudflareDeploymentEvidence:
    stored = installation.deployment_evidence or {}
    evidence = CloudflareDeploymentEvidence.model_validate(stored)
    if installation.reachability_mode == "managed_tunnel":
        evidence.dns_verified_at = await db.scalar(select(func.max(CloudflareOwnedResource.observed_at)).where(
            CloudflareOwnedResource.installation_id == installation.id,
            CloudflareOwnedResource.kind == "tunnel_dns",
            CloudflareOwnedResource.state == "present",
        ))
        if installation.tunnel_token_version is not None and (
            installation.tunnel_token_version != installation.tunnel_token_projected_version
        ):
            evidence.connector_session = "waiting_for_projection"
        elif evidence.connector_session is None or stored.get("connector_observed_token_version") != installation.tunnel_token_version:
            evidence.connector_session = "unobserved"
    return evidence


def primary_coverage(
    workers: list[ExecutionWorker], waiting: list[Request], sizing: JobSizing,
) -> set[UUID]:
    """Pack primary capacity oldest first, honoring pinned dispatches."""
    free, covered = linux_capacity_budgets(workers, waiting, sizing)
    rooms = [budget.bytes for budget in free]
    slots = [dict(budget.handler_slots) for budget in free]
    for request in waiting:
        if request.id not in covered and take_first_budget(request, free, rooms, slots) is not None:
            covered.add(request.id)
    return covered


def placement_blockers(
    *, enabled: bool, admitted: bool, release_matches: bool, cleanup_pending: bool,
    spend_ready: bool, connector_session: str | None, job_code: str | None, cooldown_until: datetime | None,
) -> list[CloudflarePlacementReason]:
    blockers = []
    if not enabled or not admitted:
        blockers.append(reason("setup_required"))
    if not release_matches:
        blockers.append(reason("release_mismatch"))
    if cleanup_pending:
        blockers.append(reason("cleanup_pending"))
    if not spend_ready:
        blockers.append(reason("spend_guard_closed"))
    if connector_session in ("waiting_for_projection", "disconnected") or job_code == "CF_CONNECTOR_UNAVAILABLE":
        blockers.append(reason("connector_unavailable"))
    if job_code == "CF_PROVIDER_UNAVAILABLE":
        blockers.append(reason("cloudflare_capacity_unavailable", cooldown_until))
    return blockers


async def overflow_status(
    db: AsyncSession, caller: Caller, installation: CloudflareInstallation, policy: CloudflareCapacityPolicy,
    *, now: datetime, approvals: int, blockers: list[CloudflarePlacementReason],
) -> list[CloudflareLaneStatus]:
    pin = CloudflareWorkerReservation
    claim = ExecutionWorkerClaim
    visibility = execution_list_filter(caller, Execution)
    claimed = (await db.execute(select(
        pin.scope,
        func.count(func.distinct(Execution.id)).filter(Execution.started_at.is_(None)),
        func.count(func.distinct(Execution.id)).filter(Execution.started_at.is_not(None)),
    ).select_from(pin).join(claim, claim.worker_id == pin.worker_id).join(
        Execution, Execution.id == claim.subject_id,
    ).where(
        pin.installation_id == installation.id, claim.family == WAKE_WORKFLOW,
        Execution.status.in_(LIVE_EXECUTION_STATUSES), visibility,
    ).group_by(pin.scope))).all()
    counts = {scope: (reserved, running) for scope, reserved, running in claimed}
    primary, cooldown, cause = await _controller_observation(db, installation)
    saturated = None
    covered: set[UUID] = set()
    if primary is not None:
        workers = list(await db.scalars(select(ExecutionWorker).where(
            ExecutionWorker.runtime_profile == LINUX_RUNTIME_PROFILE,
            ExecutionWorker.state.in_(LIVE_STATES),
        )))
        # The primary cohort determines saturation. Fit includes the Linux
        # executors the controller can pack across the installation.
        state = primary.capacity_state(workers, now)
        saturated = state == "saturated"
        if cause in (None, "unhealthy", "below_ceiling"):
            cause = None if state == "saturated" else state
        pool_policy = await load_kubernetes_policy(db)
        waiting = await waiting_capacity_rows(db, await pending_local_platform_job_ids(db, pool_policy, now), now)
        threshold = get_settings().memory_pressure_threshold
        sizing = JobSizing.of(pool_policy, threshold, job_baseline([], pool_policy.worker_memory_mib * 2**20, threshold))
        covered = primary_coverage(workers, [
            Request(row.id, row.memory_request_bytes, row.handler, row.handler_version, row.cloudflare_revision,
                    row.tenant_id)
            for row in waiting
        ], sizing)
    slots = await read_workflow_slot_admission(db, installation.id, policy.workers)
    queue = select(WorkDispatch.id).select_from(WorkDispatch).join(
        Execution, Execution.id == WorkDispatch.subject_id,
    ).where(
        WorkDispatch.assigned_worker_id.is_(None), Execution.status == ExecutionStatus.PENDING, visibility,
    )
    lanes: list[tuple[Literal["workers", "containers"], float, int]] = [
        ("workers", CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS, 1),
    ]
    containers = None
    if installation.containers_policy.get("enabled") is True:
        containers = ContainersPolicy.model_validate(installation.containers_policy)
        lanes.append(("containers", containers.queue_wait_seconds, containers.queue_depth))
    statuses = []
    for lane, gate, depth in lanes:
        blocked = slots.blocked_tenants
        full = slots.full
        if lane == "workers":
            eligible = queue.where(cloudflare_dispatch_sql())
        else:
            assert containers is not None
            live = (await db.scalars(select(pin.container_tenant).where(
                pin.installation_id == installation.id, pin.container_reserved_at.is_not(None), pin.container_stopped_at.is_(None),
            ))).all()
            blocked = blocked | {key for key in live if key is not None}
            full = full or len(live) >= containers.installation_instances
            review = CloudflareApproval.declaration
            eligible = queue.join(Workflow, Workflow.id == Execution.workflow_id).join(
                CloudflareApproval, CloudflareApproval.workflow_id == Workflow.id,
            ).where(
                WorkDispatch.execution_lane == "normal", Execution.execution_lane == "normal",
                WorkDispatch.handler == CLOUDFLARE_WORKFLOW_HANDLER[0], WorkDispatch.handler_version == CLOUDFLARE_WORKFLOW_HANDLER[1],
                WorkDispatch.cloudflare_revision.is_(None), Workflow.is_active,
                Workflow.type == "workflow", Workflow.execution_lane != "fast_path",
                (Workflow.organization_id.is_(None) | (Workflow.organization_id == Execution.organization_id)),
                CloudflareApproval.organization_id.is_not_distinct_from(Workflow.organization_id),
                CloudflareApproval.is_current, CloudflareApproval.policy_version == LINUX_REVIEW_VERSION,
                review["release_digest"].as_string() == installation.containers_release_digest,
                review["installation_id"].as_string() == str(installation.id),
                review["deployment_revision"].as_integer() == installation.admitted_revision,
                review["worker_ingress_origin"].as_string() == installation.worker_ingress_origin,
                review["path"].as_string() == Workflow.path, review["function_name"].as_string() == Workflow.function_name,
                WorkDispatch.memory_request_bytes <= review["memory_bytes"].as_integer(),
                linux_source_current_sql(),
            )
        demand = (await db.execute(eligible.with_only_columns(
            WorkDispatch.id, WorkDispatch.created_at, Execution.organization_id,
        ).order_by(WorkDispatch.created_at, WorkDispatch.id))).all()
        oldest = demand[0].created_at if demand else None
        overflow = [row for row in demand if row.id not in covered]
        unblocked = [row for row in overflow if tenant_key(row.organization_id) not in blocked]
        cutoff = now - timedelta(seconds=gate)
        observation = PlacementObservation(
            queued=len(demand), oldest=unblocked[0].created_at if unblocked else None,
            primary_fit=len(demand) - len(overflow) if primary is not None else None,
            primary_saturated=saturated, primary_cause=cause, minimum_demand=depth,
            aged_demand=sum(row.created_at <= cutoff for row in unblocked),
            tenant_blocked=len(unblocked) < len(overflow), installation_full=full,
            cooldown_until=cooldown if cooldown is not None and cooldown > now else None,
            provider_unavailable=cooldown is not None and not any(
                blocker.code == "cloudflare_capacity_unavailable" for blocker in blockers
            ),
        )
        reserved, running = counts.get(lane, (0, 0))
        lane_blockers = blockers
        if lane == "containers":
            lane_blockers = list(blockers)
            if installation.containers_ready_revision != installation.desired_revision:
                lane_blockers.append(reason("container_readiness_unverified"))
            elif read_verified_egress(installation, now) is None:
                lane_blockers.append(reason("container_egress_stale"))
            observed_issue = await _container_controller_reason(installation)
            if observed_issue is not None and not any(item.code == observed_issue.code for item in lane_blockers):
                lane_blockers.append(observed_issue)
        statuses.append(CloudflareLaneStatus(
            lane=lane, eligible_queued=len(demand), claimed=reserved, running=running, primary_fit=observation.primary_fit,
            primary_saturated=saturated, oldest_eligible_queue_age_seconds=max(0, (now - oldest).total_seconds()) if oldest else None,
            launch_gate_seconds=gate,
            reasons=lane_reasons(observation, now=now, gate_seconds=gate, approvals=approvals, blockers=lane_blockers),
        ))
    return statuses
