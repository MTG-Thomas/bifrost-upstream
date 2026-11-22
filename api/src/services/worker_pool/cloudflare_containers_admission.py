"""Plan and reserve a tenant-pinned Linux overflow incarnation."""

import json
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from shared.models import CloudflareCapacityPolicy
from sqlalchemy import String, case, cast, exists, func, literal, or_, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from src.models.contracts.cloudflare_approval import SolutionApprovalScope
from src.models.enums import ExecutionStatus
from src.models.orm.cloudflare_approvals import CloudflareApproval
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareInstallation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.executions import Execution
from src.models.orm.file_index import FileIndex, SolutionFileIndex
from src.models.orm.solutions import Solution
from src.models.orm.work_dispatches import WorkDispatch
from src.models.orm.workflows import Workflow
from src.services.worker_pool.claims import CAPACITY_LOCK
from src.services.worker_pool.cloudflare_admission import (
    PrimaryPool,
    workers_release_matches,
)
from src.services.worker_pool.cloudflare_containers_policy import (
    CONTAINERS_SCOPE,
    ContainersPolicy,
    container_envelope,
    container_reserve_cost,
    fleet_envelope,
)
from src.services.worker_pool.cloudflare_containers_release import (
    Digest,
    VerifiedContainerRelease,
)
from src.services.worker_pool.cloudflare_slots import (
    read_workflow_slot_admission,
    tenant_key,
)
from src.services.worker_pool.cloudflare_spend import (
    cloudflare_estimated_usage,
    usage_period,
)
from src.services.worker_pool.eligibility import cloudflare_dispatch_sql
from src.worker.remote.protocol import (
    CLOUDFLARE_WORKFLOW_HANDLER,
    LINUX_RUNTIME_PROFILE,
)

LINUX_REVIEW_VERSION = "containers-linux-v1"
LIVE_STATES = ("reserved", "starting", "ready", "draining")


class LinuxReview(BaseModel):
    """Internal review pins consumed by placement. Public review is a later phase."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    findings: frozenset[Literal["CF_NATIVE", "CF_PROCESS", "CF_PACKAGE"]] = Field(min_length=1)
    release_digest: Digest
    installation_id: UUID
    deployment_revision: int = Field(gt=0)
    consent_version: str = Field(min_length=1)
    worker_ingress_origin: str
    egress: Literal["worker-ingress-only-v1"]
    path: str
    function_name: str
    source_pins: dict[str, Digest] = Field(min_length=1, max_length=128)
    wheels: dict[str, Digest] = Field(max_length=128)
    solution: SolutionApprovalScope | None = None
    memory_bytes: int = Field(gt=0, lt=4 * 1024**3)
    timeout_seconds: int = Field(gt=0)
    ingress_bytes: int = Field(gt=0)
    outbound_bytes: int = Field(gt=0)
    previous_revision: Digest | None
    inactive_history_count: int = Field(ge=0)

    def matches_release(self, release: VerifiedContainerRelease) -> bool:
        return (
            self.release_digest == release.digest
            and self.egress == release.manifest.ingress_contract
            and ("CF_PACKAGE" not in self.findings or bool(self.wheels))
            and all(release.manifest.wheels.get(pin) == digest for pin, digest in self.wheels.items())
        )


def linux_review_digest(workflow_id: UUID, owner: UUID | None, declaration: dict) -> str:
    # The stored declaration includes the predecessor or inactive history
    # count at acceptance, so an identical review can be accepted after revoke.
    return sha256(json.dumps({
        "policy_version": LINUX_REVIEW_VERSION,
        "workflow_id": str(workflow_id), "organization_id": str(owner) if owner else None,
        "declaration": declaration,
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def eligible_linux_review(
    *, dispatch: WorkDispatch, execution: Execution, workflow: Workflow,
    approval: CloudflareApproval, installation: CloudflareInstallation, policy: ContainersPolicy,
    release: VerifiedContainerRelease,
) -> LinuxReview | None:
    """Require a current reviewed closure in the exact execution tenant."""
    if (
        dispatch.execution_lane != "normal" or execution.execution_lane != "normal"
        or (dispatch.handler, dispatch.handler_version) != CLOUDFLARE_WORKFLOW_HANDLER
        or dispatch.cloudflare_revision is not None
        or dispatch.subject_id != execution.id or execution.workflow_id != workflow.id
        or workflow.type != "workflow" or not workflow.is_active or workflow.execution_lane == "fast_path"
        or dispatch.payload.get("script_name") or dispatch.payload.get("artifact_workspace_id") is not None
        or dispatch.message.get("code") is not None
        or approval.policy_version != LINUX_REVIEW_VERSION or not approval.is_current
        or approval.workflow_id != workflow.id
        or (workflow.organization_id is not None and workflow.organization_id != execution.organization_id)
        or approval.organization_id != workflow.organization_id
        or approval.revision != linux_review_digest(workflow.id, workflow.organization_id, approval.declaration)
    ):
        return None
    try:
        review = LinuxReview.model_validate(approval.declaration)
    except ValidationError:
        return None
    if (
        not review.matches_release(release)
        or review.installation_id != installation.id
        or review.deployment_revision != installation.admitted_revision
        or review.consent_version != installation.consent_version
        or review.worker_ingress_origin != installation.worker_ingress_origin
        or (review.path, review.function_name) != (workflow.path, workflow.function_name)
        or review.path not in review.source_pins
        or dispatch.memory_request_bytes > review.memory_bytes
        or review.timeout_seconds + policy.idle_seconds >= policy.lifetime_seconds
        or review.ingress_bytes > policy.ingress_bytes or review.outbound_bytes > policy.outbound_bytes
        or (review.solution.install_id if review.solution else None) != workflow.solution_id
    ):
        return None
    return review


def linux_source_current_sql() -> ColumnElement[bool]:
    """Check the review's indexed closure and Solution generation in the candidate query."""
    declaration = CloudflareApproval.declaration
    source = declaration["source_pins"]
    pins = func.jsonb_each_text(case(
        (func.jsonb_typeof(source) == "object", source), else_=literal({}, type_=JSONB),
    )).table_valued("key", "value")
    loose = exists(select(FileIndex.path).where(
        FileIndex.path == pins.c.key, FileIndex.content_hash == pins.c.value,
    ).correlate(pins))
    installed = exists(select(SolutionFileIndex.path).where(
        SolutionFileIndex.solution_id == Workflow.solution_id,
        SolutionFileIndex.path == pins.c.key, SolutionFileIndex.content_hash == pins.c.value,
    ).correlate(pins, Workflow))
    solution = declaration["solution"]
    current_solution = exists(select(Solution.id).where(
        Solution.id == Workflow.solution_id, Solution.status == "active",
        Solution.organization_id.is_not_distinct_from(Workflow.organization_id),
        ~Solution.allow_outbound_access,
        cast(Solution.runtime_source_generation, String) == solution["source_generation"].as_string(),
        Solution.version.is_not_distinct_from(solution["version"].as_string()),
    ).correlate(Workflow, CloudflareApproval))
    mismatched = exists(select(pins.c.key).where(or_(
        (Workflow.solution_id.is_(None) & ~loose),
        (Workflow.solution_id.is_not(None) & ~installed),
    )).correlate(Workflow, CloudflareApproval))
    return ~mismatched & (Workflow.solution_id.is_(None) | current_solution)


def primary_has_no_fit(
    primary: PrimaryPool, workers: list[ExecutionWorker], now: datetime, requested_bytes: int,
) -> bool:
    if not primary.saturated(workers, now):
        return False
    for worker in workers:
        if (
            worker.form != primary.form or worker.capacity_scope != primary.scope
            or worker.execution_lane != "normal"
        ):
            continue
        budget = worker.memory_budget_bytes
        if worker.state in ("starting", "reserved") or (worker.state == "ready" and budget is None):
            return False
        if (
            worker.state == "ready" and not worker.memory_pressured
            and CLOUDFLARE_WORKFLOW_HANDLER[0] in worker.supported_handlers
            and budget is not None and budget - worker.memory_reserved_bytes >= requested_bytes
        ):
            return False
    return True


async def reserve_container(
    db: AsyncSession, *, installation_id: UUID, controller_id: UUID,
    primary: PrimaryPool, release: VerifiedContainerRelease, verified_egress_origin: str | None,
) -> CloudflareWorkerReservation | None:
    """Reserve capacity. Step 3 must wire verified egress, stop and settlement before any production caller."""
    await db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": CAPACITY_LOCK})
    installation = await db.get(
        CloudflareInstallation, installation_id, with_for_update={"key_share": True}, populate_existing=True,
    )
    if (
        installation is None or installation.retired_at is not None
        or installation.desired_state != "enabled" or installation.admitted_revision is None
        or installation.admitted_revision != installation.containers_ready_revision
        or installation.containers_release_digest != release.digest
        or verified_egress_origin != installation.worker_ingress_origin
        or installation.control_root_version is None
    ):
        return None
    revision = installation.admitted_revision
    try:
        containers = ContainersPolicy.model_validate(installation.containers_policy)
        policy = CloudflareCapacityPolicy.model_validate(installation.policy["limits"])
        workers_envelope = Decimal(installation.policy["fleet_envelope_usd"])
    except (ValidationError, KeyError, ArithmeticError, TypeError):
        return None
    if not containers.enabled or await db.scalar(
        select(workers_release_matches()).where(CloudflareInstallation.id == installation.id),
    ) is not True:
        return None
    scope = await db.get(CloudflareCapacityScope, (installation.id, CONTAINERS_SCOPE, revision))
    if scope is None:
        return None
    now = await db.scalar(select(func.clock_timestamp()))
    assert now is not None
    # A month can contain 31 days. The current account balance is checked
    # separately, including a reservation that crosses the next boundary.
    if workers_envelope + fleet_envelope(containers, release.manifest.rates, 31 * 86400) > policy.monthly_limit_usd:
        return None
    pin = CloudflareWorkerReservation
    live = (await db.execute(select(pin.container_tenant, ExecutionWorker.reserved_for_dispatch_id).join(
        ExecutionWorker, ExecutionWorker.id == pin.worker_id,
    ).where(
        pin.installation_id == installation_id, pin.container_reserved_at.is_not(None), pin.container_stopped_at.is_(None),
    ))).all()
    if len(live) >= containers.installation_instances:
        return None
    workers = list(await db.scalars(select(ExecutionWorker).where(ExecutionWorker.state.in_(LIVE_STATES))))
    if policy.shared_managed_worker_cap is not None and (
        len({worker.id for worker in workers if worker.form in ("external", "job")})
        + (await db.scalar(select(func.count()).select_from(pin).join(
            ExecutionWorker, ExecutionWorker.id == pin.worker_id,
        ).where(
            ExecutionWorker.capacity_resource_deleted_at.is_(None), ExecutionWorker.state.not_in(LIVE_STATES),
        )) or 0) >= policy.shared_managed_worker_cap
    ):
        return None
    seats = await read_workflow_slot_admission(db, installation_id, policy.workers)
    if seats.full:
        return None
    linux_workers = [worker for worker in workers if worker.runtime_profile == LINUX_RUNTIME_PROFILE]
    covered = {row.reserved_for_dispatch_id for row in workers if row.reserved_for_dispatch_id is not None}
    blocked_tenants = seats.blocked_tenants | {tenant for tenant, _ in live}
    declaration = CloudflareApproval.declaration
    queue = select(WorkDispatch, Execution, Workflow, CloudflareApproval).join(
        Execution, Execution.id == WorkDispatch.subject_id,
    ).join(Workflow, Workflow.id == Execution.workflow_id).join(
        CloudflareApproval, CloudflareApproval.workflow_id == Workflow.id,
    ).where(
        Execution.status == ExecutionStatus.PENDING, WorkDispatch.assigned_worker_id.is_(None),
        WorkDispatch.id.not_in(covered), WorkDispatch.created_at <= now - timedelta(seconds=containers.queue_wait_seconds),
        WorkDispatch.execution_lane == "normal", Execution.execution_lane == "normal",
        WorkDispatch.handler == CLOUDFLARE_WORKFLOW_HANDLER[0],
        WorkDispatch.handler_version == CLOUDFLARE_WORKFLOW_HANDLER[1],
        WorkDispatch.cloudflare_revision.is_(None),
        Workflow.is_active, Workflow.type == "workflow", Workflow.execution_lane != "fast_path",
        or_(Workflow.organization_id.is_(None), Workflow.organization_id == Execution.organization_id),
        CloudflareApproval.organization_id.is_not_distinct_from(Workflow.organization_id),
        func.coalesce(cast(Execution.organization_id, String), tenant_key(None)).not_in(blocked_tenants),
        CloudflareApproval.is_current, CloudflareApproval.policy_version == LINUX_REVIEW_VERSION,
        declaration["release_digest"].as_string() == release.digest,
        declaration["installation_id"].as_string() == str(installation_id),
        declaration["worker_ingress_origin"].as_string() == verified_egress_origin,
        linux_source_current_sql(),
    ).order_by(WorkDispatch.created_at, WorkDispatch.id).limit(1000)
    demand: list[tuple[WorkDispatch, Execution, CloudflareApproval]] = []
    for dispatch, execution, workflow, approval in (await db.execute(queue)).tuples():
        if (
            primary_has_no_fit(primary, linux_workers, now, dispatch.memory_request_bytes)
            and eligible_linux_review(
                dispatch=dispatch, execution=execution, workflow=workflow, approval=approval,
                installation=installation, policy=containers, release=release,
            ) is not None
        ):
            demand.append((dispatch, execution, approval))
            if len(demand) == containers.queue_depth:
                break
    if len(demand) < containers.queue_depth:
        return None
    dispatch, execution, approval = demand[0]
    if seats.occupied.total() == policy.workers.installation_slots - 1:
        older_workers = await db.scalars(select(WorkDispatch.memory_request_bytes).join(
            Execution, Execution.id == WorkDispatch.subject_id,
        ).where(
            cloudflare_dispatch_sql(), WorkDispatch.assigned_worker_id.is_(None),
            Execution.status == ExecutionStatus.PENDING,
            Execution.organization_id.is_distinct_from(execution.organization_id),
            func.coalesce(cast(Execution.organization_id, String), tenant_key(None)).not_in(seats.blocked_tenants),
            (WorkDispatch.created_at < dispatch.created_at)
            | ((WorkDispatch.created_at == dispatch.created_at) & (WorkDispatch.id < dispatch.id)),
        ))
        if any(primary_has_no_fit(primary, linux_workers, now, size) for size in older_workers):
            return None
    # The primary can claim during planning. Lock the selected dispatch before
    # creating the reservation and decline if a claimant already consumed it.
    if await db.scalar(select(WorkDispatch.id).where(
        WorkDispatch.id == dispatch.id, WorkDispatch.assigned_worker_id.is_(None),
    ).with_for_update(skip_locked=True)) is None:
        return None
    envelope = container_envelope(containers, release.manifest.rates)
    period = usage_period(now)
    for start, end in (period, usage_period(period[1])):
        cost = container_reserve_cost(envelope, now, start, end)
        if not cost:
            continue
        estimate = await db.scalar(select(cloudflare_estimated_usage(max(now, start))).where(
            CloudflareInstallation.id == installation_id,
        ))
        if estimate is None or estimate + cost >= policy.monthly_limit_usd:
            return None
    worker_id, uid = uuid4(), uuid4()
    db.add(ExecutionWorker(
        id=worker_id, form="external", state="reserved", runtime_profile=LINUX_RUNTIME_PROFILE,
        execution_lane="normal", capacity_scope=CONTAINERS_SCOPE, capacity_resource_uid=str(uid),
        supported_handlers=[CLOUDFLARE_WORKFLOW_HANDLER[0]], reserved_for_dispatch_id=dispatch.id,
        memory_limit_bytes=4 * 1024**3, launched_by=controller_id,
    ))
    await db.flush()
    reservation = pin(
        worker_id=worker_id, installation_id=installation_id, scope=CONTAINERS_SCOPE,
        deployment_revision=revision, control_root_version=installation.control_root_version,
        container_tenant=tenant_key(execution.organization_id), container_release=release.digest,
        container_reserved_at=now, container_envelope={
            **envelope, "approval": approval.revision, "instance_type": containers.instance_type,
            "credential_version": scope.bearer_version,
            "start_deadline": (now + timedelta(seconds=containers.start_timeout_seconds)).isoformat(),
        },
    )
    db.add(reservation)
    await db.flush()
    return reservation
