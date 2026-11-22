"""Bind module source reads to an execution's managed Linux reservation."""

from dataclasses import dataclass, replace
from uuid import UUID

from pydantic import ValidationError
from shared.sdk_modules import (
    ModuleSourceError,
    ModuleSourceScope,
    validate_module_path,
)
from sqlalchemy import Text, func, literal_column, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.cloudflare_approvals import CloudflareApproval
from src.models.orm.cloudflare_capacity import CloudflareWorkerReservation
from src.config import get_settings
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.solutions import Solution
from src.models.orm.workflows import Workflow
from src.services.authorization.runtime import RuntimeAuthority
from src.services.solutions.storage import SOLUTIONS_ROOT
from src.services.work_dispatch import WAKE_WORKFLOW
from src.services.worker_pool.cloudflare_containers_admission import (
    LINUX_REVIEW_VERSION,
    LinuxReview,
    linux_review_digest,
)
from src.services.worker_pool.cloudflare_containers_policy import (
    CONTAINERS_SCOPE,
    container_deadline_sql,
)
from src.services.worker_pool.cloudflare_slots import tenant_key


@dataclass(frozen=True, slots=True)
class ContainerAssignment:
    reservation: CloudflareWorkerReservation | None
    before_deadline: bool = False


async def container_workflow_assignment(db: AsyncSession, execution_id: UUID) -> ContainerAssignment | None:
    """Classify the latest claim even if the Container has no matching pin."""
    pin = CloudflareWorkerReservation
    claim = ExecutionWorkerClaim
    worker = ExecutionWorker
    assigned = (await db.execute(select(
        worker.capacity_scope, pin, container_deadline_sql() > func.clock_timestamp(),
    ).select_from(claim).join(worker, worker.id == claim.worker_id).outerjoin(pin, pin.worker_id == worker.id).where(
        claim.family == WAKE_WORKFLOW, claim.subject_id == execution_id,
    ).order_by(claim.claimed_at.desc(), claim.id.desc()).limit(1))).one_or_none()
    if assigned is not None:
        scope, reservation, live = assigned
        if scope is not None and scope in (CONTAINERS_SCOPE, getattr(get_settings(), "cloudflare_containers_scope", None)):
            return ContainerAssignment(reservation, live is True)
        if reservation is not None and (reservation.scope == CONTAINERS_SCOPE or reservation.container_reserved_at):
            return ContainerAssignment(reservation, live is True)
        return None

    # If the assignment disappeared, retain the closed boundary until a
    # subsequent claim shows which worker owns runtime authority.
    stranded = await db.scalar(select(pin.worker_id).where(
        pin.scope == CONTAINERS_SCOPE, pin.container_reserved_at.is_not(None),
        pin.container_envelope.op("->>", return_type=Text)(literal_column("'execution_id'")) == str(execution_id),
        pin.container_envelope["voided_unstarted"].as_boolean().is_not(True),
    ).limit(1))
    return ContainerAssignment(None) if stranded is not None else None


async def scoped_container_source(
    db: AsyncSession, runtime: RuntimeAuthority, workflow: Workflow | None, scope: ModuleSourceScope,
) -> ModuleSourceScope:
    assignment = await container_workflow_assignment(db, runtime.credential_id)
    if assignment is None:
        return scope
    reservation = assignment.reservation
    if (
        reservation is None or reservation.scope != CONTAINERS_SCOPE or not assignment.before_deadline
        or reservation.container_stopped_at is not None
        or reservation.container_tenant != tenant_key(runtime.organization_id)
        or reservation.container_envelope is None or workflow is None
        or reservation.container_envelope.get("execution_id") != str(runtime.credential_id)
        or runtime.workflow_id != workflow.id or not workflow.is_active
    ):
        raise ModuleSourceError(403, "Container source authority has expired or changed")
    pinned_revision = reservation.container_envelope.get("approval")
    if not isinstance(pinned_revision, str):
        raise ModuleSourceError(403, "Container source approval is unavailable")
    approval = await db.get(CloudflareApproval, pinned_revision)
    if (
        approval is None or not approval.is_current or approval.workflow_id != workflow.id
        or approval.organization_id != workflow.organization_id
        or approval.policy_version != LINUX_REVIEW_VERSION
        or approval.revision != linux_review_digest(approval.workflow_id, approval.organization_id, approval.declaration)
    ):
        raise ModuleSourceError(403, "Container source approval is unavailable")
    try:
        review = LinuxReview.model_validate(approval.declaration)
    except ValidationError:
        raise ModuleSourceError(403, "Container source approval is malformed") from None
    if (
        review.installation_id != reservation.installation_id or review.deployment_revision != reservation.deployment_revision
        or review.release_digest != reservation.container_release
        or (review.path, review.function_name) != (workflow.path, workflow.function_name)
        or (str(review.solution.install_id) if review.solution is not None else None) != scope.solution_id
    ):
        raise ModuleSourceError(403, "Container source approval differs from the reservation")
    if review.solution is not None:
        solution = await db.get(Solution, review.solution.install_id)
        if (
            solution is None or solution.allow_outbound_access or solution.status != "active"
            or solution.version != review.solution.version
            or solution.runtime_source_generation != review.solution.source_generation
        ):
            raise ModuleSourceError(403, "Container Solution source has changed")
    prefix = f"{SOLUTIONS_ROOT}/{scope.solution_id}/" if scope.solution_id else ""
    for path in review.source_pins:
        validate_module_path(path)
        if not path.endswith(".py") or path.startswith(f"{SOLUTIONS_ROOT}/"):
            raise ModuleSourceError(403, "Invalid Container approved source path")
    return replace(scope, source_hashes={prefix + path: digest for path, digest in review.source_pins.items()})
