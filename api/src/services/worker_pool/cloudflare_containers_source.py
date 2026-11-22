"""Bind module source reads to an execution's managed Linux reservation."""

from dataclasses import replace

from pydantic import ValidationError
from shared.sdk_modules import (
    ModuleSourceError,
    ModuleSourceScope,
    validate_module_path,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.cloudflare_approvals import CloudflareApproval
from src.models.orm.cloudflare_capacity import CloudflareWorkerReservation
from src.models.orm.execution_workers import ExecutionWorkerClaim
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


def container_execution_reservations(execution_id):
    """Identify managed executions even when their claim has already gone."""
    pin = CloudflareWorkerReservation
    return select(pin).where(
        pin.scope == CONTAINERS_SCOPE, pin.container_reserved_at.is_not(None),
        pin.container_envelope["execution_id"].as_string() == str(execution_id),
        pin.container_envelope["voided_unstarted"].as_boolean().is_not(True),
    )


async def scoped_container_source(
    db: AsyncSession, runtime: RuntimeAuthority, workflow: Workflow | None, scope: ModuleSourceScope,
) -> ModuleSourceScope:
    pin = CloudflareWorkerReservation
    claim = ExecutionWorkerClaim
    owns_claim = select(claim.id).where(
        claim.worker_id == pin.worker_id, claim.family == WAKE_WORKFLOW, claim.subject_id == runtime.credential_id,
    ).correlate(pin).exists()
    matches = (await db.execute(container_execution_reservations(runtime.credential_id).add_columns(
        container_deadline_sql() > func.clock_timestamp(), owns_claim,
    ).limit(2))).all()
    if not matches:
        return scope
    reservation, before_deadline, has_claim = matches[0]
    if (
        len(matches) != 1 or not before_deadline or not has_claim or reservation.container_stopped_at is not None
        or reservation.container_tenant != tenant_key(runtime.organization_id)
        or reservation.container_envelope is None or workflow is None
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
