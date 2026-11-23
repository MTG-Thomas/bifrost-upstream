"""Runtime eligibility for dispatch claims and capacity planning."""

from datetime import datetime, timedelta
from typing import cast
from uuid import UUID

from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.orm import aliased
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

from src.models.enums import LIVE_EXECUTION_STATUSES
from src.models.orm.agent_runs import AgentRun
from src.models.orm.cloudflare_capacity import CloudflareWorkerReservation
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.models.orm.work_dispatches import WorkDispatch
from src.models.orm.workflows import Workflow
from src.services.work_dispatch import WAKE_AGENT_RUN, WAKE_AGENT_SUMMARY, WAKE_WORKFLOW
from src.worker.remote.protocol import (
    CLOUDFLARE_DEV_BUDGET_BYTES,
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    CLOUDFLARE_WORKFLOW_HANDLER,
    LINUX_RUNTIME_PROFILE,
    WORKERS_DEADLINE_DISPATCH_SECONDS,
    WORKERS_DEADLINE_DRAIN_SECONDS,
    WORKERS_DEADLINE_SKEW_SECONDS,
)

WORKERS_CLAIM_DRAIN_SECONDS = WORKERS_DEADLINE_DRAIN_SECONDS - WORKERS_DEADLINE_SKEW_SECONDS


def dispatch_timeout_seconds() -> ColumnElement[int]:
    # Mirror the consumer's selection of frozen metadata or the current
    # workflow. An unknown timeout cannot reserve managed Workers capacity.
    workflow_timeout = select(Workflow.timeout_seconds).join(Execution, Execution.workflow_id == Workflow.id).where(
        Execution.id == WorkDispatch.subject_id,
    ).correlate(WorkDispatch).scalar_subquery()
    return func.coalesce(WorkDispatch.message["dispatch_metadata"]["timeout_seconds"].as_integer(), workflow_timeout)


def timeout_fits_workers_deadline(
    timeout_seconds: ColumnElement[int] | int, *, now: ColumnElement[datetime] | None = None,
) -> ColumnElement[bool]:
    # Allow for a launcher clock that reaches its stop time before the DB clock.
    return and_(
        timeout_seconds > 0,
        (func.clock_timestamp() if now is None else now) + func.make_interval(0, 0, 0, 0, 0, 0, timeout_seconds)
        <= CloudflareWorkerReservation.workers_deadline_at + timedelta(seconds=WORKERS_CLAIM_DRAIN_SECONDS),
    )


def managed_workflow_fits(worker_id: UUID) -> ColumnElement[bool]:
    pin = CloudflareWorkerReservation
    return exists(select(pin.worker_id).where(
        pin.worker_id == worker_id,
        pin.workers_lifetime_seconds.is_not(None),
        pin.workers_deadline_at > func.clock_timestamp() + timedelta(seconds=WORKERS_DEADLINE_DISPATCH_SECONDS),
        timeout_fits_workers_deadline(dispatch_timeout_seconds()),
    ).correlate(WorkDispatch))


def cloudflare_dispatch_sql() -> ColumnElement[bool]:
    return (
        (WorkDispatch.handler == CLOUDFLARE_WORKFLOW_HANDLER[0])
        & (WorkDispatch.handler_version == CLOUDFLARE_WORKFLOW_HANDLER[1])
        & (WorkDispatch.execution_lane == "normal")
        & WorkDispatch.cloudflare_revision.is_not(None)
        & (WorkDispatch.memory_request_bytes <= CLOUDFLARE_DEV_BUDGET_BYTES)
    )


def containers_workflow_fits(handler: str | None, version: int | None, revision: str | None, size: int) -> bool:
    return (handler, version) == CLOUDFLARE_WORKFLOW_HANDLER and (
        revision is None or size > CLOUDFLARE_DEV_BUDGET_BYTES
    )


def cloudflare_active_claims() -> Select[tuple[UUID, UUID | None, UUID | None]]:
    """Live Cloudflare claims, including any claim with a missing execution record."""
    worker = aliased(ExecutionWorker)
    return (
        select(
            ExecutionWorkerClaim.worker_id, Execution.organization_id.label("tenant_id"),
            # SQLAlchemy types the column from the ORM and does not infer
            # the outer join makes the execution identity nullable.
            cast(ColumnElement[UUID | None], Execution.id.label("execution_id")),
        )
        .join(worker, worker.id == ExecutionWorkerClaim.worker_id)
        .outerjoin(Execution, and_(
            Execution.id == ExecutionWorkerClaim.subject_id, ExecutionWorkerClaim.family == WAKE_WORKFLOW,
        ))
        .outerjoin(AgentRun, and_(
            AgentRun.id == ExecutionWorkerClaim.subject_id,
            ExecutionWorkerClaim.family.in_((WAKE_AGENT_SUMMARY, WAKE_AGENT_RUN)),
        ))
        .where(
            worker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
            worker.state.in_(("ready", "draining")),
            or_(
                and_(ExecutionWorkerClaim.family == WAKE_WORKFLOW,
                     Execution.id.is_(None) | Execution.status.in_(LIVE_EXECUTION_STATUSES)),
                and_(ExecutionWorkerClaim.family == WAKE_AGENT_SUMMARY,
                     AgentRun.summary_status.in_(("pending", "generating"))),
                and_(ExecutionWorkerClaim.family == WAKE_AGENT_RUN,
                     AgentRun.status.in_(("queued", "running", "cancelling"))),
            ),
        )
    )


def profile_matches(runtime_profile: str, handler: str | None, version: int | None, revision: str | None) -> bool:
    """Match Normal-lane planner requests, including local jobs (handler None)."""
    return runtime_profile == LINUX_RUNTIME_PROFILE or (
        runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
        and (handler, version) == CLOUDFLARE_WORKFLOW_HANDLER
        and revision is not None
    )


def declared_alternative_sql() -> ColumnElement[bool]:
    """An authenticated launcher might execute work above Linux's ceiling."""
    return (
        cloudflare_dispatch_sql()
        & exists().where(
            ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
            ExecutionWorker.form == "external",
            ExecutionWorker.state == "ready",
            ExecutionWorker.execution_lane == "normal",
            ExecutionWorker.remote_owner_lease_expires_at > func.now(),
            ExecutionWorker.supported_handlers.contains([CLOUDFLARE_WORKFLOW_HANDLER[0]]),
        )
    )
