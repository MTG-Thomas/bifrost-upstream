"""Runtime eligibility for dispatch claims and capacity planning."""

from typing import cast
from uuid import UUID

from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.orm import aliased
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

from src.models.enums import LIVE_EXECUTION_STATUSES
from src.models.orm.agent_runs import AgentRun
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.models.orm.work_dispatches import WorkDispatch
from src.services.work_dispatch import WAKE_AGENT_RUN, WAKE_AGENT_SUMMARY, WAKE_WORKFLOW
from src.worker.remote.protocol import (
    CLOUDFLARE_DEV_BUDGET_BYTES,
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    CLOUDFLARE_WORKFLOW_HANDLER,
    LINUX_RUNTIME_PROFILE,
)


def cloudflare_dispatch_sql() -> ColumnElement[bool]:
    return (
        (WorkDispatch.handler == CLOUDFLARE_WORKFLOW_HANDLER[0])
        & (WorkDispatch.handler_version == CLOUDFLARE_WORKFLOW_HANDLER[1])
        & (WorkDispatch.execution_lane == "normal")
        & WorkDispatch.cloudflare_revision.is_not(None)
        & (WorkDispatch.memory_request_bytes <= CLOUDFLARE_DEV_BUDGET_BYTES)
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
