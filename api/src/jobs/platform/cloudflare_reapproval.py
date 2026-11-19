"""Carry forward the current operator envelope for changed workspace sources."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import and_, column, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.core.storage.contracts import StorageError
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobFailure,
    define_platform_job,
)
from src.models.orm import FileIndex, PlatformJob
from src.services.execution.cloudflare_analysis import (
    MAX_MODULES,
    POLICY_VERSION,
    AnalysisRejected,
    SourceUnresolved,
    digest,
)
from src.services.execution.cloudflare_eligibility import (
    ApprovalDeclaration,
    ObservationChanged,
    accept_proposal,
    current_approval,
    indexed_pins,
    propose_approval,
)
from src.services.execution.cloudflare_hosts import egress_host_allowed
from src.services.platform_job_lease import LeaseLost, hold_lease
from src.services.platform_jobs import enqueue_platform_job

logger = logging.getLogger(__name__)

_JOB_TYPE = "workflow.cloudflare_reapproval"
_ACTOR = "Cloudflare approval policy"
_Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
_OBSERVATION_COOLDOWN = timedelta(minutes=10)


class ReapprovalPayload(BaseModel):
    workflow_id: UUID
    parent_revision: _Digest
    observed_pins: dict[str, _Digest | None] = Field(min_length=1, max_length=MAX_MODULES)


@dataclass(frozen=True)
class ReapprovalRequest:
    payload: ReapprovalPayload
    organization_id: UUID | None


def _carry_forward(
    parent: ApprovalDeclaration, candidate: ApprovalDeclaration,
) -> bool:
    parent_hosts = set(parent.https_hosts)
    return (
        (parent.entry_path, parent.function_name) == (candidate.entry_path, candidate.function_name)
        and set(candidate.packages) == set(parent.packages)
        and all(egress_host_allowed(host, parent_hosts) for host in candidate.https_hosts)
    )


def _pin_diff(before: dict[str, str], after: dict[str, str]) -> dict[str, dict[str, str | None]]:
    return {
        path: {"before": before.get(path), "after": after.get(path)}
        for path in sorted(before.keys() | after.keys())
        if before.get(path) != after.get(path)
    }


async def _observation_matches(db: AsyncSession, payload: ReapprovalPayload) -> bool:
    indexed = await indexed_pins(db, set(payload.observed_pins))
    return all(indexed.get(path) == pin for path, pin in payload.observed_pins.items())


async def _run_reapproval(context: PlatformJobContext, payload: ReapprovalPayload) -> dict[str, Any]:
    async with get_db_context() as db:
        current = await current_approval(db, payload.workflow_id)
        if (
            current is None or current.revision != payload.parent_revision
            or current.policy_version != POLICY_VERSION or current.organization_id != context.organization_id
        ):
            return {"outcome": "superseded"}
        parent = ApprovalDeclaration.model_validate(current.declaration)
        if (
            parent.manifest.keys() != payload.observed_pins.keys()
            or not await _observation_matches(db, payload)
        ):
            return {"outcome": "superseded"}

    analysed_pins = dict(payload.observed_pins)

    async def review_required(reason: str) -> dict[str, Any]:
        async with get_db_context() as db:
            indexed = await indexed_pins(db, set(analysed_pins))
        if any(indexed.get(path) != pin for path, pin in analysed_pins.items()):
            return {"outcome": "superseded"}
        return {"outcome": "review_required", "reason": reason, "analysed_pins": analysed_pins}

    await context.report("Analyzing workspace revision", percent=25)
    try:
        candidate = await propose_approval(
            payload.workflow_id, subrequest_allowance=parent.subrequest_allowance, cpu_budget_ms=parent.cpu_budget_ms,
            expected_pins=payload.observed_pins, analysed_pins=analysed_pins,
        )
    except ObservationChanged:
        return {"outcome": "superseded"}
    except (StorageError, TimeoutError, SourceUnresolved) as exc:
        raise PlatformJobFailure(
            "cloudflare_source_unavailable", "Could not resolve workspace sources", retryable=True,
        ) from exc
    except AnalysisRejected as exc:
        return await review_required(str(exc))

    if candidate.previous_revision != payload.parent_revision or candidate.organization_id != context.organization_id:
        return {"outcome": "superseded"}
    pins = candidate.declaration.manifest
    if any(payload.observed_pins[path] != pins[path] for path in payload.observed_pins.keys() & pins.keys()):
        return {"outcome": "superseded"}
    if not _carry_forward(parent, candidate.declaration):
        return await review_required("approval_envelope_changed")
    diff = _pin_diff(parent.manifest, pins)
    if not diff:
        return {"outcome": "unchanged"}

    candidate = replace(candidate, declaration=candidate.declaration.model_copy(
        update={"https_hosts": parent.https_hosts},
    ))
    try:
        async with get_db_context() as db:
            await hold_lease(db, context.job_id, context.lease_token)
            if not await _observation_matches(db, payload):
                return {"outcome": "superseded"}
            approval = await accept_proposal(db, candidate, accepted_by=_ACTOR)
            approval.derived_from = payload.parent_revision
            approval.pin_diff = diff
    except LeaseLost as exc:
        raise PlatformJobCancelled from exc
    except AnalysisRejected:
        return {"outcome": "superseded"}
    return {"outcome": "approved", "revision": candidate.revision}


CLOUDFLARE_REAPPROVAL_DEFINITION = define_platform_job(_JOB_TYPE, ReapprovalPayload, _run_reapproval)


_staging: dict[str, asyncio.Task[None]] = {}


def _dedupe_key(payload: ReapprovalPayload) -> str:
    return digest(["reapproval-v4", payload.parent_revision, payload.observed_pins])


def schedule_reapproval(request: ReapprovalRequest) -> None:
    """Start best-effort enqueue after run commit. PlatformJob owns analysis."""
    key = _dedupe_key(request.payload)
    if key not in _staging:
        task = asyncio.create_task(stage_reapproval(request))
        _staging[key] = task
        task.add_done_callback(lambda _: _staging.pop(key, None))


async def stage_reapproval(request: ReapprovalRequest) -> None:
    """Stage optional work in its own short transaction after workflow commit."""
    try:
        async with asyncio.timeout(2), get_db_context() as db:
            await _stage_reapproval(db, request)
    except Exception:
        logger.warning(
            "Could not stage Cloudflare reapproval for workflow %s", request.payload.workflow_id, exc_info=True,
        )


async def _stage_reapproval(db: AsyncSession, request: ReapprovalRequest) -> None:
    payload = request.payload
    workflow_key = f"{_JOB_TYPE}:{payload.workflow_id}"
    dedupe_key = _dedupe_key(payload)
    analysed = func.json_each_text(PlatformJob.result["analysed_pins"]).table_valued(
        column("key"), column("value"),
    )
    changed_pin = (
        select(1)
        .select_from(analysed.outerjoin(FileIndex, FileIndex.path == analysed.c.key))
        .where(FileIndex.content_hash.is_distinct_from(analysed.c.value))
        .correlate(PlatformJob)
        .exists()
    )
    outcome = PlatformJob.result["outcome"].as_string()
    refused = and_(PlatformJob.status == "succeeded", outcome.is_not_distinct_from("review_required"))
    await enqueue_platform_job(
        db, CLOUDFLARE_REAPPROVAL_DEFINITION, payload,
        dedupe_key=dedupe_key, resource_lock_key=workflow_key,
        reuse_when=or_(
            and_(~refused, PlatformJob.created_at >= datetime.now(UTC) - _OBSERVATION_COOLDOWN),
            and_(refused, ~changed_pin),
        ),
        organization_id=request.organization_id, requested_by_user_id="system",
        requested_by_email="system@gobifrost.local", requested_by_name=_ACTOR,
        resource_type="workflow", resource_id=str(payload.workflow_id),
        title="Analyze Cloudflare Workflow Revision", action_url=None,
    )
