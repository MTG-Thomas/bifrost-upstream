"""Reapproval outcomes and shared queue semantics."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, text
from src.core.storage.contracts import StorageError
from src.jobs.platform import cloudflare_reapproval as reapproval
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobFailure,
)
from src.models.orm import CloudflareApproval, FileIndex, PlatformJob
from src.services import platform_jobs
from src.services.execution.cloudflare_analysis import (
    POLICY_VERSION,
    AnalysisRejected,
    SourceUnresolved,
)
from src.services.execution.cloudflare_eligibility import (
    ApprovalDeclaration,
    ApprovalProposal,
    ObservationChanged,
    SolutionApprovalScope,
)
from src.services.platform_job_lease import LeaseLost


@pytest.fixture
def scenario():
    path = "fixture.py"
    parent = ApprovalDeclaration(
        entry_path=path, function_name="run", entry_sha256="a" * 64, module_sha256={},
        manifest_sha256="a" * 64, packages=["httpx"], https_hosts=["example.com"],
        required_https_hosts=["example.com"], estimated_subrequests=2, subrequest_allowance=5,
        estimated_cpu_ms=10, cpu_budget_ms=100,
    )
    approval = CloudflareApproval(
        workflow_id=uuid4(), revision="b" * 64, policy_version=POLICY_VERSION, declaration=parent.model_dump(),
    )
    payload = reapproval.ReapprovalPayload(
        workflow_id=approval.workflow_id, parent_revision=approval.revision, observed_pins={path: "c" * 64},
    )
    context = PlatformJobContext(uuid4(), uuid4(), None, "system", "system@gobifrost.local", "System")
    return approval, parent, payload, context


@pytest.fixture
def worker(monkeypatch, scenario):
    approval, parent, payload, _ = scenario
    candidate = ApprovalProposal(approval.workflow_id, None, approval.revision, parent.model_copy(update={
        "entry_sha256": "c" * 64,
    }))

    @asynccontextmanager
    async def db_context():
        yield object()

    monkeypatch.setattr(reapproval, "get_db_context", db_context)
    monkeypatch.setattr(reapproval, "current_approval", AsyncMock(return_value=approval))
    monkeypatch.setattr(reapproval, "indexed_pins", AsyncMock(return_value=payload.observed_pins))
    monkeypatch.setattr(reapproval, "propose_approval", AsyncMock(return_value=candidate))
    monkeypatch.setattr(reapproval, "hold_lease", AsyncMock())
    monkeypatch.setattr(reapproval, "accept_proposal", AsyncMock(return_value=CloudflareApproval()))
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    return candidate


@pytest.mark.parametrize("changed", [
    {"https_hosts": ["new.example.com"]},
    {"packages": ["httpx", "pydantic"]},
])
async def test_new_egress_or_package_needs_review(scenario, worker, changed):
    _, _, payload, context = scenario
    reapproval.propose_approval.return_value = ApprovalProposal(
        worker.workflow_id, None, worker.previous_revision, worker.declaration.model_copy(update=changed),
    )
    assert await reapproval._run_reapproval(context, payload) == {
        "outcome": "review_required", "reason": "approval_envelope_changed", "analysed_pins": payload.observed_pins,
    }
    reapproval.accept_proposal.assert_not_awaited()


@pytest.mark.parametrize("candidate_host, allowed", [
    ("acme.halopsa.com", True),
    ("halopsa.com", False),
    ("a.b.halopsa.com", False),
    ("*.other.example.com", False),
])
def test_carry_forward_requires_concrete_hosts_covered_by_parent(scenario, candidate_host, allowed):
    _, parent, _, _ = scenario
    parent = parent.model_copy(update={"https_hosts": ["*.halopsa.com", "example.com"]})
    candidate = parent.model_copy(update={"https_hosts": [candidate_host]})
    assert reapproval._carry_forward(parent, candidate) is allowed


async def test_carry_forward_rebinds_unchanged_pins_to_new_solution_generation(
    scenario, worker, monkeypatch,
):
    approval, parent, payload, context = scenario
    before = SolutionApprovalScope(install_id=uuid4(), version="1", source_generation=uuid4())
    after = before.model_copy(update={"version": "2", "source_generation": uuid4()})
    parent = parent.model_copy(update={"solution": before, "entry_sha256": worker.declaration.entry_sha256})
    approval.declaration = parent.model_dump()
    payload = payload.model_copy(update={"solution": after})
    candidate = ApprovalProposal(
        worker.workflow_id, None, worker.previous_revision, parent.model_copy(update={"solution": after}),
    )
    reapproval.propose_approval.return_value = candidate
    monkeypatch.setattr(reapproval, "solution_approval_scope", AsyncMock(return_value=after))
    assert await reapproval._run_reapproval(context, payload) == {"outcome": "approved", "revision": candidate.revision}
    assert reapproval.accept_proposal.return_value.pin_diff == {}
    other = after.model_copy(update={"install_id": uuid4()})
    assert not reapproval._carry_forward(parent, candidate.declaration.model_copy(update={"solution": other}))


@pytest.mark.parametrize("change", ["edit", "owner"])
async def test_competing_edit_or_owner_change_supersedes_job(scenario, worker, change):
    _, _, payload, context = scenario
    if change == "edit":
        reapproval.indexed_pins.side_effect = [payload.observed_pins, {}]
    else:
        reapproval.propose_approval.return_value = ApprovalProposal(
            worker.workflow_id, uuid4(), worker.previous_revision, worker.declaration,
        )
    assert await reapproval._run_reapproval(context, payload) == {"outcome": "superseded"}
    reapproval.accept_proposal.assert_not_awaited()


async def test_lease_loss_prevents_acceptance(scenario, worker):
    _, _, payload, context = scenario
    reapproval.hold_lease.side_effect = LeaseLost()
    with pytest.raises(PlatformJobCancelled):
        await reapproval._run_reapproval(context, payload)
    reapproval.accept_proposal.assert_not_awaited()


@pytest.mark.parametrize("error, outcome", [
    (ObservationChanged("changed"), "superseded"),
    (ObservationChanged("Expected an active workflow"), "superseded"),
    (AnalysisRejected("Unresolved import: requests"), "review_required"),
    (SourceUnresolved("Indexed source missing"), "retryable"),
])
async def test_analysis_failure_outcomes(scenario, worker, error, outcome):
    _, _, payload, context = scenario
    reapproval.propose_approval.side_effect = error
    if outcome == "retryable":
        with pytest.raises(PlatformJobFailure) as failure:
            await reapproval._run_reapproval(context, payload)
        assert failure.value.retryable
    else:
        result = await reapproval._run_reapproval(context, payload)
        assert result["outcome"] == outcome
        if outcome == "review_required":
            assert result["analysed_pins"] == payload.observed_pins


async def test_stage_failure_is_advisory(scenario, db_session, caplog):
    _, _, payload, _ = scenario
    definition = reapproval.CLOUDFLARE_REAPPROVAL_DEFINITION
    dedupe = reapproval._dedupe_key(payload)
    await db_session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext('bifrost:platform-job:' || :key))"),
        {"key": f"{definition.job_type}:{dedupe}"},
    )
    await reapproval.stage_reapproval(reapproval.ReapprovalRequest(payload, None))
    (record,) = [record for record in caplog.records if "Could not stage Cloudflare reapproval" in record.message]
    assert record.exc_info is not None and record.exc_info[0] is TimeoutError
    assert await db_session.scalar(select(PlatformJob.id).where(PlatformJob.dedupe_key == dedupe)) is None


async def test_shared_dedupe_cooldown_and_deterministic_refusal(scenario, db_session):
    approval, _, payload, _ = scenario
    helper = f"helper_{approval.workflow_id.hex}.py"
    file = FileIndex(path=helper, content_hash="d" * 64)
    db_session.add(file)

    async def stage():
        await reapproval._stage_reapproval(db_session, reapproval.ReapprovalRequest(payload, None))
        return (await db_session.scalars(select(PlatformJob).where(
            PlatformJob.resource_id == str(approval.workflow_id),
        ).order_by(PlatformJob.created_at))).all()

    (job,) = await stage()
    job.created_at = datetime.now(UTC) - timedelta(minutes=11)
    assert len(await stage()) == 1  # Shared active-job deduplication.
    job.status = "succeeded"
    job.result = {"outcome": "review_required", "analysed_pins": {helper: file.content_hash}}
    assert len(await stage()) == 1  # Deterministic beyond the cooldown.
    job.created_at = datetime.now(UTC)
    file.content_hash = "e" * 64
    assert len(await stage()) == 2  # Fixing a new module invalidates the refusal immediately.
    job, retry = await stage()
    retry.status = "failed"
    assert len(await stage()) == 2  # Transient within the cooldown.
    retry.created_at -= timedelta(minutes=11)
    assert len(await stage()) == 3


async def test_retryable_storage_failure_requeues_shared_job(scenario, worker, async_session_factory, monkeypatch):
    _, _, payload, context = scenario
    reapproval.propose_approval.side_effect = StorageError("backend_error", provider_code="FixtureUnavailable")
    with pytest.raises(PlatformJobFailure) as failed:
        await reapproval._run_reapproval(context, payload)
    monkeypatch.setattr(platform_jobs, "publish_platform_job_update", AsyncMock())
    definition = reapproval.CLOUDFLARE_REAPPROVAL_DEFINITION
    async with async_session_factory() as db:
        db.add(PlatformJob(
            id=context.job_id, job_type=definition.job_type, payload=payload.model_dump(mode="json"),
            requested_by_user_id="system", requested_by_email="system@gobifrost.local",
            requested_by_name="System", title="Reapproval retry", status="running", attempt=1,
            max_attempts=definition.policy.max_attempts, lease_token=context.lease_token,
        ))
        await db.commit()
    try:
        assert await platform_jobs.retry_platform_job_failure(
            context.job_id, context.lease_token, failed.value, enabled=definition.policy.retry_on_failure,
        )
        async with async_session_factory() as db:
            job = await db.get(PlatformJob, context.job_id)
            assert job is not None and job.status == "queued" and job.lease_token is None
    finally:
        async with async_session_factory() as db:
            await db.execute(delete(PlatformJob).where(PlatformJob.id == context.job_id))
            await db.commit()
