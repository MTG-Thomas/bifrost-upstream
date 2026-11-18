"""Durable approval review and exact-snapshot verification with PostgreSQL and S3."""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from src.config import get_settings
from src.core.redis_client import pending_execution_record
from src.jobs.platform.cloudflare_reapproval import CLOUDFLARE_REAPPROVAL_DEFINITION
from src.models.orm import Organization, PlatformJob, Workflow
from src.services.execution import workflow_dispatch
from src.services.execution.cloudflare_analysis import AnalysisRejected
from src.services.execution.cloudflare_eligibility import (
    accept_proposal,
    current_approval,
    prepare_approved_workspace,
    propose_approval,
)
from src.services.file_index_service import FileIndexService
from src.services.repo_storage import RepoStorage


@pytest.mark.e2e
async def test_review_is_bound_to_indexed_sources_and_workflow_owner(db_session):
    workflow_id = uuid4()
    path = f"workflows/review_{workflow_id.hex}.py"
    workflow = Workflow(id=workflow_id, name="Review fixture", path=path, function_name="run")
    db_session.add(workflow)
    index = FileIndexService(db_session)

    async def propose():
        return await propose_approval(workflow_id, subrequest_allowance=5, cpu_budget_ms=100)

    async def prepare(proposal):
        return await prepare_approved_workspace(
            workflow_id=str(workflow_id), entry_path=path, function_name="run",
            entry_sha256=proposal.declaration.entry_sha256, expected_revision=proposal.revision,
        )

    try:
        await index.write(path, b"def run(): return 1\n")
        await db_session.commit()
        stale = await propose()
        await index.write(path, b"def run(): return 2\n")
        await db_session.commit()
        with pytest.raises(AnalysisRejected, match="source pins"):
            await accept_proposal(db_session, stale, accepted_by="test operator")
        await db_session.rollback()

        reviewed = await propose()
        await accept_proposal(db_session, reviewed, accepted_by="test operator")
        await db_session.commit()
        workspace = await prepare(reviewed)
        assert workspace is not None
        assert workspace.bundle["sources"] == {path: "def run(): return 2\n"}

        new_owner = Organization(id=uuid4(), name=f"Cloudflare review {workflow_id}", created_by="test")
        db_session.add(new_owner)
        await db_session.flush()
        workflow = await db_session.get(Workflow, workflow_id)
        assert workflow is not None
        workflow.organization_id = new_owner.id
        await db_session.commit()
        assert await prepare(reviewed) is None
    finally:
        await db_session.rollback()
        workflow = await db_session.get(Workflow, workflow_id)
        if workflow is not None:
            owner_id = workflow.organization_id
            await db_session.delete(workflow)
            await db_session.flush()
            if owner_id:
                owner = await db_session.get(Organization, owner_id)
                if owner is not None:
                    await db_session.delete(owner)
        await index.delete(path)
        await db_session.commit()


@pytest.mark.e2e
async def test_changed_module_carries_forward_through_live_platform_job(
    db_session, async_session_factory, monkeypatch,
):
    workflow_id = uuid4()
    module = f"helper_{workflow_id.hex}"
    module_path = f"{module}.py"
    path = f"workflows/derive_{workflow_id.hex}.py"
    db_session.add(Workflow(id=workflow_id, name="Derivation fixture", path=path, function_name="run"))
    index = FileIndexService(db_session)
    monkeypatch.setattr(workflow_dispatch, "get_settings", lambda: get_settings().model_copy(
        update={"cloudflare_worker_hello_enabled": True},
    ))
    pending = pending_execution_record(
        execution_id=str(uuid4()), workflow_id=str(workflow_id), parameters={}, org_id=None,
        user_id=str(uuid4()), user_name="Test", user_email="test@example.invalid", lineage=None,
    )
    message = {"workflow_id": str(workflow_id)}
    job_type = CLOUDFLARE_REAPPROVAL_DEFINITION.job_type
    job_filter = (PlatformJob.job_type == job_type, PlatformJob.resource_id == str(workflow_id))

    async def marker():
        return await workflow_dispatch._cloudflare_revision(db_session, pending, message)

    async def prepare(revision, entry_sha256):
        return await prepare_approved_workspace(
            workflow_id=str(workflow_id), entry_path=path, function_name="run",
            entry_sha256=entry_sha256, expected_revision=revision,
        )

    try:
        entry_sha = await index.write(path, f"from {module} import VALUE\ndef run(): return VALUE\n".encode())
        old_sha = await index.write(module_path, b"VALUE = 'https://example.com/test'\n")
        await db_session.commit()
        proposal = await propose_approval(workflow_id, subrequest_allowance=5, cpu_budget_ms=100)
        await accept_proposal(db_session, proposal, accepted_by="test operator")
        await db_session.commit()
        assert await marker() == proposal.revision

        new_sha = await index.write(module_path, b"VALUE = 'updated'\n")
        assert await marker() is None
        assert await marker() is None
        job_id = (await db_session.execute(select(PlatformJob.id).where(*job_filter))).scalar_one()
        await db_session.commit()
        async with asyncio.timeout(120):
            while True:
                async with async_session_factory() as observer:
                    job = await observer.get(PlatformJob, job_id)
                    assert job is not None
                    if job.completed_at is not None:
                        assert job.status == "succeeded", (job.error_code, job.error_message)
                        assert job.started_at is not None
                        result = job.result
                        break
                await asyncio.sleep(0.1)

        derived = await current_approval(db_session, workflow_id)
        assert derived is not None
        assert result == {"outcome": "approved", "revision": derived.revision}
        assert derived.derived_from == proposal.revision
        assert derived.pin_diff == {module_path: {"before": old_sha, "after": new_sha}}
        assert derived.declaration["https_hosts"] == []
        assert await marker() == derived.revision
        assert await prepare(proposal.revision, entry_sha) is None
        workspace = await prepare(derived.revision, entry_sha)
        assert workspace is not None
        verified_sources = workspace.bundle["sources"]
        assert isinstance(verified_sources, dict)
        assert verified_sources[module_path] == "VALUE = 'updated'\n"
        await RepoStorage().write(module_path, b"VALUE = 'changed after enqueue'\n")
        assert await prepare(derived.revision, entry_sha) is None
    finally:
        await db_session.rollback()
        await db_session.execute(delete(PlatformJob).where(*job_filter))
        await db_session.execute(delete(Workflow).where(Workflow.id == workflow_id))
        await index.delete(path)
        await index.delete(module_path)
        await db_session.commit()
