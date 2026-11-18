"""Durable approval review and exact-snapshot verification with PostgreSQL and S3."""

from uuid import uuid4

import pytest
from src.models.orm import Organization, Workflow
from src.services.execution.cloudflare_analysis import AnalysisRejected
from src.services.execution.cloudflare_eligibility import (
    accept_proposal,
    prepare_approved_workspace,
    propose_approval,
)
from src.services.file_index_service import FileIndexService


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
