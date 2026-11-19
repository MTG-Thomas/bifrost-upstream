"""Durable approval review and exact-snapshot verification with PostgreSQL and S3."""

import asyncio
import json
from argparse import Namespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete, select

from bifrost.cloudflare_analysis import AnalysisRejected
from scripts import cloudflare_workflow_approval as approval_cli
from src.config import get_settings
from src.core.redis_client import pending_execution_record
from src.jobs.platform.cloudflare_reapproval import CLOUDFLARE_REAPPROVAL_DEFINITION
from src.models.orm import CloudflareApproval, Organization, PlatformJob, Workflow
from src.models.orm.audit import AuditLog
from src.services.authorization.tokens import RuntimeSubject, issue_run_refresh_token
from src.services.execution import workflow_dispatch
from src.services.execution.cloudflare_eligibility import (
    POLICY_VERSION,
    accept_proposal,
    current_approval,
    prepare_approved_workspace,
    propose_approval,
    revoke_approval,
)
from src.services.file_index_service import FileIndexService
from src.services.repo_storage import RepoStorage
from tests.unit.authorization.committed_runs import committed_world


@pytest.mark.e2e
@pytest.mark.parametrize(("mode", "allowed"), [("restricted", False), ("full", True)])
async def test_api_identity_sdk_operations_apply_run_permissions(async_engine, e2e_api_url, mode, allowed):
    async with committed_world(async_engine) as world:
        org = await world.organization()
        run_user = await world.person(org)
        target = await world.person(org)
        target.email = f"cloudflare-approval-{target.id}@example.com"
        role = await world.role()
        workflow = await world.workflow(org, permission_mode=mode)
        execution = await world.execution(run_user=run_user, workflow=workflow, organization=org)
        await world.session.commit()

        try:
            async with httpx.AsyncClient(base_url=e2e_api_url, timeout=30) as client:
                async def refresh_access():
                    refresh = issue_run_refresh_token(RuntimeSubject("workflow_execution", execution.id))
                    token = await client.post("/auth/refresh", json={"refresh_token": refresh})
                    token.raise_for_status()
                    client.headers["Authorization"] = f"Bearer {token.json()['access_token']}"

                await refresh_access()
                assert (await client.get("/api/sdk/context")).status_code == 200
                assignment = await client.post(f"/api/roles/{role.id}/users", json={"user_ids": [str(target.id)]})
                assert assignment.status_code == (204 if allowed else 403), assignment.text
                if allowed:
                    # A role assignment changes the API authority epoch.
                    await refresh_access()
                update = await client.patch(f"/api/users/{target.id}", json={"name": "Updated"})
                assert update.status_code == (200 if allowed else 403), update.text
                if allowed:
                    assert update.json()["name"] == "Updated"
        finally:
            await world.session.execute(delete(AuditLog).where(AuditLog.organization_id == org.id))
            await world.session.commit()


@pytest.mark.e2e
async def test_operator_cli_reviews_wildcard_and_binds_it_to_the_accepted_digest(db_session, capsys):
    workflow_id = uuid4()
    path = f"workflows/wildcard_{workflow_id.hex}.py"
    db_session.add(Workflow(id=workflow_id, name="Wildcard review fixture", path=path, function_name="run"))
    index = FileIndexService(db_session)
    args = Namespace(command="propose", workflow_id=workflow_id, subrequest_allowance=5,
                     cpu_budget_ms=100, https_host=[], operator="test operator")

    try:
        await index.write(path, b"from js import fetch\nasync def run(tenant):\n"
                                b"    return await fetch(f'https://{tenant}.halopsa.com/api')\n")
        await db_session.commit()
        await approval_cli.run(args)
        unreviewed = json.loads(capsys.readouterr().out)
        assert unreviewed["unresolved_url_calls"] == 1
        assert unreviewed["https_hosts"] == []

        args.https_host = ["*.github.io"]
        with pytest.raises(AnalysisRejected, match="registrable domain outside the PSL private roots"):
            await approval_cli.run(args)

        args.https_host = ["*.halopsa.com"]
        await approval_cli.run(args)
        reviewed = json.loads(capsys.readouterr().out)
        assert next(iter(reviewed)) == "wildcard_https_hosts"
        assert reviewed["wildcard_https_hosts"] == reviewed["https_hosts"] == ["*.halopsa.com"]

        args.command = "accept"
        args.review_sha256 = unreviewed["review_sha256"]
        with pytest.raises(AnalysisRejected, match="generated proposal has changed"):
            await approval_cli.run(args)
        assert await current_approval(db_session, workflow_id) is None

        args.review_sha256 = reviewed["review_sha256"]
        await approval_cli.run(args)
        accepted = await current_approval(db_session, workflow_id)
        assert accepted is not None and accepted.revision == args.review_sha256
        assert accepted.declaration["https_hosts"] == ["*.halopsa.com"]
    finally:
        await db_session.rollback()
        await db_session.execute(delete(Workflow).where(Workflow.id == workflow_id))
        await index.delete(path)
        await db_session.commit()


@pytest.mark.e2e
async def test_review_is_bound_to_indexed_sources_and_workflow_owner(db_session, async_session_factory):
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
        async def accept():
            async with async_session_factory() as session:
                try:
                    await accept_proposal(session, reviewed, accepted_by="test operator")
                    await session.commit()
                except AnalysisRejected as exc:
                    return str(exc)
                return "accepted"

        results = await asyncio.gather(accept(), accept())
        assert sorted(results) == ["Workflow scope, source pins or current approval changed during review", "accepted"]
        workspace = await prepare(reviewed)
        assert workspace is not None
        assert workspace.bundle["sources"] == {path: "def run(): return 2\n"}

        collision = await propose()
        db_session.add(CloudflareApproval(
            revision=collision.revision, workflow_id=workflow_id, policy_version=POLICY_VERSION,
            declaration=collision.declaration.model_dump(mode="json"), accepted_by="collision fixture",
            is_current=False,
        ))
        await db_session.commit()
        with pytest.raises(AnalysisRejected, match="current approval changed"):
            await accept_proposal(db_session, collision, accepted_by="test operator")
        await db_session.rollback()
        retained = await current_approval(db_session, workflow_id)
        assert retained is not None and retained.revision == reviewed.revision

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
        revision, _ = await workflow_dispatch._cloudflare_placement(db_session, pending, message)
        return revision

    async def prepare(revision, entry_sha256):
        return await prepare_approved_workspace(
            workflow_id=str(workflow_id), entry_path=path, function_name="run",
            entry_sha256=entry_sha256, expected_revision=revision,
        )

    original = b"from js import fetch\nasync def value(url): return await fetch('https://example.com/test')\n"
    changed = (b"from js import fetch\nasync def value(url):\n"
               b"    if url: return await fetch(url)\n"
               b"    return await fetch('https://new-tenant.halopsa.com/test')\n")

    try:
        entry_sha = await index.write(path, f"from {module} import value\nasync def run(url): return await value(url)\n".encode())
        old_sha = await index.write(module_path, original)
        await db_session.commit()
        discovered = await propose_approval(workflow_id, subrequest_allowance=5, cpu_budget_ms=100)
        proposal = await propose_approval(
            workflow_id, subrequest_allowance=5, cpu_budget_ms=100, https_hosts=["*.halopsa.com"],
        )
        assert proposal.revision != discovered.revision
        assert proposal.declaration.required_https_hosts == ["example.com"]
        await accept_proposal(db_session, proposal, accepted_by="test operator")
        await db_session.commit()
        assert await marker() == proposal.revision

        new_sha = await index.write(module_path, changed)
        await db_session.commit()
        marker_before, request = await workflow_dispatch._cloudflare_placement(db_session, pending, message)
        assert marker_before is None and request is not None
        assert (await db_session.execute(select(PlatformJob.id).where(*job_filter))).scalar_one_or_none() is None
        await db_session.commit()
        await workflow_dispatch.hand_off_workflow_run(
            workflow_dispatch.AcceptedRun(uuid4(), message, pending, request),
        )
        async with asyncio.timeout(120):
            while True:
                async with async_session_factory() as observer:
                    job = await observer.scalar(select(PlatformJob).where(*job_filter))
                    if job is not None and job.completed_at is not None:
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
        assert derived.declaration["https_hosts"] == ["*.halopsa.com", "example.com"]
        assert derived.declaration["unresolved_url_calls"] == 1
        assert derived.declaration["required_https_hosts"] == ["new-tenant.halopsa.com"]
        assert await marker() == derived.revision
        assert await prepare(proposal.revision, entry_sha) is None
        workspace = await prepare(derived.revision, entry_sha)
        assert workspace is not None
        verified_sources = workspace.bundle["sources"]
        assert isinstance(verified_sources, dict)
        assert verified_sources[module_path] == changed.decode()
        await RepoStorage().write(module_path, b"VALUE = 'changed after enqueue'\n")
        assert await prepare(derived.revision, entry_sha) is None
        await RepoStorage().write(module_path, changed)
        assert await revoke_approval(db_session, workflow_id) == derived.revision
        await db_session.commit()
        assert await marker() is None
        assert await prepare(derived.revision, entry_sha) is None
        revised = await propose_approval(workflow_id, subrequest_allowance=5, cpu_budget_ms=100)
        assert revised.previous_revision is None
        assert revised.inactive_history_count == 2
        await accept_proposal(db_session, revised, accepted_by="test operator")
        await db_session.commit()
        assert await marker() == revised.revision
    finally:
        await db_session.rollback()
        await db_session.execute(delete(PlatformJob).where(*job_filter))
        await db_session.execute(delete(Workflow).where(Workflow.id == workflow_id))
        await index.delete(path)
        await index.delete(module_path)
        await db_session.commit()
