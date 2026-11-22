"""Durable approval review and exact-snapshot verification with PostgreSQL and S3."""

import asyncio
from uuid import uuid4

import httpx
import pytest
from bifrost.cloudflare_analysis import AnalysisRejected
from sqlalchemy import delete, select
from src.config import get_settings
from src.core.redis_client import pending_execution_record
from src.jobs.platform.cloudflare_reapproval import CLOUDFLARE_REAPPROVAL_DEFINITION
from src.models.orm import (
    CloudflareApproval,
    Organization,
    PlatformJob,
    SolutionFileIndex,
    Workflow,
)
from src.models.orm.audit import AuditLog
from src.services.authorization.tokens import (
    HumanSubject,
    RuntimeSubject,
    issue_access_token,
    issue_run_refresh_token,
)
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
from src.services.solutions.storage import SolutionStorage

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
async def test_workflow_approval_rest_enforces_global_operator_and_audits_revision(async_engine, e2e_api_url):
    async with committed_world(async_engine) as world:
        org = await world.organization()
        admin = await world.person(org)
        operator_role = await world.role("platform.readwrite")
        await world.assign(admin, operator_role.id, ("platform", None))
        reader = await world.person(org)
        role = await world.role("workflows.read")
        await world.assign(reader, role.id, ("organization", org.id))
        outsider = await world.person(await world.organization())
        provider_org = await world.organization(provider=True)
        provider = await world.person(provider_org)
        provider_workflow = await world.workflow(provider_org)
        await world.assign(admin, role.id, ("organization", provider_org.id))
        workflow = await world.workflow(org)
        foreign = await world.workflow(await world.organization())
        global_workflow = await world.workflow(org)
        global_workflow.organization_id = None
        fast_path = await world.workflow(org, execution_lane="fast_path")
        foreign_id, global_id, fast_path_id, provider_id = map(
            str, (foreign.id, global_workflow.id, fast_path.id, provider_workflow.id),
        )
        global_uuid = global_workflow.id
        global_path = global_workflow.path
        global_source = f"def {global_workflow.function_name}(): return 42\n"
        workflow.path = f"workflows/wildcard_{workflow.id.hex}.py"
        workflow.function_name = "run"
        path = workflow.path
        index = FileIndexService(world.session)
        workflow_id = workflow.id
        endpoint = f"/api/workflows/{workflow_id}/cloudflare-approval"
        options = {"subrequest_allowance": 5, "cpu_budget_ms": 100}
        wildcard = {**options, "https_hosts": ["*.halopsa.com"]}

        async def headers(user):
            access = await issue_access_token(HumanSubject(user.id))
            return {"Authorization": f"Bearer {access.token}"}

        async def write_source(route):
            await index.write(path, (
                "from js import fetch\nasync def run(tenant):\n"
                f"    return await fetch(f'https://{{tenant}}.halopsa.com/{route}')\n"
            ).encode())
            await world.session.commit()

        try:
            await index.write(global_path, global_source.encode())
            await write_source("original")
            async with httpx.AsyncClient(base_url=e2e_api_url, timeout=30) as client:
                admin_headers, reader_headers = await headers(admin), await headers(reader)
                provider_headers = await headers(provider)
                list_url = "/api/workflows/cloudflare-approvals"
                listing = await client.get(list_url, headers=admin_headers)
                listing.raise_for_status()
                listed = listing.json()
                ids = [item["workflow_id"] for item in listed["workflows"]]
                assert ids.index(provider_id) < ids.index(global_id) < ids.index(str(workflow_id))
                assert foreign_id not in ids
                assert next(item for item in listed["workflows"] if item["workflow_id"] == fast_path_id)["status"] == "ineligible"
                assert sum(listed["counts"].values()) == len(ids)
                scoped = (await client.get(list_url, headers=reader_headers)).json()
                assert {item["workflow_id"] for item in scoped["workflows"]} == {str(workflow_id), fast_path_id}
                assert not scoped["can_propose"]
                assert (await client.post(
                    f"/api/workflows/{foreign_id}/cloudflare-approval/propose", json={}, headers=admin_headers,
                )).status_code == 404
                assert (await client.get(endpoint, headers=await headers(outsider))).status_code == 404
                read = await client.get(endpoint, headers=reader_headers)
                assert read.status_code == 200 and not read.json()["can_manage"]
                assert (await client.get(endpoint, headers=provider_headers)).status_code == 404
                for actor in (reader_headers, provider_headers):
                    assert (await client.post(f"{endpoint}/propose", json=options, headers=actor)).status_code == 403
                assert (await client.post(f"{endpoint}/propose", json=wildcard, headers=provider_headers)).status_code == 403
                global_endpoint = f"/api/workflows/{global_id}/cloudflare-approval"
                global_review = await client.post(f"{global_endpoint}/propose", json=options, headers=admin_headers)
                global_review.raise_for_status()
                global_sha = global_review.json()["proposal"]["review_sha256"]
                assert global_review.json()["proposal_sources"][global_path] == global_source
                global_accept = {**options, "review_sha256": global_sha}
                assert (await client.post(
                    f"{global_endpoint}/accept", json=global_accept, headers=provider_headers,
                )).status_code == 403
                accepted_global = await client.post(
                    f"{global_endpoint}/accept", json=global_accept, headers=admin_headers,
                )
                accepted_global.raise_for_status()
                assert accepted_global.json()["status"] == "approved"
                review = await client.post(f"{endpoint}/propose", json={}, headers=admin_headers)
                review.raise_for_status()
                persisted = (await client.get(endpoint, headers=admin_headers)).json()
                assert persisted["proposal"] == review.json()["proposal"]
                assert "halopsa.com/original" in persisted["proposal_sources"][path]
                assert (await client.get(endpoint, headers=reader_headers)).json()["proposal_sources"] is None
                repeated = (await client.post(f"{endpoint}/propose", json={}, headers=admin_headers)).json()
                assert repeated["proposal"]["review_sha256"] == persisted["proposal"]["review_sha256"]
                assert await current_approval(world.session, workflow_id) is None
                assert review.json()["proposal"]["https_hosts"] == []
                assert review.json()["diagnostics"][0]["rule_id"] == "CF_URL_REVIEW"
                private_root = await client.post(
                    f"{endpoint}/propose", json={**options, "https_hosts": ["*.github.io"]}, headers=admin_headers,
                )
                assert private_root.json()["status"] == "ineligible"

                async def proposal():
                    response = await client.post(f"{endpoint}/propose", json=wildcard, headers=admin_headers)
                    response.raise_for_status()
                    result = response.json()["proposal"]
                    assert result["wildcard_https_hosts"] == ["*.halopsa.com"]
                    return {**wildcard, "review_sha256": result["review_sha256"]}

                stale = await proposal()
                await write_source("changed")
                changed_state = (await client.get(endpoint, headers=admin_headers)).json()
                assert changed_state["status"] == "revision_changed"
                assert changed_state["proposal"] is None
                latest = await proposal()

                async def accept(body, actor):
                    return await client.post(f"{endpoint}/accept", json=body, headers=actor)

                assert (await accept(latest, reader_headers)).status_code == 403
                assert (await accept(latest, provider_headers)).status_code == 403
                refused = await accept(stale, admin_headers)
                assert refused.status_code == 409
                assert refused.json()["detail"]["state"]["status"] == "revision_changed"
                results = await asyncio.gather(accept(latest, admin_headers), accept(latest, admin_headers))
                assert sorted(response.status_code for response in results) == [200, 409]
                state = (await client.get(endpoint, headers=reader_headers)).json()
                assert state["status"] == "approved"
                assert state["current_revision"] == latest["review_sha256"]
                await write_source("changed-again")
                changed = await client.get(endpoint, headers=reader_headers)
                assert changed.json()["status"] == "revision_changed"
                assert (await client.delete(endpoint, headers=reader_headers)).status_code == 403
                assert (await client.delete(endpoint, headers=provider_headers)).status_code == 403
                before_revoke = await proposal()
                revoked = await client.delete(endpoint, headers=admin_headers)
                assert revoked.json()["status"] == "pending_review"
                assert (await accept(before_revoke, admin_headers)).status_code == 409
                events = (await world.session.scalars(select(AuditLog).where(
                    AuditLog.resource_id == workflow.id,
                    AuditLog.action.in_(("workflow.cloudflare_approval.accept", "workflow.cloudflare_approval.revoke")),
                ))).all()
                assert {(event.action, event.user_id, event.details["revision"]) for event in events} == {
                    ("workflow.cloudflare_approval.accept", admin.id, latest["review_sha256"]),
                    ("workflow.cloudflare_approval.revoke", admin.id, latest["review_sha256"]),
                }
        finally:
            await world.session.rollback()
            await index.delete(path)
            await index.delete(global_path)
            await world.session.execute(delete(AuditLog).where(AuditLog.resource_id.in_((workflow_id, global_uuid))))
            await world.session.commit()


@pytest.mark.e2e
async def test_accept_rejects_a_republished_solution_generation(async_engine, e2e_api_url):
    async with committed_world(async_engine) as world:
        org = await world.organization()
        admin = await world.person(org)
        await world.platform_admin(admin)
        solution = await world.solution(org)
        solution.runtime_source_generation = uuid4()
        solution.allow_outbound_access = False
        workflow = await world.workflow(org, solution_id=solution.id)
        path = workflow.path
        solution_id = solution.id
        storage = SolutionStorage(solution_id)
        pin = await storage.write(path, f"def {workflow.function_name}(): return 1\n".encode())
        endpoint = f"/api/workflows/{workflow.id}/cloudflare-approval"
        options = {"subrequest_allowance": 5, "cpu_budget_ms": 100}
        try:
            world.session.add(SolutionFileIndex(solution_id=solution.id, path=path, content_hash=pin))
            await world.session.commit()
            access = await issue_access_token(HumanSubject(admin.id))
            headers = {"Authorization": f"Bearer {access.token}"}
            async with httpx.AsyncClient(base_url=e2e_api_url, timeout=30, headers=headers) as client:
                review = await client.post(f"{endpoint}/propose", json=options)
                review.raise_for_status()
                proposal = review.json()["proposal"]
                assert proposal["solution"]["source_generation"] == str(solution.runtime_source_generation)

                solution.runtime_source_generation = uuid4()
                await world.session.commit()
                refusal = await client.post(f"{endpoint}/accept", json={
                    **options, "review_sha256": proposal["review_sha256"],
                })
                assert refusal.status_code == 409
                assert refusal.json()["detail"]["state"]["status"] == "revision_changed"
                assert await current_approval(world.session, workflow.id) is None
        finally:
            await world.session.rollback()
            await storage.delete(path)
            await world.session.execute(delete(SolutionFileIndex).where(SolutionFileIndex.solution_id == solution_id))
            await world.session.commit()


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
