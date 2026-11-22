"""Approval review distinguishes temporary admission from a real parse refusal."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from bifrost.cloudflare_analysis import AnalysisRejected
from fastapi import HTTPException
from sqlalchemy import event
from src.core.constants import PROVIDER_ORG_ID
from src.core.principal import UserPrincipal
from src.models.contracts.cloudflare_approval import (
    CloudflareApprovalAccept,
    CloudflareApprovalOptions,
    CloudflareApprovalState,
)
from src.models.orm import CloudflareApproval, Workflow
from src.services.authorization.context import AuthorizationContext, Boundary, BoundaryKind, RoleGrant
from src.services.authorization.enforce import Caller
from src.services.execution import cloudflare_approval
from src.services.execution.cloudflare_analysis_runner import AnalysisBusy


@pytest.mark.parametrize(
    ("error", "status", "rule"),
    [
        (AnalysisBusy("Busy"), "temporarily_unavailable", "CF_ANALYSIS_UNAVAILABLE"),
        (AnalysisRejected("Source exceeds parsing budget", "CF_PARSE_BUDGET"), "ineligible", "CF_PARSE_BUDGET"),
        (AnalysisRejected("Estimated requests exceed allowance", "CF_SUBREQUESTS"), "pending_review", "CF_SUBREQUESTS"),
    ],
)
@pytest.mark.parametrize("accepting", [False, True])
async def test_review_distinguishes_admission_and_parse_budget(monkeypatch, error, status, rule, accepting):
    workflow = Workflow(path="main.py", name="Test", function_name="run")
    monkeypatch.setattr(cloudflare_approval, "_authorized_workflow", AsyncMock(return_value=(workflow, Caller(UserPrincipal(uuid4(), "operator@example.com", None), None), True)))
    monkeypatch.setattr(
        cloudflare_approval, "_state",
        AsyncMock(return_value=CloudflareApprovalState(status="pending_review", can_manage=True)),
    )
    monkeypatch.setattr(cloudflare_approval, "propose_approval", AsyncMock(side_effect=error))

    save = AsyncMock()
    monkeypatch.setattr(cloudflare_approval, "_save_review", save)
    options = CloudflareApprovalOptions()
    if accepting:
        request = CloudflareApprovalAccept(**options.model_dump(), review_sha256="a" * 64)
        with pytest.raises(HTTPException) as refusal:
            await cloudflare_approval.review_approval(AsyncMock(), AsyncMock(), uuid4(), request)
        assert refusal.value.status_code == (503 if status == "temporarily_unavailable" else 409)
        assert refusal.value.detail["message"]
        review = CloudflareApprovalState.model_validate(refusal.value.detail["state"])
    else:
        review = await cloudflare_approval.review_approval(AsyncMock(), AsyncMock(), uuid4(), options)

    assert review.status == status
    assert review.proposal is None
    assert review.diagnostics[0].rule_id == rule
    assert review.diagnostics[0].hint
    if accepting or status == "temporarily_unavailable":
        save.assert_not_awaited()
    else:
        save.assert_awaited_once()



def test_provider_membership_does_not_grant_global_permission_or_cross_org_reach():
    user_id, outsider = uuid4(), uuid4()
    principal = UserPrincipal(user_id, "operator@example.com", PROVIDER_ORG_ID, is_provider_org=True)
    ctx = AuthorizationContext(user_id, PROVIDER_ORG_ID, uuid4(), False, frozenset({"workflows.read"}))
    caller = Caller(principal, ctx)
    assert not cloudflare_approval._can_propose(caller)
    assert cloudflare_approval._can_see(caller, Workflow(organization_id=PROVIDER_ORG_ID), False)
    assert not cloudflare_approval._can_see(caller, Workflow(organization_id=None), False)
    assert not cloudflare_approval._can_see(caller, Workflow(organization_id=outsider), False)

    grant = RoleGrant(uuid4(), frozenset({"platform.readwrite"}), (Boundary(BoundaryKind.PLATFORM),))
    operator = Caller(principal, AuthorizationContext(
        user_id, PROVIDER_ORG_ID, uuid4(), False, frozenset(), (grant,),
    ))
    assert cloudflare_approval._can_propose(operator)
    assert cloudflare_approval._can_see(operator, Workflow(organization_id=None), True)
    assert not cloudflare_approval._can_see(operator, Workflow(organization_id=outsider), True)


async def test_observation_checks_pinned_modules_and_missing_paths(monkeypatch):
    workflow = Workflow()
    indexed = AsyncMock(return_value={"run.py": "a" * 64})
    monkeypatch.setattr(cloudflare_approval, "indexed_pins", indexed)
    observed = {"run.py": "a" * 64, "helper.py": None}
    assert await cloudflare_approval._observations_match(AsyncMock(), workflow, observed)
    indexed.return_value["helper.py"] = "b" * 64
    assert not await cloudflare_approval._observations_match(AsyncMock(), workflow, observed)



async def test_list_batches_workflow_reviews_and_pins(db_session):
    user_id, owner = uuid4(), PROVIDER_ORG_ID
    grant = RoleGrant(uuid4(), frozenset({"platform.readwrite"}), (Boundary(BoundaryKind.PLATFORM),))
    user = UserPrincipal(
        user_id, "operator@example.com", owner,
        authorization_context=AuthorizationContext(user_id, owner, uuid4(), False, frozenset(), (grant,)),
    )
    queries = []

    def count_query(state):
        queries.append(state.statement)

    async def add_workflows(total):
        for _ in range(total):
            workflow_id = uuid4()
            path = f"batch_{workflow_id.hex}.py"
            workflow = Workflow(
                id=workflow_id, name=path, path=path, function_name="run", organization_id=owner,
            )
            db_session.add(workflow)
            await db_session.flush()
            db_session.add(CloudflareApproval(
                revision=workflow.id.hex * 2, workflow_id=workflow.id, organization_id=owner,
                policy_version=cloudflare_approval.POLICY_VERSION, is_review=True, is_current=False,
                declaration={
                    "scope": [str(owner), None, path, "run"], "solution": None, "current_revision": None,
                    "observed_pins": {path: None}, "status": "ineligible", "diagnostics": [],
                },
            ))
        await db_session.flush()

    await add_workflows(1)
    event.listen(db_session.sync_session, "do_orm_execute", count_query)
    try:
        before = await cloudflare_approval.list_approvals(db_session, user)
        one_count = len(queries)
        await add_workflows(8)
        queries.clear()
        after = await cloudflare_approval.list_approvals(db_session, user)
        assert after.counts["ineligible"] == before.counts["ineligible"] + 8
        assert len(queries) == one_count == 3
    finally:
        event.remove(db_session.sync_session, "do_orm_execute", count_query)
