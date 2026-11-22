"""Approval review distinguishes temporary admission from a real parse refusal."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from bifrost.cloudflare_analysis import AnalysisRejected
from fastapi import HTTPException
from src.models.contracts.cloudflare_approval import (
    CloudflareApprovalAccept,
    CloudflareApprovalOptions,
    CloudflareApprovalState,
)
from src.core.principal import UserPrincipal
from src.models.orm import Workflow
from src.services.authorization.enforce import Caller
from src.services.authorization.reach import OrgReach
from src.services.execution import cloudflare_approval
from src.services.execution.cloudflare_analysis_runner import AnalysisBusy


@pytest.mark.parametrize(
    ("error", "status", "rule"),
    [
        (AnalysisBusy("Busy"), "temporarily_unavailable", "CF_ANALYSIS_UNAVAILABLE"),
        (AnalysisRejected("Source exceeds parsing budget", "CF_PARSE_BUDGET"), "ineligible", "CF_PARSE_BUDGET"),
    ],
)
@pytest.mark.parametrize("accepting", [False, True])
async def test_review_distinguishes_admission_and_parse_budget(monkeypatch, error, status, rule, accepting):
    workflow = Workflow(path="main.py", name="Test", function_name="run")
    monkeypatch.setattr(cloudflare_approval, "_authorized_workflow", AsyncMock(return_value=(workflow, True)))
    monkeypatch.setattr(
        cloudflare_approval, "_state",
        AsyncMock(return_value=CloudflareApprovalState(status="pending_review", can_manage=True)),
    )
    monkeypatch.setattr(cloudflare_approval, "propose_approval", AsyncMock(side_effect=error))

    monkeypatch.setattr(cloudflare_approval, "_save_review", AsyncMock())
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



def test_global_operator_permission_does_not_extend_workflow_reach(monkeypatch):
    owner, outsider = uuid4(), uuid4()
    caller = Caller(UserPrincipal(uuid4(), "operator@example.com", owner, is_provider_org=True), None)
    monkeypatch.setattr(cloudflare_approval, "user_reach", lambda _: OrgReach(
        organization_ids=frozenset({owner}), include_global=True,
    ))
    monkeypatch.setattr(cloudflare_approval, "allows_permission", lambda *_: False)

    assert not cloudflare_approval._can_propose(caller)
    assert not cloudflare_approval._can_see(caller, Workflow(organization_id=owner), False)
    assert cloudflare_approval._can_see(caller, Workflow(organization_id=owner), True)
    assert cloudflare_approval._can_see(caller, Workflow(organization_id=None), True)
    assert not cloudflare_approval._can_see(caller, Workflow(organization_id=outsider), True)


async def test_observation_checks_pinned_modules_and_missing_paths(monkeypatch):
    workflow = Workflow()
    indexed = AsyncMock(return_value={"run.py": "a" * 64})
    monkeypatch.setattr(cloudflare_approval, "indexed_pins", indexed)
    observed = {"run.py": "a" * 64, "helper.py": None}
    assert await cloudflare_approval._observations_match(AsyncMock(), workflow, observed)
    indexed.return_value["helper.py"] = "b" * 64
    assert not await cloudflare_approval._observations_match(AsyncMock(), workflow, observed)
