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
from src.models.orm import Workflow
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

    options = CloudflareApprovalOptions(subrequest_allowance=10, cpu_budget_ms=1000)
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
