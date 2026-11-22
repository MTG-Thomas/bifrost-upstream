from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.platform_jobs import PlatformJob
from src.services.worker_pool.cloudflare_status import (
    PlacementObservation,
    lane_reasons,
    placement_blockers,
    primary_coverage,
    recovery_for_job,
)
from src.services.worker_pool.controller import Request

NOW = datetime(2026, 11, 22, tzinfo=UTC)


@pytest.mark.parametrize(("fields", "code"), [
    ({"enabled": False}, "setup_required"),
    ({"release_matches": False}, "release_mismatch"),
    ({"cleanup_pending": True}, "cleanup_pending"),
    ({"spend_ready": False}, "spend_guard_closed"),
    ({"connector_session": "disconnected"}, "connector_unavailable"),
    ({"job_code": "CF_PROVIDER_UNAVAILABLE", "cooldown_until": NOW}, "cloudflare_capacity_unavailable"),
])
def test_installation_blocker_has_remedy(fields, code):
    snapshot = {
        "enabled": True, "admitted": True, "release_matches": True, "cleanup_pending": False,
        "spend_ready": True, "connector_session": None, "job_code": None, "cooldown_until": None,
    }
    result, = placement_blockers(**(snapshot | fields))
    assert result.code == code
    assert result.remedy
    assert result.next_eligible_at == fields.get("cooldown_until")


@pytest.mark.parametrize(("fields", "approvals", "code", "next_at"), [
    ({"queued": 0}, 0, "workflow_review_required", None),
    ({"queued": 0}, 2, "waiting_for_demand", None),
    ({"primary_fit": 1}, 2, "primary_has_capacity", None),
    ({"primary_fit": None, "primary_saturated": None}, 2, "primary_capacity_unverified", None),
    ({"oldest": NOW - timedelta(seconds=1), "aged_demand": 0}, 2, "waiting_for_launch_gate", NOW + timedelta(seconds=1)),
    ({"tenant_blocked": True}, 2, "tenant_limit_reached", None),
    ({"installation_full": True}, 2, "installation_slots_full", None),
    ({"provider_unavailable": True, "cooldown_until": NOW}, 2, "cloudflare_capacity_unavailable", NOW),
    ({}, 2, "eligible_for_overflow", None),
])
def test_lane_placement_reason_has_remedy(fields, approvals, code, next_at):
    defaults = {"queued": 1, "primary_fit": 0, "primary_saturated": True, "aged_demand": 1}
    observation = PlacementObservation(**(defaults | fields))
    result, = lane_reasons(observation, now=NOW, gate_seconds=2, approvals=approvals, blockers=[])
    assert result.code == code
    assert result.remedy
    assert result.next_eligible_at == next_at


def test_recovery_does_not_return_provider_values():
    job = PlatformJob(status="requires_action", result={
        "code": "CF_OWNERSHIP_CONFLICT",
        "details": {"resource_class": "example.test", "check": ["unexpected"], "expected_category": "matching"},
    })
    recovery = recovery_for_job(job)
    assert recovery is not None
    assert recovery.resource_class is None
    assert recovery.remedy


@pytest.mark.parametrize("status", ["queued", "running"])
def test_recovery_hides_previous_attempt_during_retry(status):
    assert recovery_for_job(PlatformJob(status=status, error_code="CF_BUDGET_REACHED")) is None


def test_five_queued_workflows_fit_only_one_primary_slot():
    worker = ExecutionWorker(
        form="static", state="ready", execution_lane="normal", heartbeat_at=NOW,
        memory_budget_bytes=256 * 2**20, memory_reserved_bytes=0, supported_handlers=["workflow.execute"],
    )
    queue = [Request(uuid4(), 256 * 2**20, "workflow.execute", 1) for _ in range(5)]
    covered = primary_coverage([worker], queue, NOW)
    assert covered == {queue[0].id}
    reasons = lane_reasons(
        PlacementObservation(queued=5, primary_fit=len(covered), primary_saturated=True, aged_demand=4),
        now=NOW, gate_seconds=2, approvals=1, blockers=[],
    )
    assert [item.code for item in reasons] == ["primary_has_capacity", "eligible_for_overflow"]


def test_primary_coverage_respects_held_memory_and_handler_slots():
    from src.worker.remote.protocol import HANDLER_CONCURRENCY_LIMITS

    limited, limit = next(iter(HANDLER_CONCURRENCY_LIMITS.items()))
    held = Request(uuid4(), 30, limited)
    blocked = Request(uuid4(), 30, limited)
    fits = Request(uuid4(), 60, "workflow.execute")
    full = Request(uuid4(), 20, "workflow.execute")
    worker = ExecutionWorker(
        form="static", state="ready", execution_lane="normal", heartbeat_at=NOW, memory_budget_bytes=100,
        memory_reserved_bytes=0, supported_handlers=["workflow.execute", limited],
        active_handler_counts={limited: limit - 1}, reserved_for_dispatch_id=held.id,
    )
    assert primary_coverage([worker], [blocked, fits, full, held], NOW) == {fits.id, held.id}
    reasons = lane_reasons(
        PlacementObservation(
            queued=4, primary_fit=2, primary_saturated=True, oldest=NOW - timedelta(seconds=1),
        ),
        now=NOW, gate_seconds=2, approvals=1, blockers=[],
    )
    assert [item.code for item in reasons] == ["primary_has_capacity", "waiting_for_launch_gate"]
