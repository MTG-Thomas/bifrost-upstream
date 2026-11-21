"""Managed installation admission policy."""

from collections import Counter
from uuid import uuid4

import pytest

from shared.models import CloudflareWorkersPolicy
from src.services.worker_pool.cloudflare_slots import WorkflowSlotAdmission


def test_last_slot_goes_to_a_tenant_without_occupied_slots() -> None:
    tenant, other = uuid4(), uuid4()
    policy = CloudflareWorkersPolicy(
        launcher_cap=3, slots_per_launcher=4, max_lifetime_seconds=3600, work_kinds=frozenset({"workflow"}),
    )
    admission = WorkflowSlotAdmission(uuid4(), policy, Counter({"platform": 4, str(tenant): 4}))
    assert (policy.slots_per_tenant, policy.installation_slots) == (4, 9)
    assert not admission.allows(None)
    assert not admission.allows(tenant)
    assert admission.allows(other)
    admission.occupied[str(other)] = 1
    assert admission.full
    assert not admission.allows(uuid4())


def test_reduced_installation_limit_preserves_headroom_below_the_tenant_cap() -> None:
    policy = CloudflareWorkersPolicy(
        launcher_cap=1, slots_per_launcher=4, max_lifetime_seconds=3600,
        work_kinds=frozenset({"workflow"}), installation_slots=3,
    )
    admission = WorkflowSlotAdmission(uuid4(), policy, Counter({"platform": 2}))
    assert not admission.allows(None)
    assert admission.allows(uuid4())


@pytest.mark.parametrize(
    ("cap", "linger", "occupied", "prepared", "launcher_free", "expected"),
    [
        (0, 90, {"platform": 1}, 0, 3, 0),
        (3, 0, {"platform": 1}, 0, 3, 0),
        (2, 90, {"platform": 1}, 0, 3, 2),
        (3, 90, {"platform": 3}, 0, 3, 1),
        (3, 90, {"platform": 1, "other": 7}, 0, 3, 0),
        (3, 90, {"platform": 1, "other": 6}, 0, 3, 1),
        (3, 90, {"platform": 1}, 0, 1, 1),
        (3, 90, {"platform": 1}, 3, 3, 0),
    ],
)
def test_queue_prewarming_preserves_admission_headroom(cap, linger, occupied, prepared, launcher_free, expected):
    policy = CloudflareWorkersPolicy(
        launcher_cap=3, slots_per_launcher=4, max_lifetime_seconds=3600, work_kinds=frozenset({"workflow"}),
        queue_prewarm_per_tenant=cap, linger_seconds=linger,
    )
    admission = WorkflowSlotAdmission(uuid4(), policy, Counter(occupied))
    assert admission.prewarm_count("platform", prepared=prepared, launcher_free=launcher_free) == expected
