"""Managed installation admission policy."""

from collections import Counter
from uuid import uuid4

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
