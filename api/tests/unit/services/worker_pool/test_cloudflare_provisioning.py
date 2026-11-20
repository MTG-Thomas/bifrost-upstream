from dataclasses import replace
from uuid import uuid4

import pytest
from pydantic import SecretStr
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    OwnershipConflict,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
    resume_creation,
    resume_deletion,
)


class FakeProvisioningAdapter:
    def __init__(self) -> None:
        self.resource: ResourceObservation | None = None
        self.calls: list[str] = []
        self.lose_create_response = False
        self.confirm_delete = True
        self.observation_pending = False
        self.unmarked = False
        self.replace_on_create: ResourceObservation | None = None

    async def observe(self, *, api_token: SecretStr, intent: ResourceIntent) -> ResourceObservation | None:
        self.calls.append("observe")
        if self.observation_pending:
            raise ObservationPending
        if self.unmarked:
            raise OwnershipConflict("Unmarked Cloudflare occupant requires operator action")
        return self.resource

    async def create(
        self, *, api_token: SecretStr, intent: ResourceIntent, scope_bearer: SecretStr | None,
    ) -> None:
        self.calls.append("create")
        assert self.resource is None
        self.resource = self.replace_on_create or ResourceObservation(intent, "provider-id", "provider-version")
        if self.lose_create_response:
            raise ObservationPending

    async def delete(self, *, api_token: SecretStr, expected: ResourceObservation) -> None:
        self.calls.append("delete")
        assert self.resource == expected
        if self.confirm_delete:
            self.resource = None


@pytest.fixture
def intent() -> ResourceIntent:
    target = ResourceTarget(uuid4(), "a" * 32, "workers", "worker_script", "bifrost-test-installation")
    return ResourceIntent(target, uuid4(), 1, "b" * 64)


@pytest.mark.asyncio
async def test_creation_retry_adopts_exact_effect_after_lost_response(intent: ResourceIntent) -> None:
    adapter = FakeProvisioningAdapter()
    adapter.lose_create_response = True
    token, bearer = SecretStr("fake-api-token"), SecretStr("fake-scope-bearer")

    with pytest.raises(ObservationPending):
        await resume_creation(adapter, api_token=token, scope_bearer=bearer, committed_intent=intent)

    observed = await resume_creation(adapter, api_token=token, scope_bearer=bearer, committed_intent=intent)

    assert observed == ResourceObservation(intent, "provider-id", "provider-version")
    assert adapter.calls == ["observe", "create", "observe"]


@pytest.mark.asyncio
async def test_unknown_observation_cannot_authorize_creation(intent: ResourceIntent) -> None:
    adapter = FakeProvisioningAdapter()
    adapter.observation_pending = True

    with pytest.raises(ObservationPending):
        await resume_creation(
            adapter, api_token=SecretStr("token"), scope_bearer=SecretStr("bearer"), committed_intent=intent,
        )

    assert adapter.calls == ["observe"]


@pytest.mark.asyncio
async def test_unmarked_occupant_prevents_create(intent: ResourceIntent) -> None:
    adapter = FakeProvisioningAdapter()
    adapter.unmarked = True
    with pytest.raises(OwnershipConflict, match="requires operator action"):
        await resume_creation(adapter, api_token=SecretStr("token"), scope_bearer=None, committed_intent=intent)
    assert adapter.calls == ["observe"]


@pytest.mark.asyncio
async def test_foreign_result_after_create_requires_operator_action(intent: ResourceIntent) -> None:
    adapter = FakeProvisioningAdapter()
    adapter.replace_on_create = ResourceObservation(replace(intent, creation_effect_id=uuid4()), "id", "version")
    with pytest.raises(OwnershipConflict):
        await resume_creation(adapter, api_token=SecretStr("token"), scope_bearer=None, committed_intent=intent)
    assert adapter.calls == ["observe", "create", "observe"]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["account", "installation", "scope", "effect", "revision", "digest", "id", "version"])
async def test_foreign_or_changed_resource_cannot_be_deleted(intent: ResourceIntent, changed: str) -> None:
    adapter = FakeProvisioningAdapter()
    record = ResourceObservation(intent, "provider-id", "provider-version")
    changes = {
        "account": replace(record, intent=replace(intent, target=replace(intent.target, account_id="c" * 32))),
        "installation": replace(record, intent=replace(intent, target=replace(intent.target, installation_id=uuid4()))),
        "scope": replace(record, intent=replace(intent, target=replace(intent.target, scope="other"))),
        "effect": replace(record, intent=replace(intent, creation_effect_id=uuid4())),
        "revision": replace(record, intent=replace(intent, desired_revision=2)),
        "digest": replace(record, intent=replace(intent, artifact_digest="d" * 64)),
        "id": replace(record, provider_id="other-id"),
        "version": replace(record, provider_version="other-version"),
    }
    adapter.resource = changes[changed]

    with pytest.raises(OwnershipConflict):
        await resume_deletion(adapter, api_token=SecretStr("token"), committed_deletion=record)

    assert adapter.calls == ["observe"]


@pytest.mark.asyncio
async def test_deletion_requires_independent_absence_and_replays_without_mutation(intent: ResourceIntent) -> None:
    adapter = FakeProvisioningAdapter()
    record = ResourceObservation(intent, "provider-id", "provider-version")
    adapter.resource = record
    adapter.confirm_delete = False

    with pytest.raises(ObservationPending):
        await resume_deletion(adapter, api_token=SecretStr("token"), committed_deletion=record)

    assert adapter.calls == ["observe", "delete", "observe"]
    adapter.resource = None  # The previously sent delete completed after our read.
    await resume_deletion(adapter, api_token=SecretStr("token"), committed_deletion=record)
    assert adapter.calls == ["observe", "delete", "observe", "observe"]
