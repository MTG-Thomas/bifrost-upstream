"""Adapter contract for a qualified Cloudflare provisioning backend."""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from pydantic import SecretStr


@dataclass(frozen=True, slots=True)
class ResourceTarget:
    installation_id: UUID
    account_id: str
    scope: str
    kind: str
    name: str


@dataclass(frozen=True, slots=True)
class ResourceIntent:
    target: ResourceTarget
    creation_effect_id: UUID
    desired_revision: int
    artifact_digest: str


@dataclass(frozen=True, slots=True)
class ResourceObservation:
    intent: ResourceIntent
    provider_id: str
    provider_version: str


class ObservationPending(Exception):
    """The adapter cannot establish current ownership or settle an earlier effect."""


class OwnershipConflict(Exception):
    """The provider returned an incompatible owner, resource ID or revision."""


class ProvisioningAdapter(Protocol):
    """A single qualified path, bound to the authenticated account.

    Adapters check exact preconditions immediately before each mutation and
    reconcile on the same path. Worker upload has no conditional PUT, so an
    account-side writer can still race the create. The mandatory post-PUT
    readback detects a foreign result and requires operator action.
    """

    async def observe(
        self, *, api_token: SecretStr, intent: ResourceIntent
    ) -> ResourceObservation | None:
        """Read the exact target, rejecting foreign or unmarked occupants.

        None confirms absence and excludes late arrivals. Uncertain state raises
        ObservationPending. An unmarked occupant raises OwnershipConflict.
        """
        ...

    async def create(
        self, *, api_token: SecretStr, intent: ResourceIntent, scope_bearer: SecretStr | None
    ) -> None:
        """Check absence and bind the effect to the exact target before sending.

        Only the capacity Worker's script upload receives a scope bearer.
        """
        ...

    async def delete(self, *, api_token: SecretStr, expected: ResourceObservation) -> None:
        """Delete only if the target still matches the entire observation."""
        ...


async def observe_owned(
    adapter: ProvisioningAdapter,
    *,
    api_token: SecretStr,
    intent: ResourceIntent,
    recorded: ResourceObservation | None,
) -> ResourceObservation | None:
    """Adopt only the exact journaled creation effect or a recorded resource."""
    observed = await adapter.observe(api_token=api_token, intent=intent)
    if observed is not None and (
        observed.intent != intent
        or not observed.provider_id
        or not observed.provider_version
        or (recorded is not None and observed != recorded)
    ):
        raise OwnershipConflict("Cloudflare resource does not match its journal")
    return observed


async def resume_creation(
    adapter: ProvisioningAdapter,
    *,
    api_token: SecretStr,
    scope_bearer: SecretStr | None,
    committed_intent: ResourceIntent,
) -> ResourceObservation:
    """Resume a committed creation intent under the caller's mutation fence."""
    if scope_bearer is not None and committed_intent.target.kind != "worker_script":
        raise ValueError("A scope bearer can be installed only on a Worker script")
    observed = await observe_owned(adapter, api_token=api_token, intent=committed_intent, recorded=None)
    if observed is not None:
        return observed
    await adapter.create(api_token=api_token, intent=committed_intent, scope_bearer=scope_bearer)
    observed = await observe_owned(adapter, api_token=api_token, intent=committed_intent, recorded=None)
    if observed is None:
        raise ObservationPending("Cloudflare creation has no confirmed resource")
    return observed


async def resume_deletion(
    adapter: ProvisioningAdapter,
    *,
    api_token: SecretStr,
    committed_deletion: ResourceObservation,
) -> None:
    """Resume a committed deletion intent under the caller's mutation fence."""
    observed = await observe_owned(
        adapter, api_token=api_token, intent=committed_deletion.intent, recorded=committed_deletion,
    )
    if observed is None:
        return
    await adapter.delete(api_token=api_token, expected=observed)
    if await observe_owned(
        adapter, api_token=api_token, intent=committed_deletion.intent, recorded=committed_deletion,
    ) is not None:
        raise ObservationPending("Cloudflare resource deletion is unconfirmed")
