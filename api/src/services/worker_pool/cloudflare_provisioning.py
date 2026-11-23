"""Adapter contract for a qualified Cloudflare provisioning backend."""

from dataclasses import dataclass
from enum import Enum, StrEnum
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


class OwnershipResource(StrEnum):
    UNKNOWN = "unknown"
    WORKER_SCRIPT = "worker_script"
    DURABLE_OBJECT_NAMESPACE = "durable_object_namespace"
    WORKERS_RELEASE = "workers_release"
    WORKERS_SCOPE = "workers_scope"
    UPGRADE_REVISION = "upgrade_revision"
    CONTAINER_APPLICATION = "container_application"
    TUNNEL = "tunnel"
    TUNNEL_INGRESS = "tunnel_ingress"
    CONNECTOR_CREDENTIAL = "connector_credential"
    DNS_ZONE = "dns_zone"
    DNS_RECORD = "dns_record"
    PRIVATE_ROUTE = "private_route"
    ACCOUNT = "account"
    WORKERS_ENDPOINT = "workers_endpoint"
    CONNECTOR = "connector"
    CANARY_LAUNCHER = "canary_launcher"


class ObservationCheck(Enum):
    UNCLASSIFIED = ("unclassified", "unknown", "unknown")
    REST_REJECTED = ("rest_request", "rejected", "successful_response")
    REST_JSON = ("response_envelope", "invalid_json", "json_object")
    REST_RESULT = ("response_envelope", "unconfirmed_result", "successful_result")
    SCRIPT_BINDINGS = ("script_bindings", "missing_or_invalid", "binding_inventory")
    SCRIPT_DEPLOYMENT_PENDING = ("active_deployment", "empty", "single_full_version")
    SCRIPT_UPLOAD_RESULT = ("upload_acknowledgement", "missing_or_different_identity", "matching_script_identity")
    SCRIPT_UPLOAD_PENDING = ("upload_readback", "absent", "exact_uploaded_script")
    SCRIPT_RECORDED_ABSENT = ("recorded_script", "absent", "exact_recorded_script")
    SCRIPT_CHANGED_DURING_READBACK = ("script_readback", "absent", "exact_observed_script")
    NAMESPACE_PAGE = ("namespace_inventory", "malformed_page", "valid_page")
    NAMESPACE_IDENTITY = ("namespace_inventory", "invalid_identity", "valid_identity")
    NAMESPACE_UNASSIGNED = ("namespace_assignment", "unassigned_without_receipt", "script_or_exact_receipt")
    NAMESPACE_PAGINATION = ("namespace_inventory", "inconsistent_pages", "complete_snapshot")
    NAMESPACE_LIMIT = ("namespace_inventory", "incomplete", "complete_snapshot")
    NAMESPACE_EXPORTS_PENDING = ("namespace_exports", "missing_expected_classes", "exact_export_set")
    NAMESPACE_EXPORTS_UNEXPECTED = ("namespace_exports", "unexpected_classes", "exact_export_set")
    ENDPOINT_UPDATE = ("endpoint_update", "unconfirmed_settings", "requested_settings")
    ACCOUNT_SUBDOMAIN = ("account_subdomain", "missing_or_invalid", "valid_subdomain")
    SCOPE_ENDPOINT = ("deployment_endpoint", "missing", "confirmed_endpoint")
    SCOPE_ABSENCE = ("scope_retirement", "missing_absence_receipt", "verified_absence")
    TUNNEL_INVENTORY = ("exact_inventory", "malformed", "valid_inventory")
    TUNNEL_ID = ("tunnel_identity", "missing_or_invalid", "valid_identity")
    TUNNEL_VERSION = ("tunnel_version", "missing_or_invalid", "valid_creation_timestamp")
    TUNNEL_CREATE = ("tunnel_create_readback", "absent", "exact_creation_receipt")
    TUNNEL_ABSENT = ("recorded_tunnel", "absent", "exact_recorded_tunnel")
    DNS_ID = ("dns_identity", "missing_or_invalid", "valid_identity")
    DNS_VERSION = ("dns_version", "missing_or_invalid", "valid_revision")
    DNS_CREATE = ("dns_create_readback", "absent", "exact_creation_receipt")
    DNS_ABSENT = ("recorded_dns", "absent", "exact_recorded_dns")
    CONNECTOR_INVENTORY = ("connector_inventory", "incomplete", "complete_inventory")
    CONNECTOR_IDENTITY = ("connector_inventory", "malformed_identity", "valid_identity")
    CONNECTOR_TOKEN = ("connector_token", "invalid", "valid_token")
    PRIVATE_ROUTES = ("private_route_inventory", "unconfirmed", "confirmed_inventory")
    CANARY_RETIREMENT = ("canary_retirement", "unverified", "exact_deletion_receipt")
    CONTAINER_INVENTORY = ("container_inventory", "incomplete", "complete_inventory")
    CONTAINER_PLACEMENTS = ("container_placements", "present", "empty_inventory")
    CONTAINER_CREATE = ("container_create_readback", "absent", "exact_creation_receipt")


def verification_details(resource_class: str, detail: tuple[str, str, str]) -> dict[str, str]:
    check, observed, expected = detail
    return {
        "resource_class": resource_class,
        "check": check,
        "observed_category": observed,
        "expected_category": expected,
    }


class ObservationPending(Exception):
    """The adapter cannot establish current ownership or settle an earlier effect."""

    def __init__(
        self, message: str = "", resource: OwnershipResource = OwnershipResource.UNKNOWN,
        check: ObservationCheck = ObservationCheck.UNCLASSIFIED,
    ) -> None:
        super().__init__(message)
        self.resource = resource
        self.check = check

    def as_result(self) -> dict[str, str]:
        return verification_details(self.resource.value, self.check.value)


class ObservationSettling(ObservationPending):
    """A bounded retry may re-read a provider observation under existing fences."""


class DeletionSettling(ObservationPending):
    """An owned resource or its namespace is still visible during deletion."""


class OwnershipCheck(Enum):
    # Wire values are fixed categories. Provider values and exception text are never serialized.
    UNCLASSIFIED = ("unclassified", "unknown", "unknown")
    TARGET_CONTRACT = ("target_contract", "unsupported", "supported")
    TARGET_BINDING = ("target_binding", "different", "matching")
    FOREIGN_OCCUPANT = ("foreign_occupant", "owner_missing_or_different", "exact_owner")
    TARGET_OCCUPIED = ("target_occupied", "present", "absent")
    ACTIVE_VERSION = ("active_version", "missing_or_not_single_full_version", "single_full_version")
    RECEIPT_MISMATCH = ("receipt_mismatch", "different", "exact_receipt")
    RECEIPT_MISSING = ("receipt_missing", "missing", "exact_receipt")
    REAPPEARED = ("reappeared", "present", "absent")
    RELEASE_INTENT = ("release_intent", "incompatible", "matching_release")
    JOURNAL_STATE = ("journal_state", "incompatible", "creating_or_receipted")
    JOURNAL_INTENT = ("journal_intent", "missing", "present")
    SCOPE_BEARER = ("scope_bearer", "unexpected_presence_or_absence", "capacity_only")
    SLOT_POLICY = ("slot_policy", "above_release_limit", "supported")
    SECRET_BINDING = ("secret_binding", "invalid", "supported")
    DYNAMIC_BINDING = ("dynamic_binding", "invalid", "supported")
    CROSS_SCRIPT_BINDING = ("cross_script_binding", "invalid", "supported")
    DUPLICATE_BINDING = ("duplicate_binding", "multiple", "unique")
    NAMESPACE_CLASS = ("namespace_class", "missing_or_invalid", "valid_class")
    NAMESPACE_BINDINGS = ("namespace_bindings", "multiple", "one_per_class")
    UPGRADE_FENCE = ("upgrade_fence", "not_owned", "owned_and_paused")
    ZONE_IDENTITY = ("zone_identity", "missing_or_different", "matching")
    ZONE_ACCOUNT = ("zone_account", "missing_or_different", "matching")
    EXACT_INVENTORY = ("exact_inventory", "ambiguous", "single_page")
    MULTIPLE_OCCUPANTS = ("multiple_occupants", "multiple", "at_most_one")
    CONNECTOR_SECRET = ("connector_secret", "missing_or_different", "matching")
    ADOPTION_CREDENTIAL = ("adoption_credential", "missing_or_different", "committed")
    TUNNEL_METADATA = ("tunnel_metadata", "incompatible", "active_and_account_owned")
    INGRESS_OCCUPIED = ("ingress_occupied", "already_configured", "empty_or_matching")
    INGRESS_IDENTITY = ("ingress_identity", "different_or_missing", "approved_ingress")
    INGRESS_CONFIG_OWNER = ("ingress_config_owner", "missing_or_different", "exact_owner")
    PRIVATE_ROUTE = ("private_route", "present", "absent")
    DNS_TARGET_MOVED = ("dns_target_moved", "different", "matching")


class OwnershipConflict(Exception):
    """The provider returned an incompatible owner, resource ID or revision."""

    def __init__(
        self, message: str, resource: OwnershipResource = OwnershipResource.UNKNOWN,
        check: OwnershipCheck = OwnershipCheck.UNCLASSIFIED,
    ) -> None:
        super().__init__(message)
        self.resource = resource
        self.check = check

    def as_result(self) -> dict[str, object]:
        return dict(verification_details(self.resource.value, self.check.value))


class OwnedResourceCleanup(Protocol):
    async def observe(
        self, *, api_token: SecretStr, intent: ResourceIntent,
    ) -> ResourceObservation | None:
        """Return None only on verified absence. Reject foreign occupants."""
        ...

    async def delete(self, *, api_token: SecretStr, expected: ResourceObservation) -> None:
        """Delete only an exact observation and report whether the write succeeded."""
        ...

    async def verify_absent(
        self, *, api_token: SecretStr, target: ResourceTarget, namespace_ids: frozenset[str] = frozenset(),
    ) -> None:
        """Check late effects and any resources implicitly owned by this target."""
        ...


class ProvisioningAdapter(OwnedResourceCleanup, Protocol):
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

async def observe_owned(
    adapter: OwnedResourceCleanup,
    *,
    api_token: SecretStr,
    intent: ResourceIntent,
    recorded: ResourceObservation | None,
) -> ResourceObservation | None:
    """Adopt only the exact journaled creation effect or a recorded resource."""
    observed = await adapter.observe(api_token=api_token, intent=intent)
    if observed is None:
        return None
    if observed.intent != intent:
        check = OwnershipCheck.TARGET_BINDING
    elif not observed.provider_id or not observed.provider_version:
        check = OwnershipCheck.RECEIPT_MISSING
    elif recorded is not None and observed != recorded:
        check = OwnershipCheck.RECEIPT_MISMATCH
    else:
        return observed
    resource = next((item for item in OwnershipResource if item.value == intent.target.kind), OwnershipResource.UNKNOWN)
    raise OwnershipConflict("Cloudflare resource does not match its journal", resource, check)


async def request_creation(
    adapter: ProvisioningAdapter,
    *,
    api_token: SecretStr,
    scope_bearer: SecretStr | None,
    committed_intent: ResourceIntent,
) -> None:
    """Request creation once, or witness the exact earlier effect under its fence."""
    if scope_bearer is not None and committed_intent.target.kind != "worker_script":
        raise ValueError("A scope bearer can be installed only on a Worker script")
    observed = await observe_owned(adapter, api_token=api_token, intent=committed_intent, recorded=None)
    if observed is None:
        await adapter.create(api_token=api_token, intent=committed_intent, scope_bearer=scope_bearer)


async def resume_deletion(
    adapter: OwnedResourceCleanup,
    *,
    api_token: SecretStr,
    committed_deletion: ResourceObservation,
    namespace_ids: frozenset[str] = frozenset(),
) -> None:
    """Resume a committed deletion intent under the caller's mutation fence."""
    observed = await observe_owned(
        adapter, api_token=api_token, intent=committed_deletion.intent, recorded=committed_deletion,
    )
    if observed is not None:
        await adapter.delete(api_token=api_token, expected=observed)
        if await observe_owned(
            adapter, api_token=api_token, intent=committed_deletion.intent, recorded=committed_deletion,
        ) is not None:
            raise DeletionSettling("Cloudflare Worker is still present after deletion")
    await adapter.verify_absent(api_token=api_token, target=committed_deletion.intent.target, namespace_ids=namespace_ids)
