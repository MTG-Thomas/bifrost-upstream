"""Small shared request and response models used outside a single API domain."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

SEMVER_PATTERN = (
    r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)"
    r"(?:-(?:(?:0|[1-9]\d*)|(?:\d*[A-Za-z-][0-9A-Za-z-]*))"
    r"(?:\.(?:(?:0|[1-9]\d*)|(?:\d*[A-Za-z-][0-9A-Za-z-]*)))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)

CONTAINERS_PILOT_INSTANCE_HOURS = 100


class CloudflareWorkersPolicy(BaseModel):
    """Launch limits for the Phase 1 Workers scope."""

    model_config = ConfigDict(extra="forbid")

    launcher_cap: int = Field(ge=1, le=1000)
    slots_per_launcher: int = Field(ge=1, le=4)
    slots_per_tenant: int = Field(default=4, ge=1, le=32)
    installation_slots: int = Field(default=9, ge=2, le=256)
    max_lifetime_seconds: int = Field(ge=60, le=604800)
    linger_seconds: int = Field(default=90, ge=0, le=300, description="Tenant engine reuse window after a workflow.")
    queue_prewarm_per_tenant: int = Field(
        default=0, ge=0, le=32,
        description="Maximum extra engines to prepare for queued workflows per tenant. Zero disables queue pre-warming.",
    )
    work_kinds: frozenset[Literal["workflow"]]


class CloudflareCapacityPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    workers: CloudflareWorkersPolicy
    monthly_limit_usd: Decimal = Field(
        gt=0, max_digits=12, decimal_places=2,
        description="Monthly admission target for Bifrost's estimated usage. The Workers Paid subscription is separate.",
    )
    shared_managed_worker_cap: int | None = Field(default=None, ge=1, le=1000)
    container_instance_hours: int | None = Field(default=None, ge=1, le=1488)


class CloudflareUsageAdvisory(BaseModel):
    checked_at: datetime
    period_start: datetime
    worker_requests: Decimal | None = None
    ledger_request_proxy: Decimal | None = None
    request_difference: Decimal | None = None


class CloudflareSpendStatus(BaseModel):
    state: Literal["ready", "budget_reached", "ledger_unavailable"]
    estimated_usd: Decimal | None = Field(description="Bifrost ledger estimate with unsettled usage reserves.")
    workers_paid_subscription_usd: Decimal = Field(description="Monthly Workers Paid subscription billed separately by Cloudflare")
    rate_table_version: str
    coverage: list[str] = Field(default_factory=list)
    period_start: datetime
    period_end: datetime
    analytics: CloudflareUsageAdvisory | None = None
    settled_usd: Decimal | None = Field(default=None, description="Estimated usage outside identifiable resource reservations.")
    reserved_usd: Decimal | None = None
    remaining_headroom_usd: Decimal | None = None


CloudflarePlacementCode = Literal[
    "setup_required", "workflow_review_required", "waiting_for_demand",
    "primary_has_capacity", "primary_capacity_unverified", "waiting_for_launch_gate",
    "tenant_limit_reached", "installation_slots_full", "spend_guard_closed",
    "cleanup_pending", "cloudflare_capacity_unavailable", "release_mismatch",
    "connector_unavailable", "eligible_for_overflow", "primary_limited_by_cloudflare_cleanup",
    "primary_limited_by_cloudflare_drain",
    "container_readiness_unverified", "container_egress_stale", "container_allocation_cooldown",
    "container_receipt_limit",
    "container_instance_budget_exhausted",
    "container_egress_refresh_failed",
]


class CloudflareRecoveryDetails(BaseModel):
    code: str
    remedy: str
    resource_class: str | None = None
    check: str | None = None
    observed_category: str | None = None
    expected_category: str | None = None


class CloudflarePlacementReason(BaseModel):
    code: CloudflarePlacementCode
    label: str
    remedy: str
    next_eligible_at: datetime | None = None


class CloudflareLaneStatus(BaseModel):
    lane: Literal["workers", "containers"]
    eligible_queued: int
    claimed: int
    running: int
    primary_fit: int | None = None
    primary_saturated: bool | None = None
    oldest_eligible_queue_age_seconds: float | None = None
    launch_gate_seconds: float
    reasons: list[CloudflarePlacementReason]


class CloudflareDeploymentEvidence(BaseModel):
    revision: int | None = None
    connector_session: Literal["unobserved", "waiting_for_projection", "connected", "disconnected"] | None = None
    connector_checked_at: datetime | None = None
    connector_last_ready_at: datetime | None = None
    dns_verified_at: datetime | None = None
    ingress_verified_at: datetime | None = None
    signed_canary_at: datetime | None = None


CloudflareReachabilityMode = Literal["existing_https_endpoint", "managed_tunnel"]
CLOUDFLARE_ID_PATTERN = r"^[a-f0-9]{32}$"


class CloudflareInstallationStatus(BaseModel):
    installation_id: UUID
    reachability_mode: CloudflareReachabilityMode = "existing_https_endpoint"
    connector_token_version: int | None = None
    consent_version: str
    consent_at: datetime
    desired_revision: int
    deployed_revision: int | None = None
    upgrade_pending: bool = False
    upgrade_available: bool = False
    desired_state: Literal["scaled_in", "enabled", "disconnected"]
    admission_open: bool
    policy: CloudflareCapacityPolicy
    fleet_envelope_usd: Decimal = Field(description="Estimated fleet usage for the configured lifetime and cleanup margin.")
    owned_resources_pending: int
    spend: CloudflareSpendStatus | None = None
    containers_remaining_instance_hours: Decimal | None = None
    containers_instance_budget_resets_at: datetime | None = None


class CloudflareCapacityStatus(BaseModel):
    consent_text_version: str
    consent_text: str
    installation: CloudflareInstallationStatus | None
    latest_job_id: UUID | None = None
    suggested_policy: CloudflareCapacityPolicy | None = None
    observed_at: datetime | None = None
    approved_workflow_count: int | None = None
    lanes: list[CloudflareLaneStatus] = Field(default_factory=list)
    placement_reasons: list[CloudflarePlacementReason] = Field(default_factory=list)
    deployment: CloudflareDeploymentEvidence | None = None
    recovery: CloudflareRecoveryDetails | None = None
    disconnected_at: datetime | None = None


class CloudflareCapacityAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    expected_revision: int = Field(gt=0)


class CloudflareReconcileAction(CloudflareCapacityAction):
    reset_release_fence: bool = Field(default=False, description="Explicitly accept a conflicting or older packaged Workers release.")


class CloudflareAccountTokenRotation(CloudflareCapacityAction):
    model_config = ConfigDict(hide_input_in_errors=True)

    api_token: SecretStr = Field(min_length=16, max_length=512, json_schema_extra={"writeOnly": True})


class CloudflareWorkerCredentialRotation(CloudflareCapacityAction):
    credential: Literal["control_root", "scope_bearer", "connector_token"]


class CloudflareAccountTokenReferenceRotation(CloudflareCapacityAction):
    credential_ref: UUID


class CloudflareStageCredentialRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    account_id: str | None = Field(default=None, pattern=CLOUDFLARE_ID_PATTERN)
    installation_id: UUID | None = None
    purpose: Literal["connect", "rotate"]
    api_token: SecretStr = Field(min_length=16, max_length=512, json_schema_extra={"writeOnly": True})

    @model_validator(mode="after")
    def validate_target(self) -> CloudflareStageCredentialRequest:
        if (self.account_id is None) == (self.installation_id is None):
            raise ValueError("Specify exactly one Cloudflare credential target")
        if self.purpose == "connect" and self.installation_id is not None:
            raise ValueError("Connecting Cloudflare requires the account ID")
        return self


class CloudflareStagedCredential(BaseModel):
    credential_ref: UUID
    expires_at: datetime


class CloudflarePolicyUpdate(CloudflareCapacityAction):
    action: Literal["policy"]
    policy: CloudflareCapacityPolicy


class CloudflareCredentialInput(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    api_token: SecretStr | None = Field(default=None, min_length=16, max_length=512, json_schema_extra={"writeOnly": True})
    credential_ref: UUID | None = None

    @model_validator(mode="after")
    def validate_credential(self) -> CloudflareCredentialInput:
        if (self.api_token is None) == (self.credential_ref is None):
            raise ValueError("Provide one Cloudflare credential")
        return self


class ContainersPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    instance_type: Literal["standard-1"] = "standard-1"
    installation_instances: int = Field(default=2, ge=1, le=2)
    tenant_instances: Literal[1] = 1
    workflow_slots: Literal[1] = 1
    idle_seconds: int = Field(default=120, ge=1, le=120)
    monthly_instance_hours: int = Field(default=CONTAINERS_PILOT_INSTANCE_HOURS, ge=1, le=1488)
    start_timeout_seconds: int = Field(ge=1, le=3600)
    lifetime_seconds: int = Field(ge=121, le=86400)
    queue_wait_seconds: int = Field(ge=1, le=3600)
    queue_depth: int = Field(ge=1, le=1000)
    ingress_bytes: int = Field(ge=1, le=10**9)
    outbound_bytes: int = Field(ge=1, le=10**9)


class CloudflarePreflightRequest(CloudflareCredentialInput):
    account_id: str | None = Field(default=None, pattern=CLOUDFLARE_ID_PATTERN)
    zone_id: str | None = Field(default=None, pattern=CLOUDFLARE_ID_PATTERN)
    reachability_mode: CloudflareReachabilityMode | None = None
    worker_ingress_origin: str | None = Field(default=None, max_length=255)
    policy: CloudflareCapacityPolicy | None = None
    containers: bool | None = None
    container_instance_hours: int | None = Field(default=None, ge=1, le=1488)


class CloudflareDiscoveryChoice(BaseModel):
    id: str = Field(pattern=CLOUDFLARE_ID_PATTERN)
    name: str = Field(min_length=1, max_length=255)


class CloudflareActivationPreview(BaseModel):
    policy: CloudflareCapacityPolicy
    fleet_envelope_usd: Decimal
    estimated_usage_min_usd: Decimal | None = None
    estimated_usage_max_usd: Decimal | None = None
    workers_paid_subscription_usd: Decimal
    containers_policy: ContainersPolicy | None = None
    containers_fleet_envelope_usd: Decimal | None = None
    containers_remaining_instance_hours: Decimal | None = None


class CloudflarePreflight(BaseModel):
    accounts: list[CloudflareDiscoveryChoice] | None
    zones: list[CloudflareDiscoveryChoice] | None = None
    account_id: str | None = None
    zone_id: str | None = None
    zone_name: str | None = None
    workers_paid: Literal["verified", "unverified", "denied"] | None = None
    connector_channel: Literal["compose", "kustomize"] | None
    connector_ready: bool
    primary: Literal["static", "job", "external", "unknown"]
    recommended_reachability: CloudflareReachabilityMode
    reachability_mode: CloudflareReachabilityMode
    worker_ingress_origin: str | None = None
    hostname_collision: bool = False
    preview: CloudflareActivationPreview | None = None
    consent_version: str
    consent_text: str
    preflight_receipt: str | None = None
    expires_at: datetime | None = None


class CloudflareActivateRequest(CloudflareCredentialInput):
    preflight_receipt: str = Field(min_length=1, max_length=8192)
    consent_version: str = Field(max_length=64)
    consent: Literal[True]
    monthly_limit_usd: Decimal = Field(gt=0, max_digits=12, decimal_places=2)


class GitAppManifestRequest(BaseModel):
    purpose: Literal["workspace", "solution"] = "workspace"
    organization_id: UUID | None = None
    owner: str = Field(min_length=1, max_length=39, pattern=r"^[A-Za-z0-9][A-Za-z0-9-]*$")
    owner_type: Literal["organization", "user"] = "organization"
    name: str = Field(min_length=1, max_length=100)
    public: bool = False
    workflows: bool = False


class GitAppExistingRequest(BaseModel):
    purpose: Literal["workspace", "solution"] = "workspace"
    organization_id: UUID | None = None
    app_id: int = Field(gt=0)
    client_id: str = Field(min_length=1, max_length=100)
    private_key: SecretStr
    client_secret: SecretStr


class GitAppSetupRequest(BaseModel):
    setup_id: UUID


class GitAppCallbackRequest(BaseModel):
    code: str = Field(min_length=1, max_length=1024, repr=False)
    state: str = Field(min_length=1, max_length=4096, repr=False)


class GitAppInstallationRequest(GitAppSetupRequest):
    installation_id: int = Field(gt=0)


class GitAppRepositoryRequest(GitAppInstallationRequest):
    repository_id: int = Field(gt=0)


class GitAppInstallation(BaseModel):
    id: int
    account: str


class GitAppRepository(BaseModel):
    id: int
    full_name: str
    private: bool
    default_branch: str


class GitAppSetupResponse(BaseModel):
    purpose: Literal["workspace", "solution"] = "workspace"
    source_organization_id: UUID | None = None
    source_id: UUID | None = None
    setup_id: UUID
    stage: Literal["registration", "registration_needs_review", "authorization", "installation", "repository", "ready", "connected"]
    expires_at: datetime
    app_name: str | None = None
    install_url: str | None = None
    installations: list[GitAppInstallation]
    repositories: list[GitAppRepository]


class GitAppManifestResponse(GitAppSetupResponse):
    registration_url: str
    manifest: dict[str, Any]


class GitAppAuthorizeResponse(BaseModel):
    authorization_url: str


class GitAppReuseRequest(BaseModel):
    source_id: UUID | None = None
    organization_id: UUID | None = None


class SolutionGitSource(BaseModel):
    id: UUID
    app_name: str
    installation_id: int
    organization_id: UUID | None
    enabled: bool
    repositories: list[GitAppRepository]


class SolutionGitSources(BaseModel):
    sources: list[SolutionGitSource]


class AppConnectionDetails(BaseModel):
    """A user-bound preview of one external app connection."""

    client_name: str
    redirect_uri: str
    consent_token: str


class AppConnectionDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approved: bool
    consent_token: str = Field(min_length=64, max_length=64)


class AppConnectionResult(BaseModel):
    redirect_url: str | None = None


class MCPClientRegistration(BaseModel):
    client_name: str | None = Field(default=None, max_length=255)
    redirect_uris: list[str] = Field(min_length=1, max_length=20)


class UsageReport(BaseModel):
    """Pseudonymous aggregate activity for one completed UTC day."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    installation_id: UUID
    version: str = Field(min_length=5, max_length=128, pattern=SEMVER_PATTERN)
    period: date
    workflow_runs: int = Field(ge=0, le=1_000_000_000)
    agent_runs: int = Field(ge=0, le=1_000_000_000)
    environment: Literal["production"] = "production"

    @field_validator("installation_id")
    @classmethod
    def require_v4_installation_id(cls, value: UUID) -> UUID:
        if value.version != 4:
            raise ValueError("installation_id must be a UUIDv4")
        return value


class BrowserImpersonationTokenResponse(BaseModel):
    """A short-lived bearer credential for one browser impersonation tab."""
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: Literal[1800] = 1800


class SolutionDevContextRequest(BaseModel):
    """The selected portable app identity for a local Solution preview."""

    app_id: UUID


class SolutionDevContextResponse(BaseModel):
    """Short-lived, app-bound context accepted only as a supplemental header."""

    token: str
    expires_at: float
    solution_id: UUID
    organization_id: UUID | None
    app_id: UUID


class RuntimeLogRequest(BaseModel):
    """A line for the authenticated runtime's own log stream."""

    model_config = ConfigDict(extra="forbid")
    level: str = Field(min_length=1, max_length=32)
    message: str = Field(max_length=65536)
    metadata: dict[str, Any] | None = None
    timestamp: datetime | None = None


class RuntimeLogResponse(BaseModel):
    entry_id: str | None


class ServiceControlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ready: bool = False
    token_expires_at: AwareDatetime


class ServiceControlResponse(BaseModel):
    stop_requested: bool
    access_token: str | None = None
    token_expires_at: datetime | None = None


class ProviderCacheReadRequest(BaseModel):
    """Parameters for the authenticated runtime's own data-provider cache read."""

    model_config = ConfigDict(extra="forbid")
    parameters: dict[str, Any] = Field(default_factory=dict)


class ProviderCacheReadResponse(BaseModel):
    hit: bool
    result: Any | None = None
    expires_at: datetime | None = None


class ProviderCacheWriteRequest(BaseModel):
    """Result for the authenticated runtime's own data-provider cache entry."""

    model_config = ConfigDict(extra="forbid")
    parameters: dict[str, Any] = Field(default_factory=dict)
    result: Any


class ProviderCacheWriteResponse(BaseModel):
    expires_at: datetime | None = None


class DeviceAuthorizeRequest(BaseModel):
    """The code for a CLI device login being approved or denied."""

    user_code: str


class AppSdkUpgradeSettings(BaseModel):
    """Platform-wide policy for automatic App SDK upgrades."""

    enabled: bool = False


class AppSdkUpgradeSettingsUpdate(BaseModel):
    """Requested platform-wide App SDK upgrade policy."""

    enabled: bool


class ApplicationSdkUpdatePayloadV1(BaseModel):
    """Legacy manual App SDK update payload retained for already queued jobs."""

    model_config = ConfigDict(extra="forbid")

    application_id: UUID
    deployment_id: UUID = Field(default_factory=uuid4)
    expected_active_deployment_id: UUID | None = None
    expected_sdk_package_version: str | None = None
    expected_sdk_fingerprint: str | None = None
    expected_sdk_contract_version: int | None = None
    expected_sdk_built_at: datetime | None = None


class HostedAppSdkDescriptor(BaseModel):
    """The current immutable browser SDK factory for one binding API major."""

    api_major: int = Field(ge=1)
    factory_version: int = Field(ge=1)
    factory_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    integrity: str = Field(pattern=r"^sha256-[A-Za-z0-9+/]{43}=$")
    asset_url: str = Field(pattern=r"^/api/app-sdk/assets/[a-f0-9]{64}\.js$")


class ApplicationSdkUpdatePayloadV2(ApplicationSdkUpdatePayloadV1):
    """Pinned-target jobs queued before hosted binding provenance existed."""

    target_sdk_fingerprint: str = Field(min_length=1)
    target_sdk_contract_version: int = Field(ge=1)
    automatic_upgrade: bool = False


class ApplicationSdkUpdatePayload(ApplicationSdkUpdatePayloadV2):
    """Version 3 fences the binding API major as well as the compiled bytes."""

    expected_sdk_binding_api_major: int | None = Field(default=None, ge=1)
    target_sdk_binding_api_major: int | None = Field(default=None, ge=1)


class VersionResponse(BaseModel):
    version: str
    min_cli_version: str
    contract_version: int
    sdk_fingerprint: str
    sdk_contract_version: int
    sdk_binding_api_major: int | None = None
    sdk_supported_binding_api_majors: list[int] = Field(default_factory=list)


class ApplicationBundleManifest(BaseModel):
    """Built App assets and the host contracts needed before entry evaluation."""

    entry: str | None
    css: str | None
    base_url: str
    mode: Literal["preview", "live"]
    dependencies: dict[str, str]
    migrated: bool = False
    organization_id: UUID | None = None
    app_model: Literal["inline_v1", "standalone_v2"] = "inline_v1"
    runtime_contract: Literal["mount-v1"] | None = None
    sdk_binding_api_major: int | None = Field(default=None, ge=1)


class WorkerCapacityProfile(BaseModel):
    """Capabilities of an authenticated external physical-capacity provider."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    protocol_version: Literal[1, 2]
    scope: str = Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9._:-]+$")
    worker_version: str = Field(min_length=1, max_length=100)
    memory_limit_bytes: int = Field(ge=67108864, le=137438953472, multiple_of=1048576, strict=True)
    max_workers: int = Field(ge=1, le=1000, strict=True)
    max_lifetime_seconds: int = Field(ge=60, le=604800, strict=True)
    runtime_profile: Literal["linux-process-v1", "cloudflare-python-v1"] = "linux-process-v1"

    @model_validator(mode="after")
    def _managed_prepare_is_workers_only(self) -> WorkerCapacityProfile:
        expected = 2 if self.runtime_profile == "cloudflare-python-v1" else 1
        if self.protocol_version != expected:
            raise ValueError("Capacity protocol does not match the provider runtime")
        return self


class WorkerCapacityReservation(BaseModel):
    """Immutable preparation of one canonical worker incarnation."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    worker_id: UUID
    worker_version: str = Field(min_length=1, max_length=100)
    memory_limit_bytes: int = Field(ge=67108864, le=137438953472, multiple_of=1048576, strict=True)
    max_lifetime_seconds: int = Field(ge=60, le=604800, strict=True)
    drain_grace_seconds: int = Field(ge=0, le=3600, strict=True)


class CloudflareLauncherCounts(BaseModel):
    """Cumulative workload counts for one launcher."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    sqlite_rows_written: int = Field(strict=True, ge=0, le=2**53 - 1)
    do_requests: int = Field(strict=True, ge=0, le=2**53 - 1)
    log_events: int = Field(strict=True, ge=0, le=2**53 - 1)


class CloudflareEngineInvocation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity: str = Field(min_length=1, max_length=128)
    kind: Literal["boot", "prime", "workflow"]
    cold: bool = Field(strict=True)
    started_at: AwareDatetime
    wall_ms: int | None = Field(default=None, strict=True, ge=0, le=2**53 - 1, exclude_if=lambda value: value is None)


CLOUDFLARE_LEGACY_RECEIPT_ROUTES = 128
CLOUDFLARE_MAX_RECEIPT_ROUTES = 512


class CloudflareLauncherUsage(CloudflareLauncherCounts):
    """Terminal or deletion receipt including workload admission state."""

    sqlite_database_bytes: int | None = Field(default=None, ge=0, le=2**53 - 1, exclude_if=lambda value: value is None)
    launcher_storage_cleared: Literal[True] | None = Field(default=None, exclude_if=lambda value: value is None)
    ledger_receipt_pending: Literal[True] | None = Field(default=None, exclude_if=lambda value: value is None)
    routes: dict[
        Annotated[str, Field(min_length=1, max_length=128)],
        Annotated[list[CloudflareEngineInvocation], Field(max_length=4)],
    ] | None = Field(
        default=None, max_length=2 * CLOUDFLARE_MAX_RECEIPT_ROUTES, exclude_if=lambda value: value is None,
    )
    exhausted: bool = Field(default=False, strict=True)


class WorkerCapacityResource(BaseModel):
    """Provider state; canonical enrollment and work status remain in Postgres."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    scope: str = Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9._:-]+$")
    worker_id: UUID
    worker_version: str = Field(min_length=1, max_length=100)
    resource_uid: str = Field(min_length=1, max_length=255, pattern=r"^[a-zA-Z0-9._:-]+$")
    state: Literal["prepared", "starting", "running", "stopped", "deleted"]
    out_of_memory: bool = Field(default=False, strict=True)
    usage: CloudflareLauncherUsage | None = Field(default=None, exclude_if=lambda value: value is None)


class SolutionAppBuildCandidate(BaseModel):
    """Manifest app identity and a digest computed before local compilation."""

    app_id: UUID
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class SolutionAppBuildLookup(BaseModel):
    apps: list[SolutionAppBuildCandidate]


class SolutionAppBuildMatches(BaseModel):
    matches: dict[str, UUID]


class MCPPackageBrandingUpdate(BaseModel):
    """Instance-wide identity for downloaded agent plugin packages."""

    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=60)

    @field_validator("name")
    @classmethod
    def validate_package_name(cls, value: str) -> str:
        import re

        value = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 _-]*", value):
            raise ValueError("Use letters, numbers, spaces, hyphens, or underscores; start with a letter or number")
        return value


class MCPPackageBranding(BaseModel):
    """Saved package display name, installation identifier, and optional logo."""

    name: str
    slug: str
    logo_url: str | None = None


ConversionStatus = Literal["ready", "needs_review", "blocked", "already_declared", "stale"]


class WorkflowConversionAnalyzeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # UUID-only selection remains bounded while supporting large filtered
    # workflow inventories; analysis itself still pages the durable snapshot.
    workflow_ids: list[UUID] | None = Field(default=None, max_length=10_000)
    scope: str | None = None
    platform_cutover: bool = False

    @field_validator("workflow_ids")
    @classmethod
    def _nonempty_explicit_selection(cls, value: list[UUID] | None) -> list[UUID] | None:
        if value == []:
            raise ValueError("workflow_ids must be omitted or contain at least one workflow")
        if value is not None and len(value) != len(set(value)):
            raise ValueError("workflow_ids must not contain duplicates")
        return value

    @field_validator("scope")
    @classmethod
    def _scope_shape(cls, value: str | None) -> str | None:
        if value in (None, "all", "global"):
            return value
        try:
            UUID(value)
        except ValueError as exc:
            raise ValueError("scope must be all, global, or an organization UUID") from exc
        return value

    @model_validator(mode="after")
    def _platform_cutover_uses_complete_inventory(self) -> "WorkflowConversionAnalyzeRequest":
        if self.platform_cutover and (self.workflow_ids is not None or self.scope not in (None, "all")):
            raise ValueError("platform_cutover requires the whole platform inventory")
        return self


class WorkflowConversionSelection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    workflow_id: UUID
    source_fingerprint: str = Field(min_length=64, max_length=64)
    acknowledged_unknowns: bool = False


class WorkflowConversionApplyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    selections: list[WorkflowConversionSelection] = Field(min_length=1, max_length=10_000)
    acknowledged_solution_requests: bool = False

    @field_validator("selections")
    @classmethod
    def _unique_workflows(cls, value: list[WorkflowConversionSelection]) -> list[WorkflowConversionSelection]:
        if len({selection.workflow_id for selection in value}) != len(value):
            raise ValueError("selections must not contain duplicate workflows")
        return value


class WorkflowConversionCutoverRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    acknowledged_remaining_exceptions: bool = False


class WorkflowConversionEvidence(BaseModel):
    path: str
    line: int | None = None
    call: str
    via: list[str] = Field(default_factory=list)


class WorkflowConversionIdentitySummary(BaseModel):
    id: UUID | None = None
    name: str | None = None
    identity_kind: str | None = None
    organization_id: UUID | None = None
    shared: bool = False


class WorkflowConversionSolutionRequestSummary(BaseModel):
    status: str
    request_digest: str
    approved_digest: str | None = None
    requested_mode: str
    requested_permissions: list[str] = Field(default_factory=list)


class WorkflowConversionReportItem(BaseModel):
    workflow_id: UUID
    workflow_name: str
    organization_id: UUID | None = None
    solution_id: UUID | None = None
    source_fingerprint: str
    status: ConversionStatus
    current_permission_mode: Literal["full", "restricted"]
    proposed_permissions: list[str] = Field(default_factory=list)
    source_permissions: list[str] = Field(default_factory=list)
    observed_permissions: list[str] = Field(default_factory=list)
    observed_operations: list[str] = Field(default_factory=list)
    evidence: list[WorkflowConversionEvidence] = Field(default_factory=list)
    unknowns: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    identity: WorkflowConversionIdentitySummary | None = None
    solution_permission_request: WorkflowConversionSolutionRequestSummary | None = None
    error: str | None = None
    review: dict[str, Any] | None = None


class WorkflowConversionReportSummary(BaseModel):
    platform_cutover: bool = False
    analyzer_version: str
    total: int
    ready: int = 0
    needs_review: int = 0
    blocked: int = 0
    already_declared: int = 0
    stale: int = 0
    observation_window_days: int = 30
    default_permission_mode: Literal["full", "restricted"]
    cutover_ready: bool = False
    remaining_full: int = 0


class WorkflowConversionReport(BaseModel):
    job_id: UUID
    total: int
    offset: int
    limit: int
    items: list[WorkflowConversionReportItem]
    summary: WorkflowConversionReportSummary


class WorkflowConversionAnalyzePayload(WorkflowConversionAnalyzeRequest):
    pass


class WorkflowConversionApplyPayload(WorkflowConversionApplyRequest):
    analysis_job_id: UUID


class WorkflowConversionCutoverPayload(WorkflowConversionCutoverRequest):
    analysis_job_id: UUID
