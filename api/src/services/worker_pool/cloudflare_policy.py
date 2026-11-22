"""Desired-policy writes and their admission revision fence."""

import json
import re
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from functools import cache
from hashlib import sha256
from typing import Literal
from uuid import UUID, uuid4

from pydantic import SecretStr
from shared.models import (
    CloudflareCapacityPolicy,
    CloudflareCapacityStatus,
    CloudflareInstallationStatus,
    CloudflareReachabilityMode,
    CloudflareSpendStatus,
    CloudflareUsageAdvisory,
    CloudflareWorkersPolicy,
)
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.platform_jobs import PlatformJob
from src.services.authorization.enforce import Caller
from src.services.platform_job_access import platform_job_visibility_filter
from src.services.work_dispatch import (
    WAKE_CLOUDFLARE_CAPACITY,
    wake_pool_workers_after_commit,
)
from src.services.worker_pool.cloudflare_containers_policy import (
    CONTAINERS_CLEANUP_PENDING,
    overdue_container_reservations,
    unresolved_container_reservation,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    seal_credential,
)
from src.services.worker_pool.cloudflare_release import (
    WorkersRelease,
    WorkersReleaseOrder,
    WorkersReleaseUnavailable,
    installed_workers_release_digest,
    installed_workers_release_order,
    load_workers_release,
    validate_workers_cleanup_margin,
)
from src.services.worker_pool.cloudflare_spend import (
    LAUNCHER_RECEIPT_METERS,
    WORKERS_PAID_SUBSCRIPTION_USD,
    WORKERS_RATE_TABLE_VERSION,
    cloudflare_estimated_usage,
    cloudflare_unsettled_resource_usage,
    exhausted_managed_workers,
    launcher_lifetime_rate,
    launcher_root_rate,
    spend_coverage,
    usage_period,
    valid_cpu_limits,
    workers_fleet_envelope_usd,
)
from src.services.worker_pool.cloudflare_status import (
    deployment_evidence,
    overflow_status,
    placement_blockers,
    reason,
    recovery_for_job,
)
from src.services.worker_pool.cloudflare_tunnel_rest import dedicated_hostname

CONSENT_TEXT_VERSION = "cloudflare-reachability-v5"
CONSENT_TEXT = (
    "Cloudflare will process approved workflow code, workload data, and run-scoped credentials. "
    "With Managed Tunnel, Cloudflare terminates TLS and Bifrost manages the dedicated hostname, "
    "tunnel, and DNS record. In Compose, the connector can reach the API's main port over their dedicated bridge. "
    "It can reach host-published ports through the host gateway, including the Bifrost client port. "
    "Depending on host routing and firewall rules, "
    "it can also reach the LAN, the instance metadata IP, and the internet. "
    "With Existing HTTPS Endpoint, your proxy controls ingress. The Workers Scripts Write "
    "grant can change other Workers in the selected account. You authorize Bifrost to provision "
    "and remove its verified owned resources in that account."
)


@dataclass(frozen=True, slots=True)
class WorkersReleaseMeterBounds:
    """Release-bound measurements and admission backstops."""

    cleanup_margin_seconds: int
    per_launcher_second: Mapping[str, Decimal]
    per_fleet_fixed: Mapping[str, Decimal]
    release: str
    max_slots_per_launcher: int
    launcher_caps: Mapping[str, int]
    cpu_limits_ms: Mapping[str, int]
    startup_expected_us: int
    release_order: WorkersReleaseOrder
    expected_cpu_ms: Mapping[str, int] | None = None
    bounded_linger: bool = False
    lifetime_meters: bool = False


def release_meter_bounds(release: WorkersRelease) -> WorkersReleaseMeterBounds:
    bounds = release.manifest.meter_bounds
    expected = bounds.qualification.expected_cpu_ms
    # One cold dispatch and one managed capacity request per launcher-second
    # give the fleet projection a measured CPU scenario. Keep the artifact's
    # request, duration, storage and cleanup allowances.
    startup_us = bounds.qualification.cpu_reference.samples.cold_exercise.p99_us
    cpu_per_second = expected["workflow"] + expected["boot"] + expected["capacity"] + Decimal(startup_us) / 1000
    return WorkersReleaseMeterBounds(
        bounds.cleanup_margin_seconds,
        {**bounds.per_launcher_second, "worker_cpu_ms": cpu_per_second},
        {**bounds.per_fleet_fixed, "worker_cpu_ms": bounds.per_fleet_fixed["worker_requests"] * expected["capacity"]},
        release.digest, release.manifest.max_slots_per_launcher,
        bounds.qualification.launcher_caps, bounds.qualification.cpu_limits_ms, startup_us,
        release_order=WorkersReleaseOrder(release.manifest.release_version, release.manifest.release_sequence),
        expected_cpu_ms=expected,
        bounded_linger=any(
            binding.name == "BIFROST_TENANT_LINGER_SECONDS" and binding.source == "linger_seconds"
            for binding in release.manifest.workers["launcher"].bindings
        ),
        lifetime_meters=any(
            binding.name == "BIFROST_LAUNCHER_METERS_PER_SECOND" and binding.type == "plain_text"
            for binding in release.manifest.workers["launcher"].bindings
        ),
    )


def workers_release_floor(row: CloudflareInstallation) -> WorkersReleaseOrder | None:
    if row.workers_release_version is None or row.workers_release_sequence is None:
        return None
    return WorkersReleaseOrder(row.workers_release_version, row.workers_release_sequence)


def set_workers_release_floor(row: CloudflareInstallation, release: str, order: WorkersReleaseOrder) -> None:
    row.workers_release_version = order.version
    row.workers_release_sequence = order.sequence
    row.workers_release_digest = release


def workers_release_error(
    row: CloudflareInstallation, release: str, order: WorkersReleaseOrder,
) -> Literal["CF_RELEASE_DOWNGRADE", "CF_RELEASE_CONFLICT"] | None:
    floor = workers_release_floor(row)
    if floor is not None:
        if order.precedence == floor.precedence and release != row.workers_release_digest:
            return "CF_RELEASE_CONFLICT"
        if order.key < floor.key:
            return "CF_RELEASE_DOWNGRADE"
    return None


def workers_upgrade_available(
    row: CloudflareInstallation, release: str | None, order: WorkersReleaseOrder | None,
) -> bool:
    if row.desired_state != "enabled" or release is None or order is None or workers_release_error(row, release, order):
        return False
    return row.upgrade_release is not None or row.drain_required or release != row.policy.get("bounds_release")


class CloudflarePolicyConflict(ValueError):
    """The installation changed since the caller read its desired policy."""


class CloudflareBudgetTooLow(ValueError):
    """The full supported fleet lifetime exceeds the policy's monthly limit."""

    def __init__(self, envelope: Decimal) -> None:
        self.envelope = envelope
        super().__init__(f"The estimated fleet usage is USD {envelope}")


class CloudflareInstallationExists(ValueError):
    """A current installation must finish disconnecting before another is created."""


def suggested_cloudflare_policy() -> CloudflareCapacityPolicy | None:
    try:
        return _qualified_workers_suggestion()
    except (WorkersReleaseUnavailable, CloudflareBudgetTooLow):
        return None


@cache
def _qualified_workers_suggestion() -> CloudflareCapacityPolicy:
    bounds = release_meter_bounds(load_workers_release())
    if not bounds.lifetime_meters:
        raise WorkersReleaseUnavailable("Workers activation suggestion requires lifetime meters")
    proposal = CloudflareCapacityPolicy(
        workers=CloudflareWorkersPolicy(
            launcher_cap=3, slots_per_launcher=4, installation_slots=9, max_lifetime_seconds=3600,
            work_kinds=frozenset({"workflow"}),
        ),
        monthly_limit_usd=Decimal("75.00"),
    )
    _price_policy(proposal, bounds)
    return proposal


async def get_cloudflare_capacity_status(db: AsyncSession, caller: Caller) -> CloudflareCapacityStatus:
    from src.services.execution.cloudflare_approval import current_visible_approval_count

    approvals = await current_visible_approval_count(db, caller)
    row = (await db.execute(
        select(
            CloudflareInstallation,
            cloudflare_estimated_usage().label("estimated_usage"),
            cloudflare_unsettled_resource_usage().label("reserved_usage"),
            func.statement_timestamp().label("as_of"),
            select(func.count()).select_from(CloudflareOwnedResource).where(
                CloudflareOwnedResource.installation_id == CloudflareInstallation.id,
                or_(CloudflareOwnedResource.state != "absent", CloudflareOwnedResource.observed_at.is_(None)),
            ).correlate(CloudflareInstallation).scalar_subquery().label("owned_resources_pending"),
            overdue_container_reservations(CloudflareInstallation.account_id, func.statement_timestamp())
            .with_only_columns(func.count()).correlate(CloudflareInstallation)
            .scalar_subquery().label("overdue_containers"),
        ).where(CloudflareInstallation.retired_at.is_(None)).execution_options(populate_existing=True)
    )).tuples().one_or_none()
    if row is None:
        retired_at, observed_at = (await db.execute(select(
            select(func.max(CloudflareInstallation.retired_at)).scalar_subquery(),
            func.statement_timestamp(),
        ))).one()
        return CloudflareCapacityStatus(
            consent_text_version=CONSENT_TEXT_VERSION, consent_text=CONSENT_TEXT, installation=None,
            managed_tunnel_available=get_settings().cloudflare_tunnel_channel is not None,
            suggested_policy=suggested_cloudflare_policy(),
            observed_at=observed_at,
            approved_workflow_count=approvals,
            placement_reasons=[reason("setup_required")],
            disconnected_at=retired_at,
        )
    installation, estimate, reserved, as_of, owned_resources_pending, overdue_containers = row
    policy = CloudflareCapacityPolicy.model_validate(installation.policy["limits"])
    period_start, period_end = usage_period(as_of)
    rate = installation.policy.get("rate_table_version")
    advisory = (
        CloudflareUsageAdvisory.model_validate(installation.usage_advisory)
        if installation.usage_advisory else None
    )
    coverage = (
        await spend_coverage(db, installation.account_id, period_start)
        if estimate is not None and isinstance(rate, str) else []
    )
    exhausted = await exhausted_managed_workers(db, installation.id)
    if exhausted:
        coverage.append(
            f"Managed cleanup expired for {exhausted} Cloudflare worker(s). Unverified capacity remains reserved. "
            "Run Scale In to verify retirement. Use Disconnect to remove the owned deployment if retirement cannot be verified."
        )
    if overdue_containers:
        coverage.append(
            f"Unresolved cleanup for {overdue_containers} Cloudflare Container reservation(s) past their deadline. "
            f"[{CONTAINERS_CLEANUP_PENDING}] Container admission is paused for this account until stop is verified."
        )
    outstanding = min(max(Decimal(0), reserved), estimate) if estimate is not None and reserved is not None else None
    spend = CloudflareSpendStatus(
        state="ledger_unavailable" if estimate is None else (
            "budget_reached" if estimate >= policy.monthly_limit_usd else "ready"
        ),
        estimated_usd=estimate.quantize(Decimal("0.01")) if estimate is not None else None,
        workers_paid_subscription_usd=WORKERS_PAID_SUBSCRIPTION_USD,
        rate_table_version=rate if estimate is not None and isinstance(rate, str) else "unavailable",
        coverage=coverage,
        period_start=period_start, period_end=period_end,
        analytics=advisory if advisory is not None and advisory.period_start == period_start else None,
        settled_usd=estimate - outstanding if estimate is not None and outstanding is not None else None,
        reserved_usd=outstanding,
        remaining_headroom_usd=max(Decimal(0), policy.monthly_limit_usd - estimate) if estimate is not None else None,
    )
    latest_job = await db.scalar(select(PlatformJob).where(
        PlatformJob.resource_type == "cloudflare_installation",
        PlatformJob.resource_id == str(installation.id),
        platform_job_visibility_filter(caller),
        or_(
            PlatformJob.requested_by_user_id != "system",
            PlatformJob.status.in_(("failed", "requires_action")),
            (PlatformJob.job_type == "capacity.cloudflare_reconcile")
            & (PlatformJob.payload["operation"].as_string() == "upgrade"),
        ),
    ).order_by(PlatformJob.created_at.desc(), PlatformJob.id.desc()).limit(1))
    installed_release = installed_workers_release_digest()
    release_matches = (
        installation.upgrade_release is None
        and installed_release is not None and installed_release == installation.policy.get("bounds_release")
    )
    deployment = await deployment_evidence(db, installation)
    recovery = recovery_for_job(latest_job) if latest_job is not None and (
        latest_job.payload.get("revision") == installation.desired_revision
    ) else None
    job_code = recovery.code if recovery else None
    cooldown = latest_job.available_at if latest_job is not None and latest_job.available_at > as_of else None
    blockers = placement_blockers(
        enabled=installation.desired_state == "enabled", admitted=installation.admitted_revision is not None,
        release_matches=release_matches, spend_ready=spend.state == "ready",
        cleanup_pending=bool(exhausted or overdue_containers or installation.drain_required
                             or installation.desired_state == "disconnected"
                             or job_code in ("CF_CLEANUP_PENDING", "CF_CLEANUP_SETTLING", "CF_PRIOR_WORKERS_LIVE")),
        connector_session=deployment.connector_session, job_code=job_code, cooldown_until=cooldown,
    )
    lanes = await overflow_status(db, caller, installation, policy, now=as_of, approvals=approvals, blockers=blockers)
    return CloudflareCapacityStatus(
        consent_text_version=CONSENT_TEXT_VERSION,
        consent_text=CONSENT_TEXT,
        managed_tunnel_available=get_settings().cloudflare_tunnel_channel is not None,
        latest_job_id=latest_job.id if latest_job else None,
        observed_at=as_of,
        approved_workflow_count=approvals,
        lanes=lanes,
        placement_reasons=blockers,
        deployment=deployment,
        recovery=recovery,
        installation=CloudflareInstallationStatus.model_validate({
            "installation_id": installation.id,
            "reachability_mode": installation.reachability_mode,
            "connector_token_version": installation.tunnel_token_version,
            "consent_version": installation.consent_version,
            "consent_at": installation.consent_at,
            "desired_revision": installation.desired_revision,
            "deployed_revision": installation.deployed_revision,
            "upgrade_pending": installation.upgrade_release is not None,
            "upgrade_available": workers_upgrade_available(
                installation, installed_release, installed_workers_release_order(),
            ),
            "desired_state": installation.desired_state,
            "admission_open": (
                installation.admitted_revision is not None and release_matches and spend.state == "ready"
            ),
            "policy": policy,
            "fleet_envelope_usd": installation.policy["fleet_envelope_usd"],
            "owned_resources_pending": owned_resources_pending + overdue_containers,
            "spend": spend,
        }),
    )


def _price_policy(
    policy: CloudflareCapacityPolicy, bounds: WorkersReleaseMeterBounds
) -> dict[str, object]:
    if policy.workers.slots_per_launcher > bounds.max_slots_per_launcher:
        raise ValueError("Workers meter bounds do not cover the configured slot count")
    if set(bounds.launcher_caps) != set(LAUNCHER_RECEIPT_METERS) or any(
        type(cap) is not int or cap <= 0 for cap in bounds.launcher_caps.values()
    ):
        raise ValueError("Workers release needs positive caps for all launcher meters")
    if not valid_cpu_limits(bounds.cpu_limits_ms):
        raise ValueError("Workers release needs positive CPU limits for all qualified paths")
    if not 0 < bounds.startup_expected_us <= bounds.cpu_limits_ms["startup"] * 1000:
        raise ValueError("Workers release needs a measured startup cost within its live reservation")
    digest = sha256(json.dumps({
        "cleanup_margin_seconds": bounds.cleanup_margin_seconds,
        "max_slots_per_launcher": bounds.max_slots_per_launcher,
        "per_launcher_second": {k: str(v.normalize()) for k, v in bounds.per_launcher_second.items()},
        "per_fleet_fixed": {k: str(v.normalize()) for k, v in bounds.per_fleet_fixed.items()},
        "launcher_caps": dict(bounds.launcher_caps),
        "cpu_limits_ms": dict(bounds.cpu_limits_ms),
        "startup_p99_us": bounds.startup_expected_us,
        "cpu_expected_ms": dict(bounds.expected_cpu_ms) if bounds.expected_cpu_ms is not None else None,
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    root_rate = Decimal(0) if bounds.expected_cpu_ms is not None else launcher_root_rate(bounds.per_launcher_second)
    meter_launcher = {
        "caps": dict(bounds.launcher_caps),
        "second_usd": str(launcher_lifetime_rate(bounds.per_launcher_second) - launcher_root_rate(bounds.per_launcher_second) + root_rate),
        "cpu_ms": dict(bounds.cpu_limits_ms),
        "startup_p99_us": bounds.startup_expected_us,
        "root_second_usd": str(root_rate),
    }
    if bounds.lifetime_meters:
        validate_workers_cleanup_margin(bounds.cleanup_margin_seconds)
        rates = {name: int(bounds.per_launcher_second[name]) for name in LAUNCHER_RECEIPT_METERS}
        meter_launcher["per_second_caps"] = rates
        meter_launcher["caps"] = {name: rate * policy.workers.max_lifetime_seconds for name, rate in rates.items()}
    if bounds.expected_cpu_ms is not None:
        meter_launcher["cpu_expected_ms"] = dict(bounds.expected_cpu_ms)
    if bounds.bounded_linger:
        meter_launcher["linger_seconds"] = policy.workers.linger_seconds
    envelope = workers_fleet_envelope_usd(
        launcher_cap=policy.workers.launcher_cap,
        max_lifetime_seconds=policy.workers.max_lifetime_seconds,
        cleanup_margin_seconds=bounds.cleanup_margin_seconds,
        per_launcher_second=bounds.per_launcher_second,
        per_fleet_fixed=bounds.per_fleet_fixed,
        rotation_slots=min(policy.workers.installation_slots, policy.workers.launcher_cap * policy.workers.slots_per_launcher),
    )
    if envelope > policy.monthly_limit_usd:
        raise CloudflareBudgetTooLow(envelope)
    limits = policy.model_dump(mode="json")
    limits["workers"]["work_kinds"] = sorted(policy.workers.work_kinds)
    return {
        "limits": limits,
        "rate_table_version": WORKERS_RATE_TABLE_VERSION,
        "fleet_envelope_usd": str(envelope),
        "bounds_digest": digest,
        "bounds_release": bounds.release,
        "meter_launcher": meter_launcher,
    }


async def create_cloudflare_installation(
    db: AsyncSession,
    *,
    account_id: str,
    worker_ingress_origin: str,
    consent_version: str,
    actor_id: UUID,
    verified_api_token: SecretStr,
    policy: CloudflareCapacityPolicy,
    bounds: WorkersReleaseMeterBounds,
    reachability_mode: CloudflareReachabilityMode = "existing_https_endpoint",
    zone_id: str | None = None,
) -> UUID:
    """Persist an already validated connection in the caller's transaction."""
    if not re.fullmatch("[a-f0-9]{32}", account_id):
        raise ValueError("Invalid Cloudflare account ID")
    canonical_origin = f"https://{dedicated_hostname(worker_ingress_origin)}"
    if consent_version != CONSENT_TEXT_VERSION:
        raise ValueError("Cloudflare installation requires consent to the current text")
    actor = str(actor_id)
    desired = _price_policy(policy, bounds)
    installation_id = uuid4()
    now = datetime.now(UTC)
    result = await db.execute(
        insert(CloudflareInstallation)
        .values(
            id=installation_id,
            account_id=account_id,
            worker_ingress_origin=canonical_origin,
            reachability_mode=reachability_mode,
            zone_id=zone_id,
            consent_version=consent_version,
            consent_actor=actor,
            consent_at=now,
            policy=desired,
            workers_release_sequence=bounds.release_order.sequence,
            workers_release_version=bounds.release_order.version,
            workers_release_digest=bounds.release,
            desired_revision=1,
            desired_state="scaled_in",
            api_token_version=1,
            control_root_version=1,
        )
        .on_conflict_do_nothing(
            index_elements=[CloudflareInstallation.singleton],
            index_where=CloudflareInstallation.retired_at.is_(None),
        )
        .returning(CloudflareInstallation.id)
    )
    if result.scalar_one_or_none() is None:
        raise CloudflareInstallationExists("A Cloudflare installation already exists")

    ref = CredentialRef(installation_id, account_id, "account_token", "", 1)
    root = CredentialRef(installation_id, account_id, "control_root", "", 1)
    db.add_all([
        seal_credential(ref, verified_api_token, actor=actor, now=now),
        seal_credential(root, SecretStr(secrets.token_urlsafe(32)), actor=actor, now=now),
    ])
    await db.flush()
    return installation_id


async def retire_cloudflare_installation(
    db: AsyncSession, *, installation_id: UUID, expected_revision: int,
) -> None:
    """Finish a fenced disconnect after launcher drain and verified provider absence."""
    row = await db.get(
        CloudflareInstallation, installation_id, with_for_update=True, populate_existing=True,
    )
    if row is None or row.desired_revision != expected_revision or row.desired_state != "disconnected":
        raise CloudflarePolicyConflict("Cloudflare disconnect revision changed")
    if row.retired_at is not None:
        return
    remaining = await db.scalar(
        select(CloudflareOwnedResource.id).where(
            CloudflareOwnedResource.installation_id == installation_id,
            or_(CloudflareOwnedResource.state != "absent", CloudflareOwnedResource.observed_at.is_(None)),
        ).limit(1)
    )
    scope = await db.scalar(select(CloudflareCapacityScope.scope).where(
        CloudflareCapacityScope.installation_id == installation_id,
    ).limit(1))
    if remaining is not None or scope is not None or await cloudflare_workers_remain(db, installation_id):
        raise CloudflarePolicyConflict("Cloudflare cleanup is pending")
    row.api_token_version = None
    row.control_root_version = None
    row.deployed_revision = None
    row.retired_at = datetime.now(UTC)
    await db.execute(update(CloudflareCredential).where(
        CloudflareCredential.installation_id == installation_id,
    ).values(ciphertext=None))
    await db.flush()


async def cloudflare_workers_remain(db: AsyncSession, installation_id: UUID, *, before_revision: int | None = None) -> bool:
    pinned = CloudflareWorkerReservation.installation_id == installation_id
    if before_revision is not None:
        pinned = pinned & (CloudflareWorkerReservation.deployment_revision < before_revision)
    return (await db.scalar(
        select(ExecutionWorker.id)
        .outerjoin(CloudflareWorkerReservation, ExecutionWorker.id == CloudflareWorkerReservation.worker_id)
        .where(
            or_(
                pinned,
                (CloudflareWorkerReservation.worker_id.is_(None)
                 & ExecutionWorker.capacity_scope.is_not(None)
                 & (ExecutionWorker.runtime_profile == "cloudflare-python-v1")),
            ),
            or_(
                ExecutionWorker.state.not_in(("lost", "retired")),
                ExecutionWorker.capacity_resource_deleted_at.is_(None),
                unresolved_container_reservation(),
            ),
        )
        .limit(1)
    )) is not None


async def save_cloudflare_policy(
    db: AsyncSession,
    *,
    installation_id: UUID,
    expected_revision: int,
    policy: CloudflareCapacityPolicy,
    bounds: WorkersReleaseMeterBounds,
) -> int:
    desired = _price_policy(policy, bounds)
    result = await db.execute(
        select(CloudflareInstallation)
        .where(CloudflareInstallation.id == installation_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    row = result.scalar_one_or_none()
    if row is None or row.desired_revision != expected_revision or row.desired_state == "disconnected":
        raise CloudflarePolicyConflict("Cloudflare installation changed. Reload its current policy.")
    if code := workers_release_error(row, bounds.release, bounds.release_order):
        raise CloudflarePolicyConflict(f"{code}. Use Reconcile with Reset Release Fence to accept this package.")
    if row.upgrade_release is not None and row.upgrade_release != bounds.release:
        raise CloudflarePolicyConflict("Edit policy from the matched Cloudflare release.")
    if row.policy != desired:
        floor = workers_release_floor(row)
        if floor is None or bounds.release_order.key > floor.key:
            set_workers_release_floor(row, bounds.release, bounds.release_order)
        row.policy = desired
        row.admitted_revision = None
        row.desired_revision += 1
        row.upgrade_drained_at = None
        await db.flush()
        wake_pool_workers_after_commit(db, WAKE_CLOUDFLARE_CAPACITY)
    return row.desired_revision
