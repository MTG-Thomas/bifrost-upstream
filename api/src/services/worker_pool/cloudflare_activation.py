"""Read-only Cloudflare discovery and a fenced, atomic first Enable."""

import json
import os
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_CEILING
from hashlib import sha256
from hmac import compare_digest
from typing import Any, Literal, NotRequired, TypedDict
from uuid import UUID, uuid4

import httpx
from cryptography.fernet import InvalidToken
from pydantic import SecretStr
from shared.models import (
    CLOUDFLARE_ID_PATTERN,
    CloudflareActivateRequest,
    CloudflareActivationPreview,
    CloudflareCapacityPolicy,
    CloudflareCredentialInput,
    CloudflareDiscoveryChoice,
    CloudflarePreflight,
    CloudflarePreflightRequest,
    CloudflareReachabilityMode,
    ContainersPolicy,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.core.security import decrypt_secret, encrypt_secret
from src.jobs.platform.cloudflare_enable import enqueue_cloudflare_enable
from src.models.orm.cloudflare_capacity import CloudflareInstallation
from src.models.orm.platform_jobs import PlatformJob
from src.services.audit import emit_audit
from src.services.authorization.enforce import Caller
from src.services.kubernetes_control import may_manage_jobs
from src.services.worker_pool.cloudflare_connector_channel import connector_channel_ready
from src.services.worker_pool.cloudflare_containers_policy import CONTAINERS_PILOT_POLICY, container_account_usage, fleet_envelope
from src.services.worker_pool.cloudflare_containers_release import load_containers_release
from src.services.worker_pool.cloudflare_credentials import (
    StagedCloudflareCredentialUnavailable,
    consume_staged_token,
    read_staged_connect_token,
)
from src.services.worker_pool.cloudflare_policy import (
    CONTAINERS_CONSENT_VERSION,
    CONSENT_TEXT,
    CONSENT_TEXT_VERSION,
    WORKERS_PAID_SUBSCRIPTION_USD,
    CloudflareBudgetTooLow,
    CloudflareInstallationExists,
    CloudflarePolicyConflict,
    _price_policy,
    create_cloudflare_installation,
    release_meter_bounds,
    suggested_cloudflare_policy,
)
from src.services.worker_pool.cloudflare_provisioning import OwnershipConflict
from src.services.worker_pool.cloudflare_release import WorkersReleaseUnavailable, load_workers_release
from src.services.worker_pool.cloudflare_tunnel_rest import TunnelRest, dedicated_hostname
from src.services.worker_pool.cloudflare_workers_rest import (
    CloudflareManagementDenied,
    WorkersPaidUnverified,
    WorkersRestCleanup,
)


class _Receipt(TypedDict):
    purpose: str
    installation_id: str
    actor: str
    credential_ref: str | None
    token_digest: str
    generation: str | None
    release: str
    consent_version: str
    expires_at: str
    account_id: str
    zone_id: str | None
    origin: str
    mode: CloudflareReachabilityMode
    policy: dict[str, Any]
    containers_release: NotRequired[str]
    containers_policy: NotRequired[dict[str, Any]]


# SPEND-DEFAULTS.md, Moderate Workers-only forecast sensitivity for Preset B.
PRESET_B_ESTIMATED_USAGE_MIN_USD = Decimal("2")
PRESET_B_ESTIMATED_USAGE_MAX_USD = Decimal("7")
CONTAINERS_CONSENT_TEXT = (
    "I authorize Bifrost to provision paid Cloudflare Containers for approved Linux workflows. "
    "Containers usage draws against the confirmed shared monthly limit and the Container instance-hour budget "
    "and may incur Cloudflare charges."
)


class CloudflarePreflightFieldError(ValueError):
    def __init__(self, field: str, message: str) -> None:
        super().__init__(message)
        self.field = field


def _digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


async def _generation(db: AsyncSession) -> str | None:
    retired_at = await db.scalar(select(func.max(CloudflareInstallation.retired_at)))
    return retired_at.isoformat() if retired_at is not None else None


async def _credential(
    db: AsyncSession, actor_id: UUID, body: CloudflareCredentialInput,
) -> tuple[str | None, SecretStr, datetime | None]:
    if body.credential_ref is not None:
        return await read_staged_connect_token(db, body.credential_ref, actor_id=actor_id)
    assert body.api_token is not None
    return None, body.api_token, None


def _choices(entries: list[dict[str, Any]] | None) -> list[CloudflareDiscoveryChoice] | None:
    if entries is None:
        return None
    choices = {}
    for item in entries:
        identity, name = item.get("id"), item.get("name")
        if isinstance(identity, str) and re.fullmatch(CLOUDFLARE_ID_PATTERN, identity) and isinstance(name, str) and name:
            choices[identity] = CloudflareDiscoveryChoice(id=identity, name=name[:255])
    return sorted(choices.values(), key=lambda item: (item.name.casefold(), item.id))


def _selection(choices: list[CloudflareDiscoveryChoice] | None, selected: str | None, field: str) -> str | None:
    if choices is None:
        return selected
    if selected is not None:
        if not any(choice.id == selected for choice in choices):
            raise CloudflarePreflightFieldError(field, "Select an item from the discovered Cloudflare inventory.")
        return selected
    return choices[0].id if len(choices) == 1 else None


async def _managed_hostname(
    http: httpx.AsyncClient, token: SecretStr, account_id: str, zone_id: str,
) -> tuple[TunnelRest, str]:
    tunnel = TunnelRest(http, account_id=account_id, zone_id=zone_id, hostname="", token=token)
    zone = await tunnel.active_zone_name()
    suffix = _digest(get_settings().public_url.rstrip("/"))[:16]
    tunnel.hostname = dedicated_hostname(f"https://bifrost-workers-{suffix}.{zone}")
    return tunnel, zone


async def preflight_cloudflare(
    db: AsyncSession, caller: Caller, body: CloudflarePreflightRequest,
) -> CloudflarePreflight:
    if await db.scalar(select(CloudflareInstallation.id).where(CloudflareInstallation.retired_at.is_(None))):
        raise CloudflareInstallationExists("A Cloudflare installation already exists.")
    generation = await _generation(db)
    proposal = suggested_cloudflare_policy()
    if proposal is None:
        raise WorkersReleaseUnavailable("Install a qualified Workers release with lifetime meters")
    policy = body.policy or proposal
    workers_release = load_workers_release()
    bounds = release_meter_bounds(workers_release)
    priced = _price_policy(policy, bounds)
    container_policy = None
    if body.containers:
        container_policy = CONTAINERS_PILOT_POLICY
        hours = body.container_instance_hours or policy.container_instance_hours
        if hours is not None:
            container_policy = container_policy.model_copy(update={"monthly_instance_hours": hours})
        policy = policy.model_copy(update={"container_instance_hours": container_policy.monthly_instance_hours})
    actor = caller.principal.user_id
    bound_account, token, credential_expiry = await _credential(db, actor, body)
    preset_b = policy.model_dump(exclude={"monthly_limit_usd"}) == proposal.model_dump(exclude={"monthly_limit_usd"})
    if bound_account and body.account_id not in (bound_account, None):
        raise CloudflarePreflightFieldError("account_id", "The staged credential is bound to a different Cloudflare account.")

    settings = get_settings()
    primary: Literal["static", "job", "external", "unknown"] = "static"
    if settings.worker_capacity_url and settings.worker_capacity_token:
        primary = "external"
    elif os.environ.get("KUBERNETES_SERVICE_HOST"):
        try:
            primary = "job" if await may_manage_jobs(settings) else "unknown"
        except httpx.HTTPError:
            primary = "unknown"
    ready = await connector_channel_ready()
    recommended = "managed_tunnel" if ready else "existing_https_endpoint"
    mode = body.reachability_mode or recommended
    async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
        containers_release = await load_containers_release(http, workers_release) if body.containers else None
        container_floor = None
        workers_floor = Decimal(str(priced["fleet_envelope_usd"]))
        combined_floor = workers_floor
        if containers_release is not None and container_policy is not None:
            container_floor = fleet_envelope(
                container_policy, containers_release.manifest.rates, 31 * 86400,
            ).quantize(Decimal("0.01"), rounding=ROUND_CEILING)
            combined_floor += container_floor
        client = WorkersRestCleanup(http)
        accounts = _choices(await client.discovery_inventory(token))
        if bound_account and accounts is not None:
            accounts = [account for account in accounts if account.id == bound_account]
        account_id = bound_account or _selection(accounts, body.account_id, "account_id")
        remaining_hours = None
        if container_policy is not None:
            used = await db.scalar(select(container_account_usage(
                func.now(), account_id=account_id, instance_seconds=True,
            ))) if account_id is not None else 0
            remaining_hours = max(Decimal(0), Decimal(container_policy.monthly_instance_hours) - Decimal(used or 0) / 3600)
        result = CloudflarePreflight(
            accounts=accounts, account_id=account_id, connector_channel=settings.cloudflare_tunnel_channel,
            connector_ready=ready,
            primary=primary,
            recommended_reachability=recommended, reachability_mode=mode,
            preview=CloudflareActivationPreview(
                policy=policy, fleet_envelope_usd=combined_floor,
                estimated_usage_min_usd=PRESET_B_ESTIMATED_USAGE_MIN_USD if preset_b else None,
                estimated_usage_max_usd=PRESET_B_ESTIMATED_USAGE_MAX_USD if preset_b else None,
                workers_paid_subscription_usd=WORKERS_PAID_SUBSCRIPTION_USD,
                containers_policy=container_policy,
                containers_fleet_envelope_usd=container_floor,
                containers_remaining_instance_hours=remaining_hours,
            ),
            consent_version=CONTAINERS_CONSENT_VERSION if containers_release else CONSENT_TEXT_VERSION,
            consent_text=f"{CONSENT_TEXT} {CONTAINERS_CONSENT_TEXT}" if containers_release else CONSENT_TEXT,
        )
        if account_id is None:
            return result
        try:
            await client.verify_workers_paid(account_id, token)
        except WorkersPaidUnverified:
            result.workers_paid = "unverified"
            return result
        except CloudflareManagementDenied:
            result.workers_paid = "denied"
            return result
        result.workers_paid = "verified"
        zones = await client.discovery_inventory(token, account_id)
        result.zones = _choices(None if zones is None else [
            item for item in zones if item.get("status") == "active"
            and isinstance(item.get("account"), dict) and item["account"].get("id") == account_id
        ])
        if result.zones:
            selected_zone = next((item for item in result.zones if item.id == body.zone_id), None)
            if selected_zone is None and len(result.zones) == 1:
                selected_zone = result.zones[0]
            if selected_zone is not None:
                result.zone_name = selected_zone.name
        if mode == "managed_tunnel":
            if not ready:
                return result
            result.zone_id = _selection(result.zones, body.zone_id, "zone_id")
            if result.zone_id is None:
                return result
            tunnel, result.zone_name = await _managed_hostname(http, token, account_id, result.zone_id)
            result.worker_ingress_origin = f"https://{tunnel.hostname}"
            try:
                await tunnel.require_unused_hostname()
            except OwnershipConflict:
                result.hostname_collision = True
                return result
        elif body.worker_ingress_origin:
            try:
                result.worker_ingress_origin = f"https://{dedicated_hostname(body.worker_ingress_origin)}"
            except ValueError as exc:
                raise CloudflarePreflightFieldError("worker_ingress_origin", str(exc)) from None
        if result.worker_ingress_origin is None:
            return result

    expires_at = datetime.now(UTC) + timedelta(minutes=10)
    if credential_expiry is not None:
        expires_at = min(expires_at, credential_expiry)
        if expires_at <= datetime.now(UTC):
            raise StagedCloudflareCredentialUnavailable("Invalid or expired Cloudflare credential reference")
    receipt = _Receipt(
        purpose="cloudflare-activation-v1", installation_id=str(uuid4()), actor=str(actor),
        credential_ref=str(body.credential_ref) if body.credential_ref else None,
        token_digest=_digest(token.get_secret_value()), generation=generation, release=bounds.release,
        consent_version=result.consent_version, expires_at=expires_at.isoformat(),
        account_id=account_id, zone_id=result.zone_id, origin=result.worker_ingress_origin, mode=mode,
        policy=policy.model_dump(mode="json"),
    )
    if containers_release is not None and container_policy is not None:
        receipt["containers_release"] = containers_release.digest
        receipt["containers_policy"] = container_policy.model_dump(mode="json")
    result.preflight_receipt = encrypt_secret(json.dumps(receipt))
    result.expires_at = expires_at
    return result


def _open_receipt(body: CloudflareActivateRequest, actor_id: UUID) -> _Receipt:
    try:
        receipt: _Receipt = json.loads(decrypt_secret(body.preflight_receipt))
        if (
            receipt["purpose"] == "cloudflare-activation-v1" and receipt["actor"] == str(actor_id)
            and receipt["credential_ref"] == (str(body.credential_ref) if body.credential_ref else None)
            and receipt["consent_version"] == body.consent_version == (
                CONTAINERS_CONSENT_VERSION if receipt.get("containers_release") else CONSENT_TEXT_VERSION
            )
            and (body.api_token is None or compare_digest(
                receipt["token_digest"], _digest(body.api_token.get_secret_value()),
            ))
        ):
            return receipt
    except (InvalidToken, ValueError, KeyError, TypeError):
        pass
    raise CloudflarePolicyConflict("Run Cloudflare preflight and review consent again.")


async def _resume_activation(
    db: AsyncSession, caller: Caller, installation_id: UUID, fingerprint: str,
) -> tuple[PlatformJob, bool] | None:
    row = await db.get(CloudflareInstallation, installation_id, with_for_update=True, populate_existing=True)
    if row is None:
        return None
    if (
        row.retired_at is not None or row.desired_state != "enabled" or row.desired_revision != 1
        or row.policy.get("activation_fingerprint") != fingerprint
    ):
        raise CloudflarePolicyConflict("Cloudflare activation already changed. Review the current connection.")
    return await enqueue_cloudflare_enable(db, caller, installation_id=installation_id, expected_revision=1)


async def activate_cloudflare(
    db: AsyncSession, caller: Caller, body: CloudflareActivateRequest,
) -> tuple[PlatformJob, bool]:
    actor_id = caller.principal.user_id
    receipt = _open_receipt(body, actor_id)
    installation_id = UUID(receipt["installation_id"])
    fingerprint = _digest(f"{body.preflight_receipt}:{body.monthly_limit_usd.normalize()}")
    existing = await _resume_activation(db, caller, installation_id, fingerprint)
    if existing is not None:
        return existing
    if datetime.fromisoformat(receipt["expires_at"]) <= datetime.now(UTC):
        if body.credential_ref is not None:
            raise StagedCloudflareCredentialUnavailable("Invalid or expired Cloudflare credential reference")
        raise CloudflarePolicyConflict("Cloudflare preflight expired. Run preflight again.")
    workers_release = load_workers_release()
    bounds = release_meter_bounds(workers_release)
    if receipt["release"] != bounds.release or receipt["generation"] != await _generation(db):
        raise CloudflarePolicyConflict("Cloudflare installation or Workers release changed. Run preflight again.")
    policy = CloudflareCapacityPolicy.model_validate({**receipt["policy"], "monthly_limit_usd": body.monthly_limit_usd})
    priced = _price_policy(policy, bounds)
    containers_policy = None
    account_id, zone_id = receipt["account_id"], receipt["zone_id"]
    try:
        bound_account, token, _ = await _credential(db, actor_id, body)
        if bound_account not in (None, account_id) or not compare_digest(
            receipt["token_digest"], _digest(token.get_secret_value()),
        ):
            raise CloudflarePolicyConflict("Cloudflare credential changed. Run preflight again.")
        async with httpx.AsyncClient(timeout=10, trust_env=False, follow_redirects=False) as http:
            await WorkersRestCleanup(http).verify_workers_paid(account_id, token)
            if (expected_digest := receipt.get("containers_release")) is not None:
                release = await load_containers_release(http, workers_release)
                if release.digest != expected_digest:
                    raise CloudflarePolicyConflict("Cloudflare Containers release changed. Run preflight again.")
                containers_policy = ContainersPolicy.model_validate(receipt["containers_policy"])
                combined_floor = (
                    Decimal(str(priced["fleet_envelope_usd"]))
                    + fleet_envelope(containers_policy, release.manifest.rates, 31 * 86400)
                ).quantize(Decimal("0.01"), rounding=ROUND_CEILING)
                if combined_floor > policy.monthly_limit_usd:
                    raise CloudflareBudgetTooLow(combined_floor)
            if zone_id is not None:
                if not await connector_channel_ready():
                    raise CloudflarePolicyConflict("Managed Tunnel connector channel is unavailable. Run preflight again.")
                tunnel, _ = await _managed_hostname(http, token, account_id, zone_id)
                if f"https://{tunnel.hostname}" != receipt["origin"]:
                    raise CloudflarePolicyConflict("Cloudflare hostname changed. Run preflight again.")
                await tunnel.require_unused_hostname()
    except (ValueError, OwnershipConflict):
        # A concurrent identical submission may have consumed the reference or
        # started provisioning after the first replay lookup.
        existing = await _resume_activation(db, caller, installation_id, fingerprint)
        if existing is not None:
            return existing
        raise
    try:
        await create_cloudflare_installation(
            db, installation_id=installation_id, account_id=account_id, worker_ingress_origin=receipt["origin"],
            consent_version=body.consent_version, actor_id=actor_id, verified_api_token=token, policy=policy,
            bounds=bounds, reachability_mode=receipt["mode"], zone_id=zone_id, activation_fingerprint=fingerprint,
            containers_policy=containers_policy,
        )
    except CloudflareInstallationExists:
        existing = await _resume_activation(db, caller, installation_id, fingerprint)
        if existing is not None:
            return existing
        raise
    if receipt["generation"] != await _generation(db):
        raise CloudflarePolicyConflict("Cloudflare installation changed. Run preflight again.")
    if body.credential_ref is not None:
        await consume_staged_token(db, body.credential_ref, actor_id=actor_id, account_id=account_id, purpose="connect")
    await emit_audit(
        db, "capacity.cloudflare_activate", resource_type="cloudflare_installation", resource_id=installation_id,
        details={"account_id": account_id, "consent_version": body.consent_version, "zone_id": zone_id,
                 "reachability_mode": receipt["mode"], "monthly_limit_usd": str(body.monthly_limit_usd),
                 "containers": containers_policy is not None},
    )
    return await enqueue_cloudflare_enable(db, caller, installation_id=installation_id, expected_revision=1)
