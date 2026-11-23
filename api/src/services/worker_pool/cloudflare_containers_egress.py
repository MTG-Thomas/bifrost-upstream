"""Verify the network boundary against the owned scope and dedicated ingress."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.platform_jobs import PlatformJob
from src.services.worker_pool.cloudflare_containers_auth import request_container_control
from src.services.worker_pool.cloudflare_containers_policy import (
    CONTAINERS_SCOPE,
    container_identity_matches,
    container_provider_pin,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    load_managed_control_key,
    open_credential,
)
from src.services.worker_pool.cloudflare_tunnel_rest import dedicated_hostname
from src.worker.remote.container_egress import CONTAINER_EGRESS_DENIALS
from src.worker.remote.protocol import nonce_digest

_MINT = object()
# A brief cache lets the planner reuse a successful probe within one
# reconciliation window. A provider version change invalidates it immediately.
EGRESS_PROOF_MAX_AGE = timedelta(hours=1)
PENDING_PROOF_MIN_REMAINING = timedelta(minutes=15)


class ContainerEgressMismatch(Exception):
    """An authenticated challenge produced conflicting evidence."""


def _active_probe_job():
    pin = CloudflareWorkerReservation
    return (
        (PlatformJob.id == pin.canary_job_id) & (PlatformJob.lease_token == pin.canary_lease_token)
        & (PlatformJob.status == "running") & (PlatformJob.lease_expires_at > func.clock_timestamp())
    )


class ProbeReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nonce: str
    worker_id: UUID
    revision: int
    release_digest: str
    origin: str


class ContainerEgressRefresh(BaseModel):
    revision: int
    failures: int = Field(ge=1)
    retry_at: AwareDatetime


EGRESS_REFRESH_KEY = "container_egress_refresh"
QUALIFICATION_BACKOFF_KEY = "container_qualification_backoff"
PENDING_QUALIFICATION_KEY = "container_pending_qualification"
QUALIFICATION_BACKOFF_CODE = "CF_CONTAINERS_QUALIFICATION_BACKOFF"


def read_egress_refresh(
    installation: CloudflareInstallation, *, qualification: bool = False,
) -> ContainerEgressRefresh | None:
    key = QUALIFICATION_BACKOFF_KEY if qualification else EGRESS_REFRESH_KEY
    evidence = (installation.deployment_evidence or {}).get(key)
    if evidence is None:
        return None
    try:
        refresh = ContainerEgressRefresh.model_validate(evidence)
    except ValidationError:
        return None
    return refresh if refresh.revision == installation.desired_revision else None


def record_egress_refresh_failure(
    installation: CloudflareInstallation, now: datetime, *, qualification: bool = False,
) -> None:
    key = QUALIFICATION_BACKOFF_KEY if qualification else EGRESS_REFRESH_KEY
    previous = read_egress_refresh(installation, qualification=qualification)
    failures = previous.failures + 1 if previous else 1
    delay = min(timedelta(minutes=5 * 2 ** min(failures - 1, 4)), EGRESS_PROOF_MAX_AGE)
    refresh = ContainerEgressRefresh(revision=installation.desired_revision, failures=failures, retry_at=now + delay)
    installation.deployment_evidence = {
        **(installation.deployment_evidence or {}), key: refresh.model_dump(mode="json"),
    }


@dataclass(frozen=True, slots=True)
class ContainerEgressObservation:
    installation_id: UUID
    revision: int
    release_digest: str
    origin: str
    provider_id: str
    provider_version: str
    observed_at: datetime
    report: ProbeReport
    signed_control_round_trip: bool
    denied: frozenset[str]


class ContainerEgressRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    installation_id: UUID
    revision: int
    release_digest: str
    origin: str
    provider_id: str
    provider_version: str
    observed_at: datetime


@dataclass(frozen=True, slots=True, init=False)
class VerifiedEgress:
    installation_id: UUID
    revision: int
    release_digest: str
    origin: str
    provider_id: str
    provider_version: str
    observed_at: datetime

    def __init__(self, observation: ContainerEgressObservation | ContainerEgressRecord, *, _mint: object) -> None:
        if _mint is not _MINT:
            raise TypeError("Container egress must be verified by the network probe")
        for field in self.__dataclass_fields__:
            object.__setattr__(self, field, getattr(observation, field))

    def as_record(self) -> dict:
        return ContainerEgressRecord.model_validate(self, from_attributes=True).model_dump(mode="json")


def _read_egress_proof(
    installation: CloudflareInstallation, now: datetime, key: str, revision: int | None,
) -> VerifiedEgress | None:
    try:
        record = ContainerEgressRecord.model_validate((installation.deployment_evidence or {}).get(key))
        if (
            (record.installation_id, record.revision, record.origin) != (
                installation.id, revision, installation.worker_ingress_origin,
            )
            or record.observed_at.tzinfo is None
            or not now - EGRESS_PROOF_MAX_AGE <= record.observed_at <= now
        ):
            return None
    except ValidationError:
        return None
    return VerifiedEgress(record, _mint=_MINT)


def read_verified_egress(installation: CloudflareInstallation, now: datetime) -> VerifiedEgress | None:
    """Reconstitute only the current, fresh proof committed by joint qualification."""
    proof = _read_egress_proof(installation, now, "container_egress", installation.containers_ready_revision)
    return proof if proof is not None and proof.release_digest == installation.containers_release_digest else None


def read_pending_qualification(installation: CloudflareInstallation, now: datetime) -> VerifiedEgress | None:
    """Read a proof with enough lifetime left to admit after verified retirement."""
    proof = _read_egress_proof(installation, now, PENDING_QUALIFICATION_KEY, installation.desired_revision)
    if proof is not None and proof.observed_at + EGRESS_PROOF_MAX_AGE - now >= PENDING_PROOF_MIN_REMAINING:
        return proof
    return None


def current_container_application(installation: CloudflareInstallation, revision: int, release_digest: str):
    return select(CloudflareOwnedResource.provider_id, CloudflareOwnedResource.provider_version).where(
        CloudflareOwnedResource.installation_id == installation.id,
        CloudflareOwnedResource.account_id == installation.account_id,
        CloudflareOwnedResource.scope == CONTAINERS_SCOPE,
        CloudflareOwnedResource.kind == "container_application",
        CloudflareOwnedResource.target_name == f"bifrost-{installation.id.hex}-r{revision}",
        CloudflareOwnedResource.desired_revision == revision,
        CloudflareOwnedResource.artifact_digest == release_digest,
        CloudflareOwnedResource.state == "present",
        CloudflareOwnedResource.observed_at.is_not(None),
    )


async def _probe_container_egress(
    db: AsyncSession, http: httpx.AsyncClient, *, installation_id: UUID, revision: int,
    release_digest: str, origin: str, probe_worker_id: UUID,
) -> ContainerEgressObservation | None:
    installation = await db.get(CloudflareInstallation, installation_id, populate_existing=True)
    scope = await db.get(CloudflareCapacityScope, (installation_id, CONTAINERS_SCOPE, revision), populate_existing=True)
    if (
        installation is None or scope is None or installation.retired_at is not None
        or installation.desired_state != "enabled" or installation.desired_revision != revision
        or installation.worker_ingress_origin != origin
    ):
        return None
    app = (await db.execute(current_container_application(installation, revision, release_digest))).one_or_none()
    if app is None or app.provider_id is None or app.provider_version is None:
        return None
    pin = CloudflareWorkerReservation
    worker = ExecutionWorker
    binding = (await db.execute(select(pin, worker.capacity_resource_uid).join(
        worker, worker.id == pin.worker_id,
    ).join(PlatformJob, _active_probe_job()).where(
        pin.worker_id == probe_worker_id, pin.installation_id == installation_id,
        pin.deployment_revision == revision, pin.canary_job_id.is_not(None),
        worker.state.in_(("starting", "ready")),
        container_identity_matches(probe_worker_id, release_digest),
    ))).one_or_none()
    if binding is None:
        return None
    ref = CredentialRef(installation_id, installation.account_id, "scope_bearer", CONTAINERS_SCOPE, scope.bearer_version)
    credential = await db.get(CloudflareCredential, (ref.installation_id, ref.purpose, ref.scope, ref.version))
    if credential is None:
        return None
    observed_at = await db.scalar(select(func.clock_timestamp()))
    assert observed_at is not None
    try:
        reservation, resource_uid = binding
        nonce = (reservation.container_envelope or {}).get("probe_nonce")
        if not isinstance(nonce, str) or re.fullmatch(r"[A-Za-z0-9_-]{43}", nonce) is None:
            return None
        if resource_uid is None:
            return None
        token = open_credential(ref, credential).get_secret_value()
        provider_pin = container_provider_pin(reservation, resource_uid)
        if provider_pin["credentialVersion"] != scope.bearer_version:
            return None
        control_key = await load_managed_control_key(db, reservation, resource_uid)
        payload = await request_container_control(
            http, scope.endpoint, token, control_key, operation="egress-probe", pin=provider_pin,
            now_ms=int(observed_at.timestamp() * 1000),
            payload={"nonce": nonce, "origin": origin, "release_digest": release_digest},
        )
        report = ProbeReport.model_validate_json(payload, strict=True)
    except (httpx.HTTPError, ValueError):
        return None
    if report.nonce != nonce or report.worker_id != probe_worker_id:
        raise ContainerEgressMismatch("Container probe identity changed")

    # The API records the signed supervisor's own network observations only
    # after the dedicated ingress accepted the same session's SDK credential.
    witness = await db.scalar(select(pin.container_envelope).select_from(worker).join(pin, pin.worker_id == worker.id).join(
        PlatformJob, _active_probe_job(),
    ).where(
        worker.id == probe_worker_id,
        worker.remote_auth_nonce_digest == nonce_digest(nonce),
        worker.remote_owner_lease_expires_at > func.clock_timestamp(),
        pin.canary_ack_at.is_not(None), pin.canary_sdk_generation == worker.remote_session_generation,
        pin.container_envelope["denial_generation"].as_integer() == worker.remote_session_generation,
        pin.installation_id == installation_id, pin.deployment_revision == revision,
        container_identity_matches(worker.id, release_digest),
    ))
    return ContainerEgressObservation(
        installation_id, revision, release_digest, origin, app.provider_id, app.provider_version,
        observed_at, report, witness is not None, frozenset(witness.get("egress_denials", ())) if witness else frozenset(),
    )


async def verify_container_egress(
    db: AsyncSession, http: httpx.AsyncClient, *, installation_id: UUID, revision: int,
    release_digest: str, origin: str, probe_worker_id: UUID,
) -> VerifiedEgress | None:
    """Challenge the authenticated scope and independently witness signed control."""
    try:
        if origin != f"https://{dedicated_hostname(origin)}":
            return None
    except ValueError:
        return None
    observation = await _probe_container_egress(
        db, http, installation_id=installation_id, revision=revision, release_digest=release_digest,
        origin=origin, probe_worker_id=probe_worker_id,
    )
    if observation is None:
        return None
    report = observation.report
    if (
        report.origin != observation.origin
        or report.revision != revision or report.release_digest != release_digest
    ):
        raise ContainerEgressMismatch("Container probe application or origin changed")
    if not observation.signed_control_round_trip:
        return None
    if observation.denied != CONTAINER_EGRESS_DENIALS:
        raise ContainerEgressMismatch("Container probe did not witness the required denials")
    return VerifiedEgress(observation, _mint=_MINT)
