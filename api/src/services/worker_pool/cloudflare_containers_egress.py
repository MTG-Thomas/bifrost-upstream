"""Verify the network boundary against the owned scope and dedicated ingress."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict
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
from src.services.worker_pool.cloudflare_containers_policy import (
    CONTAINERS_SCOPE,
    container_identity_matches,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    open_credential,
)
from src.services.worker_pool.cloudflare_tunnel_rest import dedicated_hostname
from src.services.worker_pool.external_client import (
    CAPACITY_REQUEST_SECONDS,
    ExternalCapacityClient,
)
from src.worker.remote.protocol import new_nonce, nonce_digest

_MINT = object()
# A brief cache lets the planner reuse a successful probe within one
# reconciliation window. A provider version change invalidates it immediately.
EGRESS_PROOF_MAX_AGE = timedelta(minutes=5)
REQUIRED_DENIALS = frozenset({"alternate-origin", "wrong-host", "raw-tcp", "disallowed-route", "redirect"})


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
    application_id: str
    provider_version: str
    origin: str
    internet_enabled: bool
    intercepted_origins: frozenset[str]
    bounded_relay: bool
    denied: frozenset[str]


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


@dataclass(frozen=True, slots=True, init=False)
class VerifiedEgress:
    installation_id: UUID
    revision: int
    release_digest: str
    origin: str
    provider_id: str
    provider_version: str
    observed_at: datetime

    def __init__(self, observation: ContainerEgressObservation, *, _mint: object) -> None:
        if _mint is not _MINT:
            raise TypeError("Container egress must be verified by the network probe")
        for field in self.__dataclass_fields__:
            object.__setattr__(self, field, getattr(observation, field))


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
    binding = await db.scalar(select(pin.worker_id).join(PlatformJob, _active_probe_job()).where(
        pin.worker_id == probe_worker_id, pin.installation_id == installation_id,
        pin.deployment_revision == revision, pin.canary_job_id.is_not(None),
        container_identity_matches(probe_worker_id, release_digest),
    ))
    if binding is None:
        return None
    ref = CredentialRef(installation_id, installation.account_id, "scope_bearer", CONTAINERS_SCOPE, scope.bearer_version)
    credential = await db.get(CloudflareCredential, (ref.installation_id, ref.purpose, ref.scope, ref.version))
    if credential is None:
        return None
    observed_at = await db.scalar(select(func.clock_timestamp()))
    assert observed_at is not None
    nonce = new_nonce()
    try:
        token = open_credential(ref, credential).get_secret_value()
        endpoint = ExternalCapacityClient.check_configuration(scope.endpoint, token)
        async with http.stream(
            "POST", f"{endpoint}/v1/egress-probe",
            headers={"Authorization": f"Bearer {token}", "X-Bifrost-Capacity-Scope": CONTAINERS_SCOPE},
            json={"worker_id": str(probe_worker_id), "nonce": nonce},
            timeout=CAPACITY_REQUEST_SECONDS, follow_redirects=False,
        ) as response:
            response.raise_for_status()
            payload = bytearray()
            async for chunk in response.aiter_bytes():
                if len(payload) + len(chunk) > 8192:
                    return None
                payload.extend(chunk)
        report = ProbeReport.model_validate_json(payload, strict=True)
    except (httpx.HTTPError, ValueError):
        return None
    if report.nonce != nonce or report.worker_id != probe_worker_id:
        return None

    # The scope's report alone cannot witness signed control. Only the API
    # accepts this nonce, over the dedicated ingress, on the reserved canary.
    worker = ExecutionWorker
    signed = await db.scalar(select(worker.id).join(pin, pin.worker_id == worker.id).join(
        PlatformJob, _active_probe_job(),
    ).where(
        worker.id == probe_worker_id,
        worker.remote_auth_nonce_digest == nonce_digest(nonce),
        worker.remote_owner_lease_expires_at > func.clock_timestamp(),
        pin.installation_id == installation_id, pin.deployment_revision == revision,
        container_identity_matches(worker.id, release_digest),
    ))
    return ContainerEgressObservation(
        installation_id, revision, release_digest, origin, app.provider_id, app.provider_version,
        observed_at, report, signed is not None,
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
        report.application_id != observation.provider_id
        or report.provider_version != observation.provider_version
        or report.origin != observation.origin
        or report.internet_enabled
        or report.intercepted_origins != {origin}
        or not observation.signed_control_round_trip
        or not report.bounded_relay
        or not REQUIRED_DENIALS <= report.denied
    ):
        return None
    return VerifiedEgress(observation, _mint=_MINT)
