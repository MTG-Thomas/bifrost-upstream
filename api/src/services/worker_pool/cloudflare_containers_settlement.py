"""Settle a managed incarnation only after two independent stop observations."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import SecretStr
from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.enums import ExecutionStatus
from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.services.worker_pool.cloudflare_containers_policy import CONTAINERS_SCOPE
from src.services.worker_pool.cloudflare_containers_rest import ContainersRest
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    ResourceObservation,
)


@dataclass(frozen=True, slots=True)
class ClosedContainerStart:
    """Receipt from the authenticated scope after it permanently closes the UID's start fence."""

    application: ResourceObservation
    worker_id: UUID
    resource_uid: str
    instance_id: str
    image: str
    last_start_effect_at: datetime
    placement_started_at: datetime
    outbound_bytes: int | None
    meter_complete: bool = False


async def settle_container_stop(
    db: AsyncSession, rest: ContainersRest, *, token: SecretStr, closure: ClosedContainerStart,
) -> None:
    """Pair a closed start fence with independent authenticated management readback.

    The caller must obtain the closure from the trusted scope controller after
    pending starts and sends have been fenced. A last-reported provider state
    alone cannot authorize release.
    """
    intent = closure.application.intent
    stop = await rest.observe_instance_stop(
        token, closure.application, instance_id=closure.instance_id,
        image=closure.image, not_before=closure.last_start_effect_at,
    )
    installation = await db.get(
        CloudflareInstallation, intent.target.installation_id,
        with_for_update={"key_share": True}, populate_existing=True,
    )
    if installation is None or installation.account_id != intent.target.account_id:
        raise ObservationPending("Container stop belongs to a different installation")
    worker = await db.get(ExecutionWorker, closure.worker_id, with_for_update=True, populate_existing=True)
    pin = await db.get(CloudflareWorkerReservation, closure.worker_id, populate_existing=True)
    journal = await db.scalar(select(CloudflareOwnedResource.id).where(
        CloudflareOwnedResource.installation_id == installation.id,
        CloudflareOwnedResource.account_id == installation.account_id,
        CloudflareOwnedResource.scope == CONTAINERS_SCOPE,
        CloudflareOwnedResource.kind == "container_application",
        CloudflareOwnedResource.target_name == intent.target.name,
        CloudflareOwnedResource.effect_id == intent.creation_effect_id,
        CloudflareOwnedResource.desired_revision == intent.desired_revision,
        CloudflareOwnedResource.artifact_digest == intent.artifact_digest,
        CloudflareOwnedResource.provider_id == closure.application.provider_id,
        CloudflareOwnedResource.provider_version == closure.application.provider_version,
        CloudflareOwnedResource.state == "present",
    ))
    if (
        worker is None or pin is None or journal is None
        or pin.installation_id != installation.id or pin.scope != CONTAINERS_SCOPE
        or pin.deployment_revision != intent.desired_revision or pin.container_release != intent.artifact_digest
        or worker.form != "external" or worker.capacity_scope != CONTAINERS_SCOPE
        or worker.capacity_resource_uid != closure.resource_uid
        or worker.state not in ("draining", "lost", "retired")
        or pin.container_reserved_at is None or pin.container_envelope is None
        or pin.container_envelope.get("provider_instance_id") != closure.instance_id
        or pin.container_envelope.get("prepared_image") != closure.image
        or stop.started_at < pin.container_reserved_at
        or stop.started_at != closure.placement_started_at
    ):
        raise ObservationPending("Container stop does not match the durable reservation")
    if pin.container_stopped_at is not None:
        if stop.stopped_at < pin.container_stopped_at:
            raise ObservationPending("Container's settled stop witness moved backwards")
        return
    if await db.scalar(select(exists().where(
        ExecutionWorkerClaim.worker_id == worker.id, ExecutionWorkerClaim.subject_id == Execution.id,
        Execution.status.in_((ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING)),
    ))):
        raise ObservationPending("Container still owns a live execution claim")
    envelope = {**pin.container_envelope, "observed_started_at": stop.started_at.isoformat()}
    count = closure.outbound_bytes
    if closure.meter_complete is True:
        if type(count) is not int or count < 0:
            raise ObservationPending("Container egress receipt is invalid")
        envelope["measured_outbound_bytes"] = count
        # An aggregate receipt spanning months cannot allocate bytes between
        # them. Preserve the conservative monthly holds in that case.
        if (stop.started_at.year, stop.started_at.month) == (stop.stopped_at.year, stop.stopped_at.month):
            envelope["settled_fixed_usd"] = str(
                Decimal(envelope["control_usd"]) + Decimal(envelope["outbound_gb_usd"]) * count / 10**9,
            )
    pin.container_envelope = envelope
    pin.container_stopped_at = stop.stopped_at
    worker.state = "retired"
    worker.ended_at = stop.stopped_at
    await db.flush()
