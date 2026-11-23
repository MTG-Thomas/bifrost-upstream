"""Settle a managed incarnation only after two independent stop observations."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from pydantic import SecretStr
from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.enums import ExecutionStatus
from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.services.worker_pool.cloudflare_containers_policy import CONTAINER_CLOCK_SKEW, CONTAINERS_SCOPE
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


@dataclass(frozen=True, slots=True)
class VoidContainerStart:
    """Authenticated registry tombstone for a UID that never crossed the start fence."""

    application: ResourceObservation
    worker_id: UUID
    resource_uid: str
    instance_id: str
    start_deadline_ms: int
    voided_at_ms: int
    allocation_refused: bool = False


@dataclass(frozen=True, slots=True)
class AbsentContainerStart:
    """Closed fence whose attempted placement is absent after the boot bound."""

    application: ResourceObservation
    worker_id: UUID
    resource_uid: str
    instance_id: str
    start_deadline_ms: int
    authorized_at_ms: int
    closed_at_ms: int


# Matches the scope registry's boot observation bound.
BOOT_OBSERVATION_MS = 60_000


async def settle_container_stop(
    db: AsyncSession, rest: ContainersRest, *, token: SecretStr,
    closure: ClosedContainerStart | VoidContainerStart | AbsentContainerStart,
) -> None:
    """Pair a closed start fence with independent authenticated management readback.

    The caller must obtain the closure from the trusted scope controller after
    pending starts and sends have been fenced. A last-reported provider state
    alone cannot authorize release.
    """
    intent = closure.application.intent
    installation = await db.get(
        CloudflareInstallation, intent.target.installation_id,
        with_for_update={"key_share": True}, populate_existing=True,
    )
    if installation is None or installation.account_id != intent.target.account_id:
        raise ObservationPending("Container stop belongs to a different installation")
    worker = await db.get(ExecutionWorker, closure.worker_id, with_for_update=True, populate_existing=True)
    pin = await db.get(CloudflareWorkerReservation, closure.worker_id, populate_existing=True)
    if (
        worker is None or pin is None
        or pin.installation_id != installation.id or pin.scope != CONTAINERS_SCOPE
        or pin.deployment_revision != intent.desired_revision or pin.container_release != intent.artifact_digest
        or worker.form != "external" or worker.capacity_scope != CONTAINERS_SCOPE
        or worker.capacity_resource_uid != closure.resource_uid
        or pin.container_reserved_at is None or pin.container_envelope is None
    ):
        raise ObservationPending("Container stop does not match the durable reservation")
    envelope = dict(pin.container_envelope)
    instance = envelope.get("provider_instance_id")
    void = isinstance(closure, VoidContainerStart)
    absent = isinstance(closure, AbsentContainerStart)
    refused = isinstance(closure, VoidContainerStart) and closure.allocation_refused
    if pin.container_stopped_at is not None:
        if (
            instance != closure.instance_id or bool(envelope.get("voided_unstarted")) != void
            or bool(envelope.get("settled_absent")) != absent
            or bool(envelope.get("allocation_refused")) != refused
        ):
            raise ObservationPending("Container stop replay does not match the settled reservation")
        return
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
    if journal is None:
        raise ObservationPending("Container application ownership is unavailable")
    if isinstance(closure, (VoidContainerStart, AbsentContainerStart)):
        if worker.state not in ("reserved", "starting", "draining", "lost", "retired") or instance not in (
            None, closure.instance_id,
        ) or worker.remote_hello_sequence:
            raise ObservationPending("Container absence does not match the unstarted reservation")
        try:
            deadline = datetime.fromisoformat(envelope["start_deadline"])
            closed_at_ms = closure.voided_at_ms if isinstance(closure, VoidContainerStart) else closure.closed_at_ms
            when = datetime.fromtimestamp(closed_at_ms / 1000, UTC)
            now = await db.scalar(select(func.clock_timestamp()))
            if (
                now is None or deadline.tzinfo is None or not pin.container_reserved_at < deadline
                or type(closure.start_deadline_ms) is not int or type(closed_at_ms) is not int
                or closure.start_deadline_ms != int(deadline.timestamp() * 1000)
                or closed_at_ms < int(pin.container_reserved_at.timestamp() * 1000) or when > now
                or refused != (closed_at_ms < closure.start_deadline_ms)
                or (
                    isinstance(closure, AbsentContainerStart) and (
                        type(closure.authorized_at_ms) is not int
                        or not int((pin.container_reserved_at - CONTAINER_CLOCK_SKEW).timestamp() * 1000)
                        <= closure.authorized_at_ms < closure.start_deadline_ms
                        or closed_at_ms < closure.start_deadline_ms + BOOT_OBSERVATION_MS
                    )
                )
            ):
                raise ValueError
        except (KeyError, OSError, TypeError, ValueError, OverflowError):
            raise ObservationPending("Container absence does not witness its expired start deadline") from None
        when = max(when, pin.container_reserved_at)
        if await db.scalar(select(exists().where(ExecutionWorkerClaim.worker_id == worker.id))):
            raise ObservationPending("An unstarted Container cannot own a claim")
        await rest.observe_no_placement(token, closure.application, closure.instance_id)
        pin.container_envelope = {
            **envelope, "provider_instance_id": closure.instance_id,
            ("voided_unstarted" if void else "settled_absent"): True,
            **({"allocation_refused": True} if refused else {}),
        }
        pin.container_stopped_at = when
        worker.state = "retired"
        # Without a start or a claim the original dispatch still belongs to the
        # canonical pending queue. Dropping the placement pointer is sufficient.
        worker.reserved_for_dispatch_id = None
        worker.ended_at = worker.capacity_resource_deleted_at = when
        await db.flush()
        return
    if (
        worker.state not in ("draining", "lost", "retired")
        or instance != closure.instance_id or envelope.get("prepared_image") != closure.image
    ):
        raise ObservationPending("Container stop does not match the prepared placement")
    if await db.scalar(select(exists().where(
        ExecutionWorkerClaim.worker_id == worker.id, ExecutionWorkerClaim.subject_id == Execution.id,
        Execution.status.in_((ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING)),
    ))):
        raise ObservationPending("Container still owns a live execution claim")
    stop = await rest.observe_instance_stop(
        token, closure.application, instance_id=closure.instance_id,
        image=closure.image, not_before=closure.last_start_effect_at,
    )
    if (
        stop.started_at < pin.container_reserved_at - CONTAINER_CLOCK_SKEW
        or stop.started_at != closure.placement_started_at
    ):
        raise ObservationPending("Container stop does not match the witnessed start")
    envelope["observed_started_at"] = stop.started_at.isoformat()
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
    pin.container_stopped_at = max(stop.stopped_at, pin.container_reserved_at)
    worker.state = "retired"
    worker.ended_at = stop.stopped_at
    await db.flush()
