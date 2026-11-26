"""Reconcile one Container incarnation against the scope and management inventory."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db_context
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.work_dispatches import WorkDispatch
from src.services.worker_pool.cloudflare_admission import cloudflare_claim_allowed
from src.services.worker_pool.cloudflare_containers_auth import request_container_control
from src.services.worker_pool.cloudflare_containers_policy import CONTAINERS_SCOPE, container_provider_pin
from src.services.worker_pool.cloudflare_containers_rest import ContainerStartUnverified, ContainersRest
from src.services.worker_pool.cloudflare_containers_settlement import (
    BOOT_OBSERVATION_MS,
    AbsentContainerStart,
    ClosedContainerStart,
    EnrolledContainerStop,
    VoidContainerStart,
    settle_container_stop,
)
from src.services.worker_pool.cloudflare_credentials import CredentialRef, load_managed_control_key, open_credential
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationPending,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
)


class Receipt(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    status: Literal[
        "allocation-refused", "void", "absent", "closed", "conflict", "invalid", "pending", "observed",
        "reserved", "retired", "receipt-limit", "enrolled-stop",
    ]
    instance: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    uid: str | None = Field(default=None, pattern=r"^[a-f0-9]{32}$")
    authorizedAt: int | None = None
    startedAt: int | None = None
    closedAt: int | None = None
    enrolledStopAt: int | None = None
    startDeadline: int | None = None
    voidedAt: int | None = None
    outboundBytes: int | None = None
    meterComplete: Literal[False] | None = None


@dataclass(frozen=True, slots=True)
class ContainerControlBinding:
    application: ResourceObservation
    endpoint: str
    scope_key: SecretStr
    control_key: SecretStr
    pin: dict[str, str | int]
    prepared_image: str

    async def request(
        self, http: httpx.AsyncClient,
        operation: Literal["start", "stop", "void", "observe-start", "close-absent", "close", "retire"],
        **payload: int,
    ) -> Receipt:
        response = await request_container_control(
            http, self.endpoint, self.scope_key.get_secret_value(), self.control_key.get_secret_value(),
            operation=operation, pin=self.pin, now_ms=int(datetime.now(UTC).timestamp() * 1000), payload=payload,
        )
        return Receipt.model_validate_json(response)


async def load_container_control(db: AsyncSession, worker_id: UUID) -> ContainerControlBinding:
    row = (await db.execute(select(
        CloudflareWorkerReservation, ExecutionWorker.capacity_resource_uid,
        CloudflareInstallation.account_id, CloudflareCapacityScope,
    ).join(ExecutionWorker, ExecutionWorker.id == CloudflareWorkerReservation.worker_id).join(
        CloudflareInstallation, CloudflareInstallation.id == CloudflareWorkerReservation.installation_id,
    ).join(CloudflareCapacityScope, (
        (CloudflareCapacityScope.installation_id == CloudflareWorkerReservation.installation_id)
        & (CloudflareCapacityScope.scope == CloudflareWorkerReservation.scope)
        & (CloudflareCapacityScope.deployment_revision == CloudflareWorkerReservation.deployment_revision)
    )).where(
        CloudflareWorkerReservation.worker_id == worker_id,
        CloudflareWorkerReservation.scope == CONTAINERS_SCOPE,
    ))).one_or_none()
    if row is None or row.capacity_resource_uid is None:
        raise ObservationPending("Container has no owned control scope")
    reservation, uid, account_id, scope = row
    name = f"bifrost-{reservation.installation_id.hex}-r{reservation.deployment_revision}"
    owned = {resource.kind: resource for resource in await db.scalars(select(CloudflareOwnedResource).where(
        CloudflareOwnedResource.installation_id == reservation.installation_id,
        CloudflareOwnedResource.account_id == account_id,
        CloudflareOwnedResource.scope == CONTAINERS_SCOPE,
        CloudflareOwnedResource.kind.in_(("container_application", "container_image")),
        CloudflareOwnedResource.target_name == name,
        CloudflareOwnedResource.desired_revision == reservation.deployment_revision,
        CloudflareOwnedResource.artifact_digest == reservation.container_release,
        CloudflareOwnedResource.state == "present",
    ))}
    app, image = owned.get("container_application"), owned.get("container_image")
    wire = container_provider_pin(reservation, uid)
    expected_tag = f"registry.cloudflare.com/{account_id}/{name}:release"
    if (
        app is None or not app.provider_id or not app.provider_version
        or image is None or image.provider_id != expected_tag or image.provider_version != wire["image"]
        or wire["credentialVersion"] != scope.bearer_version
    ):
        raise ObservationPending("Container control scope lacks the pinned application and image")
    ref = CredentialRef(reservation.installation_id, account_id, "scope_bearer", CONTAINERS_SCOPE, scope.bearer_version)
    credential = await db.get(CloudflareCredential, (ref.installation_id, ref.purpose, ref.scope, ref.version))
    if credential is None:
        raise ObservationPending("Container control scope credential is unavailable")
    application = ResourceObservation(ResourceIntent(
        ResourceTarget(ref.installation_id, account_id, CONTAINERS_SCOPE, app.kind, name),
        app.effect_id, app.desired_revision, app.artifact_digest,
    ), app.provider_id, app.provider_version)
    return ContainerControlBinding(
        application, scope.endpoint, open_credential(ref, credential),
        SecretStr(await load_managed_control_key(db, reservation, uid)),
        wire, f"{expected_tag.removesuffix(':release')}@{wire['image']}",
    )


async def reconcile_container_stop(
    http: httpx.AsyncClient, adapter: ContainersRest, management: SecretStr, worker_id: UUID,
    *, refusal: Receipt | None = None,
) -> bool:
    """Return true only once Bifrost settlement and registry retirement agree."""
    async with get_db_context() as db:
        binding = await load_container_control(db, worker_id)
        pin = await db.get(CloudflareWorkerReservation, worker_id)
        assert pin is not None and pin.container_reserved_at is not None and pin.container_envelope is not None
        worker = await db.get(ExecutionWorker, worker_id)
        if worker is None:
            raise ObservationPending("Container incarnation is missing")
        if worker.capacity_resource_deleted_at is not None:
            return True
        envelope = dict(pin.container_envelope)
        start = pin.container_reserved_at
        enrolled = worker.remote_hello_sequence > 0

    uid = str(binding.pin["uid"])
    deadline = binding.pin["startDeadline"]
    hard_deadline = binding.pin["deadline"]
    assert isinstance(deadline, int) and isinstance(hard_deadline, int)
    closed_at = envelope.get("registry_closed_at")
    if closed_at is None:
        stopped = await binding.request(http, "stop")
        if stopped.instance is None or stopped.status not in ("conflict", "pending"):
            raise ObservationPending("Container stop has no exact actor receipt")
        instance = stopped.instance
        void = refusal if refusal is not None else await binding.request(http, "void")
        if void.status in ("void", "allocation-refused"):
            if void.uid != uid or void.instance != instance or void.startDeadline != deadline or void.voidedAt is None:
                raise ObservationPending("Container void does not match its reservation")
            closure: ClosedContainerStart | VoidContainerStart | AbsentContainerStart | EnrolledContainerStop = VoidContainerStart(
                binding.application, worker_id, uid, instance, deadline, void.voidedAt,
                allocation_refused=void.status == "allocation-refused",
            )
        else:
            fence = await binding.request(http, "close")
            if fence.status == "pending" or (enrolled and fence.status == "absent"):
                placement = await adapter.observe_instance_outcome(
                    management, binding.application, instance_id=instance, image=binding.prepared_image, not_before=start,
                )
                if placement is None or placement.started_at is None or fence.status == "absent":
                    now_ms = int(datetime.now(UTC).timestamp() * 1000)
                    if enrolled:
                        if now_ms >= hard_deadline:
                            await binding.request(http, "close", enrolled_stop_at=now_ms)
                        return False
                    if now_ms < deadline + BOOT_OBSERVATION_MS:
                        return False
                    async with get_db_context() as db:
                        current = await db.get(ExecutionWorker, worker_id, with_for_update=True)
                        if current is None or current.remote_hello_sequence:
                            return False
                        # Close enrollment before the irreversible scope call.
                        # The ledger and registry still hold the reservation.
                        current.state = "retired"
                    # The active start ticket and the post-boot inventory read
                    # must agree on no start before this fence can close as absent.
                    await binding.request(http, "close-absent", observed_absent_at=now_ms)
                else:
                    observed = await binding.request(
                        http, "observe-start", started_at=int(placement.started_at.timestamp() * 1000),
                    )
                    if observed.status == "observed":
                        await binding.request(http, "close")
                # Reserve the next paced read on the next pass. The registry
                # retains its closed fence across controller or API restarts.
                return False
            if fence.status not in ("absent", "closed", "enrolled-stop"):
                return False
            if fence.instance != instance or fence.authorizedAt is None or fence.closedAt is None:
                raise ObservationPending("Container closure differs from its actor fence")
            closed_at = fence.closedAt
            if fence.status in ("absent", "enrolled-stop"):
                if fence.uid != uid or fence.startDeadline != deadline:
                    raise ObservationPending("Absent Container receipt differs from its start fence")
                if fence.status == "enrolled-stop":
                    if fence.enrolledStopAt is None:
                        raise ObservationPending("Enrolled Container closure lacks its independent stop observation")
                    closure = EnrolledContainerStop(
                        binding.application, worker_id, uid, instance, binding.prepared_image,
                        fence.authorizedAt, fence.enrolledStopAt,
                    )
                else:
                    closure = AbsentContainerStart(
                        binding.application, worker_id, uid, instance, deadline, fence.authorizedAt, closed_at,
                        binding.prepared_image,
                    )
            else:
                if fence.startedAt is None:
                    raise ObservationPending("Closed Container fence lacks the observed start")
                closure = ClosedContainerStart(
                    binding.application, worker_id, uid, instance, binding.prepared_image,
                    datetime.fromtimestamp(fence.authorizedAt / 1000, UTC),
                    datetime.fromtimestamp(fence.startedAt / 1000, UTC),
                )

        if isinstance(closure, (ClosedContainerStart, EnrolledContainerStop)):
            async with get_db_context() as db:
                updated = await db.get(CloudflareWorkerReservation, worker_id, with_for_update=True)
                assert updated is not None and updated.container_envelope is not None
                if updated.container_stopped_at is None:
                    existing_instance = updated.container_envelope.get("provider_instance_id")
                    existing_image = updated.container_envelope.get("prepared_image")
                    if existing_instance not in (None, instance) or existing_image not in (None, binding.prepared_image):
                        raise ObservationPending("Container placement differs from its recorded actor")
                    updated.container_envelope = {
                        **updated.container_envelope, "provider_instance_id": instance, "prepared_image": binding.prepared_image,
                    }
        missing_start = False
        async with get_db_context() as db:
            try:
                await settle_container_stop(db, adapter, token=management, closure=closure)
            except ContainerStartUnverified:
                # A previously observed fence can outlive the provider's
                # placement record. Confirm a stopped enrolled actor after
                # its hard deadline without claiming an unstarted placement.
                if not enrolled:
                    raise
                missing_start = True
            else:
                if closed_at is not None:
                    updated = await db.get(CloudflareWorkerReservation, worker_id)
                    assert updated is not None and updated.container_envelope is not None
                    updated.container_envelope = {**updated.container_envelope, "registry_closed_at": closed_at}
        if missing_start:
            now_ms = int(datetime.now(UTC).timestamp() * 1000)
            if now_ms >= hard_deadline:
                await binding.request(http, "close", enrolled_stop_at=now_ms)
            return False
    if closed_at is not None:
        receipt = await binding.request(http, "retire", closed_at=closed_at)
        if receipt.status != "retired":
            return False
        async with get_db_context() as db:
            worker = await db.get(ExecutionWorker, worker_id, with_for_update=True)
            if worker is None or worker.capacity_resource_uid != uid:
                raise ObservationPending("Registry retirement differs from the settled Container")
            worker.capacity_resource_deleted_at = await db.scalar(select(func.clock_timestamp()))
    return True


async def start_container(http: httpx.AsyncClient, worker_id: UUID) -> Receipt | None:
    """Send the launch only while the ordinary reservation can still claim its dispatch."""
    async with get_db_context() as db:
        if not await cloudflare_claim_allowed(db, worker_id):
            return None
        worker = await db.get(ExecutionWorker, worker_id, with_for_update=True)
        if worker is None or worker.state not in ("reserved", "starting") or worker.reserved_for_dispatch_id is None:
            return None
        dispatch = await db.get(WorkDispatch, worker.reserved_for_dispatch_id)
        if dispatch is None or dispatch.assigned_worker_id is not None:
            return None
        binding = await load_container_control(db, worker_id)
        now = await db.scalar(select(func.clock_timestamp()))
        assert now is not None and isinstance(binding.pin["startDeadline"], int)
        if int(now.timestamp() * 1000) >= binding.pin["startDeadline"]:
            return None
        worker.state = "starting"
        # The guest cannot enroll before the worker is starting. Commit that
        # durable fence before sending the idempotent incarnation request.
    async with get_db_context() as db:
        if not await cloudflare_claim_allowed(db, worker_id):
            return None
        receipt = await binding.request(http, "start")
    if receipt.instance is None:
        raise ObservationPending("Cloudflare Container start lacks its actor identity")
    return receipt
