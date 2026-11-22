"""Run a reserved, lease-bound Cloudflare launcher through the real worker ingress."""

import asyncio
import hmac
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
from shared.models import WorkerCapacityReservation
from sqlalchemy import and_, func, select, update
from sqlalchemy.sql.elements import ColumnElement

from src.core.database import get_db_context
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobRequiresAction,
)
from src.models.orm.cloudflare_capacity import (
    CloudflareInstallation,
    CloudflareUsageIncarnation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.platform_jobs import PlatformJob
from src.services.platform_job_lease import LeaseLost, hold_lease
from src.services.worker_ingress import seal_capacity_canary
from src.services.worker_ingress_probes import CanaryFailure, RuntimeCanary
from src.services.worker_pool.cloudflare_admission import ManagedCloudflareScope
from src.services.worker_pool.cloudflare_credentials import load_managed_control_key
from src.services.worker_pool.cloudflare_policy import cloudflare_workers_remain
from src.services.worker_pool.cloudflare_provisioning import (
    ObservationCheck,
    ObservationPending,
    OwnershipResource,
)
from src.services.worker_pool.cloudflare_spend import (
    compact_ledger_receipt,
    seal_launcher_meter,
)
from src.services.worker_pool.cloudflare_workers_rest import WorkersDeployment
from src.services.worker_pool.external_client import (
    CapacityProtocolCheck,
    CapacityProtocolError,
    ExternalCapacityClient,
)
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    WORKERS_DEADLINE_DRAIN_SECONDS,
    cloudflare_slots,
)

CANARY_DEADLINE_SECONDS = 45


def _active_lease() -> ColumnElement[bool]:
    pin = CloudflareWorkerReservation
    return and_(
        PlatformJob.id == pin.canary_job_id,
        PlatformJob.lease_token == pin.canary_lease_token,
        PlatformJob.status == "running",
        PlatformJob.lease_expires_at > func.now(),
    )


def active_cloudflare_canary(worker_id: ColumnElement[UUID]) -> ColumnElement[bool]:
    """Keep a live Enable reservation out of ordinary burst reconciliation."""
    return select(CloudflareWorkerReservation.worker_id).join(PlatformJob, _active_lease()).where(
        CloudflareWorkerReservation.worker_id == worker_id,
    ).exists()


def _proof(key: str, worker_id: UUID, lease_token: UUID, generation: int) -> str:
    subject = f"bifrost:cloudflare:sdk-canary:v1:{worker_id}:{lease_token}:{generation}"
    return hmac.digest(bytes.fromhex(key), subject.encode(), "sha256").hex()


async def cloudflare_sdk_canary_proof(
    worker_id: UUID, generation: int, *, received: str | None = None, acknowledged: bool = False,
) -> str | None:
    """Issue or acknowledge the one-purpose SDK credential for a signed session."""
    async with get_db_context() as db:
        pin = CloudflareWorkerReservation
        record = (await db.execute(select(pin, ExecutionWorker.capacity_resource_uid).join(
            PlatformJob, _active_lease(),
        ).join(ExecutionWorker, ExecutionWorker.id == pin.worker_id).join(
            CloudflareInstallation, CloudflareInstallation.id == pin.installation_id,
        ).where(
            pin.worker_id == worker_id,
            CloudflareInstallation.retired_at.is_(None),
            CloudflareInstallation.desired_state == "enabled",
            CloudflareInstallation.desired_revision == pin.deployment_revision,
            ExecutionWorker.state == "ready",
            ExecutionWorker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
            ExecutionWorker.remote_session_generation == generation,
            ExecutionWorker.remote_hello_sequence > 0,
            ExecutionWorker.remote_owner_lease_expires_at > func.now(),
        ).with_for_update(of=pin))).one_or_none()
        if record is None:
            return None
        binding, resource_uid = record
        if resource_uid is None or binding.canary_lease_token is None:
            return None
        key = await load_managed_control_key(db, binding, resource_uid)
        expected = seal_capacity_canary(
            worker_id, generation, _proof(key, worker_id, binding.canary_lease_token, generation)[:32],
        )
        if received is not None:
            if not hmac.compare_digest(expected, received):
                return None
            if generation != binding.canary_sdk_generation:
                binding.canary_ack_at = None
            binding.canary_sdk_at = datetime.now(UTC)
            binding.canary_sdk_generation = generation
        if acknowledged:
            if binding.canary_sdk_generation != generation:
                return None
            binding.canary_ack_at = datetime.now(UTC)
        return expected


async def record_canary_failure(worker_id: UUID, generation: int, failure: CanaryFailure) -> None:
    """Store only a redacted outcome on the current Enable job lease."""
    pin = CloudflareWorkerReservation
    worker = ExecutionWorker
    async with get_db_context() as db:
        job = await db.scalar(select(PlatformJob).where(
            select(pin.worker_id).join(worker, worker.id == pin.worker_id).where(
                pin.worker_id == worker_id, worker.remote_session_generation == generation,
                _active_lease(),
            ).correlate(PlatformJob).exists(),
        ).with_for_update())
        if job is not None:
            job.result = {
                **(job.result or {}),
                "canary_failure": {"worker_id": str(worker_id), "generation": generation, "reason": failure.as_result()},
            }


def _canary_session_failure(checkpoint: dict, worker_id: UUID, generation: int) -> CanaryFailure | None:
    record = checkpoint.get("canary_failure")
    if isinstance(record, dict) and record.get("worker_id") == str(worker_id) and record.get("generation") == generation:
        return CanaryFailure.from_result(record.get("reason"))
    return None


async def _retire_canary(client: ExternalCapacityClient, worker_id: UUID, resource_uid: str) -> bool:
    async with get_db_context() as db:
        matched = await db.scalar(update(ExecutionWorker).where(
            ExecutionWorker.id == worker_id, ExecutionWorker.capacity_resource_uid == resource_uid,
        ).values(state="retired", ended_at=datetime.now(UTC), end_reason="cloudflare_enable_canary")
            .returning(ExecutionWorker.id))
        if matched is None:
            return False
    try:
        observed = await client.get(worker_id, "workers")
        if observed is None or observed.resource_uid != resource_uid:
            return False
        if observed.state != "deleted":
            observed = await client.remove(worker_id, "workers", resource_uid)
        if observed is None or observed.resource_uid != resource_uid or observed.state != "deleted":
            return False
    except (httpx.HTTPError, CapacityProtocolError):
        return False
    async with get_db_context() as db:
        matched = await db.scalar(update(ExecutionWorker).where(
            ExecutionWorker.id == worker_id, ExecutionWorker.capacity_resource_uid == resource_uid,
            ExecutionWorker.state == "retired", ExecutionWorker.capacity_resource_deleted_at.is_(None),
        ).values(capacity_resource_deleted_at=func.clock_timestamp()).returning(ExecutionWorker.id))
        if matched is None:
            return await db.scalar(select(ExecutionWorker.id).where(
                ExecutionWorker.id == worker_id, ExecutionWorker.capacity_resource_uid == resource_uid,
                ExecutionWorker.state.in_(("retired", "lost")),
                ExecutionWorker.capacity_resource_deleted_at.is_not(None),
            )) is not None
        await seal_launcher_meter(db, worker_id, observed.usage)
    if observed.usage is not None and observed.usage.ledger_receipt_pending:
        try:
            await compact_ledger_receipt(
                worker_id, lambda: client.acknowledge_receipt(worker_id, "workers", resource_uid),
            )
        except (httpx.HTTPError, CapacityProtocolError):
            # The receipt is committed. The supervisor can retry compaction
            # after the canary job stops owning this worker.
            pass
    return True


async def run_workers_canary(
    context: PlatformJobContext, installation_id: UUID, revision: int,
    origin: str, client: ExternalCapacityClient, deployment: WorkersDeployment,
) -> RuntimeCanary:
    if origin != deployment.worker_ingress_origin:
        raise CapacityProtocolError("The canary ingress origin changed", CapacityProtocolCheck.INGRESS_ORIGIN)
    profile = await client.profile()
    if (
        profile.scope != "workers" or profile.protocol_version != 2
        or profile.worker_version != deployment.release.digest
        or profile.runtime_profile != CLOUDFLARE_PYTHON_RUNTIME_PROFILE
        or profile.max_workers != deployment.policy.launcher_cap
        or profile.max_lifetime_seconds != deployment.policy.max_lifetime_seconds
        or cloudflare_slots(profile.memory_limit_bytes) != deployment.policy.slots_per_launcher
    ):
        raise CapacityProtocolError(
            "The canary provider contract differs from the installed release", CapacityProtocolCheck.PROFILE_CONTRACT,
        )
    worker_id, resource_uid = uuid4(), uuid4().hex
    async with get_db_context() as db:
        try:
            await hold_lease(db, context.job_id, context.lease_token)
        except LeaseLost as exc:
            raise PlatformJobCancelled from exc
        row = await db.get(CloudflareInstallation, installation_id, with_for_update=True)
        if (
            row is None or row.desired_revision != revision or row.desired_state != "enabled"
            or row.retired_at is not None or row.control_root_version is None
            or row.admitted_revision is not None
        ):
            return RuntimeCanary(False, False, CanaryFailure("hello", "lease_changed"))
        if await cloudflare_workers_remain(db, row.id, before_revision=revision):
            raise PlatformJobRequiresAction("Wait for Cloudflare Workers to Retire", {"code": "CF_PRIOR_WORKERS_LIVE"})
        db.add(ExecutionWorker(
            id=worker_id, form="external", state="starting", execution_lane="normal",
            runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, supported_handlers=[],
            capacity_scope="workers", capacity_resource_uid=resource_uid, version=profile.worker_version,
            memory_limit_bytes=profile.memory_limit_bytes, remote_launcher_id=worker_id,
        ))
        await db.flush()
        lifetime = min(deployment.policy.max_lifetime_seconds, 120)
        reserved_at = await db.scalar(select(func.clock_timestamp()))
        assert reserved_at is not None
        pin = ManagedCloudflareScope(installation_id, "workers", revision).pin(
            worker_id, row.control_root_version, reserved_at=reserved_at, lifetime=lifetime,
            deadline_wire=any(
                binding.name == "BIFROST_LAUNCHER_METERS_PER_SECOND" and binding.type == "plain_text"
                for binding in deployment.release.manifest.workers["launcher"].bindings
            ),
        )
        pin.canary_job_id = context.job_id
        pin.canary_lease_token = context.lease_token
        db.add(pin)
        if pin.workers_deadline_wire:
            await db.flush()
            usage = await db.get(CloudflareUsageIncarnation, worker_id, with_for_update=True)
            assert usage is not None and pin.workers_lifetime_seconds is not None
            if usage.meter_bounds is not None and (rates := usage.meter_bounds.get("per_second_caps")) is not None:
                usage.meter_bounds = {
                    **usage.meter_bounds,
                    "caps": {name: rate * pin.workers_lifetime_seconds for name, rate in rates.items()},
                }
        key = await load_managed_control_key(db, pin, resource_uid)

    signed = scoped = False
    failure = None
    try:
        await client.prepare(WorkerCapacityReservation(
            worker_id=worker_id, worker_version=profile.worker_version, memory_limit_bytes=profile.memory_limit_bytes,
            max_lifetime_seconds=lifetime,
            drain_grace_seconds=WORKERS_DEADLINE_DRAIN_SECONDS,
        ), "workers", managed_identity=(resource_uid, key),
            **({"managed_deadline": pin.workers_deadline_at} if pin.workers_deadline_wire else {}),
        )
        async with get_db_context() as db:
            try:
                await hold_lease(db, context.job_id, context.lease_token)
            except LeaseLost as exc:
                raise PlatformJobCancelled from exc
            row = await db.get(CloudflareInstallation, installation_id, with_for_update={"read": True})
            if row is None or row.desired_revision != revision or row.desired_state != "enabled":
                return RuntimeCanary(False, False, CanaryFailure("hello", "lease_changed"))
            await client.start(worker_id, "workers", resource_uid)
        try:
            async with asyncio.timeout(CANARY_DEADLINE_SECONDS):
                while not scoped:
                    async with get_db_context() as db:
                        proof = (await db.execute(select(
                            ExecutionWorker.remote_hello_sequence, ExecutionWorker.remote_session_generation,
                            CloudflareWorkerReservation.canary_ack_at, CloudflareWorkerReservation.canary_sdk_generation,
                            PlatformJob.result,
                        ).join(
                            CloudflareWorkerReservation, CloudflareWorkerReservation.worker_id == ExecutionWorker.id,
                        ).join(PlatformJob, _active_lease()).where(ExecutionWorker.id == worker_id))).one_or_none()
                    if proof is None:
                        failure = CanaryFailure("hello", "lease_changed")
                        break
                    signed = proof.remote_hello_sequence > 0
                    scoped = (
                        signed and proof.canary_ack_at is not None
                        and proof.canary_sdk_generation == proof.remote_session_generation
                    )
                    if scoped:
                        failure = None
                        break
                    failure = _canary_session_failure(proof.result or {}, worker_id, proof.remote_session_generation)
                    if failure is not None and (failure.stage, failure.error_class) not in {
                        ("launcher", "connection_lost"), ("proof", "unavailable"),
                    }:
                        break
                    await asyncio.sleep(0.5)
        except TimeoutError:
            failure = failure or CanaryFailure("ack" if signed else "hello", "timeout")
    finally:
        retired = await _retire_canary(client, worker_id, resource_uid)
    if not retired and signed and scoped:
        raise ObservationPending(
            "The runtime canary has no verified deletion receipt",
            OwnershipResource.CANARY_LAUNCHER, ObservationCheck.CANARY_RETIREMENT,
        )
    return RuntimeCanary(signed, scoped, failure)
