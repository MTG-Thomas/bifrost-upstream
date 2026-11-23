"""Watch database-owned Workers scopes alongside the configured primary."""

import asyncio
import logging
import os
import time
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
from pydantic import SecretStr
from shared.models import CloudflareCapacityPolicy, CloudflarePlacementCode
from sqlalchemy import and_, func, select, update

from src.config import Settings
from src.core.database import get_db_context
from src.jobs.platform.cloudflare_capacity import (
    RECONCILE_INTERVAL_SECONDS,
    schedule_cloudflare_reconcile,
)
from src.models.enums import ExecutionStatus
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.models.orm.work_dispatches import WorkDispatch
from src.scheduler.leadership import SchedulerLeadershipLease
from src.services.kubernetes_control import may_manage_jobs
from src.services.kubernetes_execution import load_kubernetes_policy
from src.services.pool_cadence import CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS
from src.services.work_dispatch import (
    WAKE_CLOUDFLARE_CAPACITY,
    WAKE_WORKFLOW,
    listen_for_pool_wake,
    wait_for_next_pass,
)
from src.services.worker_pool.cloudflare_admission import (
    ManagedCloudflareScope,
    PrimaryPool,
    workers_release_matches,
)
from src.services.worker_pool.cloudflare_canary import active_cloudflare_canary
from src.services.worker_pool.cloudflare_containers_admission import ContainerAdmissionRefused, reserve_container
from src.services.worker_pool.cloudflare_containers_control import Receipt, reconcile_container_stop, start_container
from src.services.worker_pool.cloudflare_containers_egress import read_verified_egress
from src.services.worker_pool.cloudflare_containers_policy import CONTAINERS_SCOPE, container_deadline_sql
from src.services.worker_pool.cloudflare_containers_release import VerifiedContainerRelease, load_containers_release
from src.services.worker_pool.cloudflare_containers_rest import ContainersRest
from src.services.worker_pool.cloudflare_containers_settlement import BOOT_OBSERVATION_MS
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    load_account_token,
    open_credential,
)
from src.services.worker_pool.cloudflare_provisioning import ObservationPending
from src.services.worker_pool.cloudflare_release import load_workers_release
from src.services.worker_pool.cloudflare_spend import (
    capacity_request_reservation,
    cloudflare_usage_within_limit,
    prune_spend_receipt_keys,
)
from src.services.worker_pool.cloudflare_status import PrimaryCause, record_controller_observation
from src.services.worker_pool.controller import (
    RECONCILE_SECONDS,
    SWEEP_SECONDS,
    _withdraw_live,
)
from src.services.worker_pool.eligibility import cloudflare_dispatch_sql
from src.services.worker_pool.external_client import (
    CapacityProtocolError,
    ExternalCapacityClient,
)
from src.services.worker_pool.external_controller import ExternalWorkerPoolController
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    LINUX_RUNTIME_PROFILE,
    WORKER_DRAIN_GRACE_SECONDS,
    cloudflare_slots,
)

logger = logging.getLogger(__name__)

LAUNCH_AGE = timedelta(seconds=CLOUDFLARE_WORKERS_LAUNCH_AGE_SECONDS)


async def _next_launch_age(since: float) -> tuple[datetime, float] | None:
    """Find the first compatible dispatch whose age could change this pass."""
    elapsed = timedelta(seconds=max(0.0, time.monotonic() - since))
    async with get_db_context() as db:
        queued = select(func.min(WorkDispatch.created_at)).where(
            cloudflare_dispatch_sql(),
            WorkDispatch.assigned_worker_id.is_(None),
            WorkDispatch.created_at > func.now() - (elapsed + LAUNCH_AGE),
        ).scalar_subquery()
        database_now, queued_at = (await db.execute(select(func.now(), queued))).one()
    if queued_at is None:
        return None
    deadline = queued_at + LAUNCH_AGE
    return deadline, (deadline - database_now).total_seconds()


def _schedule_launch_wake(wake: asyncio.Event, delay: float) -> asyncio.TimerHandle:
    # Feed the same coalescing event as the Redis workflow and capacity hints.
    # The next pass still observes spacing and rechecks admission and demand.
    loop = asyncio.get_running_loop()
    # asyncio timers may run up to one clock-resolution tick early.
    return loop.call_later(max(0.0, delay) + time.get_clock_info("monotonic").resolution, wake.set)


async def _primary_pool(
    settings: Settings, http: httpx.AsyncClient, *, kubernetes: bool, shared_cap: int | None,
) -> tuple[PrimaryPool | None, PrimaryCause | None]:
    async with get_db_context() as db:
        limit = (await load_kubernetes_policy(db)).max_workers
    if shared_cap is not None:
        limit = min(limit, shared_cap)
    if settings.worker_capacity_url and settings.worker_capacity_token:
        client = ExternalCapacityClient(
            settings.worker_capacity_url, settings.worker_capacity_token.get_secret_value(), http=http,
        )
        profile = await client.profile()
        if profile.runtime_profile != LINUX_RUNTIME_PROFILE:
            return None, "profile_mismatch"
        return PrimaryPool("external", profile.scope, min(limit, profile.max_workers)), None
    if kubernetes:
        return PrimaryPool("job", ceiling=limit), None
    # A pod without Job access cannot infer that another replica's cluster
    # primary is a static cohort.
    return (None, "unreachable") if os.environ.get("KUBERNETES_SERVICE_HOST") else (PrimaryPool("static"), None)


async def _installations() -> list[ManagedCloudflareScope]:
    async with get_db_context() as db:
        rows = await db.execute(
            select(CloudflareInstallation.id, CloudflareCapacityScope.scope, CloudflareCapacityScope.deployment_revision)
            .join(CloudflareCapacityScope, CloudflareCapacityScope.installation_id == CloudflareInstallation.id)
            .where(
                CloudflareInstallation.retired_at.is_(None),
                CloudflareCapacityScope.scope.in_(("workers", CONTAINERS_SCOPE)),
            )
        )
        return [ManagedCloudflareScope(*row) for row in rows.tuples()]


async def _reconcile_containers(
    binding: ManagedCloudflareScope, settings: Settings, http: httpx.AsyncClient, controller_id: UUID,
    releases: dict[ManagedCloudflareScope, VerifiedContainerRelease], *, kubernetes: bool,
) -> None:
    adapter = ContainersRest(http)
    pin = CloudflareWorkerReservation
    async with get_db_context() as db:
        installation = await db.get(CloudflareInstallation, binding.installation_id)
        if installation is None:
            return
        management = await load_account_token(db, installation)
        now = await db.scalar(select(func.clock_timestamp()))
        assert now is not None
        # Preserve the first observation across scheduler ownership changes.
        await db.execute(update(pin).where(
            pin.installation_id == binding.installation_id, pin.scope == CONTAINERS_SCOPE,
            pin.deployment_revision == binding.revision, pin.cleanup_started_at.is_(None),
            pin.container_stopped_at.is_(None),
            pin.worker_id.in_(select(ExecutionWorker.id).where(ExecutionWorker.state == "draining")),
        ).values(cleanup_started_at=now))
        live_claim = select(ExecutionWorkerClaim.worker_id).join(
            Execution, Execution.id == ExecutionWorkerClaim.subject_id,
        ).where(
            ExecutionWorkerClaim.worker_id == ExecutionWorker.id,
            Execution.status.in_((ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING)),
        ).correlate(ExecutionWorker).exists()
        existing = (await db.execute(select(
            pin.worker_id, pin.container_envelope, pin.canary_job_id, ExecutionWorker.state, container_deadline_sql(),
            pin.cleanup_started_at, (ExecutionWorker.active_runs > 0) | live_claim,
        ).join(ExecutionWorker, ExecutionWorker.id == pin.worker_id).where(
            pin.installation_id == binding.installation_id, pin.scope == CONTAINERS_SCOPE,
            pin.deployment_revision == binding.revision,
            ExecutionWorker.capacity_resource_deleted_at.is_(None), ~active_cloudflare_canary(ExecutionWorker.id),
        ))).tuples().all()
        admitting = (
            installation.desired_state == "enabled" and installation.admitted_revision == binding.revision
            and installation.containers_ready_revision == binding.revision
        )
        verified = read_verified_egress(installation, now) if admitting else None
        release_digest = installation.containers_release_digest
        primary_cap = CloudflareCapacityPolicy.model_validate(installation.policy["limits"]).shared_managed_worker_cap

    async def retire(worker_id: UUID) -> None:
        async with get_db_context() as db:
            await db.execute(update(ExecutionWorker).where(
                ExecutionWorker.id == worker_id, ExecutionWorker.state.in_(("reserved", "starting", "ready")),
            ).values(state="draining"))
        try:
            await reconcile_container_stop(http, adapter, management, worker_id)
        except ObservationPending:
            pass  # A readback may still be settling. Revisit at the next pass.

    receipt_limit = False
    for worker_id, envelope, canary_job_id, worker_state, deadline, drain_started, busy in existing:
        if envelope is None:
            continue
        start_deadline = datetime.fromisoformat(envelope["start_deadline"])
        if (
            canary_job_id is not None or worker_state in ("lost", "retired") or deadline <= now
            or (worker_state == "draining" and (not busy or (
                drain_started is not None and now >= drain_started + timedelta(seconds=WORKER_DRAIN_GRACE_SECONDS)
            )))
            or (worker_state in ("reserved", "starting") and (
                not admitting or now >= start_deadline + timedelta(milliseconds=BOOT_OBSERVATION_MS)
            ))
        ):
            await retire(worker_id)
        elif worker_state in ("reserved", "starting") and now < start_deadline:
            receipt = await _start_container_reservation(http, adapter, management, worker_id)
            receipt_limit |= receipt is not None and receipt.status == "receipt-limit"

    if verified is None:
        await record_controller_observation(binding)
        return
    release = releases.get(binding)
    if release is None or release.digest != release_digest:
        release = await load_containers_release(http, load_workers_release())
        if release.digest != release_digest:
            return
        releases[binding] = release
    primary, cause = await _primary_pool(settings, http, kubernetes=kubernetes, shared_cap=primary_cap)
    if primary is None or receipt_limit:
        await record_controller_observation(
            binding, primary=primary, cause=cause, placement_code="container_receipt_limit" if receipt_limit else None,
        )
        return
    async with get_db_context() as db:
        reserved = await reserve_container(
            db, installation_id=binding.installation_id, controller_id=controller_id,
            release=release, primary=primary, verified_egress=verified,
        )
    if isinstance(reserved, ContainerAdmissionRefused):
        await record_controller_observation(
            binding, primary=primary, cause=cause, next_eligible_at=reserved.retry_at,
            placement_code="container_allocation_cooldown" if reserved.code == "CF_CONTAINERS_COOLDOWN" else "cleanup_pending",
        )
        return
    receipt = None
    if reserved is not None:
        receipt = await _start_container_reservation(http, adapter, management, reserved.worker_id)
    placement: dict[str, CloudflarePlacementCode] = {
        "allocation-refused": "container_allocation_cooldown", "receipt-limit": "container_receipt_limit",
    }
    await record_controller_observation(
        binding, primary=primary, cause=cause, placement_code=placement.get(receipt.status) if receipt else None,
    )


async def _start_container_reservation(
    http: httpx.AsyncClient, adapter: ContainersRest, management: SecretStr, worker_id: UUID,
) -> Receipt | None:
    receipt = await start_container(http, worker_id)
    if receipt is not None and receipt.status == "allocation-refused":
        try:
            await reconcile_container_stop(http, adapter, management, worker_id, refusal=receipt)
        except ObservationPending:
            pass
    return receipt


async def _reconcile_scope(
    binding: ManagedCloudflareScope, settings: Settings, http: httpx.AsyncClient,
    controllers: dict[ManagedCloudflareScope, ExternalWorkerPoolController],
    *, kubernetes: bool,
) -> None:
    installation_id, name = binding.installation_id, binding.name
    async with get_db_context() as db:
        row = (await db.execute(
            select(CloudflareInstallation, CloudflareCapacityScope, CloudflareCredential)
            .join(CloudflareCapacityScope, CloudflareCapacityScope.installation_id == CloudflareInstallation.id)
            .join(CloudflareCredential, and_(
                CloudflareCredential.installation_id == CloudflareCapacityScope.installation_id,
                CloudflareCredential.purpose == "scope_bearer",
                CloudflareCredential.scope == CloudflareCapacityScope.scope,
                CloudflareCredential.version == CloudflareCapacityScope.bearer_version,
            ))
            .where(
                CloudflareInstallation.id == installation_id, CloudflareInstallation.retired_at.is_(None),
                CloudflareCapacityScope.scope == name, CloudflareCapacityScope.deployment_revision == binding.revision,
            )
        )).one_or_none()
        if row is None:
            return
        installation, scope, credential = row
        policy = CloudflareCapacityPolicy.model_validate(installation.policy["limits"])
        bearer = open_credential(
            CredentialRef(installation_id, installation.account_id, "scope_bearer", name, scope.bearer_version),
            credential,
        )
        endpoint = scope.endpoint
        admitting = installation.admitted_revision == scope.deployment_revision and bool(await db.scalar(
            select(cloudflare_usage_within_limit() & workers_release_matches()).where(CloudflareInstallation.id == installation_id),
        ))

    client = ExternalCapacityClient(
        endpoint, bearer.get_secret_value(), http=http,
        reserve_request=capacity_request_reservation(installation_id, scope.capacity_cpu_ms),
    )
    controller = controllers.get(binding)
    if controller is None:
        profile = await client.profile()
        if (
            profile.scope != name or profile.protocol_version != 2
            or profile.runtime_profile != CLOUDFLARE_PYTHON_RUNTIME_PROFILE
        ):
            raise CapacityProtocolError("Managed Cloudflare endpoint has the wrong scope or protocol")
        controller = ExternalWorkerPoolController(client, settings, profile)
        controller.managed_cloudflare_scope = binding
        controllers[binding] = controller
    if controller.client.endpoint != client.endpoint:
        raise CapacityProtocolError("Managed Cloudflare endpoint changed within a deployment revision")
    controller.client = client
    matches = (
        controller.profile.max_workers == policy.workers.launcher_cap
        and cloudflare_slots(controller.profile.memory_limit_bytes) == policy.workers.slots_per_launcher
        and policy.workers.max_lifetime_seconds <= controller.profile.max_lifetime_seconds
    )
    controller.burst_launch_allowed = matches and admitting
    cause: PrimaryCause | None = "profile_mismatch" if not matches else "not_admitting" if not admitting else None
    controller.cloudflare_primary = None
    if controller.burst_launch_allowed:
        try:
            controller.cloudflare_primary, cause = await _primary_pool(
                settings, http, kubernetes=kubernetes, shared_cap=policy.shared_managed_worker_cap,
            )
        except (CapacityProtocolError, httpx.HTTPError) as exc:
            cause = "profile_mismatch" if isinstance(exc, CapacityProtocolError) else "unreachable"
            logger.warning("Primary worker capacity is unavailable for Cloudflare placement")
        if controller.cloudflare_primary is None:
            controller.burst_launch_allowed = False
    if await controller.reachability.check():
        await controller.reconcile()
        primary = controller.cloudflare_primary
        if primary is not None:
            async with get_db_context() as db:
                cohort = list(await db.scalars(select(ExecutionWorker).where(
                    ExecutionWorker.runtime_profile == LINUX_RUNTIME_PROFILE,
                    ExecutionWorker.form == primary.form, ExecutionWorker.capacity_scope == primary.scope,
                    ExecutionWorker.state.in_(("reserved", "starting", "ready", "draining")),
                )))
            state = primary.capacity_state(cohort, datetime.now(UTC))
            cause = None if state == "saturated" else state
        await record_controller_observation(binding, primary=primary, cause=cause)
    else:
        await record_controller_observation(binding, cause="unreachable")


async def cloudflare_supervisor_loop(
    stop_event: asyncio.Event, settings: Settings, *, lease: SchedulerLeadershipLease,
) -> None:
    controllers: dict[ManagedCloudflareScope, ExternalWorkerPoolController] = {}
    container_releases: dict[ManagedCloudflareScope, VerifiedContainerRelease] = {}
    container_controller_id = uuid4()
    next_inventory = 0.0
    async with httpx.AsyncClient(trust_env=False) as http:
        wake = asyncio.Event()
        listener = asyncio.create_task(listen_for_pool_wake(
            settings.redis_url, {WAKE_CLOUDFLARE_CAPACITY: wake, WAKE_WORKFLOW: wake},
        ))
        age_deadline = None
        age_wake = None
        try:
            while not stop_event.is_set() and lease.holds_lease():
                started = time.monotonic()
                try:
                    async with asyncio.timeout(lease.seconds_remaining()):
                        scopes = await _installations()
                        kubernetes = await may_manage_jobs(settings) if scopes else False
                        for binding in list(controllers):
                            if binding not in scopes:
                                await _withdraw_live(controllers.pop(binding).id)
                        for binding in list(container_releases):
                            if binding not in scopes:
                                container_releases.pop(binding)
                        for binding in scopes:
                            if not lease.holds_lease():
                                return
                            try:
                                if binding.name == CONTAINERS_SCOPE:
                                    await _reconcile_containers(
                                        binding, settings, http, container_controller_id, container_releases,
                                        kubernetes=kubernetes,
                                    )
                                else:
                                    await _reconcile_scope(
                                        binding, settings, http, controllers, kubernetes=kubernetes,
                                    )
                            except (CapacityProtocolError, httpx.HTTPError) as exc:
                                await record_controller_observation(
                                    binding, unavailable=True,
                                    cause="profile_mismatch" if isinstance(exc, CapacityProtocolError) else "unreachable",
                                )
                                logger.warning("Managed Cloudflare scope reconciliation failed", exc_info=True)
                            except Exception:
                                logger.warning("Managed Cloudflare scope reconciliation failed", exc_info=True)
                except TimeoutError:
                    await wait_for_next_pass(stop_event, wake, since=started, spacing=RECONCILE_SECONDS, sweep=SWEEP_SECONDS)
                    continue
                except Exception:
                    logger.warning("Could not read managed Cloudflare scopes", exc_info=True)
                if started >= next_inventory and lease.holds_lease():
                    next_inventory = started + RECONCILE_INTERVAL_SECONDS
                    try:
                        async with asyncio.timeout(lease.seconds_remaining()), get_db_context() as db:
                            await schedule_cloudflare_reconcile(db, now=datetime.now(UTC))
                    except Exception:
                        logger.warning("Could not schedule Cloudflare inventory reconciliation", exc_info=True)
                    if lease.holds_lease():
                        try:
                            async with asyncio.timeout(lease.seconds_remaining()), get_db_context() as db:
                                await prune_spend_receipt_keys(db)
                        except Exception:
                            logger.warning("Could not prune Cloudflare spend history", exc_info=True)
                age = None
                try:
                    if lease.holds_lease() and any(c.burst_launch_allowed for c in controllers.values()):
                        async with asyncio.timeout(lease.seconds_remaining()):
                            age = await _next_launch_age(started)
                    # An ordinary wake can run a pass before the deadline.
                    # Keep the existing timer armed when the same row is due.
                    deadline = age[0] if age is not None else None
                    if deadline != age_deadline:
                        if age_wake is not None:
                            age_wake.cancel()
                        age_wake = _schedule_launch_wake(wake, age[1]) if age is not None else None
                        age_deadline = deadline
                except Exception:
                    logger.warning("Could not schedule Cloudflare launch-age wake", exc_info=True)
                await wait_for_next_pass(stop_event, wake, since=started, spacing=RECONCILE_SECONDS, sweep=SWEEP_SECONDS)
        finally:
            if age_wake is not None:
                age_wake.cancel()
            listener.cancel()
            with suppress(asyncio.CancelledError):
                await listener
            for controller in controllers.values():
                await _withdraw_live(controller.id)
