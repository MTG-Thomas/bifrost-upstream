"""Watch database-owned Workers scopes alongside the configured primary."""

import asyncio
import logging
import os
import time
from contextlib import suppress
from datetime import UTC, datetime

import httpx
from shared.models import CloudflareCapacityPolicy
from sqlalchemy import and_, select

from src.config import Settings
from src.core.database import get_db_context
from src.jobs.platform.cloudflare_capacity import (
    RECONCILE_INTERVAL_SECONDS,
    schedule_cloudflare_reconcile,
)
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
)
from src.scheduler.leadership import SchedulerLeadershipLease
from src.services.kubernetes_control import may_manage_jobs
from src.services.kubernetes_execution import load_kubernetes_policy
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
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    open_credential,
)
from src.services.worker_pool.cloudflare_spend import (
    capacity_request_reservation,
    cloudflare_usage_within_limit,
    prune_spend_receipt_keys,
)
from src.services.worker_pool.controller import (
    RECONCILE_SECONDS,
    SWEEP_SECONDS,
    _withdraw_live,
)
from src.services.worker_pool.external_client import (
    CapacityProtocolError,
    ExternalCapacityClient,
)
from src.services.worker_pool.external_controller import ExternalWorkerPoolController
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    LINUX_RUNTIME_PROFILE,
    cloudflare_slots,
)

logger = logging.getLogger(__name__)


async def _primary_pool(
    settings: Settings, http: httpx.AsyncClient, *, kubernetes: bool, shared_cap: int | None,
) -> PrimaryPool | None:
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
            return None
        return PrimaryPool("external", profile.scope, min(limit, profile.max_workers))
    if kubernetes:
        return PrimaryPool("job", ceiling=limit)
    # A pod without Job access cannot infer that another replica's cluster
    # primary is a static cohort.
    return None if os.environ.get("KUBERNETES_SERVICE_HOST") else PrimaryPool("static")


async def _installations() -> list[ManagedCloudflareScope]:
    async with get_db_context() as db:
        rows = await db.execute(
            select(CloudflareInstallation.id, CloudflareCapacityScope.scope, CloudflareCapacityScope.deployment_revision)
            .join(CloudflareCapacityScope, CloudflareCapacityScope.installation_id == CloudflareInstallation.id)
            .where(
                CloudflareInstallation.retired_at.is_(None),
                CloudflareCapacityScope.scope == "workers",
            )
        )
        return [ManagedCloudflareScope(*row) for row in rows.tuples()]


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
    controller.managed_max_lifetime_seconds = policy.workers.max_lifetime_seconds
    controller.burst_launch_allowed = (
        controller.profile.max_workers == policy.workers.launcher_cap
        and cloudflare_slots(controller.profile.memory_limit_bytes) == policy.workers.slots_per_launcher
        and policy.workers.max_lifetime_seconds <= controller.profile.max_lifetime_seconds
        and admitting
    )
    controller.cloudflare_primary = None
    if controller.burst_launch_allowed:
        try:
            controller.cloudflare_primary = await _primary_pool(
                settings, http, kubernetes=kubernetes, shared_cap=policy.shared_managed_worker_cap,
            )
        except (CapacityProtocolError, httpx.HTTPError):
            logger.warning("Primary worker capacity is unavailable for Cloudflare placement")
        if controller.cloudflare_primary is None:
            controller.burst_launch_allowed = False
    if await controller.reachability.check():
        await controller.reconcile()


async def cloudflare_supervisor_loop(
    stop_event: asyncio.Event, settings: Settings, *, lease: SchedulerLeadershipLease,
) -> None:
    controllers: dict[ManagedCloudflareScope, ExternalWorkerPoolController] = {}
    next_inventory = 0.0
    async with httpx.AsyncClient(trust_env=False) as http:
        wake = asyncio.Event()
        listener = asyncio.create_task(listen_for_pool_wake(
            settings.redis_url, {WAKE_CLOUDFLARE_CAPACITY: wake, WAKE_WORKFLOW: wake},
        ))
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
                        for binding in scopes:
                            if not lease.holds_lease():
                                return
                            try:
                                await _reconcile_scope(
                                    binding, settings, http, controllers, kubernetes=kubernetes,
                                )
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
                await wait_for_next_pass(stop_event, wake, since=started, spacing=RECONCILE_SECONDS, sweep=SWEEP_SECONDS)
        finally:
            listener.cancel()
            with suppress(asyncio.CancelledError):
                await listener
            for controller in controllers.values():
                await _withdraw_live(controller.id)
