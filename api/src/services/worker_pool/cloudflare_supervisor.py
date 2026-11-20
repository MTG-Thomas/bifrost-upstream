"""Watch database-owned Workers scopes alongside the configured primary."""

import asyncio
import logging
import os
from uuid import UUID

import httpx
from shared.models import CloudflareCapacityPolicy
from sqlalchemy import and_, select, text

from src.config import Settings
from src.core.database import get_db_context
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
)
from src.services.kubernetes_execution import load_kubernetes_policy
from src.services.worker_pool.cloudflare_admission import (
    ManagedCloudflareScope,
    PrimaryPool,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    open_credential,
)
from src.services.worker_pool.controller import SWEEP_SECONDS, _withdraw_live
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


async def _primary_pool(settings: Settings, http: httpx.AsyncClient, *, kubernetes: bool) -> PrimaryPool | None:
    async with get_db_context() as db:
        limit = (await load_kubernetes_policy(db)).max_workers
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
            .where(CloudflareInstallation.retired_at.is_(None))
        )
        return [ManagedCloudflareScope(*row) for row in rows.tuples()]


async def _reconcile_scope(
    installation_id: UUID, name: str, settings: Settings, http: httpx.AsyncClient,
    controllers: dict[ManagedCloudflareScope, ExternalWorkerPoolController],
    *, kubernetes: bool,
) -> None:
    # Only one replica at a time drives each installation's physical pass.
    # This lock is separate from the admission lock and does not block scale-in.
    async with get_db_context() as db:
        owned = await db.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"bifrost:cloudflare:supervisor:{installation_id}"},
        )
        if not owned:
            return
        row = (await db.execute(
            select(CloudflareInstallation, CloudflareCapacityScope, CloudflareCredential)
            .join(CloudflareCapacityScope, CloudflareCapacityScope.installation_id == CloudflareInstallation.id)
            .join(CloudflareCredential, and_(
                CloudflareCredential.installation_id == CloudflareCapacityScope.installation_id,
                CloudflareCredential.purpose == "scope_bearer",
                CloudflareCredential.scope == CloudflareCapacityScope.scope,
                CloudflareCredential.version == CloudflareCapacityScope.bearer_version,
            ))
            .where(CloudflareInstallation.id == installation_id, CloudflareInstallation.retired_at.is_(None),
                   CloudflareCapacityScope.scope == name)
        )).one_or_none()
        if row is None:
            return
        installation, scope, credential = row
        policy = CloudflareCapacityPolicy.model_validate(installation.policy["limits"])
        binding = ManagedCloudflareScope(installation_id, name, scope.deployment_revision)
        bearer = open_credential(
            CredentialRef(installation_id, installation.account_id, "scope_bearer", name, scope.bearer_version),
            credential,
        )
        client = ExternalCapacityClient(scope.endpoint, bearer.get_secret_value(), http=http)
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
        controller.shared_managed_worker_cap = policy.shared_managed_worker_cap
        controller.burst_launch_allowed = (
            controller.profile.max_workers == policy.workers.launcher_cap
            and cloudflare_slots(controller.profile.memory_limit_bytes) == policy.workers.slots_per_launcher
            and policy.workers.max_lifetime_seconds <= controller.profile.max_lifetime_seconds
            and installation.admitted_revision == scope.deployment_revision
        )
        controller.cloudflare_primary = None
        if controller.burst_launch_allowed:
            try:
                controller.cloudflare_primary = await _primary_pool(settings, http, kubernetes=kubernetes)
            except (CapacityProtocolError, httpx.HTTPError):
                logger.warning("Primary worker capacity is unavailable for Cloudflare placement")
            if controller.cloudflare_primary is None:
                controller.burst_launch_allowed = False
        if await controller.reachability.check():
            await controller.reconcile()


async def cloudflare_supervisor_loop(
    stop_event: asyncio.Event, settings: Settings, *, kubernetes: bool,
) -> None:
    controllers: dict[ManagedCloudflareScope, ExternalWorkerPoolController] = {}
    async with httpx.AsyncClient(trust_env=False) as http:
        try:
            while not stop_event.is_set():
                try:
                    scopes = await _installations()
                    for binding in list(controllers):
                        if binding not in scopes:
                            await _withdraw_live(controllers.pop(binding).id)
                    for binding in scopes:
                        try:
                            await _reconcile_scope(
                                binding.installation_id, binding.name, settings, http, controllers, kubernetes=kubernetes,
                            )
                        except Exception:
                            logger.warning("Managed Cloudflare scope reconciliation failed", exc_info=True)
                except Exception:
                    logger.warning("Could not read managed Cloudflare scopes", exc_info=True)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=SWEEP_SECONDS)
                except TimeoutError:
                    pass
        finally:
            for controller in controllers.values():
                await _withdraw_live(controller.id)
