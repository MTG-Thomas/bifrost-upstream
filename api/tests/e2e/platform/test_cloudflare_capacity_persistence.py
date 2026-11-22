"""Exercise the Cloudflare journal against the migrated platform database."""

import asyncio
import base64
import json
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from fastapi import HTTPException, Response
from pydantic import SecretStr
from scripts.test_cloudflare_fixture import block_cloudflare_network
from shared.models import (
    CloudflareCapacityPolicy,
    CloudflareLauncherUsage,
    CloudflareReconcileAction,
    CloudflareWorkersPolicy,
)
from sqlalchemy import delete, func, insert, literal, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from src.config import Settings
from src.core.principal import UserPrincipal
from src.jobs.platform import cloudflare_capacity as lifecycle_job
from src.jobs.platform import cloudflare_enable as enable_job
from src.jobs.platform.base import (
    PlatformJobCancelled,
    PlatformJobContext,
    PlatformJobFailure,
    PlatformJobRequiresAction,
)
from src.jobs.platform.kubernetes_client import KubernetesJobClient
from src.models.enums import ExecutionStatus
from src.models.orm.audit import AuditLog
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareOwnedResource,
    CloudflareStagedToken,
    CloudflareUsageActivity,
    CloudflareUsageIncarnation,
    CloudflareWorkerReservation,
    CloudflareWorkflowSlot,
)
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.models.orm.executions import Execution
from src.models.orm.organizations import Organization
from src.models.orm.platform_jobs import PlatformJob
from src.models.orm.work_dispatches import WorkDispatch
from src.routers import cloudflare_capacity as capacity_routes
from src.services import worker_ingress_probes as ingress_probes
from src.services.audit_context import ActorContext, clear_actor, set_actor
from src.services.authorization.context import (
    AuthorizationContext,
    Boundary,
    BoundaryKind,
    RoleGrant,
)
from src.services.authorization.enforce import Caller
from src.services.execution.workflow_dispatch import release_workflow_run
from src.services.kubernetes_execution import KubernetesExecutionService
from src.services.work_dispatch import WAKE_WORKFLOW, stage_dispatch
from src.services.worker_pool import claims as claim_service
from src.services.worker_pool import (
    cloudflare_admission,
    cloudflare_canary,
    cloudflare_connector_channel,
    cloudflare_policy,
    cloudflare_spend,
    cloudflare_supervisor,
    cloudflare_tunnel,
    controller,
    external_controller,
)
from src.services.worker_pool.claims import claim_next
from src.services.worker_pool.cloudflare_admission import (
    ManagedCloudflareScope,
    cloudflare_claim_allowed,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    consume_staged_token,
    open_credential,
    seal_credential,
    stage_account_token,
)
from src.services.worker_pool.cloudflare_policy import (
    CONSENT_TEXT_VERSION,
    CloudflareBudgetTooLow,
    CloudflareInstallationExists,
    CloudflarePolicyConflict,
    WorkersReleaseMeterBounds,
    create_cloudflare_installation,
    get_cloudflare_capacity_status,
    retire_cloudflare_installation,
    save_cloudflare_policy,
)
from src.services.worker_pool.cloudflare_provisioning import (
    DeletionSettling,
    ObservationPending,
    ResourceIntent,
    ResourceObservation,
    ResourceTarget,
)
from src.services.worker_pool.cloudflare_release import WorkersReleaseOrder
from src.services.worker_pool.cloudflare_slots import (
    adoptable_workflow_slot,
    authorized_queue_preparation,
    confirm_workflow_slot_release,
    count_released_prewarms,
    fence_workflow_slot_send,
    read_workflow_slot_admission,
    settle_workflow_slot,
    stage_queue_preparations,
)
from src.services.worker_pool.cloudflare_spend import (
    WORKERS_CALIBRATED_RATE_TABLE_VERSION,
    WORKERS_INVENTORY_RATE_TABLE_VERSION,
    WORKERS_LEGACY_RATE_TABLE_VERSION,
    WORKERS_RATE_TABLE_VERSION,
    WORKERS_RATES,
    cloudflare_estimated_usage,
    read_workers_usage,
    reserve_tenant_host_usage,
    usage_period,
)
from src.services.worker_pool.cloudflare_workers_rest import (
    OWNER_BINDING,
    CloudflareManagementDenied,
    CloudflareRestUnavailable,
    ConfiguredWorkersScript,
    WorkersRestCleanup,
    worker_owner_value,
)
from src.services.worker_pool.recovery import end_runs_of_lost_worker
from src.worker.remote.fencing import LeaseRef, fence_remote_owner, remote_owner_context
from src.worker.remote.host import ApiExecutor, Bridge, _cloudflare_tenant_key
from src.worker.remote.ownership import RemoteOwnerLease
from src.worker.remote.protocol import (
    CLOUDFLARE_DEV_BUDGET_BYTES,
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    CredentialRouteChanged,
)
from src.worker.remote.remote_pool import ThinRemoteProcessPool

from tests.helpers.cloudflare_containers import (
    app_journal,
    make_release,
    verified_egress,
)
from tests.helpers.cloudflare_containers import policy as container_policy

OPERATOR_ID = UUID("20cb7fe5-78e2-48d7-8f1e-8edcac69ffab")
TEST_ORDER = WorkersReleaseOrder("1.0.0", 1)
TEST_LAUNCHER_CAPS = {"sqlite_rows_written": 100, "do_requests": 20, "log_events": 5}
TEST_CPU_LIMITS = {
    "capacity": 87, "gateway": 50, "workflow": 30_000, "boot": 2000, "warm": 200, "exercise": 100, "startup": 1000,
}


@pytest.fixture(autouse=True)
def packaged_release(monkeypatch) -> None:
    for module in (cloudflare_admission, cloudflare_policy, lifecycle_job):
        monkeypatch.setattr(module, "installed_workers_release_digest", lambda: "fixture-release")
    for module in (cloudflare_policy, lifecycle_job):
        monkeypatch.setattr(module, "installed_workers_release_order", lambda: TEST_ORDER)


class _TunnelAPI:
    """Strict local Cloudflare transport with deterministic uncertain writes."""

    account = "a" * 32
    zone = "b" * 32
    dns_id = "c" * 32
    tunnel_id = "20000000-0000-4000-8000-000000000001"

    def __init__(self):
        self.tunnel = None
        self.dns = None
        self.config = None
        self.secret = None
        self.connectors = []
        self.tunnel_creates = 0
        self.dns_creates = 0
        self.rotations = 0
        self.evicted = []
        self.denied_write = None
        self.lose_dns_probe = False
        self.read_outage = False
        self.private_routes = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.cloudflare.com"
        assert request.headers["Authorization"] == "Bearer synthetic-cloudflare-management-token"
        method = request.method
        path = request.url.path.removeprefix("/client/v4")
        tunnels = f"/accounts/{self.account}/cfd_tunnel"
        tunnel = f"{tunnels}/{self.tunnel_id}"
        zone = f"/zones/{self.zone}"
        records = f"{zone}/dns_records"

        def result(value):
            return httpx.Response(200, json={"success": True, "result": value})

        if path == f"/accounts/{self.account}":
            return result({"id": self.account})
        if path == f"/accounts/{self.account}/entitlements":
            return result([
                {"id": feature, "deleted_date": "", "allocation": {"type": "bool", "value": True}}
                for feature in ("workers.enabled", "containers.enabled")
            ])
        if path == f"/accounts/{self.account}/teamnet/routes" and method == "GET":
            assert request.url.params["tunnel_id"] == self.tunnel_id
            assert request.url.params["is_deleted"] == "false"
            return result(self.private_routes)
        if path == zone and method == "GET":
            return result({"account": {"id": self.account}, "id": self.zone, "name": "example.test", "status": "active"})
        if path == tunnels and method == "GET":
            return result([self.tunnel] if self.tunnel and self.tunnel["deleted_at"] is None else [])
        if path == tunnels and method == "POST":
            self.tunnel_creates += 1
            body = json.loads(request.content)
            self.secret = body["tunnel_secret"]
            self.tunnel = {
                "id": self.tunnel_id, "name": body["name"], "account_tag": self.account,
                "config_src": body["config_src"], "created_at": "fixture-create", "deleted_at": None,
            }
            raise httpx.ReadError("lost tunnel create response", request=request)
        if path == tunnel and method == "GET":
            return result(self.tunnel)
        if path == tunnel and method == "DELETE":
            assert self.tunnel and not self.connectors
            assert self.dns is None or self.dns.get("comment") == "foreign"
            self.tunnel["deleted_at"] = "fixture-delete"
            return result(self.tunnel)
        if path == tunnel and method == "PATCH":
            self.rotations += 1
            self.secret = json.loads(request.content)["tunnel_secret"]
            raise httpx.ReadError("lost tunnel rotation response", request=request)
        if path == tunnel + "/token" and method == "GET":
            token = json.dumps({"a": self.account, "t": self.tunnel_id, "s": self.secret})
            return result(base64.b64encode(token.encode()).decode())
        if path == tunnel + "/connections" and method == "GET":
            if self.read_outage:
                return httpx.Response(503)
            return result([{"id": connector} for connector in self.connectors])
        if path == tunnel + "/connections" and method == "DELETE":
            connector = request.url.params["client_id"]
            self.evicted.append(connector)
            self.connectors.remove(connector)
            return result({})
        if path == tunnel + "/configurations" and method == "GET":
            if self.config is None:
                return httpx.Response(404)
            return result({"account_id": self.account, "tunnel_id": self.tunnel_id, "config": self.config})
        if path == tunnel + "/configurations" and method == "PUT":
            if self.denied_write == "tunnel":
                return httpx.Response(403)
            self.config = json.loads(request.content)["config"]
            assert set(self.config) == {"ingress", "warp-routing"}
            assert self.config["warp-routing"] == {"enabled": False}
            assert all(set(rule) <= {"hostname", "service"} for rule in self.config["ingress"])
            return result({})
        if path == records and method == "GET":
            assert request.url.params["name.exact"] == "workers.example.test"
            return result([self.dns] if self.dns else [])
        if path == records and method == "POST":
            self.dns_creates += 1
            if self.dns_creates == 1:
                return httpx.Response(503)
            self.dns = {**json.loads(request.content), "id": self.dns_id, "modified_on": "fixture-dns-version"}
            raise httpx.ReadError("lost DNS create response", request=request)
        if path == f"{records}/{self.dns_id}" and method == "GET":
            return result(self.dns) if self.dns and self.dns.get("id") == self.dns_id else httpx.Response(404)
        if path == f"{records}/{self.dns_id}" and method == "PATCH":
            if self.denied_write == "dns":
                return httpx.Response(403)
            assert self.dns and json.loads(request.content) == {
                key: value for key, value in self.dns.items() if key not in ("id", "modified_on")
            }
            self.dns["modified_on"] += "-updated"
            if self.lose_dns_probe:
                raise httpx.ReadError("lost DNS probe response", request=request)
            return result(self.dns)
        if path == f"{records}/{self.dns_id}" and method == "DELETE":
            assert self.dns
            self.dns = None
            return result({})
        raise AssertionError(f"Unexpected stub request: {method} {path}")


@pytest.mark.asyncio
@pytest.mark.parametrize(("layout", "origin"), [("compose", "http://api:8001"), ("kustomize", "http://worker-ingress:8001")])
async def test_managed_tunnel_lifecycle_resumes_and_rolls_connector(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, tmp_path, layout, origin,
) -> None:
    installation.reachability_mode = "managed_tunnel"
    installation.zone_id = _TunnelAPI.zone
    await db_session.flush()
    connection = await db_session.connection()
    installation_id = installation.id

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    async def change(operation, client, **kwargs):
        async with context() as db:
            row = await db.get(CloudflareInstallation, installation_id)
            return await operation(db, row, client, **kwargs)

    settings = SimpleNamespace(cloudflare_tunnel_channel=layout)
    monkeypatch.setattr(cloudflare_tunnel, "get_settings", lambda: settings)
    monkeypatch.setattr(cloudflare_connector_channel, "get_settings", lambda: settings)
    monkeypatch.setattr(cloudflare_connector_channel, "get_db_context", context)
    token_file = tmp_path / "token"
    monkeypatch.setattr(cloudflare_connector_channel, "COMPOSE_PATH", token_file)
    secret_data = {}
    secret_version = 0

    def kube_api(request):
        nonlocal secret_version, secret_data
        assert request.url.path == "/api/v1/namespaces/test/secrets/bifrost-cloudflare-connector"
        assert request.headers["Authorization"] == "Bearer fixture-projector-identity"
        if request.method == "PATCH":
            patch = json.loads(request.content)
            assert patch["metadata"]["resourceVersion"] == str(secret_version)
            assert set(patch["data"]) == {"token"}
            secret_data = patch["data"]
            secret_version += 1
        return httpx.Response(200, json={
            "metadata": {
                "name": "bifrost-cloudflare-connector", "namespace": "test", "resourceVersion": str(secret_version),
                "labels": {"app.kubernetes.io/component": "cloudflare-connector"},
            },
            "type": "Opaque", "data": secret_data,
        })

    def delivered() -> str:
        if layout == "compose":
            return token_file.read_text() if token_file.exists() else ""
        return base64.b64decode(secret_data.get("token", "")).decode()

    async with httpx.AsyncClient(base_url="https://kube.example.test", transport=httpx.MockTransport(kube_api)) as kube:
        monkeypatch.setattr(KubernetesJobClient, "_read_token", lambda _: "fixture-projector-identity")
        monkeypatch.setattr(cloudflare_connector_channel, "in_cluster_namespace", lambda: "test")
        monkeypatch.setattr(cloudflare_connector_channel, "KubernetesJobClient",
                            lambda **kwargs: KubernetesJobClient(http_client=kube, **kwargs))
        provider = _TunnelAPI()
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
            client = cloudflare_tunnel.adapter(http, installation, SecretStr("synthetic-cloudflare-management-token"))
            actor = str(OPERATOR_ID)
            async with context() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                await cloudflare_tunnel.prepare(db, row, actor=actor)
            with pytest.raises(CloudflareRestUnavailable):
                await change(cloudflare_tunnel.provision_tunnel, client)
            await change(cloudflare_tunnel.provision_tunnel, client)
            assert provider.tunnel_creates == 1
            assert provider.config == {"ingress": [
                {"hostname": "workers.example.test", "service": origin}, {"service": "http_status:404"},
            ], "warp-routing": {"enabled": False}}
            with pytest.raises(CloudflareRestUnavailable):
                await change(cloudflare_tunnel.provision_dns, client, actor=actor)
            with pytest.raises(CloudflareRestUnavailable):
                await change(cloudflare_tunnel.provision_dns, client, actor=actor)
            await change(cloudflare_tunnel.provision_dns, client, actor=actor)
            assert provider.dns_creates == 2
            key = await cloudflare_connector_channel.project_connector(None)
            first = delivered()
            assert first == (await client.connector_token(provider.tunnel_id)).get_secret_value()
            old, new = str(uuid4()), str(uuid4())
            provider.connectors = [old]
            assert provider.config
            provider.config = {
                "originRequest": {}, "warp-routing": {"enabled": False},
                "ingress": [{**rule, "originRequest": {}} for rule in provider.config["ingress"]],
            }
            await change(cloudflare_tunnel.verify, client)
            monkeypatch.setattr(lifecycle_job, "get_db_context", context)
            monkeypatch.setattr(lifecycle_job, "_hold_installation_lease", AsyncMock())
            monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
            with monkeypatch.context() as patch:
                patch.setattr(lifecycle_job, "WorkersRestCleanup", lambda _: WorkersRestCleanup(http))
                patch.setattr(cloudflare_tunnel, "adapter", lambda *_: client)
                for version, permission in enumerate(("tunnel", "dns", None), start=2):
                    provider.denied_write = permission
                    if permission is None:
                        async with context() as db:
                            row = await db.get(CloudflareInstallation, installation_id)
                            row.desired_state = "enabled"
                            row.admitted_revision = 1
                        provider.config["warp-routing"] = {"enabled": True}
                    job, _ = await lifecycle_job.enqueue_cloudflare_account_token_rotation(
                        db_session, _platform_operator(), installation_id=installation_id, expected_revision=1,
                        replacement=SecretStr("synthetic-cloudflare-management-token"),
                    )
                    await db_session.flush()
                    payload = lifecycle_job.CloudflareRotateAccountPayload.model_validate(job.payload)
                    running = PlatformJobContext(job.id, uuid4(), None, str(OPERATOR_ID), "operator@example.test", "")
                    if permission:
                        with pytest.raises(PlatformJobRequiresAction) as denied:
                            await lifecycle_job._rotate_account_token(running, payload)
                        assert denied.value.result == {"code": "CF_MANAGEMENT_CREDENTIAL"}
                        await db_session.refresh(installation)
                        assert installation.api_token_version == 1
                        job.status = "requires_action"
                    else:
                        assert await lifecycle_job._rotate_account_token(running, payload) == {
                            "outcome": "rotated", "credential_version": version,
                        }
                        await db_session.refresh(installation)
                        assert installation.api_token_version == version
                        assert installation.admitted_revision is None
                        assert provider.config["warp-routing"] == {"enabled": False}
                        job.status = "succeeded"
                    await db_session.flush()
            provider.denied_write = None
            provider.lose_dns_probe = True
            with pytest.raises(CloudflareRestUnavailable):
                await change(cloudflare_tunnel.verify_management_writes, client)
            provider.lose_dns_probe = False
            await change(cloudflare_tunnel.verify_management_writes, client)
            async with context() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                row.desired_state = "enabled"
                row.admitted_revision = 1
            reconcile = lifecycle_job.CloudflareReconcilePayload(installation_id=installation_id, revision=1)
            running = PlatformJobContext(uuid4(), uuid4(), None, str(OPERATOR_ID), "operator@example.test", "")
            provider.read_outage = True
            with pytest.raises(CloudflareRestUnavailable):
                await lifecycle_job._settle_reconcile(running, reconcile, WorkersRestCleanup(http), tunnel_http=http)
            await db_session.refresh(installation)
            assert installation.admitted_revision == 1
            provider.read_outage = False
            provider.config["ingress"].append({"service": "http://unexpected.example.test"})
            with pytest.raises(PlatformJobRequiresAction) as mismatch:
                await lifecycle_job._settle_reconcile(running, reconcile, WorkersRestCleanup(http), tunnel_http=http)
            assert mismatch.value.result == {"code": "CF_TUNNEL_INGRESS_MISMATCH"}
            await db_session.refresh(installation)
            assert installation.admitted_revision is None
            assert provider.config == {"ingress": [
                {"hostname": "workers.example.test", "service": origin}, {"service": "http_status:404"},
            ], "warp-routing": {"enabled": False}}
            with monkeypatch.context() as patch:
                patch.setattr(enable_job, "get_db_context", context)
                patch.setattr(enable_job, "_hold_installation_lease", AsyncMock())
                patch.setattr(enable_job, "WorkersRestCleanup", lambda _: SimpleNamespace(verify_workers_paid=AsyncMock()))
                patch.setattr(enable_job, "load_workers_release", lambda: SimpleNamespace(
                    digest="fixture-release", manifest=SimpleNamespace(release_version="1.0.0", release_sequence=1),
                ))
                patch.setattr(cloudflare_tunnel, "adapter", lambda *_: client)
                provider.config["warp-routing"] = {"enabled": True}
                provider.connectors = []
                with pytest.raises(PlatformJobFailure) as unavailable:
                    await lifecycle_job._reconcile(running, reconcile)
                assert unavailable.value.code == "CF_CONNECTOR_UNAVAILABLE"
                await db_session.refresh(installation)
                assert installation.admitted_revision is None
                assert provider.config["warp-routing"] == {"enabled": False}
                provider.connectors = [old]

                savepoint = await db_session.begin_nested()
                try:
                    installation.desired_revision = 2
                    installation.deployed_revision = 1
                    installation.upgrade_release = "fixture-release"
                    prior = CloudflareOwnedResource(
                        id=uuid4(), installation_id=installation_id, account_id=installation.account_id,
                        scope="workers", kind="worker_script", target_name="prior-capacity", desired_revision=1,
                        artifact_digest="c" * 64, effect_id=uuid4(), state="present",
                        provider_id="prior-deployment", provider_version="prior-version",
                    )
                    db_session.add(prior)
                    await db_session.flush()
                    observation = ResourceObservation(
                        lifecycle_job._resource_intent(prior), "prior-deployment", "prior-version",
                    )
                    workers = SimpleNamespace(
                        verify_workers_paid=AsyncMock(), observe=AsyncMock(side_effect=[observation, None]),
                        delete=AsyncMock(), verify_absent=AsyncMock(),
                    )
                    patch.setattr(enable_job, "WorkersRestCleanup", lambda _: workers)
                    zero = dict.fromkeys(WORKERS_RATES, Decimal(0))
                    patch.setattr(enable_job, "release_meter_bounds", lambda _: WorkersReleaseMeterBounds(
                        60, zero, zero, "fixture-release", 2, TEST_LAUNCHER_CAPS, TEST_CPU_LIMITS, 500_000, TEST_ORDER,
                    ))
                    provider.private_routes = [{"tunnel_id": provider.tunnel_id, "network": "192.0.2.0/24"}]
                    upgrade = lifecycle_job.CloudflareReconcilePayloadV2(
                        installation_id=installation_id, revision=2, operation="upgrade",
                    )
                    with pytest.raises(PlatformJobRequiresAction) as upgrade_conflict:
                        await lifecycle_job._reconcile(running, upgrade)
                    assert upgrade_conflict.value.result == {"code": "CF_OWNERSHIP_CONFLICT"}
                    workers.delete.assert_not_awaited()
                    await db_session.refresh(prior)
                    await db_session.refresh(installation)
                    assert prior.state == "present"
                    assert installation.deployed_revision == 1 and installation.upgrade_drained_at is None
                finally:
                    provider.private_routes = []
                    await savepoint.rollback()
                    await db_session.refresh(installation)
            installation.admitted_revision = 1
            await db_session.flush()
            provider.private_routes = [{"tunnel_id": provider.tunnel_id, "network": "192.0.2.0/24"}]
            with pytest.raises(PlatformJobRequiresAction) as private_route:
                await lifecycle_job._settle_reconcile(running, reconcile, WorkersRestCleanup(http), tunnel_http=http)
            assert private_route.value.result == {"code": "CF_OWNERSHIP_CONFLICT"}
            await db_session.refresh(installation)
            assert installation.admitted_revision is None
            provider.private_routes = []
            installation.desired_state = "scaled_in"
            await db_session.flush()
            provider.connectors = []
            with pytest.raises(ObservationPending, match="no connected"):
                await change(cloudflare_tunnel.verify, client)
            provider.connectors = [old]
            await change(cloudflare_tunnel.verify, client)
            async with context() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                await cloudflare_tunnel.stage_rotation(db, row, version=2, actor=actor)
            await change(cloudflare_tunnel.record_rotation_intent, client, version=2)
            assert await change(cloudflare_tunnel.connector_ready, client)
            assert not provider.evicted
            late = str(uuid4())
            provider.connectors.append(late)
            with pytest.raises(CloudflareRestUnavailable):
                await change(cloudflare_tunnel.rotate, client, version=2, actor=actor)
            provider.connectors.remove(old)  # Temporarily absent from the provider's connection list.
            await change(cloudflare_tunnel.rotate, client, version=2, actor=actor)
            provider.connectors.append(old)
            assert provider.rotations == 1
            key = await cloudflare_connector_channel.project_connector(key)
            assert delivered() != first
            assert delivered() == (await client.connector_token(provider.tunnel_id)).get_secret_value()
            async with context() as db:
                job, _ = await lifecycle_job.enqueue_cloudflare_worker_credential_rotation(
                    db, _platform_operator(), installation_id=installation_id, expected_revision=1,
                    credential="connector_token",
                )
                assert job.payload["replacement_version"] == 3
            await change(cloudflare_tunnel.record_rotation_intent, client, version=3)
            # Enable's readiness check must still evict both v1 sessions after v3 aborts.
            assert not await change(cloudflare_tunnel.connector_ready, client)
            assert provider.evicted == sorted([old, late])
            provider.connectors = [new]
            assert await change(cloudflare_tunnel.connector_ready, client)
            assert not await change(cloudflare_tunnel.cleanup, client)
            with pytest.raises(DeletionSettling, match="projection"):
                await change(cloudflare_tunnel.cleanup, client)
            await cloudflare_connector_channel.project_connector(key)
            assert delivered() == ""
            assert not await change(cloudflare_tunnel.cleanup, client)  # DNS delete intent
            assert not await change(cloudflare_tunnel.cleanup, client)  # DNS verified absent
            assert not await change(cloudflare_tunnel.cleanup, client)  # Tunnel delete intent
            with pytest.raises(DeletionSettling, match="remain attached"):
                await change(cloudflare_tunnel.cleanup, client)
            provider.connectors = []
            assert not await change(cloudflare_tunnel.cleanup, client)  # Tunnel verified absent
            assert await change(cloudflare_tunnel.cleanup, client)
            assert provider.dns is None
            assert provider.tunnel and provider.tunnel["deleted_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize("recorded", [False, True])
async def test_managed_tunnel_disconnect_preserves_foreign_dns(db_session, installation, monkeypatch, recorded):
    installation.reachability_mode = "managed_tunnel"
    installation.zone_id = _TunnelAPI.zone
    monkeypatch.setattr(cloudflare_tunnel, "get_settings", lambda: SimpleNamespace(cloudflare_tunnel_channel="compose"))
    provider = _TunnelAPI()
    foreign = {"name": "workers.example.test", "type": "CNAME", "comment": "foreign", "content": "foreign.example.test"}
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = cloudflare_tunnel.adapter(http, installation, SecretStr("synthetic-cloudflare-management-token"))
        await cloudflare_tunnel.prepare(db_session, installation, actor=str(OPERATOR_ID))
        await db_session.flush()
        with pytest.raises(CloudflareRestUnavailable):
            await cloudflare_tunnel.provision_tunnel(db_session, installation, client)
        await cloudflare_tunnel.provision_tunnel(db_session, installation, client)
        record = await cloudflare_tunnel.resource(db_session, installation, cloudflare_tunnel.DNS)
        assert record is not None
        if recorded:
            record.provider_id = provider.dns_id
            record.provider_version = "fixture-dns-version"
            record.state = "present"
        provider.dns = foreign
        with pytest.raises(lifecycle_job.OwnershipConflict):
            await cloudflare_tunnel.provision_dns(db_session, installation, client, actor=str(OPERATOR_ID))
        await db_session.flush()
        assert not await cloudflare_tunnel.cleanup(db_session, installation, client)
        if not recorded:
            with pytest.raises(DeletionSettling, match="late Managed Tunnel write"):
                await cloudflare_tunnel.cleanup(db_session, installation, client)
            record.observed_at = datetime.now(UTC) - timedelta(minutes=2)
            assert not await cloudflare_tunnel.cleanup(db_session, installation, client)  # DNS verified absent
        assert not await cloudflare_tunnel.cleanup(db_session, installation, client)  # Tunnel delete intent
        assert not await cloudflare_tunnel.cleanup(db_session, installation, client)  # Tunnel verified absent
        assert await cloudflare_tunnel.cleanup(db_session, installation, client)
    assert provider.dns == foreign and provider.tunnel and provider.tunnel["deleted_at"]


def _platform_operator() -> Caller:
    role = RoleGrant(uuid4(), frozenset({"platform.readwrite"}), (Boundary(BoundaryKind.PLATFORM),))
    return Caller(
        UserPrincipal(OPERATOR_ID, "operator@example.test", None),
        AuthorizationContext(OPERATOR_ID, None, uuid4(), False, frozenset(), (role,)),
    )


@pytest.fixture(autouse=True)
def refuse_hosted_cloudflare(monkeypatch):
    block_cloudflare_network(monkeypatch.setattr)


@pytest.mark.asyncio
async def test_managed_tunnel_disconnect_commits_absence_before_retirement(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, lifecycle_audit_actor,
) -> None:
    installation.reachability_mode = "managed_tunnel"
    installation.zone_id = _TunnelAPI.zone
    monkeypatch.setattr(cloudflare_tunnel, "get_settings", lambda: SimpleNamespace(cloudflare_tunnel_channel="compose"))
    provider = _TunnelAPI()
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = cloudflare_tunnel.adapter(http, installation, SecretStr("synthetic-cloudflare-management-token"))
        await cloudflare_tunnel.prepare(db_session, installation, actor=str(OPERATOR_ID))
        await db_session.flush()
        with pytest.raises(CloudflareRestUnavailable):
            await cloudflare_tunnel.provision_tunnel(db_session, installation, client)
        await cloudflare_tunnel.provision_tunnel(db_session, installation, client)
    record = await cloudflare_tunnel.resource(db_session, installation, cloudflare_tunnel.DNS)
    assert record is not None
    record.state = "absent"
    record.observed_at = datetime.now(UTC)
    await db_session.flush()

    admin = _platform_operator()
    job, _ = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="disconnect",
    )
    lease = uuid4()
    job.lease_token = lease
    job.status = "running"
    await db_session.flush()
    connection = await db_session.connection()
    sessions = async_sessionmaker(
        connection, autoflush=False, expire_on_commit=False, join_transaction_mode="create_savepoint",
    )
    monkeypatch.setattr(lifecycle_job, "get_db_context", sessions.begin)
    monkeypatch.setattr(lifecycle_job, "httpx", SimpleNamespace(
        AsyncClient=lambda **_: httpx.AsyncClient(transport=httpx.MockTransport(provider)),
    ))
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    running = PlatformJobContext(job.id, lease, None, str(OPERATOR_ID), admin.principal.email, "")
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)

    assert await lifecycle_job.CLOUDFLARE_DISABLE_DEFINITION.handler(running, payload) == {"outcome": "disconnected"}
    assert provider.tunnel and provider.tunnel["deleted_at"]
    await db_session.refresh(installation)
    assert installation.retired_at is not None
    assert set(await db_session.scalars(select(CloudflareOwnedResource.state).where(
        CloudflareOwnedResource.installation_id == installation.id,
    ))) == {"absent"}


@pytest.fixture
def lifecycle_audit_actor(platform_admin):
    token = set_actor(ActorContext(platform_admin.user_id, platform_admin.organization_id))
    try:
        yield
    finally:
        clear_actor(token)


def test_cloudflare_routes_refuse_provider_org_membership(e2e_client, provider_org_user, platform_admin) -> None:
    path = "/api/capacity/cloudflare"
    assert e2e_client.get(path, headers=platform_admin.headers).status_code == 200
    assert e2e_client.get(path, headers=provider_org_user.headers).status_code == 403
    assert e2e_client.post(f"{path}/credential", headers=provider_org_user.headers, json={
        "account_id": "a" * 32, "purpose": "connect", "api_token": "synthetic-management-token",
    }).status_code == 403
    body = {"installation_id": str(uuid4()), "expected_revision": 1}
    assert e2e_client.patch(path, headers=provider_org_user.headers, json={
        **body, "action": "policy", "policy": {
            "workers": {"launcher_cap": 1, "slots_per_launcher": 2, "max_lifetime_seconds": 3600, "work_kinds": ["workflow"]},
            "monthly_limit_usd": "10.00",
        },
    }).status_code == 403
    for action in ("scale-in", "disconnect", "enable", "reconcile", "upgrade"):
        assert e2e_client.post(f"{path}/{action}", headers=provider_org_user.headers, json=body).status_code == 403
    assert e2e_client.post(
        f"{path}/rotate", headers=provider_org_user.headers,
        json={**body, "api_token": "synthetic-replacement-token"},
    ).status_code == 403


@pytest.mark.asyncio
async def test_staged_credential_is_encrypted_bound_expiring_and_single_use(db_session: AsyncSession) -> None:
    token = SecretStr("synthetic-staged-account-token")
    account = "a" * 32
    ref, expires_at = await stage_account_token(
        db_session, actor_id=OPERATOR_ID, account_id=account, purpose="connect", token=token,
    )
    await db_session.flush()
    staged = await db_session.get(CloudflareStagedToken, ref)
    assert staged is not None and token.get_secret_value() not in staged.ciphertext
    assert expires_at > datetime.now(UTC)
    for bad_actor, bad_account, bad_purpose in (
        (uuid4(), account, "connect"), (OPERATOR_ID, "b" * 32, "connect"), (OPERATOR_ID, account, "rotate"),
    ):
        with pytest.raises(ValueError, match="Invalid or expired"):
            await consume_staged_token(
                db_session, ref, actor_id=bad_actor, account_id=bad_account, purpose=bad_purpose,
            )
    assert await consume_staged_token(
        db_session, ref, actor_id=OPERATOR_ID, account_id=account, purpose="connect",
    ) == token
    with pytest.raises(ValueError, match="Invalid or expired"):
        await consume_staged_token(db_session, ref, actor_id=OPERATOR_ID, account_id=account, purpose="connect")
    expired, _ = await stage_account_token(
        db_session, actor_id=OPERATOR_ID, account_id=account, purpose="rotate", token=token,
    )
    await db_session.flush()
    await db_session.execute(update(CloudflareStagedToken).where(
        CloudflareStagedToken.id == expired,
    ).values(expires_at=datetime.now(UTC) - timedelta(seconds=1)))
    with pytest.raises(ValueError, match="Invalid or expired"):
        await consume_staged_token(db_session, expired, actor_id=OPERATOR_ID, account_id=account, purpose="rotate")


@pytest.mark.asyncio
async def test_connect_uses_opaque_credential_from_protected_rest_entry(e2e_client, platform_admin, async_session_factory) -> None:
    path = "/api/capacity/cloudflare"
    headers = platform_admin.headers
    account = "f" * 32
    token = "browser-fixture-cloudflare-token"
    staged = e2e_client.post(f"{path}/credential", headers=headers, json={
        "account_id": account, "purpose": "connect", "api_token": token,
    })
    assert staged.status_code == 201 and token not in staged.text
    reference = staged.json()["credential_ref"]
    request = {
        "action": "connect", "account_id": account, "worker_ingress_origin": "https://workers.browser-fixture.test",
        "consent_version": CONSENT_TEXT_VERSION, "consent": True, "credential_ref": reference,
        "policy": {"workers": {
            "launcher_cap": 10, "slots_per_launcher": 1, "max_lifetime_seconds": 3600, "work_kinds": ["workflow"],
        }, "monthly_limit_usd": "50.00"},
    }
    installation_id = None
    try:
        connected = e2e_client.patch(path, headers=headers, json=request)
        assert connected.status_code == 200, connected.text
        installation = connected.json()["installation"]
        installation_id = UUID(installation["installation_id"])
        assert installation["fleet_envelope_usd"] == "45.26"
        assert installation["spend"]["estimated_usd"] == "0.00"
        assert installation["spend"]["workers_paid_subscription_usd"] == "5.00"
        assert token not in connected.text and reference not in connected.text
        assert e2e_client.patch(path, headers=headers, json=request).status_code == 422
    finally:
        async with async_session_factory() as db:
            await db.execute(delete(CloudflareStagedToken).where(CloudflareStagedToken.id == UUID(reference)))
            if installation_id is not None:
                await db.execute(delete(CloudflareCredential).where(CloudflareCredential.installation_id == installation_id))
                await db.execute(delete(CloudflareInstallation).where(CloudflareInstallation.id == installation_id))
            await db.commit()


@pytest.mark.asyncio
async def test_canary_proof_requires_current_lease_worker_generation_and_revision(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    job, _ = await enable_job.enqueue_cloudflare_enable(
        db_session, _platform_operator(), installation_id=installation.id, expected_revision=1,
    )
    now = datetime.now(UTC)
    job.status = "running"
    job.lease_token = uuid4()
    job.lease_expires_at = now + timedelta(minutes=2)
    worker = ExecutionWorker(
        id=uuid4(), form="external", state="ready", capacity_scope="workers",
        runtime_profile="cloudflare-python-v1", memory_limit_bytes=128 * 2**20,
        capacity_resource_uid=uuid4().hex, remote_hello_sequence=1, remote_session_generation=7,
        remote_owner_lease_expires_at=now + timedelta(minutes=2),
    )
    db_session.add(worker)
    await db_session.flush()
    pin = ManagedCloudflareScope(installation.id, "workers", 1).pin(worker.id, 1)
    pin.canary_job_id, pin.canary_lease_token = job.id, job.lease_token
    db_session.add(pin)
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(cloudflare_canary, "get_db_context", context)
    proof_for = cloudflare_canary.cloudflare_sdk_canary_proof
    proof = await proof_for(worker.id, 7)
    assert proof is not None and await proof_for(worker.id, 8, received=proof) is None
    assert await proof_for(worker.id, 7, acknowledged=True) is None
    failure = ingress_probes.CanaryFailure("sdk_response", "http_status", 404)
    await cloudflare_canary.record_canary_failure(worker.id, 8, failure)
    await db_session.refresh(job)
    assert job.result is None
    job.result = {"prior_checkpoint": "retain"}
    await db_session.flush()
    await cloudflare_canary.record_canary_failure(worker.id, 7, failure)
    await db_session.refresh(job)
    assert job.result == {
        "prior_checkpoint": "retain",
        "canary_failure": {"worker_id": str(worker.id), "generation": 7, "reason": failure.as_result()},
    }
    assert await proof_for(worker.id, 7, received="a" * 64) is None
    assert await proof_for(worker.id, 7, received=proof) == proof
    assert await proof_for(worker.id, 7, acknowledged=True) == proof
    await db_session.refresh(pin)
    assert pin.canary_sdk_at is not None and pin.canary_ack_at is not None and pin.canary_sdk_generation == 7
    worker.remote_session_generation = 8
    await db_session.flush()
    assert await proof_for(worker.id, 8, acknowledged=True) is None
    proof8 = await proof_for(worker.id, 8)
    assert proof8 is not None and proof8 != proof
    assert await proof_for(worker.id, 8, received=proof8) == proof8
    await db_session.refresh(pin)
    assert pin.canary_ack_at is None and pin.canary_sdk_generation == 8
    assert await proof_for(worker.id, 8, acknowledged=True) == proof8
    worker.remote_session_generation = 7
    worker.state = "starting"
    await db_session.flush()
    assert await proof_for(worker.id, 7) is None
    worker.state = "ready"
    installation.desired_revision = 2
    await db_session.flush()
    assert await proof_for(worker.id, 7) is None
    installation.desired_revision = 1
    job.lease_expires_at = now - timedelta(seconds=1)
    await db_session.flush()
    assert await proof_for(worker.id, 7) is None
    await cloudflare_canary.record_canary_failure(worker.id, 7, ingress_probes.CanaryFailure("ack", "timeout"))
    await db_session.refresh(job)
    assert job.result is not None and job.result["canary_failure"]["reason"] == failure.as_result()


@pytest.mark.asyncio
@pytest.mark.parametrize("aborted_rotation", [0, 2, 3])
async def test_enable_requires_signed_sdk_evidence_and_resumes_owned_uploads(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, aborted_rotation,
) -> None:
    installation.policy = {**installation.policy, "bounds_release": "d" * 64}
    if aborted_rotation:
        installation.reachability_mode = "managed_tunnel"
        installation.zone_id = _TunnelAPI.zone
        active_version = aborted_rotation - 1
        installation.tunnel_token_version = installation.tunnel_token_projected_version = active_version
        receipt = CloudflareOwnedResource(
            id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
            scope="reachability", kind=cloudflare_tunnel.TUNNEL, target_name="fixture-tunnel", desired_revision=1,
            artifact_digest="c" * 64, effect_id=uuid4(), state="present",
            provider_id=str(uuid4()), provider_version="fixture-created",
        )
        db_session.add(receipt)
        await db_session.flush()
        connector = str(uuid4())
        connectors = {connector}
        previous = str(uuid4())
        if active_version == 2:
            receipt.provider_details = {"rotation_version": 2, "rotation_previous_connectors": [previous]}
            connectors.add(previous)
        tunnel = SimpleNamespace(
            tunnel=AsyncMock(side_effect=lambda _, expected: expected),
            connectors=AsyncMock(side_effect=lambda _: frozenset(connectors)),
            evict_connectors=AsyncMock(side_effect=lambda _, excluded: connectors.difference_update(excluded)),
        )
        await cloudflare_tunnel.record_rotation_intent(db_session, installation, tunnel, version=aborted_rotation)
        assert set(receipt.provider_details["rotation_candidates"]) == connectors
        # The replacement failed before activation, so the incumbent connector must remain eligible.
        monkeypatch.setattr(cloudflare_tunnel, "adapter", lambda *_: tunnel)
        for step in ("prepare", "provision_tunnel", "provision_dns"):
            monkeypatch.setattr(cloudflare_tunnel, step, AsyncMock())
    job, reused = await enable_job.enqueue_cloudflare_enable(
        db_session, _platform_operator(), installation_id=installation.id, expected_revision=1,
    )
    assert not reused and job.payload == {"installation_id": str(installation.id), "revision": 1}
    lease_token = uuid4()
    job.status = "running"
    job.lease_token = lease_token
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(enable_job, "get_db_context", context)
    monkeypatch.setattr(cloudflare_spend, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    observed = {}
    writes = []

    class Provider:
        async def observe(self, *, api_token, intent):
            assert api_token == SecretStr("synthetic-cloudflare-management-token")
            return observed.get(intent.target.name)

        async def create(self, *, api_token, intent, scope_bearer):
            assert (scope_bearer is not None) == intent.target.name.endswith("-capacity")
            writes.append(intent.target.name)
            observed[intent.target.name] = ResourceObservation(intent, str(uuid4()), str(uuid4()))

        async def configure_endpoint(self, *, api_token, expected):
            name = expected.intent.target.name
            return ConfiguredWorkersScript(
                "https://capacity.example.test" if name.endswith("-capacity") else None,
                {} if name.endswith("-gateway") else {"OwnedNamespace": "a" * 32},
            )

    monkeypatch.setattr(enable_job, "WorkersRestProvisioner", lambda *_: Provider())
    monkeypatch.setattr(ingress_probes, "_public_addresses", AsyncMock(return_value=("203.0.113.10",)))
    reachable = ingress_probes.WorkerIngressProbeResult(
        installation.worker_ingress_origin, True, True, True,
        (ingress_probes.RouteProbe("GET", "/api/sdk/context", 401, True),),
    )
    route = "/api/forms/{form_id}"
    failure = ingress_probes.CanaryFailure(
        "ingress_http", "route_timeout", address_family="ipv4", method="GET", route_template=route,
        handshake_completed=True, connection_reused=False, exception_class="ReadTimeout", phase="response_headers",
    )
    refused = ingress_probes.WorkerIngressProbeResult(
        installation.worker_ingress_origin, True, False, False,
        (ingress_probes.RouteProbe("GET", route, None, False, error_class="route_timeout", failure=failure),),
    )
    probe_results = iter((refused, reachable, reachable))

    async def probe_address(_origin, _host, _address, *, progress):
        progress.tcp_connects_started = 2
        progress.tls_handshakes_started = 1
        progress.tls_handshakes_completed = 1
        progress.reused_requests = 12
        return next(probe_results)

    monkeypatch.setattr(ingress_probes, "_probe_address", probe_address)
    canary = AsyncMock(side_effect=[
        ingress_probes.RuntimeCanary(True, False, ingress_probes.CanaryFailure("sdk_response", "http_status", 404)),
        ingress_probes.RuntimeCanary(True, True),
    ])
    monkeypatch.setattr(enable_job, "run_workers_canary", canary)
    paid_reads = []

    def provider_read(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET" and request.url.host == "api.cloudflare.com"
        assert request.headers["Authorization"] == "Bearer synthetic-cloudflare-management-token"
        account_path = f"/client/v4/accounts/{installation.account_id}"
        paid_reads.append(request.url.path)
        if request.url.path == account_path:
            return httpx.Response(200, json={"success": True, "result": {"id": installation.account_id}})
        assert request.url.path == account_path + "/entitlements"
        return httpx.Response(200, json={"success": True, "result": [
            {"id": feature, "deleted_date": "", "allocation": {"type": "bool", "value": True}}
            for feature in ("workers.enabled", "containers.enabled")
        ]})

    running = PlatformJobContext(job.id, lease_token, None, str(OPERATOR_ID), "operator@example.test", "")
    payload = enable_job.CloudflareEnablePayload.model_validate(job.payload)
    release = SimpleNamespace(
        digest="d" * 64,
        manifest=SimpleNamespace(
            release_version="1.1.0", release_sequence=1,
            meter_bounds=SimpleNamespace(qualification=SimpleNamespace(expected_cpu_ms={"capacity": 9})),
        ),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider_read)) as http:
        with monkeypatch.context() as patch:
            patch.setattr(enable_job, "cloudflare_workers_remain", AsyncMock(return_value=True))
            recovering = AsyncMock(return_value="settling")
            patch.setattr(enable_job, "_cleanup_stranded_workflow_slots", recovering)
            month, _ = usage_period(datetime.now(UTC))
            blocked = CloudflareUsageActivity(installation_id=installation.id, period_start=month, sdk_requests=200_000)
            db_session.add(blocked)
            await db_session.flush()
            with pytest.raises(PlatformJobRequiresAction) as budget:
                await enable_job._settle_enable(running, payload, http, release)
            assert budget.value.result == {"code": "CF_BUDGET_REACHED"}
            recovering.assert_not_awaited()
            await db_session.delete(blocked)
            await db_session.flush()
            with pytest.raises(PlatformJobFailure) as settling:
                await enable_job._settle_enable(running, payload, http, release)
            assert settling.value.code == "CF_CLEANUP_SETTLING" and settling.value.retryable
            recovering.assert_awaited_once()
        paid_reads.clear()
        with pytest.raises(PlatformJobRequiresAction) as ingress_denied:
            await enable_job._settle_enable(running, payload, http, release)
        http_connections = [{
            "address_family": "ipv4", "tcp_connects_started": 2,
            "tls_handshakes_started": 1, "tls_handshakes_completed": 1, "reused_requests": 12,
        }]
        assert ingress_denied.value.result == {
            "code": "CF_WORKER_INGRESS_UNVERIFIED",
            "canary_failure": {
                "stage": "ingress_http", "error_class": "route_timeout", "address_family": "ipv4",
                "method": "GET", "route_template": "/api/forms/{form_id}",
                "handshake_completed": True, "connection_reused": False, "exception_class": "ReadTimeout",
                "phase": "response_headers",
            },
            "http_connections": http_connections,
        }
        assert ingress_denied.value.phase == "Worker Ingress Probe Failed (Ingress HTTP, Route Timeout)"
        canary.assert_not_awaited()
        with pytest.raises(PlatformJobRequiresAction) as denied:
            await enable_job._settle_enable(running, payload, http, release)
        assert denied.value.result == {
            "code": "CF_WORKER_INGRESS_UNVERIFIED",
            "canary_failure": {"stage": "sdk_response", "error_class": "http_status", "status": 404},
            "http_connections": http_connections,
        }
        assert denied.value.phase == "Cloudflare Canary Failed (SDK Response, HTTP Status, HTTP 404)"
        await db_session.refresh(installation)
        assert installation.admitted_revision is None
        assert await enable_job._settle_enable(
            running, payload, http, release,
        ) == {"outcome": "enabled", "http_connections": http_connections}
    assert [name.rsplit("-", 1)[-1] for name in writes] == ["gateway", "launcher", "capacity"]
    assert canary.await_count == 2
    if aborted_rotation == 3:
        tunnel.evict_connectors.assert_awaited_once()
        assert tunnel.evict_connectors.await_args.args[1] == {previous}
    assert sum(path.endswith("/entitlements") for path in paid_reads) == 3
    await db_session.refresh(installation)
    assert installation.admitted_revision == 1


@pytest.mark.asyncio
async def test_upgrade_drains_before_replacement_and_resumes_after_lost_lease(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, lifecycle_audit_actor,
) -> None:
    release = "d" * 64
    now = datetime.now(UTC)
    installation.desired_state = "enabled"
    installation.admitted_revision = installation.deployed_revision = 1
    worker = ExecutionWorker(
        id=uuid4(), form="external", state="ready", capacity_scope="workers",
        runtime_profile="cloudflare-python-v1", memory_limit_bytes=256 * 2**20,
    )
    db_session.add(worker)
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr("synthetic-scope-bearer"), actor=str(OPERATOR_ID), now=now,
    ))
    await db_session.flush()
    db_session.add(ManagedCloudflareScope(installation.id, "workers", 1).pin(worker.id, 1))
    old_scope = CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", deployment_revision=1,
        endpoint="https://old-capacity.example.test", bearer_version=1,
    )
    db_session.add(old_scope)
    execution, fence = uuid4(), uuid4()
    claim = ExecutionWorkerClaim(worker_id=worker.id, family=WAKE_WORKFLOW, subject_id=execution)
    slot = CloudflareWorkflowSlot(
        installation_id=installation.id, tenant="platform", ordinal=0, worker_id=worker.id,
        execution_id=execution, dispatch_id=uuid4(), fence=fence, sent=True,
    )
    db_session.add_all([claim, slot])
    old_scripts = []
    for role in ("capacity", "launcher"):
        script = CloudflareOwnedResource(
            id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
            scope="workers", kind="worker_script", target_name=f"prior-{role}", desired_revision=1,
            artifact_digest="c" * 64, effect_id=uuid4(), state="present",
            provider_id=str(uuid4()), provider_version=str(uuid4()), namespace_ids={"OwnedNamespace": str(uuid4())},
        )
        db_session.add(script)
        old_scripts.append(script)
    await db_session.flush()
    assert await cloudflare_claim_allowed(db_session, worker.id)
    for module in (cloudflare_admission, cloudflare_policy, lifecycle_job):
        monkeypatch.setattr(module, "installed_workers_release_digest", lambda: release)
    monkeypatch.setattr(lifecycle_job, "installed_workers_release_order", lambda: WorkersReleaseOrder("1.1.0", 20))
    assert not await cloudflare_claim_allowed(db_session, worker.id)
    assert installation.admitted_revision == 1  # Local release fencing precedes the scheduler.

    admin = _platform_operator()
    monkeypatch.setattr(capacity_routes, "load_caller", AsyncMock(return_value=admin))
    response = Response()
    accepted = await capacity_routes.upgrade_cloudflare_capacity(
        CloudflareReconcileAction(installation_id=installation.id, expected_revision=1),
        response, db_session, object(),
    )
    assert response.headers["Location"] == f"/api/platform-jobs/{accepted.job_id}"
    job = await db_session.get(PlatformJob, accepted.job_id)
    assert job is not None and job.payload == {
        "installation_id": str(installation.id), "revision": 2, "operation": "upgrade", "reset_release_fence": False,
    }
    assert installation.admitted_revision is None and installation.upgrade_release == release
    assert installation.workers_release_sequence == 20
    lease, successor = uuid4(), uuid4()
    job.status, job.lease_token, job.started_at = "running", lease, now - timedelta(hours=1)
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(enable_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    scopes = {script.target_name: ResourceObservation(
        lifecycle_job._resource_intent(script), script.provider_id, script.provider_version,
    ) for script in old_scripts}
    effects = []
    replacement = SecretStr("synthetic-replacement-token")
    revoked = False

    class Provider:
        async def verify_workers_paid(self, account, token):
            assert account == installation.account_id
            if revoked and token != replacement:
                raise CloudflareManagementDenied

        async def observe(self, *, api_token, intent):
            return scopes.get(intent.target.name)

        async def verify_absent(self, *, api_token, target, namespace_ids=frozenset()):
            assert target.name not in scopes
            assert namespace_ids

        async def delete(self, *, api_token, expected):
            assert api_token == replacement
            effects.append(("delete", expected.intent.target.name))
            del scopes[expected.intent.target.name]

        async def create(self, *, api_token, intent, scope_bearer):
            assert api_token == replacement
            assert all(not name.startswith("prior-") for name in scopes)
            effects.append(("create", intent.target.name.rsplit("-", 1)[-1]))
            scopes[intent.target.name] = ResourceObservation(intent, str(uuid4()), str(uuid4()))

        async def configure_endpoint(self, *, api_token, expected):
            is_capacity = expected.intent.target.name.endswith("-capacity")
            return ConfiguredWorkersScript(
                "https://capacity.example.test" if is_capacity else None,
                {} if expected.intent.target.name.endswith("-gateway") else {"OwnedNamespace": "a" * 32},
            )

        async def verify_workers_write(self, api_token, expected):
            assert api_token == replacement
            return scopes[expected.intent.target.name] == expected

    provider = Provider()
    monkeypatch.setattr(enable_job, "WorkersRestCleanup", lambda *_: provider)
    monkeypatch.setattr(lifecycle_job, "WorkersRestCleanup", lambda *_: provider)
    monkeypatch.setattr(enable_job, "WorkersRestProvisioner", lambda *_: provider)
    monkeypatch.setattr(enable_job, "load_workers_release", lambda: SimpleNamespace(
        digest=release, manifest=SimpleNamespace(
            release_version="1.1.0", release_sequence=20,
            meter_bounds=SimpleNamespace(qualification=SimpleNamespace(expected_cpu_ms={"capacity": 9})),
        ),
    ))
    zero = dict.fromkeys(WORKERS_RATES, Decimal(0))
    monkeypatch.setattr(enable_job, "release_meter_bounds", lambda _: WorkersReleaseMeterBounds(
        60, zero, zero, release, 2, TEST_LAUNCHER_CAPS, TEST_CPU_LIMITS, 500_000, TEST_ORDER,
    ))

    async def lose_runner(_seconds):
        nonlocal revoked
        assert effects == []
        await db_session.refresh(installation)
        assert installation.upgrade_drained_at is None
        assert await db_session.get(CloudflareCapacityScope, (installation.id, "workers", 1)) is not None
        job.lease_token = successor
        revoked = True
        await db_session.flush()

    monkeypatch.setattr(enable_job, "sleep", lose_runner)
    payload = lifecycle_job.CloudflareReconcilePayloadV2.model_validate(job.payload)
    handler = lifecycle_job.CLOUDFLARE_RECONCILE_DEFINITION.handler
    running = PlatformJobContext(job.id, lease, None, str(OPERATOR_ID), admin.principal.email, "")
    with monkeypatch.context() as older_runner:
        older_runner.setattr(lifecycle_job, "installed_workers_release_digest", lambda: "fixture-release")
        older_runner.setattr(lifecycle_job, "installed_workers_release_order", lambda: WorkersReleaseOrder("1.0.0", 21))
        with pytest.raises(PlatformJobRequiresAction) as mismatch:
            await handler(running, payload)
        assert mismatch.value.result == {
            "code": "CF_RELEASE_MISMATCH", "instructions": [lifecycle_job.RELEASE_FENCE_REMEDY],
        }
        assert effects == []
    with pytest.raises(PlatformJobCancelled):
        await handler(running, payload)
    await db_session.refresh(slot)
    assert slot.sent and slot.fence == fence and slot.released_at is None
    assert await db_session.get(ExecutionWorkerClaim, claim.id) is not None
    assert effects == []

    recovered = PlatformJobContext(job.id, successor, None, str(OPERATOR_ID), admin.principal.email, "")
    with pytest.raises(PlatformJobRequiresAction) as denied:
        await handler(recovered, payload)
    assert denied.value.result == {"code": "CF_MANAGEMENT_CREDENTIAL"}
    job.status = "requires_action"
    rotation, _ = await lifecycle_job.enqueue_cloudflare_account_token_rotation(
        db_session, admin, installation_id=installation.id, expected_revision=2, replacement=replacement,
    )
    rotation.status, rotation.lease_token = "running", uuid4()
    await db_session.flush()
    rotating = PlatformJobContext(rotation.id, rotation.lease_token, None, str(OPERATOR_ID), admin.principal.email, "")
    await lifecycle_job.CLOUDFLARE_ROTATE_ACCOUNT_DEFINITION.handler(
        rotating, lifecycle_job.CloudflareRotateAccountPayload.model_validate(rotation.payload),
    )
    rotation.status = "succeeded"
    await confirm_workflow_slot_release(db_session, worker.id, execution, fence, completed=True)
    worker.state, worker.capacity_resource_deleted_at = "retired", datetime.now(UTC)
    await db_session.delete(claim)
    job.status, job.lease_token = "running", uuid4()
    await db_session.flush()

    routes = (ingress_probes.RouteProbe("GET", "/api/sdk/context", 401, True),)
    probe = ingress_probes.WorkerIngressProbeResult(
        installation.worker_ingress_origin, True, True, True, routes,
        runtime_canary=ingress_probes.RuntimeCanary(
            True, False, ingress_probes.CanaryFailure("sdk_response", "http_status", 404),
        ),
    )
    good = ingress_probes.WorkerIngressProbeResult(
        installation.worker_ingress_origin, True, True, True, routes,
        runtime_canary=ingress_probes.RuntimeCanary(True, True),
    )
    monkeypatch.setattr(enable_job, "probe_worker_ingress", AsyncMock(side_effect=[probe, good]))
    recovered = PlatformJobContext(job.id, job.lease_token, None, str(OPERATOR_ID), admin.principal.email, "")
    with pytest.raises(PlatformJobRequiresAction) as readiness:
        await handler(recovered, payload)
    assert readiness.value.result["code"] == "CF_WORKER_INGRESS_UNVERIFIED"
    await db_session.refresh(installation)
    assert installation.upgrade_drained_at is not None and installation.admitted_revision is None
    assert await handler(recovered, payload) == {"outcome": "enabled", "http_connections": []}
    await db_session.refresh(installation)
    assert installation.deployed_revision == installation.admitted_revision == 2
    assert installation.upgrade_release is installation.upgrade_drained_at is None
    assert effects == [
        ("delete", "prior-capacity"), ("delete", "prior-launcher"),
        ("create", "gateway"), ("create", "launcher"), ("create", "capacity"),
    ]


@pytest.mark.asyncio
async def test_account_rotation_switches_only_a_valid_same_account_version(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    admin = _platform_operator()
    candidate = SecretStr("test-rotated-account-token")
    receipt = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="rotation-capacity", desired_revision=1,
        artifact_digest="c" * 64, effect_id=uuid4(), state="present",
        provider_id=str(uuid4()), provider_version=str(uuid4()),
    )
    db_session.add(receipt)
    await db_session.flush()
    connection = await db_session.connection()
    locked = False

    @asynccontextmanager
    async def context():
        nonlocal locked
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            locked = True
            try:
                yield session
                await session.commit()
            finally:
                locked = False

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    replacement_version = 2
    subdomain_writes = 0

    def remote(request: httpx.Request) -> httpx.Response:
        nonlocal subdomain_writes
        assert not locked
        assert request.headers["Authorization"] == "Bearer " + candidate.get_secret_value()
        path = request.url.path
        if path.endswith("/subdomain"):
            result = {"enabled": True, "previews_enabled": False}
            if request.method == "POST":
                subdomain_writes += 1
                assert json.loads(request.content) == result
                if replacement_version == 3:
                    return httpx.Response(403)
            else:
                assert request.method == "GET"
        elif path.endswith("/entitlements"):
            result = [
                {"id": feature, "deleted_date": "", "allocation": {"type": "bool", "value": True}}
                for feature in ("workers.enabled", "containers.enabled", "containers.enabled")
            ]
        elif path.endswith("/settings"):
            if replacement_version == 4:
                return httpx.Response(404)
            result = {"bindings": [{
                "name": OWNER_BINDING, "type": "plain_text",
                "text": worker_owner_value(lifecycle_job._resource_intent(receipt)),
            }]}
        elif path.endswith("/deployments"):
            result = {"deployments": [{
                "id": receipt.provider_id,
                "versions": [{"percentage": 100, "version_id": receipt.provider_version}],
            }]}
        else:
            assert path == f"/client/v4/accounts/{installation.account_id}"
            result = {"id": "b" * 32 if replacement_version == 2 else installation.account_id}
        return httpx.Response(200, json={
            "success": True, "result": result,
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        adapter = WorkersRestCleanup(http)
        monkeypatch.setattr(lifecycle_job, "WorkersRestCleanup", lambda _http: adapter)
        for replacement_version in (2, 3, 4, 5):
            ref, _ = await stage_account_token(
                db_session, actor_id=OPERATOR_ID, account_id=installation.account_id, purpose="rotate", token=candidate,
            )
            await db_session.flush()
            job, reused = await lifecycle_job.enqueue_cloudflare_account_token_rotation(
                db_session, admin, installation_id=installation.id, expected_revision=1, replacement=ref,
            )
            assert not reused and job.payload["replacement_version"] == replacement_version
            assert candidate.get_secret_value() not in json.dumps(job.payload)
            assert str(ref) not in json.dumps(job.payload)
            assert installation.api_token_version == 1
            assert (await get_cloudflare_capacity_status(db_session, admin)).latest_job_id == job.id
            token = uuid4()
            job.lease_token = token
            job.status = "running"
            await db_session.flush()
            payload = lifecycle_job.CloudflareRotateAccountPayload.model_validate(job.payload)
            running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")
            handler = lifecycle_job.CLOUDFLARE_ROTATE_ACCOUNT_DEFINITION.handler
            if replacement_version in (2, 3, 4):
                with pytest.raises(PlatformJobRequiresAction) as denied:
                    await handler(running, payload)
                assert denied.value.result == {"code": {
                    2: "CF_MANAGEMENT_CREDENTIAL",
                    3: "CF_WORKERS_WRITE_UNVERIFIED",
                    4: "CF_DEPLOYMENT_UNVERIFIED",
                }[replacement_version]}
                job.status = "requires_action"
                rejected = await db_session.get(
                    CloudflareCredential, (installation.id, "account_token", "", replacement_version),
                )
                assert rejected is not None and rejected.ciphertext is None
                previous = await db_session.get(CloudflareCredential, (installation.id, "account_token", "", 1))
                assert previous is not None and previous.ciphertext is not None
            else:
                assert await handler(running, payload) == {"outcome": "rotated", "credential_version": 5}
            await db_session.refresh(installation)
    assert subdomain_writes == 2
    assert installation.api_token_version == 5 and installation.control_root_version == 1
    previous = await db_session.get(
        CloudflareCredential, (installation.id, "account_token", "", 1), populate_existing=True,
    )
    assert previous is not None and previous.ciphertext is None
    audit = await db_session.scalar(select(AuditLog).where(
        AuditLog.resource_id == installation.id, AuditLog.action == "capacity.cloudflare_account_token_rotated",
    ))
    assert audit is not None
    assert candidate.get_secret_value() not in json.dumps(audit.details)


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["control_root", "scope_bearer"])
async def test_rotate_worker_credential_is_drain_first(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, purpose,
) -> None:
    admin = _platform_operator()
    scope = "workers" if purpose == "scope_bearer" else ""
    previous_key = (installation.id, purpose, scope, 1)
    if scope:
        db_session.add(seal_credential(
            CredentialRef(installation.id, installation.account_id, purpose, scope, 1),
            SecretStr("previous-scope-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
        ))
    installation.desired_state = "enabled"
    with pytest.raises(CloudflarePolicyConflict, match="Scale in"):
        await lifecycle_job.enqueue_cloudflare_worker_credential_rotation(
            db_session, admin, installation_id=installation.id, expected_revision=1, credential=purpose,
        )
    installation.desired_state = "scaled_in"
    await db_session.flush()
    job, reused = await lifecycle_job.enqueue_cloudflare_worker_credential_rotation(
        db_session, admin, installation_id=installation.id, expected_revision=1, credential=purpose,
    )
    assert not reused and job.payload["replacement_version"] == 2
    token = uuid4()
    job.lease_token = token
    job.status = "running"
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    payload = lifecycle_job.CloudflareRotateWorkerPayload.model_validate(job.payload)
    running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")
    assert await lifecycle_job.CLOUDFLARE_ROTATE_WORKER_DEFINITION.handler(running, payload) == {
        "outcome": "rotated", "credential_version": 2,
    }
    previous = await db_session.get(CloudflareCredential, previous_key, populate_existing=True)
    active = await db_session.get(CloudflareCredential, (*previous_key[:3], 2))
    await db_session.refresh(installation)
    assert previous is not None and previous.ciphertext is None
    assert active is not None and active.ciphertext is not None
    assert installation.control_root_version == (1 if scope else 2)
    assert installation.desired_revision == (2 if scope else 1)


@pytest.mark.asyncio
async def test_scale_in_job_uses_platform_authority_and_current_lease(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, lifecycle_audit_actor,
) -> None:
    organization_id = uuid4()
    provider_member = Caller(
        UserPrincipal(OPERATOR_ID, "member@example.test", organization_id, is_provider_org=True),
        AuthorizationContext(OPERATOR_ID, organization_id, uuid4(), False, frozenset()),
    )
    with pytest.raises(HTTPException) as denied:
        await lifecycle_job.enqueue_cloudflare_disable(
            db_session, provider_member, installation_id=installation.id, expected_revision=1, action="scale_in",
        )
    assert denied.value.status_code == 403
    assert installation.desired_revision == 1

    admin = _platform_operator()
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    job, reused = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="scale_in",
    )
    assert not reused and job.payload_version == 1
    assert installation.desired_state == "scaled_in" and installation.admitted_revision is None
    duplicate, reused = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="scale_in",
    )
    assert reused and duplicate.id == job.id
    token = uuid4()
    job.lease_token = token
    job.status = "running"
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
    running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")
    assert await lifecycle_job.CLOUDFLARE_DISABLE_DEFINITION.handler(running, payload) == {"outcome": "scaled_in"}
    assert installation.api_token_version == 1
    actions = set(await db_session.scalars(select(AuditLog.action).where(AuditLog.resource_id == installation.id)))
    assert {"capacity.cloudflare_scale_in", "capacity.cloudflare_scaled_in"} <= actions


@pytest.mark.asyncio
async def test_disconnect_resumes_owned_script_delete_until_namespace_absence(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, lifecycle_audit_actor,
) -> None:
    admin = _platform_operator()
    resource = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="test-capacity",
        desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="present",
        provider_id=str(uuid4()), provider_version=str(uuid4()),
    )
    db_session.add(resource)
    job, _ = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="disconnect",
    )
    token = uuid4()
    job.lease_token = token
    job.status = "running"
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    intent = ResourceIntent(
        ResourceTarget(installation.id, installation.account_id, "workers", "worker_script", resource.target_name),
        resource.effect_id, 1, resource.artifact_digest,
    )
    script_exists = True
    namespace_exists = True
    mutations = []

    async def remote(request: httpx.Request) -> httpx.Response:
        nonlocal script_exists
        assert request.url.host == "api.cloudflare.com"
        assert request.headers["Authorization"] == "Bearer synthetic-cloudflare-management-token"
        if request.url.path.endswith("/settings"):
            if not script_exists:
                return httpx.Response(404)
            result = {"bindings": [{
                "name": OWNER_BINDING, "type": "plain_text", "text": worker_owner_value(intent),
            }]}
        elif request.url.path.endswith("/deployments"):
            result = {"deployments": [{
                "id": resource.provider_id,
                "versions": [{"percentage": 100, "version_id": resource.provider_version}],
            }]}
        elif request.url.path.endswith("/namespaces"):
            page = int(request.url.params["page"])
            assert page in (1, 2)
            result = [{"id": str(page), "script": resource.target_name if page == 2 and namespace_exists else "other"}]
            return httpx.Response(200, json={
                "success": True, "result": result, "result_info": {"page": page, "total_pages": 2, "total_count": 2},
            })
        else:
            assert request.method == "DELETE" and request.url.path.endswith("/test-capacity")
            if request.url.params.get("force") != "true":
                return httpx.Response(400, json={"success": False, "errors": [{"message": "Worker owns a namespace"}]})
            await db_session.refresh(resource)
            assert resource.state == "deleting"
            script_exists = False
            mutations.append("DELETE")
            return httpx.Response(204)
        return httpx.Response(200, json={"success": True, "result": result})

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
    running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")

    pauses = []

    async def propagate(delay):
        nonlocal namespace_exists
        pauses.append(delay)
        await db_session.refresh(installation)
        assert installation.retired_at is None and installation.api_token_version == 1
        if len(pauses) == 2:
            namespace_exists = False

    monkeypatch.setattr(lifecycle_job, "asyncio", SimpleNamespace(sleep=propagate))
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        adapter = WorkersRestCleanup(http)
        monkeypatch.setattr(lifecycle_job, "WorkersRestCleanup", lambda _http: adapter)
        assert await lifecycle_job.CLOUDFLARE_DISABLE_DEFINITION.handler(running, payload) == {"outcome": "disconnected"}
    await db_session.refresh(resource)
    await db_session.refresh(installation)
    assert mutations == ["DELETE"]
    assert pauses == [2, 4]
    assert resource.state == "absent" and resource.observed_at is not None
    assert installation.retired_at is not None and installation.api_token_version is None
    actions = set(await db_session.scalars(select(AuditLog.action).where(AuditLog.resource_id == installation.id)))
    assert {"capacity.cloudflare_disconnect", "capacity.cloudflare_retired"} <= actions


@pytest.mark.asyncio
async def test_cleanup_waits_for_an_unsettled_create_before_confirming_absence(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    receipt = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="settlement-capacity",
        desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="creating",
    )
    db_session.add(receipt)
    db_session.add(CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="current-capacity",
        desired_revision=2, artifact_digest="d" * 64, effect_id=uuid4(), state="creating",
    ))
    await db_session.flush()
    adapter = SimpleNamespace(observe=AsyncMock(return_value=None), verify_absent=AsyncMock())
    assert await lifecycle_job._cleanup_one(db_session, installation, adapter, before_revision=2) == "progressed"
    assert receipt.observed_at is not None and receipt.state == "creating"
    assert await lifecycle_job._cleanup_one(db_session, installation, adapter, before_revision=2) == "settling"
    adapter.verify_absent.assert_not_awaited()
    receipt.observed_at -= lifecycle_job.CREATE_SETTLEMENT_WINDOW
    assert await lifecycle_job._cleanup_one(db_session, installation, adapter, before_revision=2) == "progressed"
    adapter.verify_absent.assert_awaited_once()
    assert receipt.state == "absent"
    assert await lifecycle_job._cleanup_one(db_session, installation, adapter, before_revision=2) == "complete"


@pytest.mark.asyncio
async def test_cleanup_deadline_excludes_queue_time(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    admin = _platform_operator()
    job, _ = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="scale_in",
    )
    token = uuid4()
    worker_id = uuid4()
    db_session.add(ExecutionWorker(
        id=worker_id, form="external", state="lost", capacity_resource_uid="pending-worker",
        runtime_profile="cloudflare-python-v1",
    ))
    await db_session.flush()
    db_session.add(CloudflareWorkerReservation(
        worker_id=worker_id, installation_id=installation.id, scope="workers",
        deployment_revision=1, control_root_version=1, cleanup_started_at=datetime.now(UTC),
    ))
    job.status = "running"
    job.lease_token = token
    job.created_at = datetime.now(UTC) - timedelta(days=1)
    job.started_at = datetime.now(UTC)
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    class DrainWaiting(Exception):
        pass

    monkeypatch.setattr(lifecycle_job, "asyncio", SimpleNamespace(sleep=AsyncMock(side_effect=DrainWaiting)))
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
    running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")
    with pytest.raises(DrainWaiting):
        await lifecycle_job._settle_disable(running, payload, SimpleNamespace())
    job.started_at -= lifecycle_job.DRAIN_DEADLINE
    await db_session.flush()
    with pytest.raises(PlatformJobRequiresAction) as expired:
        await lifecycle_job._settle_disable(running, payload, SimpleNamespace())
    assert expired.value.result == {
        "code": "CF_DRAIN_TIMEOUT",
        "workers": [{
            "worker_id": str(worker_id), "resource_uid": "pending-worker", "scope": "workers", "revision": 1,
        }],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("workers_remain", [False, True])
async def test_deletion_deadline_requires_action_across_attempts(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, workers_remain: bool,
) -> None:
    admin = _platform_operator()
    job, _ = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="disconnect",
    )
    job.status = "running"
    job.started_at = datetime.now(UTC) - lifecycle_job.DRAIN_DEADLINE
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(lifecycle_job, "cloudflare_workers_remain", AsyncMock(return_value=workers_remain))
    monkeypatch.setattr(lifecycle_job, "_cleanup_one", AsyncMock(side_effect=lifecycle_job.DeletionSettling()))
    monkeypatch.setattr(lifecycle_job, "_cleanup_stranded_workflow_slots", AsyncMock(return_value="settling"))
    diagnose = AsyncMock()
    monkeypatch.setattr(lifecycle_job, "_stranded_timeout_action", diagnose)
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
    for attempt in (1, 2, 3):
        job.attempt = attempt
        job.lease_token = uuid4()
        await db_session.flush()
        running = PlatformJobContext(job.id, job.lease_token, None, str(OPERATOR_ID), admin.principal.email, "")
        with pytest.raises(PlatformJobRequiresAction) as expired:
            await lifecycle_job._settle_disable(running, payload, SimpleNamespace())
        assert expired.value.result == {"code": "CF_CLEANUP_PENDING"}
    diagnose.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_dedupes_schedule_and_closes_admission_on_missing_deployment(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    resources = {}
    for role in ("launcher", "capacity"):
        resource = CloudflareOwnedResource(
            id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
            scope="workers", kind="worker_script", target_name=f"reconcile-{role}",
            desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="present",
            provider_id=str(uuid4()), provider_version=str(uuid4()), namespace_ids={"OwnedNamespace": str(uuid4())},
        )
        resources[resource.target_name] = resource
        db_session.add(resource)
    await db_session.flush()
    now = datetime.now(UTC)
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now)
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now)
    jobs = (await db_session.scalars(select(PlatformJob).where(
        PlatformJob.job_type == lifecycle_job.RECONCILE_JOB,
        PlatformJob.resource_id == str(installation.id),
    ))).all()
    assert len(jobs) == 1 and jobs[0].payload_version == 2
    job = jobs[0]
    token = uuid4()
    job.lease_token = token
    job.status = "running"
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    status_code = 503

    def remote(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/graphql"):
            body = json.loads(request.content)
            assert body["variables"]["scripts"] == sorted(resources)
            return httpx.Response(200, json={"data": {"viewer": {"accounts": [{
                "workersInvocationsAdaptive": [
                    {"dimensions": {"scriptName": name}, "sum": {"requests": 3}} for name in resources
                ],
            }]}}})
        assert request.method == "GET"
        if status_code != 200:
            assert request.url.path.endswith("/settings")
            return httpx.Response(status_code)
        target, operation = request.url.path.split("/")[-2:]
        resource = resources[target]
        if operation == "settings":
            result = {"bindings": [{
                "name": OWNER_BINDING, "type": "plain_text",
                "text": worker_owner_value(lifecycle_job._resource_intent(resource)),
            }]}
        else:
            assert operation == "deployments"
            result = {"deployments": [{
                "id": resource.provider_id,
                "versions": [{"percentage": 100, "version_id": resource.provider_version}],
            }]}
        return httpx.Response(200, json={"success": True, "result": result})

    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    running = PlatformJobContext(job.id, token, None, "system", "", "Bifrost")
    payload = lifecycle_job.CloudflareReconcilePayloadV2.model_validate(job.payload)
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        adapter = WorkersRestCleanup(http)
        monkeypatch.setattr(lifecycle_job, "WorkersRestCleanup", lambda _http: adapter)
        with pytest.raises(PlatformJobFailure) as unavailable:
            await lifecycle_job.CLOUDFLARE_RECONCILE_DEFINITION.handler(running, payload)
        assert unavailable.value.retryable and lifecycle_job.CLOUDFLARE_RECONCILE_DEFINITION.policy.retry_on_failure
        status_code = 200
        assert await lifecycle_job.CLOUDFLARE_RECONCILE_DEFINITION.handler(running, payload) == {"outcome": "verified"}
        status = await get_cloudflare_capacity_status(db_session, _platform_operator())
        assert status.installation is not None and status.installation.admission_open
        assert status.installation.spend is not None and status.installation.spend.analytics is not None
        assert status.installation.spend.analytics.request_difference == Decimal(6)
        status_code = 404
        with pytest.raises(PlatformJobRequiresAction) as missing:
            await lifecycle_job.CLOUDFLARE_RECONCILE_DEFINITION.handler(running, payload)
        assert missing.value.result == {"code": "CF_DEPLOYMENT_UNVERIFIED"}
    await db_session.refresh(installation)
    assert installation.admitted_revision is None


@pytest.mark.asyncio
@pytest.mark.parametrize("primary", ["static", "job", "external"])
async def test_supervisor_reserves_and_retires_without_restarting(
    db_session: AsyncSession, installation: CloudflareInstallation, primary: str, monkeypatch,
) -> None:
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    for module in (controller, external_controller, cloudflare_supervisor, cloudflare_spend):
        monkeypatch.setattr(module, "get_db_context", context)
    monkeypatch.setattr(controller, "_publish_live", AsyncMock(return_value=controller.LiveControllers(
        ids=frozenset(), versions=frozenset({"test"}),
    )))
    await KubernetesExecutionService(db_session).update_policy({"max_workers": 1, "warm_workers": 0}, updated_by="test")
    # Each case supplies the entire primary cohort. The test stack's own
    # in-process workers would otherwise fill demand in the Job and VM cases.
    await db_session.execute(update(ExecutionWorker).values(state="retired"))
    bearer = "test-scope-bearer-" + "a" * 30
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr(bearer), actor=str(OPERATOR_ID), now=datetime.now(UTC),
    ))
    primary_worker = ExecutionWorker(
        id=uuid4(), form=primary, state="ready",
        capacity_scope="vm" if primary == "external" else None,
        heartbeat_at=datetime.now(UTC), supported_handlers=["workflow.execute"],
        memory_budget_bytes=128 * 2**20, memory_reserved_bytes=128 * 2**20,
    )
    db_session.add(primary_worker)
    db_session.add(WorkDispatch(
        family="workflow", handler="workflow.execute", handler_version=1, subject_id=uuid4(),
        message={}, payload={}, cloudflare_revision="c" * 64, memory_request_bytes=64 * 2**20,
        created_at=datetime.now(UTC) - timedelta(minutes=1),
    ))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    await db_session.flush()

    resources = {}
    writes = []

    async def remote(request: httpx.Request) -> httpx.Response:
        if request.url.host == "vm.example.test":
            assert request.headers["Authorization"] == "Bearer " + "p" * 40
            return httpx.Response(200, json={
                "protocol_version": 1, "scope": "vm", "worker_version": "vm-build",
                "memory_limit_bytes": 128 * 2**20, "max_workers": 1, "max_lifetime_seconds": 3600,
            })
        assert request.headers["Authorization"] == f"Bearer {bearer}"
        if request.url.path == "/v1/capabilities":
            return httpx.Response(200, json={
                "protocol_version": 2, "scope": "workers", "worker_version": "test",
                "runtime_profile": "cloudflare-python-v1", "memory_limit_bytes": 256 * 2**20,
                "max_workers": 1, "max_lifetime_seconds": 3600,
            })
        worker_id = request.url.path.split("/")[3]
        if request.method == "GET":
            resource = resources.get(worker_id)
            return httpx.Response(200, json=resource) if resource else httpx.Response(404)
        writes.append(request.method)
        body = json.loads(request.content)
        if request.method == "PUT":
            assert await db_session.get(CloudflareWorkerReservation, UUID(worker_id)) is not None
            resources[worker_id] = {
                "scope": "workers", "worker_id": worker_id, "worker_version": "test",
                "resource_uid": body["resource_uid"], "state": "prepared",
            }
        else:
            assert body["resource_uid"] == resources[worker_id]["resource_uid"]
            resources[worker_id]["state"] = "starting" if request.method == "POST" else "deleted"
        return httpx.Response(200, json=resources[worker_id])

    settings = Settings(
        _env_file=None, secret_key="synthetic-supervisor-test-key-" + "x" * 32,
        worker_capacity_url="https://vm.example.test" if primary == "external" else None,
        worker_capacity_token="p" * 40 if primary == "external" else None,
    )
    controllers = {}
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        async def sweep():
            await cloudflare_supervisor._reconcile_scope(
                ManagedCloudflareScope(installation.id, "workers", 1),
                settings, http, controllers, kubernetes=primary == "job",
            )

        await sweep()
        assert not writes
        installation.desired_state = "enabled"
        installation.admitted_revision = 1
        if primary == "static":
            primary_worker.memory_reserved_bytes = 0
            installation.policy = {
                **installation.policy,
                "limits": {**installation.policy["limits"], "shared_managed_worker_cap": None},
            }
        await db_session.flush()
        if primary == "static":
            await sweep()
            assert not writes
            primary_worker.memory_reserved_bytes = 128 * 2**20
            await db_session.flush()
        await sweep()
        assert writes == ["PUT", "POST"]
        owned = tuple(controllers.values())
        worker_id = UUID(next(iter(resources)))
        db_session.add(CloudflareWorkflowSlot(
            installation_id=installation.id, tenant="platform", ordinal=0, worker_id=worker_id,
            execution_id=uuid4(), dispatch_id=uuid4(), fence=uuid4(), sent=True,
        ))
        installation.desired_state = "scaled_in"
        installation.admitted_revision = None
        installation.desired_revision = 2
        db_session.add(CloudflareCapacityScope(
            installation_id=installation.id, scope="workers", endpoint="https://new-capacity.example.test",
            deployment_revision=2, bearer_version=1,
        ))
        await db_session.flush()
        assert set(await cloudflare_supervisor._installations()) == {
            ManagedCloudflareScope(installation.id, "workers", revision) for revision in (1, 2)
        }
        await sweep()
        assert tuple(controllers.values()) == owned
        assert writes == ["PUT", "POST", "DELETE"]
        worker = await db_session.get(ExecutionWorker, worker_id)
        assert worker is not None
        await db_session.refresh(worker)
        assert worker.capacity_resource_deleted_at is not None
        assert await db_session.scalar(select(CloudflareWorkflowSlot.fence).where(
            CloudflareWorkflowSlot.worker_id == worker_id,
        )) is None


@pytest.fixture
async def installation(db_session: AsyncSession) -> CloudflareInstallation:
    installation_id = await create_installation(db_session, "a" * 32)
    row = await db_session.get(CloudflareInstallation, installation_id)
    assert row is not None
    return row


async def create_installation(
    db_session: AsyncSession, account_id: str, *, consent_version: str = CONSENT_TEXT_VERSION,
    rated_slots: int = 2,
) -> UUID:
    zero = dict.fromkeys(WORKERS_RATES, Decimal(0))
    return await create_cloudflare_installation(
        db_session,
        account_id=account_id,
        worker_ingress_origin="https://workers.example.test",
        consent_version=consent_version,
        actor_id=OPERATOR_ID,
        verified_api_token=SecretStr("synthetic-cloudflare-management-token"),
        policy=CloudflareCapacityPolicy(
            workers=CloudflareWorkersPolicy(
                launcher_cap=1, slots_per_launcher=2, max_lifetime_seconds=3600, work_kinds=frozenset({"workflow"}),
            ),
            monthly_limit_usd=Decimal("10.00"),
            shared_managed_worker_cap=8,
        ),
        bounds=WorkersReleaseMeterBounds(
            60, zero, zero, "fixture-release", rated_slots, TEST_LAUNCHER_CAPS, TEST_CPU_LIMITS, 500_000, TEST_ORDER,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", [
    "stale-hash", "missing-pin", "solution-generation", "solution-version", "solution-outbound",
    "cross-org", "wrong-approval-owner", "tenant-owned-global-approval",
    "stale-egress", "different-provider-version", "overdue-account", "overdue-other-account",
    "cold-canary",
])
async def test_container_reservation_rejects_unreviewed_source_and_scope(
    db_session, installation, reason,
) -> None:
    from src.models.contracts.cloudflare_approval import SolutionApprovalScope
    from src.models.orm.cloudflare_approvals import CloudflareApproval
    from src.models.orm.file_index import FileIndex, SolutionFileIndex
    from src.models.orm.solutions import Solution
    from src.models.orm.workflows import Workflow
    from src.services.worker_pool.cloudflare_containers_admission import (
        LINUX_REVIEW_VERSION,
        ContainerAdmissionRefused,
        LinuxReview,
        linux_review_digest,
        reserve_container,
        reserve_container_canary,
    )

    db = db_session
    release, _, _ = make_release()
    now = datetime.now(UTC)
    owner, other = uuid4(), uuid4()
    workflow_id, run_id = uuid4(), uuid4()
    primary = cloudflare_admission.PrimaryPool("external", scope=f"fixture-{workflow_id}", ceiling=1)
    owner_id = None if reason == "tenant-owned-global-approval" else owner
    origin = installation.worker_ingress_origin
    installation.desired_state = "enabled"
    installation.admitted_revision = installation.containers_ready_revision = 1
    installation.containers_release_digest = release.digest
    installation.containers_policy = container_policy().model_copy(update={"enabled": True}).model_dump(mode="json")
    installation.policy = {**installation.policy, "limits": {**installation.policy["limits"], "monthly_limit_usd": "500"}}
    db.add_all(Organization(id=tenant, name=str(tenant), created_by="fixture") for tenant in (owner, other))
    db.add(ExecutionWorker(
        id=uuid4(), form=primary.form, capacity_scope=primary.scope, state="ready", heartbeat_at=now,
        memory_budget_bytes=128, memory_reserved_bytes=128,
    ))
    db.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "containers", 1),
        SecretStr("synthetic-container-scope-bearer-key"), actor=str(OPERATOR_ID), now=now,
    ))
    await db.flush()
    db.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="containers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    application = app_journal(installation, release.digest, now)
    db.add(application)
    path, digest = f"workflows/{workflow_id}.py", "a" * 64
    solution = None
    solution_scope = None
    if reason.startswith("solution-"):
        generation = uuid4()
        solution = Solution(
            id=uuid4(), slug="fixture", name="fixture", organization_id=owner,
            runtime_source_generation=generation, version="1.0",
        )
        db.add(solution)
        await db.flush()
        db.add(SolutionFileIndex(solution_id=solution.id, path=path, content_hash=digest))
        solution_scope = SolutionApprovalScope(install_id=solution.id, source_generation=generation, version="1.0")
    else:
        db.add(FileIndex(path=path, content_hash=digest))
    db.add(Workflow(
        id=workflow_id, name="fixture", function_name="run", path=path, organization_id=owner_id,
        solution_id=solution.id if solution is not None else None,
    ))
    await db.flush()
    execution = Execution(
        id=run_id, workflow_id=workflow_id, organization_id=owner, workflow_name="fixture",
        executed_by_name="fixture", status=ExecutionStatus.PENDING,
    )
    db.add(execution)
    review = LinuxReview(
        findings=frozenset({"CF_NATIVE"}), release_digest=release.digest, installation_id=installation.id,
        deployment_revision=1, consent_version=installation.consent_version, worker_ingress_origin=origin,
        egress="worker-ingress-only-v1", path=path, function_name="run", source_pins={path: digest}, wheels={},
        solution=solution_scope, memory_bytes=128, timeout_seconds=60, ingress_bytes=100, outbound_bytes=100,
        previous_revision=None, inactive_history_count=0,
    ).model_dump(mode="json")
    approval = CloudflareApproval(
        revision=linux_review_digest(workflow_id, owner_id, review), workflow_id=workflow_id,
        organization_id=owner_id, policy_version=LINUX_REVIEW_VERSION, accepted_by="fixture", declaration=review,
    )
    db.add(approval)
    stage_dispatch(
        db, family=WAKE_WORKFLOW, subject_id=run_id, handler="workflow.execute", handler_version=1,
        payload={"execution_id": str(run_id)}, message={}, memory_request_bytes=128, queued_at=now - timedelta(seconds=10),
    )
    await db.flush()
    admission = {
        "installation_id": installation.id, "controller_id": uuid4(), "primary": primary,
        "release": release, "verified_egress": await verified_egress(installation.id, 1, release.digest, origin),
    }
    # Prove the baseline is admissible before changing only the evidence under test.
    async with db.begin_nested() as nested:
        reserved = await reserve_container(db, **admission)
        assert isinstance(reserved, CloudflareWorkerReservation)
        worker = await db.get(ExecutionWorker, reserved.worker_id)
        assert worker is not None and worker.capacity_resource_uid is not None
        assert UUID(hex=worker.capacity_resource_uid).hex == worker.capacity_resource_uid
        await nested.rollback()
    if reason == "cold-canary":
        from src.models.orm.platform_jobs import PlatformJob
        from src.services.worker_pool.cloudflare_containers_egress import (
            REQUIRED_DENIALS,
            verify_container_egress,
        )
        from src.worker.remote.protocol import nonce_digest

        installation.admitted_revision = installation.containers_ready_revision = None
        installation.containers_release_digest = None
        job_id, lease_token = uuid4(), uuid4()
        db.add(PlatformJob(
            id=job_id, job_type="capacity.cloudflare_enable", title="Container qualification",
            payload={"revision": 1}, status="running", lease_token=lease_token, lease_expires_at=now + timedelta(minutes=5),
            resource_type="cloudflare_installation", resource_id=str(installation.id),
            requested_by_user_id=str(OPERATOR_ID), requested_by_name="fixture", requested_by_email="fixture@example.test",
        ))
        await db.flush()
        assert await reserve_container(db, **{**admission, "verified_egress": None}) is None
        canary_args = {key: admission[key] for key in ("installation_id", "controller_id", "release")}
        assert await reserve_container_canary(db, **canary_args, job_id=job_id, lease_token=uuid4()) is None
        canary = await reserve_container_canary(db, **canary_args, job_id=job_id, lease_token=lease_token)
        assert isinstance(canary, CloudflareWorkerReservation) and canary.canary_job_id == job_id
        worker = await db.get(ExecutionWorker, canary.worker_id)
        assert worker is not None and worker.state == "starting" and worker.reserved_for_dispatch_id is None
        assert await reserve_container_canary(db, **canary_args, job_id=job_id, lease_token=lease_token) is None

        async def provider(request):
            assert request.url.path == "/v1/egress-probe"
            body = json.loads(request.content)
            assert body["pin"]["worker"] == str(worker.id)
            assert body["pin"]["startDeadline"] - body["pin"]["reservedAt"] <= 15_000
            nonce = body["nonce"]
            # Stand in for the dedicated ingress committing its independently authenticated hello.
            worker.remote_auth_nonce_digest = nonce_digest(nonce)
            worker.remote_owner_lease_expires_at = now + timedelta(minutes=1)
            await db.flush()
            return httpx.Response(200, json={
                "nonce": nonce, "worker_id": str(worker.id), "application_id": application.provider_id,
                "provider_version": application.provider_version, "origin": origin, "internet_enabled": False,
                "intercepted_origins": [origin], "bounded_relay": True, "denied": sorted(REQUIRED_DENIALS),
            })

        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
            admission["verified_egress"] = await verify_container_egress(
                db, http, installation_id=installation.id, revision=1, release_digest=release.digest,
                origin=origin, probe_worker_id=canary.worker_id,
            )
        assert admission["verified_egress"] is not None
        installation.admitted_revision = installation.containers_ready_revision = 1
        installation.containers_release_digest = release.digest
    elif reason == "stale-hash":
        await db.execute(update(FileIndex).where(FileIndex.path == path).values(content_hash="b" * 64))
    elif reason == "missing-pin":
        await db.execute(delete(FileIndex).where(FileIndex.path == path))
    elif reason == "cross-org":
        execution.organization_id = other
    elif reason == "stale-egress":
        admission["verified_egress"] = await verified_egress(
            installation.id, 1, release.digest, origin, observed_at=now - timedelta(minutes=6),
        )
    elif reason == "different-provider-version":
        application.provider_version = "f" * 32
    elif reason.startswith("overdue-"):
        prior_id, prior_worker = uuid4(), uuid4()
        account = installation.account_id if reason == "overdue-account" else "b" * 32
        db.add(CloudflareInstallation(
            id=prior_id, account_id=account, worker_ingress_origin=origin,
            consent_version=installation.consent_version, consent_actor="fixture", consent_at=now,
            desired_state="disconnected", desired_revision=1, retired_at=now, policy=installation.policy,
        ))
        db.add(ExecutionWorker(id=prior_worker, form="external", state="lost", capacity_scope="containers"))
        await db.flush()
        db.add(seal_credential(
            CredentialRef(prior_id, account, "control_root", "", 1),
            SecretStr("fixture-control-root"), actor="fixture", now=now,
        ))
        await db.flush()
        db.add(CloudflareWorkerReservation(
            worker_id=prior_worker, installation_id=prior_id, scope="containers",
            deployment_revision=1, control_root_version=1, container_tenant="platform",
            container_release=release.digest, container_reserved_at=now - timedelta(hours=1),
            container_envelope={"seconds": 600, "fixed_usd": "0.01", "second_usd": "0.001"},
        ))
    elif reason.endswith("approval-owner") or reason == "tenant-owned-global-approval":
        approval.organization_id = owner if owner_id is None else other
        approval.revision = linux_review_digest(workflow_id, approval.organization_id, review)
    else:
        assert solution is not None
        if reason == "solution-generation":
            solution.runtime_source_generation = uuid4()
        elif reason == "solution-version":
            solution.version = "2.0"
        else:
            solution.allow_outbound_access = True
    await db.flush()
    result = await reserve_container(db, **admission)
    if reason == "overdue-account":
        assert isinstance(result, ContainerAdmissionRefused) and result.code == "CF_CONTAINERS_CLEANUP_PENDING"
    elif reason in ("overdue-other-account", "cold-canary"):
        assert isinstance(result, CloudflareWorkerReservation)
    else:
        assert result is None
    if reason.startswith("overdue-"):
        status = await get_cloudflare_capacity_status(db, _platform_operator())
        assert status.installation is not None and status.installation.spend is not None
        assert any(
            "CF_CONTAINERS_CLEANUP_PENDING" in note for note in status.installation.spend.coverage
        ) == (reason == "overdue-account")


@pytest.mark.asyncio
@pytest.mark.parametrize("meter_complete", [False, True])
async def test_capacity_reports_overdue_unsettled_containers(db_session, installation, meter_complete) -> None:
    from src.services.worker_pool.cloudflare_containers_rest import ContainersRest
    from src.services.worker_pool.cloudflare_containers_settlement import (
        ClosedContainerStart,
        settle_container_stop,
    )
    from src.services.worker_pool.cloudflare_provisioning import (
        OwnershipConflict,
        ResourceIntent,
        ResourceObservation,
        ResourceTarget,
    )

    now = datetime.now(UTC)
    workers = [uuid4() for _ in range(3)]
    uid, actor, app_id, namespace = uuid4().hex, "b" * 64, "c" * 32, "d" * 32
    name = f"bifrost-{installation.id.hex}-r1"
    image = f"registry.cloudflare.com/{installation.account_id}/{name}@sha256:{'a' * 64}"
    release, effect = "a" * 64, uuid4()
    target = ResourceTarget(installation.id, installation.account_id, "containers", "container_application", name)
    app = ResourceObservation(ResourceIntent(target, effect, 1, release), app_id, namespace)
    db_session.add(CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id, scope="containers",
        kind=target.kind, target_name=name, desired_revision=1, artifact_digest=release, effect_id=effect,
        provider_id=app_id, provider_version=namespace, state="present", observed_at=now,
    ))
    db_session.add_all(ExecutionWorker(
        id=worker, form="external", state="lost", capacity_resource_uid=uid if i == 0 else uuid4().hex,
        capacity_scope="containers",
    ) for i, worker in enumerate(workers))
    await db_session.flush()
    for index, worker in enumerate(workers):
        reserved_at = now - timedelta(hours=1) if index < 2 else now
        db_session.add(CloudflareWorkerReservation(
            worker_id=worker, installation_id=installation.id, scope="containers",
            deployment_revision=1, control_root_version=1, container_tenant="platform",
            container_release=release, container_reserved_at=reserved_at,
            container_envelope={
                "seconds": 600, "fixed_usd": "0.01", "second_usd": "0.001", "control_usd": "0.001",
                "outbound_gb_usd": "0.05", "provider_instance_id": actor, "prepared_image": image,
            },
            container_stopped_at=reserved_at + timedelta(seconds=1) if index == 1 else None,
        ))
    await db_session.flush()
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None
    assert status.installation.owned_resources_pending == 2
    assert status.installation.spend is not None
    assert any("Unresolved cleanup for 1 Cloudflare Container" in note for note in status.installation.spend.coverage)
    started_at = (now - timedelta(minutes=55)).replace(second=0, microsecond=0)
    stopped_at = started_at + timedelta(seconds=2)
    phase = "provisioning"
    placement_ids = [actor]
    management_available = True

    def management(request: httpx.Request) -> httpx.Response:
        assert management_available, "Settlement replay must not contact management"
        assert request.method == "GET"
        if request.url.path.endswith("/instances-v2"):
            assert request.url.params["name_prefix"] == actor
            result = [{
                "id": placement, "application_id": app_id, "image": image, "started_at": started_at.isoformat(),
                "status": {"state": phase, "updated_at": stopped_at.isoformat()},
            } for placement in placement_ids]
        else:
            assert request.url.path.endswith(f"/applications/{app_id}")
            result = {
                "id": app_id, "account_id": installation.account_id, "name": name,
                "scheduling_policy": "durable_object", "durable_objects": {"namespace_id": namespace},
            }
        return httpx.Response(200, json={"success": True, "result": result})

    closure = ClosedContainerStart(
        app, workers[0], uid, actor, image, started_at, started_at, 200, meter_complete=meter_complete,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(management)) as http:
        adapter = ContainersRest(http)
        token = SecretStr("synthetic-management-token")
        with pytest.raises(ObservationPending):
            await settle_container_stop(db_session, adapter, token=token, closure=closure)
        phase = "inactive"
        placement_ids = [actor, "e" * 64]
        with pytest.raises(OwnershipConflict, match="multiple placements"):
            await settle_container_stop(db_session, adapter, token=token, closure=closure)
        placement_ids = ["e" * 64]
        with pytest.raises(OwnershipConflict, match="differs"):
            await settle_container_stop(db_session, adapter, token=token, closure=closure)
        placement_ids = [actor]
        installation.desired_state = "disconnected"
        await db_session.flush()
        await settle_container_stop(db_session, adapter, token=token, closure=closure)
        management_available = False
        await settle_container_stop(db_session, adapter, token=token, closure=closure)
    settled = await db_session.get(CloudflareWorkerReservation, workers[0])
    assert settled is not None and settled.container_envelope is not None
    assert settled.container_stopped_at == stopped_at
    if meter_complete:
        assert settled.container_envelope["measured_outbound_bytes"] == 200
        assert Decimal(settled.container_envelope["settled_fixed_usd"]) == Decimal("0.00100001")
    else:
        assert "settled_fixed_usd" not in settled.container_envelope
        assert settled.container_envelope["fixed_usd"] == "0.01"
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None and status.installation.owned_resources_pending == 1


@pytest.mark.asyncio
async def test_container_source_authority_follows_the_durable_workflow_claim(db_session, installation) -> None:
    from shared.file_access import FileCaller, FileServiceError
    from shared.sdk_files import sdk_read_file, sdk_write_file
    from shared.sdk_modules import ModuleSourceError, ModuleSourceScope
    from src.models.orm.cloudflare_approvals import CloudflareApproval
    from src.models.orm.execution_workers import ExecutionWorkerClaim
    from src.models.orm.workflows import Workflow
    from src.services.worker_pool.cloudflare_containers_admission import (
        LINUX_REVIEW_VERSION,
        LinuxReview,
        linux_review_digest,
    )
    from src.services.worker_pool.cloudflare_containers_source import (
        scoped_container_source,
    )

    release, _, _ = make_release()
    worker, workflow_id, subject = uuid4(), uuid4(), uuid4()
    now = datetime.now(UTC)
    path = "workflows/native.py"
    workflow = Workflow(id=workflow_id, name="fixture", function_name="run", path=path)
    db_session.add_all([workflow, ExecutionWorker(id=worker, form="external", state="ready", capacity_scope="containers")])
    await db_session.flush()
    review = LinuxReview(
        findings=frozenset({"CF_NATIVE"}), release_digest=release.digest, installation_id=installation.id,
        deployment_revision=1, consent_version=installation.consent_version,
        worker_ingress_origin=installation.worker_ingress_origin, egress="worker-ingress-only-v1",
        path=path, function_name="run", source_pins={path: "a" * 64}, wheels={},
        memory_bytes=128, timeout_seconds=60, ingress_bytes=100, outbound_bytes=100,
        previous_revision=None, inactive_history_count=0,
    ).model_dump(mode="json")
    revision = linux_review_digest(workflow_id, None, review)
    approval = CloudflareApproval(
        revision=revision, workflow_id=workflow_id, policy_version=LINUX_REVIEW_VERSION,
        accepted_by="fixture", declaration=review,
    )
    db_session.add_all([
        approval,
        ExecutionWorkerClaim(worker_id=worker, subject_id=subject, family=WAKE_WORKFLOW),
        CloudflareWorkerReservation(
            worker_id=worker, installation_id=installation.id, scope="containers", deployment_revision=1,
            control_root_version=1, container_tenant="platform", container_release=release.digest,
            container_reserved_at=now,
            container_envelope={"seconds": 300, "approval": revision, "execution_id": str(subject)},
        ),
    ])
    await db_session.flush()
    runtime = SimpleNamespace(kind="workflow_execution", credential_id=subject, workflow_id=workflow_id, organization_id=None)
    scope = ModuleSourceScope(solution_id=None, global_repo_access=True)
    assert (await scoped_container_source(db_session, runtime, workflow, scope)).source_hashes == {path: "a" * 64}
    principal = UserPrincipal(user_id=OPERATOR_ID, email="fixture@example.test", organization_id=None)
    principal.runtime_authority = runtime
    caller = FileCaller(principal, db_session, None)
    with pytest.raises(FileServiceError, match="Workspace files are unavailable") as read:
        await sdk_read_file(caller, path=path, location="workspace", scope=None)
    assert read.value.status_code == 403
    with pytest.raises(FileServiceError, match="Workspace files are unavailable") as write:
        await sdk_write_file(caller, path=path, content="fixture", binary=False, location="workspace", scope=None)
    assert write.value.status_code == 403
    with pytest.raises(ModuleSourceError, match="authority has expired or changed"):
        await scoped_container_source(db_session, runtime, None, scope)
    approval.is_current = False
    await db_session.flush()
    with pytest.raises(ModuleSourceError, match="approval is unavailable"):
        await scoped_container_source(db_session, runtime, workflow, scope)
    await db_session.execute(delete(ExecutionWorkerClaim).where(ExecutionWorkerClaim.worker_id == worker))
    with pytest.raises(ModuleSourceError, match="authority has expired or changed"):
        await scoped_container_source(db_session, runtime, workflow, scope)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["deadline", "settled", "release"])
async def test_managed_container_ownership_fences_live_sessions(db_session, installation, monkeypatch, change) -> None:
    from src.worker.remote import ownership
    from src.worker.remote.fencing import (
        LeaseRef,
        fence_remote_owner,
        remote_owner_context,
    )

    now = datetime.now(UTC)
    worker, owner, launcher = uuid4(), uuid4(), uuid4()
    digest = "a" * 64
    db_session.add(ExecutionWorker(
        id=worker, form="external", state="ready", capacity_scope="containers", capacity_resource_uid=uuid4().hex,
    ))
    await db_session.flush()
    pin = CloudflareWorkerReservation(
        worker_id=worker, installation_id=installation.id, scope="containers", deployment_revision=1,
        control_root_version=1, container_tenant="platform", container_release=digest,
        container_reserved_at=now - timedelta(minutes=2), container_envelope={"seconds": 600},
    )
    db_session.add(pin)
    await db_session.flush()

    @asynccontextmanager
    async def database():
        yield db_session

    monkeypatch.setattr(ownership, "get_liveness_db_context", database)
    lease = await ownership.acquire_remote_owner(
        worker_id=worker, owner_id=owner, launcher_id=launcher, nonce_digest="a" * 64,
        hello_sequence=1, container_release=digest,
    )
    assert lease is not None
    assert await ownership.renew_remote_owner(lease)
    assert await ownership.owns_remote_session(lease)
    if change == "deadline":
        pin.container_envelope = {"seconds": 60}
    elif change == "settled":
        pin.container_stopped_at = now
    else:
        pin.container_release = "b" * 64
    await db_session.flush()
    assert not await ownership.renew_remote_owner(lease)
    assert not await ownership.owns_remote_session(lease)
    with remote_owner_context(LeaseRef(lease)):
        assert not await fence_remote_owner(db_session)


@pytest.mark.asyncio
async def test_disconnect_preserves_unresolved_container_cleanup(db_session, installation) -> None:
    from src.services.worker_pool.cloudflare_containers_admission import (
        ContainerAdmissionRefused,
        reserve_container,
    )
    from src.services.worker_pool.cloudflare_containers_policy import (
        container_account_usage,
    )
    from src.services.worker_pool.cloudflare_containers_rest import ContainersRest
    from src.services.worker_pool.cloudflare_containers_settlement import (
        VoidContainerStart,
        settle_container_stop,
    )
    from src.services.worker_pool.cloudflare_policy import (
        CloudflarePolicyConflict,
        retire_cloudflare_installation,
    )
    from src.services.worker_pool.cloudflare_provisioning import (
        ResourceIntent,
        ResourceObservation,
        ResourceTarget,
    )

    now = datetime.now(UTC)
    worker, uid = uuid4(), uuid4().hex
    actor = "e" * 64
    release, _, _ = make_release()
    origin = installation.worker_ingress_origin
    installation.desired_state = "enabled"
    installation.admitted_revision = installation.containers_ready_revision = 1
    installation.containers_release_digest = release.digest
    installation.containers_policy = container_policy().model_copy(update={"enabled": True}).model_dump(mode="json")
    installation.policy = {**installation.policy, "limits": {**installation.policy["limits"], "monthly_limit_usd": "500"}}
    journal = app_journal(installation, release.digest, now)
    db_session.add(journal)
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "containers", 1),
        SecretStr("synthetic-container-scope-bearer-key"), actor=str(OPERATOR_ID), now=now,
    ))
    db_session.add(ExecutionWorker(
        id=worker, form="external", state="lost", capacity_scope="containers",
        capacity_resource_uid=uid, capacity_resource_deleted_at=now,
    ))
    await db_session.flush()
    scope = CloudflareCapacityScope(
        installation_id=installation.id, scope="containers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    )
    db_session.add(scope)
    deadline = now - timedelta(minutes=9)
    pin = CloudflareWorkerReservation(
        worker_id=worker, installation_id=installation.id, scope="containers", deployment_revision=1,
        control_root_version=1, container_tenant="platform", container_release=release.digest,
        container_reserved_at=now - timedelta(minutes=10),
        container_envelope={
            "seconds": 300, "fixed_usd": "0.01", "second_usd": "0.001", "start_deadline": deadline.isoformat(),
        },
    )
    db_session.add(pin)
    await db_session.flush()
    admission = {
        "installation_id": installation.id, "controller_id": uuid4(),
        "primary": cloudflare_admission.PrimaryPool("external", "fixture", 1),
        "release": release, "verified_egress": await verified_egress(installation.id, 1, release.digest, origin),
    }
    blocked = await reserve_container(db_session, **admission)
    assert isinstance(blocked, ContainerAdmissionRefused) and blocked.code == "CF_CONTAINERS_CLEANUP_PENDING"
    installation.desired_state = "disconnected"
    installation.admitted_revision = None
    await db_session.flush()
    from src.services.worker_pool.cloudflare_policy import cloudflare_workers_remain

    assert await cloudflare_workers_remain(db_session, installation.id)
    with pytest.raises(CloudflarePolicyConflict, match="cleanup is pending"):
        await retire_cloudflare_installation(db_session, installation_id=installation.id, expected_revision=1)
    assert installation.api_token_version is not None
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None and status.installation.owned_resources_pending == 2
    assert journal.provider_id is not None and journal.provider_version is not None
    target = ResourceTarget(installation.id, installation.account_id, "containers", journal.kind, journal.target_name)
    app = ResourceObservation(
        ResourceIntent(target, journal.effect_id, 1, release.digest), journal.provider_id, journal.provider_version,
    )
    when = int(deadline.timestamp() * 1000)
    receipt = VoidContainerStart(app, worker, uid, actor, when, when + 1000)
    placements = [{"id": actor}]
    available = True

    def management(request):
        assert available
        if request.url.path.endswith("/instances-v2"):
            assert request.url.params["name_prefix"] == actor
            result = placements
        else:
            result = {
                "id": app.provider_id, "name": target.name, "account_id": target.account_id,
                "scheduling_policy": "durable_object", "durable_objects": {"namespace_id": app.provider_version},
            }
        return httpx.Response(200, json={"success": True, "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(management)) as http:
        rest = ContainersRest(http)
        token = SecretStr("synthetic-management-token")
        with pytest.raises(ObservationPending, match="still has a provider placement"):
            await settle_container_stop(db_session, rest, token=token, closure=receipt)
        placements.clear()
        await settle_container_stop(db_session, rest, token=token, closure=receipt)
        available = False
        await settle_container_stop(db_session, rest, token=token, closure=receipt)
    assert not await cloudflare_workers_remain(db_session, installation.id)
    assert await db_session.scalar(select(container_account_usage(literal(deadline))).where(
        CloudflareInstallation.id == installation.id,
    )) == Decimal("0.01")
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    await db_session.flush()
    assert not isinstance(await reserve_container(db_session, **admission), ContainerAdmissionRefused)
    installation.desired_state = "disconnected"
    installation.admitted_revision = None
    journal.state = "absent"
    await db_session.delete(scope)
    await db_session.flush()
    await retire_cloudflare_installation(db_session, installation_id=installation.id, expected_revision=1)
    assert installation.retired_at is not None


@pytest.fixture
async def isolated_capacity_session_factory(async_engine, monkeypatch):
    """Keep committed synthetic dispatches private to the test's claimers."""
    from src.core import database

    url = async_engine.url
    if url.host == "pgbouncer":
        # The transaction pool exposes the application database only.
        url = url.set(host="postgres")
    name = f"capacity_test_{uuid4().hex}"
    admin = create_async_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool)
    race_engine = create_async_engine(url.set(database=name), poolclass=NullPool)
    try:
        async with admin.connect() as connection:
            await connection.execute(text(f"CREATE DATABASE {name} TEMPLATE bifrost_test_template"))
            try:
                contexts = async_sessionmaker(race_engine, expire_on_commit=False, autoflush=False)
                with monkeypatch.context() as patch:
                    patch.setattr(database, "_async_session_factory", contexts)
                    patch.setattr(database, "_dispatch_session_factory", contexts)
                    patch.setattr(database, "_liveness_session_factory", contexts)
                    yield async_sessionmaker(race_engine, expire_on_commit=False)
            finally:
                await race_engine.dispose()
                await connection.execute(text(f"DROP DATABASE {name} WITH (FORCE)"))
    finally:
        await admin.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("race", [
    "installation", "tenant", "prewarm",
    "claim-installation", "claim-tenant", "prepare-installation", "prepare-tenant",
])
async def test_container_reservation_serializes_shared_capacity(isolated_capacity_session_factory, monkeypatch, race) -> None:
    from src.models.orm.cloudflare_approvals import CloudflareApproval
    from src.models.orm.file_index import FileIndex
    from src.models.orm.workflows import Workflow
    from src.services.worker_pool.cloudflare_containers_admission import (
        LINUX_REVIEW_VERSION,
        LinuxReview,
        linux_review_digest,
        reserve_container,
    )
    from src.services.worker_pool.cloudflare_containers_policy import (
        container_account_usage,
    )

    release, _, _ = make_release()
    sessions = isolated_capacity_session_factory
    account, primary, warmer = uuid4().hex, uuid4(), uuid4()
    primary_pool = cloudflare_admission.PrimaryPool("external", scope=f"fixture-{primary}", ceiling=1)
    worker_contender = race.startswith(("claim-", "prepare-"))
    shared_tenant = race.endswith("tenant")
    seeded_seat = race in {"prewarm", "claim-installation", "prepare-installation"}
    installation_slots = 2 if seeded_seat else 9
    tenants = [uuid4() for _ in range(3)]
    execution_ids = [uuid4() for _ in range(3)]
    workflow_ids = [uuid4() for _ in range(3)]
    paths = [f"workflows/{workflow}.py" for workflow in workflow_ids]
    placement_tenants = [tenants[0]] * 3 if shared_tenant else tenants
    monkeypatch.setattr(cloudflare_admission, "installed_workers_release_digest", lambda: "fixture-release")
    async with sessions() as db:
        now = await db.scalar(select(func.clock_timestamp()))
        assert now is not None
        installation_id = await create_installation(db, account)
        installation = await db.get(CloudflareInstallation, installation_id)
        assert installation is not None
        original = installation.policy["limits"]
        installation.policy = {
            **installation.policy, "limits": {
                **original, "monthly_limit_usd": "500",
                "workers": {
                    **original["workers"], "installation_slots": installation_slots,
                    "slots_per_tenant": 1 if shared_tenant else 4, "queue_prewarm_per_tenant": 1,
                },
            },
        }
        installation.desired_state = "enabled"
        installation.admitted_revision = 1
        installation.containers_ready_revision = 1
        installation.containers_release_digest = release.digest
        installation.containers_policy = container_policy().model_copy(update={"enabled": True}).model_dump(mode="json")
        db.add(seal_credential(
            CredentialRef(installation_id, account, "scope_bearer", "containers", 1),
            SecretStr("synthetic-scope-bearer"), actor=str(OPERATOR_ID), now=now,
        ))
        db.add_all(Organization(id=tenant, name=str(tenant), created_by="fixture") for tenant in tenants)
        db.add(ExecutionWorker(
            id=primary, form=primary_pool.form, capacity_scope=primary_pool.scope,
            state="ready", heartbeat_at=now,
            memory_budget_bytes=128, memory_reserved_bytes=128,
        ))
        await db.flush()
        db.add(CloudflareCapacityScope(
            installation_id=installation_id, scope="containers", endpoint="https://capacity.example.test",
            deployment_revision=1, bearer_version=1,
        ))
        db.add(app_journal(installation, release.digest, now))
        if seeded_seat or worker_contender:
            db.add(ExecutionWorker(
                id=warmer, form="external", state="ready", runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
                capacity_scope="workers", memory_limit_bytes=128 * 2**20, version="fixture-release",
                remote_owner_lease_expires_at=now + timedelta(minutes=5),
            ))
            await db.flush()
        if worker_contender:
            db.add(seal_credential(
                CredentialRef(installation_id, account, "scope_bearer", "workers", 1),
                SecretStr("synthetic-workers-bearer"), actor=str(OPERATOR_ID), now=now,
            ))
            await db.flush()
            db.add(CloudflareCapacityScope(
                installation_id=installation_id, scope="workers", endpoint="https://workers.example.test",
                deployment_revision=1, bearer_version=1,
            ))
            db.add(ManagedCloudflareScope(installation_id, "workers", 1).pin(warmer, 1))
        for workflow_id, path, tenant in zip(workflow_ids, paths, placement_tenants, strict=True):
            db.add(Workflow(
                id=workflow_id, name=f"native-{workflow_id}", function_name="run", path=path,
                organization_id=None if workflow_id == workflow_ids[0] else tenant,
            ))
            db.add(FileIndex(path=path, content_hash="a" * 64))
        await db.flush()
        for index, (workflow_id, path, tenant, run) in enumerate(zip(
            workflow_ids, paths, placement_tenants, execution_ids, strict=True,
        )):
            declaration = LinuxReview(
                findings=frozenset({"CF_NATIVE"}), release_digest=release.digest, installation_id=installation_id,
                deployment_revision=1, consent_version=installation.consent_version,
                worker_ingress_origin=installation.worker_ingress_origin, egress="worker-ingress-only-v1",
                path=path, function_name="run", source_pins={path: "a" * 64}, wheels={},
                memory_bytes=128, timeout_seconds=60, ingress_bytes=100, outbound_bytes=100,
                previous_revision=None, inactive_history_count=0,
            ).model_dump(mode="json")
            owner = None if index == 0 else tenant
            db.add(CloudflareApproval(
                revision=linux_review_digest(workflow_id, owner, declaration), workflow_id=workflow_id,
                organization_id=owner, policy_version=LINUX_REVIEW_VERSION, accepted_by="fixture",
                declaration=declaration,
            ))
            db.add(Execution(
                id=run, workflow_id=workflow_id, organization_id=tenant, workflow_name="native",
                executed_by_name="fixture", status=ExecutionStatus.PENDING,
            ))
            dispatch = stage_dispatch(
                db, family=WAKE_WORKFLOW, subject_id=run, handler="workflow.execute", handler_version=1,
                payload={"execution_id": str(run)}, message={}, memory_request_bytes=128,
                queued_at=now - timedelta(seconds=10 - index),
            )
            if worker_contender and index == 1:
                dispatch.cloudflare_revision = "b" * 64
            if seeded_seat and index == 2:
                prewarming = race == "prewarm"
                if prewarming:
                    dispatch.cloudflare_revision = "b" * 64
                await db.flush()
                db.add(CloudflareWorkflowSlot(
                    installation_id=installation_id, tenant=str(tenant), worker_id=warmer, ordinal=0,
                    execution_id=run, dispatch_id=dispatch.id, fence=uuid4(), sent=not prewarming,
                    prewarm_fence=uuid4() if prewarming else None,
                    released_at=now if prewarming else None,
                    warm_until=now + timedelta(minutes=2) if prewarming else None,
                ))
        await db.commit()

    contender_pids: list[int] = []
    both_connected = asyncio.Event()
    egress = await verified_egress(installation_id, 1, release.digest, "https://workers.example.test")

    async def record_connection(db):
        pid = await db.scalar(select(func.pg_backend_pid()))
        assert pid is not None
        contender_pids.append(pid)
        if len(contender_pids) == 2:
            both_connected.set()
        return pid

    @asynccontextmanager
    async def observe_claim_connection():
        async with sessions() as db:
            await record_connection(db)
            yield db
            await db.commit()

    async def compete() -> bool:
        if race.startswith("claim-"):
            return await claim_next(warmer, fits_bytes=128 * 2**20, budget_bytes=128 * 2**20) is not None
        if race.startswith("prepare-"):
            async with sessions() as db:
                await record_connection(db)
                await db.scalar(select(CloudflareInstallation.id).where(
                    CloudflareInstallation.id == installation_id,
                ).with_for_update(key_share=True))
                admission = await read_workflow_slot_admission(db, installation_id, workers_policy)
                preparations = await stage_queue_preparations(
                    db, admission, warmer, placement_tenants[1], launcher_slots=1, occupied_launcher_slots=0,
                )
                await db.commit()
                return bool(preparations)
        return await reserve(contender=True)

    async def reserve(*, contender: bool = False) -> bool:
        async with sessions() as db:
            if contender:
                await record_connection(db)
            result = await reserve_container(
                db, installation_id=installation_id, controller_id=uuid4(),
                primary=primary_pool, release=release, verified_egress=egress,
            )
            if result is not None and (race != "installation" or not contender):
                assert result.container_tenant == str(placement_tenants[0])
            await db.commit()
            return result is not None

    async with sessions() as db:
        saved = await db.get(CloudflareInstallation, installation_id)
        assert saved is not None
        workers_policy = CloudflareWorkersPolicy.model_validate(saved.policy["limits"]["workers"])
        assert await db.scalar(select(cloudflare_admission.workers_release_matches()).where(
            CloudflareInstallation.id == installation_id,
        )) is True
        assert await db.scalar(select(cloudflare_estimated_usage()).where(
            CloudflareInstallation.id == installation_id,
        )) is not None
        assert await reserve_container(
            db, installation_id=installation_id, controller_id=uuid4(),
            primary=primary_pool, release=release, verified_egress=None,
        ) is None
    if race == "installation":
        assert await reserve()
    monkeypatch.setattr(claim_service, "get_db_context", observe_claim_connection)
    async with sessions() as blocker:
        await blocker.scalar(select(CloudflareInstallation.id).where(
            CloudflareInstallation.id == installation_id,
        ).with_for_update(key_share=True))
        # Both claimers connect before the blocker releases the row.
        first = asyncio.create_task(reserve(contender=True))
        contenders = [first, asyncio.create_task(compete())]
        try:
            async with asyncio.timeout(5), sessions() as observer:
                blocker_pid = await blocker.scalar(select(func.pg_backend_pid()))
                await both_connected.wait()
                while True:
                    chains = [await observer.scalar(select(func.pg_blocking_pids(pid))) for pid in contender_pids]
                    if any(blocker_pid in chain for chain in chains) and all(
                        set(chain) & {*contender_pids, blocker_pid} for chain in chains
                    ):
                        break
                    assert not any(task.done() for task in contenders), "reservation bypassed the lock"
                    await asyncio.sleep(0.01)
        finally:
            await blocker.rollback()
            results = await asyncio.gather(*contenders)
    assert len(set(contender_pids)) == 2
    assert sum(results) == 1
    async with sessions() as db:
        saved = await db.get(CloudflareInstallation, installation_id)
        assert saved is not None
        slots = await read_workflow_slot_admission(
            db, installation_id, CloudflareWorkersPolicy.model_validate(saved.policy["limits"]["workers"]),
        )
        assert slots.occupied.total() == (1 if shared_tenant else 2)
        charge = await db.scalar(select(cloudflare_estimated_usage()).where(
            CloudflareInstallation.id == installation_id,
        ))
        assert charge is not None
        if results[0] or not worker_contender:
            assert charge > 0
            month_end = usage_period(now)[1]
            current, end_of_month = (await db.execute(select(
                container_account_usage(literal(now)),
                container_account_usage(literal(month_end - timedelta(microseconds=1))),
            ).where(CloudflareInstallation.id == installation_id))).one()
            assert current == end_of_month > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("release", ["fixture-release", "d" * 64])
async def test_upgrade_migration_pauses_prior_pilot_and_scheduler_pins_the_new_release(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, release,
) -> None:
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    await db_session.flush()

    scripts = ScriptDirectory(str(Path(__file__).resolve().parents[3] / "alembic"))
    migration = scripts.get_revision("20261121_cf_workers_upgrade")
    assert migration is not None

    def migrate(connection: Connection) -> None:
        # The seed transaction defers credential FKs. A release migration
        # runs after those triggers have settled in the prior transaction.
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        with Operations.context(MigrationContext.configure(connection)):
            migration.module.downgrade()
            migration.module.upgrade()

    connection = await db_session.connection()
    await connection.run_sync(migrate)
    await db_session.refresh(installation)
    assert installation.deployed_revision == 1
    assert installation.admitted_revision is None
    assert installation.desired_revision == 2
    assert installation.drain_required
    assert installation.workers_release_sequence is None

    monkeypatch.setattr(lifecycle_job, "installed_workers_release_digest", lambda: release)
    now = datetime.now(UTC)
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now)
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now)
    job = (await db_session.scalars(select(PlatformJob).where(
        PlatformJob.job_type == lifecycle_job.RECONCILE_JOB,
        PlatformJob.resource_id == str(installation.id),
    ))).one()
    assert job.requested_by_user_id == "system"
    assert job.payload == {"installation_id": str(installation.id), "revision": 3, "operation": "upgrade", "reset_release_fence": False}
    assert installation.upgrade_release == release and installation.admitted_revision is None


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "code", "recoverable"), [
    ("requires_action", "CF_DRAIN_TIMEOUT", True),
    ("requires_action", "CF_CLEANUP_PENDING", True),
    ("requires_action", "CF_RELEASE_MISMATCH", True),
    ("failed", "CF_PROVIDER_UNAVAILABLE", True),
    ("requires_action", "CF_SPEND_UNVERIFIED", True),
    ("failed", "CF_CONNECTOR_UNAVAILABLE", True),
    ("failed", "unsupported_payload_version", True),
    ("failed", "runner_shutdown", True),
    ("failed", "runner_lost", True),
    ("failed", "runner_exited", True),
    ("failed", "timeout", True),
    ("requires_action", "CF_WORKER_INGRESS_UNVERIFIED", False),
    ("requires_action", "CF_BUDGET_REACHED", False),
    ("requires_action", "CF_POLICY_RELEASE_CHANGED", False),
    ("requires_action", "CF_MANAGEMENT_CREDENTIAL", False),
    ("requires_action", "CF_OWNERSHIP_CONFLICT", False),
    ("requires_action", "CF_SLOT_STOP_UNVERIFIED", False),
])
async def test_upgrade_scheduler_only_requeues_self_healing_actions(
    db_session: AsyncSession, installation: CloudflareInstallation, status, code, recoverable,
) -> None:
    installation.desired_state = "enabled"
    installation.upgrade_release = "fixture-release"
    now = datetime.now(UTC)
    count = select(func.count()).select_from(PlatformJob).where(
        PlatformJob.job_type == lifecycle_job.RECONCILE_JOB,
        PlatformJob.resource_id == str(installation.id),
    )
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now)
    job = await db_session.scalar(select(PlatformJob).where(PlatformJob.resource_id == str(installation.id)))
    assert job is not None
    def complete(job: PlatformJob, at: datetime) -> None:
        job.status, job.completed_at = status, at
        if status == "requires_action":
            job.result = {"code": code}
        else:
            job.error_code = code

    complete(job, now)
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now + timedelta(minutes=4))
    assert await db_session.scalar(count) == 1
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now + timedelta(minutes=5))
    assert await db_session.scalar(count) == (2 if recoverable else 1)
    if recoverable:
        requeued = await db_session.scalar(select(PlatformJob).where(
            PlatformJob.resource_id == str(installation.id), PlatformJob.id != job.id,
        ))
        assert requeued is not None
        requeued.created_at = now + timedelta(minutes=5)
        complete(requeued, requeued.created_at)
        await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now + timedelta(minutes=14))
        assert await db_session.scalar(count) == 2
        await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now + timedelta(minutes=15))
        assert await db_session.scalar(count) == 3
    else:
        await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now + timedelta(days=1))
        assert await db_session.scalar(count) == 1


@pytest.mark.asyncio
async def test_upgrade_and_reconcile_have_distinct_operator_receipts(
    db_session: AsyncSession, installation: CloudflareInstallation, lifecycle_audit_actor,
) -> None:
    installation.desired_state = "enabled"
    installation.upgrade_release = "fixture-release"
    admin = _platform_operator()
    arguments = {"installation_id": installation.id, "expected_revision": 1}
    reconcile, _ = await lifecycle_job.enqueue_cloudflare_reconcile(db_session, admin, **arguments)
    upgrade, _ = await lifecycle_job.enqueue_cloudflare_reconcile(db_session, admin, **arguments, upgrade=True)
    repeated, reused = await lifecycle_job.enqueue_cloudflare_reconcile(db_session, admin, **arguments, upgrade=True)
    assert reconcile.id != upgrade.id == repeated.id and reused
    assert reconcile.payload["operation"] == "upgrade"
    assert upgrade.payload["operation"] == "upgrade"


@pytest.mark.asyncio
async def test_upgrade_scheduler_does_not_supersede_operator_enable(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    installation.deployed_revision, installation.desired_revision = 1, 2
    installation.drain_required = True
    job, _ = await enable_job.enqueue_cloudflare_enable(
        db_session, _platform_operator(), installation_id=installation.id, expected_revision=2,
    )
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=datetime.now(UTC))
    assert installation.desired_revision == job.payload["revision"] == 2
    assert installation.upgrade_release is None and installation.drain_required
    assert await db_session.scalar(select(func.count()).select_from(PlatformJob).where(
        PlatformJob.resource_id == str(installation.id),
    )) == 1


@pytest.mark.asyncio
async def test_scale_in_ends_cutover_repair_even_if_enable_fails(
    db_session: AsyncSession, installation: CloudflareInstallation, lifecycle_audit_actor,
) -> None:
    installation.desired_state = "enabled"
    installation.deployed_revision, installation.desired_revision = 1, 2
    installation.drain_required = True
    operator = _platform_operator()
    scale_in, _ = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, operator, installation_id=installation.id, expected_revision=2, action="scale_in",
    )
    assert not installation.drain_required
    scale_in.status = "succeeded"
    job, _ = await enable_job.enqueue_cloudflare_enable(
        db_session, operator, installation_id=installation.id, expected_revision=3,
    )
    job.status = "failed"
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=datetime.now(UTC))
    assert installation.desired_revision == job.payload["revision"] == 3
    assert installation.upgrade_release is None
    assert await db_session.scalar(select(PlatformJob.id).where(
        PlatformJob.resource_id == str(installation.id), PlatformJob.job_type == lifecycle_job.RECONCILE_JOB,
    )) is None


@pytest.mark.asyncio
async def test_upgrade_policy_save_fences_older_replicas_without_enqueuing_upgrade(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    installation.desired_state = "enabled"
    installation.deployed_revision = installation.admitted_revision = 1
    zero = dict.fromkeys(WORKERS_RATES, Decimal(0))
    policy = CloudflareCapacityPolicy.model_validate(installation.policy["limits"])
    newer = WorkersReleaseOrder("1.3.0", 100)
    older = WorkersReleaseOrder("1.2.5", 101)
    bounds = WorkersReleaseMeterBounds(
        60, zero, zero, "current", 2, TEST_LAUNCHER_CAPS, TEST_CPU_LIMITS, 500_000, newer,
    )
    revision = await save_cloudflare_policy(
        db_session, installation_id=installation.id, expected_revision=1, policy=policy, bounds=bounds,
    )
    assert revision == 2
    assert installation.workers_release_version == newer.version
    assert installation.workers_release_sequence == newer.sequence
    assert installation.workers_release_digest == "current"
    for digest, order in (("current", newer), ("old", older)):
        monkeypatch.setattr(lifecycle_job, "installed_workers_release_digest", lambda: digest)
        monkeypatch.setattr(lifecycle_job, "installed_workers_release_order", lambda: order)
        await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=datetime.now(UTC))
    assert installation.desired_revision == revision
    assert installation.upgrade_release is None
    assert await db_session.scalar(select(PlatformJob.id).where(
        PlatformJob.resource_id == str(installation.id),
    )) is None


@pytest.mark.asyncio
async def test_upgrade_conflict_requires_an_audited_reset(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, lifecycle_audit_actor,
) -> None:
    installation.desired_state = "enabled"
    installation.deployed_revision = installation.admitted_revision = 1
    monkeypatch.setattr(lifecycle_job, "installed_workers_release_digest", lambda: "conflicting")
    monkeypatch.setattr(lifecycle_job, "installed_workers_release_order", lambda: WorkersReleaseOrder("1.0.0", 2))
    now = datetime.now(UTC)
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now)
    assert installation.admitted_revision == 1 and installation.upgrade_release is None
    job, _ = await lifecycle_job.enqueue_cloudflare_reconcile(
        db_session, _platform_operator(), installation_id=installation.id, expected_revision=1,
        reset_release_fence=True,
    )
    assert job.payload["operation"] == "upgrade" and job.payload["reset_release_fence"]
    assert installation.admitted_revision is None and installation.upgrade_release == "conflicting"
    assert installation.workers_release_sequence == 2
    audit = await db_session.scalar(select(AuditLog).where(
        AuditLog.resource_id == installation.id,
        AuditLog.details["reset_release_fence"].as_boolean().is_(True),
    ))
    assert audit is not None and audit.details["previous_digest"] == "fixture-release"


@pytest.mark.asyncio
async def test_upgrade_scheduler_resets_backoff_after_steady_state_success(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    installation.desired_state = "enabled"
    installation.deployed_revision = installation.admitted_revision = 1
    now = datetime.now(UTC)
    jobs = select(PlatformJob).where(PlatformJob.resource_id == str(installation.id))
    for index, status in enumerate(("failed", "succeeded", "requires_action")):
        at = now + timedelta(minutes=index * 5)
        await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=at)
        job = await db_session.scalar(jobs.where(PlatformJob.status == "queued"))
        assert job is not None
        job.created_at = job.completed_at = at
        job.status = status
        job.error_code = "CF_PROVIDER_UNAVAILABLE" if status == "failed" else None
        job.result = {"code": "CF_SPEND_UNVERIFIED"} if status == "requires_action" else None
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now + timedelta(minutes=14))
    assert await db_session.scalar(jobs.where(PlatformJob.status == "queued")) is None
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=now + timedelta(minutes=15))
    assert await db_session.scalar(jobs.where(PlatformJob.status == "queued")) is not None


@pytest.mark.asyncio
async def test_missing_local_release_leaves_shared_admission_unchanged(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    monkeypatch.setattr(lifecycle_job, "installed_workers_release_digest", lambda: None)
    await lifecycle_job.schedule_cloudflare_reconcile(db_session, now=datetime.now(UTC))
    assert installation.admitted_revision == 1
    assert await db_session.scalar(select(PlatformJob.id).where(
        PlatformJob.resource_id == str(installation.id),
    )) is not None


@pytest.mark.asyncio
async def test_usage_envelope_migration_preserves_saved_policy(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    scripts = ScriptDirectory(str(Path(__file__).resolve().parents[3] / "alembic"))
    migration = scripts.get_revision("20261120_cf_usage_only_envelope")
    assert migration is not None
    module = migration.module
    original = {
        **installation.policy,
        "rate_table_version": WORKERS_LEGACY_RATE_TABLE_VERSION,
        "fleet_envelope_usd": "13.31",
    }
    installation.policy = original
    await db_session.flush()
    connection = await db_session.connection()

    def migrate(sync_connection: Connection, operation: Callable[[], None]) -> None:
        with Operations.context(MigrationContext.configure(sync_connection)):
            operation()

    await connection.run_sync(migrate, module.downgrade)
    await db_session.refresh(installation)
    assert installation.policy == {**original, "fleet_envelope_usd": "18.31"}
    await connection.run_sync(migrate, module.upgrade)
    await db_session.refresh(installation)
    assert installation.policy == original


@pytest.mark.asyncio
async def test_workflow_slots_hold_headroom_and_serialize_different_tenants(isolated_capacity_session_factory, monkeypatch) -> None:
    workers = [uuid4(), uuid4(), uuid4()]
    tenants = [uuid4(), uuid4()]
    runs = [uuid4() for _ in range(7)]
    budget = 512 * 2**20
    now = datetime.now(UTC)
    async with isolated_capacity_session_factory() as db:
        account = uuid4().hex
        installation_id = await create_installation(db, account)
        installation = await db.get(CloudflareInstallation, installation_id)
        assert installation is not None
        limits = installation.policy["limits"]
        installation.policy = {
            **installation.policy,
            "limits": {**limits, "workers": {**limits["workers"], "installation_slots": 5}},
        }
        installation.desired_state = "enabled"
        installation.admitted_revision = 1
        db.add(seal_credential(
            CredentialRef(installation_id, account, "scope_bearer", "workers", 1),
            SecretStr("synthetic-scope-bearer"), actor=str(OPERATOR_ID), now=now,
        ))
        db.add_all(ExecutionWorker(
            id=worker, form="external", state="ready", capacity_scope="workers",
            runtime_profile="cloudflare-python-v1", memory_limit_bytes=budget,
            remote_owner_lease_expires_at=now + timedelta(minutes=5),
        ) for worker in workers)
        db.add_all(Organization(id=tenant, name=str(tenant), created_by="slot test") for tenant in tenants)
        await db.flush()
        db.add(CloudflareCapacityScope(
            installation_id=installation_id, scope="workers", endpoint="https://capacity.example.test",
            deployment_revision=1, bearer_version=1,
        ))
        db.add_all(CloudflareWorkerReservation(
            worker_id=worker, installation_id=installation_id, scope="workers",
            deployment_revision=1, control_root_version=1,
        ) for worker in workers)
        for index, (execution, tenant) in enumerate(zip(runs, [None] * 5 + tenants, strict=True)):
            db.add(Execution(id=execution, organization_id=tenant, workflow_name="slot", executed_by_name="fixture"))
            stage_dispatch(
                db, family=WAKE_WORKFLOW, subject_id=execution, handler="workflow.execute", handler_version=1,
                payload={"execution_id": str(execution)}, message={}, memory_request_bytes=1,
                cloudflare_revision="a" * 64, queued_at=now + timedelta(microseconds=index),
            )
        await db.commit()

    initial_workers = [workers[0], workers[2], workers[0], workers[1]]
    initial = [await claim_next(worker, fits_bytes=budget, budget_bytes=budget) for worker in initial_workers]
    assert all(claim is not None and claim.tenant_id is None for claim in initial)
    tickets = [claim.workflow_slot for claim in initial if claim is not None]
    assert [ticket.ordinal for ticket in tickets if ticket is not None] == [0, 1, 2, 3]
    contender_pids: list[int] = []
    both_connected = asyncio.Event()
    original_context = claim_service.get_db_context

    @asynccontextmanager
    async def observe_connection():
        async with original_context() as db:
            contender_pids.append(await db.scalar(select(func.pg_backend_pid())))
            if len(contender_pids) == 2:
                both_connected.set()
            yield db

    async with isolated_capacity_session_factory() as blocker, isolated_capacity_session_factory() as observer:
        await blocker.scalar(select(CloudflareInstallation.id).where(
            CloudflareInstallation.id == installation_id,
        ).with_for_update(key_share=True))
        root_pid = await blocker.scalar(select(func.pg_backend_pid()))
        with monkeypatch.context() as patch:
            patch.setattr(claim_service, "get_db_context", observe_connection)
            contenders = [asyncio.create_task(claim_next(worker, fits_bytes=budget, budget_bytes=budget))
                          for worker in workers[:2]]
            try:
                async with asyncio.timeout(5):
                    await both_connected.wait()
                    while True:
                        blocked_by = [await observer.scalar(select(func.pg_blocking_pids(pid))) for pid in contender_pids]
                        if any(root_pid in chain for chain in blocked_by) and all(
                            set(chain) & {*contender_pids, root_pid} for chain in blocked_by
                        ):
                            break
                        assert not any(task.done() for task in contenders), "claim bypassed the installation lock"
                        await asyncio.sleep(0.01)
            finally:
                await blocker.rollback()
                results = await asyncio.gather(*contenders)
    winners = [claim for claim in results if claim is not None]
    assert len(winners) == 1
    assert winners[0].tenant_id in tenants
    async with isolated_capacity_session_factory() as db:
        assert await db.scalar(select(func.count()).select_from(CloudflareWorkflowSlot).where(
            CloudflareWorkflowSlot.installation_id == installation_id, CloudflareWorkflowSlot.released_at.is_(None),
        )) == 5
        # Completed and definitively refused attempts release their exact
        # tickets. A stale fence and an uncertain stop leave theirs held.
        first, second, third, fourth = tickets
        assert first is not None and second is not None and third is not None and fourth is not None
        for index, ticket in enumerate((first, second, third)):
            worker = initial_workers[index]
            assert await fence_workflow_slot_send(db, worker, runs[index], ticket, tenant_id=None, preparation=False)
        await confirm_workflow_slot_release(db, workers[0], runs[0], uuid4())
        await settle_workflow_slot(db, workers[2], runs[1], second.fence, "completed")
        await settle_workflow_slot(db, workers[0], runs[2], third.fence, "uncertain")
        await db.execute(update(ExecutionWorker).where(ExecutionWorker.id == workers[1]).values(
            remote_owner_lease_expires_at=now - timedelta(seconds=1),
        ))
        await db.commit()
    callback = AsyncMock()
    pool = ThinRemoteProcessPool(
        SimpleNamespace(managed_cloudflare=True), worker_id=str(workers[0]),
        runtime_profile="cloudflare-python-v1", on_result=callback,
    )
    with monkeypatch.context() as patch:
        patch.setattr(pool, "_owns_session", AsyncMock(return_value=True))
        await pool.handle_event({
            "event": "result", "_event_id": str(uuid4()), "runtime_handle": "confirmed-stop",
            "route_id": first.workflow_route(runs[0]), "attempt_state": "stopped",
            "result": {"success": False, "error_type": "CancelledError"},
        })
    callback.assert_not_awaited()
    async with isolated_capacity_session_factory() as db:
        assert await db.scalar(select(CloudflareWorkflowSlot.released_at).where(
            CloudflareWorkflowSlot.fence == first.fence,
        )) is not None
    reused = await claim_next(workers[2], fits_bytes=budget, budget_bytes=budget)
    assert reused is not None and reused.subject_id == runs[4]
    assert reused.workflow_slot is not None and reused.workflow_slot.ordinal == 1
    assert reused.claimed is not None
    async with isolated_capacity_session_factory() as db:
        assert await fence_workflow_slot_send(
            db, workers[2], reused.subject_id, reused.workflow_slot, tenant_id=None, preparation=False,
        )
        await db.commit()
    with monkeypatch.context() as patch:
        patch.setattr("src.services.execution.workflow_dispatch.hand_off", AsyncMock())
        await release_workflow_run(
            worker_id=workers[2], message=reused.message, payload={**reused.claimed, "sync": True},
            memory_request_bytes=reused.memory_request_bytes, queued_at=reused.queued_at,
            cloudflare_revision=reused.cloudflare_revision, release_opaque_reservation=lambda: None,
            workflow_slot=reused.workflow_slot,
        )
    async with isolated_capacity_session_factory() as db:
        assert await db.scalar(select(CloudflareWorkflowSlot.released_at).where(
            CloudflareWorkflowSlot.fence == reused.workflow_slot.fence,
        )) is not None
        assert await db.scalar(select(CloudflareWorkflowSlot.released_at).where(
            CloudflareWorkflowSlot.fence == third.fence,
        )) is None
        assert await db.scalar(select(CloudflareWorkflowSlot.fence).where(
            CloudflareWorkflowSlot.fence == fourth.fence,
        )) is None
    reclaimed = await claim_next(workers[2], fits_bytes=budget, budget_bytes=budget)
    assert reclaimed is not None and reclaimed.subject_id == reused.subject_id
    async with isolated_capacity_session_factory() as db:
        await db.execute(update(ExecutionWorker).where(ExecutionWorker.id.in_([workers[0], workers[2]])).values(
            state="lost",
        ))
        await end_runs_of_lost_worker(db, workers[0])
        await end_runs_of_lost_worker(db, workers[2])
        remaining = set(await db.scalars(select(CloudflareWorkflowSlot.fence).where(
            CloudflareWorkflowSlot.worker_id.in_([workers[0], workers[2]]),
            CloudflareWorkflowSlot.released_at.is_(None),
        )))
        assert remaining == {third.fence}
        await db.rollback()


@pytest.mark.asyncio
async def test_queue_prewarm_respects_older_tenants_and_releases_on_drain(db_session, installation) -> None:
    now = datetime.now(UTC)
    tenants = [uuid4() for _ in range(3)]
    workers = [uuid4() for _ in tenants]
    db_session.add_all(Organization(id=tenant, name=str(tenant), created_by="fixture") for tenant in tenants)
    db_session.add_all(ExecutionWorker(id=worker, form="external", state="ready") for worker in workers)
    await db_session.flush()
    for tenant, worker, count in zip(tenants, workers, (1, 2, 2), strict=True):
        db_session.add_all(CloudflareWorkflowSlot(
            installation_id=installation.id, tenant=str(tenant), ordinal=i, worker_id=worker,
            execution_id=uuid4(), dispatch_id=uuid4(), fence=uuid4(), sent=True,
        ) for i in range(count))
    db_session.add(CloudflareWorkflowSlot(
        installation_id=installation.id, tenant=str(tenants[0]), ordinal=1, worker_id=workers[1],
        execution_id=uuid4(), dispatch_id=uuid4(), fence=uuid4(), released_at=now,
        warm_until=now + timedelta(seconds=90),
    ))
    for index, tenant in enumerate([*tenants[1:], *([tenants[0]] * 3)]):
        execution = uuid4()
        db_session.add(Execution(id=execution, organization_id=tenant, workflow_name="fixture", executed_by_name="fixture"))
        stage_dispatch(
            db_session, family=WAKE_WORKFLOW, subject_id=execution, handler="workflow.execute", handler_version=1,
            payload={"execution_id": str(execution)}, message={}, memory_request_bytes=1,
            cloudflare_revision="a" * 64, queued_at=now + timedelta(microseconds=index),
        )
    await db_session.flush()
    policy = CloudflareWorkersPolicy(
        launcher_cap=3, slots_per_launcher=4, max_lifetime_seconds=3600, queue_prewarm_per_tenant=3,
        work_kinds=frozenset({"workflow"}),
    )
    admission = await read_workflow_slot_admission(db_session, installation.id, policy)
    assert admission.occupied.total() == 5 and admission.prewarm_count(
        str(tenants[0]), prepared=0, launcher_free=3,
    ) == 3  # Filling all three seats would move the installation to N-1.
    assert not await stage_queue_preparations(
        db_session, admission, workers[0], tenants[0], launcher_slots=4, occupied_launcher_slots=1,
    )
    await db_session.execute(update(Execution).where(
        Execution.organization_id.in_(tenants[1:]),
    ).values(status=ExecutionStatus.CANCELLED))
    hints = await stage_queue_preparations(
        db_session, admission, workers[0], tenants[0], launcher_slots=2, occupied_launcher_slots=1,
    )
    await db_session.flush()
    assert [hint.ordinal for hint in hints] == [2]  # Keep ordinal 1 warm on the other launcher.
    assert await count_released_prewarms(db_session, workers[0]) == 1
    pinned = hints[0].execution_id
    for orphan in (
        update(Execution).where(Execution.id == pinned).values(status=ExecutionStatus.CANCELLED),
        update(WorkDispatch).where(WorkDispatch.subject_id == pinned).values(assigned_worker_id=workers[1]),
    ):
        savepoint = await db_session.begin_nested()
        await db_session.execute(orphan)
        assert await count_released_prewarms(db_session, workers[0]) == 0
        current = await read_workflow_slot_admission(db_session, installation.id, policy)
        assert current.occupied.total() == 5
        assert await db_session.scalar(select(
            adoptable_workflow_slot(current, workers[0], literal(str(tenants[0]))),
        ).select_from(WorkDispatch).where(WorkDispatch.subject_id != pinned).limit(1)) is None
        await savepoint.rollback()
    await db_session.execute(update(ExecutionWorker).where(ExecutionWorker.id == workers[0]).values(state="draining"))
    assert await count_released_prewarms(db_session, workers[0]) == 0
    admission = await read_workflow_slot_admission(db_session, installation.id, policy)
    assert admission.occupied.total() == 5  # Running workflows still own their seats.


@pytest.mark.asyncio
@pytest.mark.parametrize("limited_budget", [False, True], ids=["burst", "concurrent-budget"])
async def test_queue_depth_prepares_four_workflows_and_settles_their_usage(
    isolated_capacity_session_factory, monkeypatch, limited_budget: bool,
) -> None:
    """The API claim and control host use a real ledger with an in-process launcher."""
    worker, tenant = uuid4(), uuid4()
    lease = RemoteOwnerLease(
        worker_id=worker, owner_id=uuid4(), launcher_id=uuid4(), session_generation=1, declared_slots=4,
    )
    runs = [uuid4() for _ in range(4)]
    budget = 4 * CLOUDFLARE_DEV_BUDGET_BYTES
    now = datetime.now(UTC)
    async with isolated_capacity_session_factory() as db:
        account = uuid4().hex
        installation_id = await create_installation(db, account, rated_slots=4)
        installation = await db.get(CloudflareInstallation, installation_id)
        assert installation is not None
        limits = installation.policy["limits"]
        installation.policy = {
            **installation.policy,
            "limits": {**limits, "workers": {
                **limits["workers"], "slots_per_launcher": 4, "queue_prewarm_per_tenant": 3,
                "installation_slots": 5,
            }},
        }
        installation.desired_state = "enabled"
        installation.admitted_revision = 1
        db.add(seal_credential(
            CredentialRef(installation_id, account, "scope_bearer", "workers", 1),
            SecretStr("synthetic-scope-bearer"), actor=str(OPERATOR_ID), now=now,
        ))
        db.add(ExecutionWorker(
            id=worker, form="external", state="ready", capacity_scope="workers",
            runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, memory_limit_bytes=budget,
            remote_owner_id=lease.owner_id, remote_launcher_id=lease.launcher_id,
            remote_session_generation=lease.session_generation,
            remote_owner_lease_expires_at=now + timedelta(minutes=5),
        ))
        db.add(Organization(id=tenant, name=str(tenant), created_by="fixture"))
        await db.flush()
        db.add(CloudflareCapacityScope(
            installation_id=installation_id, scope="workers", endpoint="https://capacity.example.test",
            deployment_revision=1, bearer_version=1,
        ))
        db.add(CloudflareWorkerReservation(
            worker_id=worker, installation_id=installation_id, scope="workers",
            deployment_revision=1, control_root_version=1,
        ))
        await db.flush()
        usage = await db.get(CloudflareUsageIncarnation, worker)
        assert usage is not None  # The reservation trigger creates the ledger row.
        usage.meter_rate_version = WORKERS_RATE_TABLE_VERSION
        usage.meter_release_digest = "f" * 64
        usage.meter_bounds = {
            **installation.policy["meter_launcher"], "linger_seconds": 90,
            "cpu_expected_ms": {**TEST_CPU_LIMITS, "boot": 10, "warm": 5},
        }
        for index, execution in enumerate(runs):
            db.add(Execution(id=execution, organization_id=tenant, workflow_name="fixture", executed_by_name="fixture"))
            stage_dispatch(
                db, family=WAKE_WORKFLOW, subject_id=execution, handler="workflow.execute", handler_version=1,
                payload={"execution_id": str(execution)}, message={}, memory_request_bytes=1,
                cloudflare_revision="a" * 64, queued_at=now + timedelta(microseconds=index),
            )
        await db.commit()

    outgoing = []
    generation_results = []
    all_prepared = asyncio.Event()
    all_authorized = asyncio.Event()
    async def send(frame):
        command = json.loads(frame)["payload"]
        if command["action"] == "prepare_workflow":
            outgoing.append(command)
            if len(outgoing) == 4:
                all_prepared.set()
        else:
            assert command["action"] == "cloudflare_generation_reply"
            generation_results.append(command["payload"])
            await bridge.receive({"reply": command["id"], "result": {}})
            if len(generation_results) == 4:
                all_authorized.set()

    bridge = Bridge(SimpleNamespace(send_text=send), {
        "hostname": "fixture", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, lease)
    bridge.inventory = {"admission_prepare": True, "queue_prewarm": True}
    bridge.reconciliation_ready.set()
    executor = ApiExecutor(bridge, worker)
    monkeypatch.setattr(executor, "_run_claim", AsyncMock())

    async def claim():
        with remote_owner_context(LeaseRef(lease)):
            result = await claim_next(
                worker, fits_bytes=budget, budget_bytes=budget, queue_prewarm_slots=executor._queue_prewarm_slots,
            )
        assert result is not None and result.workflow_slot is not None
        return result

    try:
        first = await claim()
        assert len(first.queue_preparations) == 3
        if limited_budget:
            async with isolated_capacity_session_factory() as db:
                fixed_now = await db.scalar(select(func.clock_timestamp()))
                assert fixed_now is not None
                snapshot = await db.get(CloudflareInstallation, installation_id)
                assert snapshot is not None
                estimate = select(cloudflare_estimated_usage(fixed_now)).where(CloudflareInstallation.id == installation_id)
                before = await db.scalar(estimate)
                assert before is not None
                monkeypatch.setattr(cloudflare_spend, "_ledger_now", AsyncMock(return_value=fixed_now))
                monkeypatch.setattr(cloudflare_spend, "cloudflare_estimated_usage",
                                    lambda *_args: cloudflare_estimated_usage(fixed_now))
                probe = await db.begin_nested()
                hint = first.queue_preparations[0]
                assert await reserve_tenant_host_usage(
                    db, worker, hint.prepare_by, tenant_id=_cloudflare_tenant_key(tenant, worker, hint.ordinal),
                    route_id=f"prepare:{hint.fence}", queue_warm_until=hint.warm_until,
                )
                after_one = await db.scalar(estimate)
                assert after_one is not None and after_one > before
                await probe.rollback()
                limit = before + (after_one - before) * Decimal("1.5")
                snapshot.policy = {
                    **snapshot.policy, "limits": {**snapshot.policy["limits"], "monthly_limit_usd": str(limit)},
                }
                await db.commit()

            barrier = asyncio.Barrier(2)
            pids = set()
            written_at = {}
            route_lock_ms = {}
            from src.worker.remote import host as remote_host

            async def concurrent_reserve(db, *args, route_id, **kwargs):
                pids.add(await db.scalar(select(func.pg_backend_pid())))
                await barrier.wait()
                result = await reserve_tenant_host_usage(db, *args, route_id=route_id, **kwargs)
                # Start immediately after the activity-row upsert has returned.
                written_at[route_id] = perf_counter()
                return result

            monkeypatch.setattr(remote_host, "reserve_tenant_host_usage", concurrent_reserve)

            async def book(hint):
                route_id = f"prepare:{hint.fence}"
                try:
                    await bridge._reserve_cloudflare_usage(
                        route_id, _cloudflare_tenant_key(tenant, worker, hint.ordinal), hint.prepare_by,
                        engine_slot=hint.ordinal, org_id=str(tenant), queue_warm_until=hint.warm_until,
                    )
                finally:
                    if route_id in written_at:
                        route_lock_ms[route_id] = (perf_counter() - written_at[route_id]) * 1000

            outcomes = await asyncio.gather(*(book(hint) for hint in first.queue_preparations[:2]), return_exceptions=True)
            assert len(pids) == 2
            assert outcomes.count(None) == 1
            assert sum(isinstance(result, CredentialRouteChanged) for result in outcomes) == 1
            async with isolated_capacity_session_factory() as db:
                activity = await db.get(CloudflareUsageActivity, (installation_id, usage_period(fixed_now)[0]))
                assert activity is not None and activity.qualified_preparations == 1
                meter = cloudflare_spend.CloudflareDispatchMeter
                routes = (await db.scalars(select(meter.route_id).where(meter.worker_id == worker))).all()
                assert len(routes) == 1
                assert await db.scalar(estimate) == after_one
                print(f"prewarm lock after upsert through commit: {route_lock_ms[routes[0]]:.2f} ms")
            return
        speculative = first.queue_preparations[0]
        with monkeypatch.context() as older_runner:
            older_runner.setattr(cloudflare_admission, "installed_workers_release_digest", lambda: "older-release")
            with pytest.raises(CredentialRouteChanged, match="preparation admission closed"):
                await bridge._reserve_cloudflare_usage(
                    f"prepare:{speculative.fence}", _cloudflare_tenant_key(tenant, worker, speculative.ordinal),
                    speculative.prepare_by, engine_slot=speculative.ordinal, org_id=str(tenant),
                    queue_warm_until=speculative.warm_until,
                )

        slot = first.workflow_slot
        assert slot is not None
        claim_route = f"prepare:{slot.fence}"

        async def assert_claim_preparation_closed() -> None:
            with pytest.raises(CredentialRouteChanged, match="preparation admission closed"):
                await bridge._reserve_cloudflare_usage(
                    claim_route, _cloudflare_tenant_key(tenant, worker, slot.ordinal),
                    speculative.prepare_by,
                    execution_id=str(first.subject_id), engine_slot=slot.ordinal, org_id=str(tenant),
                )
            async with isolated_capacity_session_factory() as db:
                assert await db.get(cloudflare_spend.CloudflareDispatchMeter, (worker, claim_route)) is None

        with monkeypatch.context() as older_runner:
            older_runner.setattr(cloudflare_admission, "installed_workers_release_digest", lambda: "older-release")
            await assert_claim_preparation_closed()

        async with isolated_capacity_session_factory() as db:
            await db.execute(update(CloudflareInstallation).where(CloudflareInstallation.id == installation_id).values(
                upgrade_release="fixture-release", admitted_revision=None,
            ))
            await db.commit()
        try:
            await assert_claim_preparation_closed()
        finally:
            async with isolated_capacity_session_factory() as db:
                await db.execute(update(CloudflareInstallation).where(CloudflareInstallation.id == installation_id).values(
                    upgrade_release=None, admitted_revision=1,
                ))
                await db.commit()

        async with isolated_capacity_session_factory() as db:
            assert await count_released_prewarms(db, worker) == 3
            assert await db.get(cloudflare_spend.CloudflareDispatchMeter, (worker, f"prepare:{speculative.fence}")) is None
            # These transactions roll back. Each refusal uses the same
            # prewarm fence before the normal send and adoption below.
            for statement in (
                update(Execution).where(Execution.id == speculative.execution_id).values(status=ExecutionStatus.CANCELLED),
                update(ExecutionWorker).where(ExecutionWorker.id == worker).values(state="draining"),
                update(CloudflareWorkflowSlot).where(
                    CloudflareWorkflowSlot.prewarm_fence == speculative.fence,
                ).values(warm_until=func.now()),
            ):
                await db.execute(statement)
                assert await authorized_queue_preparation(db, worker, speculative.fence) is None
                await db.rollback()
            await db.execute(update(ExecutionWorker).where(ExecutionWorker.id == worker).values(
                remote_owner_lease_expires_at=func.now(),
            ))
            with remote_owner_context(LeaseRef(lease)):
                assert not await fence_remote_owner(db)
            await db.rollback()

        async with isolated_capacity_session_factory() as blocker:
            await blocker.scalar(select(CloudflareInstallation.id).where(
                CloudflareInstallation.id == installation_id,
            ).with_for_update(key_share=True))
            bridge.heartbeat_at = asyncio.get_running_loop().time()
            executor._start(first)
            async with asyncio.timeout(5):
                await all_prepared.wait()
            await blocker.rollback()
        assert {command["payload"]["engine_slot"] for command in outgoing} == {0, 1, 2, 3}
        assert sum("prepare_by" in command["payload"] for command in outgoing) == 3

        # A claim consumes its pending dispatch before assigning the slot.
        # Reordering the queue must preserve the exact live hold and fence.
        async with isolated_capacity_session_factory() as db:
            await db.execute(update(WorkDispatch).where(WorkDispatch.subject_id == runs[2]).values(created_at=now))
            await db.commit()
        claims = [first, await claim(), await claim(), await claim()]
        assert claims[1].subject_id == runs[2]
        for expected_ordinal, item in zip((0, 2, 1, 3), claims, strict=True):
            ticket = item.workflow_slot
            assert ticket is not None and ticket.ordinal == expected_ordinal
            if expected_ordinal:
                assert ticket.prewarming and ticket.fence != first.queue_preparations[expected_ordinal - 1].fence
                assert item.queue_preparations == ()

        # The stub reaches its Loader barrier on all four slots before any
        # preparation reply. The API applies the real identity and ledger fences.
        receipts = {}
        for index, command in enumerate(outgoing):
            payload = command["payload"]
            route_id = payload["route_id"]
            identity = f"tenant-eng-tenant-py-{_cloudflare_tenant_key(tenant, worker, payload['engine_slot'])}-{'a' * 16}-cpu30000-sr40-g0"
            await bridge.receive({
                "event": "cloudflare_generation_request",
                "request_id": str(index) * 43, "route_id": route_id, "identity": identity,
            })
            receipts[route_id] = [{"identity": identity, "kind": "boot", "cold": True, "started_at": datetime.now(UTC)}]
        async with asyncio.timeout(5):
            await all_authorized.wait()
        assert all(result["ok"] for result in generation_results)
        for command in outgoing:
            await bridge.receive({"reply": command["id"], "result": {}})
        await asyncio.gather(*tuple(bridge.results))

        async with isolated_capacity_session_factory() as db:
            for item in claims:
                ticket = item.workflow_slot
                assert ticket is not None
                assert await fence_workflow_slot_send(db, worker, item.subject_id, ticket, tenant_id=tenant, preparation=False)
                await db.execute(update(Execution).where(Execution.id == item.subject_id).values(status=ExecutionStatus.SUCCESS))
                await confirm_workflow_slot_release(db, worker, item.subject_id, ticket.fence, completed=True)
            assert await count_released_prewarms(db, worker) == 0
            slots = (await db.scalars(select(CloudflareWorkflowSlot).where(CloudflareWorkflowSlot.worker_id == worker))).all()
            assert len(slots) == 4 and all(
                row.released_at is not None and row.warm_until is not None
                and int((row.warm_until - row.released_at).total_seconds() + 0.5) == 90
                for row in slots
            )
            usage = await db.get(CloudflareUsageIncarnation, worker)
            assert usage is not None
            usage.deleted_at = datetime.now(UTC)
            await db.flush()
            await cloudflare_spend.seal_launcher_meter(db, worker, CloudflareLauncherUsage.model_validate({
                "sqlite_rows_written": 12, "do_requests": 4, "log_events": 0, "routes": receipts,
            }))
            activity = await db.get(CloudflareUsageActivity, (installation_id, usage_period(now)[0]))
            assert activity is not None
            assert activity.qualified_preparations == activity.qualified_creations == 4
            assert activity.qualified_warm_pings == 0
            assert activity.released_cpu_us == 4 * (TEST_CPU_LIMITS["startup"] - 500) * 1000
            assert 275 <= activity.inventory_tenant_seconds <= 290
            await db.commit()
    finally:
        for task in tuple(bridge.results):
            task.cancel()
        await asyncio.gather(*tuple(bridge.results), return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["replacement", "disconnect", "scale_in"])
async def test_stranded_revision_deletes_owned_scripts_before_releasing_slots(
    db_session: AsyncSession, installation: CloudflareInstallation, operation: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    stranded, exhausted, live, foreign = uuid4(), uuid4(), uuid4(), uuid4()
    now = datetime.now(UTC)
    db_session.add_all([
        ExecutionWorker(id=stranded, form="external", state="lost", runtime_profile="cloudflare-python-v1"),
        ExecutionWorker(id=exhausted, form="external", state="retired", runtime_profile="cloudflare-python-v1"),
        ExecutionWorker(id=live, form="external", state="ready", runtime_profile="cloudflare-python-v1"),
        ExecutionWorker(
            id=foreign, form="external", state="retired", runtime_profile="cloudflare-python-v1",
            created_at=now, capacity_resource_deleted_at=now,
        ),
    ])
    await db_session.flush()
    db_session.add_all(CloudflareWorkerReservation(
        worker_id=worker, installation_id=installation.id, scope="other-scope" if worker == foreign else "workers",
        deployment_revision=1, control_root_version=1,
        cleanup_started_at=now - cloudflare_spend.MANAGED_CLEANUP_WINDOW if worker == exhausted else None,
    ) for worker in (stranded, exhausted, live, foreign))
    ticket = CloudflareWorkflowSlot(
        installation_id=installation.id, tenant="platform", ordinal=0, worker_id=stranded,
        dispatch_id=uuid4(), execution_id=uuid4(), fence=uuid4(), sent=True,
    )
    db_session.add(ticket)
    scripts = {}
    for role, namespaces in (
        ("capacity", {"WorkerLedger": "ledger", "ScopeRegistry": "registry"}),
        ("launcher", {"CloudflareLauncher": "launcher", "TenantHost": "tenant"}),
    ):
        name = f"fixture-{role}"
        resource = CloudflareOwnedResource(
            id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
            scope="workers", kind="worker_script", target_name=name, desired_revision=1,
            artifact_digest="c" * 64, effect_id=uuid4(), state="present",
            provider_id=str(uuid4()), provider_version=str(uuid4()), namespace_ids=namespaces,
        )
        scripts[name] = resource
        db_session.add(resource)
    await db_session.flush()
    meter = await db_session.get(CloudflareUsageIncarnation, stranded)
    assert meter is not None
    meter.meter_final = {"ledger_receipt_pending": True}
    other_meter = await db_session.get(CloudflareUsageIncarnation, foreign)
    assert other_meter is not None
    other_meter.meter_final = {"ledger_receipt_pending": True}
    await db_session.flush()
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None and status.installation.spend is not None
    guidance = next(line for line in status.installation.spend.coverage if "Managed cleanup expired for 1" in line)
    assert "Run Scale In" in guidance and "Use Disconnect" in guidance
    present = set(scripts)
    visible_namespaces = set(scripts)
    deleted = []

    def remote(request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        if parts[-1] == "namespaces":
            return httpx.Response(200, json={
                "success": True,
                "result": [{"id": ns, "script": name}
                           for name in visible_namespaces for ns in scripts[name].namespace_ids.values()],
                "result_info": {"page": 1, "total_pages": 1, "total_count": 2 * len(visible_namespaces)},
            })
        name = parts[parts.index("scripts") + 1]
        resource = scripts[name]
        if name not in present:
            return httpx.Response(404)
        if request.method == "DELETE":
            assert request.url.params["force"] == "true"
            present.remove(name)
            visible_namespaces.discard(name)
            deleted.append(name)
            return httpx.Response(204)
        if parts[-1] == "settings":
            result = {"bindings": [{
                "name": OWNER_BINDING, "type": "plain_text",
                "text": worker_owner_value(lifecycle_job._resource_intent(resource)),
            }]}
        else:
            assert parts[-1] == "deployments"
            result = {"deployments": [{
                "id": resource.provider_id,
                "versions": [{"percentage": 100, "version_id": resource.provider_version}],
            }]}
        return httpx.Response(200, json={"success": True, "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        adapter = WorkersRestCleanup(http)
        if operation == "scale_in":
            visible_namespaces.remove("fixture-launcher")
            with pytest.raises(PlatformJobRequiresAction) as conflict:
                await lifecycle_job._verify_stuck_workflow_slots(db_session, installation, adapter)
            assert conflict.value.result["code"] == "CF_SLOT_STOP_UNVERIFIED"
            assert conflict.value.result["revision"] == 1
            assert conflict.value.result["worker_ids"] == [str(stranded)]
            assert conflict.value.result["scripts"] == {name: script.namespace_ids for name, script in scripts.items()}
            assert await db_session.get(CloudflareWorkflowSlot, (installation.id, "platform", 0)) is not None
            present.remove("fixture-launcher")
            meter.meter_final = None
            await db_session.flush()
            assert await lifecycle_job._verify_stuck_workflow_slots(db_session, installation, adapter)
            assert await db_session.scalar(select(CloudflareWorkflowSlot.fence).where(
                CloudflareWorkflowSlot.worker_id == stranded,
            )) is None
            await db_session.refresh(meter)
            assert meter.deleted_at is not None and meter.meter_uncertain
            assert present == {"fixture-capacity"} and deleted == []
            return
        recover = lifecycle_job._cleanup_stranded_workflow_slots
        assert await recover(db_session, installation, adapter, before_revision=2) == "blocked"
        assert not deleted
        await db_session.execute(update(ExecutionWorker).where(ExecutionWorker.id == live).values(
            state="retired", capacity_resource_deleted_at=now,
        ))
        deleted_meter = await db_session.get(CloudflareUsageIncarnation, live)
        assert deleted_meter is not None and deleted_meter.deleted_at is not None
        deleted_meter.meter_final = {"ledger_receipt_pending": True, "preserved": 42}
        await db_session.flush()
        if operation == "replacement":
            for _ in range(4):
                assert await recover(db_session, installation, adapter, before_revision=2) == "progressed"
                await db_session.flush()
                assert await db_session.scalar(select(CloudflareWorkflowSlot.fence).where(
                    CloudflareWorkflowSlot.worker_id == stranded,
                )) == ticket.fence
            assert await recover(db_session, installation, adapter, before_revision=2) == "progressed"
        else:
            job, _ = await lifecycle_job.enqueue_cloudflare_disable(
                db_session, _platform_operator(), installation_id=installation.id, expected_revision=1, action="disconnect",
            )
            token = uuid4()
            job.status, job.lease_token, job.started_at = "running", token, now
            await db_session.flush()
            connection = await db_session.connection()

            @asynccontextmanager
            async def context():
                async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
                    yield session
                    await session.commit()

            monkeypatch.setattr(lifecycle_job, "get_db_context", context)
            payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
            running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), "operator@example.test", "")
            assert await lifecycle_job._settle_disable(running, payload, adapter) == {"outcome": "disconnected"}
        assert deleted == ["fixture-capacity", "fixture-launcher"]
    assert not present
    assert await db_session.scalar(select(CloudflareWorkflowSlot.fence).where(
        CloudflareWorkflowSlot.worker_id == stranded,
    )) is None
    assert await db_session.scalar(select(ExecutionWorker.capacity_resource_deleted_at).where(
        ExecutionWorker.id == stranded,
    )) is not None
    await db_session.refresh(meter)
    assert meter.meter_final == {"ledger_receipt_pending": False, "ledger_namespace_destroyed": True}
    await db_session.refresh(deleted_meter)
    assert deleted_meter.meter_final == {
        "ledger_receipt_pending": False, "ledger_namespace_destroyed": True, "preserved": 42,
    }
    await db_session.refresh(other_meter)
    assert other_meter.meter_final == {"ledger_receipt_pending": True}
    unobserved = await db_session.get(CloudflareUsageIncarnation, exhausted)
    assert unobserved is not None and unobserved.deleted_at is not None
    assert unobserved.meter_uncertain and unobserved.meter_final is None


@pytest.mark.asyncio
async def test_disconnect_recovery_allows_other_revision_owner_updates_during_provider_io(async_session_factory) -> None:
    old_worker, live_worker = uuid4(), uuid4()
    account = uuid4().hex
    async with async_session_factory() as db:
        installation_id = await create_installation(db, account)
        db.add_all([
            ExecutionWorker(id=old_worker, form="external", state="lost", runtime_profile="cloudflare-python-v1"),
            ExecutionWorker(id=live_worker, form="external", state="ready", runtime_profile="cloudflare-python-v1"),
        ])
        await db.flush()
        db.add_all(CloudflareWorkerReservation(
            worker_id=worker_id, installation_id=installation_id, scope="workers",
            deployment_revision=revision, control_root_version=1,
            cleanup_started_at=(
                datetime.now(UTC) - cloudflare_spend.MANAGED_CLEANUP_WINDOW if worker_id == old_worker else None
            ),
        ) for worker_id, revision in [(old_worker, 1), (live_worker, 2)])
        for role, namespaces in (
            ("capacity", {"WorkerLedger": "ledger", "ScopeRegistry": "registry"}),
            ("launcher", {"CloudflareLauncher": "launcher", "TenantHost": "tenant"}),
        ):
            db.add(CloudflareOwnedResource(
                id=uuid4(), installation_id=installation_id, account_id=account, scope="workers",
                kind="worker_script", target_name=f"fixture-{role}", desired_revision=1,
                artifact_digest="c" * 64, effect_id=uuid4(), state="deleting",
                provider_id=str(uuid4()), provider_version=str(uuid4()), namespace_ids=namespaces,
            ))
        await db.commit()
    verified = AsyncMock()

    async def observe(**kwargs):
        assert kwargs["intent"].target.name == "fixture-capacity"
        # Simulate an owner renewal while the REST response is still pending.
        async with async_session_factory() as owner_db:
            locked = await owner_db.scalar(select(ExecutionWorker.id).where(
                ExecutionWorker.id == live_worker,
            ).with_for_update(nowait=True))
            assert locked == live_worker

    try:
        async with async_session_factory() as db:
            row = await db.get(CloudflareInstallation, installation_id, with_for_update=True)
            assert row is not None
            adapter = SimpleNamespace(observe=observe, verify_absent=verified)
            assert await lifecycle_job._cleanup_stranded_workflow_slots(db, row, adapter) == "progressed"
            verified.assert_awaited_once()
    finally:
        async with async_session_factory() as db:
            await db.execute(delete(ExecutionWorker).where(ExecutionWorker.id.in_([old_worker, live_worker])))
            for model in (CloudflareOwnedResource, CloudflareUsageIncarnation, CloudflareCredential):
                await db.execute(delete(model).where(model.installation_id == installation_id))
            await db.execute(delete(CloudflareInstallation).where(CloudflareInstallation.id == installation_id))
            await db.commit()


@pytest.mark.asyncio
async def test_scope_writer_waits_for_installation_admission_lock(async_session_factory) -> None:
    async with async_session_factory() as db:
        installation_id = await create_installation(db, "a" * 32)
        db.add(seal_credential(
            CredentialRef(installation_id, "a" * 32, "scope_bearer", "workers", 1),
            SecretStr("synthetic-scope-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
        ))
        await db.flush()
        db.add(CloudflareCapacityScope(
            installation_id=installation_id, scope="workers", endpoint="https://capacity.example.test",
            deployment_revision=1, bearer_version=1,
        ))
        await db.commit()
    statement = update(CloudflareCapacityScope).where(
        CloudflareCapacityScope.installation_id == installation_id,
    ).values(bearer_version=1)
    try:
        async with async_session_factory() as reader, async_session_factory() as writer:
            await reader.scalar(select(CloudflareInstallation.id).where(
                CloudflareInstallation.id == installation_id,
            ).with_for_update(key_share=True))
            await writer.execute(text("SET LOCAL lock_timeout = '100ms'"))
            with pytest.raises(DBAPIError, match="lock timeout"):
                await writer.execute(statement)
            await writer.rollback()
            await reader.rollback()
            await writer.execute(statement)
            await writer.commit()
    finally:
        async with async_session_factory() as db:
            await db.execute(delete(CloudflareCapacityScope).where(
                CloudflareCapacityScope.installation_id == installation_id,
            ))
            row = await db.get(CloudflareInstallation, installation_id)
            assert row is not None
            row.desired_state = "disconnected"
            await db.flush()
            await retire_cloudflare_installation(db, installation_id=installation_id, expected_revision=1)
            await db.execute(delete(CloudflareCredential).where(CloudflareCredential.installation_id == installation_id))
            await db.delete(row)
            await db.commit()


@pytest.mark.asyncio
async def test_connection_persists_consent_and_account_token_without_admitting_work(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    assert (installation.consent_actor, installation.consent_version) == (str(OPERATOR_ID), CONSENT_TEXT_VERSION)
    assert installation.desired_state == "scaled_in" and installation.admitted_revision is None
    assert installation.api_token_version == 1
    credential = await db_session.get(CloudflareCredential, (
        installation.id, "account_token", "", installation.api_token_version,
    ))
    assert credential is not None
    ref = CredentialRef(installation.id, installation.account_id, "account_token", "", 1)
    assert open_credential(ref, credential) == SecretStr("synthetic-cloudflare-management-token")

    with pytest.raises(CloudflareInstallationExists, match="already exists"):
        await create_installation(db_session, "b" * 32)


@pytest.mark.asyncio
async def test_disconnect_retires_with_receipts_and_allows_reconnect(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    installation_id = installation.id
    installation.desired_state = "disconnected"
    installation.desired_revision += 1
    await db_session.flush()
    resource = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation_id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="disconnect-receipt",
        desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="creating",
    )
    db_session.add(resource)
    await db_session.flush()
    with pytest.raises(CloudflarePolicyConflict, match="cleanup is pending"):
        await retire_cloudflare_installation(db_session, installation_id=installation_id, expected_revision=2)
    with pytest.raises(CloudflareInstallationExists):
        await create_installation(db_session, "b" * 32)

    resource.state = "absent"
    resource.observed_at = datetime.now(UTC)
    resource.provider_id = str(uuid4())
    resource.provider_version = str(uuid4())
    await db_session.flush()
    await retire_cloudflare_installation(db_session, installation_id=installation_id, expected_revision=2)
    assert installation.retired_at is not None and installation.api_token_version is None
    credential = await db_session.get(CloudflareCredential, (installation_id, "account_token", "", 1))
    assert credential is not None and credential.ciphertext is None
    new_id = await create_installation(db_session, installation.account_id)
    assert new_id != installation_id
    assert await db_session.get(CloudflareOwnedResource, resource.id) is not None
    assert await lifecycle_job.usage_script_identities(db_session, new_id) == {resource.target_name: {}}
    db_session.add(CloudflareOwnedResource(
        id=uuid4(), installation_id=new_id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name=resource.target_name,
        desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="creating",
    ))
    await db_session.flush()

    with pytest.raises(IntegrityError, match="tombstone cannot authorize"):
        async with db_session.begin_nested():
            resource.state = "creating"
            await db_session.flush()
    with pytest.raises(IntegrityError, match="tombstone must be retained"):
        async with db_session.begin_nested():
            await db_session.delete(resource)
            await db_session.flush()


@pytest.mark.asyncio
async def test_connection_requires_current_consent_and_slot_rated_bounds(db_session: AsyncSession) -> None:
    with pytest.raises(ValueError, match="current text"):
        await create_installation(db_session, "a" * 32, consent_version="obsolete")
    with pytest.raises(ValueError, match="slot count"):
        await create_installation(db_session, "a" * 32, rated_slots=1)


@pytest.mark.asyncio
async def test_active_token_reference_requires_matching_version(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            installation.api_token_version = 2
            await db_session.flush()
            await db_session.execute(text("SET CONSTRAINTS fk_cf_active_account_token IMMEDIATE"))


@pytest.mark.asyncio
async def test_scope_bearer_reference_is_versioned_and_scope_bound(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    now = datetime.now(UTC)
    ref = CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1)
    value = SecretStr("workers-scope-test-bearer")
    db_session.add(seal_credential(ref, value, actor="operator", now=now))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    await db_session.flush()
    db_session.expunge_all()

    scope = await db_session.get(CloudflareCapacityScope, (ref.installation_id, ref.scope, 1))
    assert scope is not None and scope.bearer_version == ref.version
    credential = await db_session.get(CloudflareCredential, (ref.installation_id, ref.purpose, ref.scope, ref.version))
    assert credential is not None and open_credential(ref, credential) == value

    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            db_session.add(CloudflareCapacityScope(
                installation_id=ref.installation_id, scope="other-scope",
                endpoint="https://other.example.test", deployment_revision=1, bearer_version=1,
            ))
            await db_session.flush()


@pytest.mark.asyncio
async def test_exact_budget_boundary_closes_reservations(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    installation.policy = {
        **installation.policy, "limits": {**installation.policy["limits"], "monthly_limit_usd": "120.09"},
    }
    installation.desired_state, installation.admitted_revision = "enabled", 1
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr("synthetic-boundary-test-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
    ))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    start, _ = usage_period(datetime.now(UTC))
    db_session.add(CloudflareUsageActivity(
        installation_id=installation.id, period_start=start, sdk_requests=200_000,
    ))
    await db_session.flush()
    assert await db_session.scalar(select(cloudflare_estimated_usage()).where(
        CloudflareInstallation.id == installation.id,
    )) == Decimal("120.09")
    assert await ManagedCloudflareScope(installation.id, "workers", 1).lock_admission(db_session) is None


@pytest.mark.asyncio
async def test_ledger_splits_open_incarnation_at_utc_rollover(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    boundary = datetime(2027, 1, 1, tzinfo=UTC)
    db_session.add(CloudflareUsageIncarnation(
        worker_id=uuid4(), installation_id=installation.id, started_at=boundary - timedelta(seconds=10), slots=3,
    ))
    db_session.add(CloudflareUsageActivity(
        installation_id=installation.id, period_start=datetime(2026, 12, 1, tzinfo=UTC),
        dispatched_runs=5, sdk_requests=9,
    ))
    await db_session.flush()
    dec = await read_workers_usage(db_session, installation.id, boundary - timedelta(seconds=2))
    jan = await read_workers_usage(db_session, installation.id, boundary + timedelta(seconds=4))
    assert dec is not None and (dec.launcher_seconds, dec.slot_seconds, dec.dispatched_runs, dec.sdk_requests) == (8, 24, 5, 9)
    assert jan is not None and (jan.launcher_seconds, jan.slot_seconds, jan.dispatched_runs, jan.sdk_requests) == (4, 12, 0, 0)


@pytest.mark.asyncio
async def test_admission_prices_the_qualified_ledger_bounds(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    start = datetime(2026, 11, 1, tzinfo=UTC)
    as_of = start + timedelta(days=2)
    elapsed = 1000
    current = CloudflareUsageIncarnation(
        worker_id=uuid4(), installation_id=installation.id, started_at=start,
        deleted_at=start + timedelta(seconds=elapsed), slots=2,
        meter_release_digest="a" * 64, meter_rate_version=WORKERS_CALIBRATED_RATE_TABLE_VERSION,
    )
    db_session.add_all([
        current,
        CloudflareUsageIncarnation(
            worker_id=uuid4(), installation_id=installation.id, started_at=start - timedelta(hours=1),
            deleted_at=start - timedelta(seconds=1), slots=4,
        ),
        CloudflareUsageActivity(
            installation_id=installation.id, period_start=start, dispatched_runs=11, sdk_requests=17,
        ),
    ])
    await db_session.flush()
    price = select(cloudflare_estimated_usage(as_of)).where(CloudflareInstallation.id == installation.id)
    # Per-second launcher and slot charges from the qualified bounds and
    # published rates. The out-of-period closed incarnation contributes zero.
    assert await db_session.scalar(price) == (
        Decimal("0.01681260") + elapsed * Decimal("0.000855853")
    )
    current.meter_release_digest = None
    await db_session.flush()
    assert await db_session.scalar(price) == (
        Decimal("0.01681260") + elapsed * Decimal("0.001447353")
    )
    receipt = CloudflareLauncherUsage(sqlite_rows_written=120, do_requests=22, log_events=8, exhausted=True)
    current.meter_release_digest = "b" * 64
    current.meter_rate_version = WORKERS_INVENTORY_RATE_TABLE_VERSION
    current.meter_bounds = {
        "caps": TEST_LAUNCHER_CAPS,
        "second_usd": "0.000681378",
    }
    # The production guard charges a floor per meter and retains amounts
    # above the cap even for a regressing or uncertain final receipt.
    # The launcher, two control objects and two slots each reserve 0.128 GB.
    lifetime_base = Decimal("0.01681260") + elapsed * Decimal("0.00068457800") + Decimal("0.00004280")
    month_seconds = Decimal((usage_period(start)[1] - start).total_seconds())
    lifetime_base += Decimal("0.022") * elapsed / month_seconds
    residue_seconds = Decimal((as_of - start).total_seconds()) - elapsed
    lifetime_base += Decimal("0.0002456192") * residue_seconds / month_seconds
    fallback = lifetime_base + Decimal("0.000106")
    await db_session.flush()
    observed = await db_session.scalar(price)
    assert observed is not None and abs(observed - (fallback)) < Decimal("1e-12")
    current.meter_final = receipt.model_dump()
    await db_session.flush()
    observed = await db_session.scalar(price)
    assert observed is not None and abs(observed - (lifetime_base + Decimal("0.00012810"))) < Decimal("1e-12")
    current.meter_terminal = {"sqlite_rows_written": 150, "do_requests": 18, "log_events": 1}
    current.meter_uncertain = True
    await db_session.flush()
    observed = await db_session.scalar(price)
    assert observed is not None and abs(observed - (lifetime_base + Decimal("0.00015810"))) < Decimal("1e-12")
    saved_bounds = current.meter_bounds
    current.meter_bounds = {"caps": {"do_requests": 1}, "second_usd": "0.00068126550"}
    await db_session.flush()
    assert await db_session.scalar(price) is None
    current.meter_bounds = saved_bounds
    installation.policy = {**installation.policy, "rate_table_version": "previous-rate-table"}
    await db_session.flush()
    assert await db_session.scalar(price) is None
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None
    assert status.installation.spend.state == "ledger_unavailable"
    assert status.installation.spend.rate_table_version == "unavailable"


@pytest.mark.asyncio
async def test_idle_capacity_profile_reserves_request_and_cpu_and_reports_mixed_rate_coverage(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    period, _ = usage_period(now)
    price = select(cloudflare_estimated_usage()).where(CloudflareInstallation.id == installation.id)
    assert await db_session.scalar(price) == 0
    async def charge() -> None:
        await cloudflare_spend.record_capacity_request(db_session, installation.id, 400)

    reserve = AsyncMock(side_effect=charge)

    def provider(request: httpx.Request) -> httpx.Response:
        reserve.assert_awaited_once()
        assert request.url.path == "/v1/capabilities"
        return httpx.Response(200, json={
            "protocol_version": 2, "scope": "workers", "worker_version": "fixture-release",
            "runtime_profile": "cloudflare-python-v1", "memory_limit_bytes": 128 * 1024**2,
            "max_workers": 1, "max_lifetime_seconds": 120,
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        await external_controller.ExternalCapacityClient(
            "https://capacity.example.test", "x" * 32, http=http, reserve_request=reserve,
        ).profile()
    assert await db_session.scalar(price) == Decimal("0.00000830")
    activity = await db_session.get(CloudflareUsageActivity, (installation.id, period))
    assert activity is not None
    activity.tenant_dispatches = 1
    await db_session.flush()
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None and status.installation.spend is not None
    rows = status.installation.spend.coverage
    assert any("control-cadence" in line for line in rows)
    assert any("Managed capacity calls" in line for line in rows)
    assert not any("Dynamic Worker creation" in line for line in rows)


@pytest.mark.asyncio
async def test_supervisor_waits_for_its_cadence_after_a_timeout(monkeypatch) -> None:
    stop = asyncio.Event()

    async def wait(*_args, **_kwargs):
        stop.set()

    cadence = AsyncMock(side_effect=wait)
    monkeypatch.setattr(cloudflare_supervisor, "listen_for_pool_wake", AsyncMock())
    monkeypatch.setattr(cloudflare_supervisor, "_installations", AsyncMock(side_effect=TimeoutError))
    monkeypatch.setattr(cloudflare_supervisor, "wait_for_next_pass", cadence)
    await cloudflare_supervisor.cloudflare_supervisor_loop(
        stop, SimpleNamespace(redis_url="redis://fixture"), lease=SimpleNamespace(
            holds_lease=lambda: True, seconds_remaining=lambda: 1,
        ),
    )
    cadence.assert_awaited_once()
    assert cadence.await_args.kwargs["spacing"] == 2.0


@pytest.mark.asyncio
async def test_supervisor_commits_reconcile_even_if_spend_pruning_fails(monkeypatch) -> None:
    stop = asyncio.Event()
    commits = []
    schedule = AsyncMock()
    prune = AsyncMock(side_effect=RuntimeError("injected pruning failure"))

    @asynccontextmanager
    async def transaction():
        session = object()
        yield session
        commits.append(session)

    async def wait(*_args, **_kwargs):
        stop.set()

    monkeypatch.setattr(cloudflare_supervisor, "listen_for_pool_wake", AsyncMock())
    monkeypatch.setattr(cloudflare_supervisor, "_installations", AsyncMock(return_value=[]))
    monkeypatch.setattr(cloudflare_supervisor, "wait_for_next_pass", wait)
    monkeypatch.setattr(cloudflare_supervisor, "get_db_context", transaction)
    monkeypatch.setattr(cloudflare_supervisor, "schedule_cloudflare_reconcile", schedule)
    monkeypatch.setattr(cloudflare_supervisor, "prune_spend_receipt_keys", prune)
    await cloudflare_supervisor.cloudflare_supervisor_loop(
        stop, SimpleNamespace(redis_url="redis://fixture"), lease=SimpleNamespace(
            holds_lease=lambda: True, seconds_remaining=lambda: 1,
        ),
    )
    assert schedule.await_args is not None and prune.await_args is not None
    assert commits == [schedule.await_args.args[0]]
    assert prune.await_args.args[0] is not commits[0]


@pytest.mark.asyncio
async def test_tenant_residency_reserves_route_and_warm_tail_in_the_spend_query(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    noon = now.astimezone(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    monkeypatch.setattr(cloudflare_spend, "_ledger_now", AsyncMock(return_value=noon))
    worker_id = uuid4()
    incarnation = CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, started_at=now - timedelta(minutes=1),
        slots=1, meter_release_digest="f" * 64, meter_rate_version=WORKERS_INVENTORY_RATE_TABLE_VERSION,
        meter_bounds={"caps": TEST_LAUNCHER_CAPS, "second_usd": "0.00068126550"},
    )
    db_session.add(incarnation)
    await db_session.flush()
    price = select(cloudflare_estimated_usage(now)).where(CloudflareInstallation.id == installation.id)
    assert not await reserve_tenant_host_usage(db_session, uuid4(), now)
    assert not await reserve_tenant_host_usage(db_session, worker_id, None)
    assert await reserve_tenant_host_usage(db_session, worker_id, noon + timedelta(seconds=120))
    row = await db_session.get(CloudflareUsageActivity, (installation.id, usage_period(now)[0]))
    assert row is not None
    assert row.inventory_tenant_seconds == 1402
    assert row.inventory_dispatches == 1 and row.inventory_warm_pings == 32
    incarnation.deleted_at = now
    await db_session.flush()
    with_host = await db_session.scalar(price)
    expected = (Decimal("0.040541628") + row.inventory_tenant_seconds * Decimal("0.0000016")
                + 32 * Decimal("0.002627408"))
    row.inventory_tenant_seconds = row.inventory_dispatches = row.inventory_warm_pings = 0
    await db_session.flush()
    without_host = await db_session.scalar(price)
    assert with_host is not None and without_host is not None
    assert abs(with_host - without_host - expected) < Decimal("0.000000000001")
    # A route booked across rollover leaves a residency reservation in
    # the next UTC month even though its request starts in this month.
    next_period = usage_period(now)[1]
    assert not await reserve_tenant_host_usage(db_session, worker_id, next_period)
    incarnation.deleted_at = None
    await db_session.flush()
    assert await reserve_tenant_host_usage(db_session, worker_id, next_period + timedelta(seconds=10))
    rollover = await db_session.get(CloudflareUsageActivity, (installation.id, next_period))
    assert rollover is not None
    assert (rollover.inventory_tenant_seconds, rollover.inventory_dispatches, rollover.inventory_warm_pings) == (1292, 0, 29)
    installation.policy = {**installation.policy, "meter_launcher": {}}
    await db_session.flush()
    assert await db_session.scalar(price) is None


@pytest.mark.asyncio
async def test_qualified_dispatch_prices_pinned_cpu_and_keeps_sdk_events_attributed(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_id, execution_id = uuid4(), uuid4()
    db_session.add(ExecutionWorker(
        id=worker_id, form="external", state="ready", capacity_scope="workers",
        runtime_profile="cloudflare-python-v1", memory_limit_bytes=128 * 2**20, version="fixture-release",
    ))
    await db_session.flush()
    db_session.add(CloudflareWorkerReservation(
        worker_id=worker_id, installation_id=installation.id, scope="workers", deployment_revision=1,
        control_root_version=1,
    ))
    await db_session.flush()
    db_session.add(ExecutionWorkerClaim(worker_id=worker_id, family=WAKE_WORKFLOW, subject_id=execution_id))
    await db_session.flush()
    await cloudflare_spend.record_sdk_gateway_request(
        db_session, origin=installation.worker_ingress_origin, execution_id=execution_id,
    )

    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    noon = now.astimezone(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    monkeypatch.setattr(cloudflare_spend, "_ledger_now", AsyncMock(return_value=noon))
    assert not await reserve_tenant_host_usage(db_session, worker_id, now)
    assert await reserve_tenant_host_usage(
        db_session, worker_id, noon + timedelta(seconds=120), workflow_cpu_ms=123, subrequests=12,
        tenant_id="f" * 32, route_id="fixture-1",
    )
    identity = f"tenant-eng-tenant-py-{'f' * 32}-fixture"
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, f"{identity}-g0")
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, f"{identity}-g0")
    row = await db_session.get(CloudflareUsageActivity, (installation.id, usage_period(now)[0]))
    assert row is not None
    assert (row.dispatched_runs, row.sdk_requests, row.qualified_legacy_events) == (1, 1, 2)
    assert (row.qualified_dispatches, row.qualified_warm_pings, row.qualified_cpu_ms) == (1, 32, 1_367_523)
    assert row.qualified_creations == 1
    assert row.generation_control_requests == 4
    incarnation = await db_session.get(CloudflareUsageIncarnation, worker_id)
    assert incarnation is not None
    incarnation.deleted_at = now
    await db_session.flush()
    price = select(cloudflare_estimated_usage(now)).where(CloudflareInstallation.id == installation.id)
    qualified_price = await db_session.scalar(price)
    row.qualified_legacy_events = 0
    await db_session.flush()
    duplicate_proxy = await db_session.scalar(price)
    assert duplicate_proxy is not None and qualified_price is not None
    assert duplicate_proxy - qualified_price == Decimal("0.0012")

    row.qualified_cpu_ms = 0
    await db_session.flush()
    without_reserved_cpu = await db_session.scalar(price)
    assert without_reserved_cpu is not None
    assert duplicate_proxy - without_reserved_cpu == Decimal("0.02735046")

    incarnation.deleted_at = None
    await db_session.flush()
    assert await reserve_tenant_host_usage(
        db_session, worker_id, noon + timedelta(seconds=120), workflow_cpu_ms=123, subrequests=12,
        tenant_id="f" * 32, route_id="fixture-2",
    )
    await db_session.refresh(row)
    assert row.qualified_creations == 1
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, f"{identity}-g1")
    await db_session.refresh(row)
    assert row.qualified_creations == 2
    # A different launcher package can carry the same loader code and identity.
    incarnation.meter_release_digest = "a" * 64
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, f"{identity}-g1")
    await db_session.refresh(row)
    assert row.qualified_creations == 2

    tomorrow = now + timedelta(days=1)
    await cloudflare_spend._book_daily_engine(db_session, incarnation, f"{identity}-g1", tomorrow)
    await cloudflare_spend._book_daily_engine(db_session, incarnation, f"{identity}-g1", tomorrow)
    next_day = await db_session.get(CloudflareUsageActivity, (installation.id, usage_period(tomorrow)[0]))
    assert next_day is not None
    await db_session.refresh(next_day)
    assert next_day.qualified_creations == (3 if usage_period(tomorrow)[0] == usage_period(now)[0] else 1)

    next_period = usage_period(now)[1]
    previous_rollover = await db_session.get(CloudflareUsageActivity, (installation.id, next_period))
    previous_pings = previous_rollover.qualified_warm_pings if previous_rollover is not None else 0
    assert await reserve_tenant_host_usage(
        db_session, worker_id, next_period + timedelta(seconds=10), workflow_cpu_ms=123, subrequests=12,
        tenant_id="f" * 32, route_id="fixture-rollover",
    )
    rollover = await db_session.get(CloudflareUsageActivity, (installation.id, next_period))
    assert rollover is not None and rollover.qualified_dispatches == 0
    await db_session.refresh(rollover)
    assert rollover.qualified_warm_pings == previous_pings + 29 and rollover.qualified_creations == 1
    assert rollover.generation_control_requests == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence", [
    "complete", "reloaded", "over_cpu", "missing_timing", "missing", "uncertain", "incomplete", "regressing", "drift",
])
async def test_foreground_pricing_keeps_reserves_until_a_confirmed_seal(
    db_session: AsyncSession, installation: CloudflareInstallation, evidence: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    noon = now.astimezone(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    monkeypatch.setattr(cloudflare_spend, "_ledger_now", AsyncMock(return_value=noon))
    period, _ = usage_period(now)
    worker_id = uuid4()
    tenant_id = "e" * 32
    identity = f"tenant-eng-tenant-py-{tenant_id}-fixture"
    row = CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, started_at=now - timedelta(seconds=75), slots=1,
        meter_rate_version=WORKERS_RATE_TABLE_VERSION, meter_release_digest="f" * 64,
        meter_bounds={
            **installation.policy["meter_launcher"], "second_usd": "0.0013", "root_second_usd": "0.0012",
            "cpu_expected_ms": {**installation.policy["meter_launcher"]["cpu_ms"], "workflow": 110},
        },
    )
    db_session.add(row)
    await db_session.flush()
    assert await reserve_tenant_host_usage(
        db_session, worker_id, noon + timedelta(seconds=10), workflow_cpu_ms=20_000, subrequests=10,
        tenant_id=tenant_id, route_id="fixture",
    )
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, identity)
    event = {"sqlite_rows_written": 12, "do_requests": 3, "log_events": 1}
    booking = await db_session.get(cloudflare_spend.CloudflareDispatchMeter, (worker_id, "fixture"))
    assert booking is not None and booking.workflow_cpu_ms == 20_000
    await cloudflare_spend.record_launcher_terminal_meter(
        db_session, worker_id, {
            **event, "sqlite_rows_written": 13 if evidence == "regressing" else 12,
            "do_requests": 0 if evidence == "drift" else event["do_requests"],
        },
        uncertain=evidence == "uncertain",
    )
    price = select(cloudflare_estimated_usage(now)).where(CloudflareInstallation.id == installation.id)
    payload = CloudflareLauncherUsage.model_validate({
        **event, "do_requests": 0 if evidence == "drift" else event["do_requests"],
        "routes": {} if evidence == "incomplete" else {"fixture": [
            {"identity": identity, "kind": "boot", "cold": True, "started_at": now},
            {"identity": identity, "kind": "workflow", "cold": evidence == "reloaded", "started_at": now,
             "wall_ms": {"over_cpu": 30_000, "missing_timing": None}.get(evidence, 12_000)},
        ]},
    })
    await cloudflare_spend.seal_launcher_meter(db_session, worker_id, payload)
    assert row.meter_final is None
    row.deleted_at = now
    await db_session.flush()
    reserved = await db_session.scalar(price)
    assert reserved is not None
    await cloudflare_spend.seal_launcher_meter(db_session, worker_id, None if evidence == "missing" else payload)
    await cloudflare_spend.seal_launcher_meter(db_session, worker_id, payload if evidence != "missing" else None)
    await db_session.flush()
    activity = await db_session.get(CloudflareUsageActivity, (installation.id, period))
    assert activity is not None
    settled = await db_session.scalar(price)
    assert settled is not None
    lifetime = Decimal(str((now - max(row.started_at, period)).total_seconds()))
    counter_and_root_release = Decimal("0.00009295") + lifetime * Decimal("0.0012")
    if evidence in ("complete", "reloaded", "over_cpu", "missing_timing"):
        extra_load_us = 500_000 if evidence == "reloaded" else 0
        workflow_release_us = 8_000_000 if evidence in ("complete", "reloaded") else 0
        assert activity.released_cpu_us == 7_500_000 - extra_load_us + workflow_release_us
        assert activity.released_engine_requests == 2
        assert activity.qualified_creations == 1
        assert reserved - settled == (
            counter_and_root_release + Decimal("0.00015060") + (workflow_release_us - extra_load_us) * Decimal("2e-11")
        )
    elif evidence == "incomplete":
        assert activity.released_cpu_us == activity.released_engine_requests == 0
        assert reserved - settled == counter_and_root_release
    else:
        assert activity.released_cpu_us == activity.released_engine_requests == 0
        assert reserved == settled
    if evidence == "drift":
        coverage = await cloudflare_spend.spend_coverage(db_session, installation.account_id, period)
        assert any("counts diverged" in line for line in coverage)
    if evidence != "missing":
        assert await db_session.get(cloudflare_spend.CloudflareDispatchMeter, (worker_id, "fixture")) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("booted", [True, False])
async def test_abandoned_prepare_settles_observed_load_without_idle_pings(
    db_session: AsyncSession, installation: CloudflareInstallation, booted: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 11, 15, 12, tzinfo=UTC)
    monkeypatch.setattr(cloudflare_spend, "_ledger_now", AsyncMock(return_value=now))
    period, _ = usage_period(now)
    worker_id, tenant_id, route_id = uuid4(), "e" * 32, f"prepare:{uuid4()}"
    row = CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, started_at=now, slots=1,
        meter_rate_version=WORKERS_RATE_TABLE_VERSION, meter_release_digest="f" * 64,
        meter_bounds={**installation.policy["meter_launcher"], "linger_seconds": 90,
                      "cpu_expected_ms": {**TEST_CPU_LIMITS, "boot": 10, "warm": 5}},
    )
    db_session.add(row)
    await db_session.flush()
    for _ in range(2):  # A repeated reservation never charges a second boot.
        assert await reserve_tenant_host_usage(
            db_session, worker_id, now + timedelta(seconds=20), tenant_id=tenant_id, route_id=route_id,
        )
    activity = await db_session.get(CloudflareUsageActivity, (installation.id, period))
    assert activity is not None
    assert activity.qualified_dispatches == 0
    assert activity.qualified_preparations == 1
    assert activity.qualified_warm_pings == 0
    assert activity.inventory_tenant_seconds == 20
    assert activity.qualified_cpu_ms == TEST_CPU_LIMITS["startup"] + 10
    assert activity.released_engine_requests == 0
    identity = f"tenant-eng-tenant-py-{tenant_id}-fixture"
    if booted:
        assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, identity)
    row.deleted_at = now + timedelta(seconds=20)
    await db_session.flush()
    await cloudflare_spend.seal_launcher_meter(db_session, worker_id, CloudflareLauncherUsage.model_validate({
        "sqlite_rows_written": 12, "do_requests": 3, "log_events": 0,
        "routes": {route_id: [{"identity": identity, "kind": "boot", "cold": True, "started_at": now}] if booted else []},
    }))
    await db_session.refresh(activity)
    assert activity.released_cpu_us == (TEST_CPU_LIMITS["startup"] + 10) * 1000 - (510_000 if booted else 0)
    assert activity.released_engine_requests == int(not booted)
    assert activity.qualified_creations == int(booted)


@pytest.mark.asyncio
async def test_unsent_prepare_records_zero_and_refunds_both_months(db_session, installation, monkeypatch) -> None:
    now = datetime(2026, 11, 30, 23, 59, 55, tzinfo=UTC)
    monkeypatch.setattr(cloudflare_spend, "_ledger_now", AsyncMock(return_value=now))
    worker_id = uuid4()
    route_id = f"prepare:{uuid4()}"
    db_session.add(CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, started_at=now, slots=1,
        meter_rate_version=WORKERS_RATE_TABLE_VERSION, meter_release_digest="f" * 64,
        meter_bounds={**installation.policy["meter_launcher"], "linger_seconds": 90,
                      "cpu_expected_ms": {**TEST_CPU_LIMITS, "boot": 10, "warm": 5}},
    ))
    await db_session.flush()
    bookings = []
    assert await reserve_tenant_host_usage(
        db_session, worker_id, now + timedelta(seconds=20), tenant_id="a" * 32, route_id=route_id,
        queue_warm_until=now + timedelta(seconds=90), preparation_bookings=bookings,
    )
    booking, = bookings
    assert [seconds for _, seconds in booking.periods] == [5, 85]
    for _ in range(2):
        await cloudflare_spend.settle_unsent_preparation(db_session, worker_id, route_id, booking)
    for period, _ in booking.periods:
        activity = await db_session.get(CloudflareUsageActivity, (installation.id, period))
        assert activity is not None
        assert activity.inventory_tenant_seconds == activity.qualified_preparations == 0
        assert activity.qualified_cpu_ms == activity.generation_control_requests == 0
    assert await db_session.get(
        cloudflare_spend.CloudflareEngineUsageKey, (installation.id, f"never-sent:{worker_id}:{route_id}"),
    ) is not None


@pytest.mark.asyncio
async def test_foreground_receipt_settles_engine_identity_across_utc_midnight(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch: pytest.MonkeyPatch,
) -> None:
    midnight = datetime(2026, 12, 1, tzinfo=UTC)
    reserved_at = midnight - timedelta(seconds=1)
    invoked_at = midnight + timedelta(seconds=1)
    clock = AsyncMock(return_value=reserved_at)
    monkeypatch.setattr(cloudflare_spend, "_ledger_now", clock)
    worker_id, tenant_id = uuid4(), "a" * 32
    identity = f"tenant-eng-tenant-py-{tenant_id}-fixture"
    row = CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, slots=1, started_at=reserved_at,
        meter_rate_version=WORKERS_RATE_TABLE_VERSION, meter_release_digest="f" * 64,
        meter_bounds={**installation.policy["meter_launcher"], "linger_seconds": 90},
    )
    db_session.add(row)
    await db_session.flush()
    assert await reserve_tenant_host_usage(
        db_session, worker_id, invoked_at + timedelta(seconds=20), workflow_cpu_ms=1000, subrequests=0,
        tenant_id=tenant_id, route_id="cross-midnight",
    )
    assert await cloudflare_spend.reserve_engine_generation(db_session, worker_id, identity)
    clock.return_value = row.deleted_at = invoked_at + timedelta(seconds=2)
    receipt = CloudflareLauncherUsage.model_validate({
        "sqlite_rows_written": 3, "do_requests": 1, "log_events": 0,
        "routes": {"cross-midnight": [
            {"identity": identity, "kind": "workflow", "cold": True, "started_at": invoked_at, "wall_ms": 600},
        ]},
    })
    await cloudflare_spend.seal_launcher_meter(db_session, worker_id, receipt)
    for day in (reserved_at.date(), invoked_at.date()):
        assert await db_session.get(cloudflare_spend.CloudflareEngineUsageKey,
                                    (installation.id, f"daily:{identity}:{day}")) is not None
        activity = await db_session.get(CloudflareUsageActivity, (installation.id, usage_period(
            reserved_at if day == reserved_at.date() else invoked_at,
        )[0]))
        assert activity is not None and activity.qualified_creations == 1


@pytest.mark.asyncio
async def test_full_route_receipt_does_not_flag_busy_launcher(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    now = datetime(2026, 11, 15, tzinfo=UTC)
    worker_id = uuid4()
    db_session.add(CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, started_at=now - timedelta(seconds=1),
        deleted_at=now, slots=1, meter_rate_version=WORKERS_RATE_TABLE_VERSION,
    ))
    await db_session.flush()
    meter = cloudflare_spend.CloudflareDispatchMeter
    await db_session.execute(insert(meter), [
        {"worker_id": worker_id, "route_id": f"{prefix}{i}", "period_start": usage_period(now)[0], "tenant_id": "fixture"}
        for prefix in ("route-", "prepare:") for i in range(513)
    ])
    receipt = CloudflareLauncherUsage.model_validate({
        "sqlite_rows_written": 128, "do_requests": 128, "log_events": 0,
        "routes": {
            **{f"route-{i}": [{"identity": "fixture", "kind": "boot", "cold": True, "started_at": now}]
               for i in range(128)},
            **{f"prepare:{i}": [] for i in range(128)},
        },
    })
    await cloudflare_spend.seal_launcher_meter(db_session, worker_id, receipt)
    row = await db_session.get(CloudflareUsageIncarnation, worker_id)
    assert row is not None and row.meter_final is not None
    assert row.meter_final["reconciliation_drift"] is False


@pytest.mark.asyncio
async def test_launcher_storage_separates_live_bytes_from_retained_residue(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 11, 15, tzinfo=UTC)
    start, end = usage_period(now)
    retired = start + timedelta(days=1)
    worker_id = uuid4()
    old_id = uuid4()
    digest = "f" * 64
    row = CloudflareUsageIncarnation(
        worker_id=worker_id, installation_id=installation.id, started_at=start, slots=1,
        meter_rate_version=WORKERS_RATE_TABLE_VERSION, meter_release_digest=digest, deployment_revision=1,
    )
    old = CloudflareUsageIncarnation(
        worker_id=old_id, installation_id=installation.id,
        started_at=datetime(2026, 8, 1, tzinfo=UTC), deleted_at=datetime(2026, 9, 1, tzinfo=UTC), slots=1,
        meter_rate_version=WORKERS_INVENTORY_RATE_TABLE_VERSION, meter_release_digest=digest, deployment_revision=1,
        meter_final={"sqlite_database_bytes": 1_001_000},
    )
    namespace = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="fixture-launcher", desired_revision=1,
        artifact_digest=digest, effect_id=uuid4(), state="present",
        provider_id="fixture", provider_version="1", observed_at=start,
    )
    db_session.add_all([row, old, namespace])
    await db_session.flush()
    estimate = select(cloudflare_spend._retained_storage_cost(CloudflareInstallation, literal(now))).where(
        CloudflareInstallation.id == installation.id,
    )
    event = {"sqlite_rows_written": 1, "do_requests": 1, "log_events": 1, "exhausted": False}
    await cloudflare_spend.record_launcher_terminal_meter(
        db_session, worker_id, {**event, "sqlite_database_bytes": 2_000_000}, uncertain=False,
    )
    await cloudflare_spend.record_launcher_terminal_meter(db_session, worker_id, event, uncertain=False)
    assert row.meter_terminal is not None and row.meter_terminal["sqlite_database_bytes"] == 2_000_000
    row.deleted_at = retired
    await db_session.flush()
    uncleared = await db_session.scalar(estimate)
    await cloudflare_spend.seal_launcher_meter(
        db_session, worker_id, CloudflareLauncherUsage(
            sqlite_rows_written=1, do_requests=1, log_events=1, launcher_storage_cleared=True,
            ledger_receipt_pending=True,
        ),
    )
    await db_session.flush()
    cleared = await db_session.scalar(estimate)
    rate = Decimal("0.20") / 1_000_000_000
    month_days = (end - start).days
    assert cleared is not None and uncleared is not None
    assert abs(uncleared - cleared - 2_000_000 * rate * (now - retired).days / month_days) < Decimal("1e-12")

    # A failed acknowledgement keeps the larger receipt estimate and remains
    # eligible for another controller pass. Success only prices the fence.
    @asynccontextmanager
    async def context():
        yield db_session

    monkeypatch.setattr(cloudflare_spend, "get_db_context", context)
    acknowledged_at = retired + timedelta(days=2)
    monkeypatch.setattr(cloudflare_spend, "_ledger_now", AsyncMock(return_value=acknowledged_at))
    ack = AsyncMock(side_effect=[httpx.ReadTimeout("fixture"), None])
    with pytest.raises(httpx.ReadTimeout):
        await cloudflare_spend.compact_ledger_receipt(worker_id, ack)
    assert await db_session.scalar(cloudflare_spend.pending_ledger_receipt(worker_id)) == worker_id
    await cloudflare_spend.compact_ledger_receipt(worker_id, ack)
    await cloudflare_spend.compact_ledger_receipt(worker_id, ack)
    assert ack.await_count == 2
    compacted = await db_session.scalar(estimate)
    assert compacted is not None
    assert abs(cleared - compacted - 208_000 * rate * (now - acknowledged_at).days / month_days) < Decimal("1e-12")

    monkeypatch.setattr(cloudflare_spend, "_ledger_now", AsyncMock(return_value=now))
    await cloudflare_spend.prune_spend_receipt_keys(db_session)
    await cloudflare_spend.prune_spend_receipt_keys(db_session)
    assert await db_session.get(CloudflareUsageIncarnation, old_id, populate_existing=True) is None
    rolled_up = await db_session.scalar(estimate)
    assert rolled_up is not None and abs(rolled_up - compacted) < Decimal("1e-12")
    assert any(WORKERS_INVENTORY_RATE_TABLE_VERSION in line for line in await cloudflare_spend.spend_coverage(
        db_session, installation.account_id, start,
    ))

    namespace.state = "absent"
    namespace.observed_at = start + timedelta(days=2)
    db_session.add(CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="workers", kind="worker_script", target_name="replacement-launcher", desired_revision=2,
        artifact_digest=digest, effect_id=uuid4(), state="present",
        provider_id="replacement", provider_version="1", observed_at=start + timedelta(days=3),
    ))
    await db_session.flush()
    after_deletion = await db_session.scalar(estimate)
    assert after_deletion is not None
    # Both launchers can leave TenantHost markers in that namespace.
    removed_bytes = 1_001_000 + 2 * 4096
    assert abs(compacted - after_deletion - removed_bytes * rate * (now - namespace.observed_at).days / month_days) < Decimal("1e-12")


@pytest.mark.asyncio
async def test_canary_acknowledges_only_after_the_receipt_transaction_commits(monkeypatch) -> None:
    worker_id = uuid4()
    db = SimpleNamespace(scalar=AsyncMock(return_value=worker_id))
    receipt = CloudflareLauncherUsage(sqlite_rows_written=3, do_requests=1, log_events=0, ledger_receipt_pending=True)
    committed = False
    seal = AsyncMock()

    @asynccontextmanager
    async def context():
        nonlocal committed
        yield db
        committed = seal.await_count > 0

    async def compact(actual_worker, acknowledge):
        assert actual_worker == worker_id and committed
        seal.assert_awaited_once_with(db, worker_id, receipt)
        await acknowledge()

    monkeypatch.setattr(cloudflare_canary, "get_db_context", context)
    monkeypatch.setattr(cloudflare_canary, "seal_launcher_meter", seal)
    monkeypatch.setattr(cloudflare_canary, "compact_ledger_receipt", compact)
    client = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(resource_uid="owned", state="deleted", usage=receipt)),
        acknowledge_receipt=AsyncMock(),
    )
    assert await cloudflare_canary._retire_canary(client, worker_id, "owned")
    client.acknowledge_receipt.assert_awaited_once_with(worker_id, "workers", "owned")


@pytest.mark.asyncio
@pytest.mark.parametrize("delete_uid,stored,expected", [("wrong", False, False), ("owned", False, False), ("owned", True, True)])
async def test_canary_cannot_seal_without_a_matching_delete_receipt_and_row(
    monkeypatch, delete_uid: str, stored: bool, expected: bool,
) -> None:
    worker_id = uuid4()
    db = SimpleNamespace(scalar=AsyncMock(side_effect=[worker_id, None, worker_id if stored else None]))

    @asynccontextmanager
    async def context():
        yield db

    seal = AsyncMock()
    monkeypatch.setattr(cloudflare_canary, "get_db_context", context)
    monkeypatch.setattr(cloudflare_canary, "seal_launcher_meter", seal)
    client = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(resource_uid="owned", state="running")),
        remove=AsyncMock(return_value=SimpleNamespace(resource_uid=delete_uid, state="deleted", usage={})),
    )
    assert await cloudflare_canary._retire_canary(client, worker_id, "owned") is expected
    seal.assert_not_awaited()


@pytest.mark.asyncio
async def test_claim_gate_tracks_scale_in_and_deployment_changes_without_restart(
    db_session: AsyncSession, installation: CloudflareInstallation,
) -> None:
    worker_id = uuid4()
    db_session.add(ExecutionWorker(
        id=worker_id, form="external", state="ready", capacity_scope="workers",
        runtime_profile="cloudflare-python-v1", memory_limit_bytes=256 * 2**20,
    ))
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr("synthetic-backend-test-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
    ))
    await db_session.flush()
    db_session.add(CloudflareWorkerReservation(
        worker_id=worker_id, installation_id=installation.id, scope="workers", deployment_revision=1,
        control_root_version=1,
    ))
    scope = CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    )
    db_session.add(scope)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)

    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    await db_session.flush()
    assert await cloudflare_claim_allowed(db_session, worker_id)
    db_session.add(ExecutionWorkerClaim(worker_id=worker_id, family=WAKE_WORKFLOW, subject_id=uuid4()))
    await db_session.flush()
    start, _ = usage_period(datetime.now(UTC))
    activity = await db_session.get(CloudflareUsageActivity, (installation.id, start))
    assert activity is not None and activity.dispatched_runs == 1
    activity.sdk_requests = 100_000
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None and status.installation.spend is not None
    assert status.installation.spend.state == "budget_reached"
    activity.sdk_requests = 0
    installation.usage_incomplete_until = datetime.now(UTC) + timedelta(days=1)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    installation.usage_incomplete_until = None
    await db_session.flush()
    assert await cloudflare_claim_allowed(db_session, worker_id)
    ledger = await read_workers_usage(db_session, installation.id, datetime.now(UTC))
    assert ledger is not None and ledger.launcher_seconds > 0
    assert ledger.slot_seconds == ledger.launcher_seconds * 2

    policy = installation.policy
    limits = policy["limits"]
    installation.policy = {
        **policy,
        "limits": {**limits, "workers": {**limits["workers"], "work_kinds": []}},
    }
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    installation.policy = policy

    installation.admitted_revision = None
    installation.desired_state = "scaled_in"
    installation.desired_revision = 2
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)

    installation.admitted_revision = 2
    installation.desired_state = "enabled"
    replacement_scope = CloudflareCapacityScope(
        installation_id=scope.installation_id, scope=scope.scope, endpoint="https://new-capacity.example.test",
        deployment_revision=2, bearer_version=scope.bearer_version,
    )
    db_session.add(replacement_scope)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    await db_session.delete(scope)
    await db_session.delete(replacement_scope)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    assert await cloudflare_claim_allowed(db_session, uuid4())
    orphan = ExecutionWorker(
        id=uuid4(), form="external", state="retired", capacity_scope="old-workers",
        runtime_profile="cloudflare-python-v1",
    )
    db_session.add(orphan)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, orphan.id)

    installation.admitted_revision = None
    installation.desired_state = "disconnected"
    installation.desired_revision = 3
    await db_session.flush()
    with pytest.raises(CloudflarePolicyConflict, match="cleanup is pending"):
        await retire_cloudflare_installation(db_session, installation_id=installation.id, expected_revision=3)
    worker = await db_session.get(ExecutionWorker, worker_id)
    assert worker is not None
    worker.state = "retired"
    worker.capacity_resource_deleted_at = datetime.now(UTC)
    await db_session.flush()
    with pytest.raises(CloudflarePolicyConflict, match="cleanup is pending"):
        await retire_cloudflare_installation(db_session, installation_id=installation.id, expected_revision=3)
    orphan.capacity_resource_deleted_at = datetime.now(UTC)
    await db_session.flush()
    await retire_cloudflare_installation(db_session, installation_id=installation.id, expected_revision=3)
    assert installation.retired_at is not None


@pytest.mark.asyncio
async def test_resource_journal_cannot_bind_another_account(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            db_session.add(CloudflareOwnedResource(
                id=uuid4(), installation_id=installation.id, account_id="b" * 32,
                scope="workers", kind="worker_script", target_name="bifrost-resource",
                desired_revision=1, artifact_digest="c" * 64, effect_id=uuid4(), state="creating",
            ))
            await db_session.flush()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("collision", "constraint"),
    [
        ("target_name", "uq_cf_resource_target"),
        ("provider_id", "uq_cf_resource_provider"),
        ("effect_id", "uq_cf_resource_effect"),
    ],
)
async def test_resource_journal_rejects_duplicate_identity(
    db_session: AsyncSession, installation: CloudflareInstallation, collision: str, constraint: str,
) -> None:
    identity = {"target_name": "worker-a", "provider_id": "opaque-id-a", "effect_id": uuid4()}
    common = {
        "installation_id": installation.id, "account_id": installation.account_id,
        "scope": "workers", "kind": "worker_script", "desired_revision": 1,
        "artifact_digest": "c" * 64, "state": "present", "provider_version": "etag-a",
        "observed_at": datetime.now(UTC),
    }
    db_session.add(CloudflareOwnedResource(id=uuid4(), **common, **identity))
    await db_session.flush()
    next_identity = {"target_name": "worker-b", "provider_id": "opaque-id-b", "effect_id": uuid4()}
    next_identity[collision] = identity[collision]
    with pytest.raises(IntegrityError, match=constraint):
        async with db_session.begin_nested():
            db_session.add(CloudflareOwnedResource(id=uuid4(), **common, **next_identity))
            await db_session.flush()


@pytest.mark.asyncio
async def test_scaled_in_installation_cannot_admit_its_revision(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    with pytest.raises(IntegrityError):
        async with db_session.begin_nested():
            installation.admitted_revision = installation.desired_revision
            await db_session.flush()


@pytest.mark.asyncio
async def test_policy_save_fences_old_admission_and_rejects_an_unsafe_budget(
    db_session: AsyncSession, installation: CloudflareInstallation
) -> None:
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    await db_session.flush()
    policy = CloudflareCapacityPolicy(
        workers=CloudflareWorkersPolicy(
            launcher_cap=1, slots_per_launcher=2, max_lifetime_seconds=3600, work_kinds=frozenset({"workflow"}),
        ),
        monthly_limit_usd=Decimal("4.00"),
    )
    zero = dict.fromkeys(WORKERS_RATES, Decimal(0))
    fixed = {**zero, "sqlite_rows_written": Decimal(5_000_000)}
    bounds = WorkersReleaseMeterBounds(60, zero, fixed, "fixture-release", 2, TEST_LAUNCHER_CAPS, TEST_CPU_LIMITS, 500_000, TEST_ORDER)
    with pytest.raises(CloudflareBudgetTooLow):
        await save_cloudflare_policy(
            db_session, installation_id=installation.id, expected_revision=1, policy=policy, bounds=bounds,
        )
    assert installation.admitted_revision == 1

    policy.monthly_limit_usd = Decimal("11.00")
    assert await save_cloudflare_policy(
        db_session, installation_id=installation.id, expected_revision=1, policy=policy, bounds=bounds,
    ) == 2
    assert installation.admitted_revision is None
    assert installation.policy["rate_table_version"] == WORKERS_RATE_TABLE_VERSION
    assert installation.policy["fleet_envelope_usd"] == "8.10"
    assert installation.policy["bounds_release"] == "fixture-release"
    assert len(installation.policy["bounds_digest"]) == 64
    assert await save_cloudflare_policy(
        db_session, installation_id=installation.id, expected_revision=2, policy=policy, bounds=bounds,
    ) == 2
    with pytest.raises(CloudflarePolicyConflict):
        await save_cloudflare_policy(
            db_session, installation_id=installation.id, expected_revision=1, policy=policy, bounds=bounds,
        )
