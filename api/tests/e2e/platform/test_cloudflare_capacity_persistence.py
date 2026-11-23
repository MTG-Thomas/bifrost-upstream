"""Exercise the Cloudflare journal against the migrated platform database."""

import asyncio
import base64
import json
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from email import policy as email_policy
from email.parser import BytesParser
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import UUID, uuid4

import httpx
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from fastapi import FastAPI, HTTPException, Response
from pydantic import SecretStr
from scripts.test_cloudflare_fixture import _release, block_cloudflare_network
from shared.models import (
    CloudflareCapacityPolicy,
    CloudflareLauncherUsage,
    CloudflarePolicyUpdate,
    CloudflareReconcileAction,
    CloudflareStageCredentialRequest,
    CloudflareWorkersPolicy,
)
from sqlalchemy import delete, func, insert, literal, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from src.config import Settings
from src.core.app_wiring import register_exception_handlers
from src.core.auth import get_current_active_user
from src.core.db_deps import get_db
from src.core.exceptions import StructuredRequestRefusal
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
from src.models.orm.users import User
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
from src.services.execution import cloudflare_approval
from src.services.execution.workflow_dispatch import release_workflow_run
from src.services.kubernetes_execution import KubernetesExecutionService, KubernetesPolicy
from src.services.memory_requests import JOB_BASELINE_ESTIMATE_BYTES
from src.services.work_dispatch import WAKE_WORKFLOW, stage_dispatch
from src.services.worker_pool import claims as claim_service
from src.services.worker_pool import (
    cloudflare_admission,
    cloudflare_canary,
    cloudflare_connection,
    cloudflare_connector_channel,
    cloudflare_policy,
    cloudflare_spend,
    cloudflare_status,
    cloudflare_supervisor,
    cloudflare_tunnel,
    controller,
    external_controller,
)
from src.services.worker_pool.claims import claim_next
from src.services.worker_pool.cloudflare_admission import (
    ManagedCloudflareScope,
    PrimaryPool,
    cloudflare_claim_allowed,
    cloudflare_dispatch_started_at,
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
    ObservationCheck,
    ObservationPending,
    ObservationSettling,
    OwnershipCheck,
    OwnershipConflict,
    OwnershipResource,
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
    WorkersDeployment,
    WorkersRestCleanup,
    WorkersRestProvisioner,
    worker_owner_value,
)
from src.services.worker_pool.manifest import WorkerJobSpec
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
from tests.helpers.cloudflare_containers import json_bytes
from tests.helpers.isolation import isolated_test_database

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

    def __init__(self, *, initial_configuration="version_zero", lose_create_responses=True):
        self.tunnel = None
        self.dns = None
        self.config = None
        self.initial_configuration = initial_configuration
        self.lose_create_responses = lose_create_responses
        self.config_version = 0
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
            if self.initial_configuration == "default_404":
                self.config = {"ingress": [{"service": "http_status:404"}], "warp-routing": {"enabled": False}}
                self.config_version = 1
            if self.lose_create_responses:
                raise httpx.ReadError("lost tunnel create response", request=request)
            return result(self.tunnel)
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
            if self.config_version == 0 and self.initial_configuration == "http_404":
                return httpx.Response(404)
            return result({
                "source": "cloudflare", "created_at": "fixture-create", "tunnel_id": self.tunnel_id,
                "version": self.config_version, "config": self.config,
            })
        if path == tunnel + "/configurations" and method == "PUT":
            if self.denied_write == "tunnel":
                return httpx.Response(403)
            self.config = json.loads(request.content)["config"]
            self.config_version += 1
            assert set(self.config) == {"ingress", "warp-routing"}
            assert self.config["warp-routing"] == {"enabled": False}
            assert all(set(rule) <= {"hostname", "service"} for rule in self.config["ingress"])
            return result({})
        if path == records and method == "GET":
            assert request.url.params["name.exact"] == "workers.example.test"
            return result([self.dns] if self.dns else [])
        if path == records and method == "POST":
            self.dns_creates += 1
            if self.lose_create_responses and self.dns_creates == 1:
                return httpx.Response(503)
            self.dns = {**json.loads(request.content), "id": self.dns_id, "modified_on": "fixture-dns-version"}
            if self.lose_create_responses:
                raise httpx.ReadError("lost DNS create response", request=request)
            return result(self.dns)
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

    async def connector_ready(db, row, client):
        item = await cloudflare_tunnel.connector_resource(db, row)
        snapshot = cloudflare_tunnel.snapshot_connector(row, item)
        connectors = await cloudflare_tunnel.observe_connector(client, snapshot)
        return cloudflare_tunnel.record_connector_readiness(row, item, snapshot, connectors)

    async def verify(db, row, client):
        snapshot = await cloudflare_tunnel.snapshot_verification(db, row)
        connectors = await cloudflare_tunnel.verify(client, snapshot)
        item = await cloudflare_tunnel.connector_resource(db, row)
        if not cloudflare_tunnel.record_connector_readiness(row, item, snapshot.connector, connectors):
            raise ObservationPending("Cloudflare has no connected tunnel supervisor")

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
        provider = _TunnelAPI(initial_configuration="http_404" if layout == "kustomize" else "version_zero")
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
            client = cloudflare_tunnel.adapter(http, installation, SecretStr("synthetic-cloudflare-management-token"))
            actor = str(OPERATOR_ID)
            async with context() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                await cloudflare_tunnel.prepare(db, row, actor=actor)
            with pytest.raises(CloudflareRestUnavailable):
                await change(cloudflare_tunnel.provision_tunnel, client)
            default = provider.config
            await change(cloudflare_tunnel.provision_tunnel, client)
            assert provider.tunnel_creates == 1
            assert provider.config == {"ingress": [
                {"hostname": "workers.example.test", "service": origin}, {"service": "http_status:404"},
            ], "warp-routing": {"enabled": False}}
            if layout == "compose":
                assert default is None and provider.config_version == 1
                approved, version = provider.config, provider.config_version
                provider.config = default
                provider.config_version = 0
                with pytest.raises(OwnershipConflict) as conflict:
                    await change(cloudflare_tunnel.provision_tunnel, client)
                assert conflict.value.check == OwnershipCheck.INGRESS_OCCUPIED
                assert provider.config == default
                provider.config, provider.config_version = approved, version
            with pytest.raises(CloudflareRestUnavailable):
                await change(cloudflare_tunnel.provision_dns, client, actor=actor)
            with pytest.raises(CloudflareRestUnavailable):
                await change(cloudflare_tunnel.provision_dns, client, actor=actor)
            await change(cloudflare_tunnel.provision_dns, client, actor=actor)
            assert provider.dns_creates == 2
            key = await cloudflare_connector_channel.project_connector(None)
            first = delivered()
            assert first == (await client.connector_token(provider.tunnel_id)).get_secret_value()
            credential_key = (installation_id, "connector_token", "", 1)
            async with context() as db:
                current = await db.get(CloudflareCredential, credential_key)
                assert current is not None and current.ciphertext is not None
                original_ciphertext = current.ciphertext
                row = await db.get(CloudflareInstallation, installation_id)
                await cloudflare_tunnel.prepare(db, row, actor=actor)
            await change(cloudflare_tunnel.provision_tunnel, client)
            await change(cloudflare_tunnel.provision_dns, client, actor=actor)
            async with context() as db:
                current = await db.get(CloudflareCredential, credential_key)
                row = await db.get(CloudflareInstallation, installation_id)
                assert current is not None and current.ciphertext == original_ciphertext
                assert row is not None and row.tunnel_token_version == 1
            assert await cloudflare_connector_channel.project_connector(key) == key
            assert delivered() == first and provider.tunnel_creates == 1 and provider.dns_creates == 2
            old, new = str(uuid4()), str(uuid4())
            provider.connectors = [old]
            assert provider.config
            provider.config = {
                "originRequest": {}, "warp-routing": {"enabled": False},
                "ingress": [{**rule, "originRequest": {}} for rule in provider.config["ingress"]],
            }
            async with context() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                row.deployment_evidence = {**(row.deployment_evidence or {}), "connector_session": "disconnected"}
            await change(verify, client)
            await db_session.refresh(installation)
            evidence = await cloudflare_status.deployment_evidence(db_session, installation)
            assert evidence.connector_session == "connected"
            assert evidence.connector_last_ready_at is not None and evidence.dns_verified_at is not None
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
                elapsed = 0.0

                async def advance(seconds: float) -> None:
                    nonlocal elapsed
                    elapsed += seconds

                patch.setattr(lifecycle_job, "monotonic", lambda: elapsed)
                patch.setattr(lifecycle_job, "asyncio", SimpleNamespace(sleep=advance))
                with pytest.raises(PlatformJobFailure) as unavailable:
                    await lifecycle_job._reconcile(running, reconcile)
                assert unavailable.value.code == "CF_CONNECTOR_UNAVAILABLE"
                assert not unavailable.value.retryable and elapsed == 180
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
                    assert upgrade_conflict.value.result == {
                        "code": "CF_OWNERSHIP_CONFLICT",
                        "details": {
                            "resource_class": "private_route", "check": "private_route",
                            "observed_category": "present", "expected_category": "absent",
                        },
                    }
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
                await change(verify, client)
            provider.connectors = [old]
            await change(verify, client)
            async with context() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                await cloudflare_tunnel.stage_rotation(db, row, version=2, actor=actor)
            await change(cloudflare_tunnel.record_rotation_intent, client, version=2)
            assert await change(connector_ready, client)
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
            assert not await change(connector_ready, client)
            assert provider.evicted == sorted([old, late])
            provider.connectors = [new]
            assert await change(connector_ready, client)
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
    provider = _TunnelAPI(initial_configuration="default_404")
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


def _platform_operator(user_id: UUID = OPERATOR_ID) -> Caller:
    role = RoleGrant(uuid4(), frozenset({"platform.readwrite"}), (Boundary(BoundaryKind.PLATFORM),))
    return Caller(
        UserPrincipal(user_id, "operator@example.test", None),
        AuthorizationContext(user_id, None, uuid4(), False, frozenset(), (role,)),
    )


@pytest.fixture(autouse=True)
def refuse_hosted_cloudflare(monkeypatch):
    block_cloudflare_network(monkeypatch.setattr)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_receipt", [True, False], ids=["recorded-tunnel", "unrecorded-tunnel"])
async def test_managed_tunnel_disconnect_commits_absence_before_retirement(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, lifecycle_audit_actor, with_receipt,
) -> None:
    installation.reachability_mode = "managed_tunnel"
    installation.zone_id = _TunnelAPI.zone
    monkeypatch.setattr(cloudflare_tunnel, "get_settings", lambda: SimpleNamespace(cloudflare_tunnel_channel="compose"))
    provider = _TunnelAPI()
    await cloudflare_tunnel.prepare(db_session, installation, actor=str(OPERATOR_ID))
    await db_session.flush()
    dns = await cloudflare_tunnel.resource(db_session, installation, cloudflare_tunnel.DNS)
    tunnel = await cloudflare_tunnel.resource(db_session, installation, cloudflare_tunnel.TUNNEL)
    assert dns is not None and tunnel is not None
    if with_receipt:
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
            client = cloudflare_tunnel.adapter(http, installation, SecretStr("synthetic-cloudflare-management-token"))
            with pytest.raises(CloudflareRestUnavailable):
                await cloudflare_tunnel.provision_tunnel(db_session, installation, client)
            await cloudflare_tunnel.provision_tunnel(db_session, installation, client)
        dns.state = "absent"
        dns.observed_at = datetime.now(UTC)
    else:
        assert all(record.state == "creating" and record.provider_id is None for record in (dns, tunnel))
        dns.observed_at = tunnel.observed_at = datetime.now(UTC) - timedelta(minutes=2)
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
    if with_receipt:
        assert provider.tunnel and provider.tunnel["deleted_at"]
    else:
        assert provider.tunnel is None and provider.tunnel_creates == 0
    await db_session.refresh(installation)
    assert installation.retired_at is not None
    assert set(await db_session.scalars(select(CloudflareOwnedResource.state).where(
        CloudflareOwnedResource.installation_id == installation.id,
    ))) == {"absent"}
    assert installation.api_token_version is None
    retired_at = installation.retired_at
    monkeypatch.setattr(lifecycle_job, "httpx", SimpleNamespace(
        AsyncClient=lambda **_: httpx.AsyncClient(transport=httpx.MockTransport(_unexpected_retirement_request)),
    ))
    assert await lifecycle_job.CLOUDFLARE_DISABLE_DEFINITION.handler(running, payload) == {"outcome": "disconnected"}
    await db_session.refresh(installation)
    assert installation.retired_at == retired_at


def _unexpected_retirement_request(_request: httpx.Request) -> httpx.Response:
    pytest.fail("Disconnect replay contacted the provider after retirement")


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
        db_session, actor_id=OPERATOR_ID, account_id=account, purpose="rotate", token=token, installation_id=uuid4(),
    )
    await db_session.flush()
    await db_session.execute(update(CloudflareStagedToken).where(
        CloudflareStagedToken.id == expired,
    ).values(expires_at=datetime.now(UTC) - timedelta(seconds=1)))
    with pytest.raises(ValueError, match="Invalid or expired"):
        await consume_staged_token(db_session, expired, actor_id=OPERATOR_ID, account_id=account, purpose="rotate")


def test_legacy_connect_patch_is_retired(e2e_client, platform_admin) -> None:
    rejected = e2e_client.patch("/api/capacity/cloudflare", headers=platform_admin.headers, json={
        "action": "connect", "account_id": "f" * 32,
        "api_token": "synthetic-management-token",
    })
    assert rejected.status_code == 422
    assert "synthetic-management-token" not in rejected.text


@pytest.mark.asyncio
async def test_canary_pins_meter_caps_for_120_seconds_under_preset_b(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = _release(lifetime_meters=True)
    policy = CloudflareCapacityPolicy(
        workers=CloudflareWorkersPolicy(
            launcher_cap=3, slots_per_launcher=4, installation_slots=9, max_lifetime_seconds=3600,
            work_kinds=frozenset({"workflow"}),
        ),
        monthly_limit_usd=Decimal("75"),
        shared_managed_worker_cap=8,
    )
    installation.policy = cloudflare_policy._price_policy(policy, cloudflare_policy.release_meter_bounds(release))
    deployment = WorkersDeployment(
        release, installation.worker_ingress_origin, "launcher", "capacity", policy.workers, "gateway",
    )
    job, _ = await enable_job.enqueue_cloudflare_enable(
        db_session, _platform_operator(), installation_id=installation.id, expected_revision=1,
    )
    lease = uuid4()
    job.status = "running"
    job.lease_token = lease
    job.lease_expires_at = datetime.now(UTC) + timedelta(minutes=2)
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    class PinObserved(Exception):
        pass

    async def observe_prepare(reservation, _scope, **_kwargs):
        pin = await db_session.get(CloudflareWorkerReservation, reservation.worker_id)
        usage = await db_session.get(CloudflareUsageIncarnation, reservation.worker_id)
        assert pin is not None and pin.workers_deadline_wire and pin.workers_lifetime_seconds == 120
        assert reservation.max_lifetime_seconds == 120
        assert usage is not None and usage.meter_bounds is not None
        rates = installation.policy["meter_launcher"]["per_second_caps"]
        assert installation.policy["meter_launcher"]["caps"] == {name: rate * 3600 for name, rate in rates.items()}
        assert usage.meter_bounds["caps"] == {name: rate * 120 for name, rate in rates.items()}
        raise PinObserved

    monkeypatch.setattr(cloudflare_canary, "get_db_context", context)
    monkeypatch.setattr(cloudflare_canary, "load_managed_control_key", AsyncMock(return_value="synthetic"))
    monkeypatch.setattr(cloudflare_canary, "_retire_canary", AsyncMock())
    client = Mock(
        profile=AsyncMock(return_value=SimpleNamespace(
            scope="workers", protocol_version=2, worker_version=release.digest,
            runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, max_workers=3, max_lifetime_seconds=3600,
            memory_limit_bytes=4 * 128 * 2**20,
        )),
        prepare=observe_prepare,
    )
    with pytest.raises(PinObserved):
        await cloudflare_canary.run_workers_canary(
            PlatformJobContext(job.id, lease, None, str(OPERATOR_ID), "test@example.test", "Test"),
            installation.id, 1, deployment.worker_ingress_origin, client, deployment,
        )


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
    pin = ManagedCloudflareScope(installation.id, "workers", 1).pin(
        worker.id, 1, reserved_at=datetime.now(UTC), lifetime=120, deadline_wire=False,
    )
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
async def test_enable_retracts_workers_admission_without_container_readiness(
    db_session, installation, monkeypatch,
) -> None:
    installation.desired_state = "enabled"
    installation.admitted_revision = installation.desired_revision
    installation.containers_policy = container_policy().model_copy(update={"enabled": True}).model_dump(mode="json")
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session

    async def locked_current(session, _context, payload):
        return await session.get(CloudflareInstallation, payload.installation_id, with_for_update=True)

    def unexpected_provider_request(_request):
        pytest.fail("Enable contacted the provider without joint readiness")

    monkeypatch.setattr(enable_job, "get_db_context", context)
    monkeypatch.setattr(enable_job, "_locked_current", locked_current)
    payload = enable_job.CloudflareEnablePayload(installation_id=installation.id, revision=installation.desired_revision)
    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected_provider_request)) as http:
        with pytest.raises(PlatformJobRequiresAction) as blocked:
            await enable_job._settle_enable(MagicMock(), payload, http, MagicMock())
    assert blocked.value.result["code"] == "CF_CONTAINERS_UNVERIFIED"
    await db_session.refresh(installation)
    assert installation.admitted_revision is None


@pytest.mark.asyncio
@pytest.mark.parametrize("create_failure", [
    "lost-ack", "list", 400, 401, 403, 409, 429, "ambiguous-absent", "ack-readback", "ack-absent",
    "late-400", "late-403-enable", "late-403-disconnect", "late-409", "late-429",
])
async def test_container_application_effect_survives_provisioning_failures(
    db_session, installation, monkeypatch, create_failure,
) -> None:
    from src.services.worker_pool.cloudflare_containers_rest import ContainersRest
    from src.services.worker_pool.cloudflare_workers_rest import (
        OWNER_BINDING,
        CloudflareManagementDenied,
        worker_owner_value,
    )

    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_rest._PLACEMENT_READ_INTERVAL", 0)
    release, _, _ = make_release()
    installation.desired_state = "enabled"
    installation.containers_policy = {"enabled": True}
    namespace = "b" * 32
    name = f"bifrost-{installation.id.hex}-r1"
    launcher = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="containers", kind="worker_script", target_name=f"{name}-containers",
        desired_revision=1, artifact_digest=release.digest, effect_id=uuid4(),
        state="present", provider_id=str(uuid4()), provider_version=str(uuid4()),
        namespace_ids={"ContainerLauncher": namespace}, observed_at=datetime.now(UTC),
    )
    db_session.add(launcher)
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def transaction():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    async def locked_current(session, _context, payload):
        return await session.get(CloudflareInstallation, payload.installation_id, with_for_update=True)

    monkeypatch.setattr(enable_job, "get_db_context", transaction)
    monkeypatch.setattr(enable_job, "_locked_current", locked_current)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    monkeypatch.setattr(enable_job, "load_containers_release", AsyncMock(return_value=release))
    monkeypatch.setattr(enable_job, "_deploy_container_launcher", AsyncMock(return_value=True))
    monkeypatch.setattr(enable_job, "WorkersRestCleanup", lambda _: SimpleNamespace(verify_workers_paid=AsyncMock()))
    monkeypatch.setattr(enable_job, "_drain_previous_revision", AsyncMock(return_value=True))
    workers_release = SimpleNamespace(
        digest=installation.policy["bounds_release"],
        manifest=SimpleNamespace(release_version="1.0.0", release_sequence=1),
    )
    present = False
    launcher_present = True
    placement_state = "running"
    observed_namespace = "e" * 32
    writes: list[str] = []
    empty_reads: list[float] = []
    late = isinstance(create_failure, str) and create_failure.startswith("late-")
    listed = not late
    fault = "lost-ack" if late else create_failure
    provider_id = str(uuid4())
    application = {
        "id": provider_id, "name": name, "account_id": installation.account_id,
        "scheduling_policy": "durable_object", "durable_objects": {"namespace_id": namespace},
    }

    def provider(request):
        nonlocal present, launcher_present, listed, fault
        path = request.url.path
        if path.endswith("/workers/durable_objects/namespaces"):
            result = [{
                "id": observed_namespace, "class": "ContainerLauncher", "script": launcher.target_name,
            }] if launcher_present else []
            return httpx.Response(200, json={"success": True, "result": result})
        if "/workers/scripts/" in path:
            if not launcher_present:
                return httpx.Response(404)
            if request.method == "DELETE":
                assert not present, "The application must retire before its namespace"
                launcher_present = False
                writes.append("DELETE script")
                return httpx.Response(204)
            if path.endswith("/settings"):
                result = {"bindings": [{
                    "name": OWNER_BINDING, "type": "plain_text",
                    "text": worker_owner_value(lifecycle_job._resource_intent(launcher)),
                }]}
            else:
                assert path.endswith("/deployments")
                result = {"deployments": [{
                    "id": launcher.provider_id,
                    "versions": [{"version_id": launcher.provider_version, "percentage": 100}],
                }]}
            return httpx.Response(200, json={"success": True, "result": result})
        if request.method == "POST":
            assert path.endswith("/containers/applications")
            assert not present or (late and isinstance(fault, int) and not listed)
            assert json.loads(request.content) == {
                key: application[key] for key in ("name", "scheduling_policy", "durable_objects")
            }
            writes.append("POST")
            if isinstance(fault, int):
                status, fault = fault, None
                return httpx.Response(status)
            if fault in ("lost-ack", "ambiguous-absent"):
                present = fault == "lost-ack"
                fault = None
                raise httpx.ReadTimeout("lost create acknowledgement", request=request)
            if fault == "ack-absent":
                listed = False
            present = True
            return httpx.Response(200, json={"success": True, "result": application})
        if request.method == "DELETE":
            assert path.endswith(f"/containers/applications/{provider_id}") and present
            present = False
            writes.append("DELETE")
            raise httpx.ReadTimeout("lost delete acknowledgement", request=request)
        if path.endswith("/applications"):
            assert request.url.params["name"] == name
            if fault == "list" or (fault == "ack-readback" and present):
                fault = None
                return httpx.Response(503)
            result = [application] if present and listed else []
            if not result and writes == ["POST"]:
                empty_reads.append(perf_counter())
        elif path.endswith(f"/{provider_id}/instances-v2"):
            assert request.url.params["state"] == "active"
            result = [{
                "application_id": provider_id, "id": "f" * 64, "status": {"state": placement_state},
            }] if placement_state == "running" else []
        else:
            assert path.endswith(f"/applications/{provider_id}")
            if not present or not listed:
                return httpx.Response(404)
            result = application
        return httpx.Response(200, json={"success": True, "result": result})

    payload = enable_job.CloudflareEnablePayload(installation_id=installation.id, revision=1)
    job_context = PlatformJobContext(uuid4(), uuid4(), None, str(OPERATOR_ID), "", "")
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        adapter = ContainersRest(http)
        with pytest.raises(OwnershipConflict) as wrong_namespace:
            await enable_job._settle_enable(job_context, payload, http, workers_release)
        assert wrong_namespace.value.as_result() == {
            "resource_class": "durable_object_namespace", "check": "receipt_mismatch",
            "observed_category": "different", "expected_category": "exact_receipt",
        }
        assert not writes
        observed_namespace = namespace
        failure = {
            400: ObservationSettling, 401: CloudflareManagementDenied, 403: CloudflareManagementDenied,
            409: ObservationSettling, "ack-absent": ObservationSettling,
        }.get(create_failure, CloudflareRestUnavailable)
        with pytest.raises(failure):
            await enable_job._settle_enable(job_context, payload, http, workers_release)

        journal = await db_session.scalar(select(CloudflareOwnedResource).where(
            CloudflareOwnedResource.installation_id == installation.id,
            CloudflareOwnedResource.kind == "container_application",
        ))
        async def cleanup():
            async with transaction() as session:
                row = await session.get(CloudflareInstallation, installation.id)
                assert row is not None
                return await lifecycle_job._cleanup_one(
                    session, row, adapter, container_adapter=adapter,
                )

        if create_failure == "list":
            assert journal is None and not writes
        else:
            assert journal is not None and writes == ["POST"]
            assert (journal.provider_details.get("creation_sent") is True) == (create_failure not in (401, 403))
            assert (journal.provider_id == provider_id) == (create_failure in ("ack-readback", "ack-absent"))
            if create_failure == "ack-absent":
                with pytest.raises(ObservationPending, match="recorded Container application is absent"):
                    await enable_job._settle_enable(job_context, payload, http, workers_release)
                assert writes == ["POST"]
                listed = True
            window = timedelta(milliseconds=100)
            monkeypatch.setattr(lifecycle_job, "CREATE_SETTLEMENT_WINDOW", window)
            monkeypatch.setattr(enable_job, "CREATE_SETTLEMENT_WINDOW", window)
            if create_failure in (400, 409, 429) or late:
                # Age of the send alone does not establish a settled absence.
                assert journal.observed_at is not None
                await asyncio.sleep(window.total_seconds())
                with pytest.raises(ObservationSettling) as waiting:
                    await enable_job._provision_container_application(job_context, payload, adapter, release)
                assert waiting.value.check is ObservationCheck.CONTAINER_CREATE
                assert writes == ["POST"]
                await db_session.refresh(journal)
                assert journal.observed_at is not None and journal.provider_details["creation_empty_observed"]
            if late:
                fault = int(create_failure.split("-")[1])
                retry_failure = ({400: ObservationSettling, 403: CloudflareManagementDenied, 409: ObservationSettling}
                                 .get(fault, CloudflareRestUnavailable))
                with pytest.raises(retry_failure):
                    await enable_job._settle_enable(job_context, payload, http, workers_release)
                await db_session.refresh(journal)
                assert journal.provider_details["creation_ambiguous"] is True
                assert (journal.provider_details.get("creation_sent") is True) == ("403" not in create_failure)
                assert writes == ["POST", "POST"]
                # A fresh post-attempt empty read cannot discard the prior send.
                assert await cleanup() == "settling"
                listed = True

        if create_failure != "late-403-disconnect":
            for _ in range(2):
                with pytest.raises(PlatformJobRequiresAction) as held:
                    await enable_job._settle_enable(job_context, payload, http, workers_release)
                assert held.value.result["code"] == "CF_CONTAINERS_UNVERIFIED"
            await db_session.refresh(installation)
            assert installation.admitted_revision is None
            if create_failure == "ambiguous-absent":
                assert len(empty_reads) >= 2
                assert empty_reads[-1] - empty_reads[0] >= window.total_seconds()

        assert await cleanup() == "progressed"
        with pytest.raises(lifecycle_job.DeletionSettling):
            await cleanup()
        expected_posts = ["POST"] * (
            2 if isinstance(create_failure, int) or create_failure == "ambiguous-absent" or late else 1
        )
        assert writes == expected_posts
        placement_state = "stopped"
        with pytest.raises(CloudflareRestUnavailable):
            await cleanup()
        assert await cleanup() == "progressed"
        assert await cleanup() == "progressed"
        assert await cleanup() == "progressed"
        assert await cleanup() == "complete"
    assert writes == [*expected_posts, "DELETE", "DELETE script"]
    assert await db_session.scalar(select(CloudflareOwnedResource.state).where(
        CloudflareOwnedResource.installation_id == installation.id,
        CloudflareOwnedResource.kind == "container_application",
    )) == "absent"


@pytest.mark.asyncio
@pytest.mark.parametrize("canary_result", ["missing", "failed", "ready"])
async def test_cold_enable_prepares_image_and_deploys_container_application(
    db_session, installation, monkeypatch, tmp_path, canary_result,
) -> None:
    from src.models.orm.platform_jobs import PlatformJob
    from src.services.worker_pool import cloudflare_containers_control, cloudflare_containers_release, cloudflare_containers_rest
    from src.worker.remote.container_egress import CONTAINER_EGRESS_DENIALS
    from src.worker.remote.protocol import nonce_digest

    canary_started = canary_result != "missing"
    canary_ready = canary_result == "ready"
    container, image, config = make_release()
    manifest = container.manifest
    workers = SimpleNamespace(digest=manifest.workers_release_digest, manifest=SimpleNamespace(
        release_version="1.0.0", release_sequence=1, source_revision=container.manifest.source_revision,
        meter_bounds=SimpleNamespace(qualification=SimpleNamespace(expected_cpu_ms={"capacity": 9})),
    ))
    installation.policy = {
        **installation.policy, "bounds_release": workers.digest,
        "limits": {**installation.policy["limits"], "monthly_limit_usd": "500"},
    }
    cloudflare_policy.set_workers_release_floor(installation, workers.digest, TEST_ORDER)
    installation.desired_state = "enabled"
    installation.containers_policy = container_policy().model_copy(update={"enabled": True}).model_dump(mode="json")
    await db_session.flush()
    connection = await db_session.connection()
    signer = tmp_path / "linux-release-signer"
    signer.write_text("https://github.com/example/bifrost/.github/workflows/ci.yml@refs/tags/v1.0.0\n", encoding="ascii")
    assets = {
        "manifest.json": json_bytes(manifest.model_dump(mode="json")),
        "manifest.sigstore.json": b"{}",
        "container-registry.mjs": container.launcher_module,
    }
    digest = sha256(assets["manifest.json"]).hexdigest()
    name = f"bifrost-{installation.id.hex}-r1"
    namespace = "a" * 32
    version = str(uuid4())
    deployment = str(uuid4())
    full_image = f"registry.cloudflare.com/{installation.account_id}/{name}@{manifest.image_digest}"
    events = []
    bindings = []
    application = None
    started = stopped = None
    actor = "f" * 64
    context = PlatformJobContext(uuid4(), uuid4(), None, str(OPERATOR_ID), "", "")
    if canary_started:
        db_session.add(PlatformJob(
            id=context.job_id, job_type="capacity.cloudflare_enable", title="Container qualification",
            payload={"revision": 1}, status="running", lease_token=context.lease_token,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            resource_type="cloudflare_installation", resource_id=str(installation.id),
            requested_by_user_id=str(OPERATOR_ID), requested_by_name="fixture", requested_by_email="fixture@example.test",
        ))
        await db_session.flush()
    copied = False
    endpoint = {"enabled": True, "previews_enabled": False}

    @asynccontextmanager
    async def transaction():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    async def locked_current(session, _context, payload, *, lock_row=True):
        return await session.get(CloudflareInstallation, payload.installation_id, with_for_update=lock_row)

    async def tool(executable, *args, **_kwargs):
        nonlocal copied
        match (executable.name, args):
            case ("cosign", ("version",)):
                return b"GitVersion: v2.4.1\n"
            case ("cosign", ("initialize",)):
                return b""
            case ("cosign", ("verify-blob", *_)):
                return b""
            case ("crane", ("version",)):
                return b"0.20.6"
            case ("crane", ("manifest", _)):
                return image
            case ("crane", ("config", _)):
                return config
            case ("crane", ("copy", source, target)):
                assert source == manifest.image and target == full_image.split("@")[0] + ":release" and not copied
                copied = True
                events.append("copy")
                return b""
            case ("cf", ("--version",)):
                return b"1.0.0-beta.31"
            case ("cf", ("accounts", "get", "-q")):
                return json_bytes({"id": installation.account_id})
            case ("cf", ("containers", "images", "list", "--filter", _)):
                return json_bytes([{"name": name, "tags": ["release"]}] if copied else [])
            case _:
                pytest.fail(f"Unexpected OCI tool call {executable.name}, {args}")

    async def provider(request):
        nonlocal bindings, application, started, stopped
        path = request.url.path
        if request.url.host == "github.com":
            return httpx.Response(200, content=assets[path.split("cloudflare-containers-")[1]])
        if request.url.host == f"{name}-containers.local-fixture.workers.dev":
            assert canary_started and application is not None
            assert request.headers.get("authorization") is None
            assert len(request.headers["x-bifrost-request-signature"]) == 128
            body = json.loads(request.content)
            identity = body["pin"]
            assert identity["tenant"] == "qualification-canary"
            canary_id = UUID(identity["worker"])
            if path == "/v1/egress-probe":
                assert len(body["control_key"]) == 64
                row = await db_session.get(ExecutionWorker, canary_id)
                pin = await db_session.get(CloudflareWorkerReservation, canary_id)
                assert row is not None and row.reserved_for_dispatch_id is None
                assert pin is not None and pin.container_envelope is not None
                assert pin.canary_job_id == context.job_id
                started = datetime.fromtimestamp(identity["reservedAt"] / 1000, UTC) + timedelta(milliseconds=1)
                row.state = "ready"
                row.remote_hello_sequence = 1
                row.remote_auth_nonce_digest = nonce_digest(body["nonce"])
                row.remote_session_generation = 1
                row.remote_owner_lease_expires_at = datetime.now(UTC) + timedelta(minutes=1)
                pin.canary_sdk_at = datetime.now(UTC)
                pin.canary_sdk_generation = 1
                if canary_ready:
                    pin.canary_ack_at = pin.canary_sdk_at
                    pin.container_envelope = {
                        **pin.container_envelope, "denial_generation": 1,
                        "egress_denials": sorted(CONTAINER_EGRESS_DENIALS),
                    }
                else:
                    job = await db_session.get(PlatformJob, context.job_id)
                    assert job is not None
                    job.result = {"canary_failure": {
                        "worker_id": str(canary_id), "generation": 1,
                        "reason": {"stage": "ack", "error_class": "invalid_reply"},
                    }}
                await db_session.flush()
                events.append("canary")
                return httpx.Response(200, json={
                    "nonce": body["nonce"], "worker_id": str(canary_id), "application_id": application["id"],
                    "provider_version": namespace, "origin": installation.worker_ingress_origin,
                    "revision": 1, "release_digest": digest,
                })
            assert "control_key" not in body
            if path == "/v1/stop":
                if stopped is None:
                    stopped = datetime.now(UTC)
                    events.append("stop")
                return httpx.Response(200, json={"status": "pending", "instance": actor})
            if path == "/v1/void":
                return httpx.Response(200, json={"status": "conflict"})
            assert started is not None and stopped is not None
            closed_at = int(stopped.timestamp() * 1000)
            if path == "/v1/retire":
                assert body["closed_at"] == closed_at
                events.append("retire")
                return httpx.Response(200, json={"status": "retired"})
            assert path == "/v1/close"
            return httpx.Response(200, json={
                "status": "closed", "instance": actor, "startedAt": int(started.timestamp() * 1000),
                "authorizedAt": identity["reservedAt"], "closedAt": closed_at, "meterComplete": False,
            })
        assert request.url.host == "api.cloudflare.com"
        assert request.headers["Authorization"] == "Bearer synthetic-cloudflare-management-token"
        if path.endswith("/entitlements"):
            result = [
                {"id": feature, "deleted_date": "", "allocation": {"type": "bool", "value": True}}
                for feature in ("workers.enabled", "containers.enabled")
            ]
        elif path.endswith(f"/accounts/{installation.account_id}"):
            result = {"id": installation.account_id}
        elif path.endswith("/credentials"):
            result = {"account_id": installation.account_id, "registry_host": "registry.cloudflare.com",
                      "username": "fixture", "password": "fixture"}
        elif path.endswith("/image-preparations"):
            assert copied and json.loads(request.content) == {"image": full_image}
            events.append("prepare")
            result = {"image": full_image, "status": "ready"}
        elif path.endswith(f"/workers/scripts/{name}-containers") and request.method == "PUT":
            parts = BytesParser(policy=email_policy.default).parsebytes(
                f"Content-Type: {request.headers['Content-Type']}\r\n\r\n".encode() + request.content,
            )
            content = {part.get_param("name", header="content-disposition"): part.get_payload(decode=True)
                       for part in parts.iter_parts()}
            metadata = json.loads(content["metadata"])
            assert content["container-registry.mjs"] == container.launcher_module
            assert metadata["containers"] == [{
                "name": "linux", "class_name": "ContainerLauncher", "images": {"bifrost-linux": full_image},
            }]
            assert metadata["exports"]["ContainerLauncher"]["container"] == "linux"
            bindings = metadata["bindings"]
            assert bindings[0]["name"] == OWNER_BINDING
            assert not any(binding["type"] == "secret_text" for binding in bindings)
            assert any(binding["name"] == "CONTAINERS_PUBLIC_KEY" and len(binding["text"]) == 64 for binding in bindings)
            assert bindings[0]["text"].endswith(f":1:{digest}")
            events.append("launcher")
            result = {"id": f"{name}-containers"}
        elif path.endswith(f"/{name}-containers/settings"):
            if not bindings:
                return httpx.Response(404)
            result = {"bindings": bindings}
        elif path.endswith(f"/{name}-containers/deployments"):
            result = {"deployments": [{"id": deployment, "versions": [{"version_id": version, "percentage": 100}]}]}
        elif path.endswith(f"/{name}-containers/subdomain"):
            result = endpoint
        elif path.endswith("/workers/subdomain"):
            result = {"subdomain": "local-fixture"}
        elif path.endswith("/workers/durable_objects/namespaces"):
            result = [
                {"id": namespace, "class": "ContainerLauncher", "script": f"{name}-containers"},
                {"id": "b" * 32, "class": "ContainerScopeRegistry", "script": f"{name}-containers"},
            ]
        elif path.endswith("/containers/applications"):
            if request.method == "POST":
                assert application is None and json.loads(request.content)["durable_objects"] == {"namespace_id": namespace}
                application = {**json.loads(request.content), "id": "c" * 32, "account_id": installation.account_id}
                events.append("application")
                result = application
            else:
                result = [application] if application else []
        elif path.endswith(f"/instances/{actor}"):
            assert application is not None and started is not None and stopped is not None
            result = {
                "id": actor, "application_id": application["id"], "image": full_image,
                "started_at": started.isoformat(), "status": {"state": "stopped", "updated_at": stopped.isoformat()},
            }
        else:
            assert application is not None and path.endswith(f"/containers/applications/{application['id']}")
            result = application
        return httpx.Response(200, json={"success": True, "result": result})

    monkeypatch.setattr(enable_job, "get_db_context", transaction)
    monkeypatch.setattr(cloudflare_containers_control, "get_db_context", transaction)
    monkeypatch.setattr(enable_job, "_locked_current", locked_current)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    monkeypatch.setattr(cloudflare_containers_release, "_SIGNER_FILE", signer)
    for module in (cloudflare_containers_release, cloudflare_containers_rest):
        monkeypatch.setattr(module, "run_oci_tool", tool)
    workers_deployments = {}

    async def create_workers(*, api_token, intent, scope_bearer):
        assert events[-1] == "retire"
        workers_deployments[intent.target.name] = ResourceObservation(intent, str(uuid4()), str(uuid4()))

    provisioner = SimpleNamespace(
        observe=AsyncMock(side_effect=lambda *, api_token, intent: workers_deployments.get(intent.target.name)),
        create=AsyncMock(side_effect=create_workers),
        configure_endpoint=AsyncMock(side_effect=lambda *, api_token, expected: ConfiguredWorkersScript(
            "https://capacity.example.test" if expected.intent.target.name.endswith("-capacity") else None, {},
        )),
    )
    monkeypatch.setattr(enable_job, "WorkersRestProvisioner", lambda *_: provisioner)
    monkeypatch.setattr(enable_job, "probe_worker_ingress", AsyncMock(return_value=ingress_probes.WorkerIngressProbeResult(
        installation.worker_ingress_origin, True, True, True,
        (ingress_probes.RouteProbe("GET", "/api/sdk/context", 401, True),),
        runtime_canary=ingress_probes.RuntimeCanary(True, True),
    )))
    payload = enable_job.CloudflareEnablePayload(installation_id=installation.id, revision=1)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        for _ in range(1 if canary_result == "failed" else 2):
            if canary_ready:
                assert (await enable_job._settle_enable(context, payload, http, workers))["outcome"] == "enabled"
            else:
                with pytest.raises(PlatformJobRequiresAction) as held:
                    await enable_job._settle_enable(context, payload, http, workers)
                assert held.value.result["code"] == "CF_CONTAINERS_UNVERIFIED"
                if canary_result == "failed":
                    assert held.value.result["canary_failure"] == {"stage": "ack", "error_class": "invalid_reply"}
    assert events == ["copy", "prepare", "launcher", "application"] + (["canary", "stop", "retire"] if canary_started else [])
    assert provisioner.create.await_count == (3 if canary_ready else 0)
    journals = (await db_session.scalars(select(CloudflareOwnedResource).where(
        CloudflareOwnedResource.installation_id == installation.id,
        CloudflareOwnedResource.scope == "containers",
    ))).all()
    assert {row.kind for row in journals} == {"container_image", "container_application", "worker_script"}
    assert all(row.state == "present" and row.artifact_digest == digest and row.scope == "containers" for row in journals)
    scope = await db_session.get(CloudflareCapacityScope, (installation.id, "containers", 1))
    assert scope is not None and scope.endpoint == f"https://{name}-containers.local-fixture.workers.dev"
    await db_session.refresh(installation)
    assert installation.admitted_revision == installation.containers_ready_revision == (1 if canary_ready else None)
    assert installation.containers_release_digest == (digest if canary_ready else None)


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", [
    "fetch", "unpublished", "signature", "wrong_digest", "missing_readiness", "ready", "readiness_lost",
])
async def test_opted_in_enable_admits_only_the_verified_revision(
    db_session, installation, monkeypatch, scenario, tmp_path,
) -> None:
    from src.services.worker_pool import cloudflare_containers_release
    from src.services.worker_pool.cloudflare_containers_release import ContainersReleaseFetchError, ContainersReleaseUnavailable

    revision = installation.desired_revision
    release, _, _ = make_release()
    installation.desired_state = "enabled"
    installation.containers_policy = {"enabled": True}
    installation.containers_ready_revision = None if scenario == "missing_readiness" else revision
    installation.containers_release_digest = None if scenario == "missing_readiness" else release.digest
    if scenario in ("fetch", "unpublished", "signature", "wrong_digest"):
        installation.admitted_revision = revision
    now = datetime.now(UTC)
    app = app_journal(installation, release.digest, now)
    if scenario == "missing_readiness":
        db_session.add(CloudflareOwnedResource(
            id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
            scope="containers", kind="worker_script", target_name=f"bifrost-{installation.id.hex}-r{revision}-containers",
            desired_revision=revision, artifact_digest=release.digest, effect_id=uuid4(),
            state="present", provider_id=str(uuid4()), provider_version=str(uuid4()),
            namespace_ids={"ContainerLauncher": "b" * 32}, observed_at=now,
        ))
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "containers", revision),
        SecretStr("synthetic-container-scope-bearer-key"), actor=str(OPERATOR_ID), now=now,
    ))
    await db_session.flush()
    db_session.add_all([app, CloudflareCapacityScope(
        installation_id=installation.id, scope="containers", deployment_revision=revision,
        endpoint="https://containers.example.test", bearer_version=revision,
    )])
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def transaction():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    async def locked_current(session, _context, payload):
        return await session.get(CloudflareInstallation, payload.installation_id, with_for_update=True)

    async def probe(origin, **_kwargs):
        # Model the independently verified readiness disappearing after deployment.
        if scenario == "readiness_lost":
            async with transaction() as session:
                live_app = await session.get(CloudflareOwnedResource, app.id)
                assert live_app is not None
                live_app.state = "absent"
        return ingress_probes.WorkerIngressProbeResult(
            origin, True, True, True, (ingress_probes.RouteProbe("GET", "/api/sdk/context", 401, True),),
            runtime_canary=ingress_probes.RuntimeCanary(True, True),
        )

    workers_release = SimpleNamespace(digest=installation.policy["bounds_release"], manifest=SimpleNamespace(
        release_version="1.0.0", release_sequence=1,
        meter_bounds=SimpleNamespace(qualification=SimpleNamespace(expected_cpu_ms={"capacity": 9})),
    ))
    loader = AsyncMock(return_value=release, side_effect={
        "fetch": ContainersReleaseFetchError("Fixture network error"),
        "signature": ContainersReleaseUnavailable("Fixture signature failure"),
    }.get(scenario))
    if scenario == "wrong_digest":
        loader.return_value = replace(release, digest="f" * 64)
    if scenario == "unpublished":
        trust = tmp_path / "linux-release-signer"
        trust.write_text(
            "https://github.com/example/bifrost/.github/workflows/ci.yml@refs/tags/v1.0.0\n", encoding="ascii",
        )
        monkeypatch.setattr(cloudflare_containers_release, "_SIGNER_FILE", trust)
        loader = AsyncMock(wraps=cloudflare_containers_release.load_containers_release)

    def provider(request: httpx.Request) -> httpx.Response:
        assert scenario == "unpublished" and request.method == "GET"
        assert str(request.url) == (
            "https://github.com/example/bifrost/releases/download/v1.0.0/cloudflare-containers-manifest.json"
        )
        return httpx.Response(404)

    workers_deployments = {}

    async def create(*, api_token, intent, scope_bearer):
        workers_deployments[intent.target.name] = ResourceObservation(intent, str(uuid4()), str(uuid4()))

    provisioner = SimpleNamespace(
        observe=AsyncMock(side_effect=lambda *, api_token, intent: workers_deployments.get(intent.target.name)),
        create=AsyncMock(side_effect=create),
        configure_endpoint=AsyncMock(side_effect=lambda *, api_token, expected: ConfiguredWorkersScript(
            "https://capacity.example.test" if expected.intent.target.name.endswith("-capacity") else None, {},
        )),
    )
    monkeypatch.setattr(enable_job, "get_db_context", transaction)
    monkeypatch.setattr(enable_job, "_locked_current", locked_current)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    monkeypatch.setattr(enable_job, "load_containers_release", loader)
    monkeypatch.setattr(enable_job, "_deploy_container_launcher", AsyncMock(return_value=True))
    paid = AsyncMock()
    monkeypatch.setattr(enable_job, "WorkersRestCleanup", lambda _: SimpleNamespace(verify_workers_paid=paid))
    monkeypatch.setattr(enable_job, "_drain_previous_revision", AsyncMock(return_value=True))
    provision = AsyncMock(return_value=True)
    monkeypatch.setattr(enable_job, "_provision_container_application", provision)
    monkeypatch.setattr(enable_job, "WorkersRestProvisioner", lambda *_: provisioner)
    monkeypatch.setattr(enable_job, "probe_worker_ingress", probe)

    payload = enable_job.CloudflareEnablePayload(installation_id=installation.id, revision=revision)
    running = PlatformJobContext(uuid4(), uuid4(), None, str(OPERATOR_ID), "", "")
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        if scenario == "ready":
            assert await enable_job._settle_enable(running, payload, http, workers_release) == {
                "outcome": "enabled", "http_connections": [],
            }
        elif scenario in ("fetch", "unpublished"):
            with pytest.raises(PlatformJobFailure) as exc:
                await enable_job._settle_enable(running, payload, http, workers_release, repair_tunnel_ingress=True)
            assert exc.value.code == "CF_CONTAINERS_RELEASE_FETCH" and exc.value.retryable
        else:
            with pytest.raises(PlatformJobRequiresAction) as exc:
                await enable_job._settle_enable(running, payload, http, workers_release)
            assert exc.value.result["code"] == (
                "CF_CONTAINERS_UNVERIFIED"
                if scenario in ("missing_readiness", "readiness_lost")
                else "CF_CONTAINERS_RELEASE_UNAVAILABLE"
            )
        loader.assert_awaited_once_with(http, workers_release)
    await db_session.refresh(installation)
    assert installation.admitted_revision == (revision if scenario in ("ready", "fetch", "unpublished") else None)
    if scenario in ("ready", "readiness_lost"):
        provision.assert_awaited_once()
        assert provisioner.create.await_count == 3
    else:
        provisioner.create.assert_not_awaited()
        if scenario == "missing_readiness":
            provision.assert_awaited_once()
        else:
            paid.assert_not_awaited()
            provision.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("readback", ["ambiguous_namespaces", "invisible_upload"])
async def test_enable_keeps_admission_closed_on_unverified_readback(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, readback,
) -> None:
    digest = "d" * 64
    installation.policy = {**installation.policy, "bounds_release": digest}
    release = SimpleNamespace(digest=digest, manifest=SimpleNamespace(
        release_version="1.1.0", release_sequence=1,
        workers={role: SimpleNamespace(exports={}) for role in ("gateway", "launcher")},
    ))
    job, _ = await enable_job.enqueue_cloudflare_enable(
        db_session, _platform_operator(), installation_id=installation.id, expected_revision=1,
    )
    lease = uuid4()
    job.status, job.lease_token = "running", lease
    await db_session.flush()
    sessions = async_sessionmaker(
        await db_session.connection(), autoflush=False, expire_on_commit=False, join_transaction_mode="create_savepoint",
    )
    uploaded: dict[str, ResourceObservation] = {}

    def provider(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET" and request.url.host == "api.cloudflare.com"
        if request.url.path.endswith("/namespaces"):
            result = [
                {"script": name, "class": "FixtureNamespace", "id": f"fixture-namespace-{index}", "preview": False}
                for name in uploaded if name.endswith("-launcher") for index in range(2)
            ]
            return httpx.Response(200, json={
                "success": True, "result": result, "result_info": {"page": 1, "total_count": len(result)},
            })
        assert request.url.path.endswith("-gateway/subdomain")
        return httpx.Response(200, json={"success": True, "result": {"enabled": False, "previews_enabled": False}})

    class Provisioner(WorkersRestProvisioner):
        async def observe(self, *, api_token: SecretStr, intent: ResourceIntent) -> ResourceObservation | None:
            if readback == "invisible_upload":
                return None
            return uploaded.get(intent.target.name)

        async def create(self, *, api_token: SecretStr, intent: ResourceIntent, scope_bearer: SecretStr | None) -> None:
            name = intent.target.name
            assert name not in uploaded and scope_bearer is None
            uploaded[name] = ResourceObservation(intent, str(uuid4()), str(uuid4()))

    monkeypatch.setattr(enable_job, "get_db_context", sessions.begin)
    monkeypatch.setattr(enable_job, "load_workers_release", lambda: release)
    monkeypatch.setattr(enable_job, "WorkersRestCleanup", lambda _: SimpleNamespace(verify_workers_paid=AsyncMock()))
    monkeypatch.setattr(enable_job, "WorkersRestProvisioner", Provisioner)
    monkeypatch.setattr(enable_job, "httpx", SimpleNamespace(
        AsyncClient=lambda **_: httpx.AsyncClient(transport=httpx.MockTransport(provider)),
    ))
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    ingress = AsyncMock()
    monkeypatch.setattr(enable_job, "probe_worker_ingress", ingress)
    context = PlatformJobContext(job.id, lease, None, str(OPERATOR_ID), "operator@example.test", "")
    payload = enable_job.CloudflareEnablePayload.model_validate(job.payload)

    if readback == "invisible_upload":
        for _ in range(2):
            with pytest.raises(PlatformJobFailure) as pending:
                await enable_job.CLOUDFLARE_ENABLE_DEFINITION.handler(context, payload)
            assert pending.value.retryable
            assert pending.value.result == {"code": "CF_DEPLOYMENT_UNVERIFIED", "details": {
                "resource_class": "worker_script", "check": "upload_readback",
                "observed_category": "absent", "expected_category": "exact_uploaded_script",
            }}
        name, = uploaded
        journal = await db_session.scalar(select(CloudflareOwnedResource).where(
            CloudflareOwnedResource.installation_id == installation.id,
            CloudflareOwnedResource.target_name == name,
        ))
        assert journal is not None and journal.state == "creating"
        assert journal.provider_details == {"upload_acknowledged": True}
        ingress.assert_not_awaited()
        await db_session.refresh(installation)
        assert installation.admitted_revision is None
        return

    with pytest.raises(PlatformJobRequiresAction) as failure:
        await enable_job.CLOUDFLARE_ENABLE_DEFINITION.handler(context, payload)

    assert failure.value.result == {
        "code": "CF_OWNERSHIP_CONFLICT",
        "details": {
            "resource_class": "durable_object_namespace", "check": "namespace_bindings",
            "observed_category": "multiple", "expected_category": "one_per_class",
        },
    }
    ingress.assert_not_awaited()
    await db_session.refresh(installation)
    assert installation.admitted_revision is None
    rows = (await db_session.scalars(select(CloudflareOwnedResource).where(
        CloudflareOwnedResource.installation_id == installation.id,
    ))).all()
    assert {row.target_name.rsplit("-", 1)[1]: row.state for row in rows} == {
        "gateway": "present", "launcher": "creating", "capacity": "creating",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(("aborted_rotation", "connector_delay"), [
    (0, None), (2, None), (3, None),
    (0, 140), (0, -1),
], ids=["existing", "rotation-intent", "rotation-eviction", "slow-projection", "no-connector"])
async def test_enable_requires_signed_sdk_evidence_and_resumes_owned_uploads(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, aborted_rotation, connector_delay,
) -> None:
    installation.policy = {**installation.policy, "bounds_release": "d" * 64}
    connector = str(uuid4())
    connectors: set[str] = set()
    if aborted_rotation or connector_delay is not None:
        installation.reachability_mode = "managed_tunnel"
        installation.zone_id = _TunnelAPI.zone
        active_version = aborted_rotation - 1 if aborted_rotation else 1
        installation.tunnel_token_version = active_version
        installation.tunnel_token_projected_version = active_version if connector_delay is None else 0
        receipt = CloudflareOwnedResource(
            id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
            scope="reachability", kind=cloudflare_tunnel.TUNNEL, target_name="fixture-tunnel", desired_revision=1,
            artifact_digest="c" * 64, effect_id=uuid4(), state="present",
            provider_id=str(uuid4()), provider_version="fixture-created",
        )
        db_session.add(receipt)
        await db_session.flush()
        if connector_delay is None:
            connectors.add(connector)
        previous = str(uuid4())
        if active_version == 2:
            receipt.provider_details = {"rotation_version": 2, "rotation_previous_connectors": [previous]}
            connectors.add(previous)
        tunnel = SimpleNamespace(
            tunnel=AsyncMock(side_effect=lambda _, expected: expected),
            connectors=AsyncMock(side_effect=lambda _: frozenset(connectors)),
            evict_connectors=AsyncMock(side_effect=lambda _, excluded: connectors.difference_update(excluded)),
        )
        if aborted_rotation:
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
    monkeypatch.setattr(lifecycle_job, "get_db_context", context)
    monkeypatch.setattr(cloudflare_spend, "get_db_context", context)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    observed = {}
    writes = []
    provider_seconds = 0.0

    async def advance(seconds: float) -> None:
        nonlocal provider_seconds
        provider_seconds += seconds
        if connector_delay is not None and 0 <= connector_delay <= provider_seconds and not connectors:
            async with context() as db:
                row = await db.get(CloudflareInstallation, installation.id)
                assert row is not None
                row.tunnel_token_projected_version = row.tunnel_token_version
            connectors.add(connector)

    if connector_delay is not None:
        monkeypatch.setattr(lifecycle_job, "asyncio", SimpleNamespace(sleep=advance))
        monkeypatch.setattr(lifecycle_job, "monotonic", lambda: provider_seconds)
    capacity_namespaces_visible_at = None

    class Provider:
        async def observe(self, *, api_token, intent):
            assert api_token == SecretStr("synthetic-cloudflare-management-token")
            return observed.get(intent.target.name)

        async def create(self, *, api_token, intent, scope_bearer):
            nonlocal capacity_namespaces_visible_at
            assert (scope_bearer is not None) == intent.target.name.endswith("-capacity")
            writes.append(intent.target.name)
            observed[intent.target.name] = ResourceObservation(intent, str(uuid4()), str(uuid4()))
            if scope_bearer is not None and not aborted_rotation and connector_delay is None:
                capacity_namespaces_visible_at = provider_seconds + 10

        async def configure_endpoint(self, *, api_token, expected):
            name = expected.intent.target.name
            if (
                name.endswith("-capacity") and capacity_namespaces_visible_at is not None
                and provider_seconds < capacity_namespaces_visible_at
            ):
                raise ObservationSettling(
                    "Fixture namespace is not yet visible",
                    OwnershipResource.DURABLE_OBJECT_NAMESPACE, ObservationCheck.NAMESPACE_EXPORTS_PENDING,
                )
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
        if connector_delay is not None:
            if connector_delay < 0:
                with pytest.raises(PlatformJobFailure) as unavailable:
                    await enable_job._settle_enable(running, payload, http, release)
                assert unavailable.value.code == "CF_CONNECTOR_UNAVAILABLE"
                assert not unavailable.value.retryable and provider_seconds == 180
                assert not writes
            else:
                probe_results = iter((reachable,))
                canary.side_effect = None
                canary.return_value = ingress_probes.RuntimeCanary(True, True)
                result = await enable_job._settle_enable(running, payload, http, release)
                assert result["outcome"] == "enabled" and provider_seconds == 140
            await db_session.refresh(installation)
            assert installation.admitted_revision == (None if connector_delay < 0 else payload.revision)
            return

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
            assert budget.value.result == {
                "code": "CF_BUDGET_REACHED",
                "details": {
                    "resource_class": "spend_guard", "check": "monthly_usage",
                    "observed_category": "limit_reached", "expected_category": "under_limit",
                },
            }
            recovering.assert_not_awaited()
            await db_session.delete(blocked)
            await db_session.flush()
            with pytest.raises(PlatformJobFailure) as settling:
                await enable_job._settle_enable(running, payload, http, release)
            assert settling.value.code == "CF_CLEANUP_SETTLING" and settling.value.retryable
            recovering.assert_awaited_once()
        if not aborted_rotation:
            with pytest.raises(ObservationSettling) as lag:
                await enable_job._settle_enable(running, payload, http, release)
            assert lag.value.check is ObservationCheck.NAMESPACE_EXPORTS_PENDING
            assert sum(name.endswith("-capacity") for name in writes) == 1
            await db_session.refresh(installation)
            assert installation.admitted_revision is None
            provider_seconds += int(enable_job.CLOUDFLARE_ENABLE_DEFINITION.policy.retry_delay[0].total_seconds())
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


@pytest.fixture
def isolated_connector_projection(isolated_capacity_session_factory, monkeypatch, tmp_path):
    sessions = isolated_capacity_session_factory
    settings = SimpleNamespace(cloudflare_tunnel_channel="compose")
    monkeypatch.setattr(cloudflare_tunnel, "get_settings", lambda: settings)
    monkeypatch.setattr(cloudflare_connector_channel, "get_settings", lambda: settings)
    token_file = tmp_path / "token"
    monkeypatch.setattr(cloudflare_connector_channel, "COMPOSE_PATH", token_file)

    @asynccontextmanager
    async def projection_context():
        async with sessions.begin() as db:
            await db.execute(text("SET LOCAL lock_timeout = '500ms'"))
            yield db

    monkeypatch.setattr(cloudflare_connector_channel, "get_db_context", projection_context)
    monkeypatch.setattr(lifecycle_job, "get_db_context", sessions.begin)
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    return sessions, token_file


@pytest.mark.asyncio
async def test_enable_replays_tunnel_after_readiness_timeout_without_blocking_projection(
    isolated_connector_projection, monkeypatch,
) -> None:
    sessions, token_file = isolated_connector_projection
    provider = _TunnelAPI(lose_create_responses=False)
    async with sessions.begin() as db:
        installation_id = await create_installation(db, provider.account)
        row = await db.get(CloudflareInstallation, installation_id)
        assert row is not None
        row.reachability_mode, row.zone_id = "managed_tunnel", provider.zone
        row.policy = {**row.policy, "bounds_release": "d" * 64}
        job, _ = await enable_job.enqueue_cloudflare_enable(
            db, _platform_operator(), installation_id=installation_id, expected_revision=1,
        )
        lease = uuid4()
        job.status, job.lease_token = "running", lease
    monkeypatch.setattr(enable_job, "get_db_context", sessions.begin)
    monkeypatch.setattr(cloudflare_spend, "get_db_context", sessions.begin)
    create_script = AsyncMock()
    monkeypatch.setattr(enable_job, "request_creation", create_script)

    async def install_script(_adapter, resource, *, token):
        resource.state = "present"
        resource.provider_id = resource.target_name
        resource.provider_version = "fixture-replay"
        endpoint = "https://capacity.example.test" if resource.target_name.endswith("-capacity") else None
        return ConfiguredWorkersScript(endpoint, {})

    monkeypatch.setattr(enable_job, "_install_script", install_script)
    monkeypatch.setattr(enable_job, "probe_worker_ingress", AsyncMock(return_value=SimpleNamespace(
        ready=True, verified_at=None, runtime_canary=None, http_connections_as_result=lambda: [],
    )))

    elapsed = 0.0
    replaying = False
    projected = False

    async def advance(seconds):
        nonlocal elapsed
        elapsed += seconds

    async def slow_provider(request):
        nonlocal projected
        if replaying and not projected and request.url.path.endswith("/connections"):
            # The provider has not returned the readiness read yet. Projection uses
            # a different DB connection and must still acquire the installation row.
            await cloudflare_connector_channel.project_connector(None)
            projected = True
            assert token_file.read_bytes()
            provider.connectors.append(str(uuid4()))
        return provider(request)

    async def connector_credentials():
        async with sessions() as db:
            return (await db.execute(select(CloudflareCredential.version, CloudflareCredential.ciphertext).where(
                CloudflareCredential.installation_id == installation_id,
                CloudflareCredential.purpose == "connector_token",
            ))).all()

    monkeypatch.setattr(lifecycle_job, "asyncio", SimpleNamespace(sleep=advance))
    monkeypatch.setattr(lifecycle_job, "monotonic", lambda: elapsed)
    running = PlatformJobContext(job.id, lease, None, str(OPERATOR_ID), "operator@example.test", "")
    payload = enable_job.CloudflareEnablePayload.model_validate(job.payload)
    release = SimpleNamespace(
        digest="d" * 64, manifest=SimpleNamespace(
            release_version="1.1.0", release_sequence=1,
            meter_bounds=SimpleNamespace(qualification=SimpleNamespace(expected_cpu_ms={"capacity": 9})),
        ),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_provider)) as http:
        with pytest.raises(PlatformJobFailure) as first:
            await enable_job._settle_enable(running, payload, http, release)
        assert first.value.code == "CF_CONNECTOR_UNAVAILABLE" and not first.value.retryable
        assert elapsed == 180
        create_script.assert_not_awaited()
        credentials = await connector_credentials()
        assert len(credentials) == 1 and credentials[0].version == 1 and credentials[0].ciphertext
        replaying = True
        assert (await enable_job._settle_enable(running, payload, http, release))["outcome"] == "enabled"
    assert projected
    assert create_script.await_count == 3
    assert await connector_credentials() == credentials
    assert provider.tunnel_creates == 1 and provider.rotations == 0
    async with sessions() as db:
        row = await db.get(CloudflareInstallation, installation_id)
        assert row is not None and row.deployment_evidence["connector_session"] == "connected"
        assert row.admitted_revision == row.deployed_revision == payload.revision


@pytest.fixture
async def managed_tunnel_for_projection(isolated_connector_projection):
    sessions, token_file = isolated_connector_projection
    provider = _TunnelAPI(lose_create_responses=False)
    actor = str(OPERATOR_ID)
    async with sessions.begin() as db:
        installation_id = await create_installation(db, provider.account)
        row = await db.get(CloudflareInstallation, installation_id)
        assert row is not None
        row.reachability_mode, row.zone_id = "managed_tunnel", provider.zone
        await cloudflare_tunnel.prepare(db, row, actor=actor)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http, sessions.begin() as db:
        row = await db.get(CloudflareInstallation, installation_id)
        assert row is not None
        client = cloudflare_tunnel.adapter(http, row, SecretStr("synthetic-cloudflare-management-token"))
        await cloudflare_tunnel.provision_tunnel(db, row, client)
        await cloudflare_tunnel.provision_dns(db, row, client, actor=actor)
    return sessions, token_file, installation_id, provider


def _running_tunnel_job(
    db: AsyncSession, installation_id: UUID, job_type: str, payload: dict,
) -> PlatformJobContext:
    job_id, lease = uuid4(), uuid4()
    db.add(PlatformJob(
        id=job_id, job_type=job_type, title="Fixture Managed Tunnel", payload=payload,
        status="running", lease_token=lease, lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        resource_type="cloudflare_installation", resource_id=str(installation_id),
        requested_by_user_id=str(OPERATOR_ID), requested_by_name="Fixture Operator",
        requested_by_email="operator@example.test",
    ))
    return PlatformJobContext(job_id, lease, None, str(OPERATOR_ID), "operator@example.test", "")


@pytest.mark.asyncio
@pytest.mark.parametrize(("race", "code"), [
    ("disconnected", "CF_CONNECTOR_UNAVAILABLE"),
    ("projection_completed", None),
    ("projection_completed_final", "CF_TUNNEL_VERIFICATION_CHANGED"),
    ("disconnected_final", "CF_CONNECTOR_UNAVAILABLE"),
    ("credential_denied", "CF_MANAGEMENT_CREDENTIAL"),
    ("credential_denied_final", "CF_MANAGEMENT_CREDENTIAL"),
    ("inventory_unverified_final", "CF_DEPLOYMENT_UNVERIFIED"),
    ("credential_replaced", None),
    ("ownership", "CF_OWNERSHIP_CONFLICT"),
    ("repair", "CF_TUNNEL_INGRESS_MISMATCH"),
    ("repair_superseded_before_reads", None),
    ("repair_superseded", None),
    ("repair_superseded_after_readback", None),
    ("target_replaced", None),
    ("admission_superseded", None),
    ("unhealthy_admission_superseded", None),
    ("healthy_projection_changed", None),
])
async def test_reconcile_fails_closed_and_fences_repair(managed_tunnel_for_projection, monkeypatch, race, code) -> None:
    sessions, token_file, installation_id, provider = managed_tunnel_for_projection
    payload = lifecycle_job.CloudflareReconcilePayload(installation_id=installation_id, revision=1)
    healthy_read = race in ("admission_superseded", "healthy_projection_changed", "inventory_unverified_final")
    if healthy_read or race in (
        "repair", "disconnected", "disconnected_final", "credential_denied", "credential_denied_final",
        "credential_replaced",
    ):
        assert await cloudflare_connector_channel.project_connector(None) == (installation_id, 1, 1)
    if healthy_read:
        provider.connectors = [str(uuid4())]
    if race == "ownership":
        provider.private_routes = [{"tunnel_id": provider.tunnel_id, "network": "192.0.2.0/24"}]
    if race.startswith("repair"):
        provider.config["warp-routing"] = {"enabled": True}
    async with sessions.begin() as db:
        row = await db.get(CloudflareInstallation, installation_id)
        assert row is not None
        row.desired_state = "enabled"
        row.admitted_revision = row.deployed_revision = 1
        row.deployment_evidence = {"connector_session": "disconnected"}
        running = _running_tunnel_job(db, installation_id, lifecycle_job.RECONCILE_JOB, payload.model_dump(mode="json"))
        await db.flush()
        job = await db.get(PlatformJob, running.job_id)
        assert job is not None
        job.max_attempts = 3
        job.attempt = job.max_attempts if race.endswith("_final") else 1

    reads = writes = 0
    superseded_after_closure = False

    @asynccontextmanager
    async def supersede_after_closure():
        nonlocal superseded_after_closure
        async with sessions.begin() as db:
            yield db
        if not superseded_after_closure:
            async with sessions.begin() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                assert row is not None
                if row.admitted_revision is None:
                    row.desired_revision = 2
                    superseded_after_closure = True

    if race == "repair_superseded_before_reads":
        monkeypatch.setattr(lifecycle_job, "get_db_context", supersede_after_closure)

    async def slow_provider(request):
        nonlocal reads, writes
        if request.url.path.endswith("/configurations"):
            # The provider waits for a separate projection transaction to finish.
            assert await cloudflare_connector_channel.project_connector(None) == (installation_id, 1, 1)
            assert token_file.read_bytes()
            if request.method == "PUT":
                writes += 1
            else:
                reads += 1
            async with sessions.begin() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                assert row is not None
                if race.startswith("repair") and reads >= 2:
                    assert row.admitted_revision is None
                    if race == "repair_superseded" or (
                        race == "repair_superseded_after_readback" and request.method == "GET" and writes == 1
                    ):
                        row.desired_revision = 2
                if race == "credential_replaced":
                    version = row.api_token_version + 1
                    db.add(seal_credential(
                        CredentialRef(row.id, row.account_id, "account_token", "", version),
                        SecretStr("synthetic-replacement-management-token"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
                    ))
                    row.api_token_version = version
                if race == "target_replaced":
                    item = await cloudflare_tunnel.connector_resource(db, row)
                    item.provider_version = "replacement-receipt"
                if race.endswith("admission_superseded"):
                    row.admitted_revision = None
                if race in (
                    "healthy_projection_changed", "disconnected_final", "credential_denied_final",
                    "inventory_unverified_final",
                ):
                    row.tunnel_token_projected_version = 0
            if race in ("credential_denied", "credential_denied_final", "credential_replaced"):
                return httpx.Response(403)
        return provider(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_provider)) as http:
        reconcile = lifecycle_job._settle_reconcile(running, payload, WorkersRestCleanup(http), tunnel_http=http)
        if code is not None:
            with pytest.raises(PlatformJobRequiresAction) as issue:
                await reconcile
            assert issue.value.result == {"code": code}
            if race == "disconnected_final":
                # Persist the actual terminal outcome consumed by the scheduler.
                now = datetime.now(UTC)
                async with sessions.begin() as db:
                    job = await db.get(PlatformJob, running.job_id)
                    assert job is not None
                    job.status, job.completed_at, job.result = "requires_action", now, issue.value.result
                async with sessions.begin() as db:
                    query = select(PlatformJob).where(
                        PlatformJob.resource_id == str(installation_id), PlatformJob.id != running.job_id,
                    )
                    await lifecycle_job.schedule_cloudflare_reconcile(db, now=now + timedelta(minutes=4))
                    assert await db.scalar(query) is None
                    await lifecycle_job.schedule_cloudflare_reconcile(db, now=now + timedelta(minutes=5))
                    followup = await db.scalar(query)
                    assert followup is not None and followup.status == "queued"
                    assert followup.payload["revision"] == 1 and followup.payload["operation"] == "reconcile"
        elif race in ("healthy_projection_changed", "projection_completed"):
            with pytest.raises(PlatformJobFailure) as failure:
                await reconcile
            assert failure.value.code == "CF_TUNNEL_VERIFICATION_CHANGED" and failure.value.retryable
        else:
            assert await reconcile == {"outcome": "superseded"}

    assert superseded_after_closure == (race == "repair_superseded_before_reads")
    assert reads == {
        "repair": 3, "repair_superseded": 2, "repair_superseded_after_readback": 3,
    }.get(race, 1)
    wrote_ingress = race in ("repair", "repair_superseded_after_readback")
    assert writes == int(wrote_ingress)
    if race.startswith("repair"):
        assert provider.config["warp-routing"] == {"enabled": not wrote_ingress}
    async with sessions() as db:
        row = await db.get(CloudflareInstallation, installation_id)
        assert row is not None
        still_admitted = race in (
            "target_replaced", "healthy_projection_changed", "projection_completed", "projection_completed_final",
            "credential_replaced",
        )
        assert row.admitted_revision == (1 if still_admitted else None)
        if race == "disconnected":
            assert row.deployment_evidence["connector_session"] == "disconnected"
            assert row.deployment_evidence["connector_observed_token_version"] == 1
        else:
            assert row.deployment_evidence == {"connector_session": "disconnected"}


@pytest.mark.asyncio
@pytest.mark.parametrize("deny_readback", [False, True])
async def test_rotation_evicts_only_old_sessions_when_a_new_token_projects_in_flight(
    managed_tunnel_for_projection, deny_readback: bool,
) -> None:
    sessions, token_file, installation_id, provider = managed_tunnel_for_projection
    actor = str(OPERATOR_ID)
    old, active, replacement = (str(uuid4()) for _ in range(3))
    provider.connectors = [old]

    async def rotate(version: int) -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
            async with sessions.begin() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                assert row is not None
                client = cloudflare_tunnel.adapter(http, row, SecretStr("synthetic-cloudflare-management-token"))
                await cloudflare_tunnel.stage_rotation(db, row, version=version, actor=actor)
                await cloudflare_tunnel.record_rotation_intent(db, row, client, version=version)
            # The fixture loses the provider's PATCH response after applying it. The replay adopts the token.
            with pytest.raises(CloudflareRestUnavailable):
                async with sessions.begin() as db:
                    row = await db.get(CloudflareInstallation, installation_id)
                    assert row is not None
                    await cloudflare_tunnel.rotate(db, row, client, version=version, actor=actor)
            async with sessions.begin() as db:
                row = await db.get(CloudflareInstallation, installation_id)
                assert row is not None
                await cloudflare_tunnel.rotate(db, row, client, version=version, actor=actor)

    await rotate(2)
    provider.connectors.append(active)
    assert await cloudflare_connector_channel.project_connector(None) == (installation_id, 2, 1)
    initial_token = token_file.read_bytes()
    payload = lifecycle_job.CloudflareRotateWorkerPayload(
        installation_id=installation_id, revision=1, credential="connector_token", replacement_version=2,
    )
    async with sessions.begin() as db:
        row = await db.get(CloudflareInstallation, installation_id)
        assert row is not None
        row.desired_state = "scaled_in"
        row.deployment_evidence = {"connector_session": "disconnected"}
        running = _running_tunnel_job(db, installation_id, lifecycle_job.ROTATE_WORKER_JOB, payload.model_dump(mode="json"))

    async def slow_provider(request):
        if request.url.path.endswith("/connections"):
            if request.method == "DELETE":
                assert await cloudflare_connector_channel.project_connector(None) == (installation_id, 2, 1)
                assert request.url.params["client_id"] == old
                await rotate(3)
                provider.connectors.append(replacement)
            elif provider.evicted:
                assert await cloudflare_connector_channel.project_connector(None) == (installation_id, 3, 1)
                assert token_file.read_bytes() != initial_token
                if deny_readback:
                    return httpx.Response(403)
        return provider(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_provider)) as http:
        assert await lifecycle_job._rotate_connector(running, payload, http) == {"outcome": "superseded"}
    assert provider.evicted == [old]
    assert provider.connectors == [active, replacement]
    async with sessions.begin() as db:
        row = await db.get(CloudflareInstallation, installation_id)
        assert row is not None and row.tunnel_token_projected_version == row.tunnel_token_version == 3
        item = await cloudflare_tunnel.connector_resource(db, row)
        assert item.provider_details["rotation_version"] == 3
        assert set(item.provider_details["rotation_previous_connectors"]) == {old, active}
        assert row.deployment_evidence == {"connector_session": "disconnected"}


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
    db_session.add(ManagedCloudflareScope(installation.id, "workers", 1).pin(
        worker.id, 1, reserved_at=datetime.now(UTC), lifetime=3600, deadline_wire=False,
    ))
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
    ingress_time = datetime(2026, 1, 1, tzinfo=UTC)
    canary_time = ingress_time + timedelta(seconds=1)
    good = ingress_probes.WorkerIngressProbeResult(
        installation.worker_ingress_origin, True, True, True, routes,
        runtime_canary=ingress_probes.RuntimeCanary(True, True, verified_at=canary_time),
        verified_at=ingress_time,
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
    evidence = await cloudflare_status.deployment_evidence(db_session, installation)
    assert (evidence.ingress_verified_at, evidence.signed_canary_at) == (ingress_time, canary_time)
    assert evidence.dns_verified_at is None
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
                installation_id=installation.id,
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
    now = datetime.now(UTC)
    app = app_journal(installation, "e" * 64, now)
    db_session.add(app)
    container_ref = CredentialRef(installation.id, installation.account_id, "scope_bearer", "containers", 1)
    db_session.add(seal_credential(container_ref, SecretStr("container-scope-bearer"), actor=str(OPERATOR_ID), now=now))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="containers", deployment_revision=1, bearer_version=1,
        endpoint="https://containers.example.test",
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
    await db_session.refresh(app)
    assert app.state == "present"
    assert await db_session.get(CloudflareCapacityScope, (installation.id, "containers", 1)) is not None


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
    release, _, _ = make_release()
    now = datetime.now(UTC)
    installation.desired_state = "enabled"
    installation.admitted_revision = installation.deployed_revision = 1
    installation.containers_ready_revision = 1
    installation.containers_release_digest = release.digest
    installation.containers_policy = container_policy().model_copy(update={"enabled": True}).model_dump(mode="json")
    application = app_journal(installation, release.digest, now)
    launcher = CloudflareOwnedResource(
        id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
        scope="containers", kind="worker_script", target_name=f"bifrost-{installation.id.hex}-r1-containers",
        desired_revision=1, artifact_digest=release.digest, effect_id=uuid4(),
        state="present", provider_id=str(uuid4()), provider_version=str(uuid4()),
        namespace_ids=application.namespace_ids, observed_at=now,
    )
    db_session.add_all([application, launcher, seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "containers", 1),
        SecretStr("synthetic-container-scope-bearer-key"), actor=str(OPERATOR_ID), now=now,
    )])
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="containers", endpoint="https://containers.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    await db_session.flush()
    job, reused = await lifecycle_job.enqueue_cloudflare_disable(
        db_session, admin, installation_id=installation.id, expected_revision=1, action="scale_in",
    )
    assert not reused and job.payload_version == 1
    assert installation.desired_state == "scaled_in" and installation.admitted_revision is None
    assert installation.desired_revision == job.payload["revision"] == 1
    with pytest.raises(CloudflarePolicyConflict, match="finish draining"):
        await enable_job.enqueue_cloudflare_enable(
            db_session, admin, installation_id=installation.id, expected_revision=1,
        )
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
    cleanup = AsyncMock(side_effect=AssertionError("Scale In must retain owned provider resources"))
    monkeypatch.setattr(lifecycle_job, "_cleanup_one", cleanup)
    payload = lifecycle_job.CloudflareDisablePayload.model_validate(job.payload)
    running = PlatformJobContext(job.id, token, None, str(OPERATOR_ID), admin.principal.email, "")
    assert await lifecycle_job.CLOUDFLARE_DISABLE_DEFINITION.handler(running, payload) == {"outcome": "scaled_in"}
    cleanup.assert_not_awaited()
    await db_session.refresh(installation)
    assert installation.api_token_version == 1
    assert installation.admitted_revision is None
    assert installation.deployed_revision == 1 and installation.containers_ready_revision is None
    assert installation.containers_release_digest is None
    for resource in (application, launcher):
        await db_session.refresh(resource)
        assert resource.state == "present"
    assert await db_session.get(CloudflareCapacityScope, (installation.id, "containers", 1)) is not None
    actions = set(await db_session.scalars(select(AuditLog.action).where(AuditLog.resource_id == installation.id)))
    assert {"capacity.cloudflare_scale_in", "capacity.cloudflare_scaled_in"} <= actions
    job.status = "succeeded"
    await db_session.flush()
    enabling, _ = await enable_job.enqueue_cloudflare_enable(
        db_session, admin, installation_id=installation.id, expected_revision=1,
    )
    assert enabling.payload["revision"] == 1 and installation.containers_ready_revision is None
    assert installation.desired_state == "enabled" and installation.admitted_revision is None
    await db_session.flush()
    assert await db_session.scalar(select(cloudflare_admission.joint_container_readiness(1)).where(
        CloudflareInstallation.id == installation.id,
    )) is False


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
@pytest.mark.parametrize("operation", ["disconnect", "upgrade", "reenable"])
async def test_joint_cleanup_waits_for_container_absence_before_deleting_workers(
    db_session, installation, monkeypatch, lifecycle_audit_actor, operation,
) -> None:
    from src.services.worker_pool.cloudflare_containers_rest import ContainersRest

    app = app_journal(installation, "c" * 64, datetime.now(UTC))
    app.namespace_ids = {"ContainerLauncher": app.provider_version}
    scripts = [
        CloudflareOwnedResource(
            id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
            scope=scope, kind="worker_script",
            target_name=f"{app.target_name}-{suffix}", desired_revision=1, artifact_digest="c" * 64,
            effect_id=uuid4(), state="present", provider_id=str(uuid4()), provider_version=str(uuid4()),
        )
        for scope, suffix in (("workers", "launcher"), ("containers", "containers"), ("workers", "capacity"))
    ]
    db_session.add_all([app, *scripts])
    installation.desired_revision = 2
    installation.desired_state = "disconnected" if operation == "disconnect" else "enabled"
    installation.upgrade_release = "d" * 64 if operation == "upgrade" else None
    installation.deployed_revision = installation.containers_ready_revision = 1
    installation.containers_release_digest = app.artifact_digest
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def transaction():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as db:
            yield db
            await db.commit()

    written = []
    present = {resource.target_name for resource in scripts}
    script_receipts = {
        resource.target_name: ResourceObservation(
            lifecycle_job._resource_intent(resource), resource.provider_id, resource.provider_version,
        ) for resource in scripts
    }
    listed = True
    by_id = True
    app_row = {
        "id": app.provider_id, "name": app.target_name, "account_id": app.account_id,
        "scheduling_policy": "durable_object", "durable_objects": {"namespace_id": app.provider_version},
        "health": {"instances": {"active": 0, "starting": 0}},
    }

    def remote(request):
        nonlocal listed
        path = request.url.path
        if request.method == "DELETE":
            assert path.endswith(f"/applications/{app.provider_id}") and listed
            written.append("application")
            listed = False
            return httpx.Response(204)
        if path.endswith("/applications"):
            result = [app_row] if listed else []
        elif path.endswith("/instances-v2"):
            assert request.url.params["state"] == "active"
            result = []
        else:
            assert path.endswith(f"/applications/{app.provider_id}")
            if not by_id:
                return httpx.Response(404)
            result = app_row
        return httpx.Response(200, json={"success": True, "result": result})

    async def observe(*, api_token, intent):
        return script_receipts[intent.target.name] if intent.target.name in present else None

    async def delete_script(*, api_token, expected):
        name = expected.intent.target.name
        present.remove(name)
        written.append(name.rsplit("-", 1)[1])

    cleanup = SimpleNamespace(observe=observe, delete=delete_script, verify_absent=AsyncMock())

    async def wait_for_readback(_delay):
        nonlocal by_id
        assert by_id and written == ["application"]
        await db_session.refresh(installation)
        assert installation.containers_ready_revision == 1
        by_id = False

    monkeypatch.setattr(lifecycle_job, "get_db_context", transaction)
    monkeypatch.setattr(enable_job, "get_db_context", transaction)
    for target in (lifecycle_job, enable_job):
        monkeypatch.setattr(target, "_hold_installation_lease", AsyncMock())
    monkeypatch.setattr(PlatformJobContext, "report", AsyncMock())
    monkeypatch.setattr(lifecycle_job, "asyncio", SimpleNamespace(sleep=wait_for_readback))
    monkeypatch.setattr(enable_job, "sleep", wait_for_readback)

    context = PlatformJobContext(uuid4(), uuid4(), None, str(OPERATOR_ID), "", "")
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        if operation == "disconnect":
            payload = lifecycle_job.CloudflareDisablePayload(installation_id=installation.id, revision=2, action="disconnect")
            assert await lifecycle_job._settle_disable(context, payload, cleanup, tunnel_http=http) == {"outcome": "disconnected"}
        else:
            payload = enable_job.CloudflareEnablePayload(installation_id=installation.id, revision=2)
            if operation == "reenable":
                with pytest.raises(PlatformJobFailure) as waiting:
                    await enable_job._drain_previous_revision(
                        context, payload, cleanup, upgrading=False, container_adapter=ContainersRest(http),
                    )
                assert waiting.value.code == "CF_CLEANUP_SETTLING" and waiting.value.retryable
                await wait_for_readback(0)
            assert await enable_job._drain_previous_revision(
                context, payload, cleanup, upgrading=operation == "upgrade", container_adapter=ContainersRest(http),
            )
    assert written == ["application", "capacity", "launcher", "containers"]
    for resource in [app, *scripts]:
        await db_session.refresh(resource)
        assert resource.state == "absent" and resource.observed_at is not None
    await db_session.refresh(installation)
    if operation == "disconnect":
        assert installation.retired_at is not None
        assert installation.containers_ready_revision is None and installation.containers_release_digest is None
    elif operation == "upgrade":
        assert installation.upgrade_drained_at is not None
    else:
        assert installation.deployed_revision is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", [False, True])
async def test_cleanup_confirms_container_create_absence(db_session, installation, ambiguous) -> None:
    from src.services.worker_pool.cloudflare_containers_rest import ContainersRest

    app = app_journal(installation, "c" * 64, datetime.now(UTC))
    app.namespace_ids = {"ContainerLauncher": app.provider_version}
    app.provider_id = app.provider_version = app.observed_at = None
    app.state = "creating"
    if ambiguous:
        app.observed_at = datetime.now(UTC) - lifecycle_job.CREATE_SETTLEMENT_WINDOW
        app.provider_details = {"creation_ambiguous": True}
    db_session.add(app)
    await db_session.flush()

    def remote(request):
        assert request.method == "GET" and request.url.path.endswith("/applications")
        return httpx.Response(200, json={"success": True, "result": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as http:
        adapter = ContainersRest(http)
        if ambiguous:
            assert await lifecycle_job._cleanup_one(db_session, installation, adapter, container_adapter=adapter) == "settling"
            assert app.state == "creating" and app.observed_at is not None
            app.observed_at -= lifecycle_job.CREATE_SETTLEMENT_WINDOW
            await db_session.flush()
        assert await lifecycle_job._cleanup_one(db_session, installation, adapter, container_adapter=adapter) == "progressed"
    assert app.state == "absent" and app.observed_at is not None


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


@pytest.fixture
async def isolated_capacity_installation(isolated_capacity_session_factory):
    # The supervisor plans the whole queue. Give it only this test's demand
    # and keep real test-stack executors from consuming the fixture.
    async with isolated_capacity_session_factory() as db:
        installation_id = await create_installation(db, uuid4().hex)
        installation = await db.get(CloudflareInstallation, installation_id)
        assert installation is not None
        yield db, installation


@pytest.mark.asyncio
@pytest.mark.parametrize("primary", ["static", "job", "external"])
async def test_supervisor_reserves_and_retires_without_restarting(
    isolated_capacity_installation, primary: str, monkeypatch,
) -> None:
    db_session, installation = isolated_capacity_installation
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    from src.services.worker_pool import cloudflare_timing

    for module in (controller, external_controller, cloudflare_supervisor, cloudflare_spend, cloudflare_timing):
        monkeypatch.setattr(module, "get_db_context", context)
    monkeypatch.setattr(controller, "_publish_live", AsyncMock(return_value=controller.LiveControllers(
        ids=frozenset(), versions=frozenset({"test"}),
    )))
    await KubernetesExecutionService(db_session).update_policy({"max_workers": 1, "warm_workers": 0}, updated_by="test")
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
    role = RoleGrant(uuid4(), frozenset({"platform.read", "executions.read.all"}), (Boundary(BoundaryKind.PLATFORM),))
    caller = Caller(
        UserPrincipal(OPERATOR_ID, "operator@example.test", None),
        AuthorizationContext(OPERATOR_ID, None, uuid4(), False, frozenset(), (role,)),
    )
    run = Execution(workflow_name="fixture", executed_by_name="fixture")
    db_session.add_all((primary_worker, run))
    await db_session.flush()
    dispatch = WorkDispatch(
        family="workflow", handler="workflow.execute", handler_version=1, subject_id=run.id,
        message={}, payload={}, cloudflare_revision="c" * 64, memory_request_bytes=64 * 2**20,
        created_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    db_session.add(dispatch)
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    await db_session.flush()

    resources = {}
    writes = []
    start_ages = []
    stop = asyncio.Event()

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
            if request.method == "POST":
                started_at = await db_session.scalar(select(func.clock_timestamp()))
                assert started_at is not None
                start_ages.append((started_at - dispatch.created_at).total_seconds())
                stop.set()
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
        if primary == "job":
            policy_service = KubernetesExecutionService(db_session)
            await policy_service.update_policy({"max_workers": 2}, updated_by="test")
            await db_session.flush()
            await sweep()
            assert not writes
            await policy_service.update_policy({"max_workers": 1}, updated_by="test")
        if primary in ("job", "external"):
            static_executor = ExecutionWorker(
                form="static", state="ready", heartbeat_at=datetime.now(UTC),
                supported_handlers=["workflow.execute"], memory_budget_bytes=128 * 2**20,
            )
            db_session.add(static_executor)
            await db_session.flush()
            for heartbeat_age in (timedelta(0), timedelta(minutes=5)):
                as_of = datetime.now(UTC)
                static_executor.heartbeat_at = as_of - heartbeat_age
                await db_session.flush()
                await sweep()
                assert not writes
                lanes = await cloudflare_status.overflow_status(
                    db_session, caller, installation,
                    CloudflareCapacityPolicy.model_validate(installation.policy["limits"]),
                    now=as_of, approvals=1, blockers=[],
                )
                assert lanes[0].primary_saturated is True
                assert lanes[0].primary_fit == 1
                assert [reason.code for reason in lanes[0].reasons] == ["primary_has_capacity"]
            static_executor.memory_reserved_bytes = 128 * 2**20
        elif primary == "static":
            await sweep()
            assert not writes
            primary_worker.memory_reserved_bytes = 128 * 2**20
        queued_at = await db_session.scalar(select(func.clock_timestamp()))
        assert queued_at is not None
        dispatch.created_at = queued_at
        await db_session.flush()
        if primary == "job":
            lanes = await cloudflare_status.overflow_status(
                db_session, caller, installation,
                CloudflareCapacityPolicy.model_validate(installation.policy["limits"]),
                now=queued_at, approvals=1, blockers=[],
            )
            reason, = lanes[0].reasons
            assert reason.code == "waiting_for_launch_gate"
            assert reason.next_eligible_at == queued_at + timedelta(seconds=2)
        # Exercise the real supervisor without Redis hints for each primary form.
        reconcile_scope = cloudflare_supervisor._reconcile_scope

        async def reconcile(binding, settings, _http, controllers, *, kubernetes):
            await reconcile_scope(binding, settings, http, controllers, kubernetes=kubernetes)

        with monkeypatch.context() as supervisor_stubs:
            supervisor_stubs.setattr(cloudflare_supervisor, "_reconcile_scope", reconcile)
            supervisor_stubs.setattr(cloudflare_supervisor, "listen_for_pool_wake", AsyncMock())
            supervisor_stubs.setattr(cloudflare_supervisor, "may_manage_jobs", AsyncMock(return_value=primary == "job"))
            supervisor_stubs.setattr(cloudflare_supervisor, "schedule_cloudflare_reconcile", AsyncMock())
            supervisor_stubs.setattr(cloudflare_supervisor, "prune_spend_receipt_keys", AsyncMock())
            supervisor_stubs.setattr(cloudflare_supervisor, "_withdraw_live", AsyncMock())
            await asyncio.wait_for(cloudflare_supervisor.cloudflare_supervisor_loop(
                stop, settings, lease=SimpleNamespace(holds_lease=lambda: True, seconds_remaining=lambda: 5),
            ), timeout=5)
        assert len(start_ages) == 1
        assert 2 <= start_ages[0] < 5
        assert writes == ["PUT", "POST"]
        owned = tuple(controllers.values())
        worker_id = UUID(next(iter(resources)))
        reservation = await db_session.get(CloudflareWorkerReservation, worker_id)
        assert reservation is not None
        await db_session.refresh(reservation)
        timing = reservation.launch_timing
        assert timing is not None
        assert dispatch.created_at + timedelta(seconds=2) <= datetime.fromisoformat(timing["controller_decision_at"])
        assert (
            timing["controller_decision_at"] <= timing["launcher_reserved_at"] <=
            timing["launcher_start_sent_at"] <= timing["launcher_start_accepted_at"]
        )
        if primary == "job":
            # An existing Workers launcher must leave the Job ceiling intact
            # unless the operator configured a shared ceiling.
            primary_worker.state = "retired"
            dispatch.cloudflare_revision = None  # Linux-only demand.
            spec = WorkerJobSpec(namespace="test", image="api:test", env_from=(), service_account="default")
            async with httpx.AsyncClient(base_url="https://kube.example.test") as kube:
                primary_controller = controller.WorkerPoolController(
                    KubernetesJobClient(settings, http_client=kube, namespace="test"), settings, spec,
                )
                policy = await controller.load_kubernetes_policy(db_session)
                for shared_cap in (1, None):
                    installation.policy = {
                        **installation.policy,
                        "limits": {**installation.policy["limits"], "shared_managed_worker_cap": shared_cap},
                    }
                    await db_session.flush()
                    launched = await primary_controller.launch_for_demand(policy)
                    assert len(launched) == (1 if shared_cap is None else 0)
                replacement = await db_session.get(ExecutionWorker, launched[0])
                assert replacement is not None and replacement.form == "job" and replacement.state == "reserved"
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


@pytest.mark.asyncio
@pytest.mark.parametrize(("shared_cap", "desired_state", "worker_state", "deleted", "expected_jobs", "scenario"), [
    (10, "enabled", "ready", False, 1, ""),
    (3, "enabled", "ready", False, 0, ""),
    (3, "enabled", "ready", False, 0, "settled_container"),
    (None, "enabled", "ready", False, 1, ""),
    (3, "scaled_in", "draining", False, 0, "job"),
    (3, "disconnected", "draining", False, 0, "job"),
    (3, "enabled", "draining", False, 0, "policy"),
    (3, "enabled", "draining", False, 0, "policy_stop_unconfirmed"),
    (3, "enabled", "draining", False, 0, "policy_idle"),
    (3, "enabled", "draining", False, 0, "policy_memory"),
    (3, "enabled", "draining", False, 0, "policy_claimless_idle"),
    (3, "scaled_in", "draining", False, 0, "lifetime"),
    (3, "disconnected", "retired", False, 0, "job"),
    (3, "disconnected", "retired", False, 0, "unrelated"),
    (3, "disconnected", "retired", False, 0, "action_mismatch"),
    (3, "enabled", "retired", False, 0, "upgrade"),
    (3, "enabled", "retired", False, 0, "upgrade_without_release"),
    (3, "disconnected", "retired", False, 0, "current_revision"),
    (3, "disconnected", "retired", False, 0, "partial"),
    (3, "disconnected", "retired", True, 1, ""),
    (10, "disconnected", "retired", False, 1, "job"),
])
async def test_cloudflare_pins_limit_primary_room_and_explain_cleanup(
    isolated_capacity_installation, monkeypatch, shared_cap, desired_state, worker_state, deleted, expected_jobs, scenario,
) -> None:
    db_session, installation = isolated_capacity_installation
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as db:
            yield db

    monkeypatch.setattr(controller, "get_db_context", context)
    monkeypatch.setattr(controller, "_publish_launchable", AsyncMock())
    installation.desired_state = desired_state
    installation.desired_revision = 1 if desired_state == "enabled" else 2
    installation.admitted_revision = 1 if desired_state == "enabled" else None
    installation.policy = {
        **installation.policy,
        "limits": {**installation.policy["limits"], "shared_managed_worker_cap": shared_cap},
    }
    now = datetime.now(UTC)
    policy_drains = {
        "policy": None, "policy_stop_unconfirmed": "cloudflare_stop_unconfirmed", "policy_idle": "idle",
        "policy_memory": None, "policy_claimless_idle": "idle",
    }
    reserved_only = scenario in ("policy_memory", "policy_claimless_idle")
    worker_pins = []
    for index in range(3):
        worker_id = uuid4()
        end_reason = None
        if worker_state == "draining":
            end_reason = "max_lifetime" if scenario == "lifetime" else "cloudflare_admission_closed"
        workers_lane = worker_state == "draining"
        settled = scenario == "settled_container" and index == 0
        state = "ready" if scenario in ("policy", "policy_memory") else worker_state
        if settled:
            state = "retired"
        if scenario in policy_drains:
            end_reason = policy_drains[scenario]
        db_session.add(ExecutionWorker(
            id=worker_id, form="external", state=state,
            capacity_scope="workers" if workers_lane else "containers",
            runtime_profile="cloudflare-python-v1" if workers_lane else "linux-process-v1",
            end_reason=end_reason, memory_reserved_bytes=1 if reserved_only else 0,
            capacity_resource_deleted_at=now if deleted else None,
            heartbeat_at=now, memory_budget_bytes=128 * 2**20,
        ))
        await db_session.flush()
        if workers_lane:
            scope = ManagedCloudflareScope(installation.id, "workers", 1)
            pin = scope.pin(worker_id, 1, reserved_at=now - timedelta(seconds=120), lifetime=3600, deadline_wire=False)
            worker_pins.append(pin)
            if scenario in policy_drains and not reserved_only:
                running = Execution(
                    id=uuid4(), workflow_name="fixture", executed_by_name="fixture",
                    status=ExecutionStatus.RUNNING, started_at=now - timedelta(seconds=60),
                )
                db_session.add_all([
                    running, ExecutionWorkerClaim(worker_id=worker_id, family=WAKE_WORKFLOW, subject_id=running.id),
                ])
        else:
            current = scenario == "current_revision" or (scenario == "partial" and index == 2)
            pin = CloudflareWorkerReservation(
                worker_id=worker_id, installation_id=installation.id, scope="containers",
                deployment_revision=installation.desired_revision if current else 1,
                control_root_version=installation.control_root_version,
                container_tenant=str(uuid4()), container_release="c" * 64,
                container_envelope={}, container_reserved_at=now,
                container_stopped_at=now if settled else None,
            )
        db_session.add(pin)
    await db_session.flush()

    settings = Settings(_env_file=None)
    spec = WorkerJobSpec(namespace="test", image="api:test", env_from=(), service_account="default")
    async with httpx.AsyncClient(base_url="https://kube.example.test", transport=httpx.MockTransport(Mock())) as http:
        primary = controller.WorkerPoolController(
            KubernetesJobClient(settings, http_client=http, namespace="test"), settings, spec,
        )
        policy = KubernetesPolicy(max_workers=3, warm_workers=1)
        launched = await primary.launch_for_demand(policy)
        assert len(launched) == expected_jobs
        if scenario == "settled_container":
            stopped = await db_session.scalar(select(ExecutionWorker).join(CloudflareWorkerReservation).where(
                CloudflareWorkerReservation.installation_id == installation.id,
                CloudflareWorkerReservation.container_stopped_at.is_not(None),
            ))
            assert stopped is not None
            stopped.capacity_resource_deleted_at = now
            await db_session.flush()
            launched = await primary.launch_for_demand(policy)
            assert len(launched) == 1
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.example.test")
    monkeypatch.setattr(cloudflare_status, "load_kubernetes_policy", AsyncMock(
        return_value=KubernetesPolicy(max_workers=3, warm_workers=1),
    ))
    if desired_state == "enabled":
        # Policy edits can close admission without starting a cleanup job.
        installation.admitted_revision = None
        installation.desired_revision += 1
        await db_session.flush()
    idle = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert not any(reason.code == "primary_limited_by_cloudflare_cleanup" for reason in idle.placement_reasons)
    assert not any(reason.code == "primary_limited_by_cloudflare_drain" for reason in idle.placement_reasons)
    if scenario in (
        "job", "lifetime", "unrelated", "action_mismatch", "upgrade", "upgrade_without_release",
        "current_revision", "partial",
    ):
        title = "Drain Fixture Workers" if worker_state == "draining" else "Disconnect Fixture Capacity"
        action = "scale_in" if desired_state == "scaled_in" else "disconnect"
        job_type = (
            "capacity.cloudflare_reconcile" if scenario in ("unrelated", "upgrade", "upgrade_without_release")
            else "capacity.cloudflare_disable"
        )
        payload = {"revision": installation.desired_revision, "action": action}
        if scenario == "unrelated":
            title = "Unrelated Fixture Reconcile"
        if scenario == "action_mismatch":
            payload["action"] = "scale_in"
        if scenario in ("upgrade", "upgrade_without_release"):
            title = "Upgrade Fixture Capacity"
            payload["operation"] = "upgrade"
            installation.upgrade_release = "fixture-release" if scenario == "upgrade" else None
        db_session.add(PlatformJob(
            job_type=job_type, title=title, status="requires_action",
            resource_type="cloudflare_installation", resource_id=str(installation.id),
            requested_by_user_id=str(OPERATOR_ID), requested_by_email="operator@example.test",
            requested_by_name="Fixture Operator", payload=payload,
        ))
    run = Execution(workflow_name="fixture", executed_by_name="fixture")
    db_session.add(run)
    await db_session.flush()
    db_session.add(WorkDispatch(
        family="workflow", handler="workflow.execute", handler_version=1, subject_id=run.id,
        message={}, payload={}, memory_request_bytes=64 * 2**20, created_at=now - timedelta(seconds=10),
    ))
    await db_session.flush()
    if scenario in policy_drains:
        @asynccontextmanager
        async def retirement_context():
            async with context() as db:
                yield db
                await db.commit()

        monkeypatch.setattr(external_controller, "get_db_context", retirement_context)
        profile = external_controller.WorkerCapacityProfile.model_validate({
            "protocol_version": 2, "scope": "workers", "worker_version": "test",
            "runtime_profile": "cloudflare-python-v1", "memory_limit_bytes": 128 * 2**20,
            "max_workers": 3, "max_lifetime_seconds": 3600,
        })
        async with httpx.AsyncClient(transport=httpx.MockTransport(Mock())) as http:
            client = external_controller.ExternalCapacityClient("https://capacity.example.test", "t" * 40, http=http)
            supervisor = external_controller.ExternalWorkerPoolController(client, settings, profile)
            supervisor.managed_cloudflare_scope = ManagedCloudflareScope(installation.id, "workers", 1)
            for pin in worker_pins:
                await supervisor._retire_closed_worker(pin.worker_id)
            before_expiry = await get_cloudflare_capacity_status(db_session, _platform_operator())
            assert any(reason.code == "primary_limited_by_cloudflare_drain" for reason in before_expiry.placement_reasons) == (
                scenario != "policy_claimless_idle"
            )
            for pin in worker_pins:
                pin.workers_deadline_at = now - timedelta(seconds=60)
                pin.workers_lifetime_seconds = 60
            await db_session.flush()
            for pin in worker_pins:
                await supervisor._retire_closed_worker(pin.worker_id)
                worker = await db_session.get(ExecutionWorker, pin.worker_id, populate_existing=True)
                assert worker is not None and worker.state == "draining"
                expected_reason = "idle" if scenario == "policy_claimless_idle" else "cloudflare_admission_closed"
                assert worker.end_reason == expected_reason

    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    cleanup_limit = next((reason for reason in status.placement_reasons
                          if reason.code == "primary_limited_by_cloudflare_cleanup"), None)
    assert (cleanup_limit is not None) == (shared_cap == 3 and worker_state == "retired" and not deleted)
    if cleanup_limit is not None:
        expected = {
            "job": "Disconnect Fixture Capacity", "upgrade": "Upgrade Fixture Capacity",
        }.get(scenario, "Verify deletion of the outstanding")
        assert expected in cleanup_limit.remedy
        assert "Unrelated Fixture Reconcile" not in cleanup_limit.remedy
    drain_limit = next((reason for reason in status.placement_reasons
                        if reason.code == "primary_limited_by_cloudflare_drain"), None)
    assert (drain_limit is not None) == (
        shared_cap == 3 and worker_state == "draining" and scenario not in ("lifetime", "policy_claimless_idle")
    )
    if drain_limit is not None:
        expected = "Drain Fixture Workers" if scenario == "job" else "Workers finish their current work"
        assert expected in drain_limit.remedy
        assert all(drain_limit in lane.reasons for lane in status.lanes)
        if desired_state == "disconnected":
            cleanup = next(reason for reason in status.placement_reasons if reason.code == "cleanup_pending")
            assert cleanup.label == "Cleanup Pending"
            assert all(cleanup in lane.reasons for lane in status.lanes)
    for worker_id in launched:
        worker = await db_session.get(ExecutionWorker, worker_id)
        assert worker is not None and worker.form == "job" and worker.state == "reserved"


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_pin", [False, True])
async def test_supervisor_retires_at_pinned_workers_lifetime(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, legacy_pin: bool,
) -> None:
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session
            await session.commit()

    monkeypatch.setattr(external_controller, "get_db_context", context)
    worker = ExecutionWorker(
        id=uuid4(), form="external", state="ready", capacity_scope="workers",
        runtime_profile="cloudflare-python-v1", memory_limit_bytes=256 * 2**20,
        memory_reserved_bytes=64 * 2**20,
    )
    db_session.add(worker)
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    scope = ManagedCloudflareScope(installation.id, "workers", 1)
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr("synthetic-lifetime-test-bearer"), actor=str(OPERATOR_ID), now=datetime.now(UTC),
    ))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    # Simulate a reservation made under a shorter policy before the current
    # 3600-second policy. Database time has passed only the pinned deadline.
    pinned = scope.pin(worker.id, 1, reserved_at=datetime.now(UTC) - timedelta(seconds=61),
                       lifetime=60, deadline_wire=False)
    if legacy_pin:
        pinned.workers_deadline_at = pinned.workers_lifetime_seconds = None
    db_session.add(pinned)
    await db_session.flush()
    assert await scope.lock_admission(db_session) == 1
    assert not await cloudflare_claim_allowed(db_session, worker.id)
    profile = external_controller.WorkerCapacityProfile.model_validate({
        "protocol_version": 2, "scope": "workers", "worker_version": "test",
        "runtime_profile": "cloudflare-python-v1", "memory_limit_bytes": 256 * 2**20,
        "max_workers": 1, "max_lifetime_seconds": 3600,
    })
    async with httpx.AsyncClient() as http:
        client = external_controller.ExternalCapacityClient("https://capacity.example.test", "t" * 40, http=http)
        supervisor = external_controller.ExternalWorkerPoolController(
            client, Settings(_env_file=None, secret_key="synthetic-lifetime-test-key-" + "x" * 32), profile,
        )
        supervisor.managed_cloudflare_scope = scope
        start = AsyncMock()
        record_timing = AsyncMock()
        monkeypatch.setattr(client, "start", start)
        monkeypatch.setattr(external_controller, "record_transport_timing", record_timing)
        await supervisor._start_prepared(worker.id, "never-started")
        start.assert_not_awaited()
        record_timing.assert_not_awaited()
        await supervisor._retire_closed_worker(worker.id)
        await db_session.refresh(worker)
        assert (worker.state, worker.end_reason) == ("draining", "max_lifetime")
        worker.memory_reserved_bytes = 0
        await db_session.flush()
        await supervisor._retire_closed_worker(worker.id)
        await db_session.refresh(worker)
        assert (worker.state, worker.end_reason) == ("retired", "max_lifetime")


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
    "cold-canary", "stopped-unconfirmed", "scheduler-refusal", "scheduler-claim",
])
async def test_container_reservation_rejects_unreviewed_source_and_scope(
    db_session, installation, reason, monkeypatch,
) -> None:
    from src.models.contracts.cloudflare_approval import SolutionApprovalScope
    from src.models.orm.cloudflare_approvals import CloudflareApproval
    from src.models.orm.execution_workers import MANAGED_WORKER_FORMS
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

    from src.services.worker_pool.cloudflare_containers_policy import container_envelope
    from src.services.worker_pool.sizing import LIVE_STATES
    from src.worker.remote.protocol import LINUX_RUNTIME_PROFILE

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
    primary_id = uuid4()
    db.add(ExecutionWorker(
        id=primary_id, form=primary.form, capacity_scope=primary.scope, state="ready", heartbeat_at=now,
        runtime_profile=LINUX_RUNTIME_PROFILE, supported_handlers=["workflow.execute"],
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
        from src.services.worker_pool.cloudflare_containers_egress import verify_container_egress
        from src.worker.remote.container_egress import CONTAINER_EGRESS_DENIALS
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
        assert canary.container_tenant == "qualification-canary"
        worker = await db.get(ExecutionWorker, canary.worker_id)
        assert worker is not None and worker.state == "starting" and worker.reserved_for_dispatch_id is None
        assert await reserve_container_canary(db, **canary_args, job_id=job_id, lease_token=lease_token) is None

        async def provider(request):
            assert request.url.path == "/v1/egress-probe"
            body = json.loads(request.content)
            assert body["pin"]["worker"] == str(worker.id)
            assert body["pin"]["startDeadline"] - body["pin"]["reservedAt"] <= 15_000
            nonce = body["nonce"]
            # Stand in for the signed supervisor and scoped ingress committing their separate witnesses.
            worker.remote_auth_nonce_digest = nonce_digest(nonce)
            worker.remote_owner_lease_expires_at = now + timedelta(minutes=1)
            worker.remote_session_generation = 1
            canary.canary_ack_at = canary.canary_sdk_at = now
            canary.canary_sdk_generation = 1
            assert canary.container_envelope is not None
            canary.container_envelope = {
                **canary.container_envelope, "denial_generation": 1, "egress_denials": sorted(CONTAINER_EGRESS_DENIALS),
            }
            await db.flush()
            return httpx.Response(200, json={
                "nonce": nonce, "worker_id": str(worker.id), "application_id": application.provider_id,
                "provider_version": application.provider_version, "origin": origin,
                "revision": 1, "release_digest": release.digest,
            })

        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
            admission["verified_egress"] = await verify_container_egress(
                db, http, installation_id=installation.id, revision=1, release_digest=release.digest,
                origin=origin, probe_worker_id=canary.worker_id,
            )
        assert admission["verified_egress"] is not None
        installation.admitted_revision = installation.containers_ready_revision = 1
        installation.containers_release_digest = release.digest
    elif reason in ("scheduler-refusal", "scheduler-claim"):
        from src.config import get_settings
        from src.services.worker_pool import claims, cloudflare_containers_control, cloudflare_supervisor
        from src.services.worker_pool.cloudflare_admission import ManagedCloudflareScope

        proof = admission["verified_egress"]
        installation.deployment_evidence = {"container_egress": proof.as_record()}
        image = f"registry.cloudflare.com/{installation.account_id}/{application.target_name}"
        db.add(CloudflareOwnedResource(
            id=uuid4(), installation_id=installation.id, account_id=installation.account_id,
            scope="containers", kind="container_image", target_name=application.target_name,
            desired_revision=1, artifact_digest=release.digest, effect_id=uuid4(), state="present",
            provider_id=f"{image}:release", provider_version=release.manifest.image_digest, observed_at=now,
        ))
        await db.flush()
        connection = await db.connection()

        @asynccontextmanager
        async def transaction():
            async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
                yield session
                await session.commit()

        for module in (cloudflare_supervisor, cloudflare_containers_control, claims):
            monkeypatch.setattr(module, "get_db_context", transaction)
        monkeypatch.setattr(cloudflare_supervisor, "_primary_pool", AsyncMock(return_value=(primary, None)))
        monkeypatch.setattr(cloudflare_supervisor, "record_controller_observation", AsyncMock())
        actor = "a" * 64
        requests = []
        refuse = reason == "scheduler-refusal"

        def placement_provider(request):
            if request.url.host == "capacity.example.test":
                assert len(request.headers["x-bifrost-incarnation-certificate"]) == 128
                payload = json.loads(request.content)
                if request.url.path == "/v1/start":
                    assert len(payload["control_key"]) == 64
                    requests.append(UUID(payload["pin"]["worker"]))
                    if not refuse:
                        return httpx.Response(200, json={"status": "pending", "instance": actor})
                    pin = payload["pin"]
                    return httpx.Response(200, json={
                        "status": "allocation-refused", "instance": actor, "uid": pin["uid"],
                        "startDeadline": pin["startDeadline"], "voidedAt": pin["reservedAt"],
                    })
                assert refuse and request.url.path == "/v1/stop" and "control_key" not in payload
                return httpx.Response(200, json={"status": "conflict", "instance": actor})
            assert request.url.host == "api.cloudflare.com"
            if request.url.path.endswith(f"/instances/{actor}"):
                return httpx.Response(404)
            assert request.url.path.endswith(f"/containers/applications/{application.provider_id}")
            return httpx.Response(200, json={"success": True, "result": {
                "id": application.provider_id, "account_id": installation.account_id,
                "name": application.target_name, "scheduling_policy": "durable_object",
                "durable_objects": {"namespace_id": application.provider_version},
            }})

        binding = ManagedCloudflareScope(installation.id, "containers", 1)
        async with httpx.AsyncClient(transport=httpx.MockTransport(placement_provider)) as http:
            await cloudflare_supervisor._reconcile_containers(
                binding, get_settings(), http, uuid4(), {binding: release}, kubernetes=False,
            )
        assert len(requests) == 1
        pin = await db.get(CloudflareWorkerReservation, requests[0])
        worker = await db.get(ExecutionWorker, requests[0])
        assert pin is not None and pin.container_envelope is not None and worker is not None
        if refuse:
            assert pin.container_envelope["allocation_refused"] and pin.container_stopped_at is not None
            assert worker.state == "retired" and worker.reserved_for_dispatch_id is None
            cooldown = await reserve_container(db, **admission)
            assert isinstance(cooldown, ContainerAdmissionRefused) and cooldown.code == "CF_CONTAINERS_COOLDOWN"
            claimant = primary_id
        else:
            assert worker.state == "starting" and worker.capacity_resource_uid is not None
            assert await claims.admit_external_worker(
                worker.id, resource_uid=worker.capacity_resource_uid, launcher_id=worker.id,
                version=worker.version, hostname="fixture", container_release=release.digest,
            ) == worker.id
            claimant = worker.id
        claim = await claims.claim_next(
            claimant, fits_bytes=512, budget_bytes=512, handlers=[("workflow.execute", 1)],
        )
        assert claim is not None and claim.subject_id == run_id and claim.workflow_slot is None
        assert await claims.claim_next(primary_id, fits_bytes=512, budget_bytes=512) is None
        assert await claims.claim_next(worker.id, fits_bytes=512, budget_bytes=512) is None
        if not refuse:
            worker.state = "draining"
            worker.active_runs = 1
            await db.flush()
            settlement = AsyncMock(return_value=True)
            monkeypatch.setattr(cloudflare_supervisor, "reconcile_container_stop", settlement)
            async with httpx.AsyncClient(transport=httpx.MockTransport(placement_provider)) as http:
                await cloudflare_supervisor._reconcile_containers(
                    binding, get_settings(), http, uuid4(), {binding: release}, kubernetes=False,
                )
                settlement.assert_not_awaited()
                await db.refresh(pin)
                assert pin.cleanup_started_at is not None
                worker.active_runs = 0
                execution.status = ExecutionStatus.RUNNING
                await db.flush()
                await cloudflare_supervisor._reconcile_containers(
                    binding, get_settings(), http, uuid4(), {binding: release}, kubernetes=False,
                )
                settlement.assert_not_awaited()
                execution.status = ExecutionStatus.SUCCESS
                await db.flush()
                await cloudflare_supervisor._reconcile_containers(
                    binding, get_settings(), http, uuid4(), {binding: release}, kubernetes=False,
                )
                assert settlement.await_count == 1
                assert settlement.await_args.args[-1] == worker.id
        return
    elif reason == "stale-hash":
        await db.execute(update(FileIndex).where(FileIndex.path == path).values(content_hash="b" * 64))
    elif reason == "missing-pin":
        await db.execute(delete(FileIndex).where(FileIndex.path == path))
    elif reason == "cross-org":
        execution.organization_id = other
    elif reason == "stale-egress":
        admission["verified_egress"] = await verified_egress(
            installation.id, 1, release.digest, origin, observed_at=now - timedelta(hours=2),
        )
    elif reason == "different-provider-version":
        application.provider_version = "f" * 32
    elif reason == "stopped-unconfirmed":
        live_managed = await db.scalar(select(func.count()).select_from(ExecutionWorker).where(
            ExecutionWorker.form.in_(MANAGED_WORKER_FORMS), ExecutionWorker.state.in_(LIVE_STATES),
        ))
        assert live_managed is not None
        installation.policy = {
            **installation.policy, "limits": {
                **installation.policy["limits"], "shared_managed_worker_cap": live_managed + 1,
            },
        }
        stopped = ExecutionWorker(id=uuid4(), form="external", state="retired", capacity_scope="containers")
        db.add(stopped)
        await db.flush()
        db.add(CloudflareWorkerReservation(
            worker_id=stopped.id, installation_id=installation.id, scope="containers",
            deployment_revision=1, control_root_version=installation.control_root_version,
            container_tenant="platform", container_release=release.digest,
            container_reserved_at=now - timedelta(seconds=1), container_stopped_at=now,
            container_envelope=container_envelope(container_policy(), release.manifest.rates),
        ))
        await db.flush()
        assert await reserve_container(db, **admission) is None
        stopped.capacity_resource_deleted_at = now
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
    elif reason in ("overdue-other-account", "cold-canary", "stopped-unconfirmed"):
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
async def test_capacity_reports_overdue_unsettled_containers(db_session, installation, monkeypatch) -> None:
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

    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_rest._PLACEMENT_READ_INTERVAL", 0)
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
    reserved_at = now - timedelta(hours=1)
    started_at = reserved_at - timedelta(seconds=2)
    stopped_at = started_at + timedelta(seconds=5)
    phase = "provisioning"
    placement_id = actor
    placement_app_id = app_id
    management_available = True

    def management(request: httpx.Request) -> httpx.Response:
        assert management_available, "Settlement replay must not contact management"
        assert request.method == "GET"
        if request.url.path.endswith(f"/instances/{actor}"):
            if placement_id is None:
                return httpx.Response(404)
            result = {
                "id": placement_id, "application_id": placement_app_id, "image": image, "name": "named-placement",
                "started_at": started_at.isoformat(),
                "status": {"state": phase, "updated_at": stopped_at.isoformat()},
            }
        else:
            assert request.url.path.endswith(f"/applications/{app_id}")
            result = {
                "id": app_id, "account_id": installation.account_id, "name": name,
                "scheduling_policy": "durable_object", "durable_objects": {"namespace_id": namespace},
            }
        return httpx.Response(200, json={"success": True, "result": result})

    closure = ClosedContainerStart(
        app, workers[0], uid, actor, image, reserved_at, started_at,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(management)) as http:
        adapter = ContainersRest(http)
        token = SecretStr("synthetic-management-token")
        with pytest.raises(ObservationPending):
            await settle_container_stop(db_session, adapter, token=token, closure=closure)
        phase = "inactive"
        placement_id = "e" * 64
        with pytest.raises(OwnershipConflict, match="differs from the exact actor or application"):
            await settle_container_stop(db_session, adapter, token=token, closure=closure)
        placement_id = None
        with pytest.raises(ObservationPending, match="Missing instance readback"):
            await settle_container_stop(db_session, adapter, token=token, closure=closure)
        placement_id = actor
        placement_app_id = "f" * 32
        with pytest.raises(OwnershipConflict, match="differs from the exact actor or application"):
            await settle_container_stop(db_session, adapter, token=token, closure=closure)
        placement_app_id = app_id
        started_at -= timedelta(seconds=2)
        with pytest.raises(ObservationPending, match="bounded stop"):
            await settle_container_stop(
                db_session, adapter, token=token, closure=replace(closure, placement_started_at=started_at),
            )
        started_at = closure.placement_started_at
        installation.desired_state = "disconnected"
        await db_session.flush()
        stopped_at = datetime.now(UTC) + timedelta(seconds=2)
        await settle_container_stop(db_session, adapter, token=token, closure=closure)
        management_available = False
        await settle_container_stop(db_session, adapter, token=token, closure=closure)
    settled = await db_session.get(CloudflareWorkerReservation, workers[0])
    assert settled is not None and settled.container_envelope is not None
    assert settled.container_stopped_at is not None and settled.container_stopped_at <= datetime.now(UTC)
    stopped_worker = await db_session.get(ExecutionWorker, workers[0])
    assert stopped_worker is not None and stopped_worker.ended_at == settled.container_stopped_at
    assert settled.container_envelope["fixed_usd"] == "0.01"
    status = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert status.installation is not None and status.installation.owned_resources_pending == 1


@pytest.mark.asyncio
async def test_container_source_authority_follows_the_durable_workflow_claim(db_session, installation) -> None:
    from shared.file_access import FileCaller, FileServiceError, reject_container_workspace_access
    from shared.sdk_files import sdk_read_file, sdk_write_file
    from shared.sdk_modules import ModuleSourceError, ModuleSourceScope
    from src.models.orm.cloudflare_approvals import CloudflareApproval
    from src.models.orm.execution_workers import ExecutionWorkerClaim
    from src.models.orm.workflows import Workflow
    from src.services.authorization.enforce import GLOBAL, authorize_operation
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
    for editor_operation in (
        "workspace.files.editor.read", "workspace.files.editor.write",
        "workspace.files.pull", "workspace.files.manifest",
    ):
        with pytest.raises(HTTPException, match="Workspace files are unavailable") as denied:
            await authorize_operation(db_session, principal, editor_operation, GLOBAL)
        assert denied.value.status_code == 403
    with pytest.raises(ModuleSourceError, match="authority has expired or changed"):
        await scoped_container_source(db_session, runtime, None, scope)
    wrong_subject = uuid4()
    db_session.add(ExecutionWorkerClaim(worker_id=worker, subject_id=wrong_subject, family=WAKE_WORKFLOW))
    await db_session.flush()
    wrong_runtime = SimpleNamespace(
        kind="workflow_execution", credential_id=wrong_subject, workflow_id=workflow_id, organization_id=None,
    )
    with pytest.raises(ModuleSourceError, match="authority has expired or changed"):
        await scoped_container_source(db_session, wrong_runtime, workflow, scope)
    principal.runtime_authority = wrong_runtime
    with pytest.raises(FileServiceError, match="Workspace files are unavailable"):
        await reject_container_workspace_access(db_session, principal)
    principal.runtime_authority = runtime
    approval.is_current = False
    await db_session.flush()
    with pytest.raises(ModuleSourceError, match="approval is unavailable"):
        await scoped_container_source(db_session, runtime, workflow, scope)
    await db_session.execute(delete(ExecutionWorkerClaim).where(ExecutionWorkerClaim.worker_id == worker))
    with pytest.raises(ModuleSourceError, match="authority has expired or changed"):
        await scoped_container_source(db_session, runtime, workflow, scope)
    primary = uuid4()
    db_session.add(ExecutionWorker(id=primary, form="external", state="ready"))
    await db_session.flush()
    db_session.add(ExecutionWorkerClaim(worker_id=primary, subject_id=subject, family=WAKE_WORKFLOW))
    await db_session.flush()
    assert await scoped_container_source(db_session, runtime, workflow, scope) == scope
    await reject_container_workspace_access(db_session, principal)


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
@pytest.mark.parametrize("closure_kind", ["void", "fenced-absent", "allocation-refused"])
async def test_disconnect_preserves_unresolved_container_cleanup(db_session, installation, monkeypatch, closure_kind) -> None:
    from src.services.worker_pool.cloudflare_containers_admission import (
        ContainerAdmissionRefused,
        _container_scope_cooldown,
        reserve_container,
    )
    from src.services.worker_pool.cloudflare_containers_policy import (
        container_account_usage,
    )
    from src.services.worker_pool.cloudflare_containers_rest import ContainersRest
    from src.services.worker_pool.cloudflare_containers_settlement import (
        AbsentContainerStart,
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

    monkeypatch.setattr("src.services.worker_pool.cloudflare_containers_rest._PLACEMENT_READ_INTERVAL", 0)
    now = datetime.now(UTC)
    worker, uid = uuid4(), uuid4().hex
    actor = "e" * 64
    release, _, _ = make_release()
    origin = installation.worker_ingress_origin
    installation.desired_state = "enabled"
    installation.admitted_revision = installation.containers_ready_revision = 1
    installation.containers_release_digest = release.digest
    installation.containers_policy = container_policy().model_copy(update={"enabled": True}).model_dump(mode="json")
    workers_release = "fixture-workers-release"
    monkeypatch.setattr(cloudflare_admission, "installed_workers_release_digest", lambda: workers_release)
    installation.policy = {
        **installation.policy, "bounds_release": workers_release,
        "limits": {**installation.policy["limits"], "monthly_limit_usd": "500"},
    }
    journal = app_journal(installation, release.digest, now)
    db_session.add(journal)
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "containers", 1),
        SecretStr("synthetic-container-scope-bearer-key"), actor=str(OPERATOR_ID), now=now,
    ))
    runner = ExecutionWorker(
        id=worker, form="external", state="lost", capacity_scope="containers",
        capacity_resource_uid=uid, capacity_resource_deleted_at=now,
    )
    subject, primary = uuid4(), uuid4()
    db_session.add_all([
        runner,
        ExecutionWorker(id=primary, form="static", state="ready", supported_handlers=["workflow.execute"]),
        Execution(id=subject, status=ExecutionStatus.PENDING, workflow_name="fixture", executed_by_name="fixture"),
    ])
    dispatch = stage_dispatch(
        db_session, family=WAKE_WORKFLOW, subject_id=subject, handler="workflow.execute", handler_version=1,
        payload={"execution_id": str(subject)}, message={}, memory_request_bytes=1, queued_at=now,
    )
    await db_session.flush()
    runner.reserved_for_dispatch_id = dispatch.id
    scope = CloudflareCapacityScope(
        installation_id=installation.id, scope="containers", endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    )
    db_session.add(scope)
    reserved_at = (now - timedelta(days=1)).replace(hour=12)
    deadline = reserved_at + timedelta(minutes=1)
    pin = CloudflareWorkerReservation(
        worker_id=worker, installation_id=installation.id, scope="containers", deployment_revision=1,
        control_root_version=1, container_tenant="platform", container_release=release.digest,
        container_reserved_at=reserved_at,
        container_envelope={
            "seconds": 300, "fixed_usd": "0.01", "second_usd": "0.001", "start_deadline": deadline.isoformat(),
            "execution_id": str(subject),
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
    fenced = closure_kind == "fenced-absent"
    refused = closure_kind == "allocation-refused"
    when = int(deadline.timestamp() * 1000)
    receipt = (
        AbsentContainerStart(app, worker, uid, actor, when, when - 1000, int((now - timedelta(seconds=1)).timestamp() * 1000))
        if fenced else VoidContainerStart(
            app, worker, uid, actor, when, when - 1000 if refused else when + 1000,
            allocation_refused=refused,
        )
    )
    present = True
    available = True

    def management(request):
        assert available
        if request.url.path.endswith(f"/instances/{actor}"):
            if not present:
                return httpx.Response(404)
            result = {"id": actor, "application_id": app.provider_id, "name": "named-placement"}
        else:
            assert request.url.path.endswith(f"/applications/{app.provider_id}")
            result = {
                "id": app.provider_id, "name": target.name, "account_id": target.account_id,
                "scheduling_policy": "durable_object", "durable_objects": {"namespace_id": app.provider_version},
            }
        return httpx.Response(200, json={"success": True, "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(management)) as http:
        rest = ContainersRest(http)
        token = SecretStr("synthetic-management-token")
        if fenced:
            early = AbsentContainerStart(app, worker, uid, actor, when, when - 1000, when + 59_999)
            with pytest.raises(ObservationPending, match="expired start deadline"):
                await settle_container_stop(db_session, rest, token=token, closure=early)
        with pytest.raises(ObservationPending, match="still has a provider placement"):
            await settle_container_stop(db_session, rest, token=token, closure=receipt)
        present = False
        await settle_container_stop(db_session, rest, token=token, closure=receipt)
        available = False
        await settle_container_stop(db_session, rest, token=token, closure=receipt)
    if refused:
        assert pin.container_stopped_at is not None
        fresh = SimpleNamespace(
            installation=installation, scope=scope, now=pin.container_stopped_at + timedelta(seconds=1),
        )
        cooldown = await _container_scope_cooldown(db_session, fresh)
        assert cooldown is not None and cooldown.code == "CF_CONTAINERS_COOLDOWN"
        assert cooldown.retry_at is not None and cooldown.retry_at > fresh.now
    assert not await cloudflare_workers_remain(db_session, installation.id)
    assert await db_session.scalar(select(container_account_usage(literal(deadline))).where(
        CloudflareInstallation.id == installation.id,
    )) == Decimal("0.31" if fenced else "0.01")
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    await db_session.flush()
    verdict = await reserve_container(db_session, **admission)
    if fenced:
        assert isinstance(verdict, ContainerAdmissionRefused)
        assert verdict.code == "CF_CONTAINERS_COOLDOWN" and verdict.retry_at is not None
        assert verdict.retry_at > now
        recovered = SimpleNamespace(installation=installation, scope=scope, now=verdict.retry_at)
        assert await _container_scope_cooldown(db_session, recovered) is None
        trial = ExecutionWorker(id=uuid4(), form="external", state="starting", capacity_scope="containers")
        trial_pin = CloudflareWorkerReservation(
            worker_id=trial.id, installation_id=installation.id, scope="containers", deployment_revision=1,
            control_root_version=1, container_tenant="platform", container_release=release.digest,
            container_reserved_at=verdict.retry_at, container_envelope={"seconds": 300},
        )
        db_session.add(trial)
        await db_session.flush()
        db_session.add(trial_pin)
        await db_session.flush()
        pending = await _container_scope_cooldown(db_session, recovered)
        assert pending is not None and pending.code == "CF_CONTAINERS_COOLDOWN" and pending.retry_at is None
        trial.remote_hello_sequence = 1
        await db_session.flush()
        assert await _container_scope_cooldown(db_session, recovered) is None
        another = ExecutionWorker(id=uuid4(), form="external", state="starting", capacity_scope="containers")
        another_pin = CloudflareWorkerReservation(
            worker_id=another.id, installation_id=installation.id, scope="containers", deployment_revision=1,
            control_root_version=1, container_tenant=str(uuid4()), container_release=release.digest,
            container_reserved_at=verdict.retry_at + timedelta(microseconds=1), container_envelope={"seconds": 300},
        )
        db_session.add(another)
        await db_session.flush()
        db_session.add(another_pin)
        await db_session.flush()
        assert await _container_scope_cooldown(db_session, recovered) is None
        trial_pin.container_stopped_at = verdict.retry_at + timedelta(seconds=1)
        trial.state = "retired"
        recovered.now = trial_pin.container_stopped_at
        await db_session.flush()
        pending = await _container_scope_cooldown(db_session, recovered)
        assert pending is not None and pending.code == "CF_CONTAINERS_COOLDOWN"
        await db_session.delete(another_pin)
        await db_session.delete(another)
        await db_session.delete(trial_pin)
        await db_session.delete(trial)
    else:
        assert not isinstance(verdict, ContainerAdmissionRefused)

    connection = await db_session.connection()

    @asynccontextmanager
    async def claim_transaction():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session

    assert runner.reserved_for_dispatch_id is None and dispatch.assigned_worker_id is None
    monkeypatch.setattr(claim_service, "get_db_context", claim_transaction)
    handed_back = await claim_next(primary, fits_bytes=1 << 20, budget_bytes=1 << 20)
    assert handed_back is not None and handed_back.subject_id == subject
    assert await claim_next(primary, fits_bytes=1 << 20, budget_bytes=1 << 20) is None
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

    async with isolated_test_database(async_engine.url) as engine:
        contexts = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
        with monkeypatch.context() as patch:
            patch.setattr(database, "_async_session_factory", contexts)
            patch.setattr(database, "_dispatch_session_factory", contexts)
            patch.setattr(database, "_liveness_session_factory", contexts)
            yield async_sessionmaker(engine, expire_on_commit=False)


@pytest.mark.asyncio
async def test_unadmitted_container_cannot_use_generic_linux_claims(isolated_capacity_session_factory) -> None:
    worker, older, bound = uuid4(), uuid4(), uuid4()
    now = datetime.now(UTC)
    async with isolated_capacity_session_factory() as db:
        runner = ExecutionWorker(
            id=worker, form="external", state="ready", capacity_scope="containers",
            supported_handlers=["workflow.execute"], memory_limit_bytes=1 << 20,
        )
        db.add(runner)
        for offset, subject in [(2, older), (1, bound)]:
            db.add(Execution(id=subject, status=ExecutionStatus.PENDING, workflow_name="fixture", executed_by_name="fixture"))
            dispatch = stage_dispatch(
                db, family=WAKE_WORKFLOW, subject_id=subject, handler="workflow.execute", handler_version=1,
                payload={"execution_id": str(subject)}, message={}, memory_request_bytes=1,
                queued_at=now - timedelta(seconds=offset),
            )
        await db.flush()
        runner.reserved_for_dispatch_id = dispatch.id
        await db.commit()
    assert await claim_next(worker, fits_bytes=1 << 20, budget_bytes=1 << 20) is None


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
            db.add(ManagedCloudflareScope(installation_id, "workers", 1).pin(
                warmer, 1, reserved_at=datetime.now(UTC), lifetime=3600, deadline_wire=False,
            ))
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
        monkeypatch.setattr(lifecycle_job, "installed_workers_release_digest", lambda digest=digest: digest)
        monkeypatch.setattr(lifecycle_job, "installed_workers_release_order", lambda order=order: order)
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
            workers_lifetime_seconds=3600, workers_deadline_at=datetime.now(UTC) + timedelta(hours=1),
        ) for worker in workers)
        for index, (execution, tenant) in enumerate(zip(runs, [None] * 5 + tenants, strict=True)):
            db.add(Execution(id=execution, organization_id=tenant, workflow_name="slot", executed_by_name="fixture"))
            stage_dispatch(
                db, family=WAKE_WORKFLOW, subject_id=execution, handler="workflow.execute", handler_version=1,
                payload={"execution_id": str(execution)},
                message={"dispatch_metadata": {"timeout_seconds": 1800}}, memory_request_bytes=1,
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
        for index, ticket in enumerate((second, third), start=1):
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
    route_id = first.workflow_route(runs[0])
    sent_at = now.isoformat()
    entry_ms = int(now.timestamp() * 1000)
    take_timing = Mock(side_effect=[{}, {"dispatch_sent_at": now}, {}])
    pool = ThinRemoteProcessPool(
        SimpleNamespace(managed_cloudflare=True, take_workflow_timing=take_timing), worker_id=str(workers[0]),
        runtime_profile="cloudflare-python-v1", on_result=callback,
    )
    with monkeypatch.context() as patch:
        patch.setattr(pool, "_owns_session", AsyncMock(return_value=True))
        terminal = {
            "event": "result", "_event_id": str(uuid4()), "runtime_handle": "confirmed-stop",
            "route_id": route_id, "attempt_state": "stopped",
            "result": {"success": False, "error_type": "CancelledError"},
            "timings": {"entry_at": entry_ms},
        }
        # Even a matching route cannot contribute telemetry until the send fence is durable.
        await pool.handle_event({**terminal, "attempt_state": "uncertain"})
        async with isolated_capacity_session_factory() as db:
            unseen = await db.scalar(select(Execution.cloudflare_timing).where(Execution.id == runs[0]))
            assert "entry_at" not in (unseen or {})
            assert await fence_workflow_slot_send(
                db, workers[0], runs[0], first, tenant_id=None, preparation=False,
            )
            await db.commit()
        await pool.handle_event(terminal)
        await pool.handle_event({**terminal, "timings": {"entry_at": entry_ms + 1}})
    callback.assert_not_awaited()
    async with isolated_capacity_session_factory() as db:
        timing = await db.scalar(select(Execution.cloudflare_timing).where(Execution.id == runs[0]))
        assert timing is not None
        assert timing["dispatch_sent_at"] == sent_at
        assert datetime.fromisoformat(timing["entry_at"]) == datetime.fromtimestamp(entry_ms / 1000, UTC)
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
    scope = ManagedCloudflareScope(installation.id, "workers", 1)
    db_session.add(scope.pin(workers[0], 1, reserved_at=now, lifetime=3600, deadline_wire=False))
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
            payload={"execution_id": str(execution)},
            message={"dispatch_metadata": {"timeout_seconds": 1800}}, memory_request_bytes=1,
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
            workers_lifetime_seconds=3600, workers_deadline_at=datetime.now(UTC) + timedelta(hours=1),
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
                payload={"execution_id": str(execution)},
                message={"dispatch_metadata": {"timeout_seconds": 1800}}, memory_request_bytes=1,
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
        assert first.cloudflare_claim_committed_at is not None
        assert first.cloudflare_claim_committed_at >= first.queued_at
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
    "complete", "reloaded", "over_cpu", "missing_timing", "missing", "uncertain", "stopped",
    "unbooked", "historical", "incomplete", "invalid", "regressing", "drift",
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
        meter_uncertain=evidence == "historical",
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
    attempt_route = f"{evidence}-route" if evidence in ("uncertain", "stopped", "unbooked") else None
    if evidence in ("uncertain", "stopped"):
        assert await reserve_tenant_host_usage(
            db_session, worker_id, noon + timedelta(seconds=10), workflow_cpu_ms=20_000, subrequests=10,
            tenant_id=tenant_id, route_id=attempt_route,
        )
    event = {"sqlite_rows_written": 12, "do_requests": 3, "log_events": 1}
    booking = await db_session.get(cloudflare_spend.CloudflareDispatchMeter, (worker_id, "fixture"))
    assert booking is not None and booking.workflow_cpu_ms == 20_000
    await cloudflare_spend.record_launcher_terminal_meter(
        db_session, worker_id, {
            **event, "sqlite_rows_written": {"regressing": 13, "invalid": -1}.get(evidence, 12),
            "do_requests": 0 if evidence == "drift" else event["do_requests"],
        },
        uncertain_route=attempt_route,
    )
    calls = [
        {"identity": identity, "kind": "boot", "cold": True, "started_at": now},
        {"identity": identity, "kind": "workflow", "cold": evidence == "reloaded", "started_at": now,
         "wall_ms": {"over_cpu": 30_000, "missing_timing": None}.get(evidence, 12_000)},
    ]
    routes = {} if evidence == "incomplete" else {"fixture": calls}
    if attempt_route is not None and evidence != "unbooked":
        routes[attempt_route] = calls
    price = select(cloudflare_estimated_usage(now)).where(CloudflareInstallation.id == installation.id)
    payload = CloudflareLauncherUsage.model_validate({
        **event, "do_requests": 0 if evidence == "drift" else event["do_requests"],
        "routes": routes,
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
    if evidence in ("complete", "reloaded", "over_cpu", "missing_timing", "uncertain", "stopped"):
        extra_load_us = 500_000 if evidence == "reloaded" else 0
        workflow_release_us = 8_000_000 if evidence in ("complete", "reloaded", "uncertain", "stopped") else 0
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
    if evidence in ("unbooked", "historical"):
        assert row.meter_uncertain
        assert row.meter_final is not None and row.meter_final["settled"]
        assert row.meter_final["reconciliation_drift"] is False
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
        db_session, worker_id, {**event, "sqlite_database_bytes": 2_000_000},
    )
    await cloudflare_spend.record_launcher_terminal_meter(db_session, worker_id, event)
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
async def test_workers_claim_uses_remaining_lifetime_and_leaves_unbounded_work_on_primary(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch,
) -> None:
    from src.models.orm.workflows import Workflow

    now = await db_session.scalar(select(func.clock_timestamp()))
    assert now is not None
    scope = ManagedCloudflareScope(installation.id, "workers", 1)
    installation.desired_state = "enabled"
    installation.admitted_revision = 1
    fresh, old, primary = (uuid4() for _ in range(3))
    budget = 128 * 2**20
    db_session.add_all(ExecutionWorker(
        id=worker, form="external", state="ready", capacity_scope=scope.name,
        runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, memory_limit_bytes=budget,
    ) for worker in (fresh, old))
    db_session.add(ExecutionWorker(id=primary, form="static", state="ready"))
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", scope.name, 1),
        SecretStr("synthetic-scope-bearer"), actor=str(OPERATOR_ID), now=now,
    ))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope=scope.name, endpoint="https://capacity.example.test",
        deployment_revision=1, bearer_version=1,
    ))
    db_session.add(scope.pin(fresh, 1, reserved_at=now, lifetime=3600, deadline_wire=True))
    db_session.add(scope.pin(old, 1, reserved_at=now - timedelta(seconds=2200), lifetime=3600, deadline_wire=True))
    workflow = Workflow(id=uuid4(), name="bounded fixture", function_name="run", path="workflows/bounded.py",
                        timeout_seconds=1800)
    db_session.add(workflow)
    await db_session.flush()
    unbounded, default = uuid4(), uuid4()
    for subject, timeout in ((unbounded, 0), (default, 1800)):
        db_session.add(Execution(id=subject, workflow_id=workflow.id, workflow_name="bounded fixture", executed_by_name="fixture"))
        stage_dispatch(
            db_session, family=WAKE_WORKFLOW, subject_id=subject, handler="workflow.execute", handler_version=1,
            payload={"execution_id": str(subject)}, message={"dispatch_metadata": {"timeout_seconds": 0}} if timeout == 0 else {},
            memory_request_bytes=1, cloudflare_revision="a" * 64,
            queued_at=now - timedelta(seconds=1 if timeout == 0 else 0),
        )
    await db_session.flush()
    connection = await db_session.connection()

    @asynccontextmanager
    async def context():
        async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
            yield session

    monkeypatch.setattr(claim_service, "get_db_context", context)
    assert await claim_next(old, fits_bytes=budget, budget_bytes=budget) is None
    fit = await claim_next(fresh, fits_bytes=budget, budget_bytes=budget)
    assert fit is not None and fit.subject_id == default and fit.workflow_slot is not None
    assert await claim_next(fresh, fits_bytes=budget, budget_bytes=budget) is None
    linux = await claim_next(primary, fits_bytes=budget, budget_bytes=budget)
    assert linux is not None and linux.subject_id == unbounded and linux.workflow_slot is None


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
    pin = CloudflareWorkerReservation(
        worker_id=worker_id, installation_id=installation.id, scope="workers", deployment_revision=1,
        control_root_version=1,
    )
    db_session.add(pin)
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
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    pin.workers_lifetime_seconds = 3600
    database_now = await db_session.scalar(select(func.clock_timestamp()))
    assert database_now is not None
    pin.workers_deadline_at = database_now + timedelta(seconds=2)
    await db_session.flush()
    assert not await cloudflare_claim_allowed(db_session, worker_id)
    pin.workers_deadline_at = database_now + timedelta(hours=1)
    await db_session.flush()
    assert await cloudflare_claim_allowed(db_session, worker_id)
    began = await cloudflare_dispatch_started_at(db_session, worker_id, timeout_seconds=1800)
    assert began is not None and database_now <= began <= await db_session.scalar(select(func.clock_timestamp()))
    assert await cloudflare_dispatch_started_at(db_session, worker_id, timeout_seconds=0) is None
    pin.workers_deadline_at = database_now + timedelta(seconds=1500)
    await db_session.flush()
    assert await cloudflare_dispatch_started_at(db_session, worker_id, timeout_seconds=1700) is not None
    assert await cloudflare_dispatch_started_at(db_session, worker_id, timeout_seconds=1800) is None
    pin.workers_deadline_at = database_now + timedelta(hours=1)
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


@pytest.mark.asyncio
async def test_preflight_and_activation_share_one_enable_job(db_session, monkeypatch, lifecycle_audit_actor):
    from src.services.worker_pool import cloudflare_activation

    release = _release(lifetime_meters=True)
    monkeypatch.setattr(cloudflare_activation, "load_workers_release", lambda: release)
    monkeypatch.setattr(cloudflare_policy, "load_workers_release", lambda: release)
    monkeypatch.setattr(cloudflare_activation, "connector_channel_ready", AsyncMock(return_value=False))
    cloudflare_policy._qualified_workers_suggestion.cache_clear()
    caller = _platform_operator()
    monkeypatch.setattr(capacity_routes, "load_caller", AsyncMock(return_value=caller))
    limit_preflight = AsyncMock()
    monkeypatch.setattr(capacity_routes.preflight_limiter, "check", limit_preflight)
    account_id, token = "a" * 32, "synthetic-management-token"
    reads = []

    def respond(request):
        assert request.method == "GET"
        assert request.headers["Authorization"] == f"Bearer {token}"
        reads.append(request.url.path)
        if request.url.path.endswith("/entitlements"):
            result = [
                {"id": feature, "deleted_date": "", "allocation": {"type": "bool", "value": True}}
                for feature in ("workers.enabled", "containers.enabled")
            ]
        elif request.url.path.endswith(f"/accounts/{account_id}"):
            result = {"id": account_id}
        elif request.url.path.endswith("/accounts"):
            result = [{"id": account_id, "name": "Example Account"}]
        elif request.url.path.endswith("/zones"):
            result = []
        else:
            raise AssertionError(request.url.path)
        return httpx.Response(200, json={"success": True, "result": result})

    monkeypatch.setattr(cloudflare_activation, "httpx", SimpleNamespace(
        AsyncClient=lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(respond), **kw),
    ))
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(capacity_routes.router)
    app.dependency_overrides[get_db] = lambda: db_session
    app.dependency_overrides[get_current_active_user] = lambda: caller.principal
    base = "/api/capacity/cloudflare"
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
            staged = await http.post(f"{base}/credential", json={
                "purpose": "connect", "account_id": account_id, "api_token": token,
            })
            assert staged.status_code == 201, staged.text
            await db_session.flush()
            ref = staged.json()["credential_ref"]
            preview = await http.post(f"{base}/preflight", json={
                "credential_ref": ref, "worker_ingress_origin": "https://workers.example.test",
            })
            assert preview.status_code == 200, preview.text
            limit_preflight.assert_awaited_once_with("capacity.cloudflare_preflight", str(caller.principal.user_id))
            assert token not in preview.text and ref not in preview.text
            plan = preview.json()
            assert datetime.fromisoformat(plan["expires_at"]) == datetime.fromisoformat(staged.json()["expires_at"])
            assert plan["preview"]["fleet_envelope_usd"] == "32.13"
            assert await db_session.scalar(select(CloudflareInstallation.id)) is None
            activation_body = {
                "credential_ref": ref, "preflight_receipt": plan["preflight_receipt"],
                "monthly_limit_usd": "75.00", "consent_version": plan["consent_version"], "consent": True,
            }
            saved_reference = await db_session.get(CloudflareStagedToken, UUID(ref))
            assert saved_reference is not None
            expiry = saved_reference.expires_at
            saved_reference.expires_at = datetime.now(UTC) - timedelta(seconds=1)
            await db_session.flush()
            expired = await http.post(f"{base}/activate", json=activation_body)
            assert expired.status_code == 409, expired.text
            assert expired.json()["details"]["code"] == "CF_STAGED_CREDENTIAL_UNAVAILABLE"
            assert "Stage the Cloudflare credential again" in expired.json()["details"]["remedy"]
            saved_reference.expires_at = expiry
            await db_session.flush()
            under_budget = await http.post(f"{base}/activate", json={**activation_body, "monthly_limit_usd": "32.12"})
            assert under_budget.status_code == 422, under_budget.text
            assert set(under_budget.json()["details"]["fields"]) == {"monthly_limit_usd"}
            accepted = await http.post(f"{base}/activate", json=activation_body)
            assert accepted.status_code == 202, accepted.text
            identity = accepted.json()
            assert not identity["reused"]
            assert accepted.headers["Location"] == f"/api/platform-jobs/{identity['job_id']}"
            assert reads == [
                "/client/v4/accounts", f"/client/v4/accounts/{account_id}",
                f"/client/v4/accounts/{account_id}/entitlements", "/client/v4/zones",
                f"/client/v4/accounts/{account_id}", f"/client/v4/accounts/{account_id}/entitlements",
            ]
            replay = await http.post(f"{base}/activate", json=activation_body)
            assert replay.status_code == 202, replay.text
            assert replay.json() == {**identity, "reused": True}
            changed = await http.post(f"{base}/activate", json={**activation_body, "monthly_limit_usd": "76.00"})
            assert changed.status_code == 409, changed.text
            assert changed.json()["details"]["code"] == "CF_REVIEW_REQUIRED"
            saved = await db_session.scalar(select(CloudflareInstallation))
            assert saved is not None
            first = await db_session.get(PlatformJob, UUID(identity["job_id"]))
            first.status = "failed"
            await db_session.flush()
            enable = await http.post(f"{base}/enable", json={
                "installation_id": str(saved.id), "expected_revision": 1,
            })
            assert enable.status_code == 202, enable.text
            second = enable.json()
            assert second["job_id"] != identity["job_id"]
            resumed = await http.post(f"{base}/activate", json=activation_body)
            assert resumed.json() == {**second, "reused": True}
            second_job = await db_session.get(PlatformJob, UUID(second["job_id"]))
            second_job.status = "failed"
            await db_session.flush()
            restarted = await http.post(f"{base}/activate", json=activation_body)
            assert restarted.status_code == 202, restarted.text
            assert restarted.json()["job_id"] not in {identity["job_id"], second["job_id"]}
            assert not restarted.json()["reused"]
        assert saved.desired_state == "enabled" and saved.desired_revision == 1
        assert saved.policy["limits"]["monthly_limit_usd"] == "75.00"
        assert await db_session.scalar(select(func.count()).select_from(PlatformJob).where(
            PlatformJob.job_type == "capacity.cloudflare_enable", PlatformJob.resource_id == str(saved.id),
        )) == 3
    finally:
        cloudflare_policy._qualified_workers_suggestion.cache_clear()


@pytest.mark.asyncio
@pytest.mark.parametrize("same_operator", [True, False], ids=["same-receipt", "conflicting-operator"])
async def test_concurrent_cloudflare_activation_keeps_one_installation_and_enable_job(
    isolated_capacity_session_factory, monkeypatch, same_operator,
):
    from shared.models import CloudflareActivateRequest, CloudflarePreflightRequest
    from src.services.worker_pool import cloudflare_activation
    from src.services.worker_pool.cloudflare_policy import CloudflareInstallationExists

    sessions = isolated_capacity_session_factory
    release = _release(lifetime_meters=True)
    monkeypatch.setattr(cloudflare_activation, "load_workers_release", lambda: release)
    monkeypatch.setattr(cloudflare_policy, "load_workers_release", lambda: release)
    monkeypatch.setattr(cloudflare_activation, "connector_channel_ready", AsyncMock(return_value=False))
    account = "a" * 32
    monkeypatch.setattr(cloudflare_activation, "WorkersRestCleanup", lambda _: SimpleNamespace(
        discovery_inventory=AsyncMock(side_effect=[[{"id": account, "name": "Account"}], []]),
        verify_workers_paid=AsyncMock(),
    ))
    first = _platform_operator()
    second = first if same_operator else _platform_operator(uuid4())
    preflight = CloudflarePreflightRequest(
        api_token="synthetic-management-token", worker_ingress_origin="https://workers.example.test",
    )

    async def review(caller):
        async with sessions() as db:
            plan = await cloudflare_activation.preflight_cloudflare(db, caller, preflight)
        return CloudflareActivateRequest(
            api_token=preflight.api_token, preflight_receipt=plan.preflight_receipt,
            monthly_limit_usd="75", consent=True, consent_version=plan.consent_version,
        )

    before_insert = asyncio.Barrier(2)
    create = cloudflare_activation.create_cloudflare_installation

    async def rendezvous(*args, **kwargs):
        await before_insert.wait()
        return await create(*args, **kwargs)

    monkeypatch.setattr(cloudflare_activation, "create_cloudflare_installation", rendezvous)

    async def activate(caller, submission):
        async with sessions.begin() as db:
            job, reused = await cloudflare_activation.activate_cloudflare(db, caller, submission)
            return job.id, reused

    cloudflare_policy._qualified_workers_suggestion.cache_clear()
    try:
        first_review = await review(first)
        second_review = first_review if same_operator else await review(second)
        outcomes = await asyncio.gather(
            activate(first, first_review), activate(second, second_review), return_exceptions=True,
        )
        accepted = [outcome for outcome in outcomes if isinstance(outcome, tuple)]
        assert sum(not reused for _, reused in accepted) == 1
        if same_operator:
            assert len(accepted) == 2 and accepted[0][0] == accepted[1][0]
        else:
            assert len(accepted) == 1
            assert any(isinstance(outcome, CloudflareInstallationExists) for outcome in outcomes)
        async with sessions() as db:
            assert await db.scalar(select(func.count()).select_from(CloudflareInstallation)) == 1
            assert await db.scalar(select(func.count()).select_from(PlatformJob)) == 1
    finally:
        cloudflare_policy._qualified_workers_suggestion.cache_clear()


@pytest.mark.asyncio
async def test_preset_b_policy_api_uses_the_qualified_fleet_price(
    db_session: AsyncSession, installation: CloudflareInstallation, monkeypatch, lifecycle_audit_actor,
) -> None:
    release = _release(lifetime_meters=True)
    installation.workers_release_digest = release.digest
    await db_session.flush()
    monkeypatch.setattr(cloudflare_connection, "load_workers_release", lambda: release)
    monkeypatch.setattr(capacity_routes, "load_caller", AsyncMock(return_value=_platform_operator()))
    workers = CloudflareWorkersPolicy(
        launcher_cap=3, slots_per_launcher=4, installation_slots=9, max_lifetime_seconds=3600,
        work_kinds=frozenset({"workflow"}),
    )

    async def save(limit: str, revision: int):
        request = CloudflarePolicyUpdate(
            action="policy", installation_id=installation.id, expected_revision=revision,
            policy=CloudflareCapacityPolicy(workers=workers, monthly_limit_usd=Decimal(limit)),
        )
        return await capacity_routes.configure_cloudflare_capacity(request, db_session, object())

    with pytest.raises(StructuredRequestRefusal) as refusal:
        await save("32.12", 1)
    assert refusal.value.details["fleet_envelope_usd"] == "32.13"
    for limit, revision in (("32.13", 1), ("75.00", 2)):
        status = await save(limit, revision)
        assert status.installation is not None
        assert status.installation.policy.monthly_limit_usd == Decimal(limit)
        assert status.installation.fleet_envelope_usd == Decimal("32.13")


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [
    "setup_required", "workflow_review_required", "waiting_for_demand", "primary_has_capacity",
    "primary_capacity_unverified", "waiting_for_launch_gate", "tenant_limit_reached", "installation_slots_full",
    "spend_guard_closed", "cleanup_pending", "cloudflare_capacity_unavailable", "release_mismatch",
    "connector_unavailable", "eligible_for_overflow", "measured_primary_budget",
])
async def test_status_explains_placement_over_rest(
    isolated_capacity_installation, monkeypatch, code,
):
    db_session, installation = isolated_capacity_installation
    role = RoleGrant(uuid4(), frozenset({"platform.read", "executions.read.all"}), (Boundary(BoundaryKind.PLATFORM),))
    caller = Caller(
        UserPrincipal(OPERATOR_ID, "operator@example.test", None),
        AuthorizationContext(OPERATOR_ID, None, uuid4(), False, frozenset(), (role,)),
    )
    monkeypatch.setattr(capacity_routes, "load_caller", AsyncMock(return_value=caller))
    monkeypatch.setattr(cloudflare_approval, "current_visible_approval_count", AsyncMock(
        return_value=0 if code == "workflow_review_required" else 1,
    ))
    monkeypatch.setattr(cloudflare_policy, "installed_workers_release_digest", lambda: (
        None if code == "release_mismatch" else installation.policy["bounds_release"]
    ))
    as_of = await db_session.scalar(select(func.statement_timestamp()))
    assert as_of is not None
    primary = None if code == "primary_capacity_unverified" else PrimaryPool("external", "gap5-fixture", 1)
    if code == "measured_primary_budget":
        primary = PrimaryPool("job", ceiling=2)
    cooldown = as_of + timedelta(seconds=30) if code == "cloudflare_capacity_unavailable" else None
    monkeypatch.setattr(cloudflare_status, "_controller_observation", AsyncMock(return_value=(primary, cooldown, None)))
    if code != "setup_required":
        installation.desired_state = "enabled"
        installation.admitted_revision = installation.desired_revision
    if code == "spend_guard_closed":
        installation.usage_incomplete_until = as_of + timedelta(days=1)
    if code == "cleanup_pending":
        installation.drain_required = True
    if code == "connector_unavailable":
        installation.reachability_mode = "managed_tunnel"
        installation.zone_id = "b" * 32
        installation.deployment_evidence = {"connector_session": "disconnected"}

    queue_codes = {
        "primary_has_capacity", "primary_capacity_unverified", "waiting_for_launch_gate",
        "tenant_limit_reached", "installation_slots_full", "eligible_for_overflow", "measured_primary_budget",
    }
    if code in queue_codes:
        run = Execution(workflow_name="fixture", executed_by_name="fixture")
        primary_worker = ExecutionWorker(
            form="external", capacity_scope="gap5-fixture", state="ready",
            heartbeat_at=as_of, supported_handlers=["workflow.execute"],
            memory_budget_bytes=2**28, memory_reserved_bytes=0 if code == "primary_has_capacity" else 2**28,
        )
        request_memory = {}
        if code == "measured_primary_budget":
            primary_worker.form = "job"
            primary_worker.capacity_scope = None
            primary_worker.kubernetes_namespace = "test"
            settings = cloudflare_status.get_settings()
            threshold = settings.memory_pressure_threshold
            limit = 1024 * 2**20
            environment = {"form": "job", "version": "test", "package_generation": 0,
                           "threshold": threshold, "image": "api:test"}
            measurement = {"version": "test", "package_generation": 0, "threshold": threshold,
                           "limit_bytes": limit, "baseline_bytes": JOB_BASELINE_ESTIMATE_BYTES + 64 * 2**20}
            db_session.add_all((
                ExecutionWorker(
                    form="job", state="starting", kubernetes_namespace="test", memory_limit_bytes=limit,
                    supported_handlers=["workflow.execute"], memory_calibration={"environment": environment},
                ),
                ExecutionWorker(
                    form="job", state="retired", kubernetes_namespace="test", memory_limit_bytes=limit,
                    memory_calibration={"environment": environment, "measurement": measurement},
                ),
            ))
            request_memory = {"memory_request_bytes": int(limit * threshold) - JOB_BASELINE_ESTIMATE_BYTES - 32 * 2**20}
        db_session.add_all((run, primary_worker))
        await db_session.flush()
        db_session.add(WorkDispatch(
            family=WAKE_WORKFLOW, handler="workflow.execute", handler_version=1, subject_id=run.id,
            message={}, payload={}, cloudflare_revision="c" * 64,
            created_at=as_of if code == "waiting_for_launch_gate" else as_of - timedelta(minutes=1),
            **request_memory,
        ))
        count = {"tenant_limit_reached": 4, "installation_slots_full": 9}.get(code, 0)
        db_session.add_all(CloudflareWorkflowSlot(
            installation_id=installation.id, tenant="platform", ordinal=ordinal, worker_id=primary_worker.id,
            execution_id=uuid4(), dispatch_id=uuid4(), fence=uuid4(), sent=True,
        ) for ordinal in range(count))
    await db_session.flush()

    app = FastAPI()
    app.include_router(capacity_routes.router)
    app.dependency_overrides[get_db] = lambda: db_session
    app.dependency_overrides[get_current_active_user] = lambda: caller.principal
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
        response = await http.get("/api/capacity/cloudflare")
    assert response.status_code == 200, response.text
    snapshot = response.json()
    assert installation.account_id not in response.text
    assert installation.worker_ingress_origin not in response.text
    workers, = snapshot["lanes"]
    reasons = {item["code"]: item for item in workers["reasons"]}
    expected = {
        "setup_required": ["setup_required", "waiting_for_demand"],
        "installation_slots_full": ["installation_slots_full", "tenant_limit_reached"],
        "measured_primary_budget": ["eligible_for_overflow"],
        **{blocker: [blocker, "waiting_for_demand"] for blocker in (
            "spend_guard_closed", "cleanup_pending", "cloudflare_capacity_unavailable",
            "release_mismatch", "connector_unavailable",
        )},
    }
    assert list(reasons) == expected.get(code, [code]), workers
    assert all(item["remedy"] for item in reasons.values())
    assert workers["eligible_queued"] == (1 if code in queue_codes else 0)
    if code in ("waiting_for_launch_gate", "cloudflare_capacity_unavailable"):
        assert reasons[code]["next_eligible_at"] is not None


@pytest.mark.asyncio
async def test_status_uses_scoped_runs_and_deployed_controller_observation(isolated_capacity_installation, monkeypatch):
    # Fit packs the whole queue before applying caller visibility.
    db_session, installation = isolated_capacity_installation
    org = Organization(name="Gap 5 Status Fixture", created_by="test")
    db_session.add(org)
    await db_session.flush()
    owner = User(email="gap5-status-fixture@example.test", organization_id=org.id)
    db_session.add(owner)
    await db_session.flush()
    role = RoleGrant(uuid4(), frozenset({"platform.read"}), (Boundary(BoundaryKind.PLATFORM),))
    caller = Caller(
        UserPrincipal(owner.id, owner.email, org.id),
        AuthorizationContext(owner.id, org.id, uuid4(), False, frozenset(), (role,)),
    )
    monkeypatch.setattr(cloudflare_policy, "installed_workers_release_digest", lambda: installation.policy["bounds_release"])
    now = await db_session.scalar(select(func.statement_timestamp()))
    assert now is not None
    installation.desired_revision = 2
    root_version = installation.control_root_version
    assert root_version is not None
    db_session.add(seal_credential(
        CredentialRef(installation.id, installation.account_id, "scope_bearer", "workers", 1),
        SecretStr("synthetic-scope-bearer"), actor=str(owner.id), now=now,
    ))
    await db_session.flush()
    db_session.add(CloudflareCapacityScope(
        installation_id=installation.id, scope="workers", deployment_revision=1,
        endpoint="https://capacity.example.test", bearer_version=1,
    ))
    primary = ExecutionWorker(form="external", capacity_scope="gap5-scoped", state="ready",
                              heartbeat_at=now, memory_budget_bytes=2**28)
    worker = ExecutionWorker(form="external", runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, state="ready")
    db_session.add_all([primary, worker])
    await db_session.flush()
    db_session.add(ManagedCloudflareScope(installation.id, "workers", 1).pin(
        worker.id, root_version, reserved_at=now, lifetime=3600, deadline_wire=False,
    ))
    for executed_by in (owner.id, None):
        for state in ("queued", "claimed", "running"):
            run = Execution(
                workflow_name="fixture", executed_by=executed_by, executed_by_name="fixture", organization_id=org.id,
                status=ExecutionStatus.RUNNING if state == "running" else ExecutionStatus.PENDING,
                started_at=now if state == "running" else None,
            )
            db_session.add(run)
            await db_session.flush()
            db_session.add(
                WorkDispatch(family=WAKE_WORKFLOW, handler="workflow.execute", handler_version=1,
                             subject_id=run.id, message={}, payload={}, cloudflare_revision="c" * 64)
                if state == "queued"
                else ExecutionWorkerClaim(worker_id=worker.id, family=WAKE_WORKFLOW, subject_id=run.id)
            )
    await db_session.flush()
    await cloudflare_status.record_controller_observation(
        ManagedCloudflareScope(installation.id, "workers", 1),
        primary=PrimaryPool("external", "gap5-scoped", 2), cause="below_ceiling",
    )
    status = await get_cloudflare_capacity_status(db_session, caller)
    workers, = status.lanes
    assert status.approved_workflow_count == 0
    assert (workers.eligible_queued, workers.claimed, workers.running) == (1, 1, 1)
    assert workers.primary_saturated is False
    assert workers.primary_fit == 1


@pytest.mark.asyncio
async def test_status_splits_only_identifiable_resource_reservations(db_session, installation):
    now = await db_session.scalar(select(func.statement_timestamp()))
    assert now is not None
    period, _ = usage_period(now)
    live = CloudflareUsageIncarnation(worker_id=uuid4(), installation_id=installation.id,
                                     started_at=now - timedelta(minutes=2), slots=1)
    db_session.add_all([
        live,
        CloudflareUsageIncarnation(worker_id=uuid4(), installation_id=installation.id,
                                   started_at=now - timedelta(minutes=2), deleted_at=now, slots=1),
        CloudflareUsageActivity(installation_id=installation.id, period_start=period, dispatched_runs=10),
    ])
    await db_session.flush()
    result = await get_cloudflare_capacity_status(db_session, _platform_operator())
    assert result.installation is not None
    spend = result.installation.spend
    assert spend is not None and spend.reserved_usd is not None and spend.settled_usd is not None
    outstanding = await db_session.scalar(select(cloudflare_spend.cloudflare_unsettled_resource_usage()).where(
        CloudflareInstallation.id == installation.id,
    ))
    assert outstanding is not None and outstanding > 0
    assert spend.reserved_usd == pytest.approx(outstanding, rel=Decimal("0.01"))
    assert spend.settled_usd > 0
    assert spend.estimated_usd == (spend.settled_usd + spend.reserved_usd).quantize(Decimal("0.01"))
    live.deleted_at = now
    await db_session.flush()
    assert await db_session.scalar(select(cloudflare_spend.cloudflare_unsettled_resource_usage()).where(
        CloudflareInstallation.id == installation.id,
    )) == Decimal(0)


@pytest.mark.asyncio
async def test_status_requires_global_platform_read(db_session, monkeypatch):
    role = RoleGrant(uuid4(), frozenset({"platform.read"}), (Boundary(BoundaryKind.ORGANIZATION, uuid4()),))
    caller = Caller(
        UserPrincipal(OPERATOR_ID, "operator@example.test", None),
        AuthorizationContext(OPERATOR_ID, None, uuid4(), False, frozenset(), (role,)),
    )
    monkeypatch.setattr(capacity_routes, "load_caller", AsyncMock(return_value=caller))
    app = FastAPI()
    app.include_router(capacity_routes.router)
    app.dependency_overrides[get_db] = lambda: db_session
    app.dependency_overrides[get_current_active_user] = lambda: caller.principal
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
        response = await http.get("/api/capacity/cloudflare")
    assert response.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_target", [False, True])
async def test_rotation_staging_binds_reference_to_the_active_installation(
    db_session, installation, lifecycle_audit_actor, monkeypatch, legacy_target,
):
    caller = _platform_operator()
    monkeypatch.setattr(capacity_routes, "load_caller", AsyncMock(return_value=caller))
    token = SecretStr("synthetic-rotation-token")
    target = {"account_id": installation.account_id} if legacy_target else {"installation_id": installation.id}
    request = CloudflareStageCredentialRequest(purpose="rotate", api_token=token, **target)
    staged = await capacity_routes.stage_cloudflare_credential(request, db_session, caller.principal)
    await db_session.flush()
    with pytest.raises(ValueError, match="Invalid or expired"):
        async with db_session.begin_nested():
            await consume_staged_token(
                db_session, staged.credential_ref, actor_id=OPERATOR_ID, account_id=installation.account_id,
                purpose="rotate", installation_id=uuid4(),
            )
    assert await consume_staged_token(
        db_session, staged.credential_ref, actor_id=OPERATOR_ID, account_id=installation.account_id,
        purpose="rotate", installation_id=installation.id,
    ) == token
