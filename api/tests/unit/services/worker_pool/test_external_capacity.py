"""External capacity uses immutable resources and canonical Postgres admission."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from pydantic import SecretStr
from shared.models import CloudflareCapacityPolicy, WorkerCapacityProfile, WorkerCapacityReservation, WorkerCapacityResource
from shared.worker_control_auth import managed_cloudflare_control_key
from sqlalchemy import delete, func, select
from src.models.enums import ExecutionStatus
from src.models.orm.cloudflare_capacity import (
    CloudflareCapacityScope,
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareUsageActivity,
    CloudflareUsageIncarnation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker
from src.models.orm.executions import Execution
from src.services.worker_pool import claims, cloudflare_spend, controller, external_controller, maintenance
from src.services.worker_pool.cloudflare_admission import (
    ManagedCloudflareScope,
    PrimaryPool,
)
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    seal_credential,
)
from src.services.worker_pool.cloudflare_policy import WorkersReleaseMeterBounds, _price_policy
from src.services.worker_pool.cloudflare_spend import CPU_LIMIT_PATHS, LAUNCHER_RECEIPT_METERS, WORKERS_RATES
from src.services.worker_pool.external_client import (
    CapacityProtocolError,
    ExternalCapacityClient,
)
from src.services.worker_pool.external_controller import ExternalWorkerPoolController
from src.worker.remote import ownership
from src.worker.remote.protocol import (
    CLOUDFLARE_DEV_BUDGET_BYTES,
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    LINUX_RUNTIME_PROFILE,
)
from tests.unit.services.worker_pool.test_worker_pool import (
    GIB,
    POLICY,
    SETTINGS,
    _accept_pool_run,
    _set_request,
    _store_policy,
)
from tests.unit.services.worker_pool.test_worker_pool import (
    _clean as _clean,
)
from tests.unit.services.worker_pool.test_worker_pool import (
    db as db_fixture,
)

db = db_fixture
TOKEN = "synthetic-capacity-token-" + "x" * 32
CONTROL_ROOT = "synthetic-platform-control-root"


class Provider:
    def __init__(self):
        self.profile = WorkerCapacityProfile(
            protocol_version=1, scope="test-provider", worker_version="test-worker",
            memory_limit_bytes=2 * GIB, max_workers=2, max_lifetime_seconds=21600,
        )
        self.resources = {}
        self.requests = []
        self.starts = []
        self.before_start: Callable[[UUID], Awaitable[None]] | None = None
        self.lose_start_response = False
        self.fail_deletions = False
        self.fail_reads = False
        self.refuse_prepare = False
        self.lose_prepare_response = False
        self.installation_id = uuid4()

    async def handle(self, request):
        assert request.headers["Authorization"] == f"Bearer {TOKEN}"
        self.requests.append((request.method, request.url.path))
        if request.url.path == "/v1/capabilities":
            return httpx.Response(200, json=self.profile.model_dump())
        assert request.headers["X-Bifrost-Capacity-Scope"] == self.profile.scope
        worker_id = request.url.path.split("/")[3]
        resource = self.resources.get(worker_id)
        if request.method == "GET":
            if self.fail_reads:
                raise httpx.ReadTimeout("synthetic provider read outage", request=request)
            return httpx.Response(404) if resource is None else httpx.Response(200, json=resource)
        body = json.loads(request.content)
        if request.method == "PUT":
            uid = f"resource-{UUID(worker_id).hex}"
            if self.profile.protocol_version == 2:
                uid = body.pop("resource_uid")
                assert body.pop("control_key") == managed_cloudflare_control_key(
                    CONTROL_ROOT, self.installation_id, 1, self.profile.scope, UUID(worker_id), uid,
                )
            reservation = WorkerCapacityReservation.model_validate(body)
            assert str(reservation.worker_id) == worker_id
            if self.refuse_prepare:
                return httpx.Response(409)
            if resource is None:
                resource = {
                    "scope": self.profile.scope, "worker_id": worker_id,
                    "worker_version": reservation.worker_version,
                    "resource_uid": uid, "state": "prepared",
                }
                self.resources[worker_id] = resource
            if self.lose_prepare_response:
                raise httpx.ReadTimeout("synthetic lost prepare response", request=request)
        else:
            assert resource is not None
            if body["resource_uid"] != resource["resource_uid"]:
                return httpx.Response(409)
            if request.method == "POST":
                if self.before_start is not None:
                    await self.before_start(UUID(worker_id))
                if resource["state"] == "prepared":
                    self.starts.append(worker_id)
                    resource["state"] = "running"
                if self.lose_start_response:
                    self.lose_start_response = False
                    raise httpx.ReadTimeout("synthetic lost response", request=request)
            elif request.method == "DELETE":
                if self.fail_deletions:
                    raise httpx.ReadTimeout("synthetic provider outage", request=request)
                resource["state"] = "deleted"
        return httpx.Response(200, json=resource)


@pytest_asyncio.fixture
async def capacity(db, monkeypatch):
    provider = Provider()
    policy = replace(POLICY, largest_worker_mib=2048)
    await _store_policy(db.sessions, policy)
    monkeypatch.setattr(external_controller, "get_db_context", db.context)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider.handle)) as http:
        client = ExternalCapacityClient("https://capacity.invalid", TOKEN, http=http)
        pool = ExternalWorkerPoolController(client, SETTINGS, await client.profile())
        yield SimpleNamespace(provider=provider, client=client, pool=pool, policy=policy)


@pytest_asyncio.fixture
async def managed_pool(db, capacity, monkeypatch):
    provider = capacity.provider
    provider.profile = provider.profile.model_copy(update={
        "protocol_version": 2,
        "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        "memory_limit_bytes": CLOUDFLARE_DEV_BUDGET_BYTES,
    })
    from src.services.worker_pool import cloudflare_admission

    monkeypatch.setattr(cloudflare_admission, "installed_workers_release_digest", lambda: provider.profile.worker_version)
    installation_id, scope_name = provider.installation_id, provider.profile.scope
    account_id, now = "a" * 32, datetime.now(UTC)
    policy = CloudflareCapacityPolicy.model_validate({
        "workers": {"launcher_cap": 1, "slots_per_launcher": 1, "max_lifetime_seconds": 120, "work_kinds": ["workflow"]},
        # The lifecycle fixture uses a two-minute incarnation. Keep its
        # synthetic budget above the projected monthly creation cost.
        "monthly_limit_usd": "50.00",
    })
    units = dict.fromkeys(WORKERS_RATES, Decimal(0))
    from src.services.worker_pool.cloudflare_release import WorkersReleaseOrder

    bounds = WorkersReleaseMeterBounds(
        60, units, units, provider.profile.worker_version, 1,
        dict.fromkeys(LAUNCHER_RECEIPT_METERS, 100), dict.fromkeys(CPU_LIMIT_PATHS, 1), 100, WorkersReleaseOrder("1.0.0", 1),
    )
    async with db.context() as session:
        session.add(CloudflareInstallation(
            id=installation_id, account_id=account_id, worker_ingress_origin="https://workers.example.test",
            consent_version="test", consent_actor="fixture", consent_at=now,
            policy=_price_policy(policy, bounds),
            desired_state="enabled", desired_revision=1, admitted_revision=1, api_token_version=1, control_root_version=1,
        ))
        await session.flush()
        session.add_all([
            seal_credential(CredentialRef(installation_id, account_id, purpose, scope, 1), SecretStr(secret),
                            actor="fixture", now=now)
            for purpose, scope, secret in (
                ("account_token", "", "synthetic-account-token"),
                ("control_root", "", CONTROL_ROOT),
                ("scope_bearer", scope_name, TOKEN),
            )
        ])
        await session.flush()
        session.add(CloudflareCapacityScope(
            installation_id=installation_id, scope=scope_name, endpoint="https://capacity.invalid",
            deployment_revision=1, bearer_version=1,
        ))
        session.add(ExecutionWorker(
            id=uuid4(), form="static", state="ready", heartbeat_at=now, memory_pressured=True,
        ))
    pool = ExternalWorkerPoolController(capacity.client, SETTINGS, provider.profile)
    pool.managed_cloudflare_scope = ManagedCloudflareScope(installation_id, scope_name, 1)
    pool.cloudflare_primary = PrimaryPool("static")
    pool.shared_managed_worker_cap = 1
    yield pool
    async with db.context() as session:
        row = await session.get(CloudflareInstallation, installation_id, with_for_update=True)
        row.desired_state, row.admitted_revision, row.retired_at = "disconnected", None, now
        row.control_root_version = row.api_token_version = None
        await session.flush()
        await session.execute(delete(CloudflareWorkerReservation).where(
            CloudflareWorkerReservation.installation_id == installation_id,
        ))
        await session.execute(delete(CloudflareCapacityScope).where(
            CloudflareCapacityScope.installation_id == installation_id,
        ))
        for meter in (CloudflareUsageActivity, CloudflareUsageIncarnation):
            await session.execute(delete(meter).where(meter.installation_id == installation_id))
        await session.execute(delete(CloudflareCredential).where(CloudflareCredential.installation_id == installation_id))
        await session.delete(row)


async def test_managed_cleanup_backoff_books_each_attempt_until_window_expires(db, capacity, managed_pool):
    age = timedelta(seconds=controller.SWEEP_SECONDS + 1)
    await _set_request(db.sessions, await _accept_pool_run(), 1, age=age, approved=True)
    [worker_id] = await managed_pool.launch_for_demand(capacity.policy)
    await managed_pool.reconcile_worker(worker_id, capacity.policy)
    provider = capacity.provider
    provider.fail_deletions = True
    provider.resources[str(worker_id)]["state"] = "stopped"
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker is not None
        worker.state, worker.ended_at = "retired", datetime.now(UTC)
    attempts = 9  # Transient failures can outlast the former eight-attempt cap.
    for attempt in range(attempts):
        with pytest.raises(httpx.ReadTimeout):
            await managed_pool.reconcile_worker(worker_id, capacity.policy)
        async with db.context() as session:
            pin = await session.get(CloudflareWorkerReservation, worker_id)
            assert pin is not None
            assert pin.cleanup_attempts == attempt + 1
            assert pin.cleanup_retry_at is not None
            pin.cleanup_retry_at = datetime.now(UTC) - timedelta(seconds=1)
    async with db.context() as session:
        pin = await session.get(CloudflareWorkerReservation, worker_id)
        assert pin is not None and pin.cleanup_started_at is not None
        pin.cleanup_started_at -= external_controller.MANAGED_CLEANUP_WINDOW
    # A successful DELETE would release it if the expired window still sent one.
    provider.fail_deletions = False
    await managed_pool.reconcile_worker(worker_id, capacity.policy)
    assert sum(method == "DELETE" for method, _ in provider.requests) == attempts
    async with db.context() as session:
        requests = await session.scalar(select(CloudflareUsageActivity.capacity_cleanup_do_requests).where(
            CloudflareUsageActivity.installation_id == capacity.provider.installation_id,
        ))
        assert requests == 2 * attempts
        meter = await session.get(CloudflareUsageIncarnation, worker_id)
        assert meter is not None and meter.deleted_at is None


async def test_managed_cleanup_window_starts_when_the_first_get_fails(db, capacity, managed_pool):
    age = timedelta(seconds=controller.SWEEP_SECONDS + 1)
    await _set_request(db.sessions, await _accept_pool_run(), 1, age=age, approved=True)
    [worker_id] = await managed_pool.launch_for_demand(capacity.policy)
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker is not None
        worker.state, worker.ended_at = "retired", datetime.now(UTC)

    provider = capacity.provider
    provider.fail_reads = True
    with pytest.raises(httpx.ReadTimeout):
        await managed_pool.reconcile_worker(worker_id, capacity.policy)
    async with db.context() as session:
        pin = await session.get(CloudflareWorkerReservation, worker_id)
        assert pin is not None and pin.cleanup_started_at is not None and pin.cleanup_attempts == 0
        expired_start = pin.cleanup_started_at - external_controller.MANAGED_CLEANUP_WINDOW
        pin.cleanup_started_at = expired_start

    with pytest.raises(httpx.ReadTimeout):
        await managed_pool.reconcile_worker(worker_id, capacity.policy)
    async with db.context() as session:
        pin = await session.get(CloudflareWorkerReservation, worker_id)
        assert pin is not None and pin.cleanup_started_at == expired_start
        assert await cloudflare_spend.exhausted_managed_workers(session, provider.installation_id) == 1
    assert not any(method == "DELETE" for method, _ in provider.requests)


async def test_managed_prepare_uses_the_uid_committed_before_provider_io(db, capacity, managed_pool):
    provider = capacity.provider
    pool = managed_pool
    policy = replace(capacity.policy, max_workers=1)
    age = timedelta(seconds=controller.SWEEP_SECONDS + 1)
    await _set_request(db.sessions, await _accept_pool_run(), 1, age=age, approved=True)
    [worker_id] = await pool.launch_for_demand(policy)
    async with db.context() as session:
        reserved = await session.get(ExecutionWorker, worker_id)
        assert reserved is not None and reserved.state == "reserved"
        uid = reserved.capacity_resource_uid
        assert uid is not None and len(uid) == 32

    provider.lose_prepare_response = True
    with pytest.raises(httpx.ReadTimeout):
        await pool.reconcile_worker(worker_id, policy)
    assert provider.resources[str(worker_id)]["resource_uid"] == uid
    await pool.reconcile_worker(worker_id, policy)
    assert provider.starts == [str(worker_id)]
    assert sum(method == "PUT" for method, _ in provider.requests) == 1


async def test_prepared_uid_is_committed_before_start(db, capacity):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)

    async def before_start(starting_id):
        assert starting_id == worker_id
        async with db.context() as session:
            worker = await session.scalar(select(ExecutionWorker).where(
                ExecutionWorker.id == worker_id,
            ).with_for_update(nowait=True))
            assert worker.state == "starting"
            assert worker.capacity_resource_uid == capacity.provider.resources[str(worker_id)]["resource_uid"]

    capacity.provider.before_start = before_start
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    assert capacity.provider.starts == [str(worker_id)]


async def test_cloudflare_reserves_only_approved_fitting_dispatches_before_hello(db, capacity, managed_pool):
    pool = managed_pool
    age = timedelta(seconds=controller.SWEEP_SECONDS + 1)
    for amount, approved in ((1, False), (CLOUDFLARE_DEV_BUDGET_BYTES + 1, True)):
        await _set_request(db.sessions, await _accept_pool_run(), amount, age=age, approved=approved)
    assert await pool.launch_for_demand(capacity.policy) == []
    await _set_request(db.sessions, await _accept_pool_run(), 1, age=age, approved=True)
    [worker_id] = await pool.launch_for_demand(capacity.policy)
    assert await pool.launch_for_demand(capacity.policy) == []  # pending declared slot covers the run

    async def before_start(starting_id):
        async with db.context() as session:
            worker = await session.get(ExecutionWorker, starting_id)
            assert worker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
            assert worker.remote_launcher_id == starting_id
            assert worker.supported_handlers == ["workflow.execute"]
            assert worker.state == "starting"
            assert worker.capacity_resource_uid == capacity.provider.resources[str(starting_id)]["resource_uid"]

    capacity.provider.before_start = before_start
    await pool.reconcile_worker(worker_id, capacity.policy)
    assert capacity.provider.starts == [str(worker_id)]


async def test_ready_cloudflare_before_first_budget_report_covers_demand(db, capacity):
    profile = capacity.provider.profile.model_copy(update={
        "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        "memory_limit_bytes": CLOUDFLARE_DEV_BUDGET_BYTES,
    })
    pool = ExternalWorkerPoolController(capacity.client, SETTINGS, profile)
    await _set_request(db.sessions, await _accept_pool_run(), 1, approved=True)
    [worker_id] = await pool.launch_for_demand(capacity.policy)
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        worker.state = "ready"
        worker.remote_owner_lease_expires_at = controller._now() + timedelta(minutes=1)
        assert worker.memory_budget_bytes is None
    assert await pool.launch_for_demand(capacity.policy) == []


async def test_timed_out_cloudflare_reservation_stops_retrying_prepare(db, capacity):
    profile = capacity.provider.profile.model_copy(update={
        "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        "memory_limit_bytes": CLOUDFLARE_DEV_BUDGET_BYTES,
    })
    pool = ExternalWorkerPoolController(capacity.client, SETTINGS, profile)
    await _set_request(db.sessions, await _accept_pool_run(), 1, approved=True)
    [worker_id] = await pool.launch_for_demand(capacity.policy)
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        worker.created_at = controller._now() - timedelta(seconds=SETTINGS.pool_start_timeout_seconds + 1)
    capacity.provider.requests.clear()
    await pool.reconcile_worker(worker_id, capacity.policy)
    assert capacity.provider.requests == []
    async with db.context() as session:
        assert (await session.get(ExecutionWorker, worker_id)).end_reason == "prepare_timeout"


@pytest.mark.parametrize("lost_reply", [False, True])
async def test_unbound_cloudflare_prepare_holds_room_until_cleanup(db, capacity, lost_reply):
    provider = capacity.provider
    provider.profile = provider.profile.model_copy(update={
        "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        "memory_limit_bytes": CLOUDFLARE_DEV_BUDGET_BYTES,
        "max_workers": 1,
    })
    provider.lose_prepare_response = lost_reply
    provider.refuse_prepare = not lost_reply
    pool = ExternalWorkerPoolController(capacity.client, SETTINGS, provider.profile)
    policy = replace(capacity.policy, max_workers=1)
    await _set_request(db.sessions, await _accept_pool_run(), 1, approved=True)
    [worker_id] = await pool.launch_for_demand(policy)
    with pytest.raises(httpx.ReadTimeout if lost_reply else httpx.HTTPStatusError):
        await pool.reconcile_worker(worker_id, policy)
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        worker.created_at -= timedelta(seconds=SETTINGS.pool_start_timeout_seconds + 1)
    await pool.reconcile_worker(worker_id, policy)
    assert await pool.launch_for_demand(policy) == []
    await pool.reconcile_worker(worker_id, policy)
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker.capacity_resource_uid is None
        assert (worker.capacity_resource_deleted_at is not None) == lost_reply
        if not lost_reply:
            worker.ended_at -= external_controller.UNBOUND_PREPARE_GRACE
    if lost_reply:
        assert provider.resources[str(worker_id)]["state"] == "deleted"
    else:
        await pool.reconcile_worker(worker_id, policy)
    async with db.context() as session:
        assert await session.scalar(select(ExecutionWorker.id).where(
            ExecutionWorker.id == worker_id, pool._cleanup_scope(),
        )) is None
    assert len(await pool.launch_for_demand(policy)) == 1


async def test_terminal_scope_slot_holds_physical_room_but_does_not_trigger_burst(db, capacity):
    linux = capacity.provider.profile.model_copy(update={"max_workers": 1})
    cloudflare = linux.model_copy(update={
        "scope": "burst", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        "memory_limit_bytes": CLOUDFLARE_DEV_BUDGET_BYTES,
    })
    burst = ExternalWorkerPoolController(capacity.client, SETTINGS, cloudflare)
    burst.peer_profile = linux
    async with db.context() as session:
        session.add(ExecutionWorker(
            id=uuid4(), form="external", state="lost", capacity_scope=linux.scope,
            capacity_resource_uid="unconfirmed",
        ))
    async with db.context() as session:
        assert await burst._external_scope_counts(session, cloudflare, linux) == {linux.scope: 1}
    await _set_request(db.sessions, await _accept_pool_run(), 1, approved=True)
    assert await burst.launch_for_demand(capacity.policy) == []


async def test_burst_terminal_still_holds_shared_physical_room(db, capacity):
    pool = capacity.pool
    pool.peer_profile = pool.profile.model_copy(update={"scope": "burst", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE})
    async with db.context() as session:
        session.add(ExecutionWorker(
            id=uuid4(), form="external", state="retired", capacity_scope="burst",
            runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, capacity_resource_uid="unconfirmed",
        ))
    assert await pool.launch_for_demand(replace(capacity.policy, max_workers=1, warm_workers=1)) == []


@pytest.mark.parametrize("warm", [0, 1])
async def test_linux_can_reclaim_an_idle_cloudflare_slot(db, capacity, warm):
    pool = capacity.pool
    pool.peer_profile = pool.profile.model_copy(update={
        "scope": "burst", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    })
    policy = replace(capacity.policy, max_workers=1, warm_workers=warm)
    worker_id = uuid4()
    async with db.context() as session:
        session.add(ExecutionWorker(
            id=worker_id, form="external", state="ready", resource_class="standard",
            capacity_scope=pool.peer_profile.scope, runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
            memory_limit_bytes=2 * CLOUDFLARE_DEV_BUDGET_BYTES,
            memory_budget_bytes=2 * CLOUDFLARE_DEV_BUDGET_BYTES, supported_handlers=["workflow.execute"],
            remote_owner_lease_expires_at=controller._now() + timedelta(minutes=1),
            version=pool.peer_profile.worker_version,
        ))
    if not warm:
        await _accept_pool_run()
    assert await pool.launch_for_demand(policy) == []
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker.end_reason == "capacity_replacement"
        assert worker.state == "draining"


async def test_linux_warm_fill_does_not_replace_linux_without_a_launch_plan(db, capacity, monkeypatch):
    pool = capacity.pool
    pool.peer_profile = pool.profile.model_copy(update={
        "scope": "burst", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    })
    linux_id = uuid4()
    async with db.context() as session:
        session.add_all([
            ExecutionWorker(
                id=linux_id, form="external", state="ready", resource_class="standard",
                version=pool.version, capacity_scope=pool.profile.scope, memory_budget_bytes=GIB,
                active_handler_counts={}, supported_handlers=["workflow.execute"],
            ),
            ExecutionWorker(
                id=uuid4(), form="external", state="ready", resource_class="standard",
                capacity_scope=pool.peer_profile.scope, runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
                active_runs=1,
            ),
        ])
    await _set_request(db.sessions, await _accept_pool_run(), 2 * GIB)
    # The oversized request has no launchable worker after Linux calibration.
    monkeypatch.setattr(controller, "_plan_launches", lambda *args, **kwargs: controller.LaunchPlan(0, []))
    assert await pool.launch_for_demand(replace(capacity.policy, warm_workers=2)) == []
    async with db.context() as session:
        assert (await session.get(ExecutionWorker, linux_id)).state == "ready"


async def test_dual_limits_apply_after_discovery_without_blocking_burst_delete(db, capacity):
    primary = capacity.pool
    capacity.provider.profile = primary.profile.model_copy(update={
        "scope": "burst", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        "memory_limit_bytes": CLOUDFLARE_DEV_BUDGET_BYTES,
    })
    burst = ExternalWorkerPoolController(capacity.client, primary.settings, capacity.provider.profile)
    policy = replace(capacity.policy, max_workers=primary.profile.max_workers + burst.profile.max_workers + 1)
    primary.peer_profile = burst.profile
    burst.peer_profile = primary.profile
    with pytest.raises(CapacityProtocolError, match="max_workers"):
        primary._sizing(policy, [])

    worker_id = uuid4()
    prepared = await capacity.client.prepare(WorkerCapacityReservation(
        worker_id=worker_id, worker_version=burst.version, memory_limit_bytes=CLOUDFLARE_DEV_BUDGET_BYTES,
        max_lifetime_seconds=60, drain_grace_seconds=0,
    ), burst.profile.scope)
    async with db.context() as session:
        session.add(ExecutionWorker(
            id=worker_id, form="external", state="retired", capacity_scope=burst.profile.scope,
            runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, capacity_resource_uid=prepared.resource_uid,
        ))
    burst.burst_launch_allowed = False
    await burst.reconcile_worker(worker_id, policy)
    assert capacity.provider.resources[str(worker_id)]["state"] == "deleted"


async def test_outdated_uses_the_worker_scope_version_even_if_other_scope_runs_that_build(capacity):
    linux = Provider().profile
    burst = linux.model_copy(update={
        "scope": "burst", "worker_version": "new-burst", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    })
    pool = ExternalWorkerPoolController(capacity.client, SETTINGS, burst)
    pool.peer_profile = linux
    pool.live = controller.LiveControllers(ids=frozenset(), versions=frozenset({linux.worker_version, burst.worker_version}))
    row = ExecutionWorker(
        form="external", state="ready", capacity_scope=burst.scope,
        runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, version=linux.worker_version,
    )
    assert pool._outdated(row)
    row.version = burst.worker_version
    assert not pool._outdated(row)


async def test_idle_cloudflare_drains_even_when_linux_floor_is_unfilled(db, capacity, monkeypatch):
    await _store_policy(db.sessions, replace(capacity.policy, warm_workers=1))
    async with db.context() as session:
        linux = ExecutionWorker(id=uuid4(), form="external", state="ready", resource_class="standard")
        burst = ExecutionWorker(
            id=uuid4(), form="external", state="ready", resource_class="standard",
            runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, capacity_scope="burst",
        )
        session.add_all([linux, burst])
        linux_id, burst_id = linux.id, burst.id
    assert not await claims.begin_idle_drain(linux_id)
    assert await claims.begin_idle_drain(burst_id)


async def test_cloudflare_burst_waits_for_linux_ceiling_and_respects_shared_cap(db, capacity):
    linux = capacity.provider.profile.model_copy(update={"max_workers": 1})
    cloudflare = capacity.provider.profile.model_copy(update={
        "scope": "burst", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        "memory_limit_bytes": CLOUDFLARE_DEV_BUDGET_BYTES,
    })
    capacity.pool.profile = linux
    capacity.pool.peer_profile = cloudflare
    burst = ExternalWorkerPoolController(capacity.client, SETTINGS, cloudflare)
    burst.peer_profile = linux
    for _ in range(2):
        await _set_request(
            db.sessions, await _accept_pool_run(), CLOUDFLARE_DEV_BUDGET_BYTES, approved=True,
        )
    assert await burst.launch_for_demand(capacity.policy) == []
    [linux_id] = await capacity.pool.launch_for_demand(capacity.policy)
    assert await burst.launch_for_demand(capacity.policy) == []
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, linux_id)
        worker.state = "ready"
        worker.memory_pressured = True
        await session.commit()
    [burst_id] = await burst.launch_for_demand(capacity.policy)
    assert burst_id != linux_id
    assert await burst.launch_for_demand(capacity.policy) == []
    assert await capacity.pool.launch_for_demand(capacity.policy) == []


@pytest.mark.parametrize("max_workers", [1, 2], ids=["at-cap", "spare-slot"])
async def test_cloudflare_hello_preserves_the_external_warm_worker(db, capacity, monkeypatch, max_workers):
    policy = replace(capacity.policy, warm_workers=1, max_workers=max_workers)
    [worker_id] = await capacity.pool.launch_for_demand(policy)
    await capacity.pool.reconcile_worker(worker_id, policy)
    uid = capacity.provider.resources[str(worker_id)]["resource_uid"]
    launcher_id = uuid4()
    version = capacity.provider.profile.worker_version
    # Seed the Cloudflare binding that a future provider would prepare before hello.
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker.state == "starting"
        worker.runtime_profile = CLOUDFLARE_PYTHON_RUNTIME_PROFILE
        worker.memory_limit_bytes = 2 * CLOUDFLARE_DEV_BUDGET_BYTES
        worker.remote_launcher_id = launcher_id
        worker.supported_handlers = ["workflow.execute"]

    monkeypatch.setattr(ownership, "get_liveness_db_context", db.context)
    lease = await ownership.acquire_remote_owner(
        worker_id=worker_id, owner_id=uuid4(), launcher_id=launcher_id,
        nonce_digest="a" * 64, hello_sequence=1, runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        credential_protocol=2,
        cloudflare_enrollment=ownership.CloudflareEnrollment(uid, version, "cloudflare-worker"),
    )
    assert lease is not None and lease.declared_slots == 2
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker.state == "ready"
        assert worker.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
        worker.memory_budget_bytes = 2 * CLOUDFLARE_DEV_BUDGET_BYTES

    # A hello preserves the warm worker. Even with pending Linux demand at the
    # cap, this provider cannot free a slot by replacing its own launcher.
    if max_workers == 1:
        await _accept_pool_run()
    assert await capacity.pool.launch_for_demand(policy) == []
    async with db.context() as session:
        assert (await session.get(ExecutionWorker, worker_id)).state == "ready"


async def test_lost_start_response_is_observed_without_resending_the_start(db, capacity):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    capacity.provider.lose_start_response = True
    with pytest.raises(httpx.ReadTimeout):
        await capacity.pool.reconcile_worker(worker_id, capacity.policy)

    await capacity.pool.reconcile_worker(worker_id, capacity.policy)

    assert capacity.provider.starts == [str(worker_id)]
    assert sum(method == "POST" for method, _path in capacity.provider.requests) == 1
    assert len(capacity.provider.resources) == 1


async def test_two_controllers_adopt_the_same_prepared_resource(db, capacity):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    other = ExternalWorkerPoolController(capacity.client, SETTINGS, capacity.provider.profile)

    await asyncio.gather(
        capacity.pool.reconcile_worker(worker_id, capacity.policy),
        other.reconcile_worker(worker_id, capacity.policy),
    )

    assert len(capacity.provider.resources) == 1
    assert capacity.provider.starts == [str(worker_id)]
    assert await other.launch_for_demand(capacity.policy) == []


async def test_external_admission_binds_only_the_first_process(db, capacity):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    uid = capacity.provider.resources[str(worker_id)]["resource_uid"]
    launcher_id = uuid4()

    assert await claims.admit_external_worker(
        worker_id, resource_uid="wrong", launcher_id=launcher_id, version="test-worker", hostname="unbound",
    ) is None
    for version in (None, "previous-worker"):
        assert await claims.admit_external_worker(
            worker_id, resource_uid=uid, launcher_id=launcher_id, version=version, hostname="wrong-build",
        ) is None
    assert await claims.admit_external_worker(
        worker_id, resource_uid=uid, launcher_id=launcher_id, version="test-worker", hostname="physical-worker",
    ) == worker_id
    assert await claims.admit_external_worker(
        worker_id, resource_uid=uid, launcher_id=uuid4(), version="test-worker", hostname="replacement",
    ) is None
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker.remote_launcher_id == launcher_id
        assert worker.version == "test-worker"
        assert worker.hostname == "physical-worker"


async def test_provider_rollout_fences_an_old_reservation_instead_of_relabeling_it(db, capacity):
    await _accept_pool_run()
    [old_id] = await capacity.pool.launch_for_demand(capacity.policy)
    capacity.provider.profile = capacity.provider.profile.model_copy(update={"worker_version": "next-worker"})
    capacity.pool.profile = capacity.provider.profile
    capacity.pool.version = "next-worker"
    await capacity.pool.reconcile_worker(old_id, capacity.policy)
    assert capacity.provider.resources == {}
    assert capacity.provider.starts == []
    async with db.context() as session:
        old = await session.get(ExecutionWorker, old_id)
        assert (old.state, old.version, old.end_reason) == ("lost", "test-worker", "provider_version_changed")
    [new_id] = await capacity.pool.launch_for_demand(capacity.policy)
    assert new_id != old_id
    async with db.context() as session:
        assert (await session.get(ExecutionWorker, new_id)).version == "next-worker"


async def test_recovered_preparation_cannot_bind_or_start_a_different_build(db, capacity):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    # Preparation committed at the provider, but its response was lost before
    # the canonical resource UID could be bound.
    await capacity.client.prepare(WorkerCapacityReservation(
        worker_id=worker_id, worker_version="test-worker", memory_limit_bytes=2 * GIB,
        max_lifetime_seconds=SETTINGS.pool_max_lifetime_seconds, drain_grace_seconds=30,
    ), capacity.provider.profile.scope)
    capacity.provider.resources[str(worker_id)]["worker_version"] = "wrong-worker"

    with pytest.raises(CapacityProtocolError, match="build changed"):
        await capacity.pool.reconcile_worker(worker_id, capacity.policy)

    assert capacity.provider.starts == []
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker.state == "reserved"
        assert worker.capacity_resource_uid is None


async def test_build_mismatch_does_not_prevent_exact_cleanup_after_fencing(db, capacity):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    resource = capacity.provider.resources[str(worker_id)]
    resource["worker_version"] = "wrong-worker"
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        worker.state = "lost"
        await session.commit()

    await capacity.pool.reconcile_worker(worker_id, capacity.policy)

    assert resource["state"] == "deleted"
    assert capacity.provider.starts == [str(worker_id)]


@pytest.mark.parametrize(
    "runtime_profile", [LINUX_RUNTIME_PROFILE, CLOUDFLARE_PYTHON_RUNTIME_PROFILE], ids=["linux", "cloudflare"],
)
async def test_provider_rollout_rotates_an_admitted_worker_from_the_same_controller(db, capacity, runtime_profile):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    uid = capacity.provider.resources[str(worker_id)]["resource_uid"]
    await claims.admit_external_worker(
        worker_id, resource_uid=uid, launcher_id=uuid4(), version="test-worker", hostname="old-build",
    )
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        worker.runtime_profile = runtime_profile
    # This controller remains alive while the external provider changes builds.
    capacity.pool.live = external_controller.LiveControllers(
        ids=frozenset({capacity.pool.id}), versions=frozenset({"next-worker"}),
    )

    assert await capacity.pool.replace_outdated(replace(capacity.policy, warm_workers=0)) == [worker_id]

    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert (worker.state, worker.end_reason) == ("draining", "rotated")
    assert capacity.provider.resources[str(worker_id)]["state"] == "running"


async def test_provider_rollout_leaves_cloudflare_workers_in_other_scopes_ready(db, capacity):
    worker_id = uuid4()
    async with db.context() as session:
        session.add(ExecutionWorker(
            id=worker_id, form="external", state="ready", capacity_scope="another-provider",
            runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, version="old-worker",
        ))

    assert await capacity.pool.replace_outdated(replace(capacity.policy, warm_workers=0)) == []
    async with db.context() as session:
        assert (await session.get(ExecutionWorker, worker_id)).state == "ready"


async def test_provider_cannot_replace_an_existing_resource_uid(db, capacity):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    capacity.provider.resources[str(worker_id)]["resource_uid"] = "replacement"

    with pytest.raises(CapacityProtocolError, match="UID changed"):
        await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    assert not any(method == "DELETE" for method, _path in capacity.provider.requests)


async def test_policy_exceeding_provider_capability_cannot_reserve(db, capacity):
    await _accept_pool_run()
    with pytest.raises(CapacityProtocolError, match="capabilities"):
        await capacity.pool.launch_for_demand(replace(capacity.policy, max_workers=3))
    async with db.context() as session:
        assert list(await session.scalars(select(ExecutionWorker.id).where(ExecutionWorker.form == "external"))) == []


async def test_provider_failure_never_ends_a_fresh_heartbeat_as_silent(db, capacity):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    uid = capacity.provider.resources[str(worker_id)]["resource_uid"]
    await claims.admit_external_worker(worker_id, resource_uid=uid, launcher_id=uuid4(), version="test-worker", hostname="physical-worker")
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    async with db.context() as session:
        assert (await session.get(ExecutionWorker, worker_id)).state == "ready"


async def test_lost_provider_resource_fences_its_running_work_before_cleanup(db, capacity):
    execution_id = UUID(await _accept_pool_run())
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    resource = capacity.provider.resources[str(worker_id)]
    await claims.admit_external_worker(
        worker_id, resource_uid=resource["resource_uid"], launcher_id=uuid4(), version="test-worker", hostname="physical-worker",
    )
    await claims.heartbeat_worker(
        worker_id, active_runs=0, runs_handled=0, package_generation=0,
        memory_pressured=False, memory_budget_bytes=GIB, memory_reserved_bytes=0,
        active_handler_counts={},
    )
    assert await claims.claim_next(worker_id, fits_bytes=GIB, budget_bytes=GIB) is not None
    resource["state"] = "stopped"

    await capacity.pool.reconcile_worker(worker_id, capacity.policy)

    async with db.context() as session:
        assert (await session.get(ExecutionWorker, worker_id)).state == "lost"
        assert (await session.get(Execution, execution_id)).status == ExecutionStatus.FAILED
    assert resource["state"] == "stopped", "physical cleanup follows canonical fencing"
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    assert resource["state"] == "deleted"


async def test_managed_provider_forms_share_the_worker_ceiling(db, capacity):
    async with db.context() as session:
        session.add_all([
            ExecutionWorker(form=form, state="draining", memory_limit_bytes=2 * GIB)
            for form in ("job", "external")
        ])
    await _accept_pool_run()
    assert await capacity.pool.launch_for_demand(capacity.policy) == []


async def test_external_workers_preserve_the_shared_warm_minimum(db, capacity):
    await _store_policy(db.sessions, replace(capacity.policy, warm_workers=1))
    async with db.context() as session:
        first = ExecutionWorker(form="external", state="ready")
        session.add(first)
        await session.flush()
        first_id = first.id
    assert not await claims.begin_idle_drain(first_id)
    async with db.context() as session:
        session.add(ExecutionWorker(form="external", state="ready"))
    assert await claims.begin_idle_drain(first_id)


@pytest.mark.parametrize("endpoint", [
    "http://capacity.invalid", "https://user:secret@capacity.invalid",
    "https://capacity.invalid/?token=x", "https://capacity.invalid:broken",
])
def test_unsafe_provider_urls_are_rejected(endpoint):
    with pytest.raises(ValueError, match="HTTPS"):
        ExternalCapacityClient.check_configuration(endpoint, TOKEN)


@pytest.mark.parametrize("token", ["short", "x" * 40 + "\n", "\N{LATIN SMALL LETTER E WITH ACUTE}" * 40])
def test_invalid_bearer_tokens_fail_without_echoing_the_value(token):
    with pytest.raises(ValueError) as error:
        ExternalCapacityClient.check_configuration("https://capacity.invalid", token)
    assert token not in str(error.value)


async def test_provider_redirects_never_forward_the_bearer_to_another_origin():
    requests = []

    def redirect(request):
        requests.append(request.url.host)
        return httpx.Response(307, headers={"Location": "https://other.invalid/v1/capabilities"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(redirect), follow_redirects=True) as http:
        client = ExternalCapacityClient("https://capacity.invalid", TOKEN, http=http)
        with pytest.raises(httpx.HTTPStatusError):
            await client.profile()
    assert requests == ["capacity.invalid"]


@pytest.mark.parametrize("reply", ["fence", "receipt", "wrong_uid", "running"])
async def test_acknowledge_receipt_requires_matching_compacted_fence(reply):
    worker_id = uuid4()
    usage = {"sqlite_rows_written": 3, "do_requests": 1, "log_events": 0, "ledger_receipt_pending": True}

    def provider(request):
        assert request.method == "DELETE" and request.url.path == f"/v1/workers/{worker_id}"
        assert request.headers["X-Bifrost-Capacity-Scope"] == "workers"
        assert json.loads(request.content) == {"resource_uid": "owned", "receipt_ack": True}
        return httpx.Response(200, json={
            "worker_id": str(worker_id), "scope": "workers", "worker_version": "fixture",
            "resource_uid": "other" if reply == "wrong_uid" else "owned",
            "state": "running" if reply == "running" else "deleted",
            **({"usage": usage} if reply == "receipt" else {}),
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = ExternalCapacityClient("https://capacity.invalid", TOKEN, http=http)
        if reply == "fence":
            await client.acknowledge_receipt(worker_id, "workers", "owned")
        else:
            with pytest.raises(CapacityProtocolError):
                await client.acknowledge_receipt(worker_id, "workers", "owned")


async def test_controller_retries_receipt_ack_after_committing_settlement(db, capacity, managed_pool, monkeypatch):
    age = timedelta(seconds=controller.SWEEP_SECONDS + 1)
    await _set_request(db.sessions, await _accept_pool_run(), 1, age=age, approved=True)
    [worker_id] = await managed_pool.launch_for_demand(capacity.policy)
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker is not None
        uid = worker.capacity_resource_uid
        assert uid is not None
        worker.state = "retired"
        worker.ended_at = controller._now() - maintenance.ENDED_RETENTION - timedelta(days=1)
    scope = capacity.provider.profile.scope
    receipt = WorkerCapacityResource.model_validate({
        "scope": scope, "worker_id": worker_id, "worker_version": capacity.provider.profile.worker_version,
        "resource_uid": uid, "state": "deleted",
        "usage": {"sqlite_rows_written": 3, "do_requests": 1, "log_events": 0, "ledger_receipt_pending": True},
    })
    monkeypatch.setattr(managed_pool.client, "get", AsyncMock(return_value=receipt))
    monkeypatch.setattr(cloudflare_spend, "get_db_context", db.context)
    attempts = 0

    async def acknowledge(actual_worker, actual_scope, actual_uid):
        nonlocal attempts
        assert (actual_worker, actual_scope, actual_uid) == (worker_id, scope, uid)
        async with db.context() as session:
            settled = await session.get(CloudflareUsageIncarnation, worker_id)
            assert settled is not None and settled.deleted_at is not None
            assert settled.meter_final and settled.meter_final["ledger_receipt_pending"] is True
        attempts += 1
        if attempts == 1:
            raise httpx.ReadTimeout("lost acknowledgement")

    monkeypatch.setattr(managed_pool.client, "acknowledge_receipt", acknowledge)
    with pytest.raises(httpx.ReadTimeout):
        await managed_pool.reconcile_worker(worker_id, capacity.policy)
    await maintenance.prune_execution_workers()
    cleanup = select(ExecutionWorker.id).where(ExecutionWorker.id == worker_id, managed_pool._cleanup_scope())
    async with db.context() as session:
        assert await session.scalar(cleanup) == worker_id
        assert await session.get(CloudflareWorkerReservation, worker_id) is not None
    await managed_pool.reconcile_worker(worker_id, capacity.policy)
    assert attempts == 2
    async with db.context() as session:
        assert await session.scalar(cleanup) is None
    await maintenance.prune_execution_workers()
    async with db.context() as session:
        assert await session.get(ExecutionWorker, worker_id) is None


@pytest.mark.parametrize("terminal_state", ["lost", "retired"])
async def test_cleanup_survives_provider_outage_beyond_one_hour_and_controller_restart(
    db, capacity, monkeypatch, terminal_state,
):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    await capacity.pool.reconcile_worker(worker_id, capacity.policy)
    resource = capacity.provider.resources[str(worker_id)]
    ended_at = controller._now()
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        worker.state, worker.ended_at = terminal_state, ended_at
        await session.commit()
    monkeypatch.setattr(capacity.pool, "launch_for_demand", AsyncMock(return_value=[]))
    monkeypatch.setattr(capacity.pool, "replace_outdated", AsyncMock())
    capacity.provider.fail_deletions = True
    await capacity.pool.reconcile()
    later = ended_at + timedelta(hours=2)
    monkeypatch.setattr(controller, "_now", lambda: later)
    monkeypatch.setattr(external_controller, "_now", lambda: later)
    await capacity.pool.reconcile()
    deletions = [method for method, path in capacity.provider.requests
                 if method == "DELETE" and path.endswith(str(worker_id))]
    assert len(deletions) == 2, "cleanup must remain eligible throughout the outage"
    capacity.provider.fail_deletions = False
    restarted = ExternalWorkerPoolController(capacity.client, SETTINGS, capacity.provider.profile)
    monkeypatch.setattr(restarted, "launch_for_demand", AsyncMock(return_value=[]))
    monkeypatch.setattr(restarted, "replace_outdated", AsyncMock())
    async with db.context() as session:
        before_cleanup = await session.scalar(select(func.clock_timestamp()))
    await restarted.reconcile()
    assert resource["state"] == "deleted"
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker.state == terminal_state
        assert worker.capacity_resource_uid == resource["resource_uid"]
        after_cleanup = await session.scalar(select(func.clock_timestamp()))
        assert before_cleanup <= worker.capacity_resource_deleted_at <= after_cleanup
    before = sum(path.endswith(str(worker_id)) for _, path in capacity.provider.requests)
    await restarted.reconcile()
    assert sum(path.endswith(str(worker_id)) for _, path in capacity.provider.requests) == before


async def test_absence_during_delayed_prepare_does_not_confirm_cleanup(db, capacity, monkeypatch):
    await _accept_pool_run()
    [worker_id] = await capacity.pool.launch_for_demand(capacity.policy)
    preparing, release = asyncio.Event(), asyncio.Event()
    prepare = capacity.client.prepare

    async def delayed_prepare(reservation, scope):
        preparing.set()
        await release.wait()
        return await prepare(reservation, scope)

    monkeypatch.setattr(capacity.client, "prepare", delayed_prepare)
    pending = asyncio.create_task(capacity.pool.reconcile_worker(worker_id, capacity.policy))
    try:
        await preparing.wait()
        await capacity.pool._record_loss(worker_id, None, "provider_version_changed", False)
        restarted = ExternalWorkerPoolController(capacity.client, SETTINGS, capacity.provider.profile)
        await restarted.reconcile_worker(worker_id, capacity.policy)
        async with db.context() as session:
            worker = await session.get(ExecutionWorker, worker_id)
            assert worker.capacity_resource_deleted_at is None, "absence cannot fence a delayed prepare"
    finally:
        release.set()
        await asyncio.gather(pending)

    assert capacity.provider.resources[str(worker_id)]["state"] == "prepared"
    assert capacity.provider.starts == []
    monkeypatch.setattr(restarted, "launch_for_demand", AsyncMock(return_value=[]))
    monkeypatch.setattr(restarted, "replace_outdated", AsyncMock())
    await restarted.reconcile()

    assert capacity.provider.resources[str(worker_id)]["state"] == "deleted"
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker.state == "lost"
        assert worker.capacity_resource_deleted_at is not None


async def test_fixed_size_provider_measures_one_cold_worker_before_admission(db, capacity):
    from src.services import work_dispatch
    from tests.unit.services.worker_pool.test_worker_pool import (
        MIB,
        _claim,
        _report_budget,
        _set_request,
    )

    capacity.provider.profile = capacity.provider.profile.model_copy(update={"memory_limit_bytes": GIB})
    capacity.pool.profile = capacity.provider.profile
    policy = replace(capacity.policy, worker_memory_mib=1024, largest_worker_mib=1024)
    await _store_policy(db.sessions, policy)
    run_id = await _accept_pool_run()
    await _set_request(db.sessions, run_id, 128 * MIB)
    other = ExternalWorkerPoolController(capacity.client, SETTINGS, capacity.provider.profile)
    results = await asyncio.gather(
        capacity.pool.launch_for_demand(policy), other.launch_for_demand(policy),
    )
    assert sorted(len(ids) for ids in results) == [0, 1]
    worker_id = next(ids[0] for ids in results if ids)
    await capacity.pool.reconcile_worker(worker_id, policy)
    assert len(capacity.provider.starts) == 1
    async with db.context() as session:
        worker = await session.get(ExecutionWorker, worker_id)
        assert worker.memory_limit_bytes == GIB
        assert worker.reserved_for_dispatch_id is None
        assert await work_dispatch.largest_budget(session) is None
    assert await _claim(worker_id) is None
    uid = capacity.provider.resources[str(worker_id)]["resource_uid"]
    assert await claims.admit_external_worker(
        worker_id, resource_uid=uid, launcher_id=uuid4(),
        version=capacity.provider.profile.worker_version, hostname="cold-provider-worker",
    ) is not None
    await _report_budget(worker_id, 512 * MIB)
    await capacity.pool.reconcile()
    async with db.context() as session:
        assert await work_dispatch.largest_budget(session) == 512 * MIB
    assert await _claim(worker_id, fits=512 * MIB, budget=512 * MIB) is not None
