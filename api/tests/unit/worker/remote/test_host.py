"""API-host lifecycle contracts for remote control."""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import UUID

import pytest
from pydantic import SecretStr
from shared.worker_control_auth import external_worker_control_key
from sqlalchemy.exc import OperationalError
from src.services.execution.template_process import TEMPLATE_START_TIMEOUT_SECONDS
from src.services.memory_requests import AGENT_RUN_REQUEST_BYTES
from src.services.worker_pool.claims import PoolClaim
from src.services.worker_pool.cloudflare_slots import WorkflowSlotTicket
from src.services.worker_pool.cloudflare_spend import PreparationBooking
from src.worker.admission import Admission
from src.worker.pool_main import PoolWorker
from src.worker.remote.agent_route import AgentRoute
from src.worker.remote.cloudflare_agent import (
    CloudflareAgentRoute,
    CloudflareAgentRoutes,
)
from src.worker.remote.fencing import LeaseRef
from src.worker.remote.host import (
    ApiExecutor,
    Bridge,
    RemoteControlHost,
    RemoteSession,
    _reply_timeout,
)
from src.worker.remote.identity import LauncherIdentity
from src.worker.remote.launcher import Launcher
from src.worker.remote.ownership import RemoteOwnerLease
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    LEGACY_EXECUTION_HANDLERS,
    LINUX_RUNTIME_PROFILE,
    MAX_FRAME_BYTES,
    AuthenticationError,
    ControlEnvelope,
    CredentialRouteChanged,
    FragmentAssembler,
    Hello,
    ProtocolError,
    sign_hello,
)

LEASE = RemoteOwnerLease(
    worker_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    owner_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
    launcher_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
    session_generation=1,
)


@pytest.mark.parametrize(("profile", "expected"), [
    (CLOUDFLARE_PYTHON_RUNTIME_PROFILE, ()),
    (LINUX_RUNTIME_PROFILE, LEGACY_EXECUTION_HANDLERS),
])
def test_bridge_defaults_only_legacy_linux_handlers(profile, expected) -> None:
    bridge = Bridge(Mock(), {
        "hostname": "runner", "runtime_profile": profile, "current": 1, "limit": 100, "requirements": {},
    }, LEASE)

    assert bridge.handlers == expected


async def test_cloudflare_claim_starts_preparation_without_waiting_for_reply() -> None:
    lease = replace(LEASE, declared_slots=1)
    bridge = Bridge(Mock(), {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, lease)
    bridge.inventory = {"admission_prepare": True}
    worker = ApiExecutor(bridge, lease.worker_id)
    sent = asyncio.Event()
    finish = asyncio.Event()
    started = asyncio.Event()

    async def request(action, payload, *, expected_generation):
        assert action == "prepare_workflow"
        assert payload["org_id"] == str(lease.owner_id)
        assert payload["execution_id"] == str(lease.worker_id)
        assert payload["route_id"] == f"prepare:{lease.launcher_id}"
        assert payload["engine_slot"] == 2
        assert expected_generation == lease.session_generation
        sent.set()
        await finish.wait()
        raise ConnectionError("fixture preparation disconnected")

    async def run(_claim):
        assert sent.is_set()
        started.set()

    queued_at = datetime.now(UTC)
    claim = PoolClaim(
        lease.launcher_id, "workflow.execute", {}, lease.worker_id, 1, queued_at,
        claimed={}, cloudflare_revision="approved", tenant_id=lease.owner_id,
        workflow_slot=WorkflowSlotTicket(2, lease.launcher_id, prewarming=True),
        cloudflare_claim_committed_at=queued_at,
    )
    with patch.object(bridge, "request", request), patch.object(worker, "_run_claim", run):
        worker._start(claim)
        preparations = tuple(bridge.results)
        try:
            await asyncio.wait_for(started.wait(), 2)
            assert len(preparations) == 1 and not preparations[0].done()
        finally:
            finish.set()
            await asyncio.gather(*preparations)
    assert bridge.take_workflow_timing(claim.workflow_slot.workflow_route(claim.subject_id)) == {
        "queued_at": queued_at, "gate_eligible_at": queued_at + timedelta(seconds=2), "claim_at": queued_at,
    }


async def test_preparation_reservation_leaves_the_control_send_lock_free() -> None:
    socket = Mock()
    lease = replace(LEASE, declared_slots=1)
    bridge = Bridge(socket, {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, lease)
    bridge.reconciliation_ready.set()
    reserving = asyncio.Event()
    finish = asyncio.Event()
    sent = []

    async def reserve(*_args, **_kwargs):
        reserving.set()
        await finish.wait()

    async def send(frame):
        command = json.loads(frame)["payload"]
        sent.append(command["action"])
        await bridge.receive({"reply": command["id"], "result": {}})

    socket.send_text = send
    with patch.object(bridge, "_reserve_cloudflare_usage", reserve):
        task = asyncio.create_task(bridge.request(
            "prepare_workflow", {"route_id": f"prepare:{lease.launcher_id}", "org_id": None},
            expected_generation=lease.session_generation,
        ))
        try:
            await asyncio.wait_for(reserving.wait(), 2)
            await asyncio.wait_for(bridge.request("ready", {}), 2)
            assert sent == ["ready"]
        finally:
            finish.set()
            await task
    assert sent == ["ready", "prepare_workflow"]


async def test_managed_workflow_retains_send_and_session_stamps_on_its_route() -> None:
    lease = replace(LEASE, declared_slots=1)
    socket = Mock()
    bridge = Bridge(socket, {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, lease)

    database_started = datetime.now(UTC) - timedelta(seconds=30)
    started = Mock()

    async def send(frame):
        command = json.loads(frame)["payload"]
        if command["action"] == "workflow":
            started.assert_called_once_with(database_started)
            payload = command["payload"]
            assert payload["started_at"] == database_started.isoformat(timespec="milliseconds")
            deadline = (database_started + timedelta(seconds=1800)).isoformat(timespec="milliseconds")
            assert payload["workflow_deadline"] == payload["context"]["workflow_deadline"] == deadline
        await bridge.receive({"reply": command["id"], "result": {}})

    socket.send_text = send
    execution_id = str(lease.worker_id)
    route_id = f"workflow:{execution_id}:{lease.launcher_id}"
    with patch.object(bridge, "_reserve_cloudflare_usage", AsyncMock(return_value=database_started)) as reserve:
        await bridge.reconciliation_complete()
        await bridge.request("prepare_workflow", {
            "route_id": f"prepare:{lease.launcher_id}", "execution_id": execution_id, "org_id": None,
        })
        await bridge.request("workflow", {
            "route_id": route_id, "execution_id": execution_id, "engine_slot": 0, "active_execution": {},
            "timeout_seconds": 1800,
            "context": {"cloudflare_workspace": {"cpu_budget_ms": 1, "subrequest_allowance": 1}},
        }, workflow_started=started)

    assert reserve.call_args.kwargs["timeout_seconds"] == 1800
    stages = bridge.take_workflow_timing(route_id)
    assert set(stages) == {"session_ready_at", "prepare_sent_at", "dispatch_sent_at"}
    assert stages["session_ready_at"] <= stages["prepare_sent_at"] <= stages["dispatch_sent_at"]
    assert bridge.take_workflow_timing(route_id) == {}
    assert not bridge.results


async def test_preparation_lock_timeout_preserves_session_with_a_queued_waiter() -> None:
    lease = replace(LEASE, declared_slots=1)
    socket = Mock(send_text=AsyncMock(), close=AsyncMock())
    bridge = Bridge(socket, {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, lease)
    bridge.reconciliation_ready.set()
    queued = asyncio.Event()
    finish = asyncio.Event()

    async def other_command():
        queued.set()
        async with bridge.lock:
            await finish.wait()

    async with bridge.lock:
        waiter = asyncio.create_task(other_command())
        await queued.wait()
    # Release wakes the earlier command but it has yet to resume and claim the lock.
    assert not bridge.lock.locked()
    try:
        with (
            patch.object(bridge, "_reserve_cloudflare_usage", AsyncMock()),
            patch("src.worker.remote.host.CLOUDFLARE_PREPARE_IO_SECONDS", 0.01),
        ):
            assert await bridge.request(
                "prepare_workflow", {"route_id": f"prepare:{lease.worker_id}", "org_id": None},
                expected_generation=lease.session_generation,
            ) == {}
        assert bridge.connected and bridge.reconciliation_ready.is_set()
        assert bridge.socket is socket
        socket.send_text.assert_not_awaited()
        socket.close.assert_not_awaited()
    finally:
        finish.set()
        await waiter


async def test_preparation_rechecks_generation_after_usage_reservation() -> None:
    lease = replace(LEASE, declared_slots=1)
    socket = Mock(send_text=AsyncMock())
    bridge = Bridge(socket, {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, lease)
    bridge.reconciliation_ready.set()

    booking = PreparationBooking(lease.launcher_id, ((datetime.now(UTC), 20),), 100)
    route_id = f"prepare:{lease.worker_id}"

    async def reserve(*_args, preparation_bookings, **_kwargs):
        preparation_bookings.append(booking)
        bridge.lease = replace(lease, session_generation=lease.session_generation + 1)

    @asynccontextmanager
    async def database():
        yield session

    session = object()
    with (
        patch.object(bridge, "_reserve_cloudflare_usage", reserve),
        patch("src.worker.remote.host.get_db_context", database),
        patch("src.worker.remote.host.settle_unsent_preparation", AsyncMock()) as settle,
        pytest.raises(CredentialRouteChanged),
    ):
        await bridge.request(
            "prepare_workflow", {"route_id": route_id, "org_id": None},
            expected_generation=lease.session_generation,
        )
    settle.assert_awaited_once_with(session, lease.worker_id, route_id, booking)
    socket.send_text.assert_not_awaited()
    assert bridge.connected


@pytest.mark.parametrize("refusal", ["generation", "inventory", "heartbeat", "meter"])
async def test_preparation_refusal_precedes_reservation(refusal: str) -> None:
    lease = replace(LEASE, declared_slots=1)
    bridge = Bridge(Mock(), {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, lease)
    if refusal != "inventory":
        bridge.reconciliation_ready.set()
    if refusal == "heartbeat":
        bridge.heartbeat_at = time.monotonic() - 100
    bridge.meter_exhausted = refusal == "meter"
    generation = lease.session_generation + int(refusal == "generation")
    reserve = AsyncMock()
    with patch.object(bridge, "_reserve_cloudflare_usage", reserve), pytest.raises(CredentialRouteChanged):
        await bridge.request(
            "prepare_workflow", {"route_id": f"prepare:{lease.worker_id}", "org_id": None},
            expected_generation=generation,
        )
    reserve.assert_not_awaited()


async def test_queue_preparation_deadline_precedes_booking() -> None:
    bridge = Bridge(Mock(), {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, LEASE)
    bridge.reconciliation_ready.set()
    booking = AsyncMock()
    with patch.object(bridge, "_reserve_cloudflare_usage", booking):
        assert await bridge.request("prepare_workflow", {
            "route_id": f"prepare:{LEASE.launcher_id}", "org_id": None,
            "prepare_by": 0, "warm_until": 0,
        }) == {}
    booking.assert_not_awaited()


async def test_expired_queue_preparation_cannot_authorize_loader_entry() -> None:
    lease = replace(LEASE, declared_slots=4)
    bridge = Bridge(Mock(), {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, lease)
    bridge.reconciliation_ready.set()
    route_id = f"prepare:{lease.launcher_id}"
    bridge._preparations[route_id] = (lease.session_generation, datetime.now(UTC))

    with patch("src.worker.remote.host.get_db_context") as database:
        await bridge.receive({
            "event": "cloudflare_generation_request", "route_id": route_id, "request_id": "a" * 43,
            "identity": f"tenant-eng-tenant-py-{'a' * 32}-{'b' * 16}-cpu30000-sr40-g0",
        })
        database.assert_not_called()
    assert not bridge.results


async def test_preparation_send_oserror_keeps_the_booking() -> None:
    socket = Mock(send_text=AsyncMock(side_effect=OSError("fixture transport failure")))
    bridge = Bridge(socket, {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, LEASE)
    bridge.reconciliation_ready.set()
    now = datetime.now(UTC)
    booking = PreparationBooking(LEASE.worker_id, ((now, 90),), 100)

    async def reserve(*_args, preparation_bookings, **_kwargs):
        preparation_bookings.append(booking)

    with (
        patch.object(bridge, "_reserve_cloudflare_usage", reserve),
        patch("src.worker.remote.host.settle_unsent_preparation", AsyncMock()) as settle,
    ):
        assert await bridge.request("prepare_workflow", {
            "route_id": f"prepare:{LEASE.launcher_id}", "org_id": None,
            "prepare_by": int(now.timestamp() * 1000) + 20_000,
            "warm_until": int(now.timestamp() * 1000) + 90_000,
        }) == {}
    socket.send_text.assert_awaited_once()
    settle.assert_not_awaited()


async def test_preparation_send_timeout_leaves_live_control_session_usable() -> None:
    lease = replace(LEASE, declared_slots=1)
    socket = Mock(close=AsyncMock())
    bridge = Bridge(socket, {
        "hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, lease)
    bridge.reconciliation_ready.set()
    sequences = []

    async def send(frame):
        message = json.loads(frame)
        sequences.append(message["sequence"])
        command = message["payload"]
        if command["action"] == "prepare_workflow":
            await asyncio.Event().wait()
        await bridge.receive({"reply": command["id"], "result": {}})

    socket.send_text = send
    with (
        patch.object(bridge, "_reserve_cloudflare_usage", AsyncMock()),
        patch("src.worker.remote.host.CLOUDFLARE_PREPARE_IO_SECONDS", 0.01),
    ):
        await bridge.request("prepare_workflow", {"route_id": f"prepare:{lease.worker_id}", "org_id": None},
                             expected_generation=lease.session_generation)
        await bridge.request("ready", {})
    assert bridge.connected and sequences == [1, 2]
    socket.close.assert_not_awaited()


@pytest.mark.parametrize(("kind", "route"), [
    ("result", "test-route"), ("result", None), ("summary_result", "test-route"),
    ("cloudflare_agent_result", "test-route"),
])
@pytest.mark.parametrize("state", ["uncertain", "stopped"])
async def test_terminal_receipts_in_all_cloudflare_domains_are_checked_before_ack(
    kind: str, route: str | None, state: str,
) -> None:
    bridge = Bridge(Mock(), {
        "hostname": "runner", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE, "managed_cloudflare": True,
    }, LEASE)
    bridge.lease_ref = LeaseRef(LEASE)
    deliver = AsyncMock()
    bridge.pool = SimpleNamespace(handle_event=deliver)
    bridge.summaries = SimpleNamespace(handle_result=deliver)
    bridge.autonomous = SimpleNamespace(handle_result=deliver)
    event = {"event": kind, "_event_id": "terminal", "attempt_state": state, "route_id": route,
             "usage": {"sqlite_rows_written": 100, "log_events": 2, "do_requests": 3, "exhausted": True}}

    @asynccontextmanager
    async def context():
        yield object()

    calls = Mock()
    calls.attach_mock(deliver, "deliver")
    calls.attach_mock(AsyncMock(), "meter")
    calls.attach_mock(AsyncMock(), "ack")
    with (
        patch.object(bridge, "acknowledge", calls.ack),
        patch("src.worker.remote.host.get_db_context", context),
        patch("src.worker.remote.host.fence_remote_owner", AsyncMock(return_value=True)),
        patch("src.worker.remote.host.record_launcher_terminal_meter", calls.meter),
    ):
        assert await bridge._deliver_terminal(event)
        await asyncio.gather(*bridge.results)
    assert [call[0] for call in calls.mock_calls] == ["deliver", "meter", "ack"]
    assert calls.meter.await_args.args[1:] == (LEASE.worker_id, event["usage"])
    assert calls.meter.await_args.kwargs == {
        "uncertain_route": route if kind == "result" else None,
        "incarnation_uncertain": kind != "result" or route is None,
    }


async def test_adopted_autonomous_route_reserves_memory_and_one_physical_slot() -> None:
    lease = replace(LEASE, declared_slots=2)
    bridge = Bridge(Mock(), {"hostname": "runner", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE}, lease)
    autonomous = bridge.autonomous = CloudflareAgentRoutes(bridge, LEASE.worker_id, "https://model.example.test")
    worker = ApiExecutor(bridge, LEASE.worker_id)
    admission = worker.admission = Admission(
        budget_bytes=2 * AGENT_RUN_REQUEST_BYTES, under_pressure=lambda: False,
        running_workflow_ids=set, workflow_growth=dict,
    )
    route = CloudflareAgentRoute(LEASE.owner_id, LEASE.worker_id, datetime.now(UTC), asyncio.Event(), "dispatch")
    autonomous.routes[route.id] = route
    bridge._opaque_routes[route.id] = 1

    async def supervise(_route):
        await route.finished.wait()

    with patch.object(autonomous, "supervise", side_effect=supervise):
        worker._wait_for_adopted_autonomous_run(autonomous, route)
        tasks = list(worker._tasks["agent.run"])
        assert admission.free_bytes() == AGENT_RUN_REQUEST_BYTES
        assert bridge.opaque_slots_used(admission) == 1
        route.finished.set()
        await asyncio.gather(*tasks)

    assert admission.free_bytes() == 2 * AGENT_RUN_REQUEST_BYTES
    assert bridge.opaque_slots_used(admission) == 1
    bridge._opaque_routes.clear()
    assert bridge.opaque_slots_used(admission) == 0


@pytest.mark.asyncio
async def test_external_fast_path_hello_is_refused_before_admission() -> None:
    hello = Hello(
        launcher_id=LEASE.launcher_id, incarnation_id=LEASE.worker_id,
        job_uid=None, pod_uid=None, hostname="external-fast", form="external",
        issued_at=int(time.time()), nonce="n" * 32, hello_sequence=1,
        resource_uid="external-capacity-1", execution_lane="fast_path",
    )
    socket = AsyncMock(scope={})
    socket.receive_text.return_value = json.dumps({
        "event": "hello", "hello": sign_hello("test-control-key", hello).to_dict(),
        "sample": {"current": 1, "limit": 100},
    })
    with (
        patch("src.worker.remote.host.get_settings", return_value=SimpleNamespace(secret_key="test-control-key")),
        patch("src.worker.remote.host.admit_external_worker", new=AsyncMock()) as admit,
    ):
        with pytest.raises(AuthenticationError, match="only support the Normal lane"):
            await RemoteControlHost().connect(socket)
    admit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope", "uid", "profile", "use_derived_key", "accepted", "requirements_fail"),
    [
        ("containers", "resource-1", LINUX_RUNTIME_PROFILE, True, True, False),
        ("containers", "resource-1", LINUX_RUNTIME_PROFILE, True, False, True),
        ("containers", "resource-1", LINUX_RUNTIME_PROFILE, False, False, False),
        ("containers", "resource-2", LINUX_RUNTIME_PROFILE, True, False, False),
        ("containers", "resource-1", None, False, False, False),
        ("vm", "resource-1", None, False, True, False),
        ("vm", "resource-1", LINUX_RUNTIME_PROFILE, True, False, False),
    ],
)
async def test_containers_hello_uses_only_the_reserved_scope_key(
    scope: str, uid: str, profile: str | None, use_derived_key: bool, accepted: bool, requirements_fail: bool,
) -> None:
    token = "containers-capacity-token"
    legacy_key = "legacy-platform-signing-key"
    hello = Hello(
        launcher_id=LEASE.launcher_id, incarnation_id=LEASE.worker_id,
        job_uid=None, pod_uid=None, hostname="external", form="external",
        issued_at=int(time.time()), nonce="n" * 32, hello_sequence=1,
        resource_uid=uid, runtime_profile=profile,
    )
    key = external_worker_control_key(token, LEASE.worker_id, uid) if use_derived_key else legacy_key
    socket = AsyncMock(scope={})
    socket.receive_text.return_value = json.dumps({
        "event": "hello", "hello": sign_hello(key, hello).to_dict(),
        "sample": {"current": 1, "limit": 100}, "version": "test-build",
        "handlers": [["workflow.execute", 1]],
    })
    reservation = SimpleNamespace(
        capacity_scope=scope, capacity_resource_uid="resource-1", runtime_profile=LINUX_RUNTIME_PROFILE,
    )
    db = AsyncMock()
    db.execute.return_value = Mock(one_or_none=Mock(return_value=reservation))

    @asynccontextmanager
    async def database():
        yield db

    settings = SimpleNamespace(
        secret_key=legacy_key, cloudflare_containers_scope="containers",
        cloudflare_containers_token=SecretStr(token),
    )
    # Stop at the ownership boundary so no background session is started.
    with (
        patch("src.worker.remote.host.get_settings", return_value=settings),
        patch("src.worker.remote.host.get_liveness_db_context", database),
        patch("src.worker.remote.host.admit_external_worker", new=AsyncMock(return_value=LEASE.worker_id)) as admit,
        patch("src.worker.remote.host.acquire_remote_owner", new=AsyncMock(return_value=None)) as acquire,
        patch("src.core.requirements_cache.get_requirements", new=AsyncMock(
            return_value=None, side_effect=RuntimeError("requirements unavailable") if requirements_fail else None,
        )),
        pytest.raises(RuntimeError if requirements_fail else AuthenticationError),
    ):
        await RemoteControlHost().connect(socket)
    assert admit.await_count == int(accepted)
    assert acquire.await_count == int(accepted)
    if requirements_fail:
        socket.close.assert_awaited_once_with(code=1011, reason="launcher requirements unavailable")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["bound", "legacy-key", "changed-release", "unknown-reservation", "public-ingress"])
async def test_managed_linux_hello_is_bound_to_its_incarnation_key_and_release(mode) -> None:
    release, derived, platform = "a" * 64, "derived-incarnation-key", "platform-key"
    hello = Hello(
        launcher_id=LEASE.launcher_id, incarnation_id=LEASE.worker_id,
        job_uid=None, pod_uid=None, hostname="container", form="external",
        issued_at=int(time.time()), nonce="n" * 32, hello_sequence=1,
        resource_uid="resource-1", runtime_profile=LINUX_RUNTIME_PROFILE, container_release=release,
    )
    signed = sign_hello(platform if mode == "legacy-key" else derived, hello).to_dict()
    if mode == "changed-release":
        signed["container_release"] = "b" * 64
    scope = {} if mode == "public-ingress" else {"worker_ingress_authenticate": Mock(return_value=True)}
    socket = AsyncMock(scope=scope)
    socket.receive_text.return_value = json.dumps({
        "event": "hello", "hello": signed, "sample": {"current": 1, "limit": 100},
        "version": "test-build", "credential_protocol": 2, "handlers": [["workflow.execute", 1]],
    })
    pin = object()
    db = AsyncMock()
    db.scalar.return_value = None if mode == "unknown-reservation" else pin

    @asynccontextmanager
    async def database():
        yield db

    with (
        patch("src.worker.remote.host.get_settings", return_value=SimpleNamespace(secret_key=platform)),
        patch("src.worker.remote.host.get_liveness_db_context", database),
        patch("src.worker.remote.host.load_managed_control_key", new=AsyncMock(return_value=derived)) as load_key,
        patch("src.worker.remote.host.admit_external_worker", new=AsyncMock(return_value=LEASE.worker_id)) as admit,
        patch("src.worker.remote.host.acquire_remote_owner", new=AsyncMock(return_value=None)),
        patch("src.core.requirements_cache.get_requirements", new=AsyncMock()) as requirements,
        pytest.raises(AuthenticationError),
    ):
        await RemoteControlHost().connect(socket)
    requirements.assert_not_awaited()
    assert admit.await_count == int(mode == "bound")
    if mode == "public-ingress":
        load_key.assert_not_awaited()
        db.scalar.assert_not_awaited()
    if mode == "bound":
        assert admit.await_args.kwargs["container_release"] == release
        load_key.assert_awaited_once_with(db, pin, "resource-1")


@pytest.mark.asyncio
async def test_containers_bootstrap_round_trips_fragmented_requirements() -> None:
    api_socket = AsyncMock()
    lease = replace(LEASE, container_release="a" * 64, runtime_lease_ms=20_000)
    bridge = Bridge(
        api_socket, {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner", "containers": True},
        lease,
    )
    content = "# " + "x" * MAX_FRAME_BYTES
    await RemoteControlHost()._welcome(bridge, content, acquiring_at=time.monotonic())

    frames = [call.args[0] for call in api_socket.send_text.await_args_list]
    assert len(frames) > 2
    launcher_socket = AsyncMock()
    launcher_socket.recv.side_effect = frames
    identity = LauncherIdentity(
        launcher_id=LEASE.launcher_id, incarnation_id=LEASE.worker_id, job_uid=None, pod_uid=None,
        hostname="runner", form="external", resource_uid="resource-1",
    )
    nonce = "n" * 43
    launcher = Launcher(
        launcher_socket, identity=identity, secret="derived-key", containers=True,
        container_release="a" * 64, container_probe_nonce=nonce,
        container_deadline=time.monotonic() + 30,
    )
    with patch("src.worker.remote.launcher.signal.setitimer") as arm_watchdog:
        await launcher.hello({})
    arm_watchdog.assert_called_once()
    assert json.loads(launcher_socket.send.await_args_list[0].args[0])["hello"]["nonce"] == nonce
    assert launcher.container_probe_nonce is None
    assert launcher.requirements_content == content
    assert launcher._last_command_sequence == bridge._sequence == len(frames) - 1
    assert launcher.agent_template is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True], ids=["ttl-sent", "transport-detached"])
async def test_managed_container_renewal_sends_a_bounded_ttl(fails) -> None:
    socket = AsyncMock()
    if fails:
        socket.send_text.side_effect = OSError("closed transport")
    lease = replace(LEASE, container_release="a" * 64)
    bridge = Bridge(
        socket, {"current": 1, "limit": 100, "hostname": "runner", "containers": True, "requirements": {}}, lease,
    )
    session = object.__new__(RemoteSession)
    session.bridge, session.lease, session.owned_until = bridge, lease, time.monotonic() + 20
    session.detach = AsyncMock()
    session.lose_ownership = AsyncMock()
    with (
        patch("src.worker.remote.host.asyncio.sleep", new=AsyncMock(side_effect=[None, asyncio.CancelledError()])),
        patch("src.worker.remote.host.renew_remote_owner", new=AsyncMock(return_value=20_000)) as renew,
    ):
        if fails:
            await session._renew()
        else:
            with pytest.raises(asyncio.CancelledError):
                await session._renew()

    renew.assert_awaited_once_with(lease)
    socket.send_text.assert_awaited_once()
    frame = ControlEnvelope.from_json(socket.send_text.await_args.args[0])
    assert (frame.session_generation, frame.sequence, frame.kind) == (lease.session_generation, 1, "runtime_lease")
    assert 0 < frame.payload["ttl_ms"] <= 17_000
    assert bridge.connected is (not fails)
    assert session.detach.await_count == int(fails)
    assert socket.close.await_count == int(fails)
    session.lose_ownership.assert_not_awaited()


@pytest.mark.asyncio
async def test_idle_drain_race_reaches_the_host_as_a_workflow_handback() -> None:
    socket = AsyncMock()
    identity = LauncherIdentity(
        launcher_id=LEASE.launcher_id, incarnation_id=LEASE.worker_id, job_uid=None, pod_uid=None,
        hostname="runner", form="external", resource_uid="resource-1",
    )
    bridge = Bridge(
        socket, {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner", "containers": True}, LEASE,
    )
    launcher = Launcher(
        AsyncMock(), identity=identity, secret="derived-key", containers=True, container_idle_seconds=120,
    )
    launcher.session_generation = LEASE.session_generation

    async def send(text):
        # The API selected a runner, then its physical idle timer won the race.
        launcher._idle_since -= 121
        await launcher.monitor()
        await launcher.command(ControlEnvelope.from_json(text).payload, generation=LEASE.session_generation)

    async def receive(text):
        await bridge.receive(ControlEnvelope.from_json(text).payload)

    launcher.socket.send.side_effect = receive
    socket.send_text.side_effect = send
    with pytest.raises(MemoryError, match="idle deadline"):
        await bridge.request("workflow", {})
    assert not launcher.children


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["service", "platform_job", "agent_run", "agent_summary"])
async def test_containers_launcher_refuses_non_workflow_commands(action: str) -> None:
    socket = AsyncMock()
    identity = LauncherIdentity(
        launcher_id=LEASE.launcher_id, incarnation_id=LEASE.worker_id, job_uid=None, pod_uid=None,
        hostname="runner", form="external", resource_uid="resource-1",
    )
    launcher = Launcher(socket, identity=identity, secret="derived-key", containers=True)
    launcher.session_generation = 1
    await launcher.command({"id": "request", "action": action, "payload": {}}, generation=1)
    reply = ControlEnvelope.from_json(socket.send.await_args.args[0])
    assert reply.payload["error"] == "Work family is unavailable on Containers"


@pytest.mark.asyncio
async def test_containers_run_omits_background_job_and_service_supervisors() -> None:
    bridge = Bridge(
        AsyncMock(), {
            "current": 1, "limit": 100, "requirements": {}, "hostname": "runner",
            "containers": True, "handlers": (("workflow.execute", 1),),
        },
        LEASE,
    )
    worker = ApiExecutor(bridge, LEASE.worker_id)

    async def start_executor() -> None:
        await PoolWorker._prepare_admission(worker)

    with (
        patch.object(worker, "_initialize_runtime", new=AsyncMock(return_value=True)),
        patch.object(worker, "_start_executor", new=start_executor),
        patch.object(worker, "_on_ready", None),
        patch.object(worker, "_heartbeat_loop", new=AsyncMock()),
        patch.object(worker, "_wake_listener", new=AsyncMock()),
        patch.object(worker, "_package_loop", new=AsyncMock()),
        patch.object(worker, "_claim_loop", new=AsyncMock()),
        patch.object(worker, "_drain", new=AsyncMock()),
        patch.object(worker, "_retire", new=AsyncMock()),
        patch.object(worker, "_platform_job_loop", new=AsyncMock()) as run_jobs,
        patch.object(worker, "_start_service_claim_loop", new=AsyncMock()) as start_services,
        patch("src.worker.pool_main.replica_heartbeat_loop", new=AsyncMock()) as diagnostics,
        patch("src.worker.pool_main.remove_scheduler_replica", new=AsyncMock()),
    ):
        assert await worker.run() == 0
    run_jobs.assert_not_called()
    start_services.assert_not_awaited()
    diagnostics.assert_called_once_with(
        replica_id=str(worker.incarnation_id), runs_platform_jobs=False, stop_event=worker._platform_stop,
    )


@pytest.mark.asyncio
async def test_containers_reconcile_workflow_inventory_without_an_agent_consumer() -> None:
    bridge = Bridge(
        AsyncMock(), {
            "current": 1, "limit": 100, "requirements": {}, "hostname": "runner",
            "containers": True, "handlers": (("workflow.execute", 1),),
        },
        LEASE,
    )
    worker = ApiExecutor(bridge, LEASE.worker_id)
    worker.admission = Mock(spec=Admission)
    snapshot = {"workflow_requests": {"workflow-id": 12}, "agent_runs": {}}
    bridge.pool = SimpleNamespace(reconcile_inventory=AsyncMock(return_value=snapshot))
    bridge.inventory = {"routes": [{"kind": "workflow"}], "terminal_routes": []}
    bridge.inventory_ready.set()
    assert await worker._reconcile_transport_inventory() == snapshot
    await worker._apply_reconciled_snapshot(snapshot)
    worker.admission.hold.assert_called_once_with("workflow-id", 12, workflow=True)
    assert bridge.agents is None


@pytest.mark.asyncio
async def test_admission_stale_telemetry_still_renews_the_connected_owner() -> None:
    bridge = Bridge(
        Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE
    )
    bridge.sample_at = 96
    session = object.__new__(RemoteSession)
    session.bridge, session.lease, session.owned_until = bridge, LEASE, 120
    session.lose_ownership = AsyncMock()

    with (
        patch("src.worker.remote.host.time.monotonic", return_value=100),
        patch("src.worker.remote.host.asyncio.sleep", new=AsyncMock(side_effect=[None, asyncio.CancelledError()])),
        patch("src.worker.remote.host.renew_remote_owner", new=AsyncMock(return_value=True)) as renew,
    ):
        # Claims pause at three seconds, but that is not an expired owner.
        assert bridge.memory()[0] == -1
        with pytest.raises(asyncio.CancelledError):
            await session._renew()

    renew.assert_awaited_once_with(LEASE)
    session.lose_ownership.assert_not_awaited()


@pytest.mark.asyncio
async def test_cloudflare_admission_requires_fresh_liveness_without_usage_samples() -> None:
    clock = SimpleNamespace(now=100)
    with patch("src.worker.remote.host.time.monotonic", side_effect=lambda: clock.now):
        bridge = Bridge(Mock(), {"hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE}, replace(LEASE, declared_slots=1))
        worker = ApiExecutor(bridge, LEASE.worker_id)
        assert worker.admission is not None
        bridge.reconciliation_ready.set()
        clock.now = 104
        with pytest.raises(ProtocolError, match="must send liveness heartbeats"):
            await bridge.receive({"event": "sample", "current": 0, "limit": 2**30})
        async with worker.admission.claiming() as fits:
            assert fits is None
        assert not worker.admission.pressured()

        await bridge.receive({"event": "heartbeat"})
        async with worker.admission.claiming() as fits:
            assert fits == 128 * 2**20


@pytest.mark.asyncio
async def test_opaque_worker_stays_busy_until_launcher_reports_its_slot_empty() -> None:
    socket = Mock(send_text=AsyncMock())
    bridge = Bridge(socket, {"hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE}, replace(LEASE, declared_slots=1))
    worker = ApiExecutor(bridge, LEASE.worker_id)
    bridge.reconciliation_ready.set()

    async def heartbeat(occupied, received):
        await bridge.receive({"event": "heartbeat", "slot_occupied": occupied, "received_sequence": received})

    assert worker.active_runs() == 1
    await heartbeat(False, 0)
    assert worker.active_runs() == 0
    request = asyncio.create_task(bridge.request("workflow", {"route_id": "workflow:execution:attempt"}))
    await asyncio.sleep(0)
    reply_id = next(iter(bridge.pending))
    await bridge.receive({"reply": reply_id, "result": {}})
    await request
    await heartbeat(False, 0)  # Sample predates the workflow send.
    assert worker.active_runs() == 1
    await heartbeat(True, 1)  # The launcher still retains its terminal.
    assert worker.active_runs() == 1
    await heartbeat(False, 2)  # The launcher subsequently processed the ACK.
    assert worker.active_runs() == 0


@pytest.mark.asyncio
async def test_exhausted_cloudflare_launcher_stops_claiming_without_faking_active_runs() -> None:
    socket = Mock(send_text=AsyncMock())
    bridge = Bridge(socket, {"hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE},
                    replace(LEASE, declared_slots=1))
    worker = ApiExecutor(bridge, LEASE.worker_id)
    bridge.reconciliation_ready.set()
    await bridge.receive({"event": "heartbeat", "slot_occupied": False, "received_sequence": 0, "meter_exhausted": True})
    async with worker.admission.claiming() as fits:
        assert fits is None
    assert worker.active_runs() == 0
    with pytest.raises(CredentialRouteChanged, match="workload allowance ended"):
        await bridge.request("workflow", {"route_id": "workflow:fixture:attempt"})
    socket.send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_two_opaque_slots_merge_holds_with_confirmed_physical_routes() -> None:
    bridge = Bridge(
        Mock(send_text=AsyncMock()), {"hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE},
        replace(LEASE, declared_slots=2),
    )
    worker = ApiExecutor(bridge, LEASE.worker_id)
    admission = worker.admission
    assert admission is not None
    assert admission.budget_bytes == 256 * 2**20
    bridge.reconciliation_ready.set()

    async def heartbeat(routes, received):
        await bridge.receive({"event": "heartbeat", "occupied_routes": routes, "received_sequence": received})

    first, second = "workflow:first:attempt", "workflow:second:attempt"
    await bridge.receive({"event": "inventory", "inventory": {
        "version": 1, "workflow_slots": 2, "routes": [], "terminal_routes": [{"route_id": first}],
    }})
    await heartbeat([first], 0)
    async with admission.claiming() as fits:
        assert fits == admission.budget_bytes
        admission.hold("second", 80 * 2**20, workflow=True)
    send = asyncio.create_task(bridge.request("workflow", {"route_id": second}))
    await asyncio.sleep(0)
    await bridge.receive({"reply": next(iter(bridge.pending)), "result": {}})
    await send

    await heartbeat([first], 0)  # The second send is newer than this sample.
    assert worker.active_runs() == 2
    async with admission.claiming() as fits:
        assert fits is None
    await heartbeat([second], 1)
    assert worker.active_runs() == 1
    assert not bridge.opaque_slot_idle()

    admission.release("second")
    assert worker.active_runs() == 1  # The terminal still occupies the slot.
    await heartbeat([], 2)
    assert worker.active_runs() == 0
    assert bridge.opaque_slot_idle()


@pytest.mark.asyncio
@pytest.mark.parametrize("age", [4, 20], ids=["claims-paused", "lease-expired"])
async def test_cloudflare_owner_renewal_uses_heartbeat_age(age) -> None:
    bridge = Bridge(Mock(), {"hostname": "opaque", "runtime_profile": CLOUDFLARE_PYTHON_RUNTIME_PROFILE}, replace(LEASE, declared_slots=1))
    bridge.heartbeat_at = 100 - age
    session = object.__new__(RemoteSession)
    session.bridge, session.lease, session.owned_until = bridge, LEASE, 120
    session.lose_ownership = AsyncMock()

    with (
        patch("src.worker.remote.host.time.monotonic", return_value=100),
        patch("src.worker.remote.host.asyncio.sleep", new=AsyncMock(side_effect=[None, asyncio.CancelledError()])),
        patch("src.worker.remote.host.renew_remote_owner", new=AsyncMock(return_value=True)) as renew,
    ):
        if age == 4:
            with pytest.raises(asyncio.CancelledError):
                await session._renew()
            renew.assert_awaited_once_with(LEASE)
            session.lose_ownership.assert_not_awaited()
        else:
            await session._renew()
            renew.assert_not_awaited()
            session.lose_ownership.assert_awaited_once_with("Remote launcher heartbeat expired")


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [False, True, RuntimeError("stale renewal failed")])
async def test_stale_renewal_cannot_drain_a_reconnected_session(outcome: bool | Exception) -> None:
    bridge = Bridge(
        Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE
    )
    replacement = RemoteOwnerLease(
        worker_id=LEASE.worker_id,
        owner_id=LEASE.owner_id,
        launcher_id=LEASE.launcher_id,
        session_generation=2,
    )
    session = object.__new__(RemoteSession)
    session.bridge, session.lease = bridge, LEASE
    session.owned_until = 133
    session.lose_ownership = AsyncMock()
    renewal_started = asyncio.Event()
    release_renewal = asyncio.Event()
    renewals = 0

    async def renew(lease: RemoteOwnerLease) -> bool:
        nonlocal renewals
        renewals += 1
        if renewals == 1:
            assert lease == LEASE
            renewal_started.set()
            await release_renewal.wait()
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        assert lease == replacement
        raise asyncio.CancelledError()

    async def sleep(_seconds: float) -> None:
        return None

    with (
        patch("src.worker.remote.host.time.monotonic", return_value=100),
        patch("src.worker.remote.host.asyncio.sleep", new=sleep),
        patch("src.worker.remote.host.renew_remote_owner", new=renew),
    ):
        task = asyncio.create_task(session._renew())
        await renewal_started.wait()
        session.lease = replacement
        release_renewal.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    session.lose_ownership.assert_not_awaited()
    assert session.owned_until == 133


@pytest.mark.asyncio
@pytest.mark.parametrize("connected,age", [(False, 4), (True, 20), (False, 20)])
async def test_owner_does_not_renew_disconnected_or_expired_telemetry(connected, age) -> None:
    bridge = Bridge(
        Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE
    )
    bridge.connected, bridge.sample_at = connected, 100 - age
    session = object.__new__(RemoteSession)
    session.bridge, session.lease = bridge, LEASE
    session.lose_ownership = AsyncMock()

    with (
        patch("src.worker.remote.host.time.monotonic", return_value=100),
        patch("src.worker.remote.host.asyncio.sleep", new=AsyncMock(side_effect=[None, asyncio.CancelledError()])),
        patch("src.worker.remote.host.renew_remote_owner", new_callable=AsyncMock) as renew,
    ):
        if age < 20:
            with pytest.raises(asyncio.CancelledError):
                await session._renew()
        else:
            await session._renew()

    renew.assert_not_awaited()
    if age >= 20:
        session.lose_ownership.assert_awaited_once_with("Remote launcher telemetry expired")
    else:
        session.lose_ownership.assert_not_awaited()


@pytest.mark.asyncio
async def test_owner_renewal_retries_before_the_current_lease_expires() -> None:
    bridge = Bridge(
        Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE
    )
    bridge.sample_at = 100
    session = object.__new__(RemoteSession)
    session.bridge, session.lease, session.owned_until = bridge, LEASE, 120
    session.lose_ownership = AsyncMock()
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    with (
        patch("src.worker.remote.host.time.monotonic", return_value=100),
        patch("src.worker.remote.host.asyncio.sleep", new=sleep),
        patch(
            "src.worker.remote.host.renew_remote_owner",
            new=AsyncMock(side_effect=[RuntimeError("database restarted"), True, asyncio.CancelledError()]),
        ) as renew,
    ):
        with pytest.raises(asyncio.CancelledError):
            await session._renew()

    assert sleeps == [10, 2, 10]
    assert session.owned_until == 120
    assert renew.await_count == 3
    session.lose_ownership.assert_not_awaited()


@pytest.mark.asyncio
async def test_owner_renewal_failing_throughout_loses_ownership_only_when_no_retry_fits() -> None:
    bridge = Bridge(
        Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE
    )
    clock = SimpleNamespace(now=100.0)
    bridge.sample_at = 100
    session = object.__new__(RemoteSession)
    session.bridge, session.lease, session.owned_until = bridge, LEASE, 120.0
    lost_at: list[float] = []

    async def lose_ownership(_reason: str) -> None:
        lost_at.append(clock.now)

    async def sleep(seconds: float) -> None:
        clock.now += seconds
        bridge.sample_at = clock.now  # telemetry stays fresh; only the database fails

    session.lose_ownership = lose_ownership
    with (
        patch("src.worker.remote.host.time.monotonic", new=lambda: clock.now),
        patch("src.worker.remote.host.asyncio.sleep", new=sleep),
        patch(
            "src.worker.remote.host.renew_remote_owner",
            new=AsyncMock(side_effect=RuntimeError("database restarted")),
        ) as renew,
    ):
        await session._renew()

    # Attempts at 110, 112, 114, 116 and 118; after 118 a 2 s retry would end at the lease.
    assert renew.await_count == 5
    assert lost_at == [118.0]


@pytest.mark.asyncio
async def test_owner_renewal_failure_without_a_retry_window_loses_ownership() -> None:
    bridge = Bridge(
        Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE
    )
    bridge.sample_at = 100
    session = object.__new__(RemoteSession)
    session.bridge, session.lease, session.owned_until = bridge, LEASE, 102
    session.lose_ownership = AsyncMock()

    with (
        patch("src.worker.remote.host.time.monotonic", return_value=100),
        patch("src.worker.remote.host.asyncio.sleep", new=AsyncMock()),
        patch(
            "src.worker.remote.host.renew_remote_owner",
            new=AsyncMock(side_effect=RuntimeError("database restarted")),
        ),
    ):
        await session._renew()

    session.lose_ownership.assert_awaited_once_with(
        "Remote API owner lease could not be renewed"
    )


@pytest.mark.asyncio
async def test_owner_renewal_hung_past_the_current_lease_loses_ownership() -> None:
    bridge = Bridge(
        Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE
    )
    bridge.sample_at = time.monotonic()
    session = object.__new__(RemoteSession)
    session.bridge, session.lease = bridge, LEASE
    session.owned_until = time.monotonic() + 0.001
    session.lose_ownership = AsyncMock()

    async def hung_renewal(_lease: RemoteOwnerLease) -> bool:
        await asyncio.Future()
        return True

    with (
        patch("src.worker.remote.host.asyncio.sleep", new=AsyncMock()),
        patch("src.worker.remote.host.renew_remote_owner", new=hung_renewal),
    ):
        await session._renew()

    session.lose_ownership.assert_awaited_once_with(
        "Remote API owner lease could not be renewed"
    )


@pytest.mark.asyncio
async def test_owner_renewal_refusal_loses_ownership_immediately() -> None:
    bridge = Bridge(
        Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE
    )
    bridge.sample_at = 100
    session = object.__new__(RemoteSession)
    session.bridge, session.lease, session.owned_until = bridge, LEASE, 120
    session.lose_ownership = AsyncMock()

    with (
        patch("src.worker.remote.host.time.monotonic", return_value=100),
        patch("src.worker.remote.host.asyncio.sleep", new=AsyncMock()),
        patch("src.worker.remote.host.renew_remote_owner", new=AsyncMock(return_value=False)),
    ):
        await session._renew()

    session.lose_ownership.assert_awaited_once_with("Remote API owner lease expired")


@pytest.mark.asyncio
async def test_control_host_startup_does_not_start_a_token_worker_in_the_api() -> None:
    from src.worker.remote import host

    with patch.object(host, "_host", None), patch.object(asyncio, "create_task") as spawn:
        await host.start_remote_control_host(Mock())
        await host.start_remote_control_host(Mock())
        spawn.assert_not_called()
        await host.stop_remote_control_host()


@pytest.mark.asyncio
@pytest.mark.parametrize("reported", ["1.0.0-old", None])
@pytest.mark.parametrize("form", ["job", "static", "external"])
async def test_a_worker_is_admitted_with_the_build_its_launcher_runs(reported, form) -> None:
    socket = Mock(accept=AsyncMock(), send_text=AsyncMock())
    hello = Hello(
        launcher_id=LEASE.launcher_id,
        incarnation_id=LEASE.worker_id,
        job_uid="job" if form == "job" else None,
        pod_uid="pod" if form == "job" else None, hostname="runner-0", form=form,
        issued_at=int(time.time()), nonce="n" * 32, hello_sequence=1,
        resource_uid="capacity-resource" if form == "external" else None,
    )
    initial = {
        "event": "hello", "hello": sign_hello("secret", hello).to_dict(),
        "sample": {"current": 1, "limit": 100}, "requirements": {},
    }
    if reported is not None:
        initial["version"] = reported
    socket.receive_text = AsyncMock(return_value=json.dumps(initial))

    @asynccontextmanager
    async def database():
        yield Mock(
            scalar=AsyncMock(return_value=None),
            execute=AsyncMock(return_value=Mock(one_or_none=Mock(return_value=None))),
        )

    with (
        patch("src.worker.remote.host.get_settings", return_value=Mock(secret_key="secret", memory_pressure_threshold=.85)),
        patch("src.worker.remote.host.admit_worker", new_callable=AsyncMock) as admit,
        patch("src.worker.remote.host.admit_external_worker", new_callable=AsyncMock) as external,
        patch("src.worker.remote.host.register_static_worker", new_callable=AsyncMock) as register,
        patch("src.worker.remote.host.get_liveness_db_context", new=database),
        patch("src.worker.remote.host.acquire_remote_owner", new_callable=AsyncMock, return_value=LEASE),
        patch("src.worker.remote.host.ApiExecutor"),
        patch("src.worker.remote.host.RemoteSession", return_value=Mock(task=Mock())),
    ):
        await RemoteControlHost().connect(socket)

    methods = {"job": admit, "static": register, "external": external}
    registered = methods[form]
    assert registered.await_args.kwargs["version"] == reported
    for other_form, method in methods.items():
        if other_form != form:
            method.assert_not_awaited()
    if form == "external":
        external.assert_awaited_once_with(
            LEASE.worker_id, resource_uid="capacity-resource",
            launcher_id=LEASE.launcher_id, version=reported, hostname="runner-0",
        )


@pytest.mark.asyncio
async def test_remote_connect_starts_one_physical_executor() -> None:
    socket = Mock(accept=AsyncMock(), send_text=AsyncMock())
    hello = Hello(
        launcher_id=LEASE.launcher_id,
        incarnation_id=LEASE.worker_id,
        job_uid="job", pod_uid="pod", hostname="runner-0", form="job",
        issued_at=int(time.time()), nonce="n" * 32, hello_sequence=1,
    )
    socket.receive_text = AsyncMock(return_value=json.dumps({
        "event": "hello", "hello": sign_hello("secret", hello).to_dict(),
        "sample": {"current": 1, "limit": 100}, "requirements": {},
    }))
    session = Mock(worker=Mock(), bridge=Mock(connected=True))
    session.stop = AsyncMock()
    with (
        patch("src.worker.remote.host.get_settings", return_value=Mock(secret_key="secret", memory_pressure_threshold=.85)),
        patch("src.worker.remote.host.admit_worker", new_callable=AsyncMock),
        patch("src.worker.remote.host.acquire_remote_owner", new_callable=AsyncMock, return_value=LEASE) as acquire,
        patch("src.worker.remote.host.ApiExecutor") as executor,
        patch("src.worker.remote.host.RemoteSession", return_value=session),
    ):
        host = RemoteControlHost()
        await host.connect(socket)

        assert executor.call_count == 1
        assert executor.call_args.args[0].runtime_profile == LINUX_RUNTIME_PROFILE
        assert acquire.await_args.kwargs["runtime_profile"] == LINUX_RUNTIME_PROFILE
        await host.stop()


@pytest.mark.asyncio
async def test_connected_session_releases_lease_only_after_worker_drain() -> None:
    bridge = Bridge(Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE)
    drained = asyncio.Event()
    worker = Mock(request_stop=Mock())

    async def run():
        await drained.wait()
        return 0

    worker.run = AsyncMock(side_effect=run)
    with patch("src.worker.remote.host.release_remote_owner", new_callable=AsyncMock) as release:
        session = RemoteSession(bridge, worker, LEASE, owned_until=0.0)
        stopping = asyncio.create_task(session.stop())
        await asyncio.sleep(0)
        assert release.await_count == 0
        drained.set()
        await asyncio.gather(stopping)

    worker.request_stop.assert_called_once_with()
    release.assert_awaited_once_with(LEASE)


@pytest.mark.asyncio
async def test_disconnected_session_detaches_without_waiting_for_remote_work() -> None:
    bridge = Bridge(Mock(), {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE)
    bridge.disconnect()
    never = asyncio.Event()
    worker = Mock(request_stop=Mock(), run=AsyncMock(side_effect=never.wait))

    with patch("src.worker.remote.host.release_remote_owner", new_callable=AsyncMock) as release:
        session = RemoteSession(bridge, worker, LEASE, owned_until=0.0)
        await session.detach()

    worker.request_stop.assert_called_once_with()
    release.assert_awaited_once_with(LEASE)


@pytest.mark.asyncio
async def test_host_shutdown_does_not_stop_a_disconnected_launcher_before_handoff() -> None:
    host = RemoteControlHost()
    session = Mock()
    session.bridge.connected = False
    session.worker.request_stop = Mock()
    session.detach = AsyncMock()
    host.sessions[LEASE.worker_id] = session

    await host.stop()

    session.worker.request_stop.assert_not_called()
    session.detach.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", [LINUX_RUNTIME_PROFILE, CLOUDFLARE_PYTHON_RUNTIME_PROFILE])
async def test_finished_session_releases_its_owner_and_is_removed_from_host(profile) -> None:
    host = RemoteControlHost()
    socket = Mock(close=AsyncMock())
    bootstrap = {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner", "runtime_profile": profile}
    bridge = Bridge(socket, bootstrap, LEASE)
    worker = Mock(request_stop=Mock(), run=AsyncMock(return_value=0))

    with patch("src.worker.remote.host.release_remote_owner", new_callable=AsyncMock) as release:
        session = RemoteSession(bridge, worker, LEASE, owned_until=0.0)
        host.sessions[LEASE.worker_id] = session
        session.task.add_done_callback(
            lambda task: host._session_finished(LEASE.worker_id, session, task)
        )
        reconciliation = asyncio.create_task(asyncio.Event().wait())
        session.reconciliation_tasks.add(reconciliation)
        await session.task
        await asyncio.sleep(0)
        assert session.cleanup_task is not None
        await session.cleanup_task

        assert LEASE.worker_id not in host.sessions
        release.assert_awaited_once_with(LEASE)
        assert reconciliation.cancelled()
        if profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE:
            socket.close.assert_awaited_once_with()
        else:
            socket.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_inventory_reconciliation_does_not_block_its_terminate_reply() -> None:
    socket = Mock(send_text=AsyncMock(), close=AsyncMock())
    bridge = Bridge(
        socket, {"current": 1, "limit": 100, "requirements": {}, "hostname": "runner"}, LEASE
    )
    stopped = asyncio.Event()
    worker = Mock(request_stop=Mock())

    async def run():
        await stopped.wait()
        return 0

    async def reconcile_transport_inventory(*, expected_generation):
        await bridge.request("terminate", {"route_id": "stale-route"})

    worker.run = AsyncMock(side_effect=run)
    worker.reconcile_transport_inventory = AsyncMock(side_effect=reconcile_transport_inventory)
    session = RemoteSession(bridge, worker, LEASE, owned_until=0.0)
    task = session.reconcile_transport_inventory(socket)
    await asyncio.sleep(0)
    request_id = next(iter(bridge.pending))

    await bridge.receive({"reply": request_id, "result": {}})
    await asyncio.gather(task)

    worker.reconcile_transport_inventory.assert_awaited_once_with(expected_generation=1)
    assert socket.close.await_count == 0
    stopped.set()
    await session.detach()


@pytest.mark.asyncio
async def test_reconnect_budget_uses_launcher_baseline_not_active_working_set() -> None:
    bridge = Bridge(
        Mock(), {"current": 600, "limit": 1000, "requirements": {}, "hostname": "runner"}, LEASE
    )
    await bridge.receive({
        "event": "inventory",
        "inventory": {"version": 1, "baseline_bytes": 80, "routes": [], "terminal_routes": []},
    })
    executor = object.__new__(ApiExecutor)
    executor.bridge = bridge
    executor.settings = Mock(memory_pressure_threshold=.8)

    budget = executor._measure_budget()

    assert budget.baseline_bytes == 80
    assert budget.budget_bytes == 720


@pytest.mark.asyncio
async def test_lost_remote_owner_detaches_without_retiring_live_launcher_claims() -> None:
    executor = object.__new__(ApiExecutor)
    executor._lost = True
    executor.bridge = Mock(handoff=False, connected=True, lease=LEASE, request=AsyncMock())

    with (
        patch("src.worker.remote.host.owns_remote_session", new=AsyncMock(return_value=True)),
        patch.object(PoolWorker, "_retire", new_callable=AsyncMock) as retire,
    ):
        await executor._retire()

    retire.assert_not_awaited()
    executor.bridge.request.assert_not_awaited()


@pytest.mark.asyncio
async def test_drained_remote_owner_retires_its_launcher() -> None:
    bridge = Bridge(Mock(), {
        "current": 1, "limit": 100, "requirements": {}, "hostname": "runner",
    }, LEASE)
    executor = ApiExecutor(bridge, LEASE.worker_id)

    with (
        patch("src.worker.remote.host.owns_remote_session", new=AsyncMock(return_value=True)),
        patch.object(PoolWorker, "_retire", new_callable=AsyncMock) as retire,
        patch.object(bridge, "request", new_callable=AsyncMock) as request,
    ):
        await executor._retire()

    retire.assert_awaited_once_with()
    request.assert_awaited_once_with("retired", {})


def test_remote_admission_uses_launcher_pressure_ratio_not_unbounded_usage_ratio() -> None:
    bridge = Bridge(
        Mock(), {
            "current": 10, "limit": 1000, "pressure_ratio": .9,
            "requirements": {}, "hostname": "runner",
        }, LEASE,
    )
    executor = object.__new__(ApiExecutor)
    executor.bridge = bridge
    executor.settings = Mock(memory_pressure_threshold=.85)

    assert executor._under_pressure() is True


@pytest.mark.asyncio
@pytest.mark.parametrize("valid_proof", [False, True], ids=["invalid-proof", "owner-refused"])
async def test_refused_remote_control_handshake_closes_once(valid_proof: bool) -> None:
    """A rejected handshake must not send a second close after connect rejects it."""
    from src.worker.remote import host as host_mod
    from starlette.websockets import WebSocket

    hello = Hello(
        launcher_id=LEASE.launcher_id,
        incarnation_id=LEASE.worker_id,
        job_uid="job", pod_uid="pod", hostname="runner-0", form="job",
        issued_at=int(time.time()), nonce="n" * 32, hello_sequence=1,
    )
    initial = {
        "event": "hello",
        "hello": sign_hello("secret" if valid_proof else "wrong-secret", hello).to_dict(),
        "sample": {"current": 1, "limit": 100},
    }
    send = AsyncMock()
    socket = WebSocket(
        {"type": "websocket", "path": "/api/worker-control"},
        receive=AsyncMock(side_effect=[
            {"type": "websocket.connect"},
            {"type": "websocket.receive", "text": json.dumps(initial)},
        ]),
        send=send,
    )
    host = RemoteControlHost()
    with (
        patch.object(host_mod, "_host", host),
        patch.object(host_mod, "get_settings", return_value=Mock(secret_key="secret")),
        patch.object(host_mod, "admit_worker", new=AsyncMock(return_value=object())),
        patch.object(host_mod, "acquire_remote_owner", new=AsyncMock(return_value=None)),
    ):
        await host_mod.remote_control(socket)

    assert [call.args[0]["type"] for call in send.await_args_list] == [
        "websocket.accept", "websocket.close",
    ]
    assert send.await_args_list[-1].args[0]["code"] == 1008
    assert host.sessions == {}


def test_agent_launches_wait_long_enough_for_a_template_rebuild() -> None:
    assert _reply_timeout("agent_run") > TEMPLATE_START_TIMEOUT_SECONDS
    assert _reply_timeout("terminate") < TEMPLATE_START_TIMEOUT_SECONDS


@pytest.mark.asyncio
async def test_telemetry_skips_the_database_fence_only_while_the_lease_is_known_current() -> None:
    from src.worker.remote import host as host_mod

    session = object.__new__(RemoteSession)
    session.lease = LEASE
    session.owned_until = time.monotonic() + 10
    sample = {"event": "sample", "current": 1, "limit": 100}
    with patch.object(host_mod, "owns_remote_session", new=AsyncMock(return_value=False)) as fence:
        assert await session.may_accept("event", sample) is True
        fence.assert_not_awaited()

        assert await session.may_accept("event", {"event": "result", "route_id": "r"}) is False
        assert await session.may_accept("event", {"event": "inventory"}) is False
        assert fence.await_count == 2

        session.owned_until = time.monotonic() - 1
        assert await session.may_accept("event", sample) is False
        assert fence.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("event", ["inventory", "result", "agent_exit", "job_exit"])
async def test_control_defers_delivery_until_database_ownership_is_confirmed(event: str) -> None:
    from fastapi import WebSocketDisconnect
    from src.worker.remote import host as host_mod

    socket = Mock(receive_text=AsyncMock(side_effect=[
        ControlEnvelope(1, 1, "event", {"event": event}).to_json(),
        WebSocketDisconnect(),
    ]), close=AsyncMock())
    bridge = Bridge(socket, {"current": 1, "limit": 100, "hostname": "runner", "requirements": {}}, LEASE)
    bridge.receive = AsyncMock()
    session = object.__new__(RemoteSession)
    session.bridge, session.lease, session.owned_until = bridge, LEASE, 120
    session.last_sequence = 0
    session.reconcile_transport_inventory = Mock()
    session.lose_ownership = AsyncMock()
    host = Mock(stopping=False, connect=AsyncMock(return_value=(session, FragmentAssembler())))

    async def retry_delay(_seconds: float) -> None:
        bridge.receive.assert_not_awaited()
        socket.close.assert_not_awaited()

    with (
        patch.object(host_mod, "_host", host),
        patch.object(host_mod.time, "monotonic", return_value=100),
        patch.object(host_mod.asyncio, "sleep", new=retry_delay),
        patch.object(host_mod, "owns_remote_session", new=AsyncMock(side_effect=[
            OperationalError("select", {}, Exception("database restarted")), True,
        ])) as fence,
    ):
        await host_mod.remote_control(socket)

    assert fence.await_count == 2
    bridge.receive.assert_awaited_once_with({"event": event})
    socket.close.assert_not_awaited()
    session.lose_ownership.assert_not_awaited()
    assert session.lease == LEASE


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [True, False, "db-error", "socket-only"])
async def test_control_rejects_socket_replaced_during_ownership_check(outcome) -> None:
    from src.worker.remote import host as host_mod

    socket = Mock(receive_text=AsyncMock(return_value=ControlEnvelope(
        1, 1, "event", {"event": "draining"}
    ).to_json()), close=AsyncMock())
    bridge = Bridge(socket, {"current": 1, "limit": 100, "hostname": "runner", "requirements": {}}, LEASE)
    bridge.receive = AsyncMock()
    session = object.__new__(RemoteSession)
    session.bridge, session.lease, session.owned_until = bridge, LEASE, 120
    session.last_sequence = 0
    session.lose_ownership = AsyncMock()
    session.begin_physical_drain = Mock()
    host = Mock(stopping=False, connect=AsyncMock(return_value=(session, FragmentAssembler())))

    async def check(_lease):
        # A real reconnect replaces the shared session while the old check awaits.
        if outcome != "socket-only":
            session.lease = replace(LEASE, session_generation=2)
        session.owned_until = 140
        bridge.replace_socket(Mock(), session.lease)
        if outcome == "db-error":
            raise OperationalError("select", {}, Exception("database restarted"))
        return outcome

    with (
        patch.object(host_mod, "_host", host),
        patch.object(host_mod.time, "monotonic", return_value=100),
        patch.object(host_mod, "owns_remote_session", new=check),
    ):
        await host_mod.remote_control(socket)

    bridge.receive.assert_not_awaited()
    session.begin_physical_drain.assert_not_called()
    session.lose_ownership.assert_not_awaited()
    socket.close.assert_awaited_once_with(code=1008)
    assert bridge.connected


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["expired", "lost", "released", "renewed"])
async def test_message_retry_cannot_extend_or_outlive_its_original_lease(change: str) -> None:
    from src.worker.remote import host as host_mod

    session = object.__new__(RemoteSession)
    session.lease, session.owned_until = LEASE, 102
    session._release_lock, session._lease_released = asyncio.Lock(), False
    clock = SimpleNamespace(now=100.0)

    async def check(_lease):
        if change == "renewed":
            session.owned_until = 200
        elif change == "lost":
            session.owned_until = 0
        elif change == "released":
            await session.release_lease()
        clock.now = 100 if change in {"lost", "released"} else 103
        return True

    with (
        patch.object(host_mod.time, "monotonic", side_effect=lambda: clock.now),
        patch.object(host_mod, "owns_remote_session", new=check),
        patch.object(host_mod, "release_remote_owner", new=AsyncMock()),
    ):
        assert await session.may_accept("event", {"event": "agent_exit"}) is False


@pytest.mark.asyncio
async def test_database_outage_retries_only_until_the_original_deadline() -> None:
    from src.worker.remote import host as host_mod

    session = object.__new__(RemoteSession)
    session.lease, session.owned_until = LEASE, 120
    clock = SimpleNamespace(now=100.0)
    attempts = []

    async def check(_lease):
        attempts.append(clock.now)
        raise OperationalError("select", {}, Exception("database restarted"))

    async def delay(seconds):
        clock.now += seconds

    with (
        patch.object(host_mod.time, "monotonic", side_effect=lambda: clock.now),
        patch.object(host_mod.asyncio, "sleep", new=delay),
        patch.object(host_mod, "owns_remote_session", new=check),
    ):
        assert await session.may_accept("event", {"event": "inventory"}) is False
    assert attempts == list(range(100, 120, 2))


@pytest.mark.asyncio
async def test_programming_errors_in_ownership_checks_are_not_retried() -> None:
    from src.worker.remote import host as host_mod

    session = object.__new__(RemoteSession)
    session.lease, session.owned_until = LEASE, time.monotonic() + 10
    with patch.object(host_mod, "owns_remote_session", new=AsyncMock(side_effect=RuntimeError("bug"))):
        with pytest.raises(RuntimeError, match="bug"):
            await session.may_accept("event", {"event": "inventory"})


@pytest.mark.asyncio
async def test_drain_skips_terminate_when_fence_read_fails() -> None:
    executor = object.__new__(ApiExecutor)
    executor._lost = False
    agents = Mock(routes={"r": Mock()}, terminate_active_runs=AsyncMock())
    executor.bridge = Mock(handoff=False, lease=LEASE, agents=agents)

    with (
        patch(
            "src.worker.remote.host.owns_remote_session",
            new=AsyncMock(side_effect=RuntimeError("database restarted")),
        ),
        patch.object(PoolWorker, "_drain", new_callable=AsyncMock) as drain,
    ):
        await executor._drain()

    drain.assert_awaited_once_with()
    agents.terminate_active_runs.assert_not_awaited()


@pytest.mark.asyncio
async def test_retire_skips_when_fence_read_fails() -> None:
    executor = object.__new__(ApiExecutor)
    executor._lost = False
    executor.bridge = Mock(handoff=False, connected=True, lease=LEASE, request=AsyncMock())

    with (
        patch(
            "src.worker.remote.host.owns_remote_session",
            new=AsyncMock(side_effect=RuntimeError("database restarted")),
        ),
        patch.object(PoolWorker, "_retire", new_callable=AsyncMock) as retire,
    ):
        await executor._retire()

    retire.assert_not_awaited()
    executor.bridge.request.assert_not_awaited()


@pytest.mark.asyncio
async def test_pool_owns_session_fails_closed_on_db_error() -> None:
    from src.worker.remote import ownership as ownership_mod
    from src.worker.remote.fencing import LeaseRef, remote_owner_context
    from src.worker.remote.remote_pool import ThinRemoteProcessPool

    pool = ThinRemoteProcessPool(Mock(), worker_id="test-worker")
    with (
        patch.object(
            ownership_mod, "owns_remote_session",
            new=AsyncMock(side_effect=RuntimeError("database restarted")),
        ),
        remote_owner_context(LeaseRef(LEASE)),
    ):
        assert await pool._owns_session() is False


@pytest.mark.asyncio
async def test_fast_path_hello_requires_lane_aware_handler() -> None:
    hello = Hello(
        launcher_id=LEASE.launcher_id, incarnation_id=LEASE.worker_id,
        job_uid=None, pod_uid=None, hostname="old-fast", form="static",
        issued_at=int(time.time()), nonce="n" * 32, hello_sequence=1, execution_lane="fast_path",
    )
    socket = AsyncMock(scope={})
    socket.receive_text.return_value = json.dumps({
        "event": "hello", "hello": sign_hello("test-control-key", hello).to_dict(),
        "sample": {"current": 1, "limit": 100},
    })
    with patch("src.worker.remote.host.get_settings", return_value=SimpleNamespace(secret_key="test-control-key")):
        with pytest.raises(AuthenticationError, match="version 2"):
            await RemoteControlHost().connect(socket)


@pytest.mark.parametrize("lane", ["normal", "fast_path"])
async def test_each_lane_reconciles_inventory_before_opening_claims(lane) -> None:
    executor = object.__new__(ApiExecutor)
    executor.execution_lane = lane
    executor.runtime_profile = LINUX_RUNTIME_PROFILE
    executor.admission = object()
    executor._admission_prepared = False
    executor.bridge = SimpleNamespace(admission=None, containers=False, reconciliation_complete=AsyncMock())
    executor._reconcile_transport_inventory = AsyncMock(return_value={})
    executor._apply_reconciled_snapshot = AsyncMock()
    with patch.object(PoolWorker, "_start_service_claim_loop", new_callable=AsyncMock) as services:
        await executor._prepare_admission()
    assert services.await_count == (1 if lane == "normal" else 0)
    executor._reconcile_transport_inventory.assert_awaited_once_with()
    executor._apply_reconciled_snapshot.assert_awaited_once_with({})
    assert executor.bridge.admission is executor.admission
    assert executor._admission_prepared
    executor.bridge.reconciliation_complete.assert_awaited_once_with()
    executor._reconcile_transport_inventory.reset_mock()
    await executor.reconcile_transport_inventory()
    executor._reconcile_transport_inventory.assert_awaited_once_with(None)
    assert executor.bridge.reconciliation_complete.await_count == 2


@pytest.mark.asyncio
async def test_an_adopted_agent_precondition_failure_releases_its_memory() -> None:
    bridge = Bridge(Mock(), {"current": 1, "limit": 100, "hostname": "runner", "requirements": {}}, LEASE)
    bridge.agents = Mock()
    worker = ApiExecutor(bridge, LEASE.worker_id)
    worker.admission = Admission(
        budget_bytes=100, under_pressure=lambda: False,
        running_workflow_ids=set, workflow_growth=dict,
    )
    route = AgentRoute(route_id="adopted", run_id=LEASE.owner_id)

    worker._wait_for_adopted_agent_run(route, 40)
    tasks = list(worker._tasks[route.handler])
    assert worker.admission.free_bytes() == 60
    # The scheduled coroutine must keep its cleanup even if its precondition
    # is broken before it begins supervising the adopted child.
    bridge.agents = None
    with pytest.raises(AssertionError):
        await asyncio.gather(*tasks)

    assert worker.admission.free_bytes() == 100
    assert worker._tasks[route.handler] == set()
