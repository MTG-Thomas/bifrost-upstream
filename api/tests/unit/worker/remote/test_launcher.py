"""Physical launcher transport fencing contracts."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from ssl import SSLCertVerificationError
from contextlib import asynccontextmanager
from queue import Empty
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close
from src.services.execution.container_result import InvalidContainerResult
from src.worker.network import worker_network
from src.worker.remote.guest_diagnostics import close_evidence, guest_diagnostic_payload, verify_guest_diagnostic
from src.worker.remote.identity import LauncherIdentity
from src.worker.remote.launcher import (
    Launcher,
    Child,
    _await_container_startup,
    _load_container_lifetime,
    _load_containers_control_secret,
)
from src.worker.remote.protocol import STATIC_INCARNATION_LOST, ControlEnvelope


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", [
    "hello", "bootstrap", "runtime-grant", "requirements", "guest-init", "inventory",
    "canary-command", "sdk-tls", "sdk-witness", "egress-probes", "ack", "ready",
])
async def test_managed_guest_reports_one_signed_fatal_per_incarnation(stage) -> None:
    socket = SimpleNamespace(send=AsyncMock())
    launcher = Launcher(
        socket, identity=LauncherIdentity.from_environment({"HOSTNAME": "fixture"}),
        secret="fixture-control-key", containers=True, container_release="a" * 64,
    )
    launcher.session_generation = 1
    await launcher.note_guest_stage(stage)
    launcher.failed = True
    await launcher.report_guest_fatal(RuntimeError("secret-only-in-the-exception"))
    await launcher.report_guest_fatal(ValueError("second-failure"))

    frames = [ControlEnvelope.from_json(call.args[0]) for call in socket.send.await_args_list]
    assert len(frames) == (1 if stage == "hello" else 2)
    assert all("secret-only-in-the-exception" not in frame.to_json() for frame in frames)
    fatal = frames[-1]
    assert verify_guest_diagnostic(launcher.secret, 1, fatal.sequence, fatal.payload) == {
        "event": "guest_fatal", "stage": stage, "exception_class": "RuntimeError", "reason": "runtime",
    }


def test_guest_diagnostic_keeps_only_the_allowlisted_tls_class() -> None:
    wrapped = ConnectionError("secret in outer error")
    wrapped.__cause__ = SSLCertVerificationError("secret in verification error")
    assert guest_diagnostic_payload("sdk-witness", wrapped) == {
        "event": "guest_fatal", "stage": "sdk-witness",
        "exception_class": "SSLCertVerificationError", "reason": "tls",
    }


def test_relay_fallback_keeps_only_the_close_category() -> None:
    assert close_evidence(1011, "Container relay close fallback 1014") == {
        "close_code": 1011, "close_category": "gateway",
    }
    assert close_evidence(1011, "Container relay close fallback 1014 secret") == {
        "close_code": 1011, "close_category": "internal",
    }


@pytest.mark.asyncio
async def test_managed_guest_fatal_delivery_is_bounded() -> None:
    async def stalled_send(_raw):
        await asyncio.Event().wait()

    launcher = Launcher(
        SimpleNamespace(send=stalled_send), identity=LauncherIdentity.from_environment({"HOSTNAME": "fixture"}),
        secret="fixture-control-key", containers=True, container_release="a" * 64,
    )
    launcher.session_generation = 1
    launcher.failed = True
    async with asyncio.timeout(1):
        await launcher.report_guest_fatal(RuntimeError("guest could not start"))


@pytest.mark.asyncio
async def test_managed_guest_reconnect_and_canary_refusal_preserve_the_fatal_report(monkeypatch) -> None:
    from src.worker.remote import launcher as launcher_module

    dropped = SimpleNamespace(
        send=AsyncMock(), recv=AsyncMock(side_effect=[
            '{"event":"welcome","session_generation":1}', ConnectionError("drop"),
        ]),
    )
    launcher = Launcher(
        dropped, identity=LauncherIdentity.from_environment({"HOSTNAME": "fixture"}),
        secret="fixture-control-key", containers=True, container_release="a" * 64,
        container_probe_nonce="p" * 43, container_deadline=launcher_module.time.monotonic() + 60,
    )
    with pytest.raises(ConnectionError, match="drop"):
        await launcher.hello({})
    reconnect = SimpleNamespace(
        send=AsyncMock(), close=AsyncMock(), recv=AsyncMock(side_effect=[
            '{"event":"welcome","session_generation":2}',
            ControlEnvelope(2, 1, "bootstrap", {"requirements_content": None, "ttl_ms": 1000}).to_json(),
        ]),
    )
    monkeypatch.setattr(signal, "setitimer", Mock())
    await launcher.reconnect(reconnect, {})
    await launcher.command({"id": "probe", "action": "capacity_canary", "payload": {}}, generation=2)

    async def failed_monitor():
        raise RuntimeError("real fatal after reconnect")

    task = launcher.task(failed_monitor())
    launcher._monitor_task = task
    with pytest.raises(RuntimeError, match="real fatal"):
        await task
    assert launcher.failed and launcher._failed_guest_close is not None
    await launcher._failed_guest_close

    dropped_stage = ControlEnvelope.from_json(dropped.send.await_args_list[-1].args[0])
    assert dropped_stage.payload["event"] == "guest_stage" and dropped_stage.payload["stage"] == "bootstrap"
    envelopes = [ControlEnvelope.from_json(call.args[0]) for call in reconnect.send.await_args_list[1:]]
    faults = [frame for frame in envelopes if frame.payload.get("event") == "guest_fatal"]
    fatal, = faults
    assert verify_guest_diagnostic(launcher.secret, 2, fatal.sequence, fatal.payload) == {
        "event": "guest_fatal", "stage": "canary-command", "exception_class": "RuntimeError", "reason": "runtime",
    }
    assert any(frame.payload.get("error") == "Container qualification command failed" for frame in envelopes)
    reconnect.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown", [False, True], ids=["recover", "shutdown-during-cleanup"])
async def test_static_launcher_reconnects_until_the_host_confirms_loss(monkeypatch, shutdown) -> None:
    from src.worker.remote import launcher as launcher_module

    identity = LauncherIdentity.from_environment({"HOSTNAME": "static"})
    hellos = []
    received = []
    stopped = []
    stop_during_cleanup = Mock()
    statuses = iter([None, 1008, STATIC_INCARNATION_LOST, None])

    @asynccontextmanager
    async def connect(*_args):
        code = next(statuses)

        async def send(raw):
            frame = json.loads(raw)
            if frame.get("event") == "hello":
                hellos.append(frame["hello"])

        async def recv():
            if code is not None:
                raise ConnectionClosedError(Close(code, ""), None)
            return json.dumps({"event": "welcome", "session_generation": 1})

        yield SimpleNamespace(send=send, recv=recv)

    async def receive(launcher, _socket, stopping, _drain):
        received.append(launcher)
        if len(received) == 1:
            if shutdown:
                stop_during_cleanup.side_effect = stopping.set
            launcher.pending_terminal["old"] = {"event": "result"}
            raise ConnectionClosedError(Close(1008, ""), None)
        assert stopped == [received[0]]
        assert not launcher.pending_terminal
        launcher.retired.set()

    async def stop(launcher):
        stopped.append(launcher)
        stop_during_cleanup()

    monkeypatch.setenv("BIFROST_SECRET_KEY", "synthetic")
    monkeypatch.setattr(launcher_module, "load_launcher_identity", lambda: identity)
    monkeypatch.setattr(launcher_module, "connect_control", connect)
    monkeypatch.setattr(launcher_module, "_receive_control_commands", receive)
    monkeypatch.setattr(launcher_module, "effective_memory_sample", lambda: (1, 100))
    monkeypatch.setattr(launcher_module, "memory_pressure_ratio", lambda: 0.01)
    monkeypatch.setattr(launcher_module.Launcher, "start", AsyncMock())
    monkeypatch.setattr(launcher_module.Launcher, "stop", stop)
    monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", Mock())

    assert await launcher_module._run_launcher() == 0

    assert len(hellos) == (3 if shutdown else 4)
    assert [hello["incarnation_id"] for hello in hellos[:3]] == [str(identity.incarnation_id)] * 3
    assert [hello["hello_sequence"] for hello in hellos[:3]] == [1, 2, 3]
    if not shutdown:
        assert hellos[-1]["incarnation_id"] != str(identity.incarnation_id)
        assert hellos[-1]["launcher_id"] != str(identity.launcher_id)
        assert hellos[-1]["hello_sequence"] == 1
    assert stopped == received


@pytest.mark.parametrize("name", [
    "BIFROST_SECRET_KEY", "BIFROST_DATABASE_URL_SYNC", "OTHER_DATABASE_URL",
    "REDIS_URL", "BIFROST_REDIS_URL", "S3_TOKEN", "BIFROST_S3_SECRET_KEY",
    "CAPACITY_TOKEN", "BIFROST_WORKER_LAUNCHER_ID", "BIFROST_WORKER_NETWORK_TOKEN",
])
def test_containers_refuse_privileged_image_environment(monkeypatch, name) -> None:
    monkeypatch.setattr(os, "environ", {name: "synthetic", "BIFROST_WORKER_CONTROL_SECRET": "derived"})
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    with pytest.raises(RuntimeError, match="forbidden environment variable"):
        _load_containers_control_secret()


def test_containers_refuse_root_before_reading_the_key(monkeypatch) -> None:
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    with pytest.raises(RuntimeError, match="non-root"):
        _load_containers_control_secret()


@pytest.mark.parametrize(("deadline_ms", "idle", "valid"), [
    ("103000", "120", True), ("100000", "120", False), ("103000", "121", False),
])
def test_managed_container_requires_a_bounded_lifetime(monkeypatch, deadline_ms, idle, valid) -> None:
    monkeypatch.setattr("src.worker.remote.launcher.time.time", lambda: 100)
    monkeypatch.setenv("BIFROST_CONTAINER_DEADLINE_MS", deadline_ms)
    monkeypatch.setenv("BIFROST_CONTAINER_IDLE_SECONDS", idle)
    if valid:
        assert _load_container_lifetime() == (3, 120)
    else:
        with pytest.raises(RuntimeError, match="Invalid managed Container lifetime"):
            _load_container_lifetime()


def test_managed_container_lease_renews_only_up_to_the_absolute_deadline(monkeypatch) -> None:
    identity = LauncherIdentity(
        launcher_id=uuid4(), incarnation_id=uuid4(), job_uid=None, pod_uid=None,
        hostname="runner", form="external", resource_uid="fixture",
    )
    now = 100.0
    monkeypatch.setattr("src.worker.remote.launcher.time.monotonic", lambda: now)
    monkeypatch.setattr("src.worker.remote.launcher.time.time", lambda: 1)
    set_timer = Mock()
    monkeypatch.setattr("src.worker.remote.launcher.signal.setitimer", set_timer)
    launcher = Launcher(
        AsyncMock(), identity=identity, secret="derived", containers=True,
        container_release="a" * 64, container_deadline=125.0,
    )
    launcher.grant_runtime_lease(20_000)
    set_timer.assert_called_once_with(signal.ITIMER_REAL, 20.0)
    now = 110.0
    launcher.grant_runtime_lease(20_000)
    set_timer.assert_called_with(signal.ITIMER_REAL, 15.0)
    now = 125.0
    with pytest.raises(RuntimeError, match="already expired"):
        launcher.grant_runtime_lease(20_000)
    assert set_timer.call_count == 2


@pytest.mark.asyncio
async def test_managed_container_receives_lease_renewal_before_guest_ready(monkeypatch) -> None:
    socket = AsyncMock()
    frame = ControlEnvelope(session_generation=1, sequence=2, kind="runtime_lease", payload={"ttl_ms": 1000})
    pending = asyncio.Event()

    async def receive():
        if socket.recv.await_count == 1:
            return frame.to_json()
        await pending.wait()

    socket.recv.side_effect = receive
    identity = LauncherIdentity(
        launcher_id=uuid4(), incarnation_id=uuid4(), job_uid=None, pod_uid=None,
        hostname="runner", form="external", resource_uid="fixture",
    )
    launcher = Launcher(
        socket, identity=identity, secret="derived", containers=True,
        container_release="a" * 64, container_deadline=float("inf"),
    )
    launcher.session_generation = 1
    launcher._last_command_sequence = 1
    granted = asyncio.Event()

    def set_timer(*_args):
        granted.set()

    async def wait_for_guest():
        await granted.wait()

    monkeypatch.setattr("src.worker.remote.launcher.signal.setitimer", set_timer)
    setup = asyncio.create_task(wait_for_guest())
    await _await_container_startup(launcher, setup)
    assert launcher._last_command_sequence == 2
    assert granted.is_set()


@pytest.fixture
def managed_launcher():
    identity = LauncherIdentity(
        launcher_id=uuid4(), incarnation_id=uuid4(), job_uid=None, pod_uid=None,
        hostname="runner", form="external", resource_uid="fixture",
    )
    guest = SimpleNamespace(
        used=True, exited=False, stop=AsyncMock(return_value=True), close=Mock(),
        take=Mock(side_effect=MemoryError("Container workflow guest is unavailable")),
    )
    launcher = Launcher(
        AsyncMock(), identity=identity, secret="derived", containers=True,
        container_release="a" * 64, container_idle_seconds=120, container_guest=guest,
    )
    launcher.session_generation = 1
    return launcher, guest


def tracked_container_child(launcher):
    child = Child("route", "execution", 7, Mock(), Mock(), 0, 1800)
    child.result.fileno.return_value = 9
    launcher.children[child.route_id] = child
    return child


@pytest.mark.asyncio
async def test_startup_disconnect_fails_closed_before_guest_readiness(managed_launcher) -> None:
    launcher, _ = managed_launcher
    launcher.socket.recv.side_effect = ConnectionError("startup transport closed")
    ready = asyncio.Event()

    async def wait_for_guest():
        await ready.wait()

    setup = asyncio.create_task(wait_for_guest())
    try:
        with pytest.raises(ConnectionError, match="startup transport closed"):
            await _await_container_startup(launcher, setup)
        launcher.socket.send.assert_not_awaited()
    finally:
        setup.cancel()
        await asyncio.gather(setup, return_exceptions=True)


@pytest.mark.asyncio
async def test_container_network_options_are_refused_before_opening_a_tunnel(monkeypatch) -> None:
    monkeypatch.setenv("BIFROST_BAKED_WHEELS_ONLY", "1")
    monkeypatch.setenv("BIFROST_WORKER_NETWORK_TOKEN", "synthetic")
    with pytest.raises(ValueError, match="do not support private worker networking"):
        async with worker_network():
            pytest.fail("Container network scope opened")


@pytest.mark.asyncio
async def test_refused_second_work_offer_preserves_idle_time(managed_launcher, monkeypatch) -> None:
    launcher, guest = managed_launcher
    original_idle = launcher._idle_since
    monkeypatch.setattr(launcher, "check_fork_pressure", Mock())
    await launcher.command({
        "id": "second-offer", "action": "workflow", "payload": {"route_id": "second"},
    }, generation=1)
    guest.take.assert_called_once()
    assert launcher._idle_since == original_idle
    assert ControlEnvelope.from_json(launcher.socket.send.await_args.args[0]).payload["error_type"] == "MemoryError"


@pytest.mark.asyncio
@pytest.mark.parametrize("idle_draining", [True, False], ids=["idle-draining", "no-guest"])
async def test_container_refuses_before_reading_workflow_context(managed_launcher, monkeypatch, idle_draining) -> None:
    launcher, guest = managed_launcher
    launcher._idle_draining = idle_draining
    if not idle_draining:
        launcher.container_guest = None
    monkeypatch.setattr(launcher, "check_fork_pressure", Mock())

    await launcher.command({
        "id": "workflow-offer",
        "action": "workflow",
        "payload": {"route_id": "route", "execution_id": str(uuid4())},
    }, generation=1)

    launcher.socket.send.assert_awaited_once()
    reply = ControlEnvelope.from_json(launcher.socket.send.await_args.args[0]).payload
    assert reply["error_type"] == "MemoryError"
    assert reply["error"] == (
        "Container is draining after its idle deadline"
        if idle_draining else "Container workflow isolation is unavailable"
    )
    guest.take.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["invalid", "mismatched", "pipe-still-open"])
async def test_container_result_rejects_invalid_data_or_a_guest_crash(managed_launcher, monkeypatch, kind) -> None:
    launcher, guest = managed_launcher
    child = tracked_container_child(launcher)
    if kind == "mismatched":
        child.result.get_nowait.return_value = {"execution_id": "another-execution"}
    elif kind == "invalid":
        child.result.get_nowait.side_effect = InvalidContainerResult("invalid frame")
    else:
        child.result.get_nowait.side_effect = Empty
        guest.exited = True
    monkeypatch.setattr(asyncio.get_running_loop(), "remove_reader", Mock(return_value=True))
    terminal = AsyncMock()
    monkeypatch.setattr(launcher, "send_terminal", terminal)
    if kind == "pipe-still-open":
        await launcher.monitor()
    else:
        launcher.result_ready(child.route_id)
        await asyncio.gather(*launcher._tasks)
    assert terminal.await_args.args[0]["result"]["error_type"] == "ProcessCrashError"
    assert not launcher.children
    guest.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_delivered_container_result_wins_over_late_cancel(managed_launcher, monkeypatch) -> None:
    launcher, guest = managed_launcher
    child = tracked_container_child(launcher)
    child.result.get_nowait.return_value = {"execution_id": child.execution_id, "success": True}
    monkeypatch.setattr(asyncio.get_running_loop(), "remove_reader", Mock(return_value=True))
    terminal = AsyncMock()
    monkeypatch.setattr(launcher, "send_terminal", terminal)

    launcher.result_ready(child.route_id)
    assert child.route_id in launcher.children
    await launcher.terminate(child.route_id, "CancelledError", "Cancelled")
    await asyncio.gather(*launcher._tasks)

    assert terminal.await_args.args[0]["result"]["success"] is True
    guest.stop.assert_awaited_once_with(grace=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["finish", "terminate"])
async def test_failed_guest_stop_disarms_reader_and_leaves_terminal_unresolved(managed_launcher, monkeypatch, operation) -> None:
    launcher, guest = managed_launcher
    child = tracked_container_child(launcher)
    remove = Mock()
    monkeypatch.setattr(asyncio.get_running_loop(), "remove_reader", remove)

    async def cannot_stop(**_kwargs):
        remove.assert_called_once_with(9)
        return False

    guest.stop.side_effect = cannot_stop
    if operation == "finish":
        await launcher.finish(child, {})
    else:
        await launcher.terminate(child.route_id, "CancelledError", "Cancelled")
    assert launcher.failed and launcher.retired.is_set()
    assert not launcher.pending_terminal
    sent = [ControlEnvelope.from_json(call.args[0]).payload for call in launcher.socket.send.await_args_list]
    assert [payload.get("event") for payload in sent] == ["guest_fatal"]


@pytest.mark.asyncio
async def test_managed_container_drains_immediately_after_finishing_its_only_work(managed_launcher, monkeypatch) -> None:
    launcher, _ = managed_launcher
    child = tracked_container_child(launcher)
    monkeypatch.setattr(asyncio.get_running_loop(), "remove_reader", Mock())
    await launcher.finish(child, {"execution_id": child.execution_id, "success": True})
    event_id = next(iter(launcher.pending_terminal))
    drain_started = asyncio.Event()

    async def send(text):
        if ControlEnvelope.from_json(text).payload.get("event") == "draining":
            drain_started.set()

    launcher.socket.send.side_effect = send
    draining = asyncio.create_task(launcher.monitor())
    await drain_started.wait()
    assert not launcher.retired.is_set()
    launcher.pending_terminal.pop(event_id)
    await draining
    assert launcher.retired.is_set()


@pytest.mark.asyncio
async def test_managed_container_shutdown_stops_the_guest_and_closes_its_pipes(managed_launcher, monkeypatch) -> None:
    launcher, guest = managed_launcher
    child = tracked_container_child(launcher)
    remove = Mock()
    monkeypatch.setattr(asyncio.get_running_loop(), "remove_reader", remove)
    await launcher.stop()
    remove.assert_called_once_with(9)
    guest.stop.assert_awaited_once()
    guest.close.assert_called_once()
    child.result.close.assert_called_once()
    assert not launcher.children


@pytest.mark.asyncio
async def test_managed_container_drains_after_idle_and_fences_new_work() -> None:
    identity = LauncherIdentity(
        launcher_id=uuid4(), incarnation_id=uuid4(), job_uid=None, pod_uid=None,
        hostname="runner", form="external", resource_uid="fixture",
    )
    socket = AsyncMock()
    launcher = Launcher(
        socket, identity=identity, secret="derived", containers=True,
        container_release="a" * 64, container_idle_seconds=120,
    )
    launcher.session_generation = 1
    launcher.pending_terminal["unacknowledged"] = {}
    now = launcher._idle_since + 120
    assert not launcher._container_idle_is_due(now)
    launcher.pending_terminal.clear()
    assert not launcher._container_idle_is_due(now + 119)
    assert launcher._container_idle_is_due(now + 120)
    launcher._idle_since -= 240
    await launcher.monitor()
    assert launcher.retired.is_set()
    assert ControlEnvelope.from_json(socket.send.await_args_list[0].args[0]).payload == {"event": "draining"}
    await launcher.command({"id": "dispatch", "action": "workflow", "payload": {}}, generation=1)
    refusal = ControlEnvelope.from_json(socket.send.await_args.args[0]).payload
    assert refusal["error_type"] == "MemoryError" and "idle deadline" in refusal["error"]


@pytest.mark.asyncio
async def test_reconnect_blocks_events_until_welcome_fences_the_new_socket() -> None:
    identity = LauncherIdentity(
        launcher_id=uuid4(), incarnation_id=uuid4(), job_uid=None, pod_uid=None,
        hostname="runner", form="static",
    )
    old_socket = AsyncMock()
    new_socket = AsyncMock()
    welcome_pending = asyncio.Event()
    release_welcome = asyncio.Event()

    async def receive_welcome() -> str:
        welcome_pending.set()
        await release_welcome.wait()
        return json.dumps({"event": "welcome", "session_generation": 2})

    new_socket.recv.side_effect = receive_welcome
    launcher = Launcher(old_socket, identity=identity, secret="secret")
    launcher.session_generation = 1

    reconnect = asyncio.create_task(launcher.reconnect(new_socket, {}))
    await welcome_pending.wait()
    sample = asyncio.create_task(launcher.send({"event": "sample"}))
    await asyncio.sleep(0)

    assert new_socket.send.await_count == 1
    release_welcome.set()
    await asyncio.gather(reconnect, sample)

    assert old_socket.send.await_count == 0
    sample_frame = ControlEnvelope.from_json(new_socket.send.await_args_list[1].args[0])
    assert sample_frame.session_generation == 2
    assert sample_frame.sequence == 1
    assert sample_frame.kind == "event"
    assert sample_frame.payload == {"event": "sample"}

