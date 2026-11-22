"""Physical launcher transport fencing contracts."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from src.worker.remote.identity import LauncherIdentity
from src.worker.remote.launcher import (
    Launcher,
    _await_container_startup,
    _load_container_lifetime,
    _load_containers_control_secret,
)
from src.worker.remote.protocol import ControlEnvelope


@pytest.mark.parametrize("name", [
    "BIFROST_SECRET_KEY", "BIFROST_DATABASE_URL_SYNC", "OTHER_DATABASE_URL",
    "REDIS_URL", "BIFROST_REDIS_URL", "S3_TOKEN", "BIFROST_S3_SECRET_KEY",
    "CAPACITY_TOKEN", "BIFROST_WORKER_LAUNCHER_ID",
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

