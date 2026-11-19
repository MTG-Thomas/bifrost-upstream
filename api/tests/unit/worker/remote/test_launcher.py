"""Physical launcher transport fencing contracts."""

from __future__ import annotations

import asyncio
import json
import os
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.worker.remote.identity import LauncherIdentity
from src.worker.remote.launcher import Launcher, _load_containers_control_secret
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

