"""Authentication and prepared-row fencing for Cloudflare control hellos."""

import json
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy import delete

from shared.worker_control_auth import external_worker_control_key
from src.config import Settings
from src.models.orm.execution_workers import ExecutionWorker
from src.services.worker_pool import claims
from src.worker.remote import host, ownership
from src.worker.remote.fencing import LeaseRef, fence_remote_owner, remote_owner_context
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    LINUX_RUNTIME_PROFILE,
    AuthenticationError,
    Hello,
    ProtocolError,
    new_nonce,
    sign_hello,
)


PLATFORM_KEY = "platform-key-for-cloudflare-hello-tests"
CAPACITY_TOKEN = "test-capacity-token"
RESOURCE = "prepared-do"
BUILD = "prepared-build"


@pytest.fixture
def hello():
    return Hello(
        launcher_id=uuid4(), incarnation_id=uuid4(), job_uid=None, pod_uid=None,
        hostname="cloudflare-test", form="external", resource_uid=RESOURCE,
        runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, issued_at=int(time.time()),
        nonce=new_nonce(), hello_sequence=1,
    )


def socket_for(hello, *, key=None, **fields):
    control_key = key if key is not None else external_worker_control_key(
        CAPACITY_TOKEN, hello.incarnation_id, hello.resource_uid or RESOURCE,
    )
    frame = {
        "event": "hello", "hello": sign_hello(control_key, hello).to_dict(),
        "handlers": [["workflow.execute", 1]], "credential_protocol": 2, "version": BUILD,
        **fields,
    }
    return Mock(
        accept=AsyncMock(), receive_text=AsyncMock(return_value=json.dumps(frame)),
        send_text=AsyncMock(), close=AsyncMock(),
    )


@pytest.fixture
def enabled_settings(monkeypatch):
    settings = Settings(
        _env_file=None, secret_key=PLATFORM_KEY, worker_capacity_token=CAPACITY_TOKEN,
        cloudflare_worker_hello_enabled=True,
    )
    monkeypatch.setattr(host, "get_settings", lambda: settings)
    return settings


@pytest.fixture
def acquire(monkeypatch):
    operation = AsyncMock()
    monkeypatch.setattr(host, "acquire_remote_owner", operation)
    return operation


@pytest.mark.asyncio
async def test_cloudflare_hello_defaults_off(hello, acquire, monkeypatch):
    monkeypatch.delenv("BIFROST_CLOUDFLARE_WORKER_HELLO_ENABLED", raising=False)
    settings = Settings(_env_file=None, secret_key=PLATFORM_KEY, worker_capacity_token=CAPACITY_TOKEN)
    monkeypatch.setattr(host, "get_settings", lambda: settings)
    socket = socket_for(hello)

    with pytest.raises(ProtocolError, match="disabled"):
        await host.RemoteControlHost().connect(socket)
    acquire.assert_not_awaited()
    socket.close.assert_awaited_once_with(code=1008)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "message"),
    [
        pytest.param(
            {"form": "job", "resource_uid": None, "job_uid": "job", "pod_uid": "pod"},
            "external capacity resource", id="job",
        ),
        pytest.param({"execution_lane": "fast_path"}, "Normal lane", id="fast-path"),
    ],
)
async def test_cloudflare_hello_requires_external_normal(hello, enabled_settings, acquire, changes, message):
    socket = socket_for(replace(hello, **changes))

    with pytest.raises(AuthenticationError, match=message):
        await host.RemoteControlHost().connect(socket)
    acquire.assert_not_awaited()
    socket.close.assert_awaited_once_with(code=1008)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fields", "message"),
    [
        pytest.param({"handlers": [["workflow.execute", 2]]}, "only workflow.execute", id="wrong-handler"),
        pytest.param(
            {"handlers": [["workflow.execute", 1], ["unknown", 1]]},
            "only workflow.execute", id="extra-handler",
        ),
        pytest.param({"credential_protocol": 1}, "credential protocol 2", id="legacy-protocol"),
        pytest.param({"credential_protocol": 3}, "credential protocol 2", id="future-protocol"),
    ],
)
async def test_cloudflare_hello_requires_exact_capabilities(hello, enabled_settings, acquire, fields, message):
    socket = socket_for(hello, **fields)

    with pytest.raises(ProtocolError, match=message):
        await host.RemoteControlHost().connect(socket)
    acquire.assert_not_awaited()
    socket.close.assert_awaited_once_with(code=1008)


@pytest.mark.asyncio
async def test_cloudflare_hello_requires_configured_capacity_token(hello, enabled_settings, acquire):
    enabled_settings.worker_capacity_token = None
    socket = socket_for(hello)

    with pytest.raises(AuthenticationError, match="not configured"):
        await host.RemoteControlHost().connect(socket)
    acquire.assert_not_awaited()
    socket.close.assert_awaited_once_with(code=1008)


@pytest.mark.asyncio
@pytest.mark.parametrize("key", [PLATFORM_KEY, CAPACITY_TOKEN], ids=["platform-secret", "capacity-token"])
async def test_cloudflare_hello_rejects_underived_proof(hello, enabled_settings, acquire, key):
    socket = socket_for(hello, key=key)

    with pytest.raises(AuthenticationError, match="invalid hello proof"):
        await host.RemoteControlHost().connect(socket)
    acquire.assert_not_awaited()
    socket.close.assert_awaited_once_with(code=1008)


@pytest.fixture
async def enrollment(async_session_factory, hello, enabled_settings, monkeypatch):
    @asynccontextmanager
    async def context():
        async with async_session_factory() as db:
            yield db
            await db.commit()

    monkeypatch.setattr(ownership, "get_liveness_db_context", context)
    monkeypatch.setattr(host, "get_liveness_db_context", context)
    monkeypatch.setattr(claims, "get_db_context", context)
    monkeypatch.setattr(host, "ApiExecutor", Mock())
    monkeypatch.setattr(
        host, "RemoteSession",
        lambda bridge, worker, lease, **_:
            Mock(bridge=bridge, task=Mock(), lease=lease, lease_ref=LeaseRef(lease)),
    )
    async with context() as db:
        db.add(ExecutionWorker(
            id=hello.incarnation_id, form="external", state="starting",
            runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE, capacity_resource_uid=RESOURCE,
            version=BUILD, remote_launcher_id=hello.launcher_id,
        ))
    try:
        yield context
    finally:
        async with context() as db:
            await db.execute(delete(ExecutionWorker).where(ExecutionWorker.id == hello.incarnation_id))


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["resource", "build", "launcher", "profile"])
async def test_cloudflare_hello_requires_matching_prepared_row(hello, enrollment, mismatch):
    version = BUILD
    if mismatch == "resource":
        hello = replace(hello, resource_uid="another-do")
    elif mismatch == "build":
        version = "other-build"
    elif mismatch == "launcher":
        hello = replace(hello, launcher_id=uuid4())
    else:
        async with enrollment() as db:
            row = await db.get(ExecutionWorker, hello.incarnation_id)
            row.runtime_profile = LINUX_RUNTIME_PROFILE
    socket = socket_for(hello, version=version)

    with pytest.raises(AuthenticationError, match="ownership was refused"):
        await host.RemoteControlHost().connect(socket)
    socket.close.assert_awaited_once_with(code=1008)
    socket.send_text.assert_not_awaited()
    async with enrollment() as db:
        row = await db.get(ExecutionWorker, hello.incarnation_id)
        assert row.state == "starting"
        assert row.remote_hello_sequence == 0


@pytest.mark.asyncio
async def test_cloudflare_hello_checks_enrollment_on_reconnect_and_takeover(hello, enrollment):
    original = host.RemoteControlHost()
    initial_socket = socket_for(hello)
    session, _ = await original.connect(initial_socket)
    first = session.lease
    assert first.session_generation == 1
    assert first.credential_protocol == 2
    assert session.bridge.handlers == (("workflow.execute", 1),)
    initial_socket.send_text.assert_awaited_once()
    async with enrollment() as db:
        row = await db.get(ExecutionWorker, hello.incarnation_id)
        assert row.state == "ready"
        assert row.ready_at is not None

    reconnect = replace(hello, hello_sequence=2, nonce=new_nonce())
    with pytest.raises(AuthenticationError, match="ownership was refused"):
        await original.connect(socket_for(reconnect, version="other-build"))
    reconnected, _ = await original.connect(socket_for(reconnect))
    assert reconnected is session
    assert reconnected.lease.session_generation == 2

    successor = host.RemoteControlHost()
    takeover = replace(hello, hello_sequence=3, nonce=new_nonce())
    assert await ownership.release_remote_owner(reconnected.lease)
    with pytest.raises(AuthenticationError, match="ownership was refused"):
        await successor.connect(socket_for(replace(takeover, resource_uid="another-do")))
    adopted, _ = await successor.connect(socket_for(takeover))
    assert adopted.lease.owner_id == successor.owner_id
    assert adopted.lease.session_generation == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("already_connected", [False, True], ids=["before-first-hello", "after-first-hello"])
async def test_legacy_linux_hello_cannot_take_cloudflare_enrollment(hello, enrollment, already_connected):
    control_host = host.RemoteControlHost()
    if already_connected:
        await control_host.connect(socket_for(hello))
    linux = replace(hello, runtime_profile=None, hello_sequence=2, nonce=new_nonce())
    socket = socket_for(linux, key=PLATFORM_KEY, sample={"current": 1, "limit": 100})

    with pytest.raises(AuthenticationError, match="prepared capacity resource"):
        await control_host.connect(socket)
    socket.send_text.assert_not_awaited()
    async with enrollment() as db:
        row = await db.get(ExecutionWorker, hello.incarnation_id)
        assert row.runtime_profile == CLOUDFLARE_PYTHON_RUNTIME_PROFILE
        assert row.remote_hello_sequence == int(already_connected)


@pytest.mark.asyncio
async def test_retirement_fences_cloudflare_session_and_reconnect(hello, enrollment):
    original = host.RemoteControlHost()
    session, _ = await original.connect(socket_for(hello))
    assert await ownership.owns_remote_session(session.lease)
    await claims.retire_worker(hello.incarnation_id, runs_handled=0)

    assert not await ownership.owns_remote_session(session.lease)
    assert not await ownership.renew_remote_owner(session.lease)
    with remote_owner_context(session.lease_ref):
        async with enrollment() as db:
            assert not await fence_remote_owner(db, for_update=True)
    reconnect = replace(hello, hello_sequence=2, nonce=new_nonce())
    for control_host in (original, host.RemoteControlHost()):
        socket = socket_for(reconnect)
        with pytest.raises(AuthenticationError, match="ownership was refused"):
            await control_host.connect(socket)
        socket.close.assert_awaited_once_with(code=1008)
        socket.send_text.assert_not_awaited()
