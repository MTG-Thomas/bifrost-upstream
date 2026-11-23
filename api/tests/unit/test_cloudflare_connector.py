"""Managed Tunnel configuration and least-privilege token delivery."""

import asyncio
import base64
import json
import sys
from contextlib import asynccontextmanager, suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from cryptography.fernet import InvalidToken
from src.jobs.platform.kubernetes_client import KubernetesApiError, KubernetesJobClient
from src.services import worker_ingress
from src.services.worker_pool import cloudflare_connector_channel
from src.services.worker_pool.cloudflare_connector_channel import _publish_file
from src.services.worker_pool.cloudflare_provisioning import OwnershipConflict
from src.services.worker_pool.cloudflare_tunnel_rest import connector_secret

from scripts import cloudflare_connector


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [OwnershipConflict, InvalidToken])
async def test_projection_failure_does_not_stop_worker_ingress(monkeypatch, caplog, failure):
    started = asyncio.Event()

    @asynccontextmanager
    async def serve(_app):
        started.set()
        yield

    monkeypatch.setattr(worker_ingress, "project_connector", AsyncMock(side_effect=failure("private fixture value")))
    monkeypatch.setattr(worker_ingress, "_installation_origin", AsyncMock(return_value="https://worker.example.test"))
    monkeypatch.setattr(worker_ingress, "serve_worker_ingress", serve)
    async with worker_ingress.watch_worker_ingress(None):
        await asyncio.wait_for(started.wait(), 3)
    assert "Could not project the Managed Tunnel connector credential" in caplog.text
    assert "private fixture value" not in caplog.text


@pytest.mark.asyncio
async def test_projection_logs_only_failure_transitions(monkeypatch, caplog):
    observed = asyncio.Event()
    sleep = asyncio.sleep
    attempts = iter((True, True, False, True))

    async def project(_previous):
        failed = next(attempts, True)
        if failed:
            raise OwnershipConflict("private fixture value")

    calls = AsyncMock(side_effect=project)

    async def tick(_seconds):
        if calls.await_count >= 4:
            observed.set()
        await sleep(0)

    monkeypatch.setattr(worker_ingress, "project_connector", calls)
    monkeypatch.setattr(worker_ingress, "_installation_origin", AsyncMock(return_value=None))
    monkeypatch.setattr(worker_ingress.asyncio, "sleep", tick)
    with caplog.at_level("INFO"):
        async with worker_ingress.watch_worker_ingress(None):
            await asyncio.wait_for(observed.wait(), 3)
    assert caplog.text.count("Could not project the Managed Tunnel connector credential") == 2
    assert caplog.text.count("Managed Tunnel connector credential projection recovered") == 1
    assert "private fixture value" not in caplog.text


def test_connector_token_is_bound_to_account_and_tunnel():
    secret = base64.b64encode(bytes(32)).decode()
    token = base64.b64encode(json.dumps({"a": "account", "t": "tunnel", "s": secret}).encode()).decode()
    assert connector_secret(token, "account", "tunnel") == secret
    with pytest.raises(OwnershipConflict, match="different connector credential"):
        connector_secret(token, "account", "other")


@pytest.mark.asyncio
async def test_connector_child_gets_only_token_file_and_minimal_environment(monkeypatch, tmp_path):
    path = tmp_path / "token"
    _publish_file(path, "Zml4dHVyZQ==")
    assert path.stat().st_mode & 0o777 == 0o600
    assert cloudflare_connector.token_fingerprint(path)
    monkeypatch.setenv("BIFROST_SECRET_KEY", "fixture-platform-secret")
    monkeypatch.setenv("BIFROST_DATABASE_URL", "fixture-db-url")
    monkeypatch.setenv("BIFROST_WORKER_CAPACITY_TOKEN", "fixture-capacity-token")
    spawn = AsyncMock()
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    await cloudflare_connector.launch(path)
    spawn.assert_awaited_once()
    assert spawn.call_args.args == (
        "/usr/local/bin/cloudflared", "tunnel", "--no-autoupdate", "run", "--token-file", str(path),
    )
    assert spawn.call_args.kwargs["env"] == {"HOME": "/tmp", "PATH": "/usr/bin:/bin"}
    _publish_file(path, "")
    assert cloudflare_connector.token_fingerprint(path) is None


@pytest.mark.asyncio
async def test_kubernetes_projection_refuses_a_foreign_secret(monkeypatch):
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={
            "metadata": {"name": "bifrost-cloudflare-connector", "namespace": "test", "resourceVersion": "13"},
            "type": "Opaque",
        })

    monkeypatch.setattr(KubernetesJobClient, "_read_token", lambda _: "fixture-service-account")
    async with httpx.AsyncClient(base_url="https://kube.example.test", transport=httpx.MockTransport(handler)) as http:
        client = KubernetesJobClient(http_client=http, namespace="test")
        with pytest.raises(KubernetesApiError, match="409"):
            await client.publish_cloudflare_connector("token", fence="fixture")
    assert [(r.method, r.url.path) for r in requests] == [
        ("GET", "/api/v1/namespaces/test/secrets/bifrost-cloudflare-connector"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("mutable", "can_patch"), [(True, True), (True, False), (False, True)])
async def test_kubernetes_preflight_checks_the_exact_mutable_secret(monkeypatch, mutable, can_patch):
    requests = []
    secret_path = "/api/v1/namespaces/test/secrets/bifrost-cloudflare-connector"

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        if request.method == "GET":
            assert request.url.path == secret_path
            return httpx.Response(200, json={
                "metadata": {
                    "name": "bifrost-cloudflare-connector", "namespace": "test", "resourceVersion": "13",
                    "labels": {"app.kubernetes.io/component": "cloudflare-connector"},
                },
                "type": "Opaque",
                "immutable": not mutable,
            })
        assert json.loads(request.content)["spec"]["resourceAttributes"] == {
            "namespace": "test", "verb": "patch", "group": "", "resource": "secrets",
            "name": "bifrost-cloudflare-connector",
        }
        return httpx.Response(201, json={"status": {"allowed": can_patch}})

    monkeypatch.setattr(KubernetesJobClient, "_read_token", lambda _: "fixture-service-account")
    monkeypatch.setattr(cloudflare_connector_channel, "get_settings", lambda: SimpleNamespace(
        cloudflare_tunnel_channel="kustomize",
    ))
    monkeypatch.setattr(cloudflare_connector_channel, "in_cluster_namespace", lambda: "test")
    async with httpx.AsyncClient(base_url="https://kube.example.test", transport=httpx.MockTransport(handler)) as http:
        monkeypatch.setattr(cloudflare_connector_channel, "KubernetesJobClient", lambda *, namespace: (
            KubernetesJobClient(http_client=http, namespace=namespace)
        ))
        assert await cloudflare_connector_channel.connector_channel_ready() is (mutable and can_patch)
    expected = [("GET", secret_path)]
    if mutable:
        expected.append(("POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"))
    assert requests == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("layout", ["compose", "kustomize"])
@pytest.mark.parametrize("present_at_startup", [False, True])
async def test_supervisor_restarts_and_stops_child_on_token_projection(monkeypatch, tmp_path, layout, present_at_startup):
    token = tmp_path / "token"
    launches: asyncio.Queue[asyncio.subprocess.Process] = asyncio.Queue()
    armed = asyncio.Event()
    loop = asyncio.get_running_loop()
    add_reader = loop.add_reader

    def register_projection_reader(fd, callback, *args):
        add_reader(fd, callback, *args)
        if getattr(callback, "__module__", None) == cloudflare_connector.__name__:
            armed.set()

    async def launch(path):
        assert path == token
        process = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(90)")
        await launches.put(process)
        return process

    def project(version, value):
        if layout == "compose":
            _publish_file(token, value)
        else:
            # Kubelet atomically replaces the ..data link; token itself is a stable link.
            target = tmp_path / f"..version-{version}"
            target.mkdir()
            (target / "token").write_text(value)
            temporary = tmp_path / "..data_new"
            temporary.symlink_to(target.name, target_is_directory=True)
            temporary.replace(tmp_path / "..data")

    if layout == "kustomize":
        token.symlink_to("..data/token")
    if present_at_startup:
        project(1, "Zmlyc3Q=")
    monkeypatch.setattr(cloudflare_connector, "launch", launch)
    monkeypatch.setattr(loop, "add_reader", register_projection_reader)
    supervisor = asyncio.create_task(cloudflare_connector.supervise(token))
    try:
        async with asyncio.timeout(5):
            await armed.wait()
            if not present_at_startup:
                assert launches.empty()
                project(1, "Zmlyc3Q=")
            first = await launches.get()
            project(2, "c2Vjb25k")
            second = await launches.get()
            assert first.returncode is not None
            project(3, "")
            await second.wait()
            assert launches.empty()
    finally:
        supervisor.cancel()
        with suppress(asyncio.CancelledError):
            await supervisor
