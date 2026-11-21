"""Managed Tunnel configuration and least-privilege token delivery."""

import asyncio
import base64
import json
import sys
from contextlib import asynccontextmanager, suppress
from unittest.mock import AsyncMock

import httpx
import pytest
from cryptography.fernet import InvalidToken
from pydantic import ValidationError
from shared.models import CloudflareConnectRequest
from src.jobs.platform.kubernetes_client import KubernetesApiError, KubernetesJobClient
from src.services import worker_ingress
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


def connection(**fields):
    return CloudflareConnectRequest.model_validate({
        "action": "connect", "account_id": "a" * 32,
        "worker_ingress_origin": "https://worker.example.test",
        "api_token": "fixture-account-token", "consent": True, "consent_version": "test",
        "policy": {
            "workers": {"launcher_cap": 1, "slots_per_launcher": 2, "max_lifetime_seconds": 300, "work_kinds": ["workflow"]},
            "monthly_limit_usd": "30.00",
        },
        **fields,
    })


def test_only_managed_tunnel_accepts_and_requires_a_zone():
    assert connection().reachability_mode == "existing_https_endpoint"
    assert connection(reachability_mode="managed_tunnel", zone_id="b" * 32).zone_id == "b" * 32
    with pytest.raises(ValidationError, match="DNS zone"):
        connection(zone_id="b" * 32)
    with pytest.raises(ValidationError, match="DNS zone"):
        connection(reachability_mode="managed_tunnel")
    with pytest.raises(ValidationError, match="pattern"):
        connection(reachability_mode="managed_tunnel", zone_id="../zone")


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
@pytest.mark.parametrize("layout", ["compose", "kustomize"])
async def test_supervisor_restarts_and_stops_child_on_token_projection(monkeypatch, tmp_path, layout):
    token = tmp_path / "token"
    launches: asyncio.Queue[asyncio.subprocess.Process] = asyncio.Queue()

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
    monkeypatch.setattr(cloudflare_connector, "launch", launch)
    project(1, "Zmlyc3Q=")
    supervisor = asyncio.create_task(cloudflare_connector.supervise(token))
    try:
        async with asyncio.timeout(5):
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
