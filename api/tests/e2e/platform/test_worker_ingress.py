"""Exercise the real API listeners behind a test-owned TLS terminator."""

import asyncio
import json
import ssl
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from pydantic import SecretStr
from sqlalchemy import delete, select
from src.config import get_settings
from src.models.orm.cloudflare_capacity import (
    CloudflareCredential,
    CloudflareInstallation,
    CloudflareUsageActivity,
    CloudflareUsageIncarnation,
    CloudflareWorkerReservation,
)
from src.models.orm.execution_workers import ExecutionWorker, ExecutionWorkerClaim
from src.services import worker_ingress_probes as probes
from src.services.authorization.tokens import RuntimeSubject, issue_run_refresh_token
from src.services.worker_ingress import ALLOW_MARKER, CONTROL_PATH, INGRESS_HEADER, PORT
from src.services.work_dispatch import WAKE_WORKFLOW
from src.services.worker_pool.cloudflare_credentials import (
    CredentialRef,
    seal_credential,
)
from src.worker.remote.protocol import Hello, new_nonce, sign_hello
from websockets.asyncio.client import connect

from tests.unit.authorization.committed_runs import committed_world

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

_HOST = "worker-ingress.example.test"


def _tls(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, _HOST)])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(_HOST)]), critical=False)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp_path / "tls.pem", tmp_path / "tls.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
    ))
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(cert_path, key_path)
    return server, ssl.create_default_context(cafile=str(cert_path))


async def _relay(downstream, client, api_host, upstream_port):
    upstream, server = await asyncio.open_connection(api_host, upstream_port)

    async def copy(source, target):
        while chunk := await source.read(64 * 1024):
            target.write(chunk)
            await target.drain()

    tasks = [asyncio.create_task(copy(downstream, server)), asyncio.create_task(copy(upstream, client))]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        server.close()
        client.close()
        await asyncio.gather(server.wait_closed(), client.wait_closed(), return_exceptions=True)


async def test_worker_ingress_over_tls(
    async_engine, async_session_factory, e2e_api_url, org1_user, tmp_path, monkeypatch,
):
    origin = f"https://{_HOST}"
    api = urlsplit(e2e_api_url)
    # The staging journey can leave the previous listener shutting down.
    # Start this installation from a confirmed closed port.
    async with asyncio.timeout(8):
        while True:
            try:
                _, listener = await asyncio.open_connection(api.hostname, PORT)
            except ConnectionRefusedError:
                break
            listener.close()
            await listener.wait_closed()
            await asyncio.sleep(0.1)
    installation = CloudflareInstallation(
        id=uuid4(), account_id="a" * 32, worker_ingress_origin=origin, policy={},
        consent_version="existing-https-v1", consent_actor="test", consent_at=datetime.now(UTC),
        desired_revision=1, desired_state="scaled_in", api_token_version=1,
    )
    async with async_session_factory() as db:
        db.add(installation)
        await db.flush()
        db.add(seal_credential(
            CredentialRef(installation.id, installation.account_id, "account_token", "", 1),
            SecretStr("synthetic-ingress-test-token"), actor="test", now=installation.consent_at,
        ))
        await db.commit()
        db.expunge(installation)
    hello = Hello(
        launcher_id=uuid4(), incarnation_id=uuid4(), job_uid=None, pod_uid=None,
        hostname="ingress-e2e", form="static", issued_at=int(time.time()), nonce=new_nonce(), hello_sequence=1,
    )
    cloud_worker_id = uuid4()
    tls, trust = _tls(tmp_path)
    upstream_port = PORT

    async def forward(reader, writer):
        await _relay(reader, writer, api.hostname, upstream_port)

    try:
        async with asyncio.timeout(8), httpx.AsyncClient(timeout=2, trust_env=False) as ready:
            while True:
                try:
                    response = await ready.get(
                        f"http://{api.hostname}:{PORT}/api/sdk/context", headers={"Host": _HOST},
                    )
                    if response.status_code == 401 and response.headers.get(INGRESS_HEADER.decode()) == ALLOW_MARKER.decode():
                        break
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.1)
        async with await asyncio.start_server(forward, "127.0.0.1", 0, ssl=tls) as proxy:
            port = proxy.sockets[0].getsockname()[1]
            probe_address = probes._probe_address

            async def resolve_test_host(host):
                assert host == _HOST
                return ("127.0.0.1",)

            async def use_test_proxy(probed_origin, host, address):
                return await probe_address(probed_origin, host, address, port, trust=trust)

            monkeypatch.setattr(probes, "_public_addresses", resolve_test_host)
            monkeypatch.setattr(probes, "_probe_address", use_test_proxy)
            canary_calls = 0

            async def signed_canary(probed_origin):
                nonlocal canary_calls
                canary_calls += 1
                assert probed_origin == origin
                async with connect(
                    f"wss://{_HOST}{CONTROL_PATH}", host="127.0.0.1", port=port, ssl=trust, proxy=None,
                ) as ws:
                    await ws.send(json.dumps({
                        "event": "hello", "hello": sign_hello(get_settings().secret_key, hello).to_dict(),
                        "sample": {"current": 1, "limit": 1024**3}, "requirements": {}, "handlers": [],
                    }))
                    welcome = json.loads(await asyncio.wait_for(ws.recv(), 5))
                assert welcome["event"] == "welcome"
                async with httpx.AsyncClient(verify=trust, trust_env=False) as client:
                    response = await client.get(
                        f"https://127.0.0.1:{port}/api/sdk/context",
                        headers={**org1_user.headers, "Host": _HOST},
                        extensions={"sni_hostname": _HOST},
                    )
                assert response.status_code == 200, response.text
                return probes.RuntimeCanary(signed_hello=True, scoped_sdk=True)

            result = await probes.probe_worker_ingress(installation, runtime_canary=signed_canary)
            assert result.ready, [route for route in result.routes if not route.passed]
            assert canary_calls == 1
            async with async_session_factory() as db:
                activity = await db.scalar(select(CloudflareUsageActivity).where(
                    CloudflareUsageActivity.installation_id == installation.id,
                ))
                assert activity is None

            async with committed_world(async_engine) as world:
                org = await world.organization()
                user = await world.person(org)
                unpinned = await world.execution(run_user=user, organization=org)
                pinned = await world.execution(run_user=user, organization=org)
                db = world.session
                db.add_all([
                    seal_credential(
                        CredentialRef(installation.id, installation.account_id, "control_root", "", 1),
                        SecretStr("0" * 64), actor="test", now=installation.consent_at,
                    ),
                    ExecutionWorker(
                        id=cloud_worker_id, form="external", state="ready", capacity_scope="workers",
                        runtime_profile="cloudflare-python-v1", memory_limit_bytes=128 * 2**20,
                    ),
                ])
                await db.flush()
                db.add(CloudflareWorkerReservation(
                    worker_id=cloud_worker_id, installation_id=installation.id, scope="workers",
                    deployment_revision=1, control_root_version=1,
                ))
                await db.flush()
                db.add(ExecutionWorkerClaim(worker_id=cloud_worker_id, family=WAKE_WORKFLOW, subject_id=pinned.id))
                await db.commit()

                async with httpx.AsyncClient(
                    base_url=f"https://127.0.0.1:{port}", verify=trust, trust_env=False, headers={"Host": _HOST},
                ) as client:
                    refused = await client.get(
                        "/api/sdk/context", extensions={"sni_hostname": _HOST},
                        headers={"Authorization": "Bearer invalid"},
                    )
                    assert refused.status_code == 401
                    for run, expected_requests in ((unpinned, 0), (pinned, 2)):
                        minted = await client.post(
                            "/auth/refresh", extensions={"sni_hostname": _HOST},
                            json={"refresh_token": issue_run_refresh_token(RuntimeSubject("workflow_execution", run.id))},
                        )
                        assert minted.status_code == 200, minted.text
                        response = await client.get(
                            "/api/sdk/context", extensions={"sni_hostname": _HOST},
                            headers={"Authorization": f"Bearer {minted.json()['access_token']}"},
                        )
                        assert response.status_code == 200, response.text
                        requests = await db.scalar(select(CloudflareUsageActivity.sdk_requests).where(
                            CloudflareUsageActivity.installation_id == installation.id,
                        ))
                        assert requests == expected_requests
            refusals = {route.path: route.status for route in result.routes if route.path in {
                "/auth/login", "/api/packages", "/openapi.json", "/__fixture/v1/chat/completions",
                "/api/auth/login", "/admin/x", "/docs",
            }}
            assert refusals == dict.fromkeys((
                "/auth/login", "/api/packages", "/openapi.json", "/__fixture/v1/chat/completions",
                "/api/auth/login", "/admin/x", "/docs",
            ), 404)

            reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=trust, server_hostname=_HOST)
            try:
                writer.write(b"GET /api/sdk/context HTTP/1.1\r\nHost:")
                await writer.drain()
                assert await asyncio.wait_for(reader.read(), 5) == b""
            finally:
                writer.close()
                await writer.wait_closed()

            wrong_tls = await probe_address(origin, "wrong.example.test", "127.0.0.1", port, trust=trust)
            assert not wrong_tls.tls
            assert wrong_tls.failure.as_result() == {
                "stage": "ingress_http",
                "error_class": "tls_error",
                "address_family": "ipv4",
                "tls_reason": "CERTIFICATE_VERIFY_FAILED",
                "tls_verify_code": 62,
            }
            # Confirm the probe rejects a proxy that accidentally targets the unrestricted API.
            upstream_port = api.port
            misrouted = await probes.probe_worker_ingress(installation, runtime_canary=signed_canary)
            assert misrouted.tls and not misrouted.reachable and not misrouted.ready
            assert canary_calls == 1
    finally:
        async with async_session_factory() as db:
            await db.execute(delete(ExecutionWorker).where(
                ExecutionWorker.id.in_((hello.incarnation_id, cloud_worker_id)),
            ))
            await db.execute(delete(CloudflareUsageIncarnation).where(
                CloudflareUsageIncarnation.installation_id == installation.id,
            ))
            await db.execute(delete(CloudflareCredential).where(
                CloudflareCredential.installation_id == installation.id,
            ))
            await db.execute(delete(CloudflareUsageActivity).where(
                CloudflareUsageActivity.installation_id == installation.id,
            ))
            await db.delete(installation)
            await db.commit()
