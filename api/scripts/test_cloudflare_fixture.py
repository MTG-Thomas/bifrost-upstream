"""Cloudflare substitute for the browser test stack. Never mounted in production."""

import asyncio
import json
import ssl
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import cache
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import SecretStr
from shared.models import WorkerCapacityProfile, WorkerCapacityResource
from src.services import worker_ingress_probes as probes
from src.services.worker_ingress import ALLOW_MARKER, CONTROL_PATH, INGRESS_HEADER, PORT
from src.services.worker_pool import (
    cloudflare_admission,
    cloudflare_policy,
    cloudflare_release,
)
from src.services.worker_pool.cloudflare_provisioning import ResourceObservation
from src.services.worker_pool.cloudflare_release import (
    QUALIFIED_RUNTIME_SHA256,
    load_workers_release,
)
from src.services.worker_pool.cloudflare_workers_rest import (
    CloudflareManagementDenied,
    ConfiguredWorkersScript,
    WorkersRestCleanup,
)
from src.worker.remote.protocol import CLOUDFLARE_DEV_BUDGET_BYTES, ControlEnvelope, Hello, new_nonce, sign_hello
from websockets.asyncio.client import connect

ACCOUNT = "f" * 32
TOKEN = SecretStr("browser-fixture-cloudflare-token")
REFUSED_TOKEN = SecretStr("browser-fixture-cloudflare-refused-token")
HOST = "workers.browser-fixture.test"
ROOT = Path("/tmp/bifrost/solution-repo-fixtures/cloudflare")
CAPACITY = "https://capacity.browser-fixture.test"
EXPORTS = cloudflare_release.WORKER_EXPORTS


def block_cloudflare_network(set_attribute: Callable[..., None] = setattr) -> None:
    """Refuse hosted endpoints at HTTPX's socket transport, even without a provider stub."""
    domains = ("cloudflare.com", "workers.dev", "cfargotunnel.com", "trycloudflare.com")
    send = httpx.HTTPTransport.handle_request
    send_async = httpx.AsyncHTTPTransport.handle_async_request

    def check(request: httpx.Request) -> None:
        host = request.url.host.rstrip(".").lower()
        if any(host == domain or host.endswith("." + domain) for domain in domains):
            raise RuntimeError("Hosted Cloudflare requests are blocked in the test fixture")

    def local_request(transport: httpx.HTTPTransport, request: httpx.Request) -> httpx.Response:
        check(request)
        return send(transport, request)

    async def local_async_request(transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        check(request)
        return await send_async(transport, request)

    set_attribute(httpx.HTTPTransport, "handle_request", local_request)
    set_attribute(httpx.AsyncHTTPTransport, "handle_async_request", local_async_request)


@cache
def _release(*, lifetime_meters: bool = False):
    manifest = {
        "manifest_version": 1, "source_revision": "a" * 40, "runtime_sha256": QUALIFIED_RUNTIME_SHA256,
        "release_version": "1.0.0", "release_sequence": 1,
        "pyodide_version": "314.0.6", "compatibility_date": "2026-10-30", "compatibility_flags": [],
        "capacity_protocol": 2, "credential_protocol": 2, "runtime_profile": "cloudflare-python-v1",
        "capabilities": [["workflow.execute", 1]], "max_slots_per_launcher": 4,
        "meter_bounds": json.loads(Path(cloudflare_release.__file__).with_name("cloudflare_meter_bounds.json").read_bytes()),
        "workers": {},
    }
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        bounds = manifest["meter_bounds"]
        qualification = cloudflare_release.MeterQualification.model_validate(bounds["qualification"])
        cpu = qualification.cpu_limits_ms
        for role, classes in EXPORTS.items():
            module = f"{role}.mjs"
            body = f"export default '{role}';".encode()
            (path / module).write_bytes(body)
            manifest["workers"][role] = {
                "main_module": module,
                "modules": [{"name": module, "bytes": len(body), "sha256": sha256(body).hexdigest(),
                             "gzip_bytes": 64, "mime_type": "application/javascript+module"}],
                "exports": {name: {"type": "durable-object", "storage": "sqlite"} for name in classes},
                "bindings": [
                    {"name": "TENANT_GATEWAY", "type": "service", "script": "gateway"},
                    {"name": "BIFROST_METER_GENERATIONS", "type": "plain_text", "text": "true"},
                    {"name": "BIFROST_METER_RECEIPTS", "type": "plain_text", "text": "true"},
                    {"name": "BIFROST_METER_SDK_REQUESTS", "type": "plain_text", "text": str(qualification.sdk_request_limit)},
                    {"name": "BIFROST_METER_ENGINE_CPU_MS", "type": "plain_text", "text": str(cpu["workflow"])},
                    *({"name": f"BIFROST_METER_{name.upper()}_CPU_MS", "type": "plain_text", "text": str(cpu[name])}
                      for name in ("boot", "warm", "exercise")),
                    {"name": "BIFROST_LAUNCHER_METERS", "type": "plain_text", "text": json.dumps(
                        qualification.launcher_caps, sort_keys=True, separators=(",", ":"),
                    )},
                ] if role == "launcher" else [],
                "cpu_ms": cpu["workflow" if role == "launcher" else role],
                "public_endpoint": role == "capacity", "preview_urls": False,
            }
        if lifetime_meters:
            manifest["workers"]["launcher"]["bindings"].append({
                "name": "BIFROST_LAUNCHER_METERS_PER_SECOND", "type": "plain_text", "text": json.dumps(
                    {name: int(bounds["per_launcher_second"][name]) for name in qualification.launcher_caps},
                    sort_keys=True, separators=(",", ":"),
                ),
            })
        modules = [manifest["workers"][role]["modules"][0] for role in EXPORTS]
        bounds["qualification"]["capacity_module_sha256"] = modules[-1]["sha256"]
        bounds["qualification"]["bundle_sha256"] = sha256("\n".join(
            f'{module["name"]}:{module["sha256"]}' for module in modules
        ).encode()).hexdigest()
        digest = cloudflare_release.meter_bounds_digest(cloudflare_release.MeterBounds.model_validate(bounds))
        # The API and job runner generate this fixture in separate processes
        # with different hash seeds for the Durable Object export sets.
        (path / "manifest.json").write_text(json.dumps(manifest, sort_keys=True))
        with patch.object(cloudflare_release, "QUALIFIED_METER_BOUNDS_SHA256", digest):
            return load_workers_release(path)


class _Cloudflare(WorkersRestCleanup):
    def __init__(self, _http, deployment=None):
        self.deployment = deployment

    async def discovery_inventory(self, candidate, account_id=None):
        assert candidate == TOKEN
        return [{"id": "f" * 32, "name": "Browser Fixture Account"}] if account_id is None else []

    async def _request(self, token, account_id, method, path, **kwargs):
        if token == REFUSED_TOKEN:
            raise CloudflareManagementDenied("The browser fixture refused the replacement token")
        assert (account_id, token) == (ACCOUNT, TOKEN)
        assert method == "GET"
        if path == "":
            return {"result": {"id": account_id}}
        assert path == "entitlements"
        return {"result": [
            {"id": name, "deleted_date": "", "allocation": {"type": "bool", "value": True}}
            for name in ("workers.enabled", "containers.enabled")
        ]}

    async def advisory_workers_requests(self, account_id, api_token, scripts, start, end):
        assert (account_id, api_token) == (ACCOUNT, TOKEN)

    async def observe(self, *, api_token, intent):
        assert (intent.target.account_id, api_token) == (ACCOUNT, TOKEN)
        script = ROOT / intent.target.name
        if not script.exists():
            return None
        provider_id, version, effect_id = script.read_text().splitlines()
        assert effect_id == str(intent.creation_effect_id)
        return ResourceObservation(intent, provider_id, version)

    async def create(self, *, api_token, intent, scope_bearer):
        assert self.deployment is not None and intent.artifact_digest == self.deployment.release.digest
        assert bool(scope_bearer) == intent.target.name.endswith("-capacity")
        assert await self.observe(api_token=api_token, intent=intent) is None
        ROOT.mkdir(exist_ok=True)
        (ROOT / intent.target.name).write_text(f"{uuid4()}\n{uuid4()}\n{intent.creation_effect_id}")

    async def configure_endpoint(self, *, api_token, expected):
        name = expected.intent.target.name
        assert await self.observe(api_token=api_token, intent=expected.intent) == expected
        capacity = name.endswith("-capacity")
        classes = next(classes for role, classes in EXPORTS.items() if name.endswith("-" + role))
        return ConfiguredWorkersScript(CAPACITY if capacity else None, {
            exported: sha256(f"{name}:{exported}".encode()).hexdigest()[:32] for exported in classes
        })

    async def delete(self, *, api_token, expected):
        assert await self.observe(api_token=api_token, intent=expected.intent) == expected
        (ROOT / expected.intent.target.name).unlink()

    async def verify_absent(self, *, api_token, target, namespace_ids=frozenset()):
        assert (target.account_id, api_token) == (ACCOUNT, TOKEN)
        assert not (ROOT / target.name).exists()


def _tls(path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, HOST)])
    now = datetime.now(UTC)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(HOST)]), critical=False)
            .sign(key, hashes.SHA256()))
    cert_path, key_path = path / "tls.pem", path / "tls.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
    ))
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(cert_path, key_path)
    return server, ssl.create_default_context(cafile=str(cert_path))


async def _forward(reader, writer):
    upstream, ingress = await asyncio.open_connection("api", PORT)

    async def pipe(source, destination):
        while chunk := await source.read(65536):
            destination.write(chunk)
            await destination.drain()

    directions = (asyncio.create_task(pipe(reader, ingress)), asyncio.create_task(pipe(upstream, writer)))
    try:
        await asyncio.wait(directions, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for direction in directions:
            direction.cancel()
        await asyncio.gather(*directions, return_exceptions=True)
        ingress.close()
        writer.close()
        await asyncio.gather(ingress.wait_closed(), writer.wait_closed(), return_exceptions=True)


def install_api():
    from src.services.worker_pool import cloudflare_activation
    from src.jobs.platform import cloudflare_capacity

    block_cloudflare_network()
    cloudflare_activation.load_workers_release = _browser_release
    cloudflare_activation.WorkersRestCleanup = _Cloudflare
    cloudflare_capacity.installed_workers_release_digest = _fixture_digest
    cloudflare_capacity.installed_workers_release_order = _fixture_order
    cloudflare_admission.installed_workers_release_digest = _fixture_digest
    cloudflare_policy.installed_workers_release_digest = _fixture_digest
    cloudflare_policy.installed_workers_release_order = _fixture_order
    cloudflare_policy.load_workers_release = _browser_release


def _browser_release():
    return _release(lifetime_meters=True)


def _fixture_digest():
    return _browser_release().digest


def _fixture_order():
    return cloudflare_release.WorkersReleaseOrder("1.0.0", 1)


def install_enable(job_module):
    ingress = {}

    async def probe(installation, *, runtime_canary):
        assert installation.worker_ingress_origin == f"https://{HOST}"
        real_address_probe = probes._probe_address
        with tempfile.TemporaryDirectory() as directory:
            tls, trust = _tls(Path(directory))
            async with await asyncio.start_server(_forward, "127.0.0.1", 0, ssl=tls) as proxy:
                port = proxy.sockets[0].getsockname()[1]
                ingress.update(port=port, trust=trust)

                async def addresses(host):
                    assert host == HOST
                    return ("127.0.0.1",)

                async def address(origin, host, address, *, progress=None):
                    return await real_address_probe(origin, host, address, port, trust=trust, progress=progress)

                with patch.object(probes, "_public_addresses", addresses), patch.object(probes, "_probe_address", address):
                    return await probes.probe_worker_ingress(installation, runtime_canary=runtime_canary)

    class Launcher:
        def __init__(self, endpoint, bearer, *, http, reserve_request):
            assert endpoint == CAPACITY and len(bearer) >= 32
            self.resource = None
            self.task = None

        async def profile(self):
            return WorkerCapacityProfile(
                protocol_version=2, scope="workers", worker_version=_fixture_digest(),
                runtime_profile="cloudflare-python-v1", memory_limit_bytes=4 * CLOUDFLARE_DEV_BUDGET_BYTES,
                max_workers=3, max_lifetime_seconds=3600,
            )

        async def prepare(self, reservation, scope, *, managed_identity, managed_deadline):
            assert managed_deadline > datetime.now(UTC)
            uid, self.key = managed_identity
            self.resource = WorkerCapacityResource(
                scope=scope, worker_id=reservation.worker_id, worker_version=reservation.worker_version,
                resource_uid=uid, state="prepared",
            )
            return self.resource

        async def start(self, worker_id, scope, uid):
            self.resource = self.resource.model_copy(update={"state": "starting"})
            self.task = asyncio.create_task(self._serve(worker_id, uid))
            return self.resource

        async def _serve(self, worker_id, uid):
            port, trust = ingress["port"], ingress["trust"]
            async with connect(f"wss://{HOST}{CONTROL_PATH}", host="127.0.0.1", port=port, ssl=trust, proxy=None) as ws:
                hello = Hello(
                    launcher_id=worker_id, incarnation_id=worker_id, job_uid=None, pod_uid=None, hostname=HOST,
                    form="external", issued_at=int(time.time()), nonce=new_nonce(), hello_sequence=1,
                    resource_uid=uid, runtime_profile="cloudflare-python-v1",
                )
                await ws.send(json.dumps({
                    "event": "hello", "hello": sign_hello(self.key, hello).to_dict(),
                    "credential_protocol": 2, "version": self.resource.worker_version,
                    "handlers": [["workflow.execute", 1]],
                }))
                welcome = json.loads(await ws.recv())
                assert welcome["event"] == "welcome"
                generation = welcome["session_generation"]
                sequence = 0
                async for raw in ws:
                    command = ControlEnvelope.from_json(raw).payload
                    action = command.get("action")
                    if action not in {"ready", "capacity_canary"}:
                        continue
                    if action == "capacity_canary":
                        async with httpx.AsyncClient(verify=trust, trust_env=False) as client:
                            response = await client.get(
                                f"https://127.0.0.1:{port}/api/sdk/capacity-canary/{worker_id}",
                                headers={"Host": HOST, "Authorization": f"Bearer {command['payload']['proof']}",
                                         "X-Bifrost-Canary-Generation": str(generation)},
                                extensions={"sni_hostname": HOST},
                            )
                        assert response.status_code == 204 and response.headers[INGRESS_HEADER.decode()] == ALLOW_MARKER.decode()
                    sequence += 1
                    await ws.send(ControlEnvelope(generation, sequence, "event", {"reply": command["id"], "result": {}}).to_json())

        async def get(self, worker_id, scope):
            return self.resource

        async def remove(self, worker_id, scope, uid):
            assert self.resource.resource_uid == uid
            if self.task is not None:
                if self.task.done():
                    self.task.result()
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)
            self.resource = self.resource.model_copy(update={"state": "deleted"})
            return self.resource

    job_module.load_workers_release = _browser_release
    job_module.WorkersRestProvisioner = _Cloudflare
    job_module.WorkersRestCleanup = _Cloudflare
    job_module.ExternalCapacityClient = Launcher
    job_module.probe_worker_ingress = probe


def install_lifecycle(job_module):
    job_module.WorkersRestCleanup = _Cloudflare
    job_module.installed_workers_release_digest = _fixture_digest
    job_module.installed_workers_release_order = _fixture_order
