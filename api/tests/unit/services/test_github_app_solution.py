"""Package policy tests. No live GitHub credentials, network or registration."""

import asyncio
import importlib.util
import json
import sys
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from types import ModuleType
from uuid import UUID, uuid4

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, PublicFormat

ROOT = Path("/app/examples/github-app")


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


package = load("github_app_package", ROOT / "operator/modules/github_app.py")
builder = load("github_app_builder", ROOT / "build.py")


class MemoryStore:
    def __init__(self):
        self.rows, self.secrets = {}, {}

    async def insert(self, key, data):
        if key in self.rows:
            return False
        self.rows[key] = data
        return True

    async def get(self, key):
        return self.rows.get(key)

    async def query(self, **where):
        return sorted([row for row in self.rows.values() if all(row.get(k) == v for k, v in where.items())],
                      key=lambda row: row["order"], reverse=True)

    async def secret(self, key, value=None):
        if value is not None:
            self.secrets[key] = value
        return self.secrets.get(key)


class Clock:
    now = 1800000000.0

    def __call__(self):
        return self.now

    async def sleep(self, duration):
        self.now += duration
        await asyncio.sleep(0)


class FakeGitHub:
    def __init__(self, clock):
        self.app_id = "123"
        self.clock, self.calls = clock, []
        self.repositories = [{"id": 80001, "full_name": "example/one"}]
        self.suspended = None
        self.on_mint: Callable[[], Awaitable[None]] | None = None

    async def call(self, operation, installation=None, **kwargs):
        self.calls.append((operation, installation, kwargs))
        if operation == "installation":
            return {"id": installation, "account": {"id": 90001}, "suspended_at": self.suspended}
        if operation == "mint":
            if self.on_mint:
                await self.on_mint()
            await asyncio.sleep(0)
            return {"token": "synthetic-token", "expires_at": datetime.fromtimestamp(
                self.clock() + 3600, timezone.utc).isoformat()}
        if operation == "repositories":
            return {"repositories": self.repositories}
        if operation == "delivery":
            return {"id": kwargs["delivery"], "installation_id": installation, "guid": str(uuid4())}
        return {"accepted": True}


@pytest.fixture
def broker():
    clock, store, emissions = Clock(), MemoryStore(), []
    github = FakeGitHub(clock)

    async def emit(topic, data, **target):
        emissions.append((topic, data, target))
        return {"event_id": str(uuid4())}

    async def validate_target(organization, solution):
        assert UUID(organization) and UUID(solution)

    client = package.Broker(store, github, emit, validate_target, clock=clock, sleep=clock.sleep)
    return SimpleNamespace(client=client, store=store, clock=clock, github=github, emissions=emissions)


def incoming(kind="pull_request", repo=80001):
    return SimpleNamespace(id=str(uuid4()), type=kind,
        received_at="2027-01-15T08:00:00+00:00", data={"installation": {"id": 70001},
        "repository": {"id": repo}, "action": "opened", "organization_id": str(uuid4())})


async def test_verified_binding_routes_to_stored_destination_and_rejects_reassignment(broker):
    destination, solution = str(uuid4()), str(uuid4())
    await broker.client.bind(70001, destination, solution)
    await broker.client.bind(70002, destination, solution)
    event, guid = incoming(), str(uuid4())
    await broker.client.receive(event, {"x-github-delivery": guid})
    assert broker.emissions[0][2] == {"scope": destination, "solution": solution}
    assert broker.emissions[0][1]["delivery_id"] == guid
    with pytest.raises(package.PackageError, match="already bound"):
        await broker.client.bind(70001, str(uuid4()), solution)


async def test_unbound_or_removed_repository_never_routes(broker):
    with pytest.raises(package.PackageError, match="no verified"):
        await broker.client.receive(incoming(), {"x-github-delivery": str(uuid4())})
    await broker.client.bind(70001, str(uuid4()), str(uuid4()))
    broker.github.repositories = []
    lifecycle = incoming("installation_repositories")
    await broker.client.receive(lifecycle, {"x-github-delivery": str(uuid4())})
    with pytest.raises(package.PackageError, match="outside"):
        await broker.client.receive(incoming(), {"x-github-delivery": str(uuid4())})
    assert broker.emissions == []


async def test_suspended_installation_invalidates_old_inventory(broker):
    await broker.client.bind(70001, str(uuid4()), str(uuid4()))
    old_epoch = await broker.client.epoch("70001")
    broker.github.suspended = "2027-01-15T08:00:00Z"
    with pytest.raises(package.PackageError, match="suspended"):
        await broker.client.reconcile(70001)
    assert await broker.client.epoch("70001") != old_epoch
    with pytest.raises(package.PackageError):
        await broker.client.receive(incoming(), {"x-github-delivery": str(uuid4())})
    assert not broker.emissions


async def test_token_refresh_is_coalesced_and_expiry_requires_new_generation(broker):
    await broker.client.bind(70001, str(uuid4()), str(uuid4()))
    tokens = await asyncio.gather(*(broker.client.token("70001", "initial") for _ in range(5)))
    assert tokens == ["synthetic-token"] * 5
    assert sum(call[0] == "mint" for call in broker.github.calls) == 1
    broker.clock.now += 3550
    await asyncio.gather(*(broker.client.token("70001", "initial") for _ in range(5)))
    assert sum(call[0] == "mint" for call in broker.github.calls) == 2
    assert "synthetic-token" not in json.dumps(broker.store.rows)


async def test_new_fence_prevents_stale_refresh_from_becoming_usable(broker):
    await broker.store.insert("binding:70001", {"kind": "binding", "installation": "70001",
        "organization": str(uuid4()), "solution": str(uuid4()), "account": "90001", "app_id": "123", "order": 0})

    async def invalidate():
        await broker.client.fence("70001", "revoked", broker.clock())

    broker.github.on_mint = invalidate
    with pytest.raises(package.PackageError, match="changed during token"):
        await broker.client.reconcile(70001)
    assert not broker.store.secrets
    assert not await broker.store.query(kind="inventory", installation="70001")


async def test_redelivery_preserves_large_id_and_does_not_repeat_uncertain_intent(broker):
    await broker.client.bind(70001, str(uuid4()), str(uuid4()))
    did = "3852823007498076160"
    result = await broker.client.redeliver(70001, did)
    assert result["delivery_id"] == did
    with pytest.raises(package.PackageError, match="already requested"):
        await broker.client.redeliver(70001, did)
    assert [call[2]["delivery"] for call in broker.github.calls if call[0] == "redeliver"] == [did]


@pytest.mark.parametrize("invalid", [True, -1, 1.5, "1/../settings", "https://example.com", "", "0"])
def test_provider_ids_are_decimal_strings_only(invalid):
    with pytest.raises(package.PackageError):
        package.provider_id(invalid)


async def test_real_http_client_sanitizes_errors_and_persists_quota_cooldown():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode()
    store, clock, requests = MemoryStore(), Clock(), []

    async def respond(request):
        requests.append(request)
        assert request.url.host == "api.github.com"
        return httpx.Response(429, headers={"retry-after": "120", "x-ratelimit-remaining": "0"},
                              json={"message": "synthetic-private-response-body"})

    github = package.GitHub("123", key, store, clock=clock, sleep=clock.sleep,
                            transport=httpx.MockTransport(respond))
    with pytest.raises(package.PackageError, match="HTTP 429") as error:
        await github.call("repositories", "70001", token="synthetic-installation-token")
    assert "synthetic" not in str(error.value)
    with pytest.raises(package.PackageError, match="cooldown"):
        await github.call("repositories", "70001", token="synthetic-installation-token")
    assert len(requests) == 1
    assert "synthetic-installation-token" not in json.dumps(store.rows)
    assert (await store.query(kind="cooldown"))[0]["until"] == clock() + 120


def test_both_zip_definitions_parse_and_exclude_instance_state(tmp_path):
    from src.services.solutions.zip_install import preview_zip
    from zipfile import ZipFile

    first = builder.build(tmp_path)
    previous = [path.read_bytes() for path in first]
    assert previous == [path.read_bytes() for path in builder.build(tmp_path)]
    for kind, path in zip(("operator", "consumer"), first, strict=True):
        preview = preview_zip(path.read_bytes())
        assert preview.slug == f"github-app-{kind}"
        assert preview.tables and preview.workflows and preview.events
        with ZipFile(path) as archive:
            assert not any(name.endswith(".env") or "secret" in name for name in archive.namelist())
        if kind == "operator":
            assert preview.forms and preview.connection_schemas
            ingress = next(event for event in preview.events if event["source_type"] == "webhook")
            assert not ingress["is_active"]
            assert ingress["webhook_config"]["delivery_id_header"] == "X-GitHub-Delivery"
            assert "secret" not in ingress["webhook_config"]


@pytest.mark.parametrize("failure", ["unsupported", "global_key", "missing_override", None])
async def test_setup_doctor_requires_capability_and_operator_only_key_custody(monkeypatch, failure):
    namespace = ModuleType("modules")
    namespace.__path__ = []
    monkeypatch.setitem(sys.modules, "modules", namespace)
    monkeypatch.setitem(sys.modules, "modules.github_app", package)
    operator = load("github_app_operator_test", ROOT / "operator/workflows/operator.py")
    oid = str(uuid4())
    operator.context = SimpleNamespace(organization=SimpleNamespace(id=oid, is_provider=True), solution_id=str(uuid4()))

    async def connection(_name):
        return SimpleNamespace(integration_id=str(uuid4()), config={"app_id": "123", "private_key": "synthetic"})

    async def metadata(path):
        if path == "/api/events/adapters":
            schema = {} if failure == "unsupported" else {"delivery_id_header": {"type": "string"}}
            return httpx.Response(200, json={"adapters": [{"name": "generic", "config_schema": {"properties": schema}}]})
        return httpx.Response(200, json={"config_defaults": {"private_key": "***"} if failure == "global_key" else {},
            "mappings": [] if failure == "missing_override" else [{"organization_id": oid,
                "config": {"private_key": "***", "app_id": "123"}}]})

    monkeypatch.setattr(operator.integrations, "get", connection)
    monkeypatch.setattr(operator.api, "get", metadata)
    if failure == "unsupported":
        with pytest.raises(package.PackageError, match="#1285"):
            await operator.require_delivery_identity()
    elif failure:
        with pytest.raises(package.PackageError, match="operator-org|operator organization"):
            await operator.operator_connection()
    else:
        await operator.require_delivery_identity()
        assert (await operator.operator_connection()).config["app_id"] == "123"


async def test_late_inventory_cannot_replace_a_newer_snapshot(broker):
    await broker.client.bind(70001, str(uuid4()), str(uuid4()))
    epoch = await broker.client.epoch("70001")
    await broker.store.insert("inventory:newer", {"kind": "inventory", "installation": "70001", "epoch": epoch,
        "repositories": [{"id": "80002", "name": "example/new"}], "order": broker.clock() + 5,
        "verified_at": broker.clock()})
    await broker.store.insert("inventory:late", {"kind": "inventory", "installation": "70001", "epoch": epoch,
        "repositories": [{"id": "80001", "name": "example/old"}], "order": broker.clock() + 1,
        "verified_at": broker.clock()})
    assert (await broker.client.inventory("70001"))["repositories"][0]["id"] == "80002"


async def test_invalid_destination_leaves_no_permanent_binding(broker):
    async def invalid(_organization, _solution):
        raise package.PackageError("Consumer Solution is not accessible")

    broker.client.validate_target = invalid
    with pytest.raises(package.PackageError, match="not accessible"):
        await broker.client.bind(70001, str(uuid4()), str(uuid4()))
    assert await broker.store.get("binding:70001") is None
    assert broker.github.calls == []


async def test_lower_timestamp_lifecycle_invalidates_an_inflight_refresh(broker):
    await broker.client.bind(70001, str(uuid4()), str(uuid4()))
    await broker.client.fence("70001", "future-clock-fence", broker.clock() + 3600)
    epoch = await broker.client.epoch("70001")

    async def removed():
        await broker.client.fence("70001", "new-lower-clock-fence", broker.clock() - 3600)

    broker.github.on_mint = removed
    with pytest.raises(package.PackageError, match="changed during token"):
        await broker.client.token("70001", epoch)
    assert await broker.client.epoch("70001") != epoch


@pytest.mark.parametrize("status", [403, 429])
async def test_quota_without_primary_headers_still_stops_other_callers(status):
    store, clock, calls = MemoryStore(), Clock(), []

    async def respond(request):
        calls.append(request)
        return httpx.Response(status, json={"message": "secondary rate limit"})

    github = package.GitHub("123", "not-used-with-installation-token", store, clock=clock, sleep=clock.sleep,
                           transport=httpx.MockTransport(respond))
    with pytest.raises(package.PackageError):
        await github.call("repositories", "70001", token="synthetic")
    with pytest.raises(package.PackageError, match="cooldown"):
        await github.call("repositories", "70001", token="synthetic")
    assert len(calls) == 1


async def test_app_jwt_read_only_mint_and_lossless_redelivery_use_fixed_github_paths():
    signing_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key = signing_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode()
    public_key = signing_key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
    store, clock, calls = MemoryStore(), Clock(), []

    async def respond(request):
        claims = package.jwt.decode(request.headers["authorization"].removeprefix("Bearer "),
            public_key, algorithms=["RS256"], options={"verify_iat": False, "verify_exp": False})
        assert claims == {"iss": "123", "iat": int(clock()) - 60, "exp": int(clock()) + 540}
        assert request.url.host == "api.github.com" and request.url.scheme == "https"
        calls.append((request.method, request.url.path))
        if request.url.path.endswith("access_tokens"):
            assert json.loads(request.content) == {"permissions": {"metadata": "read"}}
            return httpx.Response(201, json={"token": "synthetic-new-token", "expires_at": "2027-01-15T09:00:00Z"})
        return httpx.Response(202 if request.method == "POST" else 200, json={"id": 70001})

    github = package.GitHub("123", private_key, store, clock=clock, sleep=clock.sleep,
                           transport=httpx.MockTransport(respond))
    assert (await github.call("installation", "70001"))["id"] == 70001
    assert (await github.call("mint", "70001"))["token"] == "synthetic-new-token"
    assert await github.call("redeliver", "70001", delivery="3852823007498076160") == {"accepted": True}
    assert calls == [("GET", "/app/installations/70001"), ("POST", "/app/installations/70001/access_tokens"),
                     ("POST", "/app/hook/deliveries/3852823007498076160/attempts")]
    assert private_key not in json.dumps(store.rows)
    assert "synthetic-new-token" not in json.dumps(store.rows)
    assert {row["bucket"] for row in await store.query(kind="request")} == {"app"}
