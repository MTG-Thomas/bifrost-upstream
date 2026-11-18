"""Operator approval, source pinning and the opaque workflow send gate."""

import hashlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from src.core.redis_client import PendingExecution, pending_execution_record
from src.core.storage.contracts import StorageError
from src.jobs.platform import cloudflare_reapproval
from src.models.orm import CloudflareApproval
from src.services.execution import cloudflare_eligibility, service, workflow_dispatch
from src.services.execution.cloudflare_analysis import (
    POLICY_VERSION,
    AnalysisRejected,
    SourceUnresolved,
    analyze,
    digest,
)
from src.worker.remote.ownership import RemoteOwnerLease
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    CloudflareRevisionRejected,
)
from src.worker.remote.remote_pool import ThinRemoteProcessPool

ENTRY = "workflows/fixture.py"
HELPER = "helpers.py"
SOURCE = b"import helpers\ndef run():\n    return helpers.VALUE\n"


@pytest.fixture
def review(monkeypatch):
    workflow = uuid4()
    sources = {ENTRY: SOURCE, HELPER: b"VALUE = 1\n"}
    manifest = {path: hashlib.sha256(content).hexdigest() for path, content in sources.items()}
    declaration = {
        "entry_path": ENTRY, "function_name": "run", "entry_sha256": manifest[ENTRY],
        "module_sha256": {HELPER: manifest[HELPER]}, "manifest_sha256": digest(manifest),
        "packages": [], "https_hosts": [], "required_https_hosts": [], "estimated_subrequests": 2,
        "subrequest_allowance": 10, "estimated_cpu_ms": 1, "cpu_budget_ms": 500,
    }
    approval = CloudflareApproval(
        workflow_id=workflow, declaration=declaration, revision="a" * 64, policy_version=POLICY_VERSION,
    )

    async def read(path, *, max_bytes):
        assert len(sources[path]) <= max_bytes
        return sources[path]

    lookup = AsyncMock(return_value=approval)

    @asynccontextmanager
    async def db_context():
        yield Mock(scalar=lookup)

    monkeypatch.setattr(cloudflare_eligibility, "get_db_context", db_context)
    monkeypatch.setattr(cloudflare_eligibility, "current_approval", lookup)
    monkeypatch.setattr(workflow_dispatch, "get_settings", lambda: SimpleNamespace(cloudflare_worker_hello_enabled=True))
    storage = SimpleNamespace(read=read, compute_hash=cloudflare_eligibility.RepoStorage.compute_hash)
    monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", lambda: storage)
    return approval, sources, lookup


async def _prepare(approval):
    return await cloudflare_eligibility.prepare_approved_workspace(
        workflow_id=str(approval.workflow_id), entry_path=ENTRY, function_name="run",
        entry_sha256=approval.declaration["entry_sha256"], expected_revision=approval.revision,
    )


@pytest.mark.parametrize("state", ["unapproved", "unchanged", "changed-helper", "changed-owner"])
async def test_enqueue_uses_only_the_durable_approval_and_index(review, monkeypatch, state):
    approval, _, stored = review
    record = dict(pending_execution_record(
        execution_id=str(uuid4()), workflow_id=str(approval.workflow_id), parameters={}, org_id=None,
        user_id=str(uuid4()), user_name="Operator", user_email="operator@example.com", lineage=None,
    ))
    del record["script_name"]
    pending = PendingExecution(**record)  # type: ignore[typeddict-item]
    if state == "unapproved":
        stored.return_value = None
    lookup = AsyncMock(return_value={
        "path": ENTRY, "function_name": "run", "content_hash": approval.declaration["entry_sha256"],
        "solution_id": None, "type": "workflow",
        "organization_id": str(uuid4()) if state == "changed-owner" else None,
    })
    monkeypatch.setattr(service, "get_workflow_for_execution", lookup)
    pins = {ENTRY: approval.declaration["entry_sha256"], **approval.declaration["module_sha256"]}
    if state == "changed-helper":
        pins[HELPER] = "b" * 64
    monkeypatch.setattr(cloudflare_eligibility, "indexed_pins", AsyncMock(return_value=pins))
    enqueue = AsyncMock()
    monkeypatch.setattr(cloudflare_reapproval, "stage_reapproval", enqueue)
    monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", Mock(side_effect=AssertionError("enqueue read source")))
    monkeypatch.setattr(workflow_dispatch, "_request_for", AsyncMock(return_value=64 << 20))
    db = Mock(scalar=AsyncMock(return_value="normal"))
    accepted = await workflow_dispatch.stage_workflow_run(db, pending, {
        "workflow_id": pending["workflow_id"], "cloudflare_revision": "f" * 64,
    }, record_exists=True)
    assert db.add.call_args.args[0].cloudflare_revision == (approval.revision if state == "unchanged" else None)
    if state == "unapproved":
        lookup.assert_not_awaited()
    enqueue.assert_not_awaited()
    if state == "changed-helper":
        assert accepted.reapproval is not None
        assert accepted.reapproval.payload.observed_pins == pins
        monkeypatch.setattr(workflow_dispatch, "hand_off", AsyncMock())
        await workflow_dispatch.hand_off_workflow_run(accepted)
        enqueue.assert_awaited_once_with(accepted.reapproval)
    else:
        assert accepted.reapproval is None


async def _analyze(sources, **limits):
    async def read(path, max_bytes):
        return sources.get(path)

    return await analyze(
        read, ENTRY, "run",
        **{"subrequest_allowance": 10, "cpu_budget_ms": 100, **limits},
    )


async def test_analyzer_pins_transitive_modules_and_infers_packages_and_hosts():
    sources = {
        ENTRY: b"from helpers.client import call\nasync def run(): return await call()\n",
        "helpers/__init__.py": b"from . import defaults\n",
        "helpers/defaults.py": b"port = 443\n",
        "helpers/client.py": b"import httpx as http\ndef call(): return http.get('https://example.com/data')\n",
    }
    proposal = await _analyze(sources)
    pins = {path: hashlib.sha256(source).hexdigest() for path, source in sources.items()}
    assert proposal["manifest_sha256"] == digest(pins)
    assert proposal["entry_sha256"] == pins.pop(ENTRY)
    assert proposal["module_sha256"] == pins
    assert proposal["packages"] == ["httpx"]
    assert proposal["https_hosts"] == proposal["required_https_hosts"] == ["example.com"]
    assert proposal["estimated_subrequests"] == 5
    assert 0 < proposal["estimated_cpu_ms"] <= proposal["cpu_budget_ms"]


@pytest.mark.parametrize("source, limits, reason", [
    (b"from time import process_time as clock\ndef run(): return clock()\n", {}, "runtime operation"),
    (b"import unknown\ndef run(): pass\n", {}, "Unresolved import"),
    (b"from js import fetch\ndef run(url): return fetch(url)\n", {}, "statically identifiable"),
    (b"def run(host): return f'https://{host}/path'\n", {}, "fixed HTTPS origin"),
    (b"def run(): return 'http://example.com'\n", {}, "exact, bare HTTPS"),
    (SOURCE, {"subrequest_allowance": 1}, "exceed the allowance"),
    (b"def run(): return [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]\n",
     {"cpu_budget_ms": 1}, "exceeds the budget"),
])
async def test_analyzer_rejects_unverifiable_or_over_budget_workflow(source, limits, reason):
    with pytest.raises(AnalysisRejected, match=reason):
        await _analyze({ENTRY: source, HELPER: b"VALUE = 1\n"}, **limits)


@pytest.mark.parametrize("statement", [
    "import re\ndef run(): return re.compile('abc')\n",
    "from re import compile\ndef run(): return compile('abc')\n",
])
async def test_regex_compile_is_allowed(statement):
    assert (await _analyze({ENTRY: statement.encode()}))["packages"] == []


@pytest.mark.parametrize("prefix, call", [
    ("from pyodide.http import pyfetch", "pyfetch({url})"),
    ("import js", "js.globalThis.fetch({url})"),
    ("import workers", "workers.fetch({url})"),
    ("import httpx", "httpx.AsyncClient().get({url})"),
    ("from httpx import AsyncClient\nclient = AsyncClient()", "client.get({url})"),
])
async def test_http_calls_require_identifiable_urls(prefix, call):
    source = prefix + "\nasync def run(url): return await " + call + "\n"
    with pytest.raises(AnalysisRejected, match="identifiable HTTPS URL"):
        await _analyze({ENTRY: source.format(url="url").encode()})
    result = await _analyze({ENTRY: source.format(url="'https://example.com/path'").encode()})
    assert result["required_https_hosts"] == ["example.com"]


async def test_http_context_manager_keeps_client_origin():
    source = b"import httpx\nasync def run():\n    async with httpx.AsyncClient() as c:\n        return await c.get('https://example.com/path')\n"
    assert (await _analyze({ENTRY: source}))["required_https_hosts"] == ["example.com"]


@pytest.mark.parametrize("source", [
    "import json, httpx\nasync def run():\n    async with httpx.AsyncClient() as c:\n        resp = await c.get('https://api.example.com')\n        return resp.json().get('id')",
    "import httpx\ndef run():\n    r = httpx.get('https://api.example.com')\n    return r.headers.get('etag')",
    "import os, httpx\ndef run():\n    httpx.get('https://api.example.com')\n    return os.environ.get('X')",
    "from urllib.parse import urlencode\ndef run(params): return urlencode(params.get('a'))",
    "import redis\ndef run(k):\n    client = redis.Redis()\n    return client.get(k)",
    "def run(session, obj): return session.delete(obj)",
    "import httpx\nasync def run():\n    async with httpx.AsyncClient(base_url='https://api.example.com') as c:\n        return await c.get('/v1/items')",
    "from httpx import Client\ndef run():\n    c = Client(base_url='https://api.example.com')\n    return c.get('items')",
    "from httpx import Client\ndef run():\n    with Client(base_url='https://api.example.com') as c:\n        return c.get('/items')",
])
async def test_http_analysis_ignores_data_access_and_tracks_client_origins(source):
    result = await _analyze({ENTRY: source.encode()})
    assert result["required_https_hosts"] == (["api.example.com"] if "https://" in source else [])


async def test_relative_client_url_cannot_supply_a_new_authority():
    source = b"import httpx\ndef run():\n    c = httpx.Client(base_url='https://api.example.com')\n    return c.get('//other.example.com/x')"
    with pytest.raises(AnalysisRejected, match="exact, bare HTTPS hostname"):
        await _analyze({ENTRY: source})


async def test_shadowed_builtin_names_are_local():
    source = b"def run(vars):\n    compile = str\n    globals = compile(vars)\n    return globals"
    assert (await _analyze({ENTRY: source}))["packages"] == []


async def test_mapping_get_without_http_intent_is_allowed():
    result = await _analyze({ENTRY: b"def run(values, key): return values.get(key)\n"})
    assert result["required_https_hosts"] == []


@pytest.mark.parametrize("statement", [
    "e = exec; e('pass')",
    "import builtins; getattr(builtins, 'ex' + 'ec')('pass')",
    "import builtins; getattr(builtins, 'exec')('pass')",
    "import builtins as b; fn = b.compile",
    "from builtins import eval as e",
    "vars()",
    "globals()",
    "import sys as s; s.modules",
    "import importlib",
    "e = __import__",
    "import builtins; builtins.__dict__['compile']",
    "__builtins__['compile']",
    "x.__subclasses__()",
    "x.__globals__",
    "x.__code__",
    "x.__code__ = None",
    "import operator; operator.attrgetter",
    "from operator import methodcaller",
    "import pydoc; pydoc.locate",
    "import pkgutil; pkgutil.resolve_name",
    "import runpy",
    "import code",
    "import codeop",
    "import httpx; getattr(httpx, 'post')",
    "import js; g = js.fetch",
    "import httpx; client = httpx.Client(); g = client.get",
])
async def test_dynamic_python_is_rejected(statement):
    with pytest.raises(AnalysisRejected, match="dynamic|Unsupported Pyodide import"):
        await _analyze({ENTRY: f"def run(): {statement}\n".encode()})


async def test_parse_rejects_oversized_source_before_parsing(monkeypatch):
    from src.services.execution import cloudflare_analysis

    parse = Mock(side_effect=AssertionError("oversized source reached parser"))
    monkeypatch.setattr(cloudflare_analysis.ast, "parse", parse)
    with pytest.raises(AnalysisRejected, match="bundle budget"):
        await _analyze({ENTRY: b" " * (128 * 1024 + 1)})
    parse.assert_not_called()


async def test_total_parse_budget_is_enforced_before_fifth_module(monkeypatch):
    from src.services.execution import cloudflare_analysis

    parse = Mock(wraps=cloudflare_analysis.ast.parse)
    monkeypatch.setattr(cloudflare_analysis.ast, "parse", parse)
    # Four 128 KiB files use the budget. Loading a fifth must stop before parsing.
    entry = b"import a, b, c, d\ndef run(): pass\n"
    size = 128 * 1024
    sources = {ENTRY: entry.ljust(size), **{f"{name}.py": b"x=1\n".ljust(size) for name in "abcd"}}
    with pytest.raises(AnalysisRejected, match="bundle budget"):
        await _analyze(sources)
    assert parse.call_count == 4


@pytest.mark.parametrize("indexed", [False, True])
async def test_missing_import_retries_only_if_the_index_has_the_path(review, monkeypatch, indexed):
    approval, sources, _ = review
    workflow = SimpleNamespace(
        is_active=True, solution_id=None, type="workflow", path=ENTRY, function_name="run", organization_id=None,
    )

    @asynccontextmanager
    async def db_context():
        yield Mock(get=AsyncMock(return_value=workflow))

    async def read(path, *, max_bytes):
        if path == ENTRY:
            return sources[path]
        raise StorageError("not_found", provider_code="NoSuchKey")

    monkeypatch.setattr(cloudflare_eligibility, "get_db_context", db_context)
    monkeypatch.setattr(cloudflare_eligibility, "indexed_pins", AsyncMock(
        side_effect=lambda _, paths: {HELPER: "a" * 64} if indexed and HELPER in paths else {},
    ))
    storage = SimpleNamespace(read=read, compute_hash=lambda content: hashlib.sha256(content).hexdigest())
    monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", lambda: storage)
    pins: dict[str, str | None] = {}
    with pytest.raises(AnalysisRejected) as failure:
        await cloudflare_eligibility.propose_approval(
            approval.workflow_id, subrequest_allowance=10, cpu_budget_ms=500, analysed_pins=pins,
        )
    assert isinstance(failure.value, SourceUnresolved) is indexed
    if not indexed:
        assert pins[HELPER] is None


async def test_oversized_storage_object_checks_observed_digest_before_refusal(review, monkeypatch):
    approval, _, _ = review
    workflow = SimpleNamespace(
        is_active=True, solution_id=None, type="workflow", path=ENTRY, function_name="run", organization_id=None,
    )

    @asynccontextmanager
    async def db_context():
        yield Mock(get=AsyncMock(return_value=workflow))

    storage = Mock(
        read=AsyncMock(side_effect=ValueError("oversized")),
        content_hash=AsyncMock(return_value="b" * 64),
    )
    monkeypatch.setattr(cloudflare_eligibility, "get_db_context", db_context)
    monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", lambda: storage)
    pins: dict[str, str | None] = {}
    with pytest.raises(cloudflare_eligibility.ObservationChanged):
        await cloudflare_eligibility.propose_approval(
            approval.workflow_id, subrequest_allowance=10, cpu_budget_ms=500,
            expected_pins={ENTRY: "a" * 64}, analysed_pins=pins,
        )
    storage.content_hash.assert_awaited_once_with(ENTRY)
    assert not pins


@pytest.mark.parametrize("problem", ["missing-marker", "removed-approval", "changed-entry", "changed-module"])
async def test_opaque_route_refuses_a_missing_or_stale_revision_before_send(review, monkeypatch, problem):
    approval, sources, stored = review
    approved = await _prepare(approval)
    assert approved is not None
    marker = approval.revision
    if problem == "missing-marker":
        marker = None
    elif problem == "removed-approval":
        stored.return_value = None
    else:
        sources[ENTRY if problem == "changed-entry" else HELPER] += b"# changed\n"

    transport = Mock(request=AsyncMock())
    pool = ThinRemoteProcessPool(transport, runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE)
    write = AsyncMock()
    monkeypatch.setattr(pool, "_write_context_to_redis", write)
    context = {
        "workflow_id": str(approval.workflow_id), "file_path": ENTRY, "function_name": "run",
        "content_hash": approval.declaration["entry_sha256"], "tags": ["workflow"],
    }
    with pytest.raises(CloudflareRevisionRejected):
        await pool.route_execution(
            str(uuid4()), context, {"workflow_id": str(approval.workflow_id)},
            credential_route=pool.credential_route(), cloudflare_revision=marker,
        )
    write.assert_not_awaited()
    transport.request.assert_not_awaited()


async def test_opaque_route_sends_the_verified_workspace_snapshot(review, monkeypatch):
    approval, sources, _ = review
    approved = await _prepare(approval)
    assert approved is not None
    transport = Mock(
        lease=RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 1, 2),
        request=AsyncMock(return_value={"runtime_handle": "fixture:1"}),
        memory_request_bytes=Mock(return_value=64 << 20),
    )
    pool = ThinRemoteProcessPool(transport, runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE)
    monkeypatch.setattr(pool, "_write_context_to_redis", AsyncMock())
    monkeypatch.setattr(pool, "_write_active_execution_lease", AsyncMock())
    context = {
        "workflow_id": str(approval.workflow_id), "file_path": ENTRY, "function_name": "run",
        "content_hash": approval.declaration["entry_sha256"], "tags": ["workflow"], "timeout_seconds": 0,
    }
    try:
        await pool.route_execution(
            str(uuid4()), context, {"workflow_id": str(approval.workflow_id)},
            credential_route=pool.credential_route(), cloudflare_revision=approval.revision,
        )
        command, payload = transport.request.await_args.args
        assert command == "workflow"
        bundle = payload["context"]["cloudflare_workspace"]
        assert bundle["sources"] == {path: source.decode() for path, source in sources.items()}
        assert bundle["manifest"] == {ENTRY: approval.declaration["entry_sha256"], **approval.declaration["module_sha256"]}
        assert bundle["https_hosts"] == approval.declaration["https_hosts"]
    finally:
        await pool.ownership_lost()
