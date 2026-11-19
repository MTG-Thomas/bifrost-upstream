"""Operator approval, source pinning and the opaque workflow send gate."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
import pytest_asyncio

from bifrost.cloudflare_analysis import (
    POLICY_VERSION,
    AnalysisRejected,
    SourceUnresolved,
    analyze,
    digest,
)
from src.core.redis_client import PendingExecution, pending_execution_record
from src.core.storage.contracts import StorageError
from src.jobs.platform import cloudflare_reapproval
from src.models.orm import (
    CloudflareApproval,
    Organization,
    Solution,
    SolutionFileIndex,
    Workflow,
)
from src.services.execution import cloudflare_eligibility, service, workflow_dispatch
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
        if path not in sources:
            raise StorageError("not_found", provider_code="NoSuchKey")
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


@pytest_asyncio.fixture
async def installed_review(review, db_session, monkeypatch):
    approval, sources, _ = review
    owner = Organization(id=uuid4(), name="Solution review fixture", created_by="test")
    db_session.add(owner)
    await db_session.flush()
    solution = Solution(
        id=uuid4(), slug=f"review-{uuid4().hex}", name="Review fixture", organization_id=owner.id,
        version="2.0.0", runtime_source_generation=uuid4(),
    )
    db_session.add(solution)
    await db_session.flush()
    workflow = Workflow(
        id=approval.workflow_id, name="Review fixture", path=ENTRY, function_name="run",
        organization_id=owner.id, solution_id=solution.id,
    )
    db_session.add(workflow)
    for path, content in sources.items():
        db_session.add(SolutionFileIndex(
            solution_id=solution.id, path=path, content_hash=hashlib.sha256(content).hexdigest(),
        ))
    await db_session.flush()
    scope = cloudflare_eligibility.SolutionApprovalScope(
        install_id=solution.id, version=solution.version, source_generation=solution.runtime_source_generation,
    )
    approval.organization_id = owner.id
    approval.declaration["solution"] = scope.model_dump(mode="json")
    approval.revision = digest(str(workflow.id))
    approval.accepted_by = "fixture operator"
    db_session.add(approval)
    await db_session.flush()
    storage = cloudflare_eligibility.RepoStorage()
    solution_storage = Mock(return_value=storage)
    monkeypatch.setattr(cloudflare_eligibility, "SolutionStorage", solution_storage)
    monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", Mock(side_effect=AssertionError("loose read")))

    @asynccontextmanager
    async def db_context():
        yield db_session

    monkeypatch.setattr(cloudflare_eligibility, "get_db_context", db_context)
    return approval, solution, scope, solution_storage


@pytest.mark.parametrize("state", ["matching", "other-install", "outbound", "new-generation"])
async def test_solution_placement_uses_current_install_scope(installed_review, db_session, state):
    approval, solution, scope, storage = installed_review
    if state == "other-install":
        approval.declaration["solution"]["install_id"] = str(uuid4())
    elif state == "outbound":
        solution.allow_outbound_access = True
    elif state == "new-generation":
        solution.runtime_source_generation = uuid4()
    await db_session.flush()
    pending = pending_execution_record(
        execution_id=str(uuid4()), workflow_id=str(approval.workflow_id), parameters={},
        org_id=str(approval.organization_id), user_id=str(uuid4()), user_name="Operator",
        user_email="operator@example.com", lineage=None,
    )
    revision, request = await workflow_dispatch._cloudflare_placement(
        db_session, pending, {"workflow_id": pending["workflow_id"]},
    )
    assert revision == (approval.revision if state == "matching" else None)
    if state == "new-generation":
        assert request is not None
        assert request.payload.solution == scope.model_copy(
            update={"source_generation": solution.runtime_source_generation},
        )
        assert request.payload.parent_revision == approval.revision
        assert request.organization_id == solution.organization_id
    else:
        assert request is None
    storage.assert_not_called()


@pytest.mark.parametrize("changed", ["none", "generation", "version", "owner", "outbound"])
async def test_solution_acceptance_requires_the_reviewed_install_scope(installed_review, db_session, changed):
    approval, solution, scope, storage = installed_review
    proposal = await cloudflare_eligibility.propose_approval(
        approval.workflow_id, subrequest_allowance=10, cpu_budget_ms=500,
    )
    assert proposal.declaration.solution == scope
    assert proposal.organization_id == solution.organization_id
    storage.assert_called_once_with(solution.id)
    if changed == "none":
        accepted = await cloudflare_eligibility.accept_proposal(db_session, proposal, accepted_by="operator")
        assert accepted.declaration["solution"] == scope.model_dump(mode="json")
        assert accepted.organization_id == solution.organization_id
        return
    if changed == "generation":
        solution.runtime_source_generation = uuid4()
    elif changed == "version":
        solution.version = "3.0.0"
    elif changed == "owner":
        solution.organization_id = None
    else:
        solution.allow_outbound_access = True
    await db_session.flush()
    with pytest.raises(AnalysisRejected, match="changed during review"):
        await cloudflare_eligibility.accept_proposal(db_session, proposal, accepted_by="operator")


@pytest.mark.parametrize("state", ["unpublished", "outbound", "inactive", "wrong-owner"])
async def test_proposal_refuses_ineligible_solution_before_storage(installed_review, db_session, state):
    approval, solution, _, storage = installed_review
    if state == "unpublished":
        solution.runtime_source_generation = None
    elif state == "outbound":
        solution.allow_outbound_access = True
    elif state == "inactive":
        solution.status = "inactive"
    else:
        solution.organization_id = None
    await db_session.flush()
    with pytest.raises(AnalysisRejected, match="published install-local source"):
        await cloudflare_eligibility.propose_approval(
            approval.workflow_id, subrequest_allowance=10, cpu_budget_ms=500,
        )
    storage.assert_not_called()


@pytest.mark.parametrize("changed", [None, "generation", "index", "bytes", "install"])
async def test_prepare_solution_uses_only_its_published_install_snapshot(review, monkeypatch, changed):
    approval, sources, _ = review
    install_id, owner = uuid4(), uuid4()
    scope = cloudflare_eligibility.SolutionApprovalScope(
        install_id=install_id, version="2.0.0", source_generation=uuid4(),
    )
    approval.organization_id = owner
    approval.declaration["solution"] = scope.model_dump(mode="json")
    scope_lookup = AsyncMock(return_value=scope)
    if changed == "generation":
        scope_lookup.side_effect = [scope, scope.model_copy(update={"source_generation": uuid4()})]
    monkeypatch.setattr(cloudflare_eligibility, "solution_approval_scope", scope_lookup)
    manifest = cloudflare_eligibility.ApprovalDeclaration.model_validate(approval.declaration).manifest
    index = {} if changed == "index" else manifest
    monkeypatch.setattr(cloudflare_eligibility, "indexed_pins", AsyncMock(return_value=index))
    storage = cloudflare_eligibility.RepoStorage()
    solution_storage = Mock(return_value=storage)
    monkeypatch.setattr(cloudflare_eligibility, "SolutionStorage", solution_storage)
    monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", Mock(side_effect=AssertionError("loose read")))
    if changed == "bytes":
        sources[HELPER] = b"VALUE = 9\n"
    workspace = await cloudflare_eligibility.prepare_approved_workspace(
        workflow_id=str(approval.workflow_id), entry_path=ENTRY, function_name="run",
        entry_sha256=None, expected_revision=approval.revision,
        solution_id=str(uuid4() if changed == "install" else install_id),
    )
    if changed is None:
        assert workspace is not None
        assert workspace.bundle["sources"] == {path: source.decode() for path, source in sources.items()}
        assert workspace.bundle["solution_id"] == str(install_id)
        solution_storage.assert_called_once_with(install_id)
    else:
        assert workspace is None


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
        assert enqueue.await_count == 0
        await asyncio.gather(*cloudflare_reapproval._staging.values())
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
async def test_http_calls_report_unresolved_urls(prefix, call):
    source = prefix + "\nasync def run(url): return await " + call + "\n"
    unresolved = await _analyze({ENTRY: source.format(url="url").encode()})
    assert unresolved["unresolved_url_calls"] == 1
    assert unresolved["estimated_subrequests"] == 2
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
    result = await _analyze({ENTRY: source})
    assert result["unresolved_url_calls"] == 1
    assert result["https_hosts"] == ["api.example.com"]


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
    "import builtins; builtins.compile = str",
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
])
async def test_dynamic_python_is_rejected(statement):
    with pytest.raises(AnalysisRejected, match="dynamic|Unsupported Pyodide import"):
        await _analyze({ENTRY: f"def run(): {statement}\n".encode()})


@pytest.mark.parametrize(("body", "call"), [
    ("return getattr(obj, method)()", "_dispatch(obj, unknown)"),
    ("return getattr(obj, method)()", "_dispatch(obj, '__class__')"),
    ("return getattr(obj, method)()", "_dispatch(obj, 'import_module')"),
    ("return getattr(obj, method)()", "_dispatch(obj, 'attrgetter')"),
    ("return getattr(obj, method)()", "_dispatch(*args, method='list')"),
    ("return getattr(obj, method)()", "map(_dispatch, [obj])"),
    ("method = unknown; return getattr(obj, method)()", "_dispatch(obj, 'list')"),
    ("return getattr(__imported_builtins, method)()", "_dispatch(obj, 'list')"),
])
async def test_entry_local_dispatch_refuses_unproven_selectors(body, call):
    source = (
        "import builtins as __imported_builtins\n"
        f"def _dispatch(obj, method):\n    {body}\n"
        f"def run(obj, unknown, args):\n    return {call}\n"
    ).encode()
    with pytest.raises(AnalysisRejected, match="dynamic attribute"):
        await _analyze({ENTRY: source})


async def test_entry_local_dispatch_counts_literal_selectors_as_unresolved():
    source = b"""
async def _dispatch(obj, method):
    return await getattr(obj, method)()

async def run(obj):
    return await _dispatch(obj, "list")

async def other_entry(obj):
    return await _dispatch(obj, method="search")
"""
    result = await _analyze({ENTRY: source})
    assert result["estimated_subrequests"] == 2
    assert result["unresolved_url_calls"] == 1
    assert result["required_https_hosts"] == []


async def test_positional_only_selector_is_not_bound_by_keyword():
    source = b"""
def _dispatch(obj, method=unknown, /, **kwargs):
    return getattr(obj, method)()
def run(obj):
    return _dispatch(obj, method="list")
"""
    with pytest.raises(AnalysisRejected, match="dynamic attribute"):
        await _analyze({ENTRY: source})


async def test_imported_entry_cannot_use_entry_local_selector_analysis():
    source = b"""
import helpers
def _dispatch(obj, method):
    return getattr(obj, method)()
def run(obj):
    return _dispatch(obj, "list")
"""
    with pytest.raises(AnalysisRejected, match="Entry-local attribute selectors"):
        await _analyze({ENTRY: source, HELPER: b"from workflows.fixture import _dispatch\n"})


async def test_unused_selector_analysis_preserves_allowed_circular_import():
    source = b"""
import helpers
def _read(obj, field):
    return getattr(obj, field)
def run(obj):
    return _read(obj, "name")
"""
    proposal = await _analyze({ENTRY: source, HELPER: b"from workflows.fixture import _read\n"})
    assert proposal["unresolved_url_calls"] == 0


async def test_parse_rejects_oversized_source_before_parsing(monkeypatch):
    from bifrost import cloudflare_analysis

    parse = Mock(side_effect=AssertionError("oversized source reached parser"))
    monkeypatch.setattr(cloudflare_analysis.ast, "parse", parse)
    with pytest.raises(AnalysisRejected, match="bundle budget"):
        await _analyze({ENTRY: b" " * (384 * 1024 + 1)})
    parse.assert_not_called()


async def test_total_parse_budget_is_enforced_before_fifth_module(monkeypatch):
    from bifrost import cloudflare_analysis

    parse = Mock(wraps=cloudflare_analysis.ast.parse)
    monkeypatch.setattr(cloudflare_analysis.ast, "parse", parse)
    # Four 256 KiB files use the budget. Loading a fifth must stop before parsing.
    entry = b"import a, b, c, d\ndef run(): pass\n"
    size = 256 * 1024
    sources = {ENTRY: entry.ljust(size), **{f"{name}.py": b"x=1\n".ljust(size) for name in "abcd"}}
    with pytest.raises(AnalysisRejected, match="bundle budget"):
        await _analyze(sources)
    assert parse.call_count == 4


async def test_parse_admits_large_sparse_source():
    source = b"def run(): pass\n#" + b"x" * (256 * 1024)
    proposal = await _analyze({ENTRY: source})
    assert proposal["estimated_subrequests"] == 1


async def test_large_dense_source_is_rejected_before_parsing(monkeypatch):
    from bifrost import cloudflare_analysis

    parse = Mock(side_effect=AssertionError("dense source reached parser"))
    monkeypatch.setattr(cloudflare_analysis.ast, "parse", parse)
    source = b"def run(): pass\n" + b"item = 1\n" * 20_000
    with pytest.raises(AnalysisRejected, match="token budget"):
        await _analyze({ENTRY: source})
    parse.assert_not_called()


async def test_analysis_bounds_inherited_scope_tables(monkeypatch):
    from bifrost import cloudflare_analysis

    monkeypatch.setattr(cloudflare_analysis, "_MAX_ACTIVE_SCOPE_BINDINGS", 6)
    source = b"a = 'a'\nb = 'b'\ndef run():\n    def nested(): pass\n"
    with pytest.raises(AnalysisRejected, match="active scope budget"):
        await _analyze({ENTRY: source})


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
    async def pins_for_scope(_, paths, *, install_id):
        assert install_id is None
        return {HELPER: "a" * 64} if indexed and HELPER in paths else {}

    monkeypatch.setattr(cloudflare_eligibility, "indexed_pins", pins_for_scope)
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


async def test_inactive_workflow_is_superseded_before_reading_source(review, monkeypatch):
    approval, _, _ = review

    @asynccontextmanager
    async def inactive_context():
        yield Mock(get=AsyncMock(return_value=SimpleNamespace(is_active=False)))

    storage = Mock()
    monkeypatch.setattr(cloudflare_eligibility, "get_db_context", inactive_context)
    monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", storage)
    with pytest.raises(cloudflare_eligibility.ObservationChanged, match="active workflow"):
        await cloudflare_eligibility.propose_approval(
            approval.workflow_id, subrequest_allowance=10, cpu_budget_ms=100,
        )
    storage.assert_not_called()


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


@pytest.mark.parametrize("outbound", [False, True])
async def test_opaque_solution_route_sends_only_the_verified_entry_digest(installed_review, monkeypatch, outbound):
    approval, solution, _, _ = installed_review
    prepare = AsyncMock(wraps=cloudflare_eligibility.prepare_approved_workspace)
    monkeypatch.setattr(cloudflare_eligibility, "prepare_approved_workspace", prepare)
    transport = Mock(
        lease=RemoteOwnerLease(uuid4(), uuid4(), uuid4(), 1, 2),
        request=AsyncMock(return_value={"runtime_handle": "fixture:1"}),
        memory_request_bytes=Mock(return_value=64 << 20),
    )
    pool = ThinRemoteProcessPool(transport, runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE)
    write = AsyncMock()
    monkeypatch.setattr(pool, "_write_context_to_redis", write)
    monkeypatch.setattr(pool, "_write_active_execution_lease", AsyncMock())
    context = {
        "workflow_id": str(approval.workflow_id), "file_path": ENTRY, "function_name": "run",
        "content_hash": None, "solution_id": str(solution.id), "solution_global_repo_access": outbound,
        "tags": ["workflow"], "timeout_seconds": 0,
    }

    async def route():
        await pool.route_execution(
            str(uuid4()), context, {"workflow_id": str(approval.workflow_id)},
            credential_route=pool.credential_route(), cloudflare_revision=approval.revision,
        )

    try:
        if outbound:
            with pytest.raises(CloudflareRevisionRejected):
                await route()
            prepare.assert_not_awaited()
            transport.request.assert_not_awaited()
            return
        await route()
        prepare.assert_awaited_once_with(
            workflow_id=str(approval.workflow_id), entry_path=ENTRY, function_name="run",
            entry_sha256=None, expected_revision=approval.revision, solution_id=str(solution.id),
        )
        sent = transport.request.await_args.args[1]["context"]
        assert sent["solution_id"] == str(solution.id)
        assert sent["content_hash"] == approval.declaration["entry_sha256"]
        assert write.await_args.args[1]["content_hash"] is None
    finally:
        await pool.ownership_lost()
