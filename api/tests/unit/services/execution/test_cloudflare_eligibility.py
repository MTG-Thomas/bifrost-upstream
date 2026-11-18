"""Operator approval, source pinning and the opaque workflow send gate."""

import hashlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from src.core.redis_client import PendingExecution, pending_execution_record
from src.models.orm import CloudflareApproval
from src.services.execution import cloudflare_eligibility, service, workflow_dispatch
from src.services.execution.cloudflare_analysis import (
    POLICY_VERSION,
    AnalysisRejected,
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


@pytest.mark.parametrize("approved", [False, True])
async def test_enqueue_uses_only_the_durable_approval_and_index(review, monkeypatch, approved):
    approval, _, stored = review
    record = dict(pending_execution_record(
        execution_id=str(uuid4()), workflow_id=str(approval.workflow_id), parameters={}, org_id=None,
        user_id=str(uuid4()), user_name="Operator", user_email="operator@example.com", lineage=None,
    ))
    del record["script_name"]
    pending = PendingExecution(**record)  # type: ignore[typeddict-item]
    if not approved:
        stored.return_value = None
    lookup = AsyncMock(return_value={
        "path": ENTRY, "function_name": "run", "content_hash": approval.declaration["entry_sha256"],
        "solution_id": None, "type": "workflow", "organization_id": None,
    })
    monkeypatch.setattr(service, "get_workflow_for_execution", lookup)
    monkeypatch.setattr(cloudflare_eligibility, "indexed_pins", AsyncMock(
        return_value=approval.declaration["module_sha256"],
    ))
    monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", Mock(side_effect=AssertionError("enqueue read source")))
    monkeypatch.setattr(workflow_dispatch, "_request_for", AsyncMock(return_value=64 << 20))
    db = Mock(scalar=AsyncMock(return_value="normal"))
    await workflow_dispatch.stage_workflow_run(db, pending, {
        "workflow_id": pending["workflow_id"], "cloudflare_revision": "f" * 64,
    }, record_exists=True)
    assert db.add.call_args.args[0].cloudflare_revision == (approval.revision if approved else None)
    if not approved:
        lookup.assert_not_awaited()


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
