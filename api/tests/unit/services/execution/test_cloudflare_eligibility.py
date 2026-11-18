"""Operator approval, source pinning and the opaque workflow send gate."""

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from src.config import CloudflareWorkflowApproval
from src.core.redis_client import PendingExecution, pending_execution_record
from src.services.execution import cloudflare_eligibility, service, workflow_dispatch
from src.worker.remote.ownership import RemoteOwnerLease
from src.worker.remote.protocol import (
    CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
    CloudflareRevisionRejected,
)
from src.worker.remote.remote_pool import ThinRemoteProcessPool

ENTRY = "workflows/fixture.py"
HELPER = "helpers.py"
SOURCE = b"def run():\n    return 1\n"


@pytest.fixture
def review(monkeypatch):
    def make(**overrides):
        source = overrides.pop("source", SOURCE)
        sources = {ENTRY: source, HELPER: b"VALUE = 1\n"}
        manifest = {path: hashlib.sha256(content).hexdigest() for path, content in sources.items()}
        approval = CloudflareWorkflowApproval(
            **{
                "workflow_id": uuid4(),
                "entry_path": ENTRY,
                "function_name": "run",
                "entry_sha256": manifest[ENTRY],
                "module_sha256": {HELPER: manifest[HELPER]},
                "manifest_sha256": hashlib.sha256(
                    json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
                "packages": ["httpx"],
                "https_hosts": ["example.com"],
                "required_https_hosts": ["example.com"],
                "estimated_subrequests": 3,
                "subrequest_allowance": 10,
                "estimated_cpu_ms": 100,
                "cpu_budget_ms": 500,
                **overrides,
            },
        )
        settings = SimpleNamespace(cloudflare_workflow_approvals=[approval])

        async def read(path, *, max_bytes):
            assert len(sources[path]) <= max_bytes
            return sources[path]

        monkeypatch.setattr(cloudflare_eligibility, "get_settings", lambda: settings)
        monkeypatch.setattr(workflow_dispatch, "get_settings", lambda: settings)
        storage = SimpleNamespace(read=read, compute_hash=cloudflare_eligibility.RepoStorage.compute_hash)
        monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", lambda: storage)
        return approval, sources, settings

    return make


async def _prepare(approval, **kwargs):
    return await cloudflare_eligibility.prepare_approved_workspace(
        workflow_id=str(approval.workflow_id), entry_path=ENTRY, function_name="run",
        entry_sha256=approval.entry_sha256, **kwargs,
    )


@pytest.mark.parametrize("approved", [False, True], ids=["empty-list", "approved"])
async def test_enqueue_computes_revision_only_from_operator_approval(review, monkeypatch, approved):
    approval, _, settings = review()
    pending = pending_execution_record(
        execution_id=str(uuid4()), workflow_id=str(approval.workflow_id), parameters={}, org_id=None,
        user_id=str(uuid4()), user_name="Operator", user_email="operator@example.com", lineage=None,
    )
    if not approved:
        settings.cloudflare_workflow_approvals.clear()
    lookup = AsyncMock(return_value={
        "path": ENTRY, "function_name": "run", "content_hash": approval.entry_sha256,
        "solution_id": None, "type": "workflow",
    })
    monkeypatch.setattr(service, "get_workflow_for_execution", lookup)
    monkeypatch.setattr(workflow_dispatch, "_request_for", AsyncMock(return_value=64 << 20))
    db = Mock(scalar=AsyncMock(return_value="normal"))
    await workflow_dispatch.stage_workflow_run(db, pending, {
        "workflow_id": pending["workflow_id"], "cloudflare_revision": "f" * 64,
    }, record_exists=True)
    dispatch = db.add.call_args.args[0]
    if approved:
        assert dispatch.cloudflare_revision == (await _prepare(approval)).revision
        assert dispatch.cloudflare_revision != "f" * 64
    else:
        assert dispatch.cloudflare_revision is None
        lookup.assert_not_awaited()


async def test_enqueue_legacy_pending_without_script_name_with_empty_approvals(review, monkeypatch):
    approval, _, settings = review()
    settings.cloudflare_workflow_approvals.clear()
    record = dict(pending_execution_record(
        execution_id=str(uuid4()), workflow_id=str(approval.workflow_id), parameters={}, org_id=None,
        user_id=str(uuid4()), user_name="Operator", user_email="operator@example.com", lineage=None,
    ))
    del record["script_name"]
    pending = PendingExecution(**record)  # type: ignore[typeddict-item]
    lookup = AsyncMock()
    monkeypatch.setattr(service, "get_workflow_for_execution", lookup)
    db = Mock(scalar=AsyncMock(return_value="normal"), execute=AsyncMock())
    db.execute.return_value.one_or_none.return_value = None

    await workflow_dispatch.stage_workflow_run(
        db, pending, {"workflow_id": pending["workflow_id"]}, record_exists=True,
    )

    assert db.add.call_args.args[0].cloudflare_revision is None
    lookup.assert_not_awaited()


@pytest.mark.parametrize("overrides", [
    {"manifest_sha256": "a" * 64},
    {"packages": ["cryptography"]},
    {"https_hosts": ["https://example.com"]},
], ids=["manifest", "package", "invalid-host"])
async def test_invalid_approval_warns_once_without_reading_source(review, monkeypatch, caplog, overrides):
    approval, _, _ = review(**overrides)
    storage = Mock()
    monkeypatch.setattr(cloudflare_eligibility, "RepoStorage", storage)

    assert await _prepare(approval) is None
    assert await _prepare(approval) is None

    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "BIFROST_CLOUDFLARE_WORKFLOW_APPROVALS" in caplog.text
    assert str(approval.workflow_id) in caplog.text
    storage.assert_not_called()


@pytest.mark.parametrize("overrides", [
    {"packages": ["cryptography"]},
    {"https_hosts": ["https://example.com"]},
    {"required_https_hosts": ["other.example.com"]},
    {"estimated_subrequests": 11},
    {"estimated_cpu_ms": 501},
    {"manifest_sha256": "a" * 64},
    {"source": b"from time import process_time as clock\ndef run():\n    return clock()\n"},
], ids=["package", "invalid-host", "missing-host", "subrequests", "cpu", "manifest", "runtime-trap"])
async def test_unfit_review_cannot_approve_exact_source(review, overrides):
    approval, _, _ = review(**overrides)
    assert await _prepare(approval) is None


@pytest.mark.parametrize("problem", ["missing-marker", "removed-approval", "changed-entry", "changed-module"])
async def test_opaque_route_refuses_a_missing_or_stale_revision_before_send(review, monkeypatch, problem):
    approval, sources, settings = review()
    approved = await _prepare(approval)
    assert approved is not None
    marker = approved.revision
    if problem == "missing-marker":
        marker = None
    elif problem == "removed-approval":
        settings.cloudflare_workflow_approvals.clear()
    else:
        sources[ENTRY if problem == "changed-entry" else HELPER] += b"# changed\n"

    transport = Mock(request=AsyncMock())
    pool = ThinRemoteProcessPool(transport, runtime_profile=CLOUDFLARE_PYTHON_RUNTIME_PROFILE)
    write = AsyncMock()
    monkeypatch.setattr(pool, "_write_context_to_redis", write)
    context = {
        "workflow_id": str(approval.workflow_id), "file_path": ENTRY, "function_name": "run",
        "content_hash": approval.entry_sha256, "tags": ["workflow"],
    }
    with pytest.raises(CloudflareRevisionRejected):
        await pool.route_execution(
            str(uuid4()), context, {"workflow_id": str(approval.workflow_id)},
            credential_route=pool.credential_route(), cloudflare_revision=marker,
        )
    write.assert_not_awaited()
    transport.request.assert_not_awaited()


async def test_opaque_route_sends_the_verified_workspace_snapshot(review, monkeypatch):
    approval, sources, _ = review()
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
        "content_hash": approval.entry_sha256, "tags": ["workflow"], "timeout_seconds": 0,
    }
    try:
        await pool.route_execution(
            str(uuid4()), context, {"workflow_id": str(approval.workflow_id)},
            credential_route=pool.credential_route(), cloudflare_revision=approved.revision,
        )
        command, payload = transport.request.await_args.args
        assert command == "workflow"
        bundle = payload["context"]["cloudflare_workspace"]
        assert bundle["sources"] == {path: source.decode() for path, source in sources.items()}
        assert bundle["manifest"] == {ENTRY: approval.entry_sha256, **approval.module_sha256}
        assert bundle["https_hosts"] == approval.https_hosts
    finally:
        await pool.ownership_lost()
