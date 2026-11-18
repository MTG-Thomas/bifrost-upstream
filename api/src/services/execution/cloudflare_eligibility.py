"""Verify operator-reviewed loose-workspace revisions for the dev launcher."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import logging
import re
import sys
from dataclasses import dataclass

from src.config import CloudflareWorkflowApproval, get_settings
from src.core.storage.contracts import StorageError
from src.services.repo_storage import RepoStorage

logger = logging.getLogger(__name__)

_POLICY_VERSION = "cloudflare-workflow-approval-v1"
_warned_invalid_revisions: set[str] = set()
_MAX_SOURCE_BYTES = 2 * 1024**2
_MAX_BUNDLE_BYTES = 4 * 1024**2
_WORKSPACE_PATH = re.compile(r"(?:[a-zA-Z_][a-zA-Z0-9_]*/)*[a-zA-Z_][a-zA-Z0-9_]*\.py")
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
# The shipped spike build's direct dependencies plus Pydantic's native companion.
# Wider package qualification is separate from inclusion in this build.
_BUNDLED_PACKAGES = frozenset({
    "httpx", "pydantic", "pydantic-core", "python-dotenv", "pyyaml", "redis",
    "workers-runtime-sdk",
})
_RUNTIME_ROOTS = sys.stdlib_module_names | {
    "bifrost", "main", "runtime_runner", "runtime_guard", "pyodide_refresh", "sdk_cleanup",
}
_PROCESS_CLOCKS = {"process_time", "process_time_ns"}


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _https_hostname(host: str) -> bool:
    labels = host.split(".")
    return (
        1 < len(labels) and len(host) <= 253
        and all(_HOST_LABEL.fullmatch(label) for label in labels)
        and not labels[-1].isdigit()
    )


def _approved_manifest(approval: CloudflareWorkflowApproval) -> dict[str, str] | None:
    manifest = {approval.entry_path: approval.entry_sha256, **approval.module_sha256}
    if (
        approval.entry_path in approval.module_sha256
        or approval.manifest_sha256 != _digest(manifest)
        or any(
            len(path) > 512 or not _WORKSPACE_PATH.fullmatch(path)
            or path.split("/")[0].removesuffix(".py") in _RUNTIME_ROOTS
            for path in manifest
        )
        or not set(approval.packages) <= _BUNDLED_PACKAGES
        or not all(_https_hostname(host) for host in approval.https_hosts)
        or not set(approval.required_https_hosts) <= set(approval.https_hosts)
        or approval.estimated_subrequests > approval.subrequest_allowance
        or approval.estimated_cpu_ms > approval.cpu_budget_ms
    ):
        return None
    return manifest


@dataclass(frozen=True)
class ApprovedWorkspace:
    revision: str
    bundle: dict[str, object]


async def prepare_approved_workspace(
    *,
    workflow_id: str,
    entry_path: str | None,
    function_name: str | None,
    entry_sha256: str | None,
    expected_revision: str | None = None,
) -> ApprovedWorkspace | None:
    """Read and check the exact source bytes that would go on the workflow wire."""
    for approval in get_settings().cloudflare_workflow_approvals:
        if (
            str(approval.workflow_id) != workflow_id
            or approval.entry_path != entry_path
            or approval.function_name != function_name
            or approval.entry_sha256 != entry_sha256
        ):
            continue
        revision = _digest([_POLICY_VERSION, approval.model_dump(mode="json")])
        manifest = _approved_manifest(approval)
        if manifest is None:
            if revision not in _warned_invalid_revisions:
                _warned_invalid_revisions.add(revision)
                logger.warning(
                    "Ignoring invalid BIFROST_CLOUDFLARE_WORKFLOW_APPROVALS entry for workflow %s "
                    "(approval revision %s). Check manifest pins, packages, HTTPS hosts and budgets.",
                    approval.workflow_id,
                    revision,
                )
            continue
        if expected_revision is not None and revision != expected_revision:
            continue
        sources: dict[str, str] = {}
        remaining = _MAX_BUNDLE_BYTES
        storage = RepoStorage()
        try:
            async with asyncio.timeout(20):
                for path, sha256 in manifest.items():
                    content = await storage.read(path, max_bytes=min(remaining, _MAX_SOURCE_BYTES))
                    if storage.compute_hash(content) != sha256:
                        break
                    source = content.decode("utf-8")
                    syntax = ast.parse(source, filename=path)
                    if any(
                        (isinstance(node, ast.Attribute) and node.attr in _PROCESS_CLOCKS)
                        or (isinstance(node, ast.alias) and node.name in _PROCESS_CLOCKS)
                        for node in ast.walk(syntax)
                    ):
                        break
                    if path == entry_path and not any(
                        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and node.name == function_name for node in syntax.body
                    ):
                        break
                    sources[path] = source
                    remaining -= len(content)
        except (StorageError, TimeoutError, ValueError, SyntaxError, RecursionError):
            continue
        if len(sources) == len(manifest):
            return ApprovedWorkspace(revision, {
                "manifest": manifest,
                "sources": sources,
                "packages": approval.packages,
                "https_hosts": approval.https_hosts,
                "subrequest_allowance": approval.subrequest_allowance,
                "cpu_budget_ms": approval.cpu_budget_ms,
            })
    return None
