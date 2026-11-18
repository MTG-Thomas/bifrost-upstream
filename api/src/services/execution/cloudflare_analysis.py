"""Static analysis shared by the offline census and workspace approval service."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import re
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

POLICY_VERSION = "cloudflare-workflow-approval-v2"
MAX_SOURCE_BYTES = 2 * 1024**2
MAX_BUNDLE_BYTES = 4 * 1024**2
MAX_MODULES = 32
MAX_SUBREQUESTS = 50
MAX_CPU_MS = 2**53 - 1  # Integer range of the launcher wire.
_MAX_NODES = 100_000
_WORKSPACE_PATH = re.compile(r"(?:[a-zA-Z_][a-zA-Z0-9_]*/)*[a-zA-Z_][a-zA-Z0-9_]*\.py")
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_PROVIDED = {"bifrost", "js", "pyodide", "_pyodide"}
_RESERVED = sys.stdlib_module_names | _PROVIDED | {
    "main", "runtime_runner", "runtime_guard", "pyodide_refresh", "sdk_cleanup",
}
# Only distributions in the launcher build can be proposed. Earlier census
# import experiments with other packages do not add them to that build.
_PACKAGES = {
    "httpx": "httpx", "pydantic": "pydantic", "pydantic_core": "pydantic-core",
    "dotenv": "python-dotenv", "yaml": "pyyaml", "redis": "redis",
    "workers": "workers-runtime-sdk",
}
_UNSUPPORTED = {"ctypes", "fcntl", "multiprocessing", "resource", "signal", "socket", "subprocess"}
_TRAPS = {"process_time", "process_time_ns", "fork", "forkpty", "import_module", "__import__", "eval", "exec", "compile"}
_HTTP_METHODS = {"fetch", "request", "get", "head", "post", "put", "patch", "delete", "options", "stream", "urlopen"}

SourceReader = Callable[[str, int], Awaitable[bytes | None]]


class AnalysisRejected(ValueError):
    """Sources or requested bounds cannot be approved by this analyzer."""


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _valid_path(path: str) -> bool:
    root = path.split("/")[0].removesuffix(".py")
    return len(path) <= 512 and bool(_WORKSPACE_PATH.fullmatch(path)) and root not in (
        _RESERVED | _PACKAGES.keys()
    )


def _https_host(value: str) -> str:
    try:
        url = urlsplit(value)
        host = url.hostname or ""
        labels = host.split(".")
        if (
            url.scheme == "https" and url.netloc.lower() == host and len(host) <= 253
            and len(labels) > 1 and not labels[-1].isdigit()
            and all(_HOST_LABEL.fullmatch(label) for label in labels)
        ):
            return host
    except ValueError:
        pass
    raise AnalysisRejected("Outbound URLs require an exact, bare HTTPS hostname")


def _url_literal(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr) and node.values:
        first = node.values[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            prefix = first.value
            if "/" in prefix.partition("://")[2]:
                return prefix
    return None


def _dotted(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_dotted(node.value)}.{node.attr}"
    return ""


@dataclass
class _Analysis:
    read: SourceReader
    pins: dict[str, str] = field(default_factory=dict)
    packages: set[str] = field(default_factory=set)
    hosts: set[str] = field(default_factory=set)
    found: dict[str, bool] = field(default_factory=dict)
    remaining: int = MAX_BUNDLE_BYTES
    nodes: int = 0
    calls: int = 0

    async def load(self, path: str, entry_function: str | None = None) -> bool:
        if path in self.found:
            return self.found[path]
        if not _valid_path(path) or len(self.found) >= 128:
            raise AnalysisRejected("Workspace import paths exceed the analysis policy")
        content = await self.read(path, min(self.remaining, MAX_SOURCE_BYTES))
        self.found[path] = content is not None
        if content is None:
            return False
        if len(self.pins) >= MAX_MODULES or len(content) > min(self.remaining, MAX_SOURCE_BYTES):
            raise AnalysisRejected("Workspace sources exceed the bundle budget")
        self.remaining -= len(content)
        self.pins[path] = hashlib.sha256(content).hexdigest()
        syntax = ast.parse(content.decode("utf-8"), filename=path)
        if entry_function and not any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == entry_function
            for node in syntax.body
        ):
            raise AnalysisRejected("Workflow entry function was not found")

        aliases: dict[str, str] = {}
        for node in ast.walk(syntax):
            if isinstance(node, ast.Import):
                for item in node.names:
                    await self.resolve(item.name)
                    aliases[item.asname or item.name.split(".")[0]] = (
                        item.name if item.asname else item.name.split(".")[0]
                    )
            elif isinstance(node, ast.ImportFrom):
                await self.import_from(path, node)
                aliases.update({
                    item.asname or item.name: f"{node.module}.{item.name}" for item in node.names
                })

        for node in ast.walk(syntax):
            self.nodes += 1
            if self.nodes > _MAX_NODES:
                raise AnalysisRejected("Workspace sources exceed the analysis complexity budget")
            self.inspect(node, aliases)
        return True

    async def resolve(self, dotted: str, *, optional: bool = False) -> bool:
        parts = dotted.split(".")
        root = parts[0]
        if root in _UNSUPPORTED:
            raise AnalysisRejected(f"Unsupported Pyodide import: {root}")
        if root in _PACKAGES:
            self.packages.add(_PACKAGES[root])
            return True
        if root in _RESERVED:
            return True

        for index in range(1, len(parts)):
            await self.load("/".join(parts[:index]) + "/__init__.py")
        base = "/".join(parts)
        package = await self.load(base + "/__init__.py")
        module = await self.load(base + ".py")
        if package and module:
            raise AnalysisRejected(f"Ambiguous workspace import: {dotted}")
        if not (package or module or optional):
            raise AnalysisRejected(f"Unresolved import: {dotted}")
        return package or module

    async def import_from(self, path: str, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if node.level:
            parents = path.split("/")[:-1]
            if node.level > len(parents):
                raise AnalysisRejected("Relative import leaves the workspace package")
            module = ".".join([*parents[:len(parents) + 1 - node.level], *([module] if module else [])])
        if module.split(".")[0] in _RESERVED | _PACKAGES.keys():
            await self.resolve(module)
            if any(alias.name in _TRAPS or alias.name == "*" for alias in node.names):
                raise AnalysisRejected("Unsupported dynamic import or Pyodide runtime operation")
            return
        found = await self.resolve(module, optional=True)
        for alias in node.names:
            if alias.name == "*":
                raise AnalysisRejected("Wildcard workspace imports require manual refactoring")
            child = await self.resolve(f"{module}.{alias.name}", optional=True)
            if not (found or child):
                raise AnalysisRejected(f"Unresolved import: {module}.{alias.name}")

    def inspect(self, node: ast.AST, aliases: dict[str, str]) -> None:
        if isinstance(node, ast.JoinedStr) and node.values:
            first = node.values[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str) and first.value.startswith(
                ("http://", "https://")
            ) and _url_literal(node) is None:
                raise AnalysisRejected("An interpolated URL must have a fixed HTTPS origin")
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.startswith(
            ("http://", "https://")
        ):
            self.hosts.add(_https_host(node.value))
        if isinstance(node, ast.Attribute) and node.attr in _TRAPS:
            raise AnalysisRejected(f"Unsupported dynamic or Pyodide runtime operation: {node.attr}")
        if not isinstance(node, ast.Call):
            return
        name = _dotted(node.func)
        root, _, tail = name.partition(".")
        qualified = ".".join(filter(None, (aliases.get(root, root), tail)))
        if qualified.rsplit(".", 1)[-1] in _TRAPS:
            raise AnalysisRejected("Unsupported dynamic or Pyodide runtime operation")
        namespace, _, method = qualified.rpartition(".")
        if method in _HTTP_METHODS and (namespace.startswith(("httpx", "urllib")) or namespace == "js"):
            self.calls += 1
            index = 1 if method == "request" else 0
            url = node.args[index] if len(node.args) > index else next(
                (keyword.value for keyword in node.keywords if keyword.arg == "url"), None
            )
            literal = _url_literal(url) if url is not None else None
            if literal is None:
                raise AnalysisRejected("Outbound call has no statically identifiable HTTPS URL")
            self.hosts.add(_https_host(literal))
        elif namespace.startswith("bifrost."):
            self.calls += 1


async def analyze(
    read: SourceReader, entry_path: str, function_name: str | None,
    *, subrequest_allowance: int, cpu_budget_ms: int,
) -> dict[str, object]:
    """Build the canonical pins and bounded routing proposal without executing code."""
    if not (0 <= subrequest_allowance <= MAX_SUBREQUESTS and 1 <= cpu_budget_ms <= MAX_CPU_MS):
        raise AnalysisRejected("Invalid Cloudflare subrequest allowance or CPU budget")
    analysis = _Analysis(read)
    try:
        async with asyncio.timeout(20):
            if not await analysis.load(entry_path, function_name):
                raise AnalysisRejected("Workflow source is missing")
    except (SyntaxError, UnicodeError, RecursionError) as exc:
        raise AnalysisRejected("Workspace source could not be analyzed as Python") from exc
    # Static sizing signals. Repeated calls, input-dependent work and library
    # work are constrained by the runtime allowance and budget.
    estimated_subrequests = analysis.calls + len(analysis.pins)
    estimated_cpu_ms = max(1, (analysis.nodes + 24) // 25)
    if estimated_subrequests > subrequest_allowance:
        raise AnalysisRejected(f"Estimated subrequests ({estimated_subrequests}) exceed the allowance")
    if estimated_cpu_ms > cpu_budget_ms:
        raise AnalysisRejected(f"Estimated CPU ({estimated_cpu_ms} ms) exceeds the budget")
    if len(analysis.hosts) > 32:
        raise AnalysisRejected("Workspace sources exceed the HTTPS host budget")
    hosts = sorted(analysis.hosts)
    return {
        "entry_path": entry_path, "function_name": function_name,
        "manifest_sha256": digest(analysis.pins),
        "entry_sha256": analysis.pins.pop(entry_path), "module_sha256": analysis.pins,
        "packages": sorted(analysis.packages), "https_hosts": hosts, "required_https_hosts": hosts,
        "estimated_subrequests": estimated_subrequests, "subrequest_allowance": subrequest_allowance,
        "estimated_cpu_ms": estimated_cpu_ms, "cpu_budget_ms": cpu_budget_ms,
    }
