"""Static analysis shared by the offline census and workspace approval service."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

POLICY_VERSION = "cloudflare-workflow-approval-v2"
MAX_SOURCE_BYTES = 2 * 1024**2
MAX_BUNDLE_BYTES = 4 * 1024**2
MAX_MODULES = 32
_MAX_PARSE_FILE_BYTES = 128 * 1024
_MAX_PARSE_BUNDLE_BYTES = 512 * 1024
MAX_SUBREQUESTS = 50
MAX_CPU_MS = 2**53 - 1  # Integer range of the launcher wire.
_MAX_NODES = 100_000
_WORKSPACE_PATH = re.compile(r"(?:[a-zA-Z_][a-zA-Z0-9_]*/)*[a-zA-Z_][a-zA-Z0-9_]*\.py")
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_PROVIDED = {"bifrost", "js", "pyodide", "_pyodide"}
# Pyodide 314.0.6, CPython 3.14 WebAssembly. Captured from the packaged
# python314.zip top-level Python files plus sys.builtin_module_names.
_PYODIDE_STDLIB = frozenset(["__future__", "__hello__", "__phello__", "_abc", "_ast", "_ast_unparse", "_asyncio", "_bisect", "_blake2", "_bz2", "_codecs", "_codecs_cn", "_codecs_hk", "_codecs_iso2022", "_codecs_jp", "_codecs_kr", "_codecs_tw", "_collections", "_collections_abc", "_colorize", "_compat_pickle", "_contextvars", "_csv", "_ctypes", "_datetime", "_decimal", "_elementtree", "_functools", "_heapq", "_hmac", "_imp", "_io", "_json", "_locale", "_lsprof", "_lzma", "_markupbase", "_md5", "_multibytecodec", "_opcode", "_opcode_metadata", "_operator", "_pickle", "_posixsubprocess", "_py_abc", "_py_warnings", "_pydatetime", "_pyio", "_pylong", "_pyodide", "_pyodide_core", "_pyrepl", "_queue", "_random", "_remote_debugging", "_sha1", "_sha2", "_sha3", "_signal", "_sitebuiltins", "_socket", "_sqlite3", "_sre", "_ssl", "_stat", "_statistics", "_string", "_strptime", "_struct", "_suggestions", "_symtable", "_sysconfig", "_sysconfigdata__emscripten_wasm32-emscripten", "_testbuffer", "_testcapi", "_testclinic", "_testclinic_limited", "_testinternalcapi", "_testlimitedcapi", "_thread", "_threading_local", "_tokenize", "_tracemalloc", "_types", "_typing", "_warnings", "_weakref", "_weakrefset", "_xxtestfuzz", "_zoneinfo", "_zstd", "abc", "annotationlib", "antigravity", "argparse", "array", "ast", "asyncio", "atexit", "base64", "bdb", "binascii", "bisect", "builtins", "bz2", "cProfile", "calendar", "cmath", "cmd", "code", "codecs", "codeop", "collections", "colorsys", "compileall", "compression", "concurrent", "configparser", "contextlib", "contextvars", "copy", "copyreg", "csv", "ctypes", "dataclasses", "datetime", "decimal", "difflib", "dis", "doctest", "email", "encodings", "enum", "errno", "faulthandler", "fcntl", "filecmp", "fileinput", "fnmatch", "fractions", "ftplib", "functools", "gc", "genericpath", "getopt", "getpass", "gettext", "glob", "graphlib", "gzip", "hashlib", "heapq", "hmac", "html", "http", "imaplib", "importlib", "inspect", "io", "ipaddress", "itertools", "json", "keyword", "linecache", "locale", "logging", "lzma", "mailbox", "marshal", "math", "mimetypes", "mmap", "modulefinder", "multiprocessing", "netrc", "ntpath", "nturl2path", "numbers", "opcode", "operator", "optparse", "os", "pathlib", "pdb", "pickle", "pickletools", "pkgutil", "platform", "plistlib", "poplib", "posix", "posixpath", "pprint", "profile", "pstats", "pty", "py_compile", "pyclbr", "pydoc", "pyexpat", "pyodide", "queue", "quopri", "random", "re", "reprlib", "rlcompleter", "runpy", "sched", "secrets", "select", "selectors", "shelve", "shlex", "shutil", "signal", "site", "smtplib", "socket", "socketserver", "sqlite3", "sre_compile", "sre_constants", "sre_parse", "ssl", "stat", "statistics", "string", "stringprep", "struct", "subprocess", "symtable", "sys", "sysconfig", "tabnanny", "tarfile", "tempfile", "termios", "textwrap", "this", "threading", "time", "timeit", "token", "tokenize", "tomllib", "trace", "traceback", "tracemalloc", "tty", "types", "typing", "unicodedata", "unittest", "urllib", "uuid", "warnings", "wave", "weakref", "webbrowser", "wsgiref", "xml", "xmlrpc", "xxsubtype", "zipapp", "zipfile", "zipimport", "zlib", "zoneinfo"])
_RESERVED = _PYODIDE_STDLIB | _PROVIDED | {
    "main", "runtime_runner", "runtime_guard", "pyodide_refresh", "sdk_cleanup",
}
# Only distributions in the launcher build can be proposed. Earlier census
# import experiments with other packages do not add them to that build.
_PACKAGES = {
    "httpx": "httpx", "pydantic": "pydantic", "pydantic_core": "pydantic-core",
    "dotenv": "python-dotenv", "yaml": "pyyaml", "redis": "redis",
    "workers": "workers-runtime-sdk",
}
_UNSUPPORTED = {"ctypes", "fcntl", "importlib", "multiprocessing", "resource", "signal", "socket", "subprocess"}
_TRAPS = {"process_time", "process_time_ns", "fork", "forkpty", "import_module"}
_DYNAMIC = {"__import__", "eval", "exec", "compile", "vars", "globals"}
_HTTP_NAMESPACES = {"httpx", "urllib.request", "js", "js.globalThis", "workers", "pyodide.http"}
_HTTP_ROOTS = {name.split(".")[0] for name in _HTTP_NAMESPACES}
_HTTP_CLIENTS = {"httpx.Client", "httpx.AsyncClient"}
_HTTP_METHODS = {"fetch", "pyfetch", "request", "get", "head", "post", "put", "patch", "delete", "options", "stream", "urlopen"}

SourceReader = Callable[[str, int], Awaitable[bytes | None]]


class AnalysisRejected(ValueError):
    """Sources or requested bounds cannot be approved by this analyzer."""


class SourceUnresolved(AnalysisRejected):
    """The current observation may be missing an unindexed dependency."""


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
        parent = _dotted(node.value)
        return f"{parent}.{node.attr}" if parent else f".{node.attr}"
    return ""


def _qualified(node: ast.expr, aliases: dict[str, str]) -> str:
    root, _, tail = _dotted(node).partition(".")
    return ".".join(filter(None, (aliases.get(root, root), tail))) if root else f".{tail}"


@dataclass
class _Analysis:
    read: SourceReader
    pins: dict[str, str] = field(default_factory=dict)
    packages: set[str] = field(default_factory=set)
    hosts: set[str] = field(default_factory=set)
    found: dict[str, bool] = field(default_factory=dict)
    remaining: int = _MAX_PARSE_BUNDLE_BYTES
    nodes: int = 0
    calls: int = 0

    async def load(self, path: str, entry_function: str | None = None) -> bool:
        if path in self.found:
            return self.found[path]
        if not _valid_path(path) or len(self.found) >= 128:
            raise AnalysisRejected("Workspace import paths exceed the analysis policy")
        read_limit = min(self.remaining, _MAX_PARSE_FILE_BYTES)
        content = await self.read(path, read_limit)
        self.found[path] = content is not None
        if content is None:
            return False
        if len(self.pins) >= MAX_MODULES or len(content) > read_limit:
            raise AnalysisRejected("Workspace sources exceed the bundle budget")
        self.remaining -= len(content)
        self.pins[path] = hashlib.sha256(content).hexdigest()
        syntax = ast.parse(content.decode("utf-8"), filename=path)
        if entry_function and not any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == entry_function
            for node in syntax.body
        ):
            raise AnalysisRejected("Workflow entry function was not found")

        imports = self.inspect_module(syntax)
        del syntax, content
        for node in imports:
            if isinstance(node, ast.Import):
                for item in node.names:
                    await self.resolve(item.name)
            else:
                await self.import_from(path, node)
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
            raise SourceUnresolved(f"Unresolved import: {dotted}")
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
            if any(
                alias.name in _TRAPS or alias.name == "*"
                or (module == "builtins" and alias.name in _DYNAMIC)
                for alias in node.names
            ):
                raise AnalysisRejected("Unsupported dynamic import or Pyodide runtime operation")
            return
        found = await self.resolve(module, optional=True)
        for alias in node.names:
            if alias.name == "*":
                raise AnalysisRejected("Wildcard workspace imports require manual refactoring")
            child = await self.resolve(f"{module}.{alias.name}", optional=True)
            if not (found or child):
                raise SourceUnresolved(f"Unresolved import: {module}.{alias.name}")

    def inspect_module(self, syntax: ast.Module) -> list[ast.Import | ast.ImportFrom]:
        aliases: dict[str, str] = {}
        imports: list[ast.Import | ast.ImportFrom] = []
        for node in ast.walk(syntax):
            self.nodes += 1
            if self.nodes > _MAX_NODES:
                raise AnalysisRejected("Workspace sources exceed the analysis complexity budget")
            if isinstance(node, ast.Import):
                imports.append(node)
                for item in node.names:
                    root = item.name.split(".")[0]
                    aliases[item.asname or root] = item.name if item.asname else root
            elif isinstance(node, ast.ImportFrom):
                imports.append(node)
                if not node.level:
                    aliases.update({item.asname or item.name: f"{node.module}.{item.name}" for item in node.names})

        clients: set[str] = set()
        dynamic_getattr_calls: set[int] = set()
        for node in ast.walk(syntax):
            if isinstance(node, ast.Call) and _qualified(node.func, aliases) in {"getattr", "builtins.getattr"}:
                dynamic_getattr_calls.add(id(node.func))
            if isinstance(node, ast.withitem):
                value, targets = node.context_expr, [node.optional_vars]
            elif isinstance(node, ast.Assign):
                value, targets = node.value, node.targets
            elif isinstance(node, ast.AnnAssign):
                value, targets = node.value, [node.target]
            else:
                continue
            if isinstance(value, ast.Call) and _qualified(value.func, aliases) in _HTTP_CLIENTS:
                clients.update(target.id for target in targets if isinstance(target, ast.Name))

        has_http_import = any(name.split(".")[0] in _HTTP_ROOTS for name in aliases.values())
        for node in ast.walk(syntax):
            self.inspect(node, aliases, clients, has_http_import, dynamic_getattr_calls)
        return imports

    def inspect(
        self, node: ast.AST, aliases: dict[str, str], clients: set[str],
        has_http_import: bool, dynamic_getattr_calls: set[int],
    ) -> None:
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
        if isinstance(node, (ast.Name, ast.Attribute)) and isinstance(node.ctx, ast.Load):
            qualified = _qualified(node, aliases)
            if (
                qualified in _DYNAMIC or qualified.removeprefix("builtins.") in _DYNAMIC
                or qualified == "sys.modules" or qualified.rsplit(".", 1)[-1] in _TRAPS
                or (qualified in {"getattr", "builtins.getattr"} and id(node) not in dynamic_getattr_calls)
            ):
                raise AnalysisRejected("Unsupported dynamic or Pyodide runtime operation")
        if not isinstance(node, ast.Call):
            return
        qualified = _qualified(node.func, aliases)
        if qualified in {"getattr", "builtins.getattr"}:
            attr = node.args[1] if len(node.args) > 1 else None
            if (
                not isinstance(attr, ast.Constant) or not isinstance(attr.value, str)
                or attr.value in _DYNAMIC | _TRAPS | {"getattr", "modules", "__dict__", *_HTTP_METHODS}
            ):
                raise AnalysisRejected("Unsupported dynamic attribute lookup")
        namespace, _, method = qualified.rpartition(".")
        if (
            isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Call)
            and _qualified(node.func.value.func, aliases) in _HTTP_CLIENTS
        ):
            namespace = "httpx"
        if namespace.startswith("bifrost."):
            self.calls += 1
            return
        if method not in _HTTP_METHODS:
            return
        index = 1 if method in {"request", "stream"} else 0
        url = node.args[index] if len(node.args) > index else next(
            (keyword.value for keyword in node.keywords if keyword.arg in {"url", "fullurl"}), None
        )
        literal = _url_literal(url) if url is not None else None
        if namespace not in _HTTP_NAMESPACES and namespace not in clients:
            if (
                method != "get" or literal is None or has_http_import
                or (literal is not None and literal.startswith(("http://", "https://")))
            ):
                raise AnalysisRejected("Outbound call has no statically identifiable HTTP receiver")
            return
        if literal is None:
            raise AnalysisRejected("Outbound call has no statically identifiable HTTPS URL")
        self.calls += 1
        self.hosts.add(_https_host(literal))



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
                raise SourceUnresolved("Workflow source is missing")
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
