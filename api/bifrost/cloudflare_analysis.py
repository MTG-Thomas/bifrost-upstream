"""Static analysis shared by the offline census and workspace approval service."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import io
import json
import re
import sys
import tokenize
from collections import Counter
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

POLICY_VERSION = "cloudflare-workflow-approval-v2"
MAX_SOURCE_BYTES = 2 * 1024**2
MAX_BUNDLE_BYTES = 4 * 1024**2
MAX_MODULES = 32
_MAX_PARSE_FILE_BYTES = 384 * 1024
_MAX_PARSE_BUNDLE_BYTES = 1024 * 1024
_MAX_TOKEN_WEIGHT = 75_000
_MAX_URL_PREFIX = 4096
_MAX_URL_PARTS = 4096
_MAX_NAME_LENGTH = 512
_MAX_FINDINGS = 256
_MAX_CALL_HINTS = 2048
MAX_SUBREQUESTS = 50
MAX_CPU_MS = 2**53 - 1  # Integer range of the launcher wire.
_MAX_NODES = 100_000
_MAX_ACTIVE_SCOPE_BINDINGS = 200_000
_MAX_SCOPE_COPY_WORK = 500_000
_MAX_SELECTOR_WORK = 1_000_000
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_PROVIDED = {"bifrost", "js", "pyodide", "_pyodide"}
# Pyodide 314.0.6, CPython 3.14 WebAssembly. Verified in Workerd against
# python314.zip top-level Python roots plus sys.builtin_module_names.
# Workerd bundle SHA-256: 3c3fd5a4179230e21e018e28b0e735e7c4abd8fe8e4ff39d2fd6bb2ea3a2e260.
_PYODIDE_STDLIB = frozenset([
    "__future__", "__hello__", "__phello__", "_abc", "_ast", "_ast_unparse", "_asyncio", "_bisect",
    "_blake2", "_bz2", "_codecs", "_codecs_cn", "_codecs_hk", "_codecs_iso2022", "_codecs_jp",
    "_codecs_kr", "_codecs_tw", "_collections", "_collections_abc", "_colorize", "_compat_pickle",
    "_contextvars", "_csv", "_ctypes", "_datetime", "_decimal", "_elementtree", "_functools",
    "_heapq", "_hmac", "_imp", "_io", "_json", "_locale", "_lsprof", "_lzma", "_markupbase", "_md5",
    "_multibytecodec", "_opcode", "_opcode_metadata", "_operator", "_pickle", "_posixsubprocess",
    "_py_abc", "_py_warnings", "_pydatetime", "_pyio", "_pylong", "_pyodide", "_pyodide_core",
    "_pyrepl", "_queue", "_random", "_remote_debugging", "_sha1", "_sha2", "_sha3", "_signal",
    "_sitebuiltins", "_socket", "_sqlite3", "_sre", "_ssl", "_stat", "_statistics", "_string",
    "_strptime", "_struct", "_suggestions", "_symtable", "_sysconfig",
    "_sysconfigdata__emscripten_wasm32-emscripten", "_testbuffer", "_testcapi", "_testclinic",
    "_testclinic_limited", "_testinternalcapi", "_testlimitedcapi", "_thread", "_threading_local",
    "_tokenize", "_tracemalloc", "_types", "_typing", "_warnings", "_weakref", "_weakrefset",
    "_xxtestfuzz", "_zoneinfo", "_zstd", "abc", "annotationlib", "antigravity", "argparse", "array",
    "ast", "asyncio", "atexit", "base64", "bdb", "binascii", "bisect", "builtins", "bz2",
    "cProfile", "calendar", "cmath", "cmd", "code", "codecs", "codeop", "collections", "colorsys",
    "compileall", "compression", "concurrent", "configparser", "contextlib", "contextvars", "copy",
    "copyreg", "csv", "ctypes", "dataclasses", "datetime", "decimal", "difflib", "dis", "doctest",
    "email", "encodings", "enum", "errno", "faulthandler", "fcntl", "filecmp", "fileinput",
    "fnmatch", "fractions", "ftplib", "functools", "gc", "genericpath", "getopt", "getpass",
    "gettext", "glob", "graphlib", "gzip", "hashlib", "heapq", "hmac", "html", "http", "imaplib",
    "importlib", "inspect", "io", "ipaddress", "itertools", "json", "keyword", "linecache",
    "locale", "logging", "lzma", "mailbox", "marshal", "math", "mimetypes", "mmap", "modulefinder",
    "multiprocessing", "netrc", "ntpath", "nturl2path", "numbers", "opcode", "operator", "optparse",
    "os", "pathlib", "pdb", "pickle", "pickletools", "pkgutil", "platform", "plistlib", "poplib",
    "posix", "posixpath", "pprint", "profile", "pstats", "pty", "py_compile", "pyclbr", "pydoc",
    "pyexpat", "pyodide", "queue", "quopri", "random", "re", "reprlib", "rlcompleter", "runpy",
    "sched", "secrets", "select", "selectors", "shelve", "shlex", "shutil", "signal", "site",
    "smtplib", "socket", "socketserver", "sqlite3", "sre_compile", "sre_constants", "sre_parse",
    "ssl", "stat", "statistics", "string", "stringprep", "struct", "subprocess", "symtable", "sys",
    "sysconfig", "tabnanny", "tarfile", "tempfile", "termios", "textwrap", "this", "threading",
    "time", "timeit", "token", "tokenize", "tomllib", "trace", "traceback", "tracemalloc", "tty",
    "types", "typing", "unicodedata", "unittest", "urllib", "uuid", "warnings", "wave", "weakref",
    "webbrowser", "wsgiref", "xml", "xmlrpc", "xxsubtype", "zipapp", "zipfile", "zipimport", "zlib",
    "zoneinfo",
])
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
_UNSUPPORTED = {"ctypes", "fcntl", "code", "codeop", "importlib", "multiprocessing", "runpy", "resource", "signal", "socket", "subprocess"}
_TRAPS = {"process_time", "process_time_ns", "fork", "forkpty", "import_module"}
_DYNAMIC = {"__import__", "eval", "exec", "compile", "vars", "globals"}
_HTTP_CLIENTS = {"httpx.Client", "httpx.AsyncClient"}
_FETCH_FUNCTIONS = {"js.fetch", "js.globalThis.fetch", "pyodide.http.pyfetch", "workers.fetch"}
_BLOCKED_ATTRIBUTES = { "__subclasses__", "__globals__", "__code__", "__builtins__"}
_REFLECTIVE_SELECTORS = _DYNAMIC | _TRAPS | _BLOCKED_ATTRIBUTES | {"getattr", "modules"}
_REFLECTION = {"sys.modules", "operator.attrgetter", "operator.methodcaller", "pydoc.locate", "pkgutil.resolve_name"}
_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
_HTTP_METHODS = {"request", "get", "head", "post", "put", "patch", "delete", "options", "stream", "send"}
_URLLIB_CALLS = {"urllib.request.urlopen", "urllib.request.urlretrieve"}
_REQUEST_BUILDERS = {"httpx.Request", "urllib.request.Request"}
_ROUTE_POLICY = json.loads(Path(__file__).with_name("cloudflare_sdk_routes.json").read_text(encoding="utf-8"))
_ROUTE_PATTERNS: list[tuple[re.Pattern[str], list[str]]] = [
    (re.compile(re.sub(r"\{(\w+)\}", lambda match: f"(?:{_ROUTE_POLICY['identifiers'][match[1]]})", path)), methods)
    for path, methods in _ROUTE_POLICY["routes"].items()
]
_ROUTE_EXAMPLE_ID = "11111111-1111-4111-8111-111111111111"
_SDK_AREA_ROUTES = {
    "agents": "/api/agent-runs", "ai": "/api/sdk/ai", "artifacts": "/api/sdk/artifacts",
    "config": "/api/sdk/config", "events": "/api/events", "executions": "/api/executions",
    "files": "/api/files", "forms": "/api/forms", "integrations": "/api/sdk/integrations",
    "knowledge": "/api/sdk/knowledge", "organizations": "/api/organizations",
    "tables": "/api/tables", "workflows": "/api/workflows",
}
_SDK_IDENTITY_OPERATIONS = {
    "list": ("GET", ""), "create": ("POST", ""), "get": ("GET", f"/{_ROUTE_EXAMPLE_ID}"),
    "update": ("PATCH", f"/{_ROUTE_EXAMPLE_ID}"), "delete": ("DELETE", f"/{_ROUTE_EXAMPLE_ID}"),
}
_SDK_ROLE_OPERATIONS = {
    **_SDK_IDENTITY_OPERATIONS,
    "list_users": ("GET", f"/{_ROUTE_EXAMPLE_ID}/users"), "assign_users": ("POST", f"/{_ROUTE_EXAMPLE_ID}/users"),
    "list_forms": ("GET", f"/{_ROUTE_EXAMPLE_ID}/forms"), "assign_forms": ("POST", f"/{_ROUTE_EXAMPLE_ID}/forms"),
}

SourceReader = Callable[[str, int], Awaitable[bytes | None]]


class AnalysisRejected(ValueError):
    """Sources or requested bounds cannot be approved by this analyzer."""

    def __init__(self, message: str, rule: str = "CF_SOURCE", *, detail: str | None = None):
        super().__init__(message)
        self.rule = rule
        self.detail = detail
        self.path: str | None = None
        self.line: int | None = None
        self.column: int | None = None

    def locate(self, path: str, node: ast.AST | None = None) -> None:
        if self.path is None:
            self.path = path
            self.line = getattr(node, "lineno", None)
            offset = getattr(node, "col_offset", None)
            self.column = offset + 1 if offset is not None else None


class SourceUnresolved(AnalysisRejected):
    """An indexed source could not be read from storage."""


_HINTS = {
    "CF_PROCESS": "Replace process creation with Python code or keep this step on Linux.",
    "CF_TRANSPORT": "Use httpx with an HTTPS URL and a hostname allowed by the operator.",
    "CF_PACKAGE": "Replace the import with an available package, bundle compatible Python source, or ask the operator to qualify a Pyodide build.",
    "CF_NATIVE": "Keep the operation on its native runner, or replace it with a WebAssembly-compatible implementation.",
    "CF_DYNAMIC": "Use explicit imports, direct method calls, and a static dispatch table.",
    "CF_PARSE_BUDGET": "Reduce the file size, split the entry into smaller modules, or trim modules from the imported bundle.",
    "CF_SUBREQUESTS": "Batch or remove API and HTTP calls, or review the subrequest allowance with the operator.",
    "CF_CPU": "Reduce the imported code or review the CPU budget with the operator.",
    "CF_HOST_BUDGET": "Reduce the number of distinct outbound HTTPS hosts.",
    "CF_SOURCE": "Check the Python source, entry function, and imports in the approved workspace or install.",
    "CF_SDK_GATEWAY": "Move the SDK operation to a Linux workflow or use a supported SDK area.",
    "CF_URL_REVIEW": "Use a statically identifiable HTTPS origin or have the operator review and approve the possible hostnames.",
    "CF_INLINE": "Save and register the code as a Normal workflow, then request source approval.",
    "CF_FAST_PATH": "Use the Normal lane if its scheduling semantics fit this workflow.",
    "CF_OUTBOUND": "Keep the install on Linux, or vendor its dependencies and review whether it can use install-local access only.",
    "CF_ARTIFACT_WORKSPACE": "Keep this run on Linux or remove its dependency on an artifact workspace.",
    "CF_EXECUTABLE_TYPE": "Keep this executable on its current runtime. Use a Normal workflow for an independently approved Workers step.",
    "CF_AGENT": "Keep agent runs on Bifrost. Agent summaries can use Workers when the operator enables the summary handler.",
    "CF_CONTAINERS_QUALIFICATION_DEFERRED": "Review the qualification allowance and reset time on the Capacity page.",
}


@dataclass(frozen=True)
class Finding:
    rule_id: str
    severity: Literal["warning", "info"]
    message: str
    hint: str
    path: str | None = None
    line: int | None = None
    column: int | None = None

    @classmethod
    def from_refusal(cls, error: AnalysisRejected) -> Finding:
        severity: Literal["warning", "info"] = "info" if error.rule == "CF_URL_REVIEW" else "warning"
        prefix = "Cloudflare host review" if severity == "info" else "Won't burst to Cloudflare"
        message = error.detail or str(error)
        return cls(error.rule, severity, f"{prefix}: {message}.", _HINTS[error.rule], error.path, error.line, error.column)

    @property
    def text(self) -> str:
        return f"[{self.rule_id}] {self.message} {self.hint}"


def placement_findings(
    *, executable_type: str = "workflow", execution_lane: str | None = None,
    inline: bool = False, artifact_workspace: bool = False, outbound: bool = False,
    path: str | None = None,
) -> list[Finding]:
    reasons = []
    if inline:
        reasons.append(("CF_INLINE", "Inline code has no approved, pinned workspace entry"))
    if artifact_workspace:
        reasons.append(("CF_ARTIFACT_WORKSPACE", "Runs with an artifact workspace require the Linux execution path"))
    if execution_lane == "fast_path":
        reasons.append(("CF_FAST_PATH", "Cloudflare supplies Normal capacity only"))
    if outbound:
        reasons.append(("CF_OUTBOUND", "The Solution install allows outbound access beyond its pinned source namespace"))
    if executable_type != "workflow":
        reasons.append(("CF_EXECUTABLE_TYPE", f"The {executable_type} executable type has no Workers placement"))
    findings = []
    for rule, reason in reasons:
        refusal = AnalysisRejected(reason, rule)
        if path is not None:
            refusal.locate(path)
        findings.append(Finding.from_refusal(refusal))
    return findings


def agent_finding() -> Finding:
    return Finding("CF_AGENT", "info", "Agent runs stay on Bifrost.", _HINTS["CF_AGENT"])


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def valid_source_path(path: str) -> bool:
    parts = path.split("/")
    root = parts[0].removesuffix(".py")
    return (
        len(path) <= 512 and path.endswith(".py")
        and "\\" not in path and "\0" not in path
        and not any(part in {"", ".", "..", ".git"} for part in parts)
        and root not in (_RESERVED | _PACKAGES.keys())
    )


def _exact_host(host: str) -> bool:
    labels = host.split(".")
    return (
        len(host) <= 253 and len(labels) > 1 and not labels[-1].isdigit()
        and all(_HOST_LABEL.fullmatch(label) for label in labels)
    )


def _https_host(value: str) -> str | None:
    try:
        url = urlsplit(value)
        host = url.hostname or ""
        if url.scheme == "https" and url.netloc.lower() == host and _exact_host(host):
            return host
    except ValueError:
        pass
    return None


def reviewed_https_hosts(hosts: list[str]) -> list[str]:
    if any(not _exact_host(host.lower()) for host in hosts):
        raise AnalysisRejected("Operator HTTPS hosts must be exact hostnames without ports or wildcards")
    return sorted({host.lower() for host in hosts})


@dataclass(frozen=True)
class _URL:
    prefix: str
    complete: bool = True

    def host(self, base_host: str | None = None) -> str | None:
        if (
            base_host is not None and self.prefix and self.prefix == self.prefix.strip()
            and not self.prefix.startswith("//") and "\\" not in self.prefix
            and ":" not in self.prefix.partition("/")[0]
        ):
            return base_host
        authority = self.prefix.partition("://")[2]
        if self.complete or any(char in authority for char in "/?#"):
            return _https_host(self.prefix)
        return None


def _url_literal(node: ast.expr, constants: dict[str, str]) -> _URL | None:
    remaining = _MAX_URL_PREFIX
    parts: list[str] = []
    stack = [iter((node,))]
    visited = 0
    while stack:
        item = next(stack[-1], None)
        if item is None:
            stack.pop()
            continue
        visited += 1
        if visited > _MAX_URL_PARTS:
            return _URL("".join(parts), False)
        if isinstance(item, ast.FormattedValue) and item.conversion == -1 and item.format_spec is None:
            item = item.value
        if isinstance(item, ast.BinOp) and isinstance(item.op, ast.Add):
            stack.append(iter((item.left, item.right)))
            continue
        if isinstance(item, ast.JoinedStr):
            stack.append(iter(item.values))
            continue
        part = item.value if isinstance(item, ast.Constant) else constants.get(item.id) if isinstance(item, ast.Name) else None
        if not isinstance(part, str):
            return _URL("".join(parts), False) if parts else None
        if len(part) > remaining:
            return _URL("".join(parts) + part[:remaining], False)
        remaining -= len(part)
        parts.append(part)
    return _URL("".join(parts))


def _check_tokens(source: str) -> None:
    """Account for dense syntax before allocating the parser arena."""
    remaining = _MAX_TOKEN_WEIGHT
    try:
        for token in tokenize.generate_tokens(io.StringIO(source, newline=None).readline):
            remaining -= 1
            # Before PEP 701 a whole f-string, including its expressions, is
            # emitted as one STRING token. Charge its characters conservatively.
            if sys.version_info < (3, 12) and token.type == tokenize.STRING:
                prefix = token.string[:2].lower()
                if prefix.startswith(("f", "rf")):
                    remaining -= len(token.string)
            if remaining < 0:
                raise AnalysisRejected("Python file exceeds the analysis token budget", "CF_PARSE_BUDGET")
    except tokenize.TokenError as exc:
        message, (line, column) = exc.args
        # EOF failures can refer to a synthetic line beyond the file.
        content = source.rstrip("\r\n")
        last_line = 1 + content.count("\n") + content.count("\r") - content.count("\r\n")
        offset = column + 1 if sys.version_info < (3, 12) else max(1, column)
        raise SyntaxError(message, (None, min(line, last_line), offset, None)) from exc


def _api_path_allowed(method: str, path: str) -> bool:
    return any(
        path == prefix or path.startswith(prefix if prefix.endswith("/") else f"{prefix}/")
        for prefix in _ROUTE_POLICY["prefixes"]
    ) or any(method in methods and pattern.fullmatch(path) for pattern, methods in _ROUTE_PATTERNS)


def _raw_api_allowed(method: str, literal: _URL | None) -> bool:
    if not literal or not literal.prefix or "\\" in literal.prefix:
        return False
    try:
        url = urlsplit(literal.prefix)
    except ValueError:
        return False
    if url.scheme or url.netloc:
        if (url.scheme, url.netloc) != ("https", "test-api.invalid"):
            return False
    path = url.path
    if not path.startswith("/") or any(part.lower().replace("%2e", ".") in {".", ".."} for part in path.split("/")):
        return False
    if literal.complete or "?" in literal.prefix or "#" in literal.prefix:
        return _api_path_allowed(method, path)
    # An unfinished segment could be a different root. Recognize only
    # established path segments, with a representative final identifier.
    return path.endswith("/") and _api_path_allowed(method, path + _ROUTE_EXAMPLE_ID)


def _sdk_routes_allowed(area: str, method: str) -> bool:
    if area in {"users", "roles"}:
        operations = _SDK_ROLE_OPERATIONS if area == "roles" else _SDK_IDENTITY_OPERATIONS
        operation = operations.get(method)
        return operation is not None and _api_path_allowed(operation[0], f"/api/{area}{operation[1]}")
    if (area, method) in {("files", "move"), ("files", "move_many"), ("artifacts", "create_video")}:
        if not _api_path_allowed("GET", f"/api/platform-jobs/{_ROUTE_EXAMPLE_ID}"):
            return False
    if area == "executions" and method == "get_current_logs":
        return _api_path_allowed("GET", "/api/sdk/runtime/logs/example")
    path = _SDK_AREA_ROUTES.get(area)
    return path is None or _api_path_allowed("GET", path)


def _dotted(node: ast.expr) -> str:
    parts = []
    remaining = _MAX_NAME_LENGTH
    while isinstance(node, ast.Attribute):
        remaining -= len(node.attr) + 1
        if remaining < 0:
            raise AnalysisRejected("Python name exceeds the analysis budget", "CF_PARSE_BUDGET")
        parts.append(node.attr)
        node = node.value
    root = node.id if isinstance(node, ast.Name) else ""
    if len(root) > remaining:
        raise AnalysisRejected("Python name exceeds the analysis budget", "CF_PARSE_BUDGET")
    parts.append(root)
    return ".".join(reversed(parts))


def _qualified(node: ast.expr, aliases: dict[str, str]) -> str:
    root, _, tail = _dotted(node).partition(".")
    if not root:
        return f".{tail}"
    resolved = aliases.get(root)
    if resolved is None:
        resolved = f"builtins.{root}" if root in _DYNAMIC | {"getattr", "__builtins__"} else f"unbound.{root}"
    if len(resolved) + len(tail) > _MAX_NAME_LENGTH:
        raise AnalysisRejected("Python name exceeds the analysis budget", "CF_PARSE_BUDGET")
    return f"{resolved}.{tail}" if tail else resolved


def _scoped_nodes(root: ast.AST) -> Iterator[ast.AST]:
    """Walk one lexical scope. Headers of nested functions run in this scope."""
    stack = [root]
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            children = [*node.decorator_list, *node.args.defaults, *filter(None, node.args.kw_defaults)]
        elif isinstance(node, ast.ClassDef):
            children = [*node.bases, *node.keywords, *node.decorator_list]
        elif isinstance(node, ast.Lambda):
            children = [*node.args.defaults, *filter(None, node.args.kw_defaults)]
        else:
            children = list(ast.iter_child_nodes(node))
        stack.extend(reversed(children))


def _argument(node: ast.Call, index: int, *keywords: str) -> ast.expr | None:
    if len(node.args) > index:
        return node.args[index]
    return next((item.value for item in node.keywords if item.arg in keywords), None)


def _bound_names(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
        return {node.id}
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return {node.name}
    if isinstance(node, ast.arg):
        return {node.arg}
    if isinstance(node, ast.alias):
        return {node.asname or node.name.split(".")[0]}
    if isinstance(node, (ast.ExceptHandler, ast.MatchAs, ast.MatchStar)) and node.name:
        return {node.name}
    if isinstance(node, ast.MatchMapping) and node.rest:
        return {node.rest}
    if isinstance(node, (ast.Global, ast.Nonlocal)):
        return set(node.names)
    return set()


def _literal_helper_selectors(syntax: ast.Module, entry_functions: tuple[str, ...]) -> set[int]:
    """Recognize a private entry-local helper called only with literal selectors."""
    helpers = [
        node for node in syntax.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("_") and not node.name.startswith("__")
        and node.name not in entry_functions and not node.decorator_list
    ]
    if not helpers:
        return set()
    nodes = list(ast.walk(syntax))
    direct = {
        id(node.func): node for node in nodes
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    bindings = Counter(name for node in nodes for name in _bound_names(node))
    references: dict[str, list[ast.Name]] = {}
    reflected_names = set()
    for node in nodes:
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            references.setdefault(node.id, []).append(node)
        elif isinstance(node, ast.Attribute):
            reflected_names.add(node.attr)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            reflected_names.add(node.value)

    resolved = set()
    remaining = _MAX_SELECTOR_WORK
    for helper in helpers:
        if bindings[helper.name] != 1 or helper.name in reflected_names:
            continue
        uses = references.get(helper.name, [])
        if not uses or any(id(use) not in direct for use in uses):
            continue
        calls = [direct[id(use)] for use in uses]
        if any(isinstance(arg, ast.Starred) for call in calls for arg in call.args):
            continue
        scope = [node for stmt in helper.body for node in _scoped_nodes(stmt)]
        rebound = {name for stmt in helper.body for node in ast.walk(stmt) for name in _bound_names(node)}
        parameters = helper.args.posonlyargs + helper.args.args
        remaining -= len(parameters) * (len(scope) + sum(1 + len(call.keywords) for call in calls))
        if remaining < 0:
            raise AnalysisRejected("Python selector analysis exceeds its work budget", "CF_PARSE_BUDGET")
        for index, parameter in enumerate(parameters):
            selectors = [
                node for node in scope
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "getattr" and len(node.args) >= 2
                and not any(isinstance(arg, ast.Starred) for arg in node.args)
                and isinstance(node.args[1], ast.Name) and node.args[1].id == parameter.arg
            ]
            if not selectors or parameter.arg in rebound:
                continue
            keywords = () if index < len(helper.args.posonlyargs) else (parameter.arg,)
            values = [_argument(call, index, *keywords) for call in calls]
            if all(
                isinstance(value, ast.Constant) and isinstance(value.value, str)
                and value.value.isidentifier() and not value.value.startswith("_")
                and value.value not in _REFLECTIVE_SELECTORS
                and all(not ref.endswith(f".{value.value}") for ref in _REFLECTION)
                for value in values
            ):
                resolved.update(map(id, selectors))
    return resolved


@dataclass(frozen=True)
class _Client:
    host: str | None


def _client(
    node: ast.expr, aliases: dict[str, str], constants: dict[str, str],
    clients: dict[str, _Client], factories: dict[str, _Client],
) -> _Client | None:
    if isinstance(node, ast.Call):
        qualified = _qualified(node.func, aliases)
        if qualified in _HTTP_CLIENTS:
            base = next((item.value for item in node.keywords if item.arg == "base_url"), None)
            literal = _url_literal(base, constants) if base is not None else None
            return _Client(literal.host() if literal else None)
        return factories.get(_dotted(node.func))
    return clients.get(_dotted(node))


def _http_target(
    node: ast.expr, aliases: dict[str, str], constants: dict[str, str],
    clients: dict[str, _Client], factories: dict[str, _Client],
) -> tuple[str, _Client] | None:
    qualified = _qualified(node, aliases)
    namespace, _, method = qualified.rpartition(".")
    if qualified in _FETCH_FUNCTIONS | _URLLIB_CALLS:
        return method, _Client(None)
    if method not in _HTTP_METHODS | {"build_request"}:
        return None
    if namespace == "httpx" or namespace in _HTTP_CLIENTS:
        return method, _Client(None)
    if isinstance(node, ast.Attribute):
        client = _client(node.value, aliases, constants, clients, factories)
        if client is not None:
            return method, client
    return None


@dataclass(frozen=True)
class _CallHint:
    receiver: str
    url: _URL | None
    path: str
    node: ast.Call


def _import_module(path: str, node: ast.ImportFrom) -> str:
    module = node.module or ""
    if not node.level:
        return module
    parents = path.split("/")[:-1]
    if node.level > len(parents):
        raise AnalysisRejected("Relative import leaves the workspace package")
    return ".".join([*parents[:len(parents) + 1 - node.level], *([module] if module else [])])


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
    unresolved_url_calls: int = 0
    url_review_count: int = 0
    first_url_review: tuple[str, int | None, int | None] | None = None
    exports: dict[str, _Client] = field(default_factory=dict)
    call_hints: list[_CallHint] = field(default_factory=list)
    single_file: bool = False
    findings: list[Finding] | None = None
    linux_packages: dict[str, str] | None = None
    linux_findings: set[str] = field(default_factory=set)
    local_helpers_entry: str | None = None
    literal_selectors: dict[int, str] = field(default_factory=dict)
    scope_copy_work: int = 0

    def observe(self, rule: str, message: str, path: str, node: ast.AST) -> None:
        if self.linux_packages is not None and rule == "CF_PROCESS":
            self.linux_findings.add(rule)
        if self.findings is not None:
            if len(self.findings) == _MAX_FINDINGS:
                raise AnalysisRejected("Workspace sources exceed the diagnostics budget", "CF_PARSE_BUDGET")
            refusal = AnalysisRejected(message, rule)
            refusal.locate(path, node)
            self.findings.append(Finding.from_refusal(refusal))

    async def load(self, path: str, entry_functions: tuple[str, ...] = ()) -> bool:
        try:
            return await self._load(path, entry_functions)
        except AnalysisRejected as exc:
            exc.locate(path)
            raise
        except (SyntaxError, UnicodeError, RecursionError) as exc:
            refusal = AnalysisRejected("Workspace source could not be analyzed as Python")
            refusal.locate(path)
            if isinstance(exc, SyntaxError):
                refusal.line, refusal.column = exc.lineno, exc.offset
            raise refusal from exc

    async def _load(self, path: str, entry_functions: tuple[str, ...]) -> bool:
        if path in self.found:
            if path == self.local_helpers_entry:
                raise AnalysisRejected("Entry-local attribute selectors cannot be used in an imported module")
            return self.found[path]
        if not valid_source_path(path) or len(self.found) >= 128:
            raise AnalysisRejected("Workspace import paths exceed the analysis policy")
        read_limit = min(self.remaining, _MAX_PARSE_FILE_BYTES)
        content = await self.read(path, read_limit)
        self.found[path] = content is not None
        if content is None:
            return False
        if len(self.pins) >= MAX_MODULES:
            raise AnalysisRejected(
                "Workspace sources exceed the bundle budget", "CF_PARSE_BUDGET",
                detail=f"Workspace bundle contains more than {MAX_MODULES} Python modules",
            )
        if len(content) > read_limit:
            reason = (
                f"Imported sources exceed the {_MAX_PARSE_BUNDLE_BYTES // 1024} KiB parse budget"
                if read_limit < _MAX_PARSE_FILE_BYTES
                else f"Python file exceeds the {_MAX_PARSE_FILE_BYTES // 1024} KiB parse budget"
            )
            raise AnalysisRejected("Workspace sources exceed the bundle budget", "CF_PARSE_BUDGET", detail=reason)
        self.remaining -= len(content)
        self.pins[path] = hashlib.sha256(content).hexdigest()
        source = content.decode("utf-8")
        _check_tokens(source)
        try:
            syntax = ast.parse(source, filename=path)
        except MemoryError as exc:
            raise AnalysisRejected("Workspace source exceeded the analysis memory limit", "CF_PARSE_BUDGET") from exc
        except ValueError as exc:
            raise AnalysisRejected("Workspace source could not be analyzed as Python") from exc
        del source, content
        if entry_functions:
            defined = {
                node.name for node in syntax.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            if any(name not in defined for name in entry_functions):
                raise AnalysisRejected("Workflow entry function was not found")

        imports = self.inspect_module(syntax, path, entry_functions)
        del syntax
        for node in imports:
            try:
                if isinstance(node, ast.Import):
                    for item in node.names:
                        await self.resolve(item.name)
                else:
                    await self.import_from(path, node)
            except AnalysisRejected as exc:
                exc.locate(path, node)
                raise
        return True

    async def resolve(self, dotted: str, *, optional: bool = False) -> bool:
        if len(dotted) > _MAX_NAME_LENGTH:
            raise AnalysisRejected("Python import exceeds the analysis budget", "CF_PARSE_BUDGET")
        parts = dotted.split(".")
        root = parts[0]
        if self.linux_packages is not None:
            if root in {"subprocess", "multiprocessing"}:
                self.linux_findings.add("CF_PROCESS")
                return True
            if root in {"_ssl", "ctypes", "fcntl", "resource", "signal"}:
                self.linux_findings.add("CF_NATIVE")
                return True
            if root in self.linux_packages and root not in _UNSUPPORTED:
                self.linux_findings.add("CF_PACKAGE")
                self.packages.add(self.linux_packages[root])
                return True
        if root in _UNSUPPORTED and dotted != "importlib.metadata":
            rule = "CF_DYNAMIC"
            if root in {"subprocess", "multiprocessing"}:
                rule = "CF_PROCESS"
            elif root == "socket":
                rule = "CF_TRANSPORT"
            elif root in {"ctypes", "fcntl", "resource", "signal"}:
                rule = "CF_NATIVE"
            raise AnalysisRejected(f"Unsupported Pyodide import: {root}", rule)
        if root in _PACKAGES:
            if self.linux_packages is not None:
                raise self.unresolved_import(dotted)
            self.packages.add(_PACKAGES[root])
            return True
        if root in _RESERVED:
            return True

        if self.single_file:
            if root == "requests":
                raise self.unresolved_import(dotted)
            return True  # Unfamiliar imports may live in another workspace file.

        for index in range(1, len(parts)):
            await self.load("/".join(parts[:index]) + "/__init__.py")
        base = "/".join(parts)
        package = await self.load(base + "/__init__.py")
        module = await self.load(base + ".py")
        if package and module:
            raise AnalysisRejected(f"Ambiguous workspace import: {dotted}")
        if not (package or module or optional):
            raise self.unresolved_import(dotted)
        return package or module

    async def import_from(self, path: str, node: ast.ImportFrom) -> None:
        module = _import_module(path, node)
        root = module.split(".")[0]
        linux_import = self.linux_packages is not None and (
            root in self.linux_packages or root in {"subprocess", "multiprocessing", "_ssl", "ctypes", "fcntl", "resource", "signal"}
        )
        if root in _RESERVED | _PACKAGES.keys() or linux_import:
            await self.resolve(module)
            if any(
                alias.name in _TRAPS or alias.name == "*" or f"{module}.{alias.name}" in _REFLECTION
                or (module == "builtins" and alias.name in _DYNAMIC - {"vars"})
                for alias in node.names
            ):
                raise AnalysisRejected("Unsupported dynamic import or Pyodide runtime operation", "CF_DYNAMIC")
            return
        found = await self.resolve(module, optional=True)
        for alias in node.names:
            if alias.name == "*":
                raise AnalysisRejected("Wildcard workspace imports require manual refactoring", "CF_DYNAMIC")
            child = await self.resolve(f"{module}.{alias.name}", optional=True)
            if not (found or child):
                refusal = self.unresolved_import(module)
                raise AnalysisRejected(f"Unresolved import: {module}.{alias.name}", refusal.rule)

    @staticmethod
    def unresolved_import(module: str) -> AnalysisRejected:
        root = module.partition(".")[0]
        if root == "requests":
            rule = "CF_TRANSPORT"
        elif root in {"awscrt", "win32api", "win32com", "pywin32"}:
            rule = "CF_NATIVE"
        else:
            rule = "CF_PACKAGE"
        return AnalysisRejected(f"Unresolved import: {module}", rule)

    def inspect_module(
        self, syntax: ast.Module, path: str, entry_functions: tuple[str, ...],
    ) -> list[ast.Import | ast.ImportFrom]:
        for _ in ast.walk(syntax):
            self.nodes += 1
            if self.nodes > _MAX_NODES:
                raise AnalysisRejected("Workspace sources exceed the analysis complexity budget", "CF_PARSE_BUDGET")
        if entry_functions:
            self.literal_selectors = dict.fromkeys(_literal_helper_selectors(syntax, entry_functions), path)
        imports: list[ast.Import | ast.ImportFrom] = []
        clients = self.inspect_scope(syntax.body, {}, {}, {}, {}, imports, path)
        self.literal_selectors.clear()
        module = path.removesuffix("/__init__.py").removesuffix(".py").replace("/", ".")
        self.exports.update({f"{module}.{name}": value for name, value in clients.items()})
        return imports

    def inspect_scope(
        self, body: list[ast.stmt] | ast.expr, inherited_aliases: dict[str, str],
        inherited_constants: dict[str, str], inherited_clients: dict[str, _Client],
        inherited_factories: dict[str, _Client], imports: list[ast.Import | ast.ImportFrom], path: str,
        arguments: ast.arguments | None = None, class_clients: dict[str, _Client] | None = None,
        *, active_bindings: int = 0,
    ) -> dict[str, _Client]:
        roots = body if isinstance(body, list) else [body]
        nodes = [node for root in roots for node in _scoped_nodes(root)]
        stores = Counter(
            node.id for node in nodes if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del))
        )
        local_names = set(stores) | {
            node.name for node in nodes if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        }
        if arguments is not None:
            local_names.update(node.arg for node in ast.iter_child_nodes(arguments) if isinstance(node, ast.arg))
        # Copy work includes shadowed entries. Retained entries are counted
        # once per live dictionary, using the filtered size of each scope.
        self.charge_scope_copy(
            len(inherited_aliases) + len(inherited_constants) + len(inherited_clients) + len(inherited_factories)
            + len(local_names)
        )
        aliases = {**inherited_aliases, **{name: f"local.{name}" for name in local_names}}
        constants = {name: value for name, value in inherited_constants.items() if name not in local_names}
        clients = {name: value for name, value in inherited_clients.items() if name not in local_names}
        factories = {name: value for name, value in inherited_factories.items() if name not in local_names}
        requests: dict[str, _URL | None] = {}
        mappings: set[str] = set()
        parameters = set()
        if arguments is not None:
            parameters = {
                item.arg for item in ast.iter_child_nodes(arguments) if isinstance(item, ast.arg)
            }
            for arg in ast.iter_child_nodes(arguments):
                if isinstance(arg, ast.arg) and arg.annotation and _qualified(arg.annotation, inherited_aliases) in _HTTP_CLIENTS:
                    clients[arg.arg] = _Client(None)
        for node in nodes:
            if isinstance(node, ast.Import):
                imports.append(node)
                for item in node.names:
                    root = item.name.split(".")[0]
                    aliases[item.asname or root] = item.name if item.asname else root
            elif isinstance(node, ast.ImportFrom):
                imports.append(node)
                module = _import_module(path, node)
                for item in node.names:
                    name = item.asname or item.name
                    aliases[name] = f"{module}.{item.name}"

        for node in nodes:
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if (
                        isinstance(target, ast.Name) and target.id not in parameters and stores[target.id] == 1
                        and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
                    ):
                        constants[target.id] = node.value.value
        for node in nodes:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                returns = [item.value for statement in node.body for item in _scoped_nodes(statement)
                           if isinstance(item, ast.Return)]
                if len(returns) == 1 and isinstance(returns[0], ast.Call) and _qualified(returns[0].func, aliases) in _HTTP_CLIENTS:
                    value = _client(returns[0], aliases, constants, clients, factories)
                    if value is not None:
                        factories[node.name] = value

        direct_calls = {id(node.func) for node in nodes if isinstance(node, ast.Call)}
        callback_args = {id(arg) for node in nodes if isinstance(node, ast.Call) for arg in node.args}
        self.check_active_scope(active_bindings, aliases, constants, clients, factories)
        if class_clients is not None:
            nodes.sort(key=lambda node: not isinstance(node, ast.FunctionDef) or node.name != "__init__")
        for node in nodes:
            try:
                self.inspect(node, aliases, constants, clients, factories, requests, direct_calls, callback_args, mappings, path)
            except AnalysisRejected as exc:
                exc.locate(path, node)
                raise
            if isinstance(node, _SCOPES):
                shared = {} if isinstance(node, ast.ClassDef) else class_clients
                ancestor_bindings = self.check_active_scope(active_bindings, aliases, constants, clients, factories)
                if isinstance(node, ast.ClassDef):
                    self.inspect_scope(
                        node.body, aliases, constants, clients, factories, imports, path,
                        class_clients=shared, active_bindings=ancestor_bindings,
                    )
                else:
                    child_clients = clients
                    if shared:
                        self.charge_scope_copy(len(clients) + len(shared))
                        child_clients = clients | shared
                        ancestor_bindings = self.check_active_scope(ancestor_bindings, child_clients)
                    result = self.inspect_scope(
                        node.body, aliases, constants, child_clients, factories, imports, path, node.args,
                        active_bindings=ancestor_bindings,
                    )
                    if shared is not None and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "__init__":
                        shared.update({name: value for name, value in result.items() if name.startswith("self.")})
                continue

            if isinstance(node, ast.withitem):
                value, targets = node.context_expr, [node.optional_vars]
            elif isinstance(node, ast.Assign):
                value, targets = node.value, node.targets
            elif isinstance(node, ast.AnnAssign):
                value, targets = node.value, [node.target]
            else:
                continue
            for target in targets:
                if target is None:
                    continue
                name = _dotted(target)
                client = _client(value, aliases, constants, clients, factories) if value is not None else None
                if client is None:
                    clients.pop(name, None)
                else:
                    clients[name] = client
                requests.pop(name, None)
                mappings.discard(name)
                if isinstance(value, (ast.Dict, ast.DictComp)):
                    mappings.add(name)
                if isinstance(value, ast.Call):
                    built = self.request_url(value, aliases, constants, clients, factories)
                    if built is not None:
                        requests[name] = built
        return clients

    def charge_scope_copy(self, count: int) -> None:
        self.scope_copy_work += count
        if self.scope_copy_work > _MAX_SCOPE_COPY_WORK:
            raise AnalysisRejected("Workspace sources exceed the scope copy budget", "CF_PARSE_BUDGET")

    @staticmethod
    def check_active_scope(active_bindings: int, *tables: dict) -> int:
        total = active_bindings + sum(map(len, tables))
        if total > _MAX_ACTIVE_SCOPE_BINDINGS:
            raise AnalysisRejected("Workspace sources exceed the active scope budget", "CF_PARSE_BUDGET")
        return total

    @staticmethod
    def request_url(
        node: ast.Call, aliases: dict[str, str], constants: dict[str, str],
        clients: dict[str, _Client], factories: dict[str, _Client],
    ) -> _URL | None:
        qualified = _qualified(node.func, aliases)
        target = _http_target(node.func, aliases, constants, clients, factories)
        if qualified not in _REQUEST_BUILDERS and (target is None or target[0] != "build_request"):
            return None
        argument = _argument(node, 0 if qualified == "urllib.request.Request" else 1, "url", "fullurl")
        literal = _url_literal(argument, constants) if argument is not None else None
        host = literal.host(target[1].host if target else None) if literal else None
        return _URL(f"https://{host}") if host is not None else _URL("", False)

    def record_call(
        self, literal: _URL | None, host: str | None = None, *,
        path: str, node: ast.AST, check_scheme: bool = False,
    ) -> None:
        self.calls += 1
        proposed = literal.host(host) if literal else None
        if proposed is None:
            self.unresolved_url_calls += 1
            if self.findings is not None and not (check_scheme and self.observe_transport(literal, path, node)):
                offset = getattr(node, "col_offset", None)
                self.url_review_count += 1
                if self.first_url_review is None:
                    self.first_url_review = (path, getattr(node, "lineno", None), offset + 1 if offset is not None else None)
        else:
            self.add_host(proposed)

    def add_host(self, host: str) -> None:
        self.hosts.add(host)
        if len(self.hosts) > 32:
            raise AnalysisRejected("Workspace sources exceed the HTTPS host budget", "CF_HOST_BUDGET")

    def inspect(
        self, node: ast.AST, aliases: dict[str, str], constants: dict[str, str], clients: dict[str, _Client],
        factories: dict[str, _Client], requests: dict[str, _URL | None], direct_calls: set[int],
        callback_args: set[int], mappings: set[str], path: str,
    ) -> None:
        if isinstance(node, ast.Attribute) and (
            node.attr in _BLOCKED_ATTRIBUTES
            or not isinstance(node.ctx, ast.Load) and _qualified(node.value, aliases) == "builtins"
            or node.attr == "__dict__" and (
                not isinstance(node.ctx, ast.Load) or _qualified(node.value, aliases) == "builtins"
            )
        ):
            raise AnalysisRejected("Unsupported dynamic or Pyodide runtime operation", "CF_DYNAMIC")
        if isinstance(node, (ast.Name, ast.Attribute)) and isinstance(node.ctx, ast.Load):
            qualified = _qualified(node, aliases)
            if qualified in {"os.system", "os.popen"} or qualified.startswith(("os.spawn", "os.exec", "os.posix_spawn")):
                self.observe("CF_PROCESS", f"{qualified} requires a native process", path, node)
            if (
                qualified in _REFLECTION or qualified.removeprefix("builtins.") in _DYNAMIC | {"__builtins__"}
                and not (qualified == "builtins.vars" and id(node) in direct_calls)
                or qualified.rsplit(".", 1)[-1] in _TRAPS
            ):
                rule = "CF_PROCESS" if qualified in {"os.fork", "os.forkpty"} else "CF_DYNAMIC"
                raise AnalysisRejected(
                    "Unsupported dynamic or Pyodide runtime operation", rule,
                    detail=f"Unsupported dynamic or Pyodide runtime operation: {qualified}",
                )
            if id(node) not in direct_calls and qualified == "builtins.getattr":
                raise AnalysisRejected("Unsupported indirect reference to a dynamic operation", "CF_DYNAMIC")
            if id(node) not in direct_calls and id(node) not in callback_args and _http_target(node, aliases, constants, clients, factories):
                self.record_call(None, path=path, node=node)
        if not isinstance(node, ast.Call):
            return
        qualified = _qualified(node.func, aliases)
        if qualified == "builtins.vars" and (
            len(node.args) != 1 or node.keywords or isinstance(node.args[0], ast.Starred)
            or _qualified(node.args[0], aliases) == "builtins"
        ):
            raise AnalysisRejected("Unsupported dynamic namespace lookup", "CF_DYNAMIC")
        if qualified == "builtins.getattr":
            attr = node.args[1] if len(node.args) > 1 else None
            if not isinstance(attr, ast.Constant) or not isinstance(attr.value, str):
                helper_module = self.literal_selectors.get(id(node))
                if (
                    helper_module is not None and id(node) in direct_calls
                    and _qualified(node.args[0], aliases) != "builtins"
                ):
                    # The selector is bounded, but the receiver may make an HTTP
                    # request. Require review without proposing an inferred host.
                    self.local_helpers_entry = helper_module
                    self.record_call(None, path=path, node=node)
                    return
                if node.args and (_qualified(node.args[0], aliases) == "builtins" or id(node) in direct_calls):
                    raise AnalysisRejected("Unsupported dynamic attribute lookup", "CF_DYNAMIC")
                return
            if (
                attr.value in _REFLECTIVE_SELECTORS
                or attr.value == "__dict__" and _qualified(node.args[0], aliases) == "builtins"
            ):
                raise AnalysisRejected("Unsupported dynamic attribute lookup", "CF_DYNAMIC")
            if _http_target(ast.Attribute(value=node.args[0], attr=attr.value, ctx=ast.Load()),
                            aliases, constants, clients, factories):
                self.record_call(None, path=path, node=node)
            return
        if qualified in _REQUEST_BUILDERS and self.findings is not None:
            url = _argument(node, 0 if qualified == "urllib.request.Request" else 1, "url", "fullurl")
            self.observe_transport(_url_literal(url, constants) if url is not None else None, path, node)
        if qualified in _HTTP_CLIENTS:
            base = _argument(node, len(node.args), "base_url")
            self.observe_transport(_url_literal(base, constants) if base is not None else None, path, node)
            client = _client(node, aliases, constants, clients, factories)
            if client and client.host is not None:
                self.add_host(client.host)
        namespace, _, method = qualified.rpartition(".")
        if namespace.startswith("bifrost.") and namespace.split(".")[1] != "types":
            area = namespace.split(".")[1]
            if area == "api":
                argument = _argument(node, 0, "path")
                literal = _url_literal(argument, constants) if argument is not None else None
                if not _raw_api_allowed(method.upper(), literal):
                    self.observe("CF_SDK_GATEWAY", "The SDK request path is outside the gateway policy or needs route review", path, node)
            elif not _sdk_routes_allowed(area, method):
                self.observe("CF_SDK_GATEWAY", f"The gateway does not forward {qualified}", path, node)
            self.calls += 1
            return
        target = _http_target(node.func, aliases, constants, clients, factories)
        if target is not None:
            method, client = target
            if method != "build_request":
                literal = self.call_url(node, method, aliases, constants, clients, factories, requests)
                self.record_call(literal, client.host, path=path, node=node, check_scheme=True)
            return
        for index, callback in enumerate(node.args):
            cb = _http_target(callback, aliases, constants, clients, factories)
            if cb is not None:
                argument = _argument(node, index + 1, "url")
                literal = _url_literal(argument, constants) if argument is not None else None
                self.record_call(literal, cb[1].host, path=path, node=node, check_scheme=True)
        if method in _HTTP_METHODS and (
            not isinstance(node.func, ast.Attribute) or _dotted(node.func.value) not in mappings
        ):
            literal = self.call_url(node, method, aliases, constants, clients, factories, requests)
            if len(self.call_hints) == _MAX_CALL_HINTS:
                raise AnalysisRejected("Workspace sources exceed the HTTP analysis budget", "CF_PARSE_BUDGET")
            self.call_hints.append(_CallHint(namespace, literal, path, node))

    def observe_transport(self, literal: _URL | None, path: str, node: ast.AST) -> bool:
        if literal is None or "://" not in literal.prefix or literal.prefix.lower().startswith("https://"):
            return False
        self.observe("CF_TRANSPORT", "The HTTP call uses a non-HTTPS URL", path, node)
        return True

    def call_url(
        self, node: ast.Call, method: str, aliases: dict[str, str], constants: dict[str, str],
        clients: dict[str, _Client], factories: dict[str, _Client], requests: dict[str, _URL | None],
    ) -> _URL | None:
        argument = _argument(node, 1 if method in {"request", "stream"} else 0, "url", "fullurl", "request")
        if argument is None:
            return None
        if isinstance(argument, ast.Call):
            built = self.request_url(argument, aliases, constants, clients, factories)
            if built is not None:
                return built
        if isinstance(argument, ast.Name) and argument.id in requests:
            return requests[argument.id]
        return _url_literal(argument, constants)

    def finish_call_hints(self) -> None:
        for hint in self.call_hints:
            client = self.exports.get(hint.receiver)
            if client is not None:
                self.record_call(hint.url, client.host, path=hint.path, node=hint.node, check_scheme=True)
            elif hint.url and hint.url.prefix.startswith(("http://", "https://")):
                self.record_call(None, path=hint.path, node=hint.node)


async def analyze(
    read: SourceReader, entry_path: str, function_name: str | None,
    *, subrequest_allowance: int, cpu_budget_ms: int, linux_packages: dict[str, str] | None = None,
) -> dict[str, object]:
    """Build the canonical pins and bounded routing proposal without executing code."""
    analysis = _Analysis(read, linux_packages=linux_packages)
    result = await _analyze(analysis, entry_path, function_name, subrequest_allowance, cpu_budget_ms)
    if linux_packages is not None:
        if not analysis.linux_findings:
            raise AnalysisRejected("No Linux runtime requirement was found")
        if result["https_hosts"] or result["unresolved_url_calls"]:
            raise AnalysisRejected("Container workflows may only use the worker ingress", "CF_TRANSPORT")
        result["linux_findings"] = sorted(analysis.linux_findings)
    return result


async def _analyze(
    analysis: _Analysis, entry_path: str, function_name: str | tuple[str, ...] | None,
    subrequest_allowance: int, cpu_budget_ms: int,
) -> dict[str, object]:
    if not (0 <= subrequest_allowance <= MAX_SUBREQUESTS and 1 <= cpu_budget_ms <= MAX_CPU_MS):
        raise AnalysisRejected("Invalid Cloudflare subrequest allowance or CPU budget")
    entries = (function_name,) if isinstance(function_name, str) else function_name or ()
    if not await asyncio.wait_for(analysis.load(entry_path, entries), timeout=20):
        raise AnalysisRejected("Workflow source is missing")
    analysis.finish_call_hints()
    if analysis.findings is not None and analysis.first_url_review is not None:
        path, line, column = analysis.first_url_review if analysis.url_review_count == 1 else (entry_path, None, None)
        analysis.findings.append(Finding(
            "CF_URL_REVIEW", "info",
            f"{analysis.url_review_count} HTTP call(s) need operator review of the destination host.",
            _HINTS["CF_URL_REVIEW"], path, line, column,
        ))
    # Static sizing signals. Repeated calls, input-dependent work and library
    # work are constrained by the runtime allowance and budget.
    estimated_subrequests = analysis.calls + len(analysis.pins)
    estimated_cpu_ms = max(1, (analysis.nodes + 24) // 25)
    if not analysis.single_file and estimated_subrequests > subrequest_allowance:
        raise AnalysisRejected(
            f"Estimated subrequests ({estimated_subrequests}) exceed the allowance", "CF_SUBREQUESTS",
            detail=f"Estimated subrequests ({estimated_subrequests}) exceed the allowance ({subrequest_allowance})",
        )
    if not analysis.single_file and estimated_cpu_ms > cpu_budget_ms:
        raise AnalysisRejected(
            f"Estimated CPU ({estimated_cpu_ms} ms) exceeds the budget", "CF_CPU",
            detail=f"Estimated CPU ({estimated_cpu_ms} ms) exceeds the budget ({cpu_budget_ms} ms)",
        )
    hosts = sorted(analysis.hosts)
    return {
        "entry_path": entry_path, "function_name": function_name,
        "manifest_sha256": digest(analysis.pins),
        "entry_sha256": analysis.pins.pop(entry_path), "module_sha256": analysis.pins,
        "packages": sorted(analysis.packages), "https_hosts": hosts, "required_https_hosts": hosts,
        "unresolved_url_calls": analysis.unresolved_url_calls,
        "estimated_subrequests": estimated_subrequests, "subrequest_allowance": subrequest_allowance,
        "estimated_cpu_ms": estimated_cpu_ms, "cpu_budget_ms": cpu_budget_ms,
    }



async def diagnose(
    read: SourceReader, path: str, function_name: str | tuple[str, ...] | None = None,
    *, single_file: bool = False, subrequest_allowance: int = MAX_SUBREQUESTS, cpu_budget_ms: int = 1000,
) -> list[Finding]:
    """Expose the first canonical refusal and any observed advisory findings."""
    findings: list[Finding] = []
    try:
        await _analyze(_Analysis(read, single_file=single_file, findings=findings),
                       path, function_name, subrequest_allowance, cpu_budget_ms)
    except (AnalysisRejected, asyncio.TimeoutError) as exc:  # noqa: UP041 - offline census supports Python 3.10
        refusal = exc if isinstance(exc, AnalysisRejected) else AnalysisRejected(
            "Source analysis timed out", "CF_PARSE_BUDGET",
        )
        refusal.locate(path)
        findings.append(Finding.from_refusal(refusal))
    return list(dict.fromkeys(findings))
