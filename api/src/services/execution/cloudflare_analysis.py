"""Static analysis shared by the offline census and workspace approval service."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Iterator
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
_UNSUPPORTED = {"ctypes", "fcntl", "code", "codeop", "importlib", "multiprocessing", "runpy", "resource", "signal", "socket", "subprocess"}
_TRAPS = {"process_time", "process_time_ns", "fork", "forkpty", "import_module"}
_DYNAMIC = {"__import__", "eval", "exec", "compile", "vars", "globals"}
_HTTP_CLIENTS = {"httpx.Client", "httpx.AsyncClient"}
_FETCH_FUNCTIONS = {"js.fetch", "js.globalThis.fetch", "pyodide.http.pyfetch", "workers.fetch"}
_BLOCKED_ATTRIBUTES = {"__dict__", "__subclasses__", "__globals__", "__code__", "__builtins__"}
_REFLECTION = {"sys.modules", "operator.attrgetter", "operator.methodcaller", "pydoc.locate", "pkgutil.resolve_name"}
_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
_HTTP_METHODS = {"fetch", "pyfetch", "request", "get", "head", "post", "put", "patch", "delete", "options", "stream", "urlopen"}

SourceReader = Callable[[str, int], Awaitable[bytes | None]]


class AnalysisRejected(ValueError):
    """Sources or requested bounds cannot be approved by this analyzer."""


class SourceUnresolved(AnalysisRejected):
    """An indexed source could not be read from storage."""


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
    if not root:
        return f".{tail}"
    resolved = aliases.get(root)
    if resolved is None:
        resolved = f"builtins.{root}" if root in _DYNAMIC | {"getattr", "__builtins__"} else f"unbound.{root}"
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


def _constructor_host(node: ast.Call) -> str | None:
    base = next((item.value for item in node.keywords if item.arg == "base_url"), None)
    if base is None:
        return None
    if not isinstance(base, ast.Constant) or not isinstance(base.value, str):
        raise AnalysisRejected("HTTP client base_url requires a literal HTTPS origin")
    return _https_host(base.value)


def _http_target(
    node: ast.expr, aliases: dict[str, str], clients: dict[str, str | None],
) -> tuple[str, str | None] | None:
    qualified = _qualified(node, aliases)
    namespace, _, method = qualified.rpartition(".")
    if qualified in _FETCH_FUNCTIONS or qualified in {"urllib.request.urlopen", "urllib.request.urlretrieve"}:
        return method, None
    if method not in _HTTP_METHODS:
        return None
    if namespace == "httpx" or namespace in _HTTP_CLIENTS:
        return method, None
    if isinstance(node, ast.Attribute):
        receiver = node.value
        if isinstance(receiver, ast.Name) and receiver.id in clients:
            return method, clients[receiver.id]
        if isinstance(receiver, ast.Call) and _qualified(receiver.func, aliases) in _HTTP_CLIENTS:
            return method, _constructor_host(receiver)
    return None


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
            if any(
                alias.name in _TRAPS or alias.name == "*" or f"{module}.{alias.name}" in _REFLECTION
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
                raise AnalysisRejected(f"Unresolved import: {module}.{alias.name}")

    def inspect_module(self, syntax: ast.Module) -> list[ast.Import | ast.ImportFrom]:
        # Count the full tree before constructing per-scope working sets.
        for _ in ast.walk(syntax):
            self.nodes += 1
            if self.nodes > _MAX_NODES:
                raise AnalysisRejected("Workspace sources exceed the analysis complexity budget")
        imports: list[ast.Import | ast.ImportFrom] = []
        self.inspect_scope(syntax.body, {}, {}, imports)
        return imports

    def inspect_scope(
        self, body: list[ast.stmt] | ast.expr, inherited_aliases: dict[str, str],
        inherited_clients: dict[str, str | None], imports: list[ast.Import | ast.ImportFrom],
        arguments: ast.arguments | None = None,
    ) -> None:
        roots = body if isinstance(body, list) else [body]
        nodes = [node for root in roots for node in _scoped_nodes(root)]
        local_names = {
            node.id for node in nodes if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del))
        } | {node.name for node in nodes if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
        if arguments is not None:
            local_names.update(node.arg for node in ast.iter_child_nodes(arguments) if isinstance(node, ast.arg))
        aliases = {**inherited_aliases, **{name: f"local.{name}" for name in local_names}}
        clients = {name: host for name, host in inherited_clients.items() if name not in local_names}
        for node in nodes:
            if isinstance(node, ast.Import):
                imports.append(node)
                for item in node.names:
                    root = item.name.split(".")[0]
                    aliases[item.asname or root] = item.name if item.asname else root
                    clients.pop(item.asname or root, None)
            elif isinstance(node, ast.ImportFrom):
                imports.append(node)
                for item in node.names:
                    name = item.asname or item.name
                    aliases[name] = f"{node.module}.{item.name}" if not node.level else f"local.{name}"
                    clients.pop(name, None)

        calls = {id(node.func) for node in nodes if isinstance(node, ast.Call)}
        for node in nodes:
            if isinstance(node, ast.withitem):
                value, targets = node.context_expr, [node.optional_vars]
            elif isinstance(node, ast.Assign):
                value, targets = node.value, node.targets
            elif isinstance(node, ast.AnnAssign):
                value, targets = node.value, [node.target]
            else:
                continue
            if isinstance(value, ast.Call) and _qualified(value.func, aliases) in _HTTP_CLIENTS:
                host = _constructor_host(value)
                clients.update({target.id: host for target in targets if isinstance(target, ast.Name)})

        for node in nodes:
            self.inspect(node, aliases, clients, calls)
            if isinstance(node, _SCOPES):
                parameters = node.args if not isinstance(node, ast.ClassDef) else None
                self.inspect_scope(node.body, aliases, clients, imports, parameters)

    def inspect(
        self, node: ast.AST, aliases: dict[str, str], clients: dict[str, str | None], calls: set[int],
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
                qualified in _REFLECTION or qualified.removeprefix("builtins.") in _DYNAMIC | {"__builtins__"}
                or qualified.rsplit(".", 1)[-1] in _TRAPS
                or (isinstance(node, ast.Attribute) and node.attr in _BLOCKED_ATTRIBUTES)
            ):
                raise AnalysisRejected("Unsupported dynamic or Pyodide runtime operation")
            if id(node) not in calls and (
                qualified == "builtins.getattr" or _http_target(node, aliases, clients) is not None
            ):
                raise AnalysisRejected("Unsupported indirect reference to an HTTP or dynamic operation")
        if not isinstance(node, ast.Call):
            return
        qualified = _qualified(node.func, aliases)
        if qualified == "builtins.getattr":
            attr = node.args[1] if len(node.args) > 1 else None
            if (
                not isinstance(attr, ast.Constant) or not isinstance(attr.value, str)
                or attr.value in _DYNAMIC | _TRAPS | _BLOCKED_ATTRIBUTES | {"getattr", "modules"}
                or _http_target(ast.Attribute(value=node.args[0], attr=attr.value), aliases, clients) is not None
            ):
                raise AnalysisRejected("Unsupported dynamic attribute lookup")
        if qualified in _HTTP_CLIENTS:
            host = _constructor_host(node)
            if host is not None:
                self.hosts.add(host)
        if qualified.rpartition(".")[0].startswith("bifrost."):
            self.calls += 1
            return
        target = _http_target(node.func, aliases, clients)
        if target is None:
            return
        method, base_host = target
        index = 1 if method in {"request", "stream"} else 0
        url = node.args[index] if len(node.args) > index else next(
            (keyword.value for keyword in node.keywords if keyword.arg in {"url", "fullurl"}), None
        )
        literal = _url_literal(url) if url is not None else None
        if literal is None:
            raise AnalysisRejected("Outbound call has no statically identifiable HTTPS URL")
        if (
            base_host is not None and literal and literal == literal.strip()
            and not literal.startswith("//") and "\\" not in literal
            and ":" not in literal.partition("/")[0]
        ):
            self.hosts.add(base_host)
        else:
            self.hosts.add(_https_host(literal))
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
