"""Static analysis shared by the offline census and workspace approval service."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import re
from collections import Counter
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
_REFLECTION = {"sys.modules", "operator.attrgetter", "operator.methodcaller", "pydoc.locate", "pkgutil.resolve_name"}
_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
_HTTP_METHODS = {"request", "get", "head", "post", "put", "patch", "delete", "options", "stream", "send"}
_URLLIB_CALLS = {"urllib.request.urlopen", "urllib.request.urlretrieve"}
_REQUEST_BUILDERS = {"httpx.Request", "urllib.request.Request"}

SourceReader = Callable[[str, int], Awaitable[bytes | None]]


class AnalysisRejected(ValueError):
    """Sources or requested bounds cannot be approved by this analyzer."""


class SourceUnresolved(AnalysisRejected):
    """An indexed source could not be read from storage."""


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _valid_path(path: str) -> bool:
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
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return _URL(node.value)
    if isinstance(node, ast.Name) and node.id in constants:
        return _URL(constants[node.id])
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _url_literal(node.left, constants)
        if left is None or not left.complete:
            return left
        right = _url_literal(node.right, constants)
        return _URL(left.prefix + (right.prefix if right else ""), bool(right and right.complete))
    if isinstance(node, ast.JoinedStr):
        prefix = ""
        for item in node.values:
            if isinstance(item, ast.FormattedValue):
                if item.conversion != -1 or item.format_spec is not None:
                    return _URL(prefix, False) if prefix else None
                item = item.value
            part = _url_literal(item, constants)
            if part is None:
                return _URL(prefix, False) if prefix else None
            prefix += part.prefix
            if not part.complete:
                return _URL(prefix, False)
        return _URL(prefix)
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


def _argument(node: ast.Call, index: int, *keywords: str) -> ast.expr | None:
    if len(node.args) > index:
        return node.args[index]
    return next((item.value for item in node.keywords if item.arg in keywords), None)


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
    exports: dict[str, _Client] = field(default_factory=dict)
    call_hints: list[_CallHint] = field(default_factory=list)

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

        imports = self.inspect_module(syntax, path)
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
        if root in _UNSUPPORTED and dotted != "importlib.metadata":
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
                or (module == "builtins" and alias.name in _DYNAMIC - {"vars"})
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

    def inspect_module(self, syntax: ast.Module, path: str) -> list[ast.Import | ast.ImportFrom]:
        for _ in ast.walk(syntax):
            self.nodes += 1
            if self.nodes > _MAX_NODES:
                raise AnalysisRejected("Workspace sources exceed the analysis complexity budget")
        imports: list[ast.Import | ast.ImportFrom] = []
        clients = self.inspect_scope(syntax.body, {}, {}, {}, {}, imports)
        module = path.removesuffix("/__init__.py").removesuffix(".py").replace("/", ".")
        self.exports.update({f"{module}.{name}": value for name, value in clients.items()})
        return imports

    def inspect_scope(
        self, body: list[ast.stmt] | ast.expr, inherited_aliases: dict[str, str],
        inherited_constants: dict[str, str], inherited_clients: dict[str, _Client],
        inherited_factories: dict[str, _Client], imports: list[ast.Import | ast.ImportFrom],
        arguments: ast.arguments | None = None, class_clients: dict[str, _Client] | None = None,
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
        aliases = {**inherited_aliases, **{name: f"local.{name}" for name in local_names}}
        constants = {name: value for name, value in inherited_constants.items() if name not in local_names}
        clients = {name: value for name, value in inherited_clients.items() if name not in local_names}
        factories = {name: value for name, value in inherited_factories.items() if name not in local_names}
        requests: dict[str, _URL | None] = {}
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
                for item in node.names:
                    name = item.asname or item.name
                    aliases[name] = f"{node.module}.{item.name}" if not node.level else f"local.{name}"

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
        for node in nodes:
            self.inspect(node, aliases, constants, clients, factories, requests, direct_calls, callback_args)
            if isinstance(node, _SCOPES):
                shared = {} if isinstance(node, ast.ClassDef) else class_clients
                if isinstance(node, ast.ClassDef):
                    self.inspect_scope(node.body, aliases, constants, clients, factories, imports, class_clients=shared)
                else:
                    child_clients = {**clients, **(shared or {})}
                    result = self.inspect_scope(node.body, aliases, constants, child_clients, factories, imports, node.args)
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
                if isinstance(value, ast.Call):
                    built = self.request_url(value, aliases, constants, clients, factories)
                    if built is not None:
                        requests[name] = built
        return clients

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

    def record_call(self, literal: _URL | None, host: str | None = None) -> None:
        self.calls += 1
        proposed = literal.host(host) if literal else None
        if proposed is None:
            self.unresolved_url_calls += 1
        else:
            self.hosts.add(proposed)

    def inspect(
        self, node: ast.AST, aliases: dict[str, str], constants: dict[str, str], clients: dict[str, _Client],
        factories: dict[str, _Client], requests: dict[str, _URL | None], direct_calls: set[int],
        callback_args: set[int],
    ) -> None:
        if isinstance(node, ast.Attribute) and (
            node.attr in _BLOCKED_ATTRIBUTES
            or not isinstance(node.ctx, ast.Load) and _qualified(node.value, aliases) == "builtins"
            or node.attr == "__dict__" and (
                not isinstance(node.ctx, ast.Load) or _qualified(node.value, aliases) == "builtins"
            )
        ):
            raise AnalysisRejected("Unsupported dynamic or Pyodide runtime operation")
        if isinstance(node, (ast.Name, ast.Attribute)) and isinstance(node.ctx, ast.Load):
            qualified = _qualified(node, aliases)
            if (
                qualified in _REFLECTION or qualified.removeprefix("builtins.") in _DYNAMIC | {"__builtins__"}
                and not (qualified == "builtins.vars" and id(node) in direct_calls)
                or qualified.rsplit(".", 1)[-1] in _TRAPS
            ):
                raise AnalysisRejected("Unsupported dynamic or Pyodide runtime operation")
            if id(node) not in direct_calls and qualified == "builtins.getattr":
                raise AnalysisRejected("Unsupported indirect reference to a dynamic operation")
            if id(node) not in direct_calls and id(node) not in callback_args and _http_target(node, aliases, constants, clients, factories):
                self.record_call(None)
        if not isinstance(node, ast.Call):
            return
        qualified = _qualified(node.func, aliases)
        if qualified == "builtins.vars" and (
            len(node.args) != 1 or node.keywords or isinstance(node.args[0], ast.Starred)
            or _qualified(node.args[0], aliases) == "builtins"
        ):
            raise AnalysisRejected("Unsupported dynamic namespace lookup")
        if qualified == "builtins.getattr":
            attr = node.args[1] if len(node.args) > 1 else None
            if (
                not isinstance(attr, ast.Constant) or not isinstance(attr.value, str)
                or attr.value in _DYNAMIC | _TRAPS | _BLOCKED_ATTRIBUTES | {"getattr", "modules"}
                or attr.value == "__dict__" and _qualified(node.args[0], aliases) == "builtins"
            ):
                raise AnalysisRejected("Unsupported dynamic attribute lookup")
            if _http_target(ast.Attribute(value=node.args[0], attr=attr.value, ctx=ast.Load()),
                            aliases, constants, clients, factories):
                self.record_call(None)
            return
        if qualified in _HTTP_CLIENTS:
            client = _client(node, aliases, constants, clients, factories)
            if client and client.host is not None:
                self.hosts.add(client.host)
        namespace, _, method = qualified.rpartition(".")
        if namespace.startswith("bifrost.") and namespace.split(".")[1] != "types":
            self.calls += 1
            return
        target = _http_target(node.func, aliases, constants, clients, factories)
        if target is not None:
            method, client = target
            if method != "build_request":
                self.record_call(self.call_url(node, method, aliases, constants, clients, factories, requests), client.host)
            return
        for index, callback in enumerate(node.args):
            cb = _http_target(callback, aliases, constants, clients, factories)
            if cb is not None:
                argument = _argument(node, index + 1, "url")
                literal = _url_literal(argument, constants) if argument is not None else None
                self.record_call(literal, cb[1].host)
        if method in _HTTP_METHODS:
            literal = self.call_url(node, method, aliases, constants, clients, factories, requests)
            self.call_hints.append(_CallHint(namespace, literal))

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
            if client is not None or hint.url and hint.url.prefix.startswith(("http://", "https://")):
                self.record_call(hint.url, client.host if client else None)


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
    analysis.finish_call_hints()
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
        "unresolved_url_calls": analysis.unresolved_url_calls,
        "estimated_subrequests": estimated_subrequests, "subrequest_allowance": subrequest_allowance,
        "estimated_cpu_ms": estimated_cpu_ms, "cpu_budget_ms": cpu_budget_ms,
    }
