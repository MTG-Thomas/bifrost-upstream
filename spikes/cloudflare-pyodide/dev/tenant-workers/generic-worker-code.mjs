// CONSTANT generic per-tenant Python worker script.
//
// This is the whole point of the pivot: one immutable script per tenant,
// created once via env.LOADER.get(tenantWorkerId, () => genericWorkerCode()).
// Workflow source NEVER appears here; it arrives per attempt as PATH-keyed
// DATA (body.modules: storage path -> source, body.entry: entry storage path)
// and is loaded through a path-keyed virtual import hook that records REAL
// storage paths (e.g. 'workflows/wf.py') for the existing import-audit path.
// Module bytes reach the child only via the in-supervisor TenantGateway
// (GET /api/sdk/modules/<path>, GET /api/sdk/modules-resolve), which verifies
// manifest pins before serving; the child never sees the raw attempt store.
// The child env is {}: the child holds NO credentials. Where the real
// executor needs a credential to satisfy run_authorized_attempt, the parent
// supplies the existing DUMMY_CREDENTIAL placeholder and the real gateway
// injects the run token (unchanged posture, see tenant-entry.mjs).
//
// Any change to GENERIC_PYTHON below is a redeploy of the same constant
// script, not a per-tenant bake: existing tenantWorkerIds keep working.
import {runtimeCode} from '../runtime/runtime-code.mjs';

export const GENERIC_PYTHON = `
import asyncio
import builtins
import importlib
import importlib.abc
import importlib.machinery
import json
import os
import re
import sys
import time
import types
from pathlib import Path
from workers import WorkerEntrypoint, Response

RECORDED = []
_VIRTUAL_NAMES = set()
_JWT_RE = re.compile(r'[A-Za-z0-9_-]{10,}\\.[A-Za-z0-9_-]{10,}\\.[A-Za-z0-9_-]{10,}')

class _PathImporter(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    # PATH-keyed virtual importer. Keys are storage paths
    # ('workflows/helper.py'); import names map to candidate paths the same
    # way production resolves logical names (dots/slashes -> base path).
    # Every concrete workspace load records its STORAGE PATH, so the parent
    # import audit reconciles paths, not module names. Namespace parents
    # (e.g. 'workflows' for 'workflows.helper') resolve transparently and are
    # not recorded: only concrete module/package loads count.
    def __init__(self, modules):
        self._modules = dict(modules)
    def _concrete(self, name):
        base = name.replace('.', '/')
        for cand in (base + '.py', base + '/__init__.py'):
            if cand in self._modules:
                return cand
        return None
    def find_spec(self, name, path=None, target=None):
        if self._concrete(name) is not None:
            return importlib.machinery.ModuleSpec(name, self)
        base = name.replace('.', '/')
        if any(p.startswith(base + '/') for p in self._modules):
            spec = importlib.machinery.ModuleSpec(name, self, is_package=True)
            spec.submodule_search_locations = []
            return spec
        return None
    def create_module(self, spec):
        return None
    def exec_module(self, module):
        cand = self._concrete(module.__name__)
        if cand is None:
            module.__path__ = []
            _VIRTUAL_NAMES.add(module.__name__)
            return
        RECORDED.append(cand)
        _VIRTUAL_NAMES.add(module.__name__)
        exec(compile(self._modules[cand], '<tenant-path>/' + cand, 'exec'), module.__dict__)

def _clean():
    _p = Path("/tmp/tenant-hostile-pwn.txt")
    return {
        "builtins": getattr(builtins, "_TENANT_X", None),
        "module": "_tenant_pwn" in sys.modules,
        "module_marker": getattr(sys.modules.get("_tenant_pwn"), "marker", None),
        "env": os.environ.get("TENANT_HOSTILE_SECRET"),
        "fs": _p.read_text() if _p.exists() else None,
    }

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        data = json.loads(await request.text())
        action = data.get("action")
        marker = data.get("marker")
        if action == "honest":
            return Response.json({"clean": _clean()})
        if action == "plant_builtins":
            builtins._TENANT_X = marker
            return Response.json({"planted": True})
        if action == "read_builtins":
            return Response.json({"builtins": getattr(builtins, "_TENANT_X", None)})
        if action == "plant_module":
            _m = types.ModuleType("_tenant_pwn")
            _m.marker = marker
            sys.modules["_tenant_pwn"] = _m
            return Response.json({"planted": True})
        if action == "read_module":
            _m = sys.modules.get("_tenant_pwn")
            return Response.json({
                "present": "_tenant_pwn" in sys.modules,
                "marker": getattr(_m, "marker", None) if _m is not None else None,
            })
        if action == "plant_env":
            os.environ["TENANT_HOSTILE_SECRET"] = marker
            return Response.json({"planted": True})
        if action == "read_env":
            return Response.json({"value": os.environ.get("TENANT_HOSTILE_SECRET")})
        if action == "plant_fs":
            Path("/tmp/tenant-hostile-pwn.txt").write_text(marker)
            return Response.json({"planted": True})
        if action == "read_fs":
            _p = Path("/tmp/tenant-hostile-pwn.txt")
            return Response.json({"exists": _p.exists(), "value": _p.read_text() if _p.exists() else None})
        if action == "slow":
            ms = int(data.get("ms", 500))
            start = time.monotonic()
            await asyncio.sleep(ms / 1000.0)
            end = time.monotonic()
            return Response.json({"marker": marker, "started_at": start, "ended_at": end})
        if action == "run":
            RECORDED.clear()
            modules = data.get("modules") or {}
            entry_path = data.get("entry_path", data.get("entry"))
            # Entry is a gateway-served storage path, loaded directly (like the
            # real executor loads file_path): no parent import, no name guess.
            if not isinstance(entry_path, str) or entry_path not in modules:
                return Response.json({"error": "unknown entry path"}, status=400)
            # Fresh DATA per attempt: evict only the names this worker loaded
            # through the path importer, so sub-imports below re-execute the
            # supplied source. This is module refresh, not residue hiding:
            # builtins/env/fs/sys residue outside the importer (e.g.
            # _tenant_pwn) is left untouched and stays observable as the
            # documented within-tenant limitation.
            for _name in list(_VIRTUAL_NAMES):
                sys.modules.pop(_name, None)
            _VIRTUAL_NAMES.clear()
            sys.meta_path.insert(0, _PathImporter(modules))
            try:
                entry_mod = types.ModuleType("__tenant_entry__")
                RECORDED.append(entry_path)
                exec(compile(modules[entry_path], '<tenant-path>/' + entry_path, 'exec'), entry_mod.__dict__)
                fn = getattr(entry_mod, "main", None)
                result = fn() if callable(fn) else {"imported": True}
            finally:
                sys.meta_path = [f for f in sys.meta_path if not isinstance(f, _PathImporter)]
            return Response.json({"result": result, "recorded": sorted(set(RECORDED))})
        if action == "env_check":
            values = list(os.environ.values())
            shaped = [v for v in values if isinstance(v, str) and _JWT_RE.search(v)]
            return Response.json({"token_shaped_present": len(shaped) > 0,
                                  "dummy_credential_present": "gateway-injected" in values})
        return Response.json({"error": "unsupported action"}, status=400)
`;

export function genericWorkerCode() {
  return {
    compatibilityDate: '2026-10-30',
    compatibilityFlags: ['python_workers'],
    mainModule: 'main.py',
    modules: {'main.py': {py: GENERIC_PYTHON}},
    env: {},
    globalOutbound: null,
    limits: {cpuMs: 1000, subRequests: 4},
  };
}

export async function genericCodeDigest() {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(GENERIC_PYTHON));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

// Engine mode: the constant child runs the real Bifrost engine instead of the
// minimal path importer, so SDK workflows work. `api` is the supervisor's
// outbound stub (ctx.exports.TenantApi), which rewrites the virtual API host and
// constrains egress. Unlike the minimal child, this one holds the attempt-scoped
// run token, because the engine authenticates SDK calls itself; see
// dev/staging/README.md for the posture note.
// Upper bound on module names one attempt reports and one warm-up imports.
export const MAX_TRACED_MODULES = 400;

// The warm-up import list: the union of packaged modules this tenant's
// attempts imported lazily, as traced by the child. It is kept per engine
// digest, so a runtime change starts a fresh list instead of priming names
// the new runtime may not have.
export function mergePrimeList(stored, engine, imported) {
  const current = stored?.engine === engine && Array.isArray(stored.modules) ? stored.modules : [];
  const names = new Set(current);
  for (const name of Array.isArray(imported) ? imported : []) {
    if (names.size >= MAX_TRACED_MODULES) break;
    if (typeof name === 'string' && IMPORT_NAME_RE.test(name)) names.add(name);
  }
  return {engine, modules: [...names].sort()};
}

export const ENGINE_PYTHON = `
import gc
import importlib
import importlib.machinery
import zipimport
import json
import re
import sys
import time
from workers import WorkerEntrypoint, Response
from runtime_runner import run_authorized_attempt
# _SERVED distinguishes a fresh child from a reused one from the child's own
# point of view (the supervisor's view is lost whenever IT is recycled). Boot
# and import cost cannot be timed in here: Workers clocks only advance across
# I/O, so it shows up as the host's child_ms minus engine_ms.
_SERVED = 0
# Automatic collection can leave attempt-local cycles in the oldest
# generation for the lifetime of a reused Pyodide child.
_ATTEMPTS_UNTIL_COLLECTION = 32
_attempts_until_collection = _ATTEMPTS_UNTIL_COLLECTION
_MODULE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*$")
_FILE_LOADERS = (importlib.machinery.SourceFileLoader,
                 importlib.machinery.SourcelessFileLoader,
                 importlib.machinery.ExtensionFileLoader,
                 zipimport.zipimporter)
_BUILTIN_LOADERS = (importlib.machinery.BuiltinImporter, importlib.machinery.FrozenImporter)
MAX_TRACED = ${MAX_TRACED_MODULES}


def _packaged(name):
    # True for a loaded module that came from the packaged runtime, the stdlib
    # or the interpreter itself. Workspace modules (the engine's virtual
    # importer) never qualify, so a warm-up can never fetch tenant code.
    module = sys.modules.get(name)
    loader = getattr(getattr(module, "__spec__", None), "loader", None)
    if isinstance(loader, _FILE_LOADERS):
        return True
    return isinstance(loader, type) and issubclass(loader, _BUILTIN_LOADERS)


def _on_disk(name):
    # Warm-up guard: the top-level package must be importable from sys.path or
    # built in, without consulting the engine's remote import hook.
    top = name.split(".", 1)[0]
    return (top in sys.builtin_module_names
            or importlib.machinery.PathFinder.find_spec(top) is not None)


async def _exercise(handle):
    # One dry engine attempt under an unregistered handle: the host's outbound
    # gateway answers its first SDK call 401 before anything reaches the API,
    # and the engine reports a load failure. Synthetic context only; the next
    # real attempt installs its own credential before it starts.
    from bifrost.client import _install_cached_access_token
    api_url = "https://test-api.invalid"
    _install_cached_access_token(api_url, handle)
    context = {"caller": {"user_id": "00000000-0000-0000-0000-000000000000",
                          "email": "keep-warm@example.invalid", "name": "keep-warm"},
               "organization_id": None, "organization": None,
               "name": "keep_warm", "function_name": "keep_warm",
               "file_path": "keep_warm/probe.py", "engine_token": handle,
               "transient": True, "no_cache": True, "parameters": {}}
    try:
        outcome = await run_authorized_attempt(
            "00000000-0000-4000-8000-000000000000", context, api_url,
            public_url="https://staging.invalid")
        return {"status": outcome.get("status"), "error_type": outcome.get("error_type")}
    except Exception as exc:
        return {"status": "raised", "error_type": type(exc).__name__}


def _prime(names):
    # Imports what earlier attempts in this tenant imported lazily (learned by
    # the host from each attempt's trace), so the first attempt after a re-boot
    # does not pay for it. A name that no longer imports is reported, not fatal:
    # priming only moves cost off the request path.
    primed, skipped = 0, []
    for name in names[:MAX_TRACED]:
        if not isinstance(name, str) or not _MODULE_NAME.match(name) or not _on_disk(name):
            skipped.append(str(name)[:80])
            continue
        if name in sys.modules:
            continue
        try:
            importlib.import_module(name)
            primed += 1
        except Exception:
            skipped.append(name)
    return primed, skipped[:16]

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        global _SERVED, _attempts_until_collection
        first_request = _SERVED == 0
        _SERVED += 1
        body = json.loads(await request.text())
        if body.get("action") == "ping":
            # Keep-warm from the tenant host: proves the isolate is resident
            # (or pays its boot off the request path), and pre-imports what
            # this tenant's attempts imported lazily (measured: ~160 ms of a
            # fresh child's first run). Module names only, no tenant data.
            primed, skipped = _prime(body.get("modules") or [])
            reply = {"pong": True, "first_request": first_request,
                     "primed": primed, "prime_skipped": skipped}
            exercise = body.get("exercise")
            if isinstance(exercise, dict) and isinstance(exercise.get("handle"), str):
                reply["exercise"] = await _exercise(exercise["handle"])
            return Response.json(reply)
        if body.get("action") == "inventory":
            # Diagnostic: which modules are loaded (names only, no data).
            return Response.json({"modules": sorted(sys.modules)})
        # One persistent child serves EVERY attempt for this tenant, so the
        # process-scoped SDK state a one-shot child gets for free must be
        # re-established at the attempt boundary. The SDK caches its client per
        # thread; updating only BIFROST_ACCESS_TOKEN via env (which the engine
        # does inside run_execution) leaves those transports on the PREVIOUS
        # attempt's run handle, and the gateway refuses them. Hand the new
        # credential to the already-cached client before the run starts.
        from bifrost.client import _install_cached_access_token

        context = body["context"]
        token = context.get("engine_token")
        if token:
            _install_cached_access_token(body["api_url"], token)
        loaded_before = set(sys.modules)
        engine_started = time.perf_counter()
        try:
            result = await run_authorized_attempt(
                body["execution_id"], context, body["api_url"],
                public_url=body.get("public_url", "https://staging.invalid"))
        finally:
            _attempts_until_collection -= 1
            if _attempts_until_collection == 0:
                _attempts_until_collection = _ATTEMPTS_UNTIL_COLLECTION
                gc.collect()
        done = time.perf_counter()
        # Import trace: packaged modules this attempt loaded for the first time
        # in this child. The host keeps the union and primes the next child.
        imported = sorted(n for n in set(sys.modules) - loaded_before if _packaged(n))[:MAX_TRACED]
        # default=str matches the Linux persistence contract
        # (src/repositories/executions.py::_make_json_safe): datetimes, UUIDs
        # and Decimals in a workflow result become strings instead of failing.
        return Response(json.dumps({"upstream_result": result, "child": {
            "first_request": first_request, "served": _SERVED,
            "engine_ms": int((done - engine_started) * 1000),
            "imported": imported}}, default=str),
            headers={"content-type": "application/json"})
`;

// Cold-boot diagnosis: a child whose only work is importing `imports` at top
// level. With `bare`, the packaged runtime is left out entirely, isolating
// Pyodide's own start from shipping/decoding the 15 MB runtime. No network.
const IMPORT_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/;
export function bootProbeCode({imports = [], bare = false} = {}) {
  if (!Array.isArray(imports) || imports.length > 16 ||
      !imports.every((name) => typeof name === 'string' && IMPORT_NAME_RE.test(name))) {
    throw new TypeError('imports must be up to 16 dotted module names');
  }
  // `workers` first: the engine's import hook (installed by runtime_runner)
  // would otherwise try to resolve it remotely, as the real engine child avoids.
  const main = `import sys
import importlib.util
from workers import WorkerEntrypoint, Response
${imports.map((name) => `import ${name}`).join('\n')}

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        return Response.json({"ok": True, "version": sys.version,
            "magic": importlib.util.MAGIC_NUMBER.hex(),
            "cache_tag": sys.implementation.cache_tag,
            "dont_write_bytecode": sys.dont_write_bytecode})
`;
  if (bare) {
    return {compatibilityDate: '2026-10-30', compatibilityFlags: ['python_workers'],
      mainModule: 'main.py', modules: {'main.py': {py: main}}, env: {}, globalOutbound: null};
  }
  return runtimeCode(null, main);
}

// cpuMs: per-request CPU budget for the child. The packaged default (1000) is a
// policy choice; the capability lab raises it to probe the platform ceiling.
// subRequests: every SDK call and every workspace-module resolution is one.
// The old fixture value (12) is below what ordinary workflows need (a missing
// optional import alone costs one; per-org loops cost hundreds), so the
// default is 1000.
export const DEFAULT_ENGINE_SUBREQUESTS = 1000;
export function tenantEngineCode(api, {cpuMs, subRequests = DEFAULT_ENGINE_SUBREQUESTS} = {}) {
  const code = runtimeCode(api, ENGINE_PYTHON);
  return {...code, limits: {...code.limits, subRequests, ...(cpuMs ? {cpuMs} : {})}};
}

// Identity of the engine child: its entry script plus the packaged runtime.
// runtimeSha256 is computed from generated-runtime.mjs at deploy time
// (cloudflare.config.ts -> env.RUNTIME_SHA256), so hashing 15 MB never
// happens in a request. Loader ids are keyed by this: LOADER.get may hand back
// a cached isolate for a known id without calling the factory.
const engineDigests = new Map();
export function engineDigest(runtimeSha256) {
  // Without the runtime identity a runtime change would silently keep the old
  // loader id (and the old cached engine), so refuse instead.
  if (typeof runtimeSha256 !== 'string' || !/^[0-9a-f]{64}$/.test(runtimeSha256)) {
    return Promise.reject(new Error('RUNTIME_SHA256 is not configured'));
  }
  const key = runtimeSha256;
  if (!engineDigests.has(key)) {
    engineDigests.set(key, crypto.subtle.digest('SHA-256',
      new TextEncoder().encode(`${ENGINE_PYTHON}\n${key}`)).then((digest) =>
      [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('')));
  }
  return engineDigests.get(key);
}
