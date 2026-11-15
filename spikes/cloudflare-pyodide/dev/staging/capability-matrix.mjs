// Capability matrix: does the Cloudflare engine run what a normal Bifrost
// Linux worker runs? Each case is ONE workflow file, registered and executed on
// the staging Linux worker (same image as production) and run unchanged through
// the Cloudflare engine. Every case returns a small JSON verdict of what it
// tried, so the two backends are compared on behaviour, not just status.
//
//   node capability-matrix.mjs --api https://bifrost-stage.midtowntg.com \
//     --worker https://<matrix-worker>.workers.dev [--worker-label default] \
//     [--only a,b] [--out ../../results/capability-matrix-<label>.json] [--skip-linux]
//
// Secrets come from files: --token-file (staging access token), --secret-file
// (worker SPIKE_AUTH). Nothing secret is written to the evidence file.
//
// `cause` is the hypothesis a difference would test, recorded BEFORE running:
//   platform  a Workers/Pyodide property no configuration changes
//   policy    this spike's gateway/limits choice (separable by configuration)
//   bundle    a package not shipped in the runtime (separable by packaging)
//   version   Python 3.14 (Pyodide) vs 3.15 (worker image)
//   none      expected to behave identically
import {readFile, writeFile} from 'node:fs/promises';
import https from 'node:https';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

const args = process.argv.slice(2);
const flag = (name, fallback) => args.includes(`--${name}`) ? args[args.indexOf(`--${name}`) + 1] : fallback;
const API = flag('api', 'https://bifrost-stage.midtowntg.com');
const WORKER = flag('worker', null);
const LABEL = flag('worker-label', 'default');
const ONLY = (flag('only', '') ?? '').split(',').filter(Boolean);
const SKIP_LINUX = args.includes('--skip-linux');
const OUT = flag('out', new URL(`../../results/capability-matrix-${LABEL}.json`, import.meta.url));
if (!WORKER) throw new Error('--worker is required');
const token = (await readFile(flag('token-file', join(tmpdir(), 'pve-token.txt')), 'utf8')).trim();
const secret = (await readFile(flag('secret-file', join(tmpdir(), 'stage-secret.txt')), 'utf8')).trim();

// Shared probe helper, prepended to every case: run a callable, report
// ok/value or the exception type and a short message.
const HELPER = `
import asyncio, json, os, sys, time, traceback
from bifrost import workflow

def _probe(fn):
    try:
        return {"ok": True, "value": fn()}
    except BaseException as error:
        return {"ok": False, "error": type(error).__name__, "detail": str(error)[:160]}

async def _aprobe(coro_fn):
    try:
        return {"ok": True, "value": await coro_fn()}
    except BaseException as error:
        return {"ok": False, "error": type(error).__name__, "detail": str(error)[:160]}
`;

const wf = (body, {params = '', sync = false} = {}) =>
  `${HELPER}\n@workflow(name="__FN__")\n${sync ? 'def' : 'async def'} __FN__(${params}):\n${body.trim().split('\n').map((l) => `    ${l}`).join('\n')}\n`;

// compare: 'result' (exact JSON), 'ok' (only the ok flags), 'status' (status only)
const CASES = [
  // --- Engine and SDK semantics -------------------------------------------
  {name: 'sdk-two-calls', cause: 'none', compare: 'ok',
    code: wf(`
from bifrost import organizations, tables
orgs = await organizations.list()
rows = await tables.list()
return {"orgs": {"ok": isinstance(orgs, list)}, "tables": {"ok": isinstance(rows, list)}}`)},
  {name: 'parameters', cause: 'none', compare: 'result', input: {name: 'ab', count: 3},
    code: wf('return {"greeting": name * count, "types": [type(name).__name__, type(count).__name__]}',
      {params: 'name: str, count: int = 2'})},
  {name: 'sync-def-workflow', cause: 'none', compare: 'status',
    code: wf('return {"ok": True}', {sync: true})},
  {name: 'raises', cause: 'none', compare: 'status', expectFailure: true,
    code: wf('raise ValueError("expected diagnostic")')},
  {name: 'asyncio-gather-sdk', cause: 'none', compare: 'result',
    code: wf(`
from bifrost import tables
results = await asyncio.gather(tables.list(), tables.list(), tables.list())
return {"concurrent_calls": len(results), "all_lists": all(isinstance(r, list) for r in results)}`)},
  {name: 'config-roundtrip', cause: 'none', compare: 'result',
    code: wf(`
import uuid
from bifrost import config
key = "capmatrix_" + uuid.uuid4().hex[:10]
await config.set(key, "v-123")
value = await config.get(key)
await config.delete(key)
return {"value": value}`)},
  {name: 'tables-roundtrip', cause: 'none', compare: 'result',
    code: wf(`
import uuid
from bifrost import tables
name = "capmatrix_" + uuid.uuid4().hex[:10]
table = await tables.create(name)
await tables.insert(name, {"k": "v", "n": 7})
rows = await tables.query(name)
await tables.delete(str(table.id))
documents = getattr(rows, "documents", rows)
first = documents[0] if documents else None
data = getattr(first, "data", first) or {}
return {"n": data.get("n"), "k": data.get("k")}`)},
  {name: 'files-roundtrip', cause: 'none', compare: 'result',
    code: wf(`
import uuid
from bifrost import files
path = "capmatrix/" + uuid.uuid4().hex[:10] + ".txt"
await files.write(path, "hello-matrix")
text = await files.read(path)
await files.delete(path)
return {"text": text}`)},
  {name: 'workflows-list-sdk', cause: 'policy', compare: 'ok',
    code: wf(`
from bifrost import workflows
return {"list": await _aprobe(lambda: workflows.list())}`)},
  {name: 'executions-list-sdk', cause: 'policy', compare: 'ok',
    code: wf(`
from bifrost import executions
return {"list": await _aprobe(lambda: executions.list())}`)},

  // --- Language and runtime -----------------------------------------------
  {name: 'python-version', cause: 'version', compare: 'result',
    code: wf('return {"major_minor": list(sys.version_info[:2])}')},
  {name: 'py315-lazy-import-syntax', cause: 'version', compare: 'ok',
    code: wf('return {"compile": _probe(lambda: compile("lazy import json\\n", "<probe>", "exec") is not None)}')},
  {name: 'process-time', cause: 'platform', compare: 'ok',
    code: wf('return {"process_time": _probe(lambda: time.process_time() >= 0)}')},
  {name: 'zoneinfo', cause: 'none', compare: 'result',
    code: wf(`
from datetime import datetime
from zoneinfo import ZoneInfo
return {"chicago": _probe(lambda: str(datetime(2026, 1, 15, 12, tzinfo=ZoneInfo("America/Chicago")).utcoffset()))}`)},
  {name: 'ssl-default-context', cause: 'none', compare: 'ok',
    code: wf('import ssl\nreturn {"ctx": _probe(lambda: ssl.create_default_context() is not None)}')},
  {name: 'sqlite-memory', cause: 'none', compare: 'result',
    code: wf(`
def go():
    import sqlite3
    db = sqlite3.connect(":memory:")
    db.execute("create table t (v int)")
    db.executemany("insert into t values (?)", [(1,), (2,), (3,)])
    return db.execute("select sum(v) from t").fetchone()[0]
return {"sum": _probe(go)}`)},
  {name: 'stdlib-compute', cause: 'none', compare: 'result',
    code: wf(`
import hashlib, re, decimal, csv, io
blob = ("bifrost" * 150000).encode()
out = io.StringIO(); csv.writer(out).writerow(["a", 1, "b,c"])
return {"sha": hashlib.sha256(blob).hexdigest()[:16], "re": re.findall(r"\\d+", "a1b22c333"),
        "dec": str(decimal.Decimal("1.10") * 3), "csv": out.getvalue().strip()}`)},
  {name: 'result-rich-types', cause: 'none', compare: 'result',
    code: wf(`
import uuid, decimal
from datetime import datetime, timezone
return {"when": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        "id": uuid.UUID("12345678-1234-5678-1234-567812345678"), "amount": decimal.Decimal("1.50")}`)},
  // Optional-dependency pattern: a missing package must fail fast as
  // ModuleNotFoundError, not hang, so try/except fallbacks work.
  {name: 'missing-module-import', cause: 'none', compare: 'result', timeoutMs: 120000,
    code: wf(`
started = time.monotonic()
try:
    import definitely_not_a_real_package_xyz  # noqa: F401
    outcome = "imported"
except ModuleNotFoundError:
    outcome = "ModuleNotFoundError"
return {"outcome": outcome}`)},
  {name: 'tmp-file', cause: 'none', compare: 'result',
    code: wf(`
def go():
    import tempfile, pathlib
    p = pathlib.Path(tempfile.gettempdir()) / "capmatrix.txt"
    p.write_text("tmp-ok")
    value = p.read_text()
    p.unlink()
    return value
return {"tmp": _probe(go)}`)},

  // --- Processes and threads ----------------------------------------------
  {name: 'subprocess', cause: 'platform', compare: 'ok',
    code: wf(`
import subprocess
return {"echo": _probe(lambda: subprocess.run(["echo", "hi"], capture_output=True, text=True, timeout=10).stdout.strip())}`)},
  {name: 'threading-thread', cause: 'platform', compare: 'ok',
    code: wf(`
import threading
def go():
    box = []
    t = threading.Thread(target=lambda: box.append(42))
    t.start(); t.join(5)
    return box
return {"thread": _probe(go)}`)},
  {name: 'asyncio-to-thread', cause: 'platform', compare: 'ok',
    code: wf('return {"to_thread": await _aprobe(lambda: asyncio.to_thread(sum, [1, 2, 3]))}')},
  {name: 'thread-pool-executor', cause: 'platform', compare: 'ok',
    code: wf(`
from concurrent.futures import ThreadPoolExecutor
def go():
    with ThreadPoolExecutor(max_workers=2) as pool:
        return list(pool.map(lambda x: x * 2, [1, 2, 3]))
return {"pool": _probe(go)}`)},

  // --- Network ------------------------------------------------------------
  {name: 'egress-httpx', cause: 'none', compare: 'result', egress: ['example.com'],
    code: wf(`
import httpx
async def go():
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get("https://example.com/")
        return response.status_code
return {"get": await _aprobe(go)}`)},
  {name: 'egress-undeclared-host', cause: 'policy', compare: 'result',
    code: wf(`
import httpx
async def go():
    async with httpx.AsyncClient(timeout=15) as client:
        return (await client.get("https://example.org/")).status_code
return {"get": await _aprobe(go)}`)},
  {name: 'raw-tcp-socket', cause: 'platform', compare: 'ok',
    code: wf(`
import socket
def go():
    s = socket.create_connection(("example.com", 443), timeout=10)
    s.close()
    return True
return {"connect": _probe(go)}`)},
  {name: 'dns-getaddrinfo', cause: 'platform', compare: 'ok',
    code: wf('import socket\nreturn {"resolve": _probe(lambda: len(socket.getaddrinfo("example.com", 443)) > 0)}')},

  // --- Packages (preinstalled on the Linux image) --------------------------
  {name: 'package-imports', cause: 'bundle', compare: 'result',
    code: wf(`
import importlib
names = ["httpx", "pydantic", "yaml", "redis", "requests", "msal", "jwt", "cryptography", "lxml",
         "PIL", "jinja2", "openpyxl", "docx", "bcrypt", "nacl", "aiohttp", "websockets",
         "asyncpg", "uvloop", "tiktoken", "dns.resolver", "azure.identity", "openai", "anthropic",
         "croniter", "reportlab"]
out = {}
for name in names:
    try:
        importlib.import_module(name)
        out[name] = "ok"
    except ModuleNotFoundError:
        out[name] = "missing"
    except BaseException as error:
        out[name] = type(error).__name__
return out`)},

  // --- Resources ----------------------------------------------------------
  {name: 'memory-200mib', cause: 'platform', compare: 'status', timeoutMs: 240000,
    code: wf(`
block = bytearray(200 * 1024 * 1024)
for i in range(0, len(block), 4096):
    block[i] = 1
return {"mib": len(block) // (1024 * 1024), "touched": sum(block[::4096])}`)},
  {name: 'memory-64mib', cause: 'none', compare: 'result', timeoutMs: 240000,
    code: wf(`
block = bytearray(64 * 1024 * 1024)
for i in range(0, len(block), 4096):
    block[i] = 1
return {"mib": len(block) // (1024 * 1024)}`)},
  // Fixed work, not a wall-clock loop: Workers clocks do not advance during CPU.
  {name: 'cpu-fixed-work', cause: 'policy', compare: 'result', timeoutMs: 400000,
    code: wf(`
total = 0
for i in range(20_000_000):
    total += i % 7
return {"total": total}`)},
  // Above the Workers per-invocation CPU ceiling (5 min) even when the child's
  // budget is raised to it. Only meaningful on a worker with ENGINE_CPU_MS at
  // the maximum; run it explicitly with --only.
  {name: 'cpu-over-platform-cap', cause: 'platform', compare: 'result', timeoutMs: 900000, explicitOnly: true,
    code: wf(`
total = 0
for i in range(3_000_000_000):
    total += i & 7
return {"total": total}`)},
  {name: 'sleep-90s', cause: 'none', compare: 'result', timeoutMs: 300000,
    code: wf('await asyncio.sleep(90)\nreturn {"slept": 90}')},
  {name: 'result-6mb', cause: 'policy', compare: 'status', timeoutMs: 240000,
    code: wf('return {"blob": "x" * 6_000_000}')},
];

async function api(path, init = {}) {
  const response = await fetch(`${API}${path}`, {...init,
    headers: {Authorization: `Bearer ${token}`, ...(init.headers ?? {})}});
  const text = await response.text();
  let body;
  try { body = JSON.parse(text); } catch { body = {raw: text.slice(0, 300)}; }
  return {status: response.status, body};
}

let meCache;
const me = async () => (meCache ??= (await api('/auth/me')).body);

async function stage(c, runId) {
  const fn = `capmatrix_${c.name.replaceAll('-', '_')}_${runId}`;
  const path = `workflows/capmatrix/${c.name}_${runId}.py`;
  const content = c.code.replaceAll('__FN__', fn);
  const written = await api('/api/files/write', {method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({path, content, location: 'workspace'})});
  if (written.status !== 200 && written.status !== 204) throw new Error(`write ${path}: ${written.status}`);
  const pin = await api(`/api/sdk/modules/${path}`);
  if (pin.status !== 200) throw new Error(`pin ${path}: ${pin.status}`);
  return {fn, path, hash: pin.body.hash};
}

async function runLinux(c, staged) {
  const started = Date.now();
  const registered = await api('/api/workflows/register', {method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({path: staged.path, function_name: staged.fn})});
  let id = registered.body?.workflow_id ?? registered.body?.id;
  if (!id) {
    const list = await api('/api/workflows');
    id = (Array.isArray(list.body) ? list.body : []).find((w) => w.function_name === staged.fn)?.id;
  }
  if (!id) return {status: `RegisterFailed(${registered.status})`, detail: registered.body};
  const run = await api('/api/workflows/execute', {method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({workflow_id: id, input_data: c.input ?? {}, sync: false, transient: true})});
  if (run.status !== 200) return {status: `SubmitFailed(${run.status})`, detail: run.body};
  const deadline = Date.now() + (c.timeoutMs ?? 120000);
  while (Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 1500));
    const r = await api(`/api/executions/${run.body.execution_id}`);
    if (r.body?.status && !['Pending', 'Running'].includes(r.body.status)) {
      return {status: r.body.status, result: r.body.result ?? null,
        error: [r.body.error_type, r.body.error_message ?? r.body.error].filter(Boolean).join(': ').slice(0, 300) || null,
        engine_ms: r.body.duration_ms ?? null, wall_ms: Date.now() - started};
    }
  }
  return {status: 'Timeout', result: null, wall_ms: Date.now() - started};
}

async function runCloud(c, staged, runId) {
  const user = await me();
  const body = {tenant_id: `capm-${LABEL}-${c.name}-${runId}`, action: 'engine',
    execution_id: crypto.randomUUID(), requirements: {modules: [staged.path]},
    manifest: {[staged.path]: staged.hash},
    context: {caller: {user_id: user.id, email: user.email, name: 'Capability matrix'},
      organization_id: user.organization_id, name: staged.fn, function_name: staged.fn,
      file_path: staged.path, content_hash: staged.hash, engine_token: token, transient: true,
      parameters: c.input ?? {}},
    public_url: 'https://staging.invalid', egress: {hosts: c.egress ?? []}};
  const started = Date.now();
  let response;
  try {
    // node:https, not fetch: fetch's built-in 300 s headers timeout cut off
    // long cases client-side and masqueraded as a platform outcome.
    response = await postJson(WORKER, body, (c.timeoutMs ?? 120000) + 60000);
  } catch (error) {
    return {status: `Transport(${error.code ?? error.name})`, wall_ms: Date.now() - started};
  }
  const text = response.text;
  let json;
  try { json = JSON.parse(text); } catch { json = {raw: text.slice(0, 200)}; }
  const out = json.output ?? null;
  return {http: response.status, phase: json.phase ?? null, error_class: json.error_class ?? null,
    lab_error: json.lab_error ?? null,
    status: out?.status ?? json.phase ?? `HTTP${response.status}`, result: out?.result ?? null,
    error: [out?.error_type, out?.error_message ?? out?.error].filter(Boolean).join(': ').slice(0, 300) || null,
    engine_ms: json.timings?.child?.engine_ms ?? null, wall_ms: Date.now() - started,
    api_calls: json.timings?.api_calls ?? null};
}

function postJson(url, body, timeoutMs) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const request = https.request(url, {method: 'POST', timeout: timeoutMs, headers: {
      'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload),
      'X-Spike-Auth': secret, 'User-Agent': 'bifrost-capmatrix/1.0'}}, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({status: res.statusCode, text: Buffer.concat(chunks).toString('utf8')}));
      res.on('error', reject);
    });
    request.on('timeout', () => request.destroy(Object.assign(new Error('client timeout'), {code: 'ClientTimeout'})));
    request.on('error', reject);
    request.end(payload);
  });
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((k) => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

// Reduce a result to its ok flags (nested one level), for cases whose values
// legitimately differ (timings, counts) but whose capability must not.
function okShape(result) {
  if (!result || typeof result !== 'object') return result;
  return Object.fromEntries(Object.entries(result).map(([k, v]) =>
    [k, v && typeof v === 'object' && 'ok' in v ? v.ok : v]));
}

function verdict(c, linux, cloud) {
  const linuxOk = linux.status === 'Success';
  const cloudOk = cloud.status === 'Success';
  if (c.compare === 'status' || c.expectFailure) {
    if (linuxOk === cloudOk) return 'parity';
    return linuxOk ? 'cloud-gap' : 'linux-gap';
  }
  if (!linuxOk && !cloudOk) return 'both-fail';
  if (linuxOk !== cloudOk) return linuxOk ? 'cloud-gap' : 'linux-gap';
  const a = c.compare === 'ok' ? okShape(linux.result) : linux.result;
  const b = c.compare === 'ok' ? okShape(cloud.result) : cloud.result;
  return canonical(a) === canonical(b) ? 'parity' : 'differs';
}

const runId = Date.now().toString(36);
const evidence = {started_at: new Date().toISOString(), run_id: runId, api: API,
  worker_label: LABEL, cases: []};
for (const c of CASES) {
  if (ONLY.length ? !ONLY.includes(c.name) : c.explicitOnly) continue;
  const staged = await stage(c, runId);
  const [linux, cloud] = await Promise.all([
    SKIP_LINUX ? Promise.resolve({status: 'Skipped'}) : runLinux(c, staged),
    runCloud(c, staged, runId),
  ]);
  const v = SKIP_LINUX ? 'cloud-only' : verdict(c, linux, cloud);
  const shrink = (r) => (r && canonical(r).length > 400 ? `${canonical(r).slice(0, 400)}…` : r);
  evidence.cases.push({name: c.name, cause: c.cause, compare: c.compare, verdict: v,
    linux: {...linux, result: shrink(linux.result)}, cloud: {...cloud, result: shrink(cloud.result)}});
  console.log(`${v.padEnd(10)} ${c.name.padEnd(26)} cause=${c.cause.padEnd(8)} linux=${linux.status} cloud=${cloud.status}` +
    (v === 'parity' ? '' : `\n    linux: ${JSON.stringify(shrink(linux.result) ?? linux.error)}\n    cloud: ${JSON.stringify(shrink(cloud.result) ?? cloud.error ?? cloud.error_class)}`));
}
evidence.finished_at = new Date().toISOString();
evidence.summary = evidence.cases.reduce((acc, c) => ({...acc, [c.verdict]: (acc[c.verdict] ?? 0) + 1}), {});
await writeFile(OUT, JSON.stringify(evidence, null, 2) + '\n');
console.log('SUMMARY', JSON.stringify(evidence.summary));
