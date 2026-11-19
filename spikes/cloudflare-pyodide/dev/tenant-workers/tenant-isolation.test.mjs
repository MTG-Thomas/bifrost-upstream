// Integration: per-tenant persistent workers under local workerd.
//   node --test dev/tenant-workers/tenant-isolation.test.mjs
// The suite starts its own local workerd (wrangler.tenant-local.jsonc, a free
// port) and stops it afterwards; nothing has to be running beforehand.
//
// Proves: (a) different tenants get different isolates (hostile canary style);
// (b) same-tenant attempts serialize, with within-tenant residue reported as a
// documented limitation; (c) warm latency vs cold first-attempt latency;
// (d) gates reject undeclared modules/egress before effects; module source is
// PATH-keyed through the gateway (/api/sdk/modules/<path>,
// /api/sdk/modules-resolve) so recorded loads are REAL storage paths.
import {after, before, test} from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {unstable_startWorker} from 'wrangler';
import {engineDigest} from './generic-worker-code.mjs';
import {supervisorDigest} from './supervisor-digest.mjs';

async function sha256Hex(text) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

const WF = 'workflows/wf.py';
const GHOST = 'workflows/ghost.py';
const HELPER = 'workflows/helper.py';
const ATTEMPT = `t${Date.now().toString(36)}`;
const tenant = (role) => `tenant-${role}-${ATTEMPT}`;

// The identities cloudflare.config.ts computes at deploy, computed the same
// way here so the local supervisor reports real values.
const runtimeSha256 = createHash('sha256')
  .update(readFileSync(new URL('../runtime/generated-runtime.mjs', import.meta.url))).digest('hex');
const supervisorSha256 = supervisorDigest(fileURLToPath(new URL('./tenant-entry.mjs', import.meta.url)));

let worker;
let base;

before(async () => {
  worker = await unstable_startWorker({
    config: fileURLToPath(new URL('./wrangler.tenant-local.jsonc', import.meta.url)),
    bindings: {
      RUNTIME_SHA256: {type: 'plain_text', value: runtimeSha256},
      SUPERVISOR_SHA256: {type: 'plain_text', value: supervisorSha256},
    },
    dev: {server: {hostname: '127.0.0.1', port: 0}, inspector: false, logLevel: 'warn'},
  });
  await worker.ready;
  base = (await worker.url).href;
});

after(async () => {
  await worker?.dispose();
});

async function post(body) {
  const started = Date.now();
  const response = await fetch(base, {method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body), signal: AbortSignal.timeout(120000)});
  return {status: response.status, body: await response.json(), wallMs: Date.now() - started};
}

test('(a) tenants never see each other; (b) one tenant keeps its own residue', async () => {
  const a = tenant('A');
  const b = tenant('B');
  assert.equal((await post({tenant_id: a, action: 'plant_builtins', marker: 'marker-A'})).status, 200);
  const readB = await post({tenant_id: b, action: 'read_builtins'});
  assert.equal(readB.status, 200);
  assert.equal(readB.body.output.builtins ?? null, null, 'B sees A\'s builtins marker');

  assert.equal((await post({tenant_id: b, action: 'plant_module', marker: 'marker-B'})).status, 200);
  const readModule = await post({tenant_id: a, action: 'read_module'});
  assert.equal(readModule.status, 200);
  assert.equal(readModule.body.output.present, false, 'A sees B\'s sys.modules marker');

  // Documented limitation: a persistent per-tenant child keeps what an
  // earlier attempt of the SAME tenant left behind.
  const readA = await post({tenant_id: a, action: 'read_builtins'});
  assert.equal(readA.body.output.builtins, 'marker-A');
});

test('(b) same-tenant attempts serialize; different tenants run in parallel', async () => {
  const serial = tenant('serial');
  const parallel = tenant('parallel');
  const slowMs = 500;
  const [s1, s2] = await Promise.all([
    post({tenant_id: serial, action: 'slow', marker: 'first', ms: slowMs}),
    post({tenant_id: serial, action: 'slow', marker: 'second', ms: slowMs}),
  ]);
  assert.equal(s1.status, 200);
  assert.equal(s2.status, 200);
  const [r1, r2] = [s1.body.output, s2.body.output];
  assert.ok(r1.ended_at <= r2.started_at || r2.ended_at <= r1.started_at,
    `attempts interleaved: ${JSON.stringify({r1, r2})}`);
  const waits = [s1.body.queue_wait_ms, s2.body.queue_wait_ms].sort((x, y) => x - y);
  assert.ok(waits[1] >= slowMs * 0.8, `second waiter should queue ~${slowMs} ms: ${waits}`);

  // Warm both so a cold boot does not confound the wall-clock comparison.
  await post({tenant_id: parallel, action: 'honest'});
  await post({tenant_id: serial, action: 'honest'});
  const started = Date.now();
  await Promise.all([
    post({tenant_id: serial, action: 'slow', marker: 'third', ms: slowMs}),
    post({tenant_id: parallel, action: 'slow', marker: 'parallel', ms: slowMs}),
  ]);
  const wall = Date.now() - started;
  assert.ok(wall < slowMs * 1.8, `two tenants took ${wall} ms for 2x${slowMs} ms`);
});

test('(c) a tenant\'s second attempt reuses its child', async () => {
  const latency = tenant('latency');
  const cold = await post({tenant_id: latency, action: 'honest'});
  const warm = await post({tenant_id: latency, action: 'honest'});
  assert.equal(cold.status, 200);
  assert.equal(warm.status, 200);
  assert.equal(warm.body.worker_reused, true);
  assert.ok(warm.wallMs < cold.wallMs, `cold=${cold.wallMs} ms warm=${warm.wallMs} ms`);
});

test('(d) capability gate rejects before any module fetch or child effect', async () => {
  const gate = tenant('gate');
  const source = 'X = 1\n';
  const rejected = await post({tenant_id: gate, action: 'run', modules: {[WF]: source}, entry: WF,
    requirements: {modules: [WF, GHOST]}, manifest: {[WF]: await sha256Hex(source)}});
  assert.equal(rejected.status, 422);
  assert.equal(rejected.body.phase, 'capability-rejected');
  assert.ok(rejected.body.missing_modules.includes(GHOST));
  assert.deepEqual(rejected.body.api_calls, []);
  const after = await post({tenant_id: gate, action: 'honest'});
  assert.equal(after.status, 200);
  assert.equal(after.body.output.clean.builtins ?? null, null);
});

test('(d) egress gate rejects an unlisted host before effects', async () => {
  const rejected = await post({tenant_id: tenant('egress'), action: 'honest',
    url: 'https://evil.example/collect', egress: {hosts: ['example.com']}});
  assert.equal(rejected.status, 422);
  assert.equal(rejected.body.phase, 'egress-rejected');
});

test('(d) import audit names undeclared storage paths; declared runs reconcile clean', async () => {
  const audit = tenant('audit');
  const entry = 'import workflows.helper\ndef main():\n    return {"ok": True}\n';
  const helper = 'X = 1\n';
  const denied = await post({tenant_id: audit, action: 'run',
    modules: {[WF]: entry, [HELPER]: helper}, entry: WF, requirements: {modules: [WF]},
    manifest: {[WF]: await sha256Hex(entry), [HELPER]: await sha256Hex(helper)}});
  assert.equal(denied.status, 502);
  assert.equal(denied.body.phase, 'undeclared-import');
  const undeclared = denied.body.provider_outcome.undeclared_modules;
  assert.ok(undeclared.includes(HELPER));
  assert.ok(undeclared.every((path) => path.endsWith('.py') && path.includes('/')));

  const ok = 'def main():\n    return {"ok": True}\n';
  const run = await post({tenant_id: audit, action: 'run', modules: {[WF]: ok}, entry: WF,
    requirements: {modules: [WF]}, manifest: {[WF]: await sha256Hex(ok)}});
  assert.equal(run.status, 200);
  assert.equal(run.body.output.result.ok, true);
  assert.ok(run.body.output.recorded.includes(WF));
  assert.ok(run.body.api_calls.some((call) => call.path === `/api/sdk/modules/${WF}` && call.status === 200));
});

test('the child holds no credentials', async () => {
  const check = await post({tenant_id: tenant('env'), action: 'env_check'});
  assert.equal(check.status, 200);
  assert.equal(check.body.output.token_shaped_present, false);
  assert.equal(check.body.output.dummy_credential_present, false);
});

test('the code digest names the engine and the supervisor', async () => {
  const response = await fetch(new URL('/__code_digest', base));
  assert.equal(response.status, 200);
  const digest = await response.json();
  assert.match(digest.generic_code_sha256, /^[0-9a-f]{64}$/);
  assert.equal(digest.engine_code_sha256, await engineDigest(runtimeSha256));
  assert.equal(digest.supervisor_code_sha256, supervisorSha256);
});

test('the packaged workflow runtime cannot initialize the Linux agent database driver', async () => {
  const probe = (imports) => post({tenant_id: tenant('agent-import'), action: 'boot_probe', imports});
  const control = await probe(['httpx']);
  assert.equal(control.status, 200, JSON.stringify(control.body));
  assert.equal(control.body.report.ok, true);

  const agent = await probe(['asyncpg']);
  assert.equal(agent.status, 502, JSON.stringify(agent.body));
  assert.equal(agent.body.phase, 'dynamic-runtime');
  assert.match(agent.body.probe_error, /ModuleNotFoundError: No module named ['"]asyncpg['"]/);
});
