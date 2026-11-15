// Pure unit (fake LOADER, no workerd): per-tenant serialization, queue wait
// accounting, and gates firing BEFORE any loader effect. Run attempts are
// PATH-keyed: body.modules maps storage paths to source, body.entry is a
// storage path, and every child byte comes out of the TenantGateway after
// manifest-pin verification (never the raw store).
import {test} from 'node:test';
import assert from 'node:assert/strict';
import entry, {enqueueTenant, hostAttempt, resetTenantState, runtimeErrorClass, warmHost,
  WARM_ATTEMPTS} from './tenant-entry.mjs';
import {bootProbeCode, engineDigest, genericCodeDigest} from './generic-worker-code.mjs';

const WF = 'workflows/wf.py';
const GHOST = 'workflows/ghost.py';
const HELPER = 'workflows/helper.py';

async function sha256Hex(text) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function fakeChild(log, latencyMs = 0) {
  return {
    getEntrypoint: () => ({
      fetch: async () => {
        if (latencyMs) await delay(latencyMs);
        log.push(Date.now());
        return Response.json({clean: {}, recorded: []});
      },
    }),
  };
}

function fakeEnv(log, latencyMs = 0) {
  return {LOCAL_PROBE: 'true', LOADER: {get: (id, factory) => {
    log.getCalls.push(id);
    factory();
    return fakeChild(log.fetchCalls, latencyMs);
  }}};
}

const post = (body) => new Request('https://sup.invalid/', {method: 'POST', body: JSON.stringify(body)});

test('same-tenant attempts serialize; queue wait is recorded', async () => {
  resetTenantState();
  const order = [];
  const first = enqueueTenant('t1', async () => { order.push('a-start'); await delay(120); order.push('a-end'); return 'A'; });
  const second = enqueueTenant('t1', async () => { order.push('b-start'); await delay(10); order.push('b-end'); return 'B'; });
  const [ra, rb] = await Promise.all([first, second]);
  assert.deepEqual(order, ['a-start', 'a-end', 'b-start', 'b-end']);
  assert.equal(ra.result, 'A');
  assert.equal(rb.result, 'B');
  assert.ok(ra.queueWaitMs < 50, `first should not wait, got ${ra.queueWaitMs}`);
  assert.ok(rb.queueWaitMs >= 100, `second should wait ~120ms, got ${rb.queueWaitMs}`);
});

test('different tenants do not block each other', async () => {
  resetTenantState();
  const t0 = Date.now();
  const [ra, rb] = await Promise.all([
    enqueueTenant('tA', async () => { await delay(120); return 'A'; }),
    enqueueTenant('tB', async () => { await delay(120); return 'B'; }),
  ]);
  const wall = Date.now() - t0;
  assert.ok(wall < 200, `parallel tenants should overlap, took ${wall}ms`);
  assert.ok(ra.queueWaitMs < 50 && rb.queueWaitMs < 50);
});

test('a failing attempt does not break the chain', async () => {
  resetTenantState();
  const bad = enqueueTenant('t1', async () => { throw new Error('boom'); });
  const good = enqueueTenant('t1', async () => 'recovered');
  await assert.rejects(() => bad, /boom/);
  assert.equal((await good).result, 'recovered');
});

test('capability gate rejects BEFORE loader get AND before any module fetch', async () => {
  resetTenantState();
  const log = {getCalls: [], fetchCalls: []};
  const res = await entry.fetch(post({tenant_id: 'org-x', action: 'run',
    modules: {[WF]: 'X=1\n'}, entry: WF,
    requirements: {modules: [WF, GHOST]},
    manifest: {[WF]: await sha256Hex('X=1\n')}}), fakeEnv(log), {});
  assert.equal(res.status, 422);
  const body = await res.json();
  assert.deepEqual(body.missing_modules, [GHOST]);
  assert.deepEqual(body.api_calls, [], 'no gateway module fetch may happen on gate reject');
  assert.deepEqual(log.getCalls, [], 'loader must not be touched on gate reject');
});

test('non-path entry is rejected before any effect', async () => {
  resetTenantState();
  const log = {getCalls: [], fetchCalls: []};
  const res = await entry.fetch(post({tenant_id: 'org-x', action: 'run',
    modules: {wf: 'X=1'}, entry: 'wf',
    requirements: {modules: ['wf']}, manifest: {wf: 'hash'}}), fakeEnv(log), {});
  assert.equal(res.status, 400);
  assert.deepEqual(log.getCalls, []);
});

test('egress gate rejects BEFORE loader get (no worker effect)', async () => {
  resetTenantState();
  const log = {getCalls: [], fetchCalls: []};
  const res = await entry.fetch(post({tenant_id: 'org-x', action: 'honest',
    url: 'https://evil.example/collect', egress: {hosts: ['example.com']}}), fakeEnv(log), {});
  assert.equal(res.status, 422);
  assert.deepEqual(log.getCalls, [], 'loader must not be touched on gate reject');
});

test('undeclared STORAGE PATH import maps to terminal AFTER run (post-hoc, like executor)', async () => {
  resetTenantState();
  const log = {getCalls: [], fetchCalls: []};
  const child = {getEntrypoint: () => ({fetch: async () =>
    Response.json({result: {ok: true}, recorded: [WF, HELPER]})})};
  const env = {LOCAL_PROBE: 'true', LOADER: {get: (id, f) => { log.getCalls.push(id); f(); return child; }}};
  const wfSrc = 'X = 1\n';
  const res = await entry.fetch(post({tenant_id: 'org-x', action: 'run',
    modules: {[WF]: wfSrc, [HELPER]: wfSrc}, entry: WF,
    requirements: {modules: [WF]},
    manifest: {[WF]: await sha256Hex(wfSrc), [HELPER]: await sha256Hex(wfSrc)}}), env, {});
  assert.equal(res.status, 502);
  const body = await res.json();
  assert.equal(body.phase, 'undeclared-import');
  assert.deepEqual(body.provider_outcome.undeclared_modules, [HELPER]);
  assert.ok(body.api_calls.some((c) => c.path === `/api/sdk/modules/${HELPER}` && c.status === 200),
    `gateway fetched the undeclared-but-loadable path: ${JSON.stringify(body.api_calls)}`);
  assert.equal(log.getCalls.length, 1, 'run executed, then mapped post-hoc');
});

test('entry that the gateway cannot resolve fails BEFORE the loader effect', async () => {
  resetTenantState();
  const log = {getCalls: [], fetchCalls: []};
  const res = await entry.fetch(post({tenant_id: 'org-x', action: 'run',
    modules: {}, entry: WF,
    requirements: {modules: [WF]}, manifest: {[WF]: await sha256Hex('X = 1\n')}}), fakeEnv(log), {});
  assert.equal(res.status, 502);
  assert.equal((await res.json()).phase, 'module-preflight');
  assert.deepEqual(log.getCalls, [], 'unresolvable entry must never reach the loader');
});

test('declared path run succeeds with gateway api_calls and path-keyed child body', async () => {
  resetTenantState();
  let seen;
  const child = {getEntrypoint: () => ({fetch: async (req) => {
    seen = await req.json();
    return Response.json({result: {ok: true}, recorded: [WF]});
  }})};
  const env = {LOCAL_PROBE: 'true', LOADER: {get: () => child}};
  const wfSrc = 'X = 1\n';
  const res = await entry.fetch(post({tenant_id: 'org-x', action: 'run',
    modules: {[WF]: wfSrc}, entry: WF,
    requirements: {modules: [WF]}, manifest: {[WF]: await sha256Hex(wfSrc)}}), env, {});
  assert.equal(res.status, 200);
  const body = await res.json();
  assert.equal(body.output.result.ok, true);
  assert.deepEqual(Object.keys(seen.modules), [WF]);
  assert.equal(seen.entry, WF);
  assert.ok(body.api_calls.some((c) => c.path === `/api/sdk/modules/${WF}` && c.status === 200));
  assert.ok(body.api_calls.some((c) => c.path.startsWith('/api/sdk/modules-resolve')));
});

test('success envelope carries tenant id, reuse flag and queue wait', async () => {
  resetTenantState();
  const log = {getCalls: [], fetchCalls: []};
  const env = fakeEnv(log);
  const first = await (await entry.fetch(post({tenant_id: 'org-x', action: 'honest'}), env, {})).json();
  assert.equal(first.worker_reused, false);
  assert.ok(typeof first.queue_wait_ms === 'number');
  const second = await (await entry.fetch(post({tenant_id: 'org-x', action: 'honest'}), env, {})).json();
  assert.equal(second.worker_reused, true);
  assert.equal(first.tenant_worker_id, second.tenant_worker_id);
});

test('child request carries only the dummy placeholder, never a secret', async () => {
  resetTenantState();
  let seen;
  const child = {getEntrypoint: () => ({fetch: async (req) => {
    seen = await req.json(); return Response.json({recorded: []});
  }})};
  const env = {LOCAL_PROBE: 'true', LOADER: {get: () => child}};
  await entry.fetch(post({tenant_id: 'org-x', action: 'honest'}), env, {});
  assert.equal(seen.credential, 'gateway-injected');
  assert.ok(!JSON.stringify(seen).match(/eyJ[A-Za-z0-9_-]{10,}/), 'no JWT may ride the child request');
});

test('/__code_digest exposes the constant script hash and the engine identity', async () => {
  const runtimeA = 'a'.repeat(64);
  const digest = async (runtime) => (await (await entry.fetch(
    new Request('https://sup.invalid/__code_digest'),
    {LOCAL_PROBE: 'true', RUNTIME_SHA256: runtime}, {})).json());
  const a = await digest(runtimeA);
  assert.equal(a.generic_code_sha256, await genericCodeDigest());
  assert.equal(a.engine_code_sha256, await engineDigest(runtimeA));
  assert.notEqual((await digest('b'.repeat(64))).engine_code_sha256, a.engine_code_sha256,
    'a runtime change must change the engine identity (and so the loader id)');
});

test('runtime failures are classified without echoing their text', () => {
  const secret = 'eyJhbGciOiJIUzI1NiJ9.payloadpayload.signaturesig';
  const cpu = Object.assign(new Error(`Worker exceeded CPU time limit ${secret}`), {retryable: false});
  assert.deepEqual(runtimeErrorClass(cpu),
    {error_class: 'cpu_limit', retryable: false, overloaded: false});
  const reset = Object.assign(new Error('Durable Object reset because its code was updated.'),
    {retryable: true});
  assert.deepEqual(runtimeErrorClass(reset),
    {error_class: 'code_updated', retryable: true, overloaded: false});
  assert.equal(runtimeErrorClass(new Error(secret)).error_class, 'other');
  assert.ok(!JSON.stringify(runtimeErrorClass(cpu)).includes('eyJ'));
});

// Fake host: refuses attempts until warmed; warm() fails `failWarms` times.
function fakeHost({failWarms = 0, retryable = true, staysCold = false} = {}) {
  const seen = {stubs: 0, warms: 0, runs: 0, dispatched: 0};
  let ready = false;
  const host = {
    async warm() {
      seen.warms++;
      if (seen.warms <= failWarms) {
        throw Object.assign(new Error('Durable Object reset because its code was updated.'), {retryable});
      }
      if (!staysCold) ready = true;
      return {child_ms: 4500, first_request: true};
    },
    async run() {
      seen.runs++;
      if (!ready) return {needsWarm: true};
      seen.dispatched++;
      return {output: {status: 'Success'}, queueWaitMs: 0, timings: {child_ms: 90}};
    },
  };
  return {seen, getHost: () => { seen.stubs++; return host; }};
}

test('a cold host is warmed with retries, then the attempt is dispatched once', async () => {
  const {seen, getHost} = fakeHost({failWarms: 2});
  const result = await hostAttempt(getHost, {tenantId: 't'});
  assert.equal(result.output.status, 'Success');
  assert.equal(result.warm.warm_attempts, 3);
  assert.equal(seen.dispatched, 1, 'an engine attempt is never dispatched twice');
  assert.equal(seen.stubs, 5, 'every call uses a fresh stub (2 runs + 3 warms)');
});

test('a terminal host outcome is passed through, never retried', async () => {
  let runs = 0;
  const host = {run: async () => { runs++; return {terminal: {phase: 'output-budget'}}; }};
  const result = await hostAttempt(() => host, {tenantId: 't'});
  assert.deepEqual(result.terminal, {phase: 'output-budget'});
  assert.equal(runs, 1);
});

test('a warm host costs one round trip', async () => {
  const {seen, getHost} = fakeHost();
  await getHost().warm();
  const result = await hostAttempt(getHost, {tenantId: 't'});
  assert.equal(result.warm, undefined);
  assert.equal(seen.runs, 1);
});

test('warm-up gives up after its budget and never dispatches', async () => {
  const {seen, getHost} = fakeHost({failWarms: WARM_ATTEMPTS});
  await assert.rejects(hostAttempt(getHost, {tenantId: 't'}), /code was updated/);
  assert.equal(seen.warms, WARM_ATTEMPTS);
  assert.equal(seen.dispatched, 0);
});

test('a non-retryable warm-up failure is not retried', async () => {
  const {seen, getHost} = fakeHost({failWarms: 1, retryable: false});
  await assert.rejects(warmHost(getHost, 't'));
  assert.equal(seen.warms, 1);
});

test('a host that stays cold after warm-up reports not-ready instead of looping', async () => {
  const {seen, getHost} = fakeHost({staysCold: true});
  const result = await hostAttempt(getHost, {tenantId: 't'});
  assert.equal(result.notReady, true);
  assert.equal(seen.dispatched, 0);
});

test('boot probe accepts only dotted module names and never reaches the network', () => {
  const code = bootProbeCode({imports: ['pydantic', 'src.core.module_cache_sync'], bare: true});
  assert.match(code.modules['main.py'].py,
    /from workers import WorkerEntrypoint, Response\nimport pydantic\nimport src\.core\.module_cache_sync\n/,
    'workers is imported before the requested set');
  assert.equal(code.globalOutbound, null);
  assert.deepEqual(Object.keys(code.modules), ['main.py'], 'bare leaves the runtime out');
  for (const bad of [['os; import subprocess'], ['x\nimport y'], ['a..b'], 'pydantic']) {
    assert.throws(() => bootProbeCode({imports: bad}), TypeError);
  }
});

test('engine identity refuses an unconfigured runtime digest', async () => {
  await assert.rejects(engineDigest(undefined), /RUNTIME_SHA256 is not configured/);
  await assert.rejects(engineDigest('not-a-digest'), /RUNTIME_SHA256 is not configured/);
});

test('engine attempts pass the gates, then fail closed without a tenant host', async () => {
  resetTenantState();
  const env = {LOCAL_PROBE: 'true', LOADER: {get: () => assert.fail('supervisor must not load the engine')}};
  const res = await entry.fetch(post({tenant_id: 'org-x', action: 'engine',
    context: {engine_token: 'x'.repeat(20)}}), env, {});
  assert.equal(res.status, 502);
  assert.equal((await res.json()).phase, 'engine-unavailable');
});

test('legacy_load uses LOADER.load with no id (per-attempt meter control)', async () => {
  resetTenantState();
  const seen = {get: 0, load: 0, bodies: []};
  const child = {getEntrypoint: () => ({fetch: async (request) => {
    seen.bodies.push(await request.json());
    return Response.json({clean: {builtins: null}});
  }})};
  const env = {LOCAL_PROBE: 'true', LOADER: {
    get: () => { seen.get++; return child; },
    load: () => { seen.load++; return child; },
  }};
  const res = await entry.fetch(post({tenant_id: 'org-meter', action: 'legacy_load'}), env, {});
  const body = await res.json();
  assert.equal(res.status, 200);
  assert.equal(body.mode, 'legacy_load');
  assert.equal(seen.load, 1, 'per-attempt control must call LOADER.load');
  assert.equal(seen.get, 0, 'per-attempt control must not use a stable tenant id');
  assert.equal(seen.bodies[0].credential, 'gateway-injected');
});
