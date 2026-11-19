import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {appendFileSync, readFileSync, utimesSync, writeFileSync} from 'node:fs';
import {mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import {createServer as createTlsServer} from 'node:https';
import {createRequire} from 'node:module';
import {posix} from 'node:path';
import {buffer} from 'node:stream/consumers';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';
import {supervisorModules} from '../tenant-workers/supervisor-digest.mjs';
import {stageRuntimeLoader} from '../stage-runtime-loader.mjs';
import {hostedRelayPath, relayHeaders, relayHttp} from './relay-http.mjs';
import {capacityWorker} from './capacity-miniflare.mjs';

const root = new URL('../', import.meta.url);
const require = createRequire(new URL('tenant-workers/package.json', root));
const {WebSocket, WebSocketServer} = require('ws');
const config = JSON.parse(readFileSync(0, 'utf8'));
const origin = 'https://phase5-api.invalid';
const fixtureOrigin = 'https://phase5-fixture.invalid';
const wire = [];
const tokens = new Map();
const gates = new Map();
const hostedReads = new Map();
const sdkPath = '/api/sdk/tables/list';
let fault = null;
let worker;
let mf;
let nextHostedGate;
let provider;
let providerServer;
let summaryCalls = 0;

function evidence(label, data) {
  console.log(JSON.stringify({label, ...data}));
}

async function until(label, probe, ms = 30_000) {
  const deadline = Date.now() + ms;
  do {
    const value = await probe();
    if (value) return value;
    await delay(100);
  } while (Date.now() < deadline);
  throw new Error('Timed out waiting for ' + label);
}

function sql(query) {
  const output = execFileSync('docker', ['exec', '-i', config.project + '-postgres-1',
    'psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', '-U', 'bifrost', '-d', 'bifrost'], {
    encoding: 'utf8', input: query, timeout: 5000,
  }).trim();
  return output ? JSON.parse(output) : null;
}

function workerRow() {
  return sql(`SELECT json_build_object('state',state,'hello',remote_hello_sequence,
    'generation',remote_session_generation,'owner',remote_owner_id,
    'profile',runtime_profile,'runs',runs_handled) FROM execution_workers
    WHERE id = '${config.incarnation}'`);
}

function managedRows() {
  return sql(`SELECT coalesce(json_agg(json_build_object('id',id,'state',state,'uid',capacity_resource_uid,
    'launcher',remote_launcher_id,'profile',runtime_profile,'hello',remote_hello_sequence,
    'budget',memory_limit_bytes,'active',active_runs,
    'deleted',capacity_resource_deleted_at IS NOT NULL,'end',end_reason)), '[]'::json)
    FROM execution_workers WHERE capacity_scope = 'phase6-local'`);
}

function runRow(id) {
  return sql(`SELECT json_build_object('status',status,'result',result,'error',error_message,
    'completed',completed_at,'credential',credential_version,'tenant',organization_id,
    'claim',(SELECT json_agg(json_build_object('id',id,'worker',worker_id))
      FROM execution_worker_claims WHERE subject_id=e.id),
    'queued',(SELECT count(*) FROM work_dispatches WHERE subject_id=e.id),
    'logs',(SELECT count(*) FROM execution_logs WHERE execution_id=e.id
      AND message LIKE '%committed fixture%')) FROM executions e WHERE id='${id}'`);
}

async function api(path, body, token = config.admin) {
  const response = await fetch(config.origin + path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {Authorization: 'Bearer ' + token, 'Content-Type': 'application/json'},
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(10_000),
  });
  const data = response.ok ? await response.json() : null;
  await response.body?.cancel().catch(() => {});
  return {status: response.status, data};
}

// Harness-only upstream relay. Log protocol identities, never entire frames.
// The API still handles hello, claims, credentials, fencing and all settlement.
function observe(direction, raw) {
  const frame = JSON.parse(raw);
  const payload = frame.payload;
  let event;
  if (frame.event === 'hello') {
    event = {kind: 'hello', hello: frame.hello.hello_sequence};
  } else if (frame.event === 'welcome') {
    event = {kind: 'welcome', generation: frame.session_generation};
  } else if (frame.kind === 'command' && payload.action === 'cloudflare_summary') {
    assert.ok(config.summary);
    assert.ok(payload.payload.context.api_key === config.summary.databaseKey);
    event = {kind: 'cloudflare_summary', route: payload.payload.route_id};
  } else if (frame.kind === 'command' && payload.action === 'workflow') {
    const command = payload.payload;
    tokens.set(command.execution_id, command.context.engine_token);
    if (nextHostedGate) {
      gates.set(command.execution_id, nextHostedGate);
      nextHostedGate = undefined;
    }
    event = {kind: 'workflow', id: command.execution_id, route: command.route_id,
      generation: frame.session_generation};
  } else if (frame.kind === 'command' && ['terminate', 'ready'].includes(payload.action)) {
    event = {kind: payload.action, route: payload.payload.route_id, generation: frame.session_generation};
  } else if (payload?.event === 'inventory') {
    event = {kind: 'inventory', generation: frame.session_generation,
      active: payload.inventory.routes.map((route) => route.route_id),
      terminal: payload.inventory.terminal_routes.map((route) => route.event_id)};
  } else if (['result', 'summary_result'].includes(payload?.event)) {
    event = {kind: payload.event, generation: frame.session_generation, route: payload.route_id,
      event: payload._event_id, attempt: payload.attempt_state, success: payload.result.success,
      errorType: payload.result.error_type};
  } else if (frame.kind === 'ack') {
    event = {kind: 'ack', event: payload.event_id, generation: frame.session_generation};
  }
  if (!event) return false;
  const drop = direction === 'api' && fault === event.kind;
  if (drop) fault = null;
  wire.push({...event, ...(drop ? {dropped: true} : {})});
  evidence('wire', wire.at(-1));
  return drop;
}

const proxy = createServer(async (request, response) => {
  const path = request.url;
  try {
    if (config.hosted && !hostedRelayPath(request.method, path)) {
      response.writeHead(404).end();
      return;
    }
    if (config.summary && path === '/__fixture/v1/chat/completions') {
      assert.equal(request.method, 'POST');
      assert.ok(request.headers.authorization === 'Bearer ' + config.summary.databaseKey);
      const body = JSON.parse((await buffer(request)).toString());
      assert.equal(body.model, 'gpt-4o-mini');
      assert.equal(body.stream, false);
      assert.equal(JSON.parse(body.messages[1].content).input.question, 'Cloudflare summary fixture');
      summaryCalls++;
      response.writeHead(200, {'Content-Type': 'application/json'}).end(JSON.stringify({
        model: body.model, choices: [{message: {content: JSON.stringify({
          asked: 'Summarize fixture', did: 'Collated the run', metadata: {source: 'stub'}, confidence: 0.75,
        })}}],
        usage: {prompt_tokens: 88, completion_tokens: 11},
      }));
      return;
    }
    if (path.startsWith('/__fixture/hold/')) {
      const scenario = path.split('/').at(-1);
      assert.ok(gates.has(scenario));
      const gate = gates.get(scenario);
      gate.entered = true;
      await gate.release.promise;
      response.end('released');
      return;
    }
    if (path === '/__fixture/__observed/403' && request.method === 'POST') {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      wire.push({kind: 'late-egress', route: Buffer.concat(chunks).toString(), status: 403});
      evidence('wire', wire.at(-1));
      response.writeHead(204).end();
      return;
    }
    if (path.startsWith('/__fixture/')) {
      wire.push({kind: 'unexpected-egress', path});
      response.writeHead(500).end();
      return;
    }
    const upstream = await relayHttp(request, config.origin + path);
    const body = await buffer(upstream);
    const status = upstream.statusCode;
    if (config.hosted && path === sdkPath) {
      const token = request.headers.authorization?.replace(/^Bearer /, '');
      const entry = [...tokens].find(([, value]) => value === token);
      if (entry) {
        const [id] = entry;
        const reads = hostedReads.get(id) ?? [];
        reads.push(status);
        hostedReads.set(id, reads);
        evidence('hosted-sdk-read', {id, status, count: reads.length});
        const gate = gates.get(id);
        if (gate && status >= 200 && status < 300) {
          gate.entered = true;
          await gate.release.promise;
        }
      }
    }
    response.writeHead(status, relayHeaders(upstream.headers));
    response.end(body);
  } catch {
    response.writeHead(502).end();
  }
});
const sockets = new WebSocketServer({noServer: true});
proxy.on('upgrade', (request, socket, head) => {
  if (request.url !== '/api/worker-control') return socket.destroy();
  const upstream = new WebSocket(config.origin.replace('http:', 'ws:') + request.url);
  upstream.on('error', () => socket.destroy());
  upstream.on('open', () => sockets.handleUpgrade(request, socket, head, (launcher) => {
    const send = (from, to, direction) => from.on('message', (message) => {
      if (to.readyState !== WebSocket.OPEN) return;
      if (observe(direction, message.toString())) {
        launcher.close(1012, 'phase5a injected transport loss');
        upstream.close(1012);
      } else {
        to.send(message.toString());
      }
    });
    send(launcher, upstream, 'launcher');
    send(upstream, launcher, 'api');
    upstream.on('close', (code) => {
      wire.push({kind: 'api-close', code});
      evidence('wire', wire.at(-1));
      if (launcher.readyState === WebSocket.OPEN) launcher.close([1005, 1006].includes(code) ? 1012 : code);
    });
    launcher.on('close', () => upstream.close());
    launcher.on('error', () => upstream.close());
  }));
});

const harnessEntry = `
  import {CloudflareLauncher as Launcher} from './cloudflare-launcher/launcher-do.mjs';
  import {TenantHost as Host, TenantApi} from './tenant-workers/tenant-api.mjs';
  import production from './tenant-workers/launcher-entry.mjs';
  export {TenantApi};
  export class CloudflareLauncher extends Launcher {
    // Test-only process loss with the real SQLite journal retained.
    reset() { this.ctx.abort('injected launcher reset'); }
    ledger() {
      const rows = this.ctx.storage.sql.exec(
        "SELECT key, data FROM ledger WHERE key = 'control' OR key = 'slot' OR key LIKE 'slot:%' ORDER BY key, part").toArray();
      const values = {};
      for (const {key, data} of rows) {
        const value = (values[key] ??= {decoder: new TextDecoder(), text: ''});
        value.text += value.decoder.decode(data, {stream: true});
      }
      const read = (value) => value && JSON.parse(value.text + value.decoder.decode());
      const control = read(values.control);
      const slots = Object.keys(values).filter((key) => key !== 'control').map((key) => {
        const slot = read(values[key]);
        return {state: slot.state, route: slot.route.route_id,
          event: slot.terminal?._event_id, attempt: slot.terminal?.attempt_state};
      });
      return {control, ...(slots.length ? {slots} : {})};
    }
  }
  export class TenantHost extends Host {
    async stopWorkflow(route) {
      const inflight = await this.ctx.storage.get('inflight');
      const stopped = super.stopWorkflow(route);
      if (!inflight) return stopped;
      // Test-only late egress at the production gateway boundary.
      const response = await super.egress(new Request('${fixtureOrigin}/after-stop'), inflight.generation);
      await stopped;
      if (response.status !== 403) throw new Error('Late egress escaped');
      await fetch('${fixtureOrigin}/__observed/403', {method: 'POST', body: route});
    }
  }
  export default {
    async fetch(request, env, ctx) {
      const url = new URL(request.url);
      const stub = ctx.exports.CloudflareLauncher.getByName(url.searchParams.get('id') ?? env.BIFROST_WORKER_INCARNATION_ID);
      switch (url.pathname) {
        case '/start':
          await production.scheduled({}, env, ctx);
          return new Response(null, {status: 204});
        case '/ledger': return Response.json(await stub.ledger());
        case '/reset':
          try { await stub.reset(); }
          catch (error) {
            if (error.durableObjectReset) return new Response(null, {status: 204});
            throw error;
          }
      }
      return new Response(null, {status: 404});
    },
  };
`;

function module(contents) {
  return {type: 'esm', contents};
}

async function launch(port) {
  const {Miniflare} = createRequire(require.resolve('wrangler'))('miniflare');
  const artifact = readFileSync(config.runtime);
  const staged = new URL('runtime/', root);
  await mkdir(staged, {recursive: true});
  await stageRuntimeLoader(staged);
  const entry = fileURLToPath(new URL('tenant-workers/launcher-entry.mjs', root));
  const modules = Object.fromEntries(supervisorModules(entry)
    .map(([path, source]) => [posix.join('dev', path), module(source)]));
  modules['dev/runtime/generated-runtime.mjs'] = module(artifact.toString('utf8'));
  modules['dev/phase5-harness.mjs'] = module(harnessEntry);
  const exports = {CloudflareLauncher: {type: 'durable-object', storage: 'sqlite'},
    TenantHost: {type: 'durable-object', storage: 'sqlite'}};
  const env = Object.fromEntries(Object.entries({
    API_ORIGIN: origin, RUNTIME_SHA256: createHash('sha256').update(artifact).digest('hex'),
    ...(config.phase6 ? {BIFROST_CLOUDFLARE_MANAGED: 'true'} : {
      BIFROST_CLOUDFLARE_LAUNCHER_ENABLED: 'true', BIFROST_WORKER_CONTROL_SECRET: config.controlKey,
      BIFROST_WORKER_INCARNATION_ID: config.incarnation, BIFROST_WORKER_LAUNCHER_ID: config.launcher,
      BIFROST_WORKER_RESOURCE_UID: config.resource, BIFROST_WORKER_VERSION: config.build,
    }),
  }).map(([key, value]) => [key, {type: 'text', value}]));
  mf = new Miniflare({cf: false, workers: [{
    dev: {outboundService: {type: 'worker', worker: 'relay'}},
    config: {name: 'launcher', compatibilityDate: '2026-10-30', exports,
      env: {...env, LOADER: {type: 'worker-loader'},
        ...(config.summary ? {SUMMARY_PROVIDER: {type: 'worker', worker: 'summary'}} : {})},
      manifest: {mainModule: 'dev/phase5-harness.mjs', modulesRoot: fileURLToPath(new URL('../', root)), modules}},
  }, {
    config: {name: 'relay', compatibilityDate: '2026-10-30',
      manifest: {mainModule: 'relay.mjs', modules: {'relay.mjs': module(`
        export default {fetch(request) {
          const url = new URL(request.url);
          if (![${JSON.stringify(origin)}, ${JSON.stringify(fixtureOrigin)}].includes(url.origin)) {
            return new Response('Harness origin refused', {status: 403});
          }
          const path = (url.origin === ${JSON.stringify(fixtureOrigin)} ? '/__fixture' : '') + url.pathname;
          return fetch(new Request('http://127.0.0.1:${port}' + path + url.search, request));
        }};
      `)}}},
  }, ...(config.summary ? [{
    dev: {outboundService: {type: 'worker', worker: 'relay'}},
    config: {name: 'summary', compatibilityDate: '2026-10-30',
      env: {BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT: {type: 'text', value: config.summary.endpoint}},
      manifest: {mainModule: 'summary-provider.mjs', modules: {
        'summary-provider.mjs': module(readFileSync(new URL('cloudflare-launcher/summary-provider.mjs', root), 'utf8')),
      }},
    },
  }] : []), ...(config.phase6 ? [capacityWorker({...config.phase6, version: config.build, maxWorkers: 1})] : [])]});
  worker = await mf.getWorker('launcher');
  if (config.phase6) provider = await mf.getWorker('capacity');
}

async function control(path, status = 204) {
  const response = await worker.fetch('https://harness.invalid/' + path +
    (config.phase6 ? '?id=' + config.incarnation : ''));
  assert.equal(response.status, status, await response.clone().text());
  return status === 200 ? response.json() : null;
}

function reloadApi() {
  const file = new URL('../../../../api/src/config.py', import.meta.url);
  const now = new Date();
  utimesSync(file, now, now);
}

async function startRun(scenario, hold = true, tenant) {
  const gate = {release: Promise.withResolvers(), entered: false};
  gates.set(scenario, gate);
  if (config.hosted && hold) nextHostedGate = gate;
  const fixture = tenant ? scenario : (config.hosted ? 'hosted' : 'local');
  const submitted = await api('/api/workflows/execute', {
    workflow_id: config.workflows[scenario === 'allowance' ? 'allowance' : fixture],
    input_data: {case: scenario}, sync: false,
    ...(tenant ? {org_id: tenant} : {}),
  });
  assert.equal(submitted.status, 200);
  const id = submitted.data.execution_id;
  assert.match(id, /^[0-9a-f-]{36}$/);
  const dispatch = await until('workflow dispatch', () => {
    const event = wire.find((event) => event.kind === 'workflow' && event.id === id);
    if (!event) {
      const row = runRow(id);
      if (row?.completed) throw new Error('Workflow settled before launcher dispatch: ' + JSON.stringify(row));
    }
    return event;
  });
  const row = runRow(id);
  assert.equal(row.claim?.length, 1);
  assert.equal(row.claim[0].worker, config.incarnation);
  assert.equal(row.credential, 2);
  evidence('claimed', {scenario, id, route: dispatch.route, row});
  if (hold) await until('Python fixture entered the hold', () => {
    if (gate.entered) return true;
    const result = wire.find((event) => event.kind === 'result' && event.route === dispatch.route);
    if (result) throw new Error('Launcher settled before the fixture entered: ' + JSON.stringify(runRow(id)));
    return false;
  });
  return {id, gate, route: dispatch.route, claim: row.claim};
}

async function settle(scenario, run, status, ackCount = 1, dispatchCount = 1) {
  await until('terminal acknowledgement', () => {
    const ids = new Set(wire.filter((event) => event.kind === 'result' && event.route === run.route)
      .map((event) => event.event));
    return wire.filter((event) => event.kind === 'ack' && ids.has(event.event)).length >= ackCount;
  });
  if (!config.hosted) await until('launcher slot cleared', async () =>
    !(await control('ledger', 200)).slots?.some(({route}) => route === run.route));
  const terminals = wire.filter((event) => event.kind === 'result' && event.route === run.route);
  assert.equal(terminals.length, ackCount);
  assert.equal(new Set(terminals.map((event) => event.event)).size, 1);
  const row = runRow(run.id);
  assert.equal(row.status, status);
  assert.deepEqual(row.claim, run.claim);
  assert.equal(row.queued, 0);
  if (status === 'Success') {
    assert.deepEqual(row.result, {case: scenario, table_count: 0, ...config.phase6?.results?.[scenario]});
    assert.equal(row.logs, 1);
  }
  assert.equal(wire.filter((event) => event.kind === 'workflow' && event.id === run.id).length, dispatchCount);
  evidence('settled', {scenario, id: run.id, row, ...(config.hosted ? {} : {ledger: await control('ledger', 200)})});
  return row;
}

async function startCapacityEndpoint() {
  providerServer = createTlsServer({
    key: readFileSync('.phase6-key.pem'), cert: readFileSync('.phase6-ca.pem'),
  }, async (req, res) => {
    try {
      const body = req.method === 'GET' ? undefined : await buffer(req);
      const start = /^\/v1\/workers\/([0-9a-f-]{36})\/start$/.exec(req.url);
      if (start && req.method === 'POST' && req.headers.authorization === 'Bearer ' + config.phase6.token) {
        const row = managedRows().find(({id}) => id === start[1]);
        assert.equal(row?.state, 'starting');
        assert.equal(row.uid, JSON.parse(body).resource_uid);
        assert.equal(row.launcher, row.id);
        assert.equal(row.profile, 'cloudflare-python-v1');
        assert.equal(row.hello, 0);
        assert.equal(row.budget, config.phase6.slots * 128 * 2 ** 20);
        config.incarnation = row.id;
        evidence('provider-start-bound', {row});
      }
      const reply = await provider.fetch('https://provider.invalid' + req.url,
        {method: req.method, headers: req.headers, body});
      if (req.method === 'GET' && req.url === '/v1/capabilities' && reply.ok) config.phase6.discovered = true;
      if (req.method === 'DELETE' && reply.ok) {
        const {worker_id, state} = await reply.clone().json();
        evidence('provider-delete', {worker: worker_id, state});
      }
      res.writeHead(reply.status, Object.fromEntries(reply.headers));
      res.end(Buffer.from(await reply.arrayBuffer()));
    } catch (error) {
      evidence('provider-relay-error', {type: error.name});
      res.writeHead(502).end();
    }
  });
  await new Promise((resolve, reject) => {
    providerServer.once('error', reject);
    providerServer.listen(0, config.phase6.providerHost, resolve);
  });
  const endpoint = `https://${config.phase6.providerHost}:${providerServer.address().port}`;
  appendFileSync('.phase5-api.env',
    `\nBIFROST_WORKER_CAPACITY_URL=${endpoint}\nBIFROST_WORKER_CAPACITY_TOKEN=${config.phase6.token}\n`);
  reloadApi();
}

async function phase6Checks() {
  const linuxLaunchers = ['worker', 'scheduler'].map((service) => config.project + '-' + service + '-1');
  execFileSync('docker', ['pause', ...linuxLaunchers]);
  try {
    await delay(3100); // Wait for the existing Linux samples to go stale.
    await launch(proxy.address().port);
    await startCapacityEndpoint();
    await until('API selected the capacity provider', () => config.phase6.discovered);
    const submitted = await api('/api/workflows/execute', {
      workflow_id: config.workflows.linux, input_data: {}, sync: false,
    });
    assert.equal(submitted.status, 200);
    const linux = submitted.data.execution_id;
    await until('Linux-only dispatch queued', () => runRow(linux)?.queued === 1);
    await delay(6500);
    assert.deepEqual(managedRows(), []);
    assert.equal(runRow(linux).claim, null);
    evidence('linux-only-zero-launches', {id: linux, row: runRow(linux)});
    const scenarios = ['phase6-a', 'phase6-b'];
    const runs = await Promise.all(scenarios.map((scenario, index) =>
      startRun(scenario, true, config.phase6.tenants[index])));
    const [row] = managedRows();
    const ledger = await control('ledger', 200);
    assert.deepEqual(new Set(ledger.slots.map(({route}) => route)), new Set(runs.map(({route}) => route)));
    assert.ok(ledger.slots.every(({state}) => state === 'entering'));
    assert.deepEqual(runs.map(({id}) => {
      const run = runRow(id);
      assert.equal(run.status, 'Running');
      return run.tenant;
    }), config.phase6.tenants);
    assert.equal(managedRows().length, 1);
    await until('API reports two occupied slots', () => managedRows()[0].active === 2);
    evidence('phase6-two-tenants-overlap', {worker: row.id, runs: runs.map(({id}) => id), ledger});
    for (const [index, run] of runs.entries()) {
      const credential = tokens.get(run.id);
      assert.equal((await api(sdkPath, {}, credential)).status, 200);
      run.gate.release.resolve();
      await settle(scenarios[index], run, 'Success');
      assert.equal((await api(sdkPath, {}, credential)).status, 401);
    }
    const released = await until('idle worker retired and provider deletion confirmed', () => {
      const [row] = managedRows();
      return row?.state === 'retired' && row.deleted && row;
    });
    assert.equal(released.end, 'idle');
    assert.deepEqual(await control('ledger', 200), {});
    const hellos = wire.filter((event) => event.kind === 'hello').length;
    await delay(16_000);
    assert.equal(wire.filter((event) => event.kind === 'hello').length, hellos);
    assert.equal(managedRows().length, 1);
    assert.equal(runRow(linux).claim, null);
    evidence('phase6-idle-release', {row: released, hellos, linuxOnly: linux, completed: runs.map(({id}) => id)});
  } finally {
    execFileSync('docker', ['unpause', ...linuxLaunchers]);
  }
}

async function hostedChecks() {
  const happy = await startRun('happy');
  const token = tokens.get(happy.id);
  assert.deepEqual(hostedReads.get(happy.id), [200]);
  happy.gate.release.resolve();
  await settle('happy', happy, 'Success');
  assert.equal((await api(sdkPath, {}, token)).status, 401);
  evidence('run-credential', {id: happy.id, before: 200, after: 401});

  const limits = await startRun('allowance', false);
  const limitRow = await settle('allowance', limits, 'Failed');
  assert.match(limitRow.error, /Too many subrequests|subrequest allowance exhausted/i);
  const limitReads = hostedReads.get(limits.id);
  assert.ok(limitReads?.length > 0 && limitReads.length <= 4 && limitReads.every((status) => status === 200));
  evidence('allowance', {id: limits.id, allowed: 4, upstreamReads: limitReads.length});

  const takeover = await startRun('takeover');
  const beforeTakeover = workerRow();
  reloadApi();
  await until('replacement API inventories the live route', () => wire.some((event) =>
    event.kind === 'inventory' && event.generation > beforeTakeover.generation && event.active.includes(takeover.route)));
  const afterTakeover = workerRow();
  assert.notEqual(afterTakeover.owner, beforeTakeover.owner);
  assert.ok(afterTakeover.hello > beforeTakeover.hello);
  evidence('takeover', {before: beforeTakeover, after: afterTakeover});
  takeover.gate.release.resolve();
  await settle('takeover', takeover, 'Success');
}

async function hostedResetCheck() {
  const reset = await startRun('reset');
  const beforeReset = workerRow();
  evidence('redeploy-requested', {id: reset.id, route: reset.route, before: beforeReset});
  await verifyHostedReset(reset, beforeReset, 1);
  reset.gate.release.resolve();
}

async function verifyHostedReset(reset, beforeReset, dispatchCount) {
  const inventory = await until('deployed launcher recovered the interrupted route', () => wire.find((event) =>
    event.kind === 'inventory' && event.generation > beforeReset.generation && event.terminal.length > 0));
  const resetRow = await settle('reset', reset, 'Failed', 1, dispatchCount);
  assert.match(resetRow.error, /may already have taken effect/);
  assert.ok(workerRow().hello > beforeReset.hello);
  const terminal = wire.find((event) => event.kind === 'result' && event.route === reset.route);
  assert.equal(terminal.attempt, 'uncertain');
  assert.equal(terminal.errorType, 'LauncherReset');
  assert.deepEqual(inventory.terminal, [terminal.event]);
  evidence('launcher-reset', {id: reset.id, before: beforeReset, after: workerRow(), inventory, terminal});
}

async function hostedCpuCheck() {
  const cpu = await startRun('cpu', false);
  const cpuRow = await settle('cpu', cpu, 'Failed');
  assert.match(cpuRow.error, /may already have taken effect/);
  const cpuTerminal = wire.find((event) => event.kind === 'result' && event.route === cpu.route);
  assert.equal(cpuTerminal.attempt, 'uncertain');
  assert.deepEqual(hostedReads.get(cpu.id), [200]);
  evidence('cpu', {id: cpu.id, budgetMs: 1000, terminal: cpuTerminal});
}

async function localChecks() {
  const happy = await startRun('happy');
  const token = tokens.get(happy.id);
  assert.equal((await api(sdkPath, {}, token)).status, 200);
  happy.gate.release.resolve();
  await settle('happy', happy, 'Success');
  assert.equal((await api(sdkPath, {}, token)).status, 401);
  evidence('run-credential', {id: happy.id, before: 200, after: 401});

  const beforeWelcome = workerRow();
  fault = 'welcome';
  for (const socket of sockets.clients) socket.close(1012, 'phase5a lost welcome setup');
  await until('higher hello after dropped welcome', () => {
    const lost = wire.find((event) => event.kind === 'welcome' && event.dropped);
    return lost && wire.find((event) => event.kind === 'inventory' && event.generation > lost.generation);
  });
  assert.ok(workerRow().hello >= beforeWelcome.hello + 2);
  evidence('lost-welcome', {before: beforeWelcome, after: workerRow(), ledger: await control('ledger', 200)});

  const replay = await startRun('lost-ack');
  fault = 'ack';
  replay.gate.release.resolve();
  await until('first ACK dropped', () => wire.some((event) => event.kind === 'ack' && event.dropped));
  const first = runRow(replay.id);
  assert.equal(first.status, 'Success');
  assert.deepEqual(await settle('lost-ack', replay, 'Success', 2), first);

  const takeover = await startRun('takeover');
  const beforeTakeover = workerRow();
  reloadApi();
  await until('replacement API inventories the live route', () => wire.some((event) =>
    event.kind === 'inventory' && event.generation > beforeTakeover.generation && event.active.includes(takeover.route)));
  assert.notEqual(workerRow().owner, beforeTakeover.owner);
  evidence('takeover', {before: beforeTakeover, after: workerRow()});
  takeover.gate.release.resolve();
  await settle('takeover', takeover, 'Success');

  const reset = await startRun('reset');
  assert.equal((await control('ledger', 200)).slots[0].state, 'entering');
  await control('reset');
  const resetRow = await settle('reset', reset, 'Failed');
  assert.match(resetRow.error, /may already have taken effect/);
  assert.ok(wire.some((event) => event.kind === 'result' && event.route === reset.route &&
    event.attempt === 'uncertain' && event.errorType === 'LauncherReset'));
  reset.gate.release.resolve();

  const cancel = await startRun('cancel');
  const cancelling = await api('/api/executions/' + cancel.id + '/cancel', {});
  assert.equal(cancelling.status, 200);
  assert.equal(cancelling.data.status, 'Cancelling');
  const cancelled = await settle('cancel', cancel, 'Failed');
  assert.match(cancelled.error, /may already have taken effect.*cancelled/);
  assert.equal(wire.filter((event) => event.kind === 'result' && event.route === cancel.route &&
    event.attempt === 'uncertain' && event.errorType === 'CancelledError').length, 1);
  assert.ok(wire.some((event) => event.kind === 'terminate' && event.route === cancel.route));
  await until('late egress refused', () => wire.some((event) => event.kind === 'late-egress' && event.route === cancel.route));
  assert.ok(wire.every((event) => event.kind !== 'unexpected-egress'));
  cancel.gate.release.resolve();
}

async function phase5Checks() {
  const prepared = workerRow();
  assert.ok((config.port ? ['starting', 'ready'] : ['starting']).includes(prepared.state));
  let interrupted;
  if (config.resetRoute) {
    const id = config.resetRoute.split(':')[1];
    const row = runRow(id);
    assert.equal(row.status, 'Running');
    assert.equal(row.claim?.length, 1);
    assert.equal(row.claim[0].worker, config.incarnation);
    interrupted = {id, route: config.resetRoute, claim: row.claim};
  }
  // Both debug launchers advertise workflow.execute on the Normal lane.
  const linuxLaunchers = ['worker', 'scheduler'].map((service) => config.project + '-' + service + '-1');
  execFileSync('docker', ['pause', ...linuxLaunchers]);
  try {
    // Claims live in the API. Wait until the last Linux samples have aged
    // past host.py's three-second admission window after pausing the sources.
    const linuxSamplesStale = delay(3100);
    if (config.hosted) {
      const {incarnation, launcher, resource, build, controlKey} = config;
      if (!config.port) {
        writeFileSync('.phase5b-deploy.json', JSON.stringify({
          port: proxy.address().port, incarnation, launcher, resource, build, controlKey,
        }), {flag: 'wx', mode: 0o600});
      }
      evidence('hosted-relay-ready', {port: proxy.address().port});
    } else {
      await launch(proxy.address().port);
      await control('start');
    }
    await until('hello inventory and ready', () => wire.some((event) => event.kind === 'ready'),
      config.hosted ? 180_000 : 30_000);
    assert.equal(workerRow().state, 'ready');
    evidence('enrollment', {row: workerRow(), ...(config.hosted ? {} : {ledger: await control('ledger', 200)})});
    await linuxSamplesStale;
    if (config.hosted) {
      if (interrupted) await verifyHostedReset(interrupted, prepared, 0);
      else {
        if (!config.fromReset) await hostedChecks();
        await hostedResetCheck();
      }
      await hostedCpuCheck();
    } else {
      await localChecks();
    }
  } finally {
    execFileSync('docker', ['unpause', ...linuxLaunchers]);
  }

  assert.equal(sql(`UPDATE execution_workers SET state='retired'
    WHERE id='${config.incarnation}' RETURNING to_json(state)`), 'retired');
  for (const socket of sockets.clients) socket.close(1012, 'phase5a reconnect after retirement');
  await until('API returned close code 4001', () => wire.some((event) => event.kind === 'api-close' && event.code === 4001));
  const retired = config.hosted ? null : await until('retirement persisted', async () => {
    const ledger = await control('ledger', 200);
    return ledger.control?.state === 'retired' && ledger;
  });
  const helloCount = wire.filter((event) => event.kind === 'hello').length;
  if (!config.hosted) await control('start');
  await delay(config.hosted ? 65_000 : 16_000); // Observe through the DO's 15 s watchdog window.
  assert.equal(wire.filter((event) => event.kind === 'hello').length, helloCount);
  evidence('retired', {row: workerRow(), ledger: retired, helloCount});
}

async function summaryChecks() {
  await launch(proxy.address().port);
  const linux = ['worker', 'scheduler'].map((name) => config.project + '-' + name + '-1');
  execFileSync('docker', ['pause', ...linux]);
  try {
    await control('start');
    await until('launcher ready', () => wire.some((event) => event.kind === 'ready'));
    assert.deepEqual(sql(`SELECT supported_handlers FROM execution_workers WHERE id='${config.incarnation}'`),
      ['workflow.execute', 'agent.summarize']);
    await delay(3500); // Close the paused Linux launchers' admission windows.
    fault = 'ack';
    const id = config.summary.run;
    const submission = await api('/api/agent-runs/' + id + '/regenerate-summary', {});
    assert.equal(submission.status, 200);
    const first = await until('summary ACK withheld', () => wire.find((event) => event.kind === 'ack' && event.dropped));
    for (const socket of sockets.clients) socket.close(1012, 'retry summary acknowledgement');
    await until('summary terminal acknowledged on reconnect', () => wire.some(
      (event) => event.kind === 'ack' && event.event === first.event && !event.dropped));
    const outcomes = wire.filter((event) => event.kind === 'summary_result');
    assert.equal(outcomes.length, 2);
    assert.ok(outcomes.every((event) => event.attempt === 'completed' && event.success && event.event === first.event));
    assert.equal(summaryCalls, 1);
    const row = sql(`SELECT json_build_object(
      'status',r.status,'summary',r.summary_status,'asked',r.asked,'did',r.did,
      'prompt',r.summary_prompt_version,'metadata',r.metadata,
      'worker',(SELECT worker_id FROM execution_worker_claims WHERE subject_id=r.id AND family='agent_summary'),
      'usage',(SELECT json_agg(json_build_array(input_tokens,output_tokens,cache_read_tokens,reasoning_tokens)) FROM ai_usage WHERE agent_run_id=r.id))
      FROM agent_runs r WHERE r.id='${id}'`);
    assert.deepEqual(row, {status: 'completed', summary: 'completed', asked: 'Summarize fixture',
      did: 'Collated the run', prompt: 'v4', metadata: {source: 'stub'}, worker: config.incarnation, usage: [[88, 11, null, null]]});
    evidence('summary-committed-once', {id, providerCalls: summaryCalls, replayedEvent: first.event, row});
  } finally {
    execFileSync('docker', ['unpause', ...linux]);
  }
}

await new Promise((resolve, reject) => {
  proxy.once('error', reject);
  proxy.listen(config.port, '127.0.0.1', resolve);
});
try {
  if (config.summary) await summaryChecks();
  else if (config.phase6) await phase6Checks();
  else await phase5Checks();
} finally {
  for (const gate of gates.values()) gate.release.resolve();
  await mf?.dispose();
  for (const socket of sockets.clients) socket.terminate();
  sockets.close();
  proxy.closeAllConnections();
  proxy.close();
  providerServer?.closeAllConnections();
  providerServer?.close();
}
