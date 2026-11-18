import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {readFileSync, utimesSync, writeFileSync} from 'node:fs';
import {mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {posix} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';
import {supervisorModules} from '../tenant-workers/supervisor-digest.mjs';
import {stageRuntimeLoader} from '../stage-runtime-loader.mjs';

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

function runRow(id) {
  return sql(`SELECT json_build_object('status',status,'result',result,'error',error_message,
    'completed',completed_at,'credential',credential_version,
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
  } else if (payload?.event === 'result') {
    event = {kind: 'result', generation: frame.session_generation, route: payload.route_id,
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
    if (config.hosted && (request.method !== 'POST' || ![sdkPath, '/auth/refresh'].includes(path))) {
      response.writeHead(404).end();
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
    const headers = new Headers(request.headers);
    headers.delete('host');
    const upstream = await fetch(config.origin + path, {
      method: request.method, headers, redirect: 'manual',
      ...(['GET', 'HEAD'].includes(request.method) ? {} : {body: request, duplex: 'half'}),
      signal: AbortSignal.timeout(10_000),
    });
    const body = Buffer.from(await upstream.arrayBuffer());
    if (config.hosted && path === sdkPath) {
      const token = headers.get('authorization')?.replace(/^Bearer /, '');
      const entry = [...tokens].find(([, value]) => value === token);
      if (entry) {
        const [id] = entry;
        const reads = hostedReads.get(id) ?? [];
        reads.push(upstream.status);
        hostedReads.set(id, reads);
        evidence('hosted-sdk-read', {id, status: upstream.status, count: reads.length});
        const gate = gates.get(id);
        if (gate && upstream.ok) {
          gate.entered = true;
          await gate.release.promise;
        }
      }
    }
    const headersOut = Object.fromEntries(upstream.headers);
    for (const key of ['connection', 'content-encoding', 'content-length', 'transfer-encoding']) delete headersOut[key];
    response.writeHead(upstream.status, headersOut);
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
        "SELECT key, data FROM ledger WHERE key IN ('control', 'slot') ORDER BY key, part").toArray();
      const values = {};
      for (const {key, data} of rows) {
        const value = (values[key] ??= {decoder: new TextDecoder(), text: ''});
        value.text += value.decoder.decode(data, {stream: true});
      }
      const read = (value) => value && JSON.parse(value.text + value.decoder.decode());
      const control = read(values.control);
      const slot = read(values.slot);
      return {control, slot: slot && {state: slot.state, route: slot.route.route_id,
        event: slot.terminal?._event_id, attempt: slot.terminal?.attempt_state}};
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
      const stub = ctx.exports.CloudflareLauncher.getByName(env.BIFROST_WORKER_INCARNATION_ID);
      switch (new URL(request.url).pathname) {
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
    API_ORIGIN: origin, BIFROST_CLOUDFLARE_LAUNCHER_ENABLED: 'true',
    BIFROST_WORKER_CONTROL_SECRET: config.controlKey, BIFROST_WORKER_INCARNATION_ID: config.incarnation,
    BIFROST_WORKER_LAUNCHER_ID: config.launcher, BIFROST_WORKER_RESOURCE_UID: config.resource,
    BIFROST_WORKER_VERSION: config.build, RUNTIME_SHA256: createHash('sha256').update(artifact).digest('hex'),
  }).map(([key, value]) => [key, {type: 'text', value}]));
  mf = new Miniflare({workers: [{
    dev: {outboundService: {type: 'worker', worker: 'relay'}},
    config: {name: 'launcher', compatibilityDate: '2026-10-30', exports,
      env: {...env, LOADER: {type: 'worker-loader'}},
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
  }]});
  worker = await mf.getWorker('launcher');
}

async function control(path, status = 204) {
  const response = await worker.fetch('https://harness.invalid/' + path);
  assert.equal(response.status, status, await response.clone().text());
  return status === 200 ? response.json() : null;
}

function reloadApi() {
  const file = new URL('../../../../api/src/config.py', import.meta.url);
  const now = new Date();
  utimesSync(file, now, now);
}

async function startRun(scenario, hold = true) {
  const gate = {release: Promise.withResolvers(), entered: false};
  gates.set(scenario, gate);
  if (config.hosted && hold) nextHostedGate = gate;
  const submitted = await api('/api/workflows/execute', {
    workflow_id: config.workflows[scenario === 'allowance' ? 'allowance' : config.hosted ? 'hosted' : 'local'],
    input_data: {case: scenario}, sync: false,
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

async function settle(scenario, run, status, ackCount = 1) {
  await until('terminal acknowledgement', () => {
    const ids = new Set(wire.filter((event) => event.kind === 'result' && event.route === run.route)
      .map((event) => event.event));
    return wire.filter((event) => event.kind === 'ack' && ids.has(event.event)).length >= ackCount;
  });
  if (!config.hosted) await until('launcher slot cleared', async () => !(await control('ledger', 200)).slot);
  const terminals = wire.filter((event) => event.kind === 'result' && event.route === run.route);
  assert.equal(terminals.length, ackCount);
  assert.equal(new Set(terminals.map((event) => event.event)).size, 1);
  const row = runRow(run.id);
  assert.equal(row.status, status);
  assert.deepEqual(row.claim, run.claim);
  assert.equal(row.queued, 0);
  if (status === 'Success') {
    assert.deepEqual(row.result, {case: scenario, table_count: 0});
    assert.equal(row.logs, 1);
  }
  assert.equal(wire.filter((event) => event.kind === 'workflow' && event.id === run.id).length, 1);
  evidence('settled', {scenario, id: run.id, row, ...(config.hosted ? {} : {ledger: await control('ledger', 200)})});
  return row;
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
  await settle('allowance', limits, 'Failed');
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

  const reset = await startRun('reset');
  const beforeReset = workerRow();
  evidence('redeploy-requested', {id: reset.id, route: reset.route, before: beforeReset});
  const inventory = await until('deployed launcher recovered the interrupted route', () => wire.find((event) =>
    event.kind === 'inventory' && event.generation > beforeReset.generation && event.terminal.length > 0));
  const resetRow = await settle('reset', reset, 'Failed');
  assert.match(resetRow.error, /may already have taken effect/);
  assert.ok(workerRow().hello > beforeReset.hello);
  const terminal = wire.find((event) => event.kind === 'result' && event.route === reset.route);
  assert.equal(terminal.attempt, 'uncertain');
  assert.equal(terminal.errorType, 'LauncherReset');
  assert.deepEqual(inventory.terminal, [terminal.event]);
  reset.gate.release.resolve();

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
  assert.equal((await control('ledger', 200)).slot.state, 'entering');
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

await new Promise((resolve) => proxy.listen(0, '127.0.0.1', resolve));
try {
  assert.equal(workerRow().state, 'starting');
  // Both debug launchers advertise workflow.execute on the Normal lane.
  const linuxLaunchers = ['worker', 'scheduler'].map((service) => config.project + '-' + service + '-1');
  execFileSync('docker', ['pause', ...linuxLaunchers]);
  try {
    // Claims live in the API. Wait until the last Linux samples have aged
    // past host.py's three-second admission window after pausing the sources.
    const linuxSamplesStale = delay(3100);
    if (config.hosted) {
      const {incarnation, launcher, resource, build, controlKey} = config;
      writeFileSync('.phase5b-deploy.json', JSON.stringify({
        port: proxy.address().port, incarnation, launcher, resource, build, controlKey,
      }), {flag: 'wx', mode: 0o600});
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
    if (config.hosted) await hostedChecks();
    else await localChecks();
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
} finally {
  for (const gate of gates.values()) gate.release.resolve();
  await mf?.dispose();
  for (const socket of sockets.clients) socket.terminate();
  sockets.close();
  proxy.closeAllConnections();
  proxy.close();
}
