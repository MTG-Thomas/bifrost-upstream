import assert from 'node:assert/strict';
import {execFileSync, spawn, spawnSync} from 'node:child_process';
import {createHash, randomBytes} from 'node:crypto';
import {appendFileSync, existsSync, readFileSync, rmSync, unlinkSync, utimesSync, writeFileSync} from 'node:fs';
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
import {expiryCapacityRelay, localLinuxProvider, localSolutionGit, OVERFLOW_MEMORY_MIB, serveContainersProviders,
  vmHoldMemoryMiB} from './containers-local.mjs';

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
const agentCalls = new Map();
const agentAborts = new Set();

function evidence(label, data) {
  console.log(JSON.stringify({label, ...(config.qualification ? {at: new Date().toISOString()} : {}), ...data}));
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

function managedRows(scope = 'phase6-local', {includeRetired = false} = {}) {
  return sql(`SELECT coalesce(json_agg(json_build_object('id',id,'state',state,'uid',capacity_resource_uid,
    'launcher_bound',remote_launcher_id=id,'profile',runtime_profile,'hello',remote_hello_sequence,
    'budget',memory_limit_bytes,'available',memory_budget_bytes,'active',active_runs,
    'deleted',capacity_resource_deleted_at IS NOT NULL,'end',end_reason,
    'created_at',created_at,'ready_at',ready_at,'ended_at',ended_at,
    'deleted_at',capacity_resource_deleted_at)), '[]'::json)
    FROM execution_workers WHERE capacity_scope = '${scope}'${includeRetired ? '' : " AND state <> 'retired'"}`);
}

function runRow(id) {
  return sql(`SELECT json_build_object('status',status,'result',result,'error',error_message,
    'created',created_at,'started',started_at,'completed',completed_at,
    'credential',credential_version,'tenant',organization_id,
    'claim',(SELECT json_agg(json_build_object('id',id,'worker',worker_id))
      FROM execution_worker_claims WHERE subject_id=e.id),
    'queued',(SELECT count(*) FROM work_dispatches WHERE subject_id=e.id),
    'logs',(SELECT count(*) FROM execution_logs WHERE execution_id=e.id
      AND message LIKE '%committed fixture%')) FROM executions e WHERE id='${id}'`);
}

async function api(path, body, token = config.admin, method = body === undefined ? 'GET' : 'POST') {
  const response = await fetch(config.origin + path, {
    method,
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
  } else if (frame.kind === 'command' && payload.action === 'cloudflare_agent') {
    event = {kind: 'cloudflare_agent', route: payload.payload.route_id};
  } else if (frame.kind === 'command' && payload.action === 'cloudflare_agent_reply' &&
      payload.payload.result?.execution_id) {
    event = {kind: 'agent-tool-ack', route: payload.payload.route_id,
      execution: payload.payload.result.execution_id};
  } else if (config.chat && frame.kind === 'command' && payload.action === 'cloudflare_agent_reply' &&
      Number.isInteger(payload.payload.result?.sequence)) {
    event = {kind: 'agent-chunk-ack', route: payload.payload.route_id,
      request: payload.payload.request_id, sequence: payload.payload.result.sequence};
  } else if (frame.kind === 'command' && payload.action === 'workflow') {
    const command = payload.payload;
    tokens.set(command.execution_id, command.context.engine_token);
    if (nextHostedGate && command.context.cloudflare_workspace) {
      gates.set(command.execution_id, nextHostedGate);
      nextHostedGate = undefined;
    }
    event = {kind: 'workflow', id: command.execution_id, route: command.route_id,
      generation: frame.session_generation};
  } else if (frame.kind === 'command' && ['terminate', 'ready', 'bootstrap'].includes(payload.action)) {
    event = {kind: payload.action, route: payload.payload.route_id, generation: frame.session_generation};
  } else if (payload?.event === 'inventory') {
    event = {kind: 'inventory', generation: frame.session_generation,
      active: payload.inventory.routes.map((route) => route.route_id),
      terminal: payload.inventory.terminal_routes.map((route) => route.event_id)};
  } else if (config.agent && payload?.event === 'heartbeat') {
    event = {kind: 'agent-launcher-heartbeat', generation: frame.session_generation,
      occupied: payload.slot_occupied};
  } else if (['result', 'summary_result', 'cloudflare_agent_result'].includes(payload?.event)) {
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
    if (config.hosted && !hostedRelayPath(request.method, path, !!config.qualification)) {
      response.writeHead(404).end();
      return;
    }
    if (config.summary && path === '/__fixture/v1/chat/completions' &&
        (!config.agent || request.headers.authorization === 'Bearer ' + config.summary.databaseKey)) {
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
    if (config.agent && path === '/__fixture/v1/chat/completions') {
      assert.ok(request.headers.authorization === 'Bearer ' + config.agent.databaseKey);
      const body = JSON.parse((await buffer(request)).toString());
      assert.equal(body.model, 'gpt-4o-mini');
      assert.equal(body.stream, true);
      const {scenario} = JSON.parse(body.messages.findLast(({role}) => role === 'user').content);
      assert.ok(['text', 'tool', 'cancel', 'reset', 'chat-text', 'chat-next', 'chat-cancel', 'chat-reset'].includes(scenario));
      if (scenario === 'chat-next') {
        assert.deepEqual(body.messages.slice(1, -1).map(({role, content}) => [role, content]),
          [['user', '{"scenario":"chat-prior"}'], ['assistant', 'fixture chat-prior'],
            ['user', '{"scenario":"chat-text"}'], ['assistant', 'fixture chat-text']]);
      }
      if (scenario === 'chat-text') assert.deepEqual(body.messages.slice(1, -1).map(({role, content}) => [role, content]),
        [['user', '{"scenario":"chat-prior"}'], ['assistant', 'fixture chat-prior']]);
      if (scenario.startsWith('chat-')) assert.equal(body.tools, undefined);
      agentCalls.set(scenario, (agentCalls.get(scenario) ?? 0) + 1);
      response.writeHead(200, {'Content-Type': 'text/event-stream'});
      const sse = (value) => response.write('data: ' + JSON.stringify(value) + '\n\n');
      if (scenario === 'tool' && body.messages.length === 2) {
        const tool = body.tools[0].function.name;
        sse({choices: [{delta: {tool_calls: [{index: 0, id: 'fixture-call',
          function: {name: tool, arguments: '{}'}}]}, finish_reason: 'tool_calls'}]});
      } else {
        if (scenario === 'tool') assert.match(body.messages.at(-1).content, /lookup result/);
        const parts = scenario === 'chat-text' ? ['fixture ', scenario] : ['fixture ' + scenario];
        for (const content of parts) sse({choices: [{delta: {content}, finish_reason: null}]});
        if (['cancel', 'reset', 'chat-cancel', 'chat-reset'].includes(scenario)) {
          response.once('close', () => agentAborts.add(scenario));
          return;
        }
        sse({choices: [{delta: {}, finish_reason: 'stop'}]});
      }
      sse({choices: [], usage: {prompt_tokens: 9, completion_tokens: 3}});
      response.end('data: [DONE]\n\n');
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
        ...(config.agent ? {AGENT_PROVIDER: {type: 'worker', worker: 'agent', exportName: 'AgentProvider'}} : {}),
        ...(config.summary ? {SUMMARY_PROVIDER: {type: 'worker', worker: 'summary', exportName: 'SummaryProvider'}} : {})},
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
    config: {name: 'summary', exports: {SummaryProvider: {type: 'worker'}}, compatibilityDate: '2026-10-30',
      env: {BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT: {type: 'text', value: config.summary.endpoint}},
      manifest: {mainModule: 'summary-worker/entry.mjs', modules: {
        'summary-worker/entry.mjs': module(readFileSync(new URL('cloudflare-launcher/summary-worker/entry.mjs', root), 'utf8')),
        'summary-provider.mjs': module(readFileSync(new URL('cloudflare-launcher/summary-provider.mjs', root), 'utf8')),
      }},
    },
  }] : []), ...(config.agent ? [{
    dev: {outboundService: {type: 'worker', worker: 'relay'}},
    config: {name: 'agent', exports: {AgentProvider: {type: 'worker'}}, compatibilityDate: '2026-10-30',
      env: {BIFROST_CLOUDFLARE_AGENT_ENDPOINT: {type: 'text', value: config.agent.endpoint}},
      manifest: {mainModule: 'agent-worker/entry.mjs', modules: {
        'agent-worker/entry.mjs': module(readFileSync(new URL('cloudflare-launcher/agent-worker/entry.mjs', root), 'utf8')),
        'agent-provider.mjs': module(readFileSync(new URL('cloudflare-launcher/agent-provider.mjs', root), 'utf8')),
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
  if (config.qualification) evidence('submitted', {scenario, id});
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
  assert.equal(row.credential, 2);
  if (config.qualification?.placement || config.qualification?.workersOnly) {
    const workers = managedRows();
    assert.equal(workers.length, 1, 'one managed Workers incarnation for the placement trial');
    assert.equal(workers[0].state, 'ready');
    // Hosted placement first asks for burst capacity when a workflow queues.
    if (config.qualification.placement) config.incarnation ??= workers[0].id;
    assert.equal(workers[0].id, config.incarnation);
  }
  assert.equal(row.claim[0].worker, config.incarnation);
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
        assert.equal(row.launcher_bound, true);
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
    await launch(proxy.address().port);
    await startCapacityEndpoint();
    await until('API selected the capacity provider', () => config.phase6.discovered);
    // A paused launcher stops claiming before its reported capacity expires.
    // Wait for the API's 30-second liveness fence and reconciliation sweep.
    await until('paused Linux launchers leave placement', () => sql(`
      SELECT to_json(NOT EXISTS (
        SELECT FROM execution_workers WHERE form = 'static'
          AND runtime_profile = 'linux-process-v1' AND state IN ('starting', 'ready', 'draining')
      ))
    `), 45_000);
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

async function containersChecks() {
  const startedAt = new Date().toISOString();
  const launchers = ['worker', 'scheduler'].map((service) => config.project + '-' + service + '-1');
  const inspect = (format) => execFileSync('docker', ['inspect', '--format', format, launchers[0]], {encoding: 'utf8'}).trim();
  const vmEnv = Object.fromEntries(JSON.parse(inspect('{{json .Config.Env}}'))
    .filter((item) => /^BIFROST_(SECRET_KEY|DATABASE_URL(?:_SYNC)?|REDIS_URL|S3_[A-Z_]+)=/.test(item))
    .map((item) => [item.slice(0, item.indexOf('=')), item.slice(item.indexOf('=') + 1)]));
  const common = {
    project: config.project, image: inspect('{{.Config.Image}}'),
    version: devVersion(), archive: process.cwd(),
  };
  const hosted = config.qualification?.placement;
  const expiry = hosted?.containerExpiry;
  const expiryRelay = expiry ? expiryCapacityRelay(hosted.containersUrl) : null;
  if (!hosted) common.memoryMiB = config.phase6.workerMemoryMiB;
  const vmToken = randomBytes(32).toString('hex');
  const containersToken = hosted?.containersToken ??
    createHash('sha256').update(config.phase6.token + ':containers').digest('hex');
  const vm = localLinuxProvider({...common, scope: 'vm', token: vmToken, vmEnv});
  const containers = hosted ? null : localLinuxProvider({...common, scope: 'containers',
    token: containersToken, gitFixture: true});
  const git = hosted ? null : localSolutionGit();
  const privateToken = randomBytes(24).toString('hex');
  const privateAuth = 'Basic ' + Buffer.from('x-access-token:' + privateToken).toString('base64');
  let privateFetches = 0;
  let vmProfileRead = false;
  execFileSync('docker', ['pause', ...launchers]);
  try {
    if (git) {
      execFileSync('docker', ['exec', config.project + '-api-1', 'python', '-m',
        'scripts.containers_private_fixture', 'retire-paused-workers'], {timeout: 15_000});
    }
    if (!hosted) await launch(proxy.address().port);
    if (hosted) evidence('hosted-relay-ready', {port: proxy.address().port});
    providerServer = await serveContainersProviders(config.phase6.providerHost, {
      ...(expiryRelay ? {containers: expiryRelay} : {}),
      vm: {
        async fetch(request) {
          const response = await vm.fetch(request);
          if (response.ok && request.method === 'GET' && new URL(request.url).pathname === '/v1/capabilities') {
            vmProfileRead = true;
          }
          return response;
        },
      },
      ...(hosted ? {} : {containers,
      git: {
        fetch(request) {
          assert.equal(request.headers.has('authorization'), false);
          return git.fetch(request);
        },
      },
      'private-git': {
        fetch(request) {
          if (request.headers.get('authorization') !== privateAuth) return new Response(null, {status: 401});
          if (request.method === 'POST') privateFetches += 1;
          return git.fetch(request);
        },
      },
      workers: {
        async fetch(request) {
          const match = /^\/v1\/workers\/([a-f0-9-]+)\/start$/.exec(new URL(request.url).pathname);
          if (match && request.method === 'POST') {
            const row = managedRows().find(({id}) => id === match[1]);
            assert.equal(row?.state, 'starting');
            assert.equal(row.uid, (await request.clone().json()).resource_uid);
            config.incarnation = row.id;
          }
          return provider.fetch(request);
        },
      }}),
    });
    const endpoint = `https://${config.phase6.providerHost}:${providerServer.address().port}`;
    if (git) {
      writeFileSync('api/scripts/.containers-private.json', JSON.stringify({
        token: privateToken, url: `${endpoint}/private-git/solution.git`,
      }), {mode: 0o644});
    }
    appendFileSync('.phase5-api.env', [
      '',
      `BIFROST_WORKER_CAPACITY_URL=${endpoint}/vm`,
      `BIFROST_WORKER_CAPACITY_TOKEN=${vmToken}`,
      `BIFROST_CLOUDFLARE_CAPACITY_URL=${hosted?.workersUrl ?? endpoint + '/workers'}`,
      `BIFROST_CLOUDFLARE_CAPACITY_TOKEN=${config.phase6.token}`,
      `BIFROST_CLOUDFLARE_CONTAINERS_URL=${expiry ? endpoint + '/containers' : hosted?.containersUrl ?? endpoint + '/containers'}`,
      'BIFROST_CLOUDFLARE_CONTAINERS_SCOPE=containers',
      `BIFROST_CLOUDFLARE_CONTAINERS_TOKEN=${containersToken}`,
      '',
    ].join('\n'));
    const content = '--no-index\npackaging\n';
    execFileSync('docker', ['exec', config.project + '-redis-1', 'redis-cli', 'SET', 'bifrost:requirements:content',
      JSON.stringify({content, hash: createHash('sha256').update(content).digest('hex')})]);
    reloadApi();
    if (hosted) {
      await until('reloaded API discovers the VM capacity provider', () => vmProfileRead);
      const response = await api('/api/admin/kubernetes/policy', hosted.policy, config.admin, 'PUT');
      assert.equal(response.status, 200);
    }
    await until('paused debug launchers leave placement', () => sql(`
      SELECT to_json(NOT EXISTS (
        SELECT FROM execution_workers WHERE form = 'static'
          AND runtime_profile = 'linux-process-v1' AND state IN ('starting', 'ready', 'draining')
      ))
    `), 60_000);
    const primary = await until('VM primary ready with measured budget', () =>
      managedRows('vm').find((row) => row.state === 'ready' && row.available > 256 * 2 ** 20), 90_000);
    const hold = await api('/api/workflows/' + config.workflows.vm,
      {memory_request_mib: vmHoldMemoryMiB(primary.available)}, config.admin, 'PATCH');
    assert.equal(hold.status, 200);
    if (expiry) {
      const overflow = await api('/api/workflows/' + config.workflows.linux,
        {memory_request_mib: OVERFLOW_MEMORY_MIB}, config.admin, 'PATCH');
      assert.equal(overflow.status, 200);
    }
    async function submit(key) {
      const reply = await api('/api/workflows/execute', {
        workflow_id: config.workflows[key], input_data: {}, sync: false,
      });
      assert.equal(reply.status, 200);
      if (hosted) evidence('submitted', {scenario: key, id: reply.data.execution_id});
      return reply.data.execution_id;
    }
    const vmRun = await submit('vm');
    await until('VM primary claims held work', () => runRow(vmRun)?.claim?.[0].worker === primary.id);
    if (git) {
      const solution = await api('/api/solutions', {slug: 'container-update', name: 'Container Update'});
      assert.ok([200, 201].includes(solution.status));
      const solutionId = solution.data.id;
      assert.match(solutionId, /^[0-9a-f-]{36}$/);
      const privateSolution = await api('/api/solutions', {slug: 'container-update-private', name: 'Private Container Update'});
      assert.ok([200, 201].includes(privateSolution.status));
      const privateId = privateSolution.data.id;
      assert.match(privateId, /^[0-9a-f-]{36}$/);
      execFileSync('docker', ['exec', config.project + '-api-1', 'python', '-m',
        'scripts.containers_private_fixture', privateId], {timeout: 15_000});
      sql(`UPDATE solutions SET version = '1.0.0', git_connected = true,
      git_repo_url = '${endpoint}/git/solution.git', git_ref = 'update', repo_subpath = 'solutions/demo'
      WHERE id = '${solutionId}'`);
      const jobId = execFileSync('docker', ['exec', config.project + '-api-1', 'python', '-c', `
import asyncio
from src.core.database import init_db
from src.jobs.platform.system_maintenance import enqueue_automatic_solution_update_check

async def run():
    await init_db()
    print((await enqueue_automatic_solution_update_check()).platform_job_id)

asyncio.run(run())
`], {encoding: 'utf8', timeout: 15_000}).trim().split('\n').at(-1);
      assert.match(jobId, /^[0-9a-f-]{36}$/);
      const update = await until('Container Git update check settles', () => {
        const row = sql(`SELECT json_build_object('status',j.status,'result',j.result,'error',j.error_code,
        'stage',j.remote_stage,'update_available',s.update_available_version,'scope',w.capacity_scope,
        'private_version',(SELECT update_available_version FROM solutions WHERE id = '${privateId}'),
        'private_events',(SELECT count(*) FROM events WHERE event_type = 'solution.update_available'
          AND data->'solution'->>'id' = '${privateId}' AND data->'solution'->>'available_version' = '2.0.0'),
        'events',(SELECT count(*) FROM events WHERE event_type = 'solution.update_available'
          AND data->'solution'->>'id' = s.id::text AND data->'solution'->>'available_version' = '2.0.0'))
        FROM platform_jobs j CROSS JOIN solutions s
        LEFT JOIN execution_workers w ON w.id = (j.remote_stage->>'worker_id')::uuid
        WHERE j.id = '${jobId}' AND s.id = '${solutionId}'`);
        return ['failed', 'succeeded'].includes(row?.status) && row;
      }, 90_000);
      assert.equal(update.status, 'succeeded', JSON.stringify(update));
      assert.equal(update.scope, 'containers', JSON.stringify(update));
      assert.equal(update.stage.capability, 'platform.solution_update_check.v1');
      assert.equal(update.update_available, '2.0.0');
      assert.equal(update.result.updates_found, 2);
      assert.equal(update.result.check_failures, 0);
      assert.equal(update.events, 1);
      assert.equal(update.private_version, '2.0.0');
      assert.equal(update.private_events, 1);
      assert.ok(privateFetches > 0 && git.fetches > privateFetches);
      evidence('containers-platform-stage', {
        jobId, solutionId, privateId, ...update, publicFetches: git.fetches - privateFetches, privateFetches,
      });
    }
    const hostedWorkflows = hosted && !expiry && (async () => {
      let loose;
      let solution;
      try {
        loose = await startRun('phase6-a', true, config.phase6.tenants[0]);
        solution = await startRun('phase6-b', true, config.phase6.tenants[1]);
        assert.deepEqual(solution.claim.map(({worker}) => worker), loose.claim.map(({worker}) => worker));
        const worker = managedRows().find(({id}) => id === config.incarnation);
        assert.equal(worker?.budget, 256 * 2 ** 20);
        evidence('hosted-two-slot-overlap', {worker, loose: loose.id, solution: solution.id});
      } finally {
        loose?.gate.release.resolve();
        solution?.gate.release.resolve();
      }
      await settle('phase6-b', solution, 'Success');
      return settle('phase6-a', loose, 'Success');
    })().then((row) => ({row}), (error) => ({error}));
    const approved = hosted ? null : await startRun('phase6-a', true, config.phase6.tenants[0]);
    const linux = await submit('linux');
    const linuxRow = await until('Linux-only workflow settles', () => {
      const result = runRow(linux);
      return result?.completed && result;
    }, 90_000);
    const container = managedRows('containers').find(({id}) => id === linuxRow.claim?.[0].worker);
    assert.equal(linuxRow.status, 'Success', JSON.stringify(linuxRow));
    assert.deepEqual(linuxRow.result, {stdout: 'containers', proc_environment_denied: true});
    assert.equal(linuxRow.claim.length, 1);
    assert.equal(container?.profile, 'linux-process-v1');
    assert.ok(container.hello > 0);
    const launcherId = sql(`SELECT to_json(remote_launcher_id) FROM execution_workers WHERE id = '${container.id}'`);
    assert.ok(launcherId && launcherId !== container.id);
    for (const name of [config.project + '-api-1',
      ...(hosted ? [] : [config.project + '-containers-' + container.id])]) {
      const read = spawnSync('docker', ['logs', '--since', startedAt, name], {
        encoding: 'utf8', timeout: 10_000, maxBuffer: 32 * 1024 ** 2,
      });
      assert.equal(read.status, 0, 'could not inspect qualification logs');
      assert.ok(!(read.stdout + read.stderr).includes(launcherId), 'physical launcher ID leaked into logs');
    }
    const workerStatus = await until('Container requirements recorded in worker heartbeat', () => JSON.parse(execFileSync(
      'docker', ['exec', config.project + '-redis-1', 'redis-cli', '--raw', 'GET',
        'bifrost:pool:' + container.id + ':heartbeat'], {encoding: 'utf8'},
    ).trim() || 'null'));
    assert.equal(Number(workerStatus.requirements_installed), 1);
    assert.equal(Number(workerStatus.requirements_total), 1);
    evidence('containers-workflow', {primary, vmRun, linux, linuxRow, container, requirements: 1});
    if (expiry) {
      const receipt = '.phase7-expiry-observed';
      rmSync(receipt, {force: true});
      expiryRelay.stopForwarding();
      assert.equal(expiryRelay.deletesForwarded, 0);
      assert.equal(container.state, 'ready');
      assert.equal(container.deleted, false);
      assert.ok(!runRow(vmRun).completed, 'VM primary remained occupied through overflow settlement');
      evidence('container-expiry-wait', {id: linux, incarnation: container.id, deletesForwarded: expiryRelay.deletesForwarded});
      await until('independent Cloudflare instance stop receipt', () => existsSync(receipt), 420_000);
      const incarnation = await until('Bifrost records Container retirement', () => {
        const current = managedRows('containers', {includeRetired: true}).find(({id}) => id === container.id);
        return current?.state === 'retired' && current;
      });
      const row = runRow(linux);
      assert.equal(incarnation.end, 'stopped');
      assert.equal(incarnation.deleted, false);
      assert.equal(row?.status, 'Success');
      assert.deepEqual(row.claim, linuxRow.claim);
      assert.equal(expiryRelay.deletesForwarded, 0);
      evidence('container-expiry-bifrost', {id: linux, row, incarnation,
        deletesForwarded: expiryRelay.deletesForwarded, deletesBlocked: expiryRelay.deletesBlocked});
      return;
    }
    approved?.gate.release.resolve();
    const localOutcome = hosted ? null : await settle('phase6-a', approved, 'Success');
    const released = await until('Containers idle retirement and provider-confirmed deletion', () =>
      managedRows('containers', {includeRetired: true}).find((row) => row.id === container.id && row.state === 'retired'
        && row.deleted && (hosted || !containers.running(row.id))), 90_000);
    evidence('containers-release', {row: released});
    const workflowOutcome = hosted ? await hostedWorkflows : {row: localOutcome};
    if (workflowOutcome.error) throw workflowOutcome.error;
    evidence('three-scope-placement', {primary, vmRun, approved: workflowOutcome.row.claim[0], linux, container});
  } finally {
    execFileSync('docker', ['unpause', ...launchers]);
    containers?.close();
    vm.close();
    git?.close();
  }
}

function devVersion() {
  return execFileSync('docker', ['exec', config.project + '-api-1', 'printenv', 'BIFROST_VERSION'],
    {encoding: 'utf8', timeout: 5000}).trim();
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
  if (config.qualification?.workersOnly) assert.equal(cpuTerminal.errorType, 'CpuLimitExceeded');
  assert.deepEqual(hostedReads.get(cpu.id), [200]);
  evidence('cpu', {id: cpu.id, budgetMs: 1000, terminal: cpuTerminal});
}

async function hostedWorkersQualification() {
  const linux = ['worker', 'scheduler'].map((name) => config.project + '-' + name + '-1');
  execFileSync('docker', ['pause', ...linux]);
  try {
    appendFileSync('.phase5-api.env', [
      '', `BIFROST_WORKER_CAPACITY_URL=${config.qualification.workersOnly}`,
      `BIFROST_WORKER_CAPACITY_TOKEN=${config.phase6.token}`, '',
    ].join('\n'));
    evidence('hosted-relay-ready', {port: proxy.address().port});
    reloadApi();
    const launcher = await until('managed Workers ready and static workers fenced', () => {
      if (sql(`SELECT to_json(EXISTS (SELECT FROM execution_workers
        WHERE form='static' AND state IN ('starting','ready','draining')))`)) return null;
      return managedRows().find(({state}) => state === 'ready');
    }, 90_000);
    config.incarnation = launcher.id;
    assert.equal(launcher.budget, 256 * 2 ** 20);
    evidence('hosted-managed-ready', {launcher});
    let loose;
    let solution;
    try {
      loose = await startRun('phase6-a', true, config.phase6.tenants[0]);
      solution = await startRun('phase6-b', true, config.phase6.tenants[1]);
      assert.equal(runRow(loose.id).status, 'Running');
      assert.equal(runRow(solution.id).status, 'Running');
      evidence('hosted-two-slot-overlap', {worker: launcher.id, loose: loose.id, solution: solution.id});
    } finally {
      gates.get('phase6-a')?.release.resolve();
      gates.get('phase6-b')?.release.resolve();
    }
    await settle('phase6-a', loose, 'Success');
    await settle('phase6-b', solution, 'Success');
    await hostedCpuCheck();
    await hostedResetCheck();
  } finally {
    execFileSync('docker', ['unpause', ...linux]);
  }
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
      did: 'Collated the run', prompt: 'v4', metadata: {source: 'stub'}, worker: config.incarnation, usage: [[88, 11, 0, 0]]});
    evidence('summary-committed-once', {id, providerCalls: summaryCalls, replayedEvent: first.event, row});
  } finally {
    execFileSync('docker', ['unpause', ...linux]);
  }
}

function agentRow(id) {
  return sql(`SELECT json_build_object(
    'status',r.status,'output',r.output,'error',r.error,'iterations',r.iterations_used,
    'tokens',r.tokens_used,'model',r.llm_model,'runUser',r.run_user_id,
    'worker',(SELECT worker_id FROM execution_worker_claims WHERE subject_id=r.id AND family='agent_run'),
    'steps',(SELECT json_agg(type ORDER BY step_number) FROM agent_run_steps WHERE run_id=r.id),
    'usage',(SELECT count(*) FROM ai_usage WHERE agent_run_id=r.id),
    'journal',(SELECT data - 'messages' FROM cloudflare_agent_attempts WHERE run_id=r.id)
  ) FROM agent_runs r WHERE id='${id}'`);
}

async function agentChecks() {
  await launch(proxy.address().port);
  // Preserve Linux workflow placement. Suppress races for agent/summary
  // dispatches only in this disposable stack's seeded Linux pool rows.
  sql(`UPDATE execution_workers SET supported_handlers=supported_handlers
    - 'agent.run' - 'agent.summarize' - 'agent.summarize_backfill'
    WHERE runtime_profile='linux-process-v1'`);
  const chats = config.chat ? await seedChatHistory() : null;
  await control('start');
  await until('agent launcher ready', () => wire.some((event) => event.kind === 'ready'));
  await until('agent launcher reports slot liveness', () => wire.some((event) =>
    event.kind === 'agent-launcher-heartbeat' && event.occupied === false));
  assert.deepEqual(sql(`SELECT supported_handlers FROM execution_workers WHERE id='${config.incarnation}'`),
    ['workflow.execute', 'agent.run']);
  const completed = [];
  for (const scenario of ['text', 'tool', 'cancel', 'reset']) {
    await until('free agent slot', async () => {
      const ledger = await control('ledger', 200);
      return !ledger.slots?.length;
    });
    if (scenario === 'text') fault = 'ack';
    if (scenario === 'tool') fault = 'agent-tool-ack';
    const queued = await api('/api/agent-runs/enqueue', {
      agent_name: config.agent.names[scenario === 'tool' ? 1 : 0], input: {scenario},
    });
    assert.equal(queued.status, 202);
    const id = queued.data.run_id;
    assert.match(id, /^[0-9a-f-]{36}$/);
    if (scenario === 'cancel' || scenario === 'reset') {
      await until('durable partial agent text', () => agentRow(id)?.journal?.text === 'fixture ' + scenario);
      if (scenario === 'cancel') {
        assert.equal((await api('/api/agent-runs/' + id + '/cancel', {})).status, 200);
        await until('provider abort', () => agentAborts.has(scenario));
      } else {
        await control('reset');
      }
    }
    const terminal = await until('agent terminal', () => {
      const row = agentRow(id);
      return row?.journal?.settled && row;
    });
    const expected = scenario === 'cancel' ? 'cancelled' : scenario === 'reset' ? 'failed' : 'completed';
    assert.equal(terminal.status, expected);
    assert.equal(terminal.worker, config.incarnation);
    assert.deepEqual(terminal.output, {text: 'fixture ' + scenario});
    assert.equal(terminal.journal.attempt_state, expected === 'completed' ? 'completed' : 'uncertain');
    const turns = scenario === 'tool' ? 2 : 1;
    assert.equal(agentCalls.get(scenario), turns);
    assert.equal(terminal.iterations, turns);
    assert.equal(terminal.tokens, expected === 'completed' ? turns * 12 : 0);
    assert.equal(terminal.usage, expected === 'completed' ? turns : 0);
    if (scenario === 'text') {
      const dropped = await until('terminal ACK loss', () => wire.find((row) => row.kind === 'ack' && row.dropped));
      await until('terminal ACK replay', () => wire.some((row) => row.event === dropped.event && row.kind === 'ack' && !row.dropped));
      assert.deepEqual(agentRow(id).steps, ['llm_request', 'llm_response']);
      assert.equal(agentRow(id).usage, 1);
    }
    if (scenario === 'tool') {
      const replies = wire.filter((row) => row.kind === 'agent-tool-ack');
      assert.equal(replies.length, 2);
      assert.equal(replies[0].dropped, true);
      assert.equal(replies[0].execution, replies[1].execution);
      assert.deepEqual(terminal.steps, ['llm_request', 'llm_response', 'tool_call', 'tool_result', 'llm_request', 'llm_response']);
      const execution = sql(`SELECT json_build_object('count',count(*),'status',min(status::text),
        'actor',min(run_user_id::text)) FROM executions WHERE workflow_id='${config.agent.workflow}'`);
      assert.deepEqual(execution, {count: 1, status: 'Success', actor: terminal.runUser});
    }
    evidence('autonomous-agent', {scenario, id, status: terminal.status, providerCalls: turns,
      usage: terminal.usage, steps: terminal.steps, attemptState: terminal.journal.attempt_state});
    if (expected === 'completed') completed.push(id);
  }
  await runLinuxFixture('agent-parity.py', completed);
  if (chats) await chatChecks(chats);
}

async function subscribeChat(conversation) {
  const socket = new WebSocket(config.origin.replace(/^http/, 'ws') + '/ws/connect', {
    headers: {Authorization: 'Bearer ' + config.admin},
  });
  const channel = 'chat:' + conversation;
  const events = [];
  let subscribed = false;
  let failure;
  socket.on('open', () => socket.send(JSON.stringify({type: 'subscribe', channels: [channel]})));
  socket.on('error', (error) => { failure = error; });
  socket.on('message', (raw) => {
    const message = JSON.parse(raw);
    if (message.type === 'subscribed' && message.channel === channel) subscribed = true;
    if (message.type === 'chat_run_event' && message.conversation_id === conversation) events.push(message);
  });
  try {
    await until('authorized chat subscription', () => {
      if (failure) throw failure;
      return subscribed;
    });
    return {socket, events};
  } catch (error) {
    socket.terminate();
    throw error;
  }
}

function chatRecords(id) {
  return sql(`SELECT json_build_object(
    'status',r.status,'output',r.output,'tokens',r.tokens_used,'iterations',r.iterations_used,'model',r.llm_model,
    'messages',(SELECT json_agg(json_build_array(sequence,role,content,token_count_input,token_count_output,model)
      ORDER BY sequence) FROM messages WHERE conversation_id=r.conversation_id),
    'usage',(SELECT json_agg(json_build_array(provider,model,input_tokens,output_tokens,
      cache_read_tokens,cache_write_tokens,reasoning_tokens) ORDER BY id)
      FROM ai_usage WHERE conversation_id=r.conversation_id),
    'steps',(SELECT count(*) FROM agent_run_steps WHERE run_id=r.id)
  ) FROM agent_runs r WHERE r.id='${id}'`);
}

async function chatConversation(agent) {
  const response = await api('/api/chat/conversations', {agent_id: agent, title: 'Chat fixture'});
  assert.equal(response.status, 201);
  return response.data.id;
}

async function submitChat(conversation, scenario) {
  const response = await api('/api/chat/runs', {
    conversation_id: conversation, content: JSON.stringify({scenario}),
  });
  assert.equal(response.status, 201);
  assert.match(response.data.run_id, /^[0-9a-f-]{36}$/);
  return response.data.run_id;
}

const chatSignature = (events) => events.map(({sequence, kind, status, payload}) => [
  sequence, kind, status, payload.content ?? null, payload.run_status ?? null,
  payload.finish_reason ?? null, payload.incomplete ?? null,
]);

function runLinuxFixture(script, ids) {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', ['exec', '-i', config.project + '-api-1', 'python', '-', ...ids], {
      stdio: ['pipe', 'inherit', 'inherit'],
      signal: AbortSignal.timeout(30_000),
    });
    child.once('error', reject);
    child.stdin.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`${script} exited with ${code ?? signal}`));
    });
    child.stdin.end(readFileSync(new URL(script, import.meta.url)));
  });
}

async function seedChatHistory() {
  const workersConversation = await chatConversation(config.agent.ids[0]);
  const linuxAgent = sql("SELECT to_json(id) FROM agents WHERE name='linux-chat-parity'");
  const linuxConversation = await chatConversation(linuxAgent);
  for (const conversation of [workersConversation, linuxConversation]) {
    const id = await submitChat(conversation, 'chat-prior');
    await runLinuxFixture('./chat-parity.py', [id]);
    assert.equal(chatRecords(id).status, 'completed');
    for (const suffix of ['events', 'sequence']) {
      const result = execFileSync('docker', ['exec', config.project + '-redis-1', 'redis-cli',
        'EXPIRE', `bifrost:chat:${conversation}:${suffix}`, '0'], {encoding: 'utf8', timeout: 5000});
      assert.equal(result.trim(), '1');
    }
  }
  return {workersConversation, linuxConversation};
}

async function chatChecks({workersConversation, linuxConversation}) {
  const {socket, events} = await subscribeChat(workersConversation);
  const linux = await subscribeChat(linuxConversation);
  try {
    fault = 'agent-chunk-ack';
    const workersId = await submitChat(workersConversation, 'chat-text');
    const dropped = await until('Workers chunk ACK withheld', () => wire.find((row) =>
      row.kind === 'agent-chunk-ack' && row.route.includes(workersId) && row.dropped));
    await until('committed chunk visible before replay', () => events.some(({kind}) => kind === 'delta'));
    for (const controlSocket of sockets.clients) controlSocket.close(1012, 'retry chat chunk ACK');
    await until('identical chat chunk ACK replay', () => wire.some((row) =>
      row.request === dropped.request && row.sequence === dropped.sequence && !row.dropped));
    await until('streamed chat done', () => events.some(({kind}) => kind === 'done'));
    const workersRow = await until('chat persisted', () => {
      const row = chatRecords(workersId);
      return row?.status === 'completed' && row;
    });
    const expected = events.filter(({run_id}) => run_id === workersId);
    assert.equal(new Set(expected.map(({event_id}) => event_id)).size, expected.length);
    const replay = await api('/api/chat/conversations/' + workersConversation + '/state');
    assert.deepEqual(replay.data.events.map(({event_id}) => event_id), expected.map(({event_id}) => event_id));
    assert.equal(agentCalls.get('chat-text'), 1);
    assert.equal(agentRow(workersId).worker, config.incarnation);

    const linuxId = await submitChat(linuxConversation, 'chat-text');
    await runLinuxFixture('./chat-parity.py', [linuxId]);
    await until('Linux chat done on the same subscription contract', () => linux.events.some(({kind}) => kind === 'done'));
    assert.deepEqual(chatRecords(linuxId), workersRow);
    assert.deepEqual(chatSignature(expected), chatSignature(linux.events));
    evidence('chat-linux-parity', {workersId, linuxId, events: chatSignature(expected), records: workersRow});

    const next = await submitChat(workersConversation, 'chat-next');
    await until('second Workers chat used conversation history', () =>
      events.some(({run_id, kind}) => run_id === next && kind === 'done'));
    for (const scenario of ['chat-cancel', 'chat-reset']) {
      const id = await submitChat(workersConversation, scenario);
      await until('live partial chat delta', () =>
        events.some(({run_id, kind}) => run_id === id && kind === 'delta'));
      if (scenario === 'chat-cancel') {
        assert.equal((await api('/api/chat/runs/' + id + '/cancel', {})).status, 200);
        await until('chat provider aborted', () => agentAborts.has(scenario));
      } else {
        await control('reset');
      }
      const cancelled = scenario === 'chat-cancel';
      await until('terminal chat event', () => events.some(({run_id, kind}) =>
        run_id === id && kind === (cancelled ? 'cancelled' : 'error')));
      const record = chatRecords(id);
      assert.equal(record.status, cancelled ? 'cancelled' : 'failed');
      assert.deepEqual(record.output, {text: 'fixture ' + scenario, partial: true});
      assert.equal(record.messages.at(-1)[2], 'fixture ' + scenario);
      assert.equal(agentRow(id).journal.attempt_state, 'uncertain');
      assert.equal(agentCalls.get(scenario), 1);
      evidence('chat-interrupted', {scenario, id, status: record.status,
        events: chatSignature(events.filter(({run_id}) => run_id === id))});
    }
    assert.deepEqual(events.map(({sequence}) => sequence), events.map((_, index) => index + 1));
  } finally {
    socket.terminate();
    linux.socket.terminate();
  }
}

async function hostedAgentAndRuntimeChecks() {
  const {incarnation} = config;
  evidence('hosted-relay-ready', {port: proxy.address().port});
  await until('hosted agent launcher ready', () => workerRow().state === 'ready', 180_000);
  const staticHandlers = sql(`
    WITH original AS (
      SELECT id, supported_handlers FROM execution_workers
      WHERE form='static' AND state='ready' AND runtime_profile='linux-process-v1' FOR UPDATE
    ), suppressed AS (
      UPDATE execution_workers w SET supported_handlers=w.supported_handlers
        - 'workflow.execute' - 'agent.run' - 'agent.summarize'
      FROM original WHERE w.id=original.id RETURNING w.id
    )
    SELECT coalesce(json_agg(original), '[]'::json) FROM original JOIN suppressed USING (id)`);
  try {
    const handlers = sql(`SELECT supported_handlers FROM execution_workers WHERE id='${incarnation}'`);
    assert.deepEqual(new Set(handlers), new Set(['workflow.execute', 'agent.run', 'agent.summarize']));

    const id = config.summary.run;
    assert.equal((await api('/api/agent-runs/' + id + '/regenerate-summary', {})).status, 200);
    const summary = await until('hosted summary settlement', () => sql(`
      SELECT json_build_object('asked',r.asked,'did',r.did,'worker',c.worker_id,
        'claims',(SELECT count(*) FROM execution_worker_claims WHERE subject_id=r.id AND family='agent_summary'),
        'usage',(SELECT json_agg(json_build_array(input_tokens,output_tokens)) FROM ai_usage WHERE agent_run_id=r.id))
      FROM agent_runs r JOIN execution_worker_claims c ON c.subject_id=r.id AND c.family='agent_summary'
      WHERE r.id='${id}' AND r.summary_status='completed'`));
    assert.deepEqual(summary, {asked: 'Summarize fixture', did: 'Collated the run',
      worker: incarnation, claims: 1, usage: [[88, 11]]});
    assert.equal(summaryCalls, 1);
    evidence('hosted-summary', {id, summary, providerCalls: summaryCalls});

    const agent = await api('/api/agent-runs/enqueue', {agent_name: config.agent.names[0], input: {scenario: 'text'}});
    assert.equal(agent.status, 202);
    const runId = agent.data.run_id;
    assert.match(runId, /^[0-9a-f-]{36}$/);
    const row = await until('hosted autonomous agent settlement', () => {
      const result = agentRow(runId);
      return result?.journal?.settled && result;
    });
    assert.equal(row.status, 'completed');
    assert.equal(row.worker, incarnation);
    assert.deepEqual(row.output, {text: 'fixture text'});
    assert.deepEqual(row.steps, ['llm_request', 'llm_response']);
    assert.equal(row.usage, 1);
    assert.equal(agentCalls.get('text'), 1);
    evidence('hosted-autonomous-agent', {id: runId, worker: row.worker, status: row.status,
      output: row.output, steps: row.steps, usage: row.usage, providerCalls: agentCalls.get('text')});

    await hostedCpuCheck();
    await hostedResetCheck();
    assert.equal(sql(`UPDATE execution_workers SET state='retired' WHERE id='${incarnation}'
      RETURNING to_json(state)`), 'retired');
    for (const socket of sockets.clients) socket.close(1012, 'qualification retirement');
    await until('retired hosted launcher fenced', () => wire.some((event) => event.kind === 'api-close' && event.code === 4001));
    evidence('retired', {row: workerRow()});
  } finally {
    sql(`UPDATE execution_workers w SET supported_handlers=original.supported_handlers
      FROM jsonb_to_recordset($fixtures$${JSON.stringify(staticHandlers)}$fixtures$::jsonb)
        AS original(id uuid, supported_handlers jsonb)
      WHERE w.id=original.id`);
  }
}

async function withHostedEnrollment() {
  const {incarnation, launcher, resource, build, controlKey} = config;
  const enrollment = '.phase7-enrollment.json';
  writeFileSync(enrollment, JSON.stringify({incarnation, launcher, resource, build, controlKey}),
    {flag: 'wx', mode: 0o600});
  try {
    await hostedAgentAndRuntimeChecks();
  } finally {
    unlinkSync(enrollment);
  }
}

await new Promise((resolve, reject) => {
  proxy.once('error', reject);
  proxy.listen(config.port, '127.0.0.1', resolve);
});
try {
  if (config.qualification?.workersOnly) await hostedWorkersQualification();
  else if (config.qualification?.placement) await containersChecks();
  else if (config.qualification) await withHostedEnrollment();
  else if (config.agent) await agentChecks();
  else if (config.summary) await summaryChecks();
  else if (config.phase6?.containers) await containersChecks();
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
