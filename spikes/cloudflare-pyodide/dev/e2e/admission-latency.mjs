// Run with Node 26. Arguments: baseline spike directory, generated runtime,
// optional preparation delay in ms (default 1000), optional pairs (default 8).
import assert from 'node:assert/strict';
import {createHash, randomUUID} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {resolve} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {envelope, parseEnvelope, signHello} from '../cloudflare-launcher/protocol.mjs';
import {stageRuntimeLoader} from '../stage-runtime-loader.mjs';
import {supervisorModules} from '../tenant-workers/supervisor-digest.mjs';

const require = createRequire(new URL('../tenant-workers/package.json', import.meta.url));
const {Miniflare} = createRequire(require.resolve('wrangler'))('miniflare');
const {WebSocketServer} = require('ws');
const [base, runtime, prepArg = '1000', pairsArg = '8'] = process.argv.slice(2);
const preparationMs = Number(prepArg);
const pairs = Number(pairsArg);
assert.ok(base && runtime && Number.isInteger(pairs) && pairs >= 8 && preparationMs >= 0 && preparationMs <= 10_000);
const artifact = readFileSync(runtime);
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
const runtimeDigest = sha256(artifact);
const origin = 'https://admission-fixture.invalid';
const secret = 'a'.repeat(64);
const filename = 'workflows/measurement.py';
const source = `from bifrost import tables, workflow

@workflow(name="measure")
async def measure():
    return {"count": len(await tables.list())}
`;
const esm = (contents) => ({type: 'esm', contents});
const http = createServer((request, response) => {
  if (request.url === '/api/sdk/tables/list') {
    assert.equal(request.headers.authorization, 'Bearer fixture');
    response.writeHead(200, {'Content-Type': 'application/json'}).end('[]');
  } else {
    response.writeHead(request.url === '/health/ready' ? 200 : 204).end();
  }
});
const sockets = new WebSocketServer({server: http, path: '/api/worker-control'});
const controls = new Map();
sockets.on('connection', (socket, request) => {
  const accept = controls.get(request.headers['x-trial']);
  assert.ok(accept);
  accept(socket);
});
await new Promise((ready) => http.listen(0, '127.0.0.1', ready));
const port = http.address().port;
const workers = [];
const peers = [];

async function arm(name, root, candidate) {
  const provisioned = Promise.withResolvers();
  controls.set(name, provisioned.resolve);
  const incarnation = randomUUID();
  const staged = new URL('dev/runtime/', root);
  await mkdir(staged, {recursive: true});
  await stageRuntimeLoader(staged);
  const sources = supervisorModules(fileURLToPath(new URL('dev/tenant-workers/launcher-entry.mjs', root)));
  const modules = Object.fromEntries(sources.map(([name, code]) => ['dev/' + name, esm(code)]));
  modules['dev/runtime/generated-runtime.mjs'] = esm(artifact.toString('utf8'));
  modules['dev/measurement.mjs'] = esm(`
    import {TenantHost as Host} from './tenant-workers/tenant-api.mjs';
    import launcher from './tenant-workers/launcher-entry.mjs';
    export {CloudflareLauncher, TenantApi} from './tenant-workers/launcher-entry.mjs';
    export class TenantHost extends Host {
      async run(input) {
        const result = await super.run(input);
        if (result.output) result.output.measured_cold = result.invocation.cold;
        return result;
      }
    }
    export default {async fetch(_request, env, ctx) {
      await launcher.scheduled({}, env, ctx);
      return new Response(null, {status: 204});
    }};
  `);
  const env = Object.fromEntries(Object.entries({
    API_ORIGIN: origin, RUNTIME_SHA256: runtimeDigest,
    BIFROST_CLOUDFLARE_LAUNCHER_ENABLED: 'true', BIFROST_WORKER_CONTROL_SECRET: secret,
    BIFROST_WORKER_INCARNATION_ID: incarnation, BIFROST_WORKER_LAUNCHER_ID: incarnation,
    BIFROST_WORKER_RESOURCE_UID: name, BIFROST_WORKER_VERSION: name,
    BIFROST_METER_ENGINE_CPU_MS: '30000', BIFROST_METER_SDK_REQUESTS: '40',
    BIFROST_METER_BOOT_CPU_MS: '30000', BIFROST_METER_WARM_CPU_MS: '30000', BIFROST_METER_EXERCISE_CPU_MS: '30000',
    BIFROST_METER_GENERATIONS: 'true', BIFROST_METER_RECEIPTS: 'true',
    ...(candidate ? {BIFROST_TENANT_LINGER_SECONDS: '90'} : {}),
  }).map(([key, value]) => [key, {type: 'text', value}]));
  const mf = new Miniflare({cf: false, workers: [
    {dev: {outboundService: {type: 'worker', worker: 'relay'}}, config: {
      name: 'launcher', compatibilityDate: '2026-10-30',
      exports: {CloudflareLauncher: {type: 'durable-object', storage: 'sqlite'},
        TenantHost: {type: 'durable-object', storage: 'sqlite'}},
      env: {...env, LOADER: {type: 'worker-loader'}},
      manifest: {mainModule: 'dev/measurement.mjs', modulesRoot: fileURLToPath(root), modules},
    }},
    {config: {name: 'relay', compatibilityDate: '2026-10-30',
      manifest: {mainModule: 'relay.mjs', modules: {'relay.mjs': esm(`
        export default {fetch(request) {
          const url = new URL(request.url);
          if (url.origin !== ${JSON.stringify(origin)}) return new Response(null, {status: 403});
          const headers = new Headers(request.headers);
          headers.set('X-Trial', ${JSON.stringify(name)});
          return fetch(new Request('http://127.0.0.1:${port}' + url.pathname, request), {headers});
        }};
      `)}}}},
  ]});
  workers.push(mf);
  const connected = Promise.withResolvers();
  let sequence = 0;
  let generationRequests = 0;
  let result;
  let socket;
  const send = (kind, payload) => socket.send(JSON.stringify(envelope(1, ++sequence, kind, payload)));
  const command = (action, payload) => send('command', {id: randomUUID(), action, payload});
  provisioned.promise.then((wire) => {
    socket = wire;
    peers.push(wire);
    wire.once('message', async (bytes) => {
      const {hello} = JSON.parse(bytes);
      const {proof, ...unsigned} = hello;
      assert.equal(hello.incarnation_id, incarnation);
      assert.equal(proof, (await signHello(secret, unsigned)).proof);
      wire.on('message', (frame) => {
        const payload = parseEnvelope(frame.toString()).payload;
        if (payload.error) result?.reject(new Error(JSON.stringify(payload)));
        if (payload.event === 'inventory') {
          assert.equal(Boolean(payload.inventory.admission_prepare), candidate);
          command('ready', {});
          connected.resolve();
        } else if (payload.event === 'cloudflare_generation_request') {
          generationRequests++;
          command('cloudflare_generation_reply', {
            request_id: payload.request_id, route_id: payload.route_id, ok: true, result: {},
          });
        } else if (payload.event === 'result') {
          send('ack', {event_id: payload._event_id});
          result.resolve(payload);
        }
      });
      wire.send(JSON.stringify({event: 'welcome', session_generation: 1}));
    });
  });
  const worker = await mf.getWorker('launcher');
  await worker.fetch('https://launcher.invalid/start');
  await connected.promise;
  return async (orgId, burst = false) => {
    const started = performance.now();
    const entriesBefore = generationRequests;
    const executionId = randomUUID();
    if (candidate) command('prepare_workflow', {
      route_id: 'prepare:' + randomUUID(), execution_id: executionId, org_id: orgId,
    });
    await delay(preparationMs);
    const startedAt = new Date();
    const deadline = new Date(startedAt.getTime() + 30_000).toISOString();
    result = Promise.withResolvers();
    command('workflow', {
      route_id: `workflow:${executionId}:${randomUUID()}`, execution_id: executionId,
      started_at: startedAt.toISOString(), timeout_seconds: 30, workflow_deadline: deadline,
      active_execution: {execution_id: executionId, org_id: orgId, sync: true}, memory_request_bytes: 8192,
      context: {
        workflow_deadline: deadline, file_path: filename, function_name: 'measure', name: 'measure',
        content_hash: sha256(source), parameters: {}, engine_token: 'fixture', organization_id: orgId,
        transient: true, no_cache: true, caller: {user_id: orgId, email: 'fixture@example.invalid', name: 'Fixture'},
        cloudflare_workspace: {sources: {[filename]: source}, manifest: {[filename]: sha256(source)},
          packages: [], https_hosts: [], cpu_budget_ms: 30_000, subrequest_allowance: 12},
      },
    });
    const terminal = await result.promise;
    const elapsed = Math.round(performance.now() - started);
    assert.equal(terminal.attempt_state, 'completed', JSON.stringify(terminal));
    assert.equal(terminal.result.success, true, JSON.stringify(terminal));
    assert.deepEqual(terminal.result.result, {count: 0});
    assert.equal(terminal.result.measured_cold, false);
    assert.equal(generationRequests - entriesBefore, burst ? 0 : 1);
    return elapsed;
  };
}

try {
  const baseline = await arm('baseline', pathToFileURL(resolve(base) + '/'), false);
  const candidate = await arm('candidate', new URL('../../', import.meta.url), true);
  for (let pair = 0; pair < pairs; pair++) {
    for (const variant of pair % 2 ? ['candidate', 'baseline'] : ['baseline', 'candidate']) {
      const orgId = randomUUID();
      const run = variant === 'baseline' ? baseline : candidate;
      const coldMs = await run(orgId);
      const burstMs = variant === 'candidate' ? await run(orgId, true) : undefined;
      console.log(JSON.stringify({variant, pair, coldMs, burstMs, preparationMs, runtime: runtimeDigest}));
    }
  }
} finally {
  for (const peer of peers) peer.terminate();
  await Promise.all(workers.map(mf => mf.dispose()));
  sockets.close();
  http.close();
}
