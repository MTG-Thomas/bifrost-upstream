import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes, randomUUID} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {mkdtemp, rm} from 'node:fs/promises';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {buffer} from 'node:stream/consumers';
import {after, before, test} from 'node:test';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';
import {envelope, signHello} from '../cloudflare-launcher/protocol.mjs';
import {capacityWorker} from '../e2e/capacity-miniflare.mjs';

const {Miniflare} = createRequire(import.meta.resolve('wrangler'))('miniflare');
const {WebSocketServer} = createRequire(import.meta.url)('ws');
const settings = {token: 'local-conformance-token-' + 'a'.repeat(36), scope: 'workerd', version: 'test', maxWorkers: 1};
const auth = {Authorization: `Bearer ${settings.token}`, 'X-Bifrost-Capacity-Scope': settings.scope};
const root = new URL('../', import.meta.url);
const modules = Object.fromEntries([
  'cloudflare-launcher/launcher-do.mjs', 'cloudflare-launcher/launcher-core.mjs',
  'cloudflare-launcher/agent-loop.mjs', 'cloudflare-launcher/agent-provider.mjs',
  'cloudflare-launcher/protocol.mjs', 'tenant-workers/launcher-host.mjs',
  'tenant-workers/tenant-dispatch.mjs', 'tenant-workers/tenant-registry.mjs',
  'tenant-workers/storage-path.mjs',
  'executor/egress-hosts.mjs', 'executor/egress-psl.generated.mjs',
].map((path) => [path, {type: 'esm', contents: readFileSync(new URL(path, root), 'utf8')}]));
modules['entry.mjs'] = {type: 'esm', contents: `
  export {CloudflareLauncher} from './cloudflare-launcher/launcher-do.mjs';
  export default {fetch() { return new Response(null, {status: 404}); }};
`};
let mf;
let storage;
let base;
let provider;
const hellos = new Map();
const sockets = new Map();
const canaries = new Map();
const upgrades = new WebSocketServer({noServer: true});

function request(path, method = 'GET', body, headers = auth) {
  return fetch(base + '/v1/' + path, {method, body: body && JSON.stringify(body), headers});
}

async function next(predicate) {
  const deadline = Date.now() + 8000;
  do {
    const result = await predicate();
    if (result) return result;
    await delay(50);
  } while (Date.now() < deadline);
  throw new Error('Workerd did not reach the expected state');
}

const server = createServer(async (req, res) => {
  try {
    const canary = canaries.get(req.url);
    if (canary) {
      assert.equal(req.method, 'GET');
      assert.equal(req.headers.authorization, 'Bearer ' + 'a'.repeat(64));
      assert.equal(req.headers['x-bifrost-canary-generation'], '1');
      canary.requests += 1;
      res.writeHead(canary.status, canary.headers).end();
      return;
    }
    if (req.url === '/hook' && req.headers.authorization === auth.Authorization) {
      const {action, worker_id} = JSON.parse(await buffer(req));
      if (action === 'restart-provider') {
        await mf.dispose();
        await start();
      } else if (action === 'stop-process') {
        (await next(() => sockets.get(worker_id))).close(4001);
      } else if (action === 'observe') {
        const current = await request(`workers/${worker_id}`);
        const {state} = await current.json();
        if (['starting', 'running'].includes(state)) await next(() => hellos.has(worker_id));
      } else {
        res.writeHead(400).end();
        return;
      }
      res.end(JSON.stringify({launches: (hellos.get(worker_id) ?? [])
        .filter(({hello}) => hello.hello.hello_sequence === 1).map((_, i) => `${worker_id}:${i + 1}`)}));
      return;
    }
    const body = req.method === 'GET' ? undefined : await buffer(req);
    const response = await provider.fetch(base + req.url, {method: req.method, headers: req.headers, body});
    res.writeHead(response.status, Object.fromEntries(response.headers));
    res.end(Buffer.from(await response.arrayBuffer()));
  } catch {
    res.writeHead(502).end();
  }
});

server.on('upgrade', (req, socket, head) => {
  if (req.url !== '/api/worker-control') return socket.destroy();
  upgrades.handleUpgrade(req, socket, head, (ws) => {
    let record;
    ws.on('message', (raw) => {
      const message = JSON.parse(raw);
      if (record) {
        if (message.payload?.event === 'inventory') record.inventory = message;
        if (message.payload?.reply === 'canary-test') record.canary = message.payload;
        return;
      }
      const id = message.hello.incarnation_id;
      record = {hello: message};
      const records = hellos.get(id) ?? [];
      records.push(record);
      hellos.set(id, records);
      sockets.set(id, ws);
      ws.send(JSON.stringify({event: 'welcome', session_generation: records.length}));
    });
  });
});

async function start() {
  const relaySource = `export default {fetch(request) {
    if (new URL(request.url).origin !== 'https://test-api.invalid') return new Response(null, {status: 403});
    return fetch(new Request(${JSON.stringify(base)} + new URL(request.url).pathname, request));
  }};`;
  mf = new Miniflare({cf: false, resourcePersistencePath: storage, workers: [
    capacityWorker(settings),
    {
      dev: {outboundService: {type: 'worker', worker: 'relay'}},
      config: {name: 'launcher', compatibilityDate: '2026-10-30',
        exports: {CloudflareLauncher: {type: 'durable-object', storage: 'sqlite'}},
        env: {API_ORIGIN: {type: 'text', value: 'https://test-api.invalid'},
          BIFROST_CLOUDFLARE_MANAGED: {type: 'text', value: 'true'}},
        manifest: {mainModule: 'entry.mjs', modulesRoot: fileURLToPath(root), modules}},
    },
    {config: {name: 'relay', compatibilityDate: '2026-10-30', manifest: {
      mainModule: 'relay.mjs', modules: {'relay.mjs': {type: 'esm', contents: relaySource}},
    }}},
  ]});
  provider = await mf.getWorker('capacity');
}

before(async () => {
  storage = await mkdtemp(join(tmpdir(), 'bifrost-capacity-'));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  base = `http://127.0.0.1:${server.address().port}`;
  await start();
});

after(async () => {
  for (const ws of upgrades.clients) ws.terminate();
  await mf?.dispose();
  upgrades.close();
  await new Promise((resolve) => server.close(resolve));
  await rm(storage, {recursive: true, force: true});
});

test('prepare, start, signed hello, observe and delete use isolated workerd Workers', async () => {
  const id = randomUUID();
  const path = `workers/${id}`;
  const uid = randomBytes(16).toString('hex');
  const key = randomBytes(32).toString('hex');
  const reservation = {worker_id: id, worker_version: settings.version,
    memory_limit_bytes: 128 * 1024 ** 2, max_lifetime_seconds: 60, drain_grace_seconds: 5,
    resource_uid: uid, control_key: key};
  assert.equal((await request(path, 'PUT', reservation, {...auth, Authorization: `Bearer ${key}`})).status, 401);
  assert.equal((await request(path, 'PUT', reservation, {...auth, 'X-Bifrost-Capacity-Scope': 'foreign'})).status, 404);
  const prepared = await request(path, 'PUT', reservation);
  assert.equal(prepared.status, 200, await prepared.clone().text());
  const {resource_uid} = await prepared.json();
  assert.equal(resource_uid, uid);
  const rejected = await request(`workers/${randomUUID()}`, 'PUT', {
    ...reservation, worker_id: randomUUID(),
  });
  assert.equal(rejected.status, 400);
  const other = randomUUID();
  assert.equal((await request(`workers/${other}`, 'PUT', {...reservation, worker_id: other})).status, 409);
  assert.equal(hellos.has(id), false);
  const started = await request(path + '/start', 'POST', {resource_uid});
  assert.equal(started.status, 200, await started.clone().text());
  const seen = await next(() => hellos.get(id)?.find((record) => record.inventory));
  const {proof, ...identity} = seen.hello.hello;
  assert.equal((await signHello(key, identity)).proof, proof);
  assert.notEqual((await signHello(settings.token, identity)).proof, proof);
  assert.equal(identity.launcher_id, id);
  assert.equal(identity.runtime_profile, 'cloudflare-python-v1');
  assert.equal(seen.inventory.payload.event, 'inventory');
  assert.equal((await (await request(path)).json()).state, 'running');
  assert.equal((await (await request(path, 'DELETE', {resource_uid})).json()).state, 'deleted');
  await next(() => sockets.get(id).readyState === 3);
  assert.equal((await request(path + '/start', 'POST', {resource_uid})).status, 409);
  assert.equal(hellos.get(id).length, 1);
});

for (const {name, status, headers, proof, expected} of [
  {name: 'accepts authenticated 204', status: 204, headers: {'x-bifrost-worker-ingress': 'v1; allowed'}, expected: {}},
  {name: 'rejects ingress 404', status: 404, expected: {stage: 'sdk_response', error_class: 'http_status', status: 404}},
  {name: 'reports proof refusal', status: 401, expected: {stage: 'sdk_response', error_class: 'http_status', status: 401}},
  {name: 'does not follow redirects', status: 302, headers: {Location: '/unexpected'},
    expected: {stage: 'sdk_response', error_class: 'http_status', status: 302}},
  {name: 'rejects a response without its ingress marker', status: 204,
    expected: {stage: 'sdk_response', error_class: 'missing_ingress_marker', status: 204}},
  {name: 'refuses a malformed proof before fetch', status: 204, proof: 'invalid',
    expected: {stage: 'proof', error_class: 'invalid_command'}},
]) {
  test(`the workerd SDK canary ${name}`, async () => {
    const worker_id = randomUUID();
    const resource_uid = randomBytes(16).toString('hex');
    const path = `workers/${worker_id}`;
    const trace = {status, headers, requests: 0};
    canaries.set(`/api/sdk/capacity-canary/${worker_id}`, trace);
    try {
      assert.equal((await request(path, 'PUT', {
        worker_id, worker_version: settings.version, memory_limit_bytes: 128 * 1024 ** 2,
        max_lifetime_seconds: 60, drain_grace_seconds: 5, resource_uid, control_key: randomBytes(32).toString('hex'),
      })).status, 200);
      assert.equal((await request(path + '/start', 'POST', {resource_uid})).status, 200);
      const record = await next(() => hellos.get(worker_id)?.find(item => item.inventory));
      sockets.get(worker_id).send(JSON.stringify(envelope(1, 1, 'command', {
        id: 'canary-test', action: 'capacity_canary', payload: {proof: proof ?? 'a'.repeat(64)},
      })));
      const reply = await next(() => record.canary);
      assert.deepEqual(reply.result, Object.keys(expected).length ? {failure: expected} : {});
      assert.equal(trace.requests, proof ? 0 : 1);
    } finally {
      canaries.delete(`/api/sdk/capacity-canary/${worker_id}`);
      await request(path, 'DELETE', {resource_uid});
    }
  });
}

// Conformance ends by simulating process loss for a reserved worker. Keep
// that reservation out of the canary cases, which share a one-worker scope.
test('shared capacity conformance exercises the real local HTTP provider', async () => {
  const suite = fileURLToPath(new URL('../../../../deploy/capacity/conformance/conformance.py', import.meta.url));
  const hook = fileURLToPath(new URL('../e2e/workers-local.py', import.meta.url));
  const python = process.platform === 'win32' ? 'py' : 'python3';
  const flags = process.platform === 'win32' ? ['-3'] : [];
  const args = [...flags, suite, '--base-url', base, '--scope', settings.scope,
    '--managed-cloudflare',
    '--hook-command', [python, ...flags, `"${hook}"`].join(' ')];
  const child = spawn(python, args, {env: {...process.env, CAPACITY_TOKEN: settings.token, CAPACITY_HOOK_URL: base + '/hook'}});
  const [out, err, code] = await Promise.all([
    buffer(child.stdout), buffer(child.stderr),
    new Promise((resolve, reject) => { child.once('error', reject); child.once('close', resolve); }),
  ]);
  assert.equal(code, 0, out.toString() + err.toString());
  assert.match(out.toString(), /17\/17 passed, 0 failed, 0 skipped/);
});
