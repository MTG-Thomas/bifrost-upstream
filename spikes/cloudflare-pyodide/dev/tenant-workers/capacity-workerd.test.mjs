import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createHash, randomBytes, randomUUID} from 'node:crypto';
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
import {envelope, fragment, FragmentAssembler, signHello} from '../cloudflare-launcher/protocol.mjs';
import {capacityWorker} from '../e2e/capacity-miniflare.mjs';
import {runLifetimeSoak} from './lifetime-soak.mjs';
import {isolateSampler, pythonMemoryProbe} from './lifetime-memory.mjs';

const {Miniflare} = createRequire(import.meta.resolve('wrangler'))('miniflare');
const {WebSocketServer} = createRequire(import.meta.url)('ws');
const settings = {token: 'local-conformance-token-' + 'a'.repeat(36), scope: 'workerd', version: 'test', maxWorkers: 1};
const auth = {Authorization: `Bearer ${settings.token}`, 'X-Bifrost-Capacity-Scope': settings.scope};
const root = new URL('../', import.meta.url);
const ingressPolicy = JSON.parse(readFileSync(new URL('../../../../api/src/services/worker_ingress_routes.json', import.meta.url), 'utf8'));
const modules = Object.fromEntries([
  'cloudflare-launcher/launcher-do.mjs', 'cloudflare-launcher/launcher-core.mjs',
  'cloudflare-launcher/launcher-meter.mjs',
  'cloudflare-launcher/agent-loop.mjs', 'cloudflare-launcher/agent-provider.mjs',
  'cloudflare-launcher/protocol.mjs', 'tenant-workers/launcher-host.mjs',
  'tenant-workers/tenant-dispatch.mjs', 'tenant-workers/tenant-registry.mjs',
  'tenant-workers/storage-path.mjs',
  'executor/egress-hosts.mjs', 'executor/egress-psl.generated.mjs',
].map((path) => [path, {type: 'esm', contents: readFileSync(new URL(path, root), 'utf8')}]));
modules['entry.mjs'] = {type: 'esm', contents: `
  import {DurableObject} from 'cloudflare:workers';
  import {launcherMeter, SQLITE_HEADROOM} from './cloudflare-launcher/launcher-meter.mjs';
  import {CloudflareLauncher as Launcher} from './cloudflare-launcher/launcher-do.mjs';
  export class CloudflareLauncher extends Launcher {
    async witnessIdleStorage() {
      await this.alarm();
      return this.ctx.storage.sql.exec(
        "SELECT name FROM sqlite_master WHERE name IN ('ledger', 'meter')",
      ).toArray();
    }
    async clearReleasedStorage(uid) {
      await super.clearReleasedStorage(uid);
      const rows = this.ctx.storage.sql.exec("SELECT name FROM sqlite_master WHERE name = 'ledger'").toArray();
      if (rows.length) throw new Error('Launcher SQLite ledger survived release');
    }
  }
  export class MeterProbe extends DurableObject {
    async witnessFenceSize() {
      const {storage} = this.ctx;
      await storage.put('row', 'x'.repeat(100_000));
      await storage.sync();
      const before = storage.sql.databaseSize;
      await storage.put('row', {state: 'deleted', resource_uid: 'fixture'});
      await storage.sync();
      return {before, after: storage.sql.databaseSize};
    }
    witness(name, count) {
      const meter = launcherMeter(this.ctx.storage.sql,
        JSON.stringify({sqlite_rows_written: SQLITE_HEADROOM + 20, do_requests: 10, log_events: 2}));
      const accepted = name === 'log_events' ? meter.log() && meter.log() : meter.charge({[name]: count});
      const rejected = name === 'log_events' ? meter.log() : meter.charge({[name]: 1});
      const exhausted = Boolean(meter.exhausted);
      meter.charge({[name]: name === 'log_events' ? 0 : 400}, true);
      return JSON.stringify({accepted, rejected, exhausted, sealed: meter.seal()});
    }
  }
  export default {fetch() { return new Response(null, {status: 404}); }};
`};
let mf;
let storage;
let base;
let provider;
let apiSdkCalls = 0;
let capacityCalls = 0;
const hellos = new Map();
const sockets = new Map();
const canaries = new Map();
let plannedReconnect;
const upgrades = new WebSocketServer({noServer: true});
const measuredBundle = process.env.BIFROST_QUALIFIED_CAPACITY_BUNDLE;
const measuredLauncher = process.env.BIFROST_QUALIFIED_LAUNCHER_BUNDLE;
const measuredGateway = process.env.BIFROST_QUALIFIED_GATEWAY_BUNDLE;
const slotQualification = process.env.BIFROST_LOCAL_TENANT_SLOTS === 'true';
const lifetimeSoak = process.env.BIFROST_LIFETIME_SOAK_RESULT;
const soakSeconds = Number(process.env.BIFROST_LIFETIME_SOAK_SECONDS ?? 900);
const memoryProbe = process.env.BIFROST_LIFETIME_MEMORY_PROBE === 'true';
const meterBounds = JSON.parse(readFileSync(new URL(
  '../../../../api/src/services/worker_pool/cloudflare_meter_bounds.json', import.meta.url,
), 'utf8'));
const lifetimeRates = Object.fromEntries(['do_requests', 'log_events', 'sqlite_rows_written'].map(
  name => [name, Number(meterBounds.per_launcher_second[name])],
));
const launcherSlots = slotQualification ? 4 : measuredLauncher ? 2 : 1;
const controllerPolls = Number(process.env.BIFROST_QUALIFIED_CONTROLLER_POLLS ?? 0);
const controllerSource = new URL('../../../../api/src/services/pool_cadence.py', import.meta.url);
const reconcileSeconds = Number(/^POOL_RECONCILE_SPACING_SECONDS = (\d+(?:\.\d+)?)$/m.exec(
  readFileSync(controllerSource, 'utf8').replaceAll('\r\n', '\n'))[1]);

const sha256 = input => createHash('sha256').update(input).digest('hex');
const heldSlots = new Map();

async function referenceAttempt(record, solution, slotCase) {
  const beforeSdk = apiSdkCalls;
  const id = randomUUID();
  const now = Date.now();
  const workflow_deadline = new Date(now + 60_000).toISOString();
  const file_path = 'workflows/reference.py';
  const function_name = 'phase5a_fixture';
  const caseName = slotCase?.name ?? (solution ? 'solution' : 'loose');
  let source = readFileSync(new URL('../e2e/fixture.py', import.meta.url), 'utf8');
  if (slotCase) {
    source = source.replace('rows = await tables.list()', 'rows = await tables.list(app=case)')
      .replace('from js import fetch', 'from js import fetch, globalThis\n    globalThis.slotFixture = case')
      .replace('"table_count": len(rows)', '"table_count": len(rows), "isolated": globalThis.slotFixture == case');
  }
  if (slotCase?.soak) {
    source = source.replace('import logging', 'import logging\nimport sys')
      .replace('"table_count": len(rows)', '"table_count": len(rows), "module_count": len(sys.modules)');
  }
  const sources = {[file_path]: source};
  if (solution) {
    source = source.replace('import logging', 'import logging\nfrom phase6_support.values import INSTALL_VALUE')
      .replace('"table_count": len(rows)', '"table_count": len(rows), "install_value": INSTALL_VALUE');
    Object.assign(sources, {[file_path]: source, 'phase6_support/values.py': 'INSTALL_VALUE = "solution"\n'});
  }
  if (slotCase?.soak && memoryProbe) {
    const probe = pythonMemoryProbe({collect: process.env.BIFROST_LIFETIME_MEMORY_COLLECT === 'true'});
    sources[file_path] = source = source.replace('    return {', `${probe}    return {"memory": memory, `);
  }
  const org = slotCase?.org ?? randomUUID();
  const solution_id = solution ? randomUUID() : null;
  const fence = randomUUID();
  const route_id = `workflow:${id}:${fence}`;
  slotCase?.routeReady?.resolve(route_id);
  const command = randomUUID();
  const result = Promise.withResolvers();
  record.terminals.set(route_id, result);
  record.replies.set(command, result);
  const engineSlot = slotCase?.ordinal ?? 0;
  if (slotCase) record.send('command', {id: randomUUID(), action: 'prepare_workflow', payload: {
    route_id: `prepare:${fence}`, execution_id: id, org_id: org, engine_slot: engineSlot,
  }});
  record.send('command', {id: command, action: 'workflow', payload: {
    route_id, execution_id: id, started_at: new Date(now).toISOString(), workflow_deadline,
    engine_slot: engineSlot,
    timeout_seconds: 60, memory_request_bytes: 64 * 1024 ** 2,
    active_execution: {execution_id: id, sync: true, org_id: org},
    context: {workflow_deadline, file_path, function_name, name: function_name, content_hash: sha256(source),
      parameters: {case: caseName}, engine_token: slotCase ? `fixture-access-${caseName}` : 'fixture-only-access',
      transient: true, no_cache: true,
      organization_id: org, organization: null, solution_id,
      caller: {user_id: org, email: 'fixture@example.invalid', name: 'Qualifying fixture'},
      cloudflare_workspace: {manifest: Object.fromEntries(Object.entries(sources).map(([file, value]) =>
        [file, sha256(value)])), sources, packages: [], https_hosts: ['phase5-fixture.invalid'],
        cpu_budget_ms: 1_000, subrequest_allowance: 12, solution_id}},
  }});
  let terminal;
  try {
    terminal = await result.promise;
  } catch (error) {
    if (!slotCase?.soak) throw error;
    const {error_type} = JSON.parse(error.message);
    if (error_type !== 'CloudflareLauncherMeterLimitExceeded') throw error;
    return {state: 'not_started', success: false, error_type};
  } finally {
    record.replies.delete(command);
    record.terminals.delete(route_id);
  }
  if (slotCase?.soak) {
    const value = terminal.result?.result;
    let timing;
    if (terminal.result?.success === true) {
      const {module_count, memory, ...output} = value;
      assert.deepEqual(output, {
        case: caseName, table_count: 0, isolated: true,
        ...(solution ? {install_value: 'solution'} : {}),
      });
      assert.ok(Number.isSafeInteger(module_count) && module_count > 0);
      const {engine_ready_at, entry_at} = terminal.timings ?? {};
      assert.ok(Number.isSafeInteger(engine_ready_at) && engine_ready_at > 0);
      assert.ok(Number.isSafeInteger(entry_at) && entry_at >= Math.max(now, engine_ready_at));
      timing = {engine_ready_to_entry_ms: entry_at - engine_ready_at, offer_to_entry_ms: entry_at - now};
    }
    return {state: terminal.attempt_state, success: terminal.result?.success === true,
      error_type: terminal.result?.error_type ?? null, route_id,
      module_count: value?.module_count, ...(timing ? {timing} : {}), ...(memoryProbe ? {memory: value?.memory} : {})};
  }
  if (slotCase?.cancel) return terminal;
  assert.equal(terminal.attempt_state, 'completed', JSON.stringify(terminal));
  assert.equal(terminal.result.success, true, JSON.stringify(terminal));
  assert.deepEqual(terminal.result.result, {case: caseName, table_count: 0,
    ...(solution ? {install_value: 'solution'} : {}), ...(slotCase ? {isolated: true} : {})});
  return {id, route_id, sdk_requests: apiSdkCalls - beforeSdk, usage: terminal.usage};
}

async function observeAtControllerCadence(path) {
  const script = fileURLToPath(new URL('../e2e/controller-cadence.py', import.meta.url));
  const observer = spawn('python3', [script, `${base}/v1/${path}`, String(controllerPolls)],
    {stdio: ['pipe', 'pipe', 'inherit']});
  const completion = new Promise((resolve, reject) => {
    observer.once('error', reject);
    observer.once('close', code => code === 0 ? resolve() : reject(new Error(`Cadence observer exited ${code}`)));
    observer.stdin.on('error', reject);
  });
  observer.stdin.end(JSON.stringify(auth));
  const [receipt] = await Promise.all([buffer(observer.stdout), completion]);
  const cadence = JSON.parse(receipt.toString('utf8'));
  console.info('controller-cadence', JSON.stringify(cadence));
  return cadence;
}

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
    const requestedAt = Date.now();
    const canary = canaries.get(req.url);
    if (canary) {
      assert.equal(req.method, 'GET');
      assert.equal(req.headers.authorization, 'Bearer ' + 'a'.repeat(64));
      assert.equal(req.headers['x-bifrost-canary-generation'], '1');
      canary.requests += 1;
      res.writeHead(canary.status, canary.headers).end();
      return;
    }
    if (measuredLauncher && req.url === '/api/sdk/tables/list' && req.method === 'POST') {
      if (slotQualification) {
        const {app} = JSON.parse(await buffer(req));
        assert.ok(heldSlots.has(app));
        assert.equal(req.headers.authorization, `Bearer fixture-access-${app}`);
        heldSlots.get(app).sdk = true;
      } else assert.equal(req.headers.authorization, 'Bearer fixture-only-access');
      apiSdkCalls++;
      res.writeHead(200, {'content-type': 'application/json'}).end('[]');
      return;
    }
    if (measuredLauncher && req.url.startsWith('/hold/')) {
      if (slotQualification) {
        const held = heldSlots.get(req.url.slice('/hold/'.length));
        assert.ok(held?.sdk, 'Held request must follow its own credential-bound SDK call');
        held.entered.resolve();
        await held.release.promise;
      }
      res.end('released');
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
    if (req.url.startsWith('/v1/')) capacityCalls++;
    const body = req.method === 'GET' ? undefined : await buffer(req);
    const response = await provider.fetch(base + req.url, {method: req.method, headers: req.headers, body});
    const headers = Object.fromEntries(response.headers);
    if (req.method === 'GET' && plannedReconnect?.path === req.url &&
        requestedAt <= plannedReconnect.finished && Date.now() >= plannedReconnect.started) {
      headers['X-Fixture-Planned-Reconnect'] = 'true';
    }
    res.writeHead(response.status, headers);
    res.end(Buffer.from(await response.arrayBuffer()));
  } catch {
    res.writeHead(502).end();
  }
});

server.on('upgrade', (req, socket, head) => {
  if (req.url !== '/api/worker-control') return socket.destroy();
  upgrades.handleUpgrade(req, socket, head, (ws) => {
    let record;
    let sequence = 0;
    const assembler = new FragmentAssembler();
    ws.on('message', (raw) => {
      const message = JSON.parse(raw);
      if (record) {
        if (message.payload?.event === 'inventory') record.inventory = message;
        if (message.payload?.reply === 'canary-test') record.canary = message.payload;
        if (message.payload?.event === 'heartbeat') record.heartbeat = message.payload;
        if (measuredLauncher && ['event', 'fragment'].includes(message.kind)) {
          const terminal = assembler.feed(message)?.payload;
          if (terminal?.event === 'cloudflare_generation_request') {
            record.generations.add(terminal.identity);
            record.send('command', {id: randomUUID(), action: 'cloudflare_generation_reply', payload: {
              route_id: terminal.route_id, request_id: terminal.request_id, ok: true, result: {},
            }});
          }
          if (terminal?.event === 'result') {
            record.send('ack', {event_id: terminal._event_id});
            record.terminalAckSequence = sequence;
            record.terminals.get(terminal.route_id)?.resolve(terminal);
          }
          if (terminal?.reply && terminal?.error_type) {
            record.replies.get(terminal.reply)?.reject(new Error(JSON.stringify(terminal)));
          }
          if (terminal?.reply && terminal?.result) record.commandAcks.get(terminal.reply)?.resolve(terminal.result);
        }
        return;
      }
      const id = message.hello.incarnation_id;
      record = {hello: message};
      const records = hellos.get(id) ?? [];
      records.push(record);
      record.terminals = new Map();
      record.replies = new Map();
      record.commandAcks = new Map();
      record.generations = new Set();
      record.send = (type, payload) => {
        for (const frame of fragment(envelope(records.length, sequence + 1, type, payload))) {
          sequence = frame.sequence;
          ws.send(JSON.stringify(frame));
        }
      };
      hellos.set(id, records);
      sockets.set(id, ws);
      ws.send(JSON.stringify({event: 'welcome', session_generation: records.length}));
    });
  });
});

async function start() {
  const relaySource = `export default {fetch(request) {
    if (!['https://test-api.invalid', 'https://phase5-fixture.invalid'].includes(new URL(request.url).origin)) {
      return new Response(null, {status: 403});
    }
    return fetch(new Request(${JSON.stringify(base)} + new URL(request.url).pathname, request));
  }};`;
  mf = new Miniflare({cf: false, ...(memoryProbe ? {inspectorPort: 0} : {}), resourcePersistencePath: storage, workers: [
    capacityWorker({...settings, bundle: measuredBundle, slots: launcherSlots}),
    {
      dev: {outboundService: {type: 'worker', worker: 'relay'}},
      config: {name: 'launcher', compatibilityDate: '2026-10-30',
        exports: {CloudflareLauncher: {type: 'durable-object', storage: 'sqlite'}, ...(measuredLauncher
          ? {TenantHost: {type: 'durable-object', storage: 'sqlite'}, TenantApi: {type: 'worker'}}
          : {MeterProbe: {type: 'durable-object', storage: 'sqlite'}})},
        env: {API_ORIGIN: {type: 'text', value: 'https://test-api.invalid'},
          BIFROST_TENANT_LINGER_SECONDS: {type: 'text', value: '90'},
          BIFROST_CLOUDFLARE_MANAGED: {type: 'text', value: 'true'}, ...(measuredLauncher ? {
            ...(lifetimeSoak ? {BIFROST_LAUNCHER_METERS_PER_SECOND: {
              type: 'text', value: JSON.stringify(lifetimeRates),
            }} : {}),
            LOADER: {type: 'worker-loader'},
            RUNTIME_SHA256: {type: 'text', value: '11ade0a1ea1b117d577803cff87d1c3c4074d793df5bcb17a9f473923e55f254'},
            BIFROST_METER_ENGINE_CPU_MS: {type: 'text', value: '30000'},
            BIFROST_METER_BOOT_CPU_MS: {type: 'text', value: process.env.BIFROST_QUALIFIED_BOOT_CPU_MS ?? '30000'},
            BIFROST_METER_WARM_CPU_MS: {type: 'text', value: process.env.BIFROST_QUALIFIED_WARM_CPU_MS ?? '30000'},
            BIFROST_METER_EXERCISE_CPU_MS: {type: 'text', value: process.env.BIFROST_QUALIFIED_EXERCISE_CPU_MS ?? '30000'},
            BIFROST_METER_SDK_REQUESTS: {type: 'text', value: '40'},
            ...(measuredGateway ? {
              TENANT_GATEWAY: {type: 'worker', worker: 'gateway'},
              BIFROST_METER_GENERATIONS: {type: 'text', value: 'true'},
              BIFROST_METER_RECEIPTS: {type: 'text', value: 'true'},
            } : {}),
            BIFROST_LAUNCHER_METERS: {type: 'text', value: process.env.BIFROST_QUALIFIED_LAUNCHER_METERS ??
              '{"sqlite_rows_written":10000,"do_requests":1000,"log_events":100}'},
          } : {MeterProbe: {type: 'durable-object', worker: 'launcher', exportName: 'MeterProbe'},
            BIFROST_LAUNCHER_METERS: {type: 'text', value: '{"sqlite_rows_written":10000,"do_requests":1000,"log_events":100}'},
          })},
        manifest: {mainModule: 'entry.mjs', modulesRoot: fileURLToPath(root), modules: measuredLauncher
          ? {'entry.mjs': {type: 'esm', contents: readFileSync(measuredLauncher, 'utf8')}} : modules}},
    },
    ...(measuredGateway ? [{config: {name: 'gateway', compatibilityDate: '2026-10-30',
      env: {API_ORIGIN: {type: 'text', value: 'https://test-api.invalid'}, SDK_SCOPE: {type: 'text', value: 'sdk'}},
      manifest: {mainModule: 'gateway.mjs', modules: {'gateway.mjs': {
        type: 'esm', contents: readFileSync(measuredGateway, 'utf8'),
      }}},
    }}] : []),
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
  const startedAt = Date.now();
  const initialCapacityCalls = capacityCalls;
  const id = randomUUID();
  const path = `workers/${id}`;
  const uid = randomBytes(16).toString('hex');
  const key = randomBytes(32).toString('hex');
  const deadlineMs = lifetimeSoak ? Date.now() + soakSeconds * 1000 : undefined;
  const reservation = {worker_id: id, worker_version: settings.version,
    memory_limit_bytes: launcherSlots * 128 * 1024 ** 2,
    max_lifetime_seconds: lifetimeSoak ? soakSeconds :
      controllerPolls ? Math.ceil(controllerPolls * reconcileSeconds) + 120 : 60,
    drain_grace_seconds: lifetimeSoak ? 285 : 5,
    ...(deadlineMs === undefined ? {} : {deadline_ms: deadlineMs}),
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
  let references = null;
  if (lifetimeSoak) {
    const tenant = randomUUID();
    let record = seen;
    const verifySession = async () => {
      const records = hellos.get(id);
      const previous = record.hello.hello;
      plannedReconnect = {path: `/v1/${path}`, started: Date.now(), finished: Infinity};
      try {
        sockets.get(id).close(1012);
        record = await next(() => records.find(item => item !== seen && item.inventory));
      } finally {
        plannedReconnect.finished = Date.now();
      }
      const {proof, ...hello} = record.hello.hello;
      assert.equal((await signHello(key, hello)).proof, proof);
      assert.equal(hello.incarnation_id, previous.incarnation_id);
      assert.equal(hello.hello_sequence, previous.hello_sequence + 1);
      assert.notEqual(hello.nonce, previous.nonce);
      assert.ok(Math.abs(Date.now() / 1000 - hello.issued_at) < 10);
      return hello.hello_sequence;
    };
    const run = async (ordinal, solution) => {
      const name = randomUUID();
      const held = {entered: Promise.withResolvers(), release: Promise.withResolvers(), sdk: false};
      held.release.resolve();
      heldSlots.set(name, held);
      try {
        const result = await referenceAttempt(record, solution, {ordinal, name, org: tenant, soak: true});
        return {...result, sdk: held.sdk};
      } finally {
        heldSlots.delete(name);
      }
    };
    await runLifetimeSoak({
      result: lifetimeSoak, seconds: soakSeconds, deadlineMs, storage, run, verifySession, launcher: measuredLauncher,
      cadence: controllerPolls ? () => observeAtControllerCadence(path) : undefined,
      spacing: Number(process.env.BIFROST_LIFETIME_SOAK_SPACING_SECONDS ?? 30),
      sampleIsolates: memoryProbe ? isolateSampler(await mf.getInspectorURL()) : undefined,
      observe: async () => (await (await request(path)).json()).state,
      settle: async () => {
        await next(() => record.heartbeat?.occupied_routes?.length === 0 &&
          record.heartbeat.received_sequence >= (record.terminalAckSequence ?? 0));
        const stopped = await (await request(path, 'DELETE', {resource_uid})).json();
        assert.equal(stopped.state, 'deleted');
        await next(() => sockets.get(id).readyState === 3);
        const compacted = await (await request(path, 'DELETE', {resource_uid, receipt_ack: true})).json();
        assert.equal(compacted.state, 'deleted');
        assert.equal(compacted.usage, undefined);
        return stopped.usage;
      },
    });
    return;
  }
  if (slotQualification) {
    assert.equal(seen.inventory.payload.inventory.tenant_slot_protocol, 1);
    const org = randomUUID();
    const startSlot = (ordinal, cancel = false) => {
      const name = randomUUID();
      const held = {entered: Promise.withResolvers(), release: Promise.withResolvers(), sdk: false};
      const routeReady = Promise.withResolvers();
      heldSlots.set(name, held);
      return {...held, routeReady, terminal: referenceAttempt(seen, false, {ordinal, name, org, cancel, routeReady})};
    };
    const concurrent = Array.from({length: 4}, (_, slot) => startSlot(slot));
    const allTerminals = Promise.all(concurrent.map(slot => slot.terminal));
    try {
      await Promise.race([
        Promise.all(concurrent.map(slot => slot.entered.promise)),
        allTerminals.then(() => assert.fail('Workflows finished before all four reached the barrier')),
      ]);
    } finally {
      concurrent.forEach(slot => slot.release.resolve());
    }
    const results = await allTerminals;
    await next(() => seen.heartbeat?.occupied_routes?.length === 0 &&
      seen.heartbeat.received_sequence >= seen.terminalAckSequence);
    const repeated = startSlot(0);
    repeated.release.resolve();
    references = [...results, await repeated.terminal];
    assert.equal(seen.generations.size, 4);
    console.info('tenant-slot-overlap', JSON.stringify({overlapping: 4, credentialBound: 5, distinctGenerations: 4}));
    await next(() => seen.heartbeat?.occupied_routes?.length === 0 &&
      seen.heartbeat.received_sequence >= seen.terminalAckSequence);
    const sibling = startSlot(1);
    const cancelled = startSlot(0, true);
    try {
      await Promise.all([sibling.entered.promise, cancelled.entered.promise]);
      const siblingRoute = await sibling.routeReady.promise;
      const route_id = await cancelled.routeReady.promise;
      const id = randomUUID();
      const stopReceived = Promise.withResolvers();
      seen.commandAcks.set(id, stopReceived);
      seen.send('command', {id, action: 'terminate', payload: {
        route_id, error_type: 'CancelledError', error: 'Local fixture cancelled',
      }});
      assert.deepEqual(await stopReceived.promise, {received: true});
      cancelled.release.resolve();
      assert.equal((await cancelled.terminal).attempt_state, 'stopped');
      await next(() => seen.heartbeat?.received_sequence >= seen.terminalAckSequence);
      assert.deepEqual(seen.heartbeat.occupied_routes, [siblingRoute]);
      const replacement = startSlot(0);
      replacement.release.resolve();
      await replacement.terminal;
      assert.equal(seen.generations.size, 5);
    } finally {
      cancelled.release.resolve();
      sibling.release.resolve();
    }
    assert.equal((await sibling.terminal).route_id, await sibling.routeReady.promise);
    console.info('tenant-slot-cancel', JSON.stringify({confirmed: true, replacement: true, siblingCompleted: true}));
  } else if (controllerPolls) {
    assert.ok(measuredLauncher);
    const loose = await referenceAttempt(seen, false);
    await observeAtControllerCadence(path);
    references = [loose, await referenceAttempt(seen, true)];
  } else if (measuredLauncher) {
    references = await Promise.all([referenceAttempt(seen, false), referenceAttempt(seen, true)]);
  }
  if (references) {
    // A later empty heartbeat witnesses that both terminal ACKs cleared
    // their slots before the provider was asked to seal the incarnation.
    await next(() => seen.heartbeat?.occupied_routes?.length === 0 &&
      seen.heartbeat.received_sequence >= seen.terminalAckSequence);
  }
  const deleted = await (await request(path, 'DELETE', {resource_uid})).json();
  assert.equal(deleted.state, 'deleted');
  assert.deepEqual(await (await request(path, 'DELETE', {resource_uid})).json(), deleted);
  await next(() => sockets.get(id).readyState === 3);
  assert.equal((await request(path + '/start', 'POST', {resource_uid})).status, 409);
  assert.equal(hellos.get(id).length, 1);
  assert.equal(deleted.usage?.ledger_receipt_pending, true);
  const compacted = await (await request(path, 'DELETE', {resource_uid, receipt_ack: true})).json();
  assert.equal(compacted.state, 'deleted');
  assert.equal(compacted.usage, undefined);
  assert.deepEqual(await (await request(path)).json(), compacted);
  if (!measuredLauncher) {
    await mf.dispose();
    await start();
    const launchers = await mf.getDurableObjectNamespace('CLOUDFLARE_LAUNCHER', 'capacity');
    assert.deepEqual(await launchers.getByName(id).witnessIdleStorage(), []);
  }
  if (references) {
    assert.ok(references.every(attempt => attempt.usage));
    assert.ok(deleted.usage);
    if (measuredGateway && !slotQualification) {
      assert.equal(seen.generations.size, 2);
      for (const {route_id} of references) {
        const calls = deleted.usage.routes[route_id];
        assert.deepEqual(calls.map(({kind, cold}) => [kind, cold]), [['boot', true], ['workflow', false]]);
        assert.equal(calls[0].identity, calls[1].identity);
        assert.ok(Number.isSafeInteger(calls[1].wall_ms) && calls[1].wall_ms >= 0);
      }
    }
    if (slotQualification) {
      const identities = references.map(({route_id}) =>
        deleted.usage.routes[route_id].find(call => call.kind === 'workflow').identity);
      assert.equal(new Set(identities.slice(0, 4)).size, 4);
      assert.equal(identities[4], identities[0]);
    }
    console.info('launcher-reference', JSON.stringify({attempts: references, final: deleted.usage,
      lifetime_seconds: (Date.now() - startedAt) / 1000, capacity_requests: capacityCalls - initialCapacityCalls,
      sha256: sha256(readFileSync(measuredLauncher))}));
  }
});

if (!slotQualification) {
  test('compacting a WorkerLedger KV receipt releases its SQLite pages', async () => {
    const probes = await mf.getDurableObjectNamespace('MeterProbe', 'launcher');
    const {before, after} = await probes.getByName('fence').witnessFenceSize();
    assert.ok(before >= 100_000 && after <= 16_000, `receipt ${before} bytes, fence ${after} bytes`);
  });

  for (const [meter, count, counted] of [
    ['sqlite_rows_written', 19, 423], ['do_requests', 6, 406], ['log_events', 2, 2],
  ]) {
    test(`workerd counts and refuses launcher ${meter} at the workload ceiling`, async () => {
      const probes = await mf.getDurableObjectNamespace('MeterProbe', 'launcher');
      const receipt = JSON.parse(await probes.getByName(meter).witness(meter, count));
      assert.equal(receipt.accepted, true);
      assert.equal(receipt.rejected, false);
      assert.equal(receipt.exhausted, meter !== 'log_events');
      assert.equal(receipt.sealed.exhausted, meter !== 'log_events');
      assert.equal(receipt.sealed[meter], counted);
      assert.ok(receipt.sealed.sqlite_database_bytes > 0);
    });
  }

  for (const {name, status, headers, proof, expected} of [
    {name: 'accepts authenticated 204', status: 204,
      headers: {'x-bifrost-worker-ingress': `v${ingressPolicy.version}; allowed`}, expected: {}},
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
}

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
