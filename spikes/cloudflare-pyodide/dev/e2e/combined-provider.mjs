// Local Cloudflare REST stand-in. The uploaded Workers run in real workerd.
import assert from 'node:assert/strict';
import {randomBytes, randomUUID} from 'node:crypto';
import {readFileSync, writeFileSync} from 'node:fs';
import {createServer, request} from 'node:http';
import {createServer as tlsServer} from 'node:https';
import {createRequire} from 'node:module';
import {BlockList, isIPv4} from 'node:net';
import {buffer} from 'node:stream/consumers';
import {relayHeaders} from './relay-http.mjs';

const require = createRequire(new URL('../tenant-workers/package.json', import.meta.url));
const {Miniflare} = createRequire(require.resolve('wrangler'))('miniflare');
const {WebSocket, WebSocketServer} = require('ws');
const config = JSON.parse(readFileSync(process.argv[2], 'utf8'));
const relayOnly = process.argv[3] === '--hosted-ingress-relay';
if (relayOnly) {
  assert.match(config.relayToken ?? '', /^[0-9a-f]{64}$/);
  assert.ok(Number.isInteger(config.relayPort) && config.relayPort > 1023 && config.relayPort < 65536);
}
const initialTunnelVersion = config.initialTunnelConfig === 'default_404' ? 1 : 0;
const account = 'a'.repeat(32);
const zone = 'b'.repeat(32);
const management = relayOnly ? config.relayToken : 'local-rehearsal-management-token';
const workers = new Map();
const runtimes = new Map();
const sockets = new Map();
const sdkCalls = new Map();
const entered = new Set();
const heldQueries = new Map();
const executionByRoute = new Map();
let connectionSequence = 0;
const transport = new WebSocketServer({noServer: true});
const witnessHeaders = ['x-bifrost-worker-ingress', 'x-bifrost-ingress-probe'];
transport.on('headers', (headers, req) => {
  for (const name of witnessHeaders) {
    const value = req.ingressWitness?.[name];
    if (typeof value === 'string') headers.push(name + ': ' + value);
  }
});
const observations = [];
let tunnel;
let dns;
const projectAddresses = new BlockList();
const [networkAddress, prefix] = config.subnet.split('/');
projectAddresses.addSubnet(networkAddress, Number(prefix), 'ipv4');
let ingressHost;
let lagNamespace;

function event(kind, details = {}) {
  observations.push({kind, ...details});
}

function controlFrame(message) {
  try {
    return JSON.parse(message) ?? {};
  } catch {
    // The ingress owns protocol validation. A malformed client frame must
    // never terminate the shared relay or expose its contents in trial logs.
    return {};
  }
}

function module(contents) {
  return {type: 'esm', contents};
}

function runtime(capacity) {
  const current = runtimes.get(capacity.name);
  if (current) return current;
  const owner = capacity.owner.split(':')[0];
  const revision = capacity.owner.split(':')[2];
  const cohort = [...workers.values()].filter((item) =>
    item.owner.startsWith(owner + ':') && item.owner.split(':')[2] === revision);
  assert.equal(cohort.length, 3);
  const origins = [...new Set(cohort.flatMap(({meta}) =>
    meta.bindings.filter(({name}) => name === 'API_ORIGIN').map(({text}) => text)))];
  assert.equal(origins.length, 1);
  const mf = new Miniflare({cf: false, workers: [
    ...cohort.map(({name, meta, modules}) => ({
      dev: {outboundService: {type: 'worker', worker: 'relay'}},
      config: {
        name, compatibilityDate: meta.compatibility_date,
        compatibilityFlags: meta.compatibility_flags, exports: meta.exports,
        manifest: {mainModule: meta.main_module, modules},
        env: Object.fromEntries(meta.bindings.map((binding) => {
          const {name: key, type, text, class_name, script_name, service} = binding;
          const spec = {
            plain_text: () => ({type: 'text', value: text}),
            secret_text: () => ({type: 'text', value: text}),
            durable_object_namespace: () => ({
              type: 'durable-object', worker: script_name ?? name, exportName: class_name,
            }),
            worker_loader: () => ({type: 'worker-loader'}),
            service: () => ({type: 'worker', worker: service}),
          };
          assert.ok(spec[type], 'Unexpected uploaded binding type');
          return [key, spec[type]()];
        })),
      },
    })),
    {config: {name: 'relay', compatibilityDate: '2026-10-30',
      manifest: {mainModule: 'relay.mjs', modules: {'relay.mjs': module(`
        export default {fetch(request) {
          const url = new URL(request.url);
          if (url.origin !== ${JSON.stringify(origins[0])}) return new Response('Local fixture only', {status: 403});
          const next = new Request('http://127.0.0.1:${relay.address().port}' + url.pathname + url.search, request);
          next.headers.set('x-rehearsal-host', url.host);
          return fetch(next);
        }};
      `)}}}},
  ]});
  const value = mf.getWorker(capacity.name).then((worker) => ({mf, worker}));
  runtimes.set(capacity.name, value);
  return value;
}

function serveIngress(req, res) {
  if (!ingressHost) return res.writeHead(503).end();
  const incoming = new URL(req.url, 'http://127.0.0.1');
  const upstream = request({
    host: ingressHost, port: 8001, path: incoming.pathname + incoming.search,
    method: req.method, headers: {...relayHeaders(req.headers),
      host: relayOnly ? req.headers.host : req.headers['x-rehearsal-host'] ?? req.headers.host},
  }, (response) => {
    const call = sdkCalls.get(req.headers.authorization?.slice(7));
    if (call && incoming.pathname === '/api/sdk/tables/list' && response.statusCode === 200) {
      entered.add(call.execution);
      if (++call.count === 2 && !relayOnly) {
        heldQueries.set(call.execution, () => {
          if (res.destroyed) return response.destroy();
          res.writeHead(response.statusCode, relayHeaders(response.headers));
          response.pipe(res);
        });
        res.on('close', () => heldQueries.delete(call.execution));
        return;
      }
    }
    res.writeHead(response.statusCode, relayHeaders(response.headers));
    response.pipe(res);
  });
  upstream.on('error', () => res.writeHead(502).end());
  req.on('aborted', () => upstream.destroy());
  req.pipe(upstream);
}

function upgrade(req, socket, head) {
  if (!ingressHost || req.url !== '/api/worker-control') return socket.destroy();
  const headers = {Host: relayOnly ? req.headers.host : req.headers['x-rehearsal-host'] ?? req.headers.host};
  if (req.headers['x-bifrost-ingress-probe']) headers['x-bifrost-ingress-probe'] = req.headers['x-bifrost-ingress-probe'];
  const upstream = new WebSocket(`ws://${ingressHost}:8001${req.url}`, {headers});
  upstream.on('upgrade', (reply) => { req.ingressWitness = reply.headers; });
  upstream.on('error', () => socket.destroy());
  upstream.on('open', () => transport.handleUpgrade(req, socket, head, (worker) => {
    let identity;
    const connection = ++connectionSequence;
    const terminate = new Map();
    worker.on('message', (message) => {
      const frame = controlFrame(message);
      if (frame.event === 'hello' && frame.hello?.incarnation_id) {
        identity = frame.hello.incarnation_id;
        if (identity) sockets.set(identity, worker);
        event('hello', {worker: identity, connection});
      } else if (frame.payload?.event === 'result') {
        event('result', {worker: identity, connection, id: frame.payload._event_id, route: frame.payload.route_id,
          attempt: frame.payload.attempt_state});
      }
      const pending = terminate.get(frame.payload?.reply);
      if (pending && frame.payload.result?.received) {
        terminate.delete(frame.payload.reply);
        const release = heldQueries.get(pending);
        if (release) {
          heldQueries.delete(pending);
          // Give the real launcher time to persist its stopping state.
          setTimeout(release, 100);
        }
      }
      if (upstream.readyState === WebSocket.OPEN) upstream.send(message.toString());
    });
    upstream.on('message', (message) => {
      const frame = controlFrame(message);
      if (frame.kind === 'ack' && frame.payload?.event_id) {
        event('ack', {worker: identity, connection, id: frame.payload.event_id});
      }
      if (frame.kind === 'command' && frame.payload?.action === 'workflow') {
        const command = frame.payload.payload;
        sdkCalls.set(command.context.engine_token, {execution: command.execution_id, count: 0});
        executionByRoute.set(command.route_id, command.execution_id);
        event('dispatch', {worker: identity, connection, route: frame.payload.payload.route_id,
          execution: frame.payload.payload.execution_id});
      }
      if (frame.kind === 'command' && frame.payload?.action === 'terminate') {
        const execution = executionByRoute.get(frame.payload.payload.route_id);
        if (heldQueries.has(execution)) terminate.set(frame.payload.id, execution);
      }
      if (worker.readyState === WebSocket.OPEN) worker.send(message.toString());
    });
    const close = (code) => {
      if (identity && sockets.get(identity) === worker) sockets.delete(identity);
      const valid = [1005, 1006, 1015].includes(code) ? 1012 : code;
      if (upstream.readyState === WebSocket.OPEN) upstream.close(valid);
      if (worker.readyState === WebSocket.OPEN) worker.close(valid);
    };
    worker.on('close', close);
    worker.on('error', () => close(1012));
    upstream.on('close', close);
  }));
}

const relay = createServer(serveIngress);
const edge = relayOnly ? null : tlsServer({key: readFileSync(config.key), cert: readFileSync(config.cert)}, serveIngress);
relay.on('upgrade', upgrade);
edge?.on('upgrade', upgrade);

async function provider(req, res) {
  const url = new URL(req.url, 'http://local.test');
  const path = url.pathname;
  const method = req.method;
  const send = (status, body) => {
    res.writeHead(status, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(body));
  };
  const ok = (result) => send(200, {success: true, result});
  const missing = () => send(404, {success: false});
  const json = async () => JSON.parse(await buffer(req));

  const capacity = path.match(/^\/edge-capacity\/([a-z0-9-]+)(\/.*)$/);
  if (!relayOnly && capacity) {
    const item = workers.get(capacity[1]);
    if (!item?.endpoint.enabled) return missing();
    const {worker} = await runtime(item);
    const body = ['GET', 'HEAD'].includes(method) ? undefined : await buffer(req);
    const response = await worker.fetch('https://capacity.test' + capacity[2] + url.search,
      {method, body, headers: relayHeaders(req.headers)});
    res.writeHead(response.status, relayHeaders(Object.fromEntries(response.headers)));
    return res.end(Buffer.from(await response.arrayBuffer()));
  }

  if (req.headers.authorization !== 'Bearer ' + management) return send(403, {success: false});
  if (path === '/local/evidence') return ok(observations);
  if (path === '/local/entered' && method === 'GET') return ok([...entered]);
  if (path === '/local/held-reads' && method === 'GET') return ok([...heldQueries.keys()]);
  if (path === '/local/ingress' && method === 'POST') {
    const {host} = await json();
    assert.ok(isIPv4(host) && projectAddresses.check(host, 'ipv4') && host !== config.address);
    ingressHost = host;
    return ok({});
  }
  if (relayOnly) return missing();
  if (path === '/local/interrupt' && method === 'POST') {
    const {worker} = await json();
    assert.ok(sockets.get(worker));
    sockets.get(worker).close(1012, 'local qualification transport loss');
    event('interrupt', {worker});
    return ok({});
  }
  if (path === '/local/tunnel-connect' && method === 'POST') {
    const {token} = await json();
    const presented = JSON.parse(Buffer.from(token, 'base64').toString());
    if (presented.s !== tunnel?.secret || presented.t !== tunnel?.record.id || tunnel.record.deleted_at) {
      return send(403, {success: false});
    }
    const id = randomUUID();
    tunnel.sessions.add(id);
    return ok({id});
  }
  if (path === '/local/tunnel-detach' && method === 'POST') {
    const {id} = await json();
    assert.ok(tunnel.sessions.delete(id));
    return ok({});
  }
  if (path === '/client/v4/graphql') {
    const body = await json();
    const samples = {requests: 1, cpuTimeUs: 1000};
    const workerRows = body.variables?.scripts?.map((scriptName) => ({dimensions: {scriptName}, sum: samples})) ??
      [{sum: samples, avg: {sampleInterval: 1}}];
    return send(200, {data: {viewer: {accounts: [{workersInvocationsAdaptive: workerRows,
      durableObjectsInvocationsAdaptiveGroups: []}]}}});
  }
  const relative = path.replace(`/client/v4/accounts/${account}`, '');
  if (relative === '') return ok({id: account});
  if (relative === '/entitlements') return ok(['workers.enabled', 'containers.enabled'].map((id) =>
    ({id, deleted_date: '', allocation: {type: 'bool', value: true}})));
  if (relative === '/workers/subdomain') return ok({subdomain: 'rehearsal'});
  if (relative === '/workers/scripts') return ok([...workers.keys()].map((id) => ({id})));
  const namespaces = [...workers.values()].flatMap((item) => item.namespaces);
  if (relative === '/workers/durable_objects/namespaces') {
    if (lagNamespace) {
      const stale = namespaces.filter(({id}) => id !== lagNamespace);
      lagNamespace = undefined;
      event('namespace-lag');
      event('namespace-inventory', {ids: stale.map(({id}) => id)});
      return ok(stale);
    }
    event('namespace-inventory', {ids: namespaces.map(({id}) => id)});
    return ok(namespaces);
  }
  const objects = relative.match(/^\/workers\/durable_objects\/namespaces\/([^/]+)\/objects$/);
  if (objects) return namespaces.some(({id}) => id === objects[1]) ? ok([]) : missing();
  const script = relative.match(/^\/workers\/scripts\/([a-z0-9-]+)(?:\/(settings|deployments|subdomain))?$/);
  if (script) {
    const [, name, child] = script;
    const item = workers.get(name);
    if (method === 'PUT' && !child) {
      assert.ok(!item, 'Unexpected duplicate upload');
      const form = await new Request(url, {method, headers: req.headers, body: await buffer(req)}).formData();
      const meta = JSON.parse(await form.get('metadata').text());
      const modules = Object.fromEntries(await Promise.all([...form.entries()]
        .filter(([key]) => key !== 'metadata').map(async ([key, value]) => [key, module(await value.text())])));
      const owner = meta.bindings.find(({name: key}) => key === 'BIFROST_CAPACITY_OWNER').text;
      workers.set(name, {name, meta, modules, owner, deployment: randomUUID(), version: randomUUID(),
        endpoint: {enabled: false, previews_enabled: false},
        namespaces: Object.keys(meta.exports).map((exported) =>
          ({id: randomBytes(16).toString('hex'), script: name, class: exported}))});
      const revision = Number(owner.split(':')[2]);
      const role = name.split('-').at(-1);
      if (config.lagWorkerNamespace && revision === 1 && role === 'capacity') {
        lagNamespace = workers.get(name).namespaces[0].id;
      }
      event('upload', {revision, role});
      return ok({id: name});
    }
    if (!item) return missing();
    if (child === 'settings') return ok({bindings: item.meta.bindings.map(({type, text, ...binding}) =>
      type === 'secret_text' ? {type, ...binding} : {type, text, ...binding})});
    if (child === 'deployments') return ok({deployments: [{id: item.deployment,
      versions: [{version_id: item.version, percentage: 100}]}]});
    if (child === 'subdomain') {
      if (method === 'POST') item.endpoint = await json();
      return ok(item.endpoint);
    }
    if (!child && method === 'DELETE') {
      const active = await runtimes.get(name);
      if (active) await active.mf.dispose();
      runtimes.delete(name);
      workers.delete(name);
      event('delete', {revision: Number(item.owner.split(':')[2]), role: name.split('-').at(-1)});
      return ok(null);
    }
  }

  if (relative === '/teamnet/routes') {
    assert.equal(tunnel.record.id, url.searchParams.get('tunnel_id'));
    assert.equal(url.searchParams.get('is_deleted'), 'false');
    event('route-list');
    return ok([]);
  }
  if (relative === '/cfd_tunnel') {
    if (method === 'POST') {
      const body = await json();
      assert.ok(!tunnel || tunnel.record.deleted_at);
      tunnel = {record: {id: randomUUID(), name: body.name, account_tag: account, config_src: body.config_src,
        created_at: new Date().toISOString(), deleted_at: null}, secret: body.tunnel_secret, sessions: new Set(),
        version: initialTunnelVersion, config: null};
      if (initialTunnelVersion === 1) {
        tunnel.config = {ingress: [{service: 'http_status:404'}], 'warp-routing': {enabled: false}};
      }
      return ok(tunnel.record);
    }
    return ok(tunnel && !tunnel.record.deleted_at ? [tunnel.record] : []);
  }
  if (tunnel && relative.startsWith('/cfd_tunnel/' + tunnel.record.id)) {
    const suffix = relative.slice('/cfd_tunnel/'.length + tunnel.record.id.length);
    if (suffix === '') {
      if (method === 'PATCH') tunnel.secret = (await json()).tunnel_secret;
      if (method === 'DELETE') {
        assert.equal(tunnel.sessions.size, 0);
        assert.equal(dns, undefined);
        tunnel.record.deleted_at = new Date().toISOString();
      }
      return ok(tunnel.record);
    }
    if (suffix === '/configurations') {
      if (method === 'PUT') {
        tunnel.config = (await json()).config;
        tunnel.version += 1;
      }
      if (method === 'GET' && tunnel.version === initialTunnelVersion) {
        event('initial-tunnel-config', {shape: config.initialTunnelConfig});
      }
      if (tunnel.config === null && config.initialTunnelConfig === 'http_404') return missing();
      return ok({tunnel_id: tunnel.record.id, created_at: tunnel.record.created_at, source: 'cloudflare',
        version: tunnel.version, config: tunnel.config});
    }
    if (suffix === '/connections') {
      if (method === 'DELETE') {
        assert.ok(tunnel.sessions.delete(url.searchParams.get('client_id')));
        event('eviction');
        return ok({});
      }
      return ok([...tunnel.sessions].map((id) => ({id})));
    }
    if (suffix === '/token') return ok(Buffer.from(JSON.stringify({a: account,
      t: tunnel.record.id, s: tunnel.secret})).toString('base64'));
  }
  if (path === `/client/v4/zones/${zone}`) return ok({id: zone, account: {id: account},
    name: 'example.test', status: 'active'});
  const records = `/client/v4/zones/${zone}/dns_records`;
  if (path === records) {
    if (method === 'POST') {
      assert.equal(dns, undefined);
      dns = {...await json(), id: randomBytes(16).toString('hex'), modified_on: new Date().toISOString()};
      return ok(dns);
    }
    return ok(dns?.name === url.searchParams.get('name.exact') ? [dns] : []);
  }
  if (path.startsWith(records + '/')) {
    if (path !== records + '/' + dns?.id) return missing();
    if (method === 'DELETE') {
      dns = undefined;
      return ok({});
    }
    return ok(dns);
  }
  throw Error('Unexpected local provider operation: ' + method + ' ' + path);
}

const api = createServer((req, res) => provider(req, res).catch((error) => {
  console.error(error.name, error.message);
  res.writeHead(500).end();
}));
await Promise.all([
  new Promise((resolve) => relay.listen(relayOnly ? config.relayPort : 0, '127.0.0.1', resolve)),
  edge && new Promise((resolve) => edge.listen(config.edgePort, config.address, resolve)),
  new Promise((resolve) => api.listen(config.providerPort, relayOnly ? '127.0.0.1' : config.address, resolve)),
]);
writeFileSync(config.ready, 'ready\n');

process.once('SIGTERM', async () => {
  await Promise.allSettled([...runtimes.values()].map(async (entry) => (await entry).mf.dispose()));
  process.exit(0);
});
