import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {after, before, test} from 'node:test';
import {fileURLToPath} from 'node:url';
import {signHello} from '../cloudflare-launcher/protocol.mjs';

const {Miniflare, Response} = createRequire(import.meta.resolve('wrangler'))('miniflare');
const root = new URL('../../', import.meta.url);
const compatibilityDate = '2026-10-30';
const engineModule = 'dev/tenant-workers/generic-worker-code.mjs';
const mainModule = 'test-host.mjs';
const secret = 'a'.repeat(64);
const incarnation = '0e9d8c7b-6a59-4847-b6a5-948372615041';
const launcher = '6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f';
const path = (name) => fileURLToPath(new URL(name, import.meta.url));
const cases = new Map(['queued-block', 'active-block', 'first-tenant', 'second-tenant', 'held-ping'].map((name) =>
  [`/${name}`, {entered: Promise.withResolvers(), release: Promise.withResolvers()}]));
const origins = [];
let nextOrigin;
let mf;
let worker;

// Keep the real Durable Object, gateway, and WorkerLoader. Use a small JS
// child in place of the packaged Python engine to exercise transport policy.
function engine() {
  let firstPing = true;
  return {
    async fetch(request) {
      const body = await request.json();
      if (body.action === 'ping') {
        if (body.exercise) return new Response('fixture ping failure', {status: 503});
        if (body.modules.includes('hold_ping')) {
          const gate = await fetch('https://approved.invalid/held-ping');
          await gate.body?.cancel();
        }
        const first_request = firstPing;
        firstPing = false;
        return Response.json({first_request, primed: 0});
      }
      const {scenario, engine_token} = body.context;
      if (scenario === 'cpu-limit') throw new Error('Worker exceeded CPU time limit: private detail');
      if (scenario === 'memory-limit') throw new Error('Worker exceeded its memory limit: private detail');
      let result;
      if (scenario === 'snapshot') {
        const headers = {Authorization: `Bearer ${engine_token}`};
        const pinned = await fetch('https://test-api.invalid/api/sdk/modules/wf.py', {headers});
        const absent = await fetch('https://test-api.invalid/api/sdk/modules/missing.py', {headers});
        result = {content: (await pinned.json()).content, missing: absent.status};
      } else if (scenario === 'identity-sdk') {
        const headers = {Authorization: `Bearer ${engine_token}`, Cookie: 'session=child'};
        const users = await fetch('https://test-api.invalid/api/users?limit=1', {headers});
        const assignment = await fetch('https://test-api.invalid/api/roles/11111111-1111-4111-8111-111111111111/users', {
          method: 'POST', headers: {...headers, 'Content-Type': 'application/json'},
          body: JSON.stringify({user_ids: ['user-fixture']}),
        });
        result = {users: await users.json(), status: assignment.status,
          denial: assignment.ok ? null : await assignment.json()};
      } else if (scenario === 'model-catalog') {
        const headers = new Headers();
        if (body.context.catalog_etag) headers.set('If-None-Match', body.context.catalog_etag);
        const response = await fetch('https://models.dev/api.json', {headers});
        result = {status: response.status, etag: response.headers.get('etag'),
          catalog: response.status === 200 ? await response.json() : null};
      } else if (scenario === 'budget') {
        const statuses = [];
        for (let index = 0; index < 2; index++) {
          const response = await fetch('https://approved.invalid/count');
          statuses.push(response.status);
          await response.body?.cancel();
        }
        result = statuses;
      } else if (scenario === 'wildcard') {
        result = [];
        for (const origin of ['halopsa.com', 'a.b.halopsa.com', 'acme.halopsa.com.',
          'acme.halopsa.com:8443', 'acme.halopsa.com:443', 'second.halopsa.com']) {
          const response = await fetch(`https://${origin}/wildcard`);
          result.push(response.status);
          await response.body?.cancel();
        }
      } else {
        const response = await fetch(`https://approved.invalid/${scenario}`);
        result = response.status;
        await response.body?.cancel();
      }
      return Response.json({upstream_result: {status: 'Success', result}});
    },
  };
}

const main = `
  import {TenantHost as Host, TenantApi as Gateway} from './dev/tenant-workers/tenant-api.mjs';
  export class TenantApi extends Gateway {
    fetch(request) {
      if (request.url === 'https://approved.invalid/held-ping') return fetch(request);
      return super.fetch(request);
    }
  }
  export class TenantHost extends Host {
    async warmWithCommitFailure(input) {
      const storage = this.ctx.storage;
      Object.defineProperty(storage, 'sync', {configurable: true, value() {
        throw new Error('injected warm-state commit failure');
      }});
      try {
        return await super.warm(input);
      } finally {
        delete storage.sync;
      }
    }
    #started = new Map();
    #signal(id) {
      if (!this.#started.has(id)) this.#started.set(id, Promise.withResolvers());
      return this.#started.get(id);
    }
    run(input) {
      const attempt = super.run(input);
      this.#signal(input.routeId).resolve();
      return attempt;
    }
    async waitForRun(id) { await this.#signal(id).promise; }
    async primeHeldPing() {
      await this.ctx.storage.put('prime', {engine: 'test-engine', modules: ['hold_ping']});
    }
    async stopAndTryEgress(id) {
      const stopped = super.stopWorkflow(id);
      const result = await super.egress(new Request('https://approved.invalid/after-stop'), 0);
      await stopped;
      return result.status;
    }
  }
  export default {
    async fetch(request, _env, ctx) {
      const {tenantId, action, input} = await request.json();
      const host = ctx.exports.TenantHost.getByName(tenantId);
      if (action === 'run') return Response.json(await host.run(input));
      if (action === 'warm') return Response.json(await host.warm({tenantId, ...input}));
      if (action === 'warm-commit-failure') return Response.json(await host.warmWithCommitFailure({tenantId, ...input}));
      if (action === 'exercise') return Response.json(await host.exercise({tenantId}));
      if (action === 'prime-held-ping') await host.primeHeldPing();
      if (action === 'wait') await host.waitForRun(input);
      if (action === 'stop') await host.stopWorkflow(input);
      if (action === 'stop-egress') return Response.json(await host.stopAndTryEgress(input));
      return Response.json(null);
    },
  };
`;

before(async () => {
  const deps = [
    'dev/tenant-workers/tenant-api.mjs', 'dev/tenant-workers/tenant-gateway.mjs', 'dev/tenant-workers/storage-path.mjs',
    'dev/tenant-workers/tenant-dispatch.mjs',
    'dev/tenant-workers/attempt-credentials.mjs', 'dev/executor/policy.mjs',
    'dev/executor/output-budget.mjs', 'dev/executor/gateway-injection.mjs',
    'dev/executor/egress-allowlist.mjs', 'dev/executor/egress-hosts.mjs', 'dev/executor/egress-psl.generated.mjs',
    'backend/module-response.mjs',
    'backend/resolution-response.mjs', 'probe-io.mjs',
  ];
  mf = new Miniflare({workers: [{
    dev: {outboundService: {type: 'worker', worker: 'api'}},
    config: {name: 'launcher', compatibilityDate,
      exports: {CloudflareLauncher: {type: 'durable-object', storage: 'sqlite'}},
      manifest: {mainModule: 'tenant-workers/fake-entry.mjs', modulesRoot: path('..'), modules: {
        'tenant-workers/fake-entry.mjs': {type: 'esm', contents: `
        export {CloudflareLauncher} from '../cloudflare-launcher/launcher-do.mjs';
        export default {
          async fetch(_request, env, ctx) {
            await ctx.exports.CloudflareLauncher.getByName(env.BIFROST_WORKER_INCARNATION_ID).start();
            return new Response(null, {status: 204});
          },
        };
      `},
        ...Object.fromEntries(['cloudflare-launcher/launcher-do.mjs', 'cloudflare-launcher/launcher-core.mjs',
          'cloudflare-launcher/agent-loop.mjs', 'cloudflare-launcher/agent-provider.mjs',
          'cloudflare-launcher/protocol.mjs', 'tenant-workers/launcher-host.mjs', 'tenant-workers/tenant-dispatch.mjs',
          'tenant-workers/tenant-registry.mjs', 'tenant-workers/storage-path.mjs',
          'executor/egress-hosts.mjs', 'executor/egress-psl.generated.mjs'].map((name) =>
          [name, {type: 'esm', contents: readFileSync(path(`../${name}`), 'utf8')}])),
      }},
      env: Object.fromEntries(Object.entries({
        BIFROST_CLOUDFLARE_LAUNCHER_ENABLED: 'true', BIFROST_WORKER_CONTROL_SECRET: secret,
        BIFROST_WORKER_INCARNATION_ID: incarnation, BIFROST_WORKER_LAUNCHER_ID: launcher,
        BIFROST_WORKER_RESOURCE_UID: 'test:do', BIFROST_WORKER_VERSION: 'test-build',
        API_ORIGIN: 'https://test-api.invalid',
      }).map(([key, value]) => [key, {type: 'text', value}]).concat([
        ['SUMMARY_PROVIDER', {type: 'worker', worker: 'summary', exportName: 'SummaryProvider'}],
      ])),
    },
  }, {
    dev: {outboundService: {type: 'worker', worker: 'api'}},
    config: {name: 'summary', exports: {SummaryProvider: {type: 'worker'}}, compatibilityDate,
      env: {BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT: {type: 'text', value: 'https://test-provider.invalid/v1'}},
      manifest: {mainModule: 'summary-worker/entry.mjs', modules: {
        'summary-worker/entry.mjs': {type: 'esm', contents: readFileSync(path('../cloudflare-launcher/summary-worker/entry.mjs'), 'utf8')},
        'summary-provider.mjs': {type: 'esm', contents: readFileSync(path('../cloudflare-launcher/summary-provider.mjs'), 'utf8')},
      }},
    },
  }, {
    config: {name: 'api', compatibilityDate,
      exports: {ScriptedPeer: {type: 'durable-object', storage: 'sqlite'}},
      manifest: {mainModule: 'api.mjs', modulesRoot: path('.'), modules: {'api.mjs': {type: 'esm', contents: `
      import {DurableObject} from 'cloudflare:workers';
      export class ScriptedPeer extends DurableObject {
        async fetch(request) {
          const url = new URL(request.url);
          const path = url.pathname;
          if (url.origin === 'https://test-provider.invalid') {
            if (path !== '/v1/chat/completions' || request.method !== 'POST' ||
                request.headers.get('authorization') !== 'Bearer workerd-stub-key') {
              return new Response(null, {status: 401});
            }
            const body = await request.json();
            if (body.model === 'workerd-held') {
              let sent = false;
              return new Response(new ReadableStream({
                pull: (controller) => {
                  if (sent) return;
                  sent = true;
                  controller.enqueue(new TextEncoder().encode('{'));
                  this.control.send(JSON.stringify({kind: 'command', sequence: 4, session_generation: 1,
                    payload: {id: 'stop-summary', action: 'terminate', payload: {
                      route_id: this.heldRoute, error_type: 'OwnerLost', error: 'fixture cancelled',
                    }}}));
                },
                cancel: () => this.observer.send(JSON.stringify({providerCancelled: true})),
              }));
            }
            return Response.json({
              model: 'workerd-stub', choices: [{message: {content: '{"asked":"workerd"}'}}],
              usage: {prompt_tokens: 7, completion_tokens: 3},
            });
          }
          if (request.headers.get('upgrade') !== 'websocket' ||
              !['/observe', '/api/worker-control'].includes(path)) {
            return new Response(null, {status: 400});
          }
          const [client, server] = Object.values(new WebSocketPair());
          server.accept();
          if (path === '/observe') {
            this.observer = server;
          } else {
            this.control = server;
            let hello;
            let inventory;
            const run = '22222222-2222-4222-8222-222222222222';
            const sendSummary = (sequence, claim, model) => {
              const route = 'summary:' + run + ':' + claim;
              server.send(JSON.stringify({kind: 'command', sequence, session_generation: 1, payload: {
                id: model, action: 'cloudflare_summary', payload: {
                  route_id: route, run_id: run, claim_id: claim,
                  timeout_seconds: 15, memory_request_bytes: 16777216,
                  context: {endpoint: 'https://test-provider.invalid/v1', api_key: 'workerd-stub-key',
                    body: {
                      model, stream: false, store: false,
                      messages: [{role: 'system', content: 'summarize'}, {role: 'user', content: 'fixture'}],
                    }},
                },
              }}));
              return route;
            };
            server.addEventListener('message', ({data}) => {
              if (!hello) {
                hello = JSON.parse(data);
                server.send(JSON.stringify({event: 'welcome', session_generation: 1}));
              } else if (!inventory) {
                inventory = JSON.parse(data);
                sendSummary(1, '33333333-3333-4333-8333-333333333333', 'workerd-stub');
              } else {
                const terminal = JSON.parse(data).payload;
                if (terminal.event !== 'summary_result') return;
                if (terminal.route_id === this.heldRoute) {
                  this.observer.send(JSON.stringify({stopped: terminal}));
                  return;
                }
                this.observer.send(JSON.stringify({hello, inventory, terminal}));
                server.send(JSON.stringify({kind: 'ack', sequence: 2, session_generation: 1,
                  payload: {event_id: terminal._event_id}}));
                this.heldRoute = sendSummary(3, '44444444-4444-4444-8444-444444444444', 'workerd-held');
              }
            });
          }
          return new Response(null, {status: 101, webSocket: client});
        }
      }
      export default {fetch(request, _env, ctx) {
        return ctx.exports.ScriptedPeer.getByName('phase-4').fetch(request);
      }};
    `}}},
    },
  }, {
    config: {
      name: 'tenant', compatibilityDate,
      exports: {TenantHost: {type: 'durable-object', storage: 'sqlite'}, TenantApi: {type: 'worker'}},
      env: {LOADER: {type: 'worker-loader'}, API_ORIGIN: {type: 'text', value: 'https://upstream.invalid'},
        ATTEMPT_DEADLINE_MS: {type: 'text', value: '1'}},
      manifest: {mainModule, modulesRoot: fileURLToPath(root), modules: {
        [mainModule]: {type: 'esm', contents: main},
        [engineModule]: {type: 'esm', contents: `
          export const engineDigest = async () => 'test-engine';
          export const mergePrimeList = () => [];
          export const tenantEngineCode = (globalOutbound) => ({
            compatibilityDate: '${compatibilityDate}',
            mainModule: 'engine.js',
            modules: {'engine.js': 'export default (' + ${JSON.stringify(engine.toString())} + ')()'},
            globalOutbound,
          });
        `},
        ...Object.fromEntries(deps.map((path) => [path, {type: 'esm', contents: readFileSync(new URL(path, root), 'utf8')}])),
      }},
    },
    dev: {outboundService: {type: 'fetcher', handler: async (request) => {
      const url = new URL(request.url);
      origins.push(url.href);
      if (url.origin === 'https://upstream.invalid' &&
          ['/api/users', '/api/roles/11111111-1111-4111-8111-111111111111/users'].includes(url.pathname)) {
        const authorization = request.headers.get('Authorization');
        assert.ok(['Bearer test-access', 'Bearer restricted-access'].includes(authorization));
        assert.equal(request.headers.get('Cookie'), null);
        if (url.pathname === '/api/users') {
          assert.equal(request.method, 'GET');
          assert.equal(url.search, '?limit=1');
          return Response.json({users: ['user-fixture']});
        }
        assert.equal(request.method, 'POST');
        assert.deepEqual(await request.json(), {user_ids: ['user-fixture']});
        return authorization === 'Bearer restricted-access'
          ? Response.json({detail: 'API permission denied'}, {status: 403})
          : new Response(null, {status: 204});
      }
      if (url.href === 'https://models.dev/api.json') {
        assert.equal(request.method, 'GET');
        assert.equal(request.headers.get('Authorization'), null);
        const etag = '"model-catalog-fixture"';
        const headers = {ETag: etag};
        return request.headers.get('If-None-Match') === etag
          ? new Response(null, {status: 304, headers})
          : Response.json({fixture: {models: {sample: {name: 'Sample'}}}}, {headers});
      }
      if (url.pathname === '/health/ready' && nextOrigin) {
        const origin = nextOrigin;
        nextOrigin = undefined;
        origin.entered.resolve();
        await origin.release.promise;
      }
      if (url.pathname === '/redirect') return Response.redirect('https://undeclared.invalid/target', 302);
      const gate = cases.get(url.pathname);
      if (gate) {
        gate.entered.resolve();
        await gate.release.promise;
      }
      return new Response('upstream');
    }}},
  }]});
  worker = await mf.getWorker('tenant');
});

after(async () => {
  for (const gate of cases.values()) gate.release.resolve();
  await mf?.dispose();
});

async function host(tenantId, action, input, signal) {
  const response = await worker.fetch('https://tenant.invalid/', {
    method: 'POST', body: JSON.stringify({tenantId, action, input}), signal,
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

function route(tenantId, {routeId = tenantId, scenario = 'count', allowance = 8,
  httpsHosts = ['approved.invalid'],
  workflowDeadline = new Date(Date.now() + 20_000).toISOString()} = {}) {
  const content = 'pinned = True\n';
  return {tenantId, executionId: routeId, routeId, workflowDeadline, dispatchedAt: 1, context: {
    scenario, engine_token: 'test-access', cloudflare_workspace: {
      sources: {'wf.py': content},
      manifest: {'wf.py': createHash('sha256').update(content).digest('hex')},
      https_hosts: httpsHosts, cpu_budget_ms: 200, subrequest_allowance: allowance,
    },
  }};
}

test('workerd forwards identity SDK requests with run authority and returns the API refusal', async () => {
  const tenant = 'identity-tenant';
  await host(tenant, 'warm', {engineOnly: true});
  const input = route(tenant, {scenario: 'identity-sdk'});
  const permitted = await host(tenant, 'run', input);
  assert.deepEqual(permitted.output.result, {
    users: {users: ['user-fixture']}, status: 204, denial: null,
  });
  input.context.engine_token = 'restricted-access';
  const refused = await host(tenant, 'run', input);
  assert.deepEqual(refused.output.result, {
    users: {users: ['user-fixture']}, status: 403, denial: {detail: 'API permission denied'},
  });
  assert.deepEqual(refused.timings.api_calls.map(({method, path, status}) => [method, path, status]), [
    ['GET', '/api/users', 200], ['POST', '/api/roles/11111111-1111-4111-8111-111111111111/users', 403],
  ]);
});

test('workerd launcher uses named summary RPC and terminate cancels the outbound provider body', {timeout: 15_000}, async (t) => {
  const {signal} = t;
  const observer = await (await mf.getWorker('api')).fetch('https://test-api.invalid/observe', {
    headers: {Upgrade: 'websocket'},
  });
  assert.equal(observer.status, 101);
  const socket = observer.webSocket;
  const received = new Promise((resolve, reject) => {
    signal.addEventListener('abort', () => reject(signal.reason), {once: true});
    const evidence = {};
    socket.addEventListener('message', ({data}) => {
      Object.assign(evidence, JSON.parse(data));
      if (evidence.terminal && evidence.stopped && evidence.providerCancelled) resolve(evidence);
    });
  });
  socket.accept();
  const launched = await (await mf.getWorker('launcher')).fetch('https://launcher.invalid/');
  assert.equal(launched.status, 204);
  const {hello, inventory, terminal, stopped, providerCancelled} = await received;
  assert.equal(stopped.attempt_state, 'uncertain');
  assert.equal(providerCancelled, true);
  socket.close();
  assert.equal(hello.hello.incarnation_id, incarnation);
  assert.equal(hello.hello.hello_sequence, 1);
  assert.equal(hello.version, 'test-build');
  assert.deepEqual(hello.handlers, [['workflow.execute', 1], ['agent.summarize', 1]]);
  assert.equal(hello.summary_claim_protocol, 1);
  assert.equal(terminal.attempt_state, 'completed');
  assert.equal(terminal.result.content, '{"asked":"workerd"}');
  assert.equal(terminal.result.input_tokens, 7);
  assert.ok(!JSON.stringify(terminal).includes('workerd-stub-key'));
  assert.deepEqual(await signHello(secret, hello.hello), hello.hello);
  assert.deepEqual(inventory, {kind: 'event', sequence: 1, session_generation: 1,
    payload: {event: 'inventory', inventory: {version: 1, routes: [], terminal_routes: []}}});
});

test('opaque route can carry a credential-free conditional model catalog read', async () => {
  const tenant = 'model-catalog';
  const url = 'https://models.dev/api.json';
  const etag = '"model-catalog-fixture"';
  const callsBefore = origins.filter((origin) => origin === url).length;
  await host(tenant, 'warm', {engineOnly: true});

  const read = (routeId, catalogEtag) => {
    const input = route(tenant, {routeId, scenario: 'model-catalog',
      httpsHosts: ['models.dev'], allowance: 1});
    input.context.catalog_etag = catalogEtag;
    return host(tenant, 'run', input);
  };

  const fresh = await read('catalog-read', null);
  assert.equal(fresh.attempt_state, 'completed');
  assert.deepEqual(fresh.output.result, {status: 200, etag,
    catalog: {fixture: {models: {sample: {name: 'Sample'}}}}});
  const unchanged = await read('catalog-revalidate', etag);
  assert.equal(unchanged.attempt_state, 'completed');
  assert.deepEqual(unchanged.output.result, {status: 304, etag, catalog: null});
  assert.equal(origins.filter((origin) => origin === url).length - callsBefore, 2);
});

test('workerd runs distinct tenants on their own children at the same time', {timeout: 10_000}, async (t) => {
  const tenants = ['first-tenant', 'second-tenant'];
  await Promise.all(tenants.map((tenant) => host(tenant, 'warm')));
  const runs = tenants.map((tenant) => host(tenant, 'run', route(tenant, {scenario: tenant}), t.signal));
  try {
    await Promise.all(tenants.map(async (tenant) => {
      const aborted = new Promise((_, reject) =>
        t.signal.addEventListener('abort', () => reject(t.signal.reason), {once: true}));
      await Promise.race([cases.get('/' + tenant).entered.promise, aborted]);
    }));
  } finally {
    for (const tenant of tenants) cases.get('/' + tenant).release.resolve();
  }
  assert.deepEqual((await Promise.all(runs)).map(({attempt_state}) => attempt_state), ['completed', 'completed']);
});

test('launcher warm-up finishes while the origin health request is still held', {timeout: 10_000}, async (t) => {
  const origin = {entered: Promise.withResolvers(), release: Promise.withResolvers()};
  nextOrigin = origin;
  const abort = new Promise((_, reject) =>
    t.signal.addEventListener('abort', () => reject(t.signal.reason), {once: true}));
  const warm = host('slow-origin', 'warm', {engineOnly: true}, t.signal);
  try {
    await Promise.race([origin.entered.promise, abort]);
    const result = await warm;
    assert.ok(Number.isFinite(result.child_ms));
    assert.equal(result.origin_status, undefined);
  } finally {
    origin.release.resolve();
    nextOrigin = undefined;
  }
});

test('launcher warm-up rejects when warm state cannot commit after a successful ping', async () => {
  const tenant = 'warm-commit-failure';
  await assert.rejects(host(tenant, 'warm-commit-failure', {engineOnly: true}), /injected warm-state commit failure/);
  assert.equal((await host(tenant, 'warm', {engineOnly: true})).first_request, false);
});

test('a failed exercise retires the child before the next workflow can enter', async () => {
  const tenant = 'failed-exercise';
  assert.equal((await host(tenant, 'warm')).first_request, true);
  await assert.rejects(host(tenant, 'exercise'), /Tenant engine ping failed/);
  assert.deepEqual(await host(tenant, 'run', route(tenant)),
    {needsWarm: true, attempt_state: 'not_started'});
  assert.equal((await host(tenant, 'warm')).first_request, true);
  assert.equal((await host(tenant, 'run', route(tenant))).attempt_state, 'completed');
});

test('a ready workflow cannot enter while a non-exercise ping primes its child', {timeout: 10_000}, async (t) => {
  const tenant = 'held-ping';
  const gate = cases.get('/held-ping');
  await host(tenant, 'warm');
  await host(tenant, 'prime-held-ping');
  const warm = host(tenant, 'warm', {engineOnly: true}, t.signal);
  const abort = new Promise((_, reject) =>
    t.signal.addEventListener('abort', () => reject(t.signal.reason), {once: true}));
  try {
    await Promise.race([gate.entered.promise, abort]);
    const attempt = route(tenant, {scenario: 'blocked-by-priming',
      workflowDeadline: new Date(Date.now() + 1000).toISOString()});
    const result = await host(tenant, 'run', attempt, t.signal);
    assert.equal(result.terminal?.phase, 'attempt-deadline');
    assert.equal(result.terminal?.attempt_state, 'not_started');
    assert.ok(!origins.includes('https://approved.invalid/blocked-by-priming'));
  } finally {
    gate.release.resolve();
    await warm;
  }
});

test('TenantHost serves the pinned module and keeps missing modules inside the snapshot', async () => {
  await host('snapshot', 'warm');
  const result = await host('snapshot', 'run', route('snapshot', {scenario: 'snapshot'}));
  assert.deepEqual(result.output.result, {content: 'pinned = True\n', missing: 404});
  assert.ok(origins.every((url) => !url.includes('/api/sdk/modules')));
});

test('TenantHost refuses a redirect to an undeclared egress host', async () => {
  await host('redirect', 'warm');
  const result = await host('redirect', 'run', route('redirect', {scenario: 'redirect'}));
  assert.equal(result.output.result, 403);
  assert.ok(origins.includes('https://approved.invalid/redirect'));
  assert.ok(!origins.includes('https://undeclared.invalid/target'));
});

test('workerd egress spends the allowance only on matching one-label HTTPS destinations', async () => {
  await host('wildcard', 'warm');
  const result = await host('wildcard', 'run', route('wildcard', {
    scenario: 'wildcard', allowance: 1, httpsHosts: ['*.halopsa.com'],
  }));
  assert.deepEqual(result.output.result, [403, 403, 403, 403, 200, 429]);
  assert.deepEqual(origins.filter((url) => url.endsWith('/wildcard')), ['https://acme.halopsa.com/wildcard']);
});

test('a null workflow deadline permits the attempt without an expiry timer', async () => {
  await host('no-deadline', 'warm');
  const result = await host('no-deadline', 'run', route('no-deadline', {workflowDeadline: null}));
  assert.equal(result.attempt_state, 'completed');
  assert.equal(result.output.result, 200);
});

test('a stop recorded before the run prevents dispatch', async () => {
  await host('pre-stop', 'warm');
  await host('pre-stop', 'stop', 'pre-stop');
  const result = await host('pre-stop', 'run', route('pre-stop', {scenario: 'before-entry'}));
  assert.deepEqual(result.terminal, {phase: 'attempt-stopped', attempt_state: 'not_started', api_calls: []});
  assert.ok(!origins.includes('https://approved.invalid/before-entry'));
});

test('stopping a queued route settles it before an unbounded predecessor finishes', {timeout: 10_000}, async (t) => {
  const tenant = 'queued-stop';
  const {entered, release} = cases.get('/queued-block');
  await host(tenant, 'warm');
  const first = host(tenant, 'run', route(tenant, {routeId: 'active', scenario: 'queued-block', workflowDeadline: null}));
  try {
    await entered.promise;
    const queued = host(tenant, 'run', route(tenant, {routeId: 'queued', workflowDeadline: null}), t.signal);
    await host(tenant, 'wait', 'queued', t.signal);
    await host(tenant, 'stop', 'queued', t.signal);
    assert.equal((await queued).terminal.attempt_state, 'not_started');
  } finally {
    release.resolve();
    await first;
  }
});

test('stopping an active route revokes its egress before retirement finishes', {timeout: 10_000}, async () => {
  const tenant = 'active-stop';
  const {entered, release} = cases.get('/active-block');
  await host(tenant, 'warm');
  const running = host(tenant, 'run', route(tenant, {scenario: 'active-block', workflowDeadline: null}));
  try {
    await entered.promise;
    assert.equal(await host(tenant, 'stop-egress', tenant), 403);
    assert.equal((await running).terminal.attempt_state, 'uncertain');
    assert.ok(!origins.includes('https://approved.invalid/after-stop'));
  } finally {
    release.resolve();
  }
});

test('TenantHost refuses and records an outbound request past the subrequest allowance', async () => {
  await host('budget', 'warm');
  const control = await host('budget', 'run', route('budget', {routeId: 'budget-control', scenario: 'budget', allowance: 2}));
  assert.deepEqual(control.output.result, [200, 200]);
  const beforeCount = origins.length;
  const limited = await host('budget', 'run', route('budget', {scenario: 'budget', allowance: 1}));
  assert.deepEqual(limited.output.result, [200, 429]);
  assert.equal(origins.slice(beforeCount).filter((url) => url.endsWith('/count')).length, 1);
  assert.deepEqual(limited.timings.api_calls.map(({status, denied}) => [status, denied ?? null]),
    [[200, null], [429, 'subrequest allowance exhausted']]);
});

for (const [scenario, phase] of [['cpu-limit', 'cpu_limit'], ['memory-limit', 'memory_limit']]) {
  test(`TenantHost forwards only the ${phase} classification across workerd RPC`, async () => {
    await host(scenario, 'warm');
    // Inject the platform's error text through the real WorkerLoader/RPC path.
    const result = await host(scenario, 'run', route(scenario, {scenario}));
    assert.deepEqual(result, {terminal: {phase, attempt_state: 'uncertain'}});
    const next = await host(scenario, 'run', route(scenario, {routeId: `${scenario}-next`}));
    assert.deepEqual(next, {needsWarm: true, attempt_state: 'not_started'});
  });
}
