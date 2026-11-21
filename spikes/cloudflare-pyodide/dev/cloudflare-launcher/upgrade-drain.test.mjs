import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {after, before, test} from 'node:test';
import {envelope} from './protocol.mjs';

const tenantRequire = createRequire(new URL('../tenant-workers/package.json', import.meta.url));
const {Miniflare} = createRequire(tenantRequire.resolve('wrangler'))('miniflare');
const files = ['cloudflare-launcher/launcher-core.mjs', 'cloudflare-launcher/protocol.mjs',
  'cloudflare-launcher/launcher-meter.mjs', 'executor/egress-hosts.mjs',
  'executor/egress-psl.generated.mjs', 'tenant-workers/storage-path.mjs'];
const modules = Object.fromEntries(files.map(name => [
  name, {type: 'esm', contents: readFileSync(new URL(`../${name}`, import.meta.url), 'utf8')},
]));
modules['fixture.mjs'] = {type: 'esm', contents: `
  import {DurableObject} from 'cloudflare:workers';
  import {LauncherCore} from './cloudflare-launcher/launcher-core.mjs';

  export class Fixture extends DurableObject {
    #entered = Promise.withResolvers();
    #release = Promise.withResolvers();
    #runs = 0;
    #socket;
    #core = new LauncherCore({
      identity: {incarnationId: '0e9d8c7b-6a59-4847-b6a5-948372615041',
        launcherId: '6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f',
        resourceUid: 'fixture', hostname: 'fixture', version: 'fixture'},
      secret: 'fixture-only-control-key',
      clock: Date.now,
      slots: 1,
      tenantSlots: true,
      storage: {
        get: key => this.ctx.storage.get(key),
        put: (key, value) => this.ctx.storage.put(key, value),
        delete: (...keys) => this.ctx.storage.delete(keys),
      },
      newId: () => crypto.randomUUID(),
      send: (_session, raw) => this.#socket.send(raw),
      runHost: async () => {
        ++this.#runs;
        this.#entered.resolve();
        await this.#release.promise;
        return {attempt_state: 'completed', result: {success: true, result: 42}};
      },
      background: task => this.ctx.waitUntil(task),
    });
    async fetch(request) {
      const path = new URL(request.url).pathname;
      if (path === '/entered') {
        await this.#entered.promise;
        return new Response();
      }
      if (path === '/complete') {
        this.#release.resolve();
        return new Response();
      }
      if (path === '/release') {
        await this.#core.release();
        return Response.json(this.#runs);
      }
      const {session, hello} = await this.#core.beginDial();
      const {0: client, 1: socket} = new WebSocketPair();
      this.#socket = socket;
      socket.accept();
      socket.addEventListener('message', event => this.ctx.waitUntil(this.#core.receive(session, event.data)));
      socket.addEventListener('close', event => this.ctx.waitUntil(this.#core.closed(session, event.code)));
      socket.send(hello);
      return new Response(null, {status: 101, webSocket: client});
    }
  }
  export default {fetch(request, _env, ctx) { return ctx.exports.Fixture.getByName('fixture').fetch(request); }};
`};

let mf;
let worker;

before(async () => {
  mf = new Miniflare({cf: false, workers: [{config: {
    name: 'upgrade-drain', compatibilityDate: '2026-10-30',
    exports: {Fixture: {type: 'durable-object', storage: 'sqlite'}},
    manifest: {mainModule: 'fixture.mjs', modules},
  }}]});
  worker = await mf.getWorker('upgrade-drain');
});

after(async () => mf?.dispose());

test('a held route finishes while draining and replays the same terminal after a lost ACK', {timeout: 8000}, async () => {
  let socket;
  try {
    const request = path => worker.fetch(`http://fixture.test${path}`);
    const attach = async generation => {
      const response = await worker.fetch('http://fixture.test/control', {headers: {Upgrade: 'websocket'}});
      assert.equal(response.status, 101);
      socket = response.webSocket;
      const stream = new TransformStream();
      const writer = stream.writable.getWriter();
      socket.addEventListener('message', event => void writer.write(JSON.parse(event.data)));
      socket.accept();
      const reader = stream.readable.getReader();
      const next = async () => (await reader.read()).value;
      await next(); // Signed hello.
      socket.send(JSON.stringify({event: 'welcome', session_generation: generation}));
      return next;
    };

    const first = await attach(1);
    assert.equal((await first()).payload.event, 'inventory');
    const execution = '0e9d8c7b-6a59-4847-b6a5-948372615045';
    const route = `workflow:${execution}:6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f`;
    const now = Date.now();
    const deadline = new Date(now + 5000).toISOString();
    const sourcePath = 'workflows/test.py';
    socket.send(JSON.stringify(envelope(1, 1, 'command', {id: 'run', action: 'workflow', payload: {
      route_id: route, execution_id: execution, started_at: new Date(now).toISOString(), workflow_deadline: deadline,
      engine_slot: 0,
      timeout_seconds: 5, memory_request_bytes: 8192,
      active_execution: {execution_id: execution, sync: true, org_id: null},
      context: {workflow_deadline: deadline, file_path: sourcePath, engine_token: 'fixture-token',
        cloudflare_workspace: {manifest: {[sourcePath]: 'a'.repeat(64)}, sources: {[sourcePath]: 'def run(): return 42'},
          packages: [], https_hosts: [], cpu_budget_ms: 10, subrequest_allowance: 5}},
    }})));
    assert.ok((await first()).payload.result?.runtime_handle);
    assert.equal((await request('/entered')).status, 200);
    // Closing admission leaves the current claim alive. Complete it without
    // ACKing the terminal on the first connection.
    assert.equal((await request('/complete')).status, 200);
    const terminal = (await first()).payload;
    assert.equal(terminal.attempt_state, 'completed');
    const closed = new Promise(resolve => socket.addEventListener('close', resolve, {once: true}));
    socket.close(1012, 'test connection loss');
    await closed;

    const second = await attach(2);
    assert.equal((await second()).payload.inventory.terminal_routes[0].route_id, route);
    assert.deepEqual((await second()).payload, terminal);
    socket.send(JSON.stringify(envelope(2, 1, 'ack', {event_id: terminal._event_id})));
    socket.send(JSON.stringify(envelope(2, 2, 'command', {id: 'ready', action: 'ready', payload: {}})));
    assert.equal((await second()).payload.reply, 'ready');
    assert.equal(await (await request('/release')).json(), 1);
  } finally {
    socket?.close();
  }
});
