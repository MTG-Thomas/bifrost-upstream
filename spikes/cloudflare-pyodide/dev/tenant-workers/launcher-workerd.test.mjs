import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {test} from 'node:test';
import {signHello} from '../cloudflare-launcher/protocol.mjs';

// Use the same workerd dependency as the pinned Wrangler harness.
const {Miniflare} = createRequire(import.meta.resolve('wrangler'))('miniflare');
const secret = 'a'.repeat(64);
const incarnation = '0e9d8c7b-6a59-4847-b6a5-948372615041';
const launcher = '6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f';
const compatibilityDate = '2026-10-30';
const path = (name) => fileURLToPath(new URL(name, import.meta.url));

test('workerd launcher alarm exchanges hello and inventory over an outbound Upgrade', {timeout: 15_000}, async (t) => {
  const mf = new Miniflare({workers: [{
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
          'cloudflare-launcher/protocol.mjs', 'tenant-workers/launcher-host.mjs', 'tenant-workers/tenant-dispatch.mjs',
          'tenant-workers/tenant-registry.mjs'].map((name) =>
          [name, {type: 'esm', contents: readFileSync(path(`../${name}`), 'utf8')}])),
      }},
      env: Object.fromEntries(Object.entries({
        BIFROST_CLOUDFLARE_LAUNCHER_ENABLED: 'true', BIFROST_WORKER_CONTROL_SECRET: secret,
        BIFROST_WORKER_INCARNATION_ID: incarnation, BIFROST_WORKER_LAUNCHER_ID: launcher,
        BIFROST_WORKER_RESOURCE_UID: 'test:do', BIFROST_WORKER_VERSION: 'test-build',
        API_ORIGIN: 'https://test-api.invalid',
      }).map(([key, value]) => [key, {type: 'text', value}])),
    },
  }, {
    config: {name: 'api', compatibilityDate,
      exports: {ScriptedPeer: {type: 'durable-object', storage: 'sqlite'}},
      manifest: {mainModule: 'api.mjs', modulesRoot: path('.'), modules: {'api.mjs': {type: 'esm', contents: `
      import {DurableObject} from 'cloudflare:workers';
      export class ScriptedPeer extends DurableObject {
        fetch(request) {
          const path = new URL(request.url).pathname;
          if (request.headers.get('upgrade') !== 'websocket' ||
              !['/observe', '/api/worker-control'].includes(path)) {
            return new Response(null, {status: 400});
          }
          const [client, server] = Object.values(new WebSocketPair());
          server.accept();
          if (path === '/observe') {
            this.observer = server;
          } else {
            let hello;
            server.addEventListener('message', ({data}) => {
              if (!hello) {
                hello = JSON.parse(data);
                server.send(JSON.stringify({event: 'welcome', session_generation: 1}));
              } else {
                this.observer.send(JSON.stringify({hello, inventory: JSON.parse(data)}));
                server.close(1000);
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
  }]});
  const {signal} = t;
  t.after(() => mf.dispose());
  const observer = await (await mf.getWorker('api')).fetch('https://test-api.invalid/observe', {
    headers: {Upgrade: 'websocket'},
  });
  assert.equal(observer.status, 101);
  const socket = observer.webSocket;
  const received = new Promise((resolve, reject) => {
    signal.addEventListener('abort', () => reject(signal.reason), {once: true});
    socket.addEventListener('message', ({data}) => resolve(JSON.parse(data)), {once: true});
  });
  socket.accept();
  const launched = await (await mf.getWorker('launcher')).fetch('https://launcher.invalid/');
  assert.equal(launched.status, 204);
  const {hello, inventory} = await received;
  socket.close();
  assert.equal(hello.hello.incarnation_id, incarnation);
  assert.equal(hello.hello.hello_sequence, 1);
  assert.equal(hello.version, 'test-build');
  assert.deepEqual(await signHello(secret, hello.hello), hello.hello);
  assert.deepEqual(inventory, {kind: 'event', sequence: 1, session_generation: 1,
    payload: {event: 'inventory', inventory: {version: 1, routes: [], terminal_routes: []}}});
});
