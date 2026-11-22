import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {once} from 'node:events';
import {mkdtemp, rm, writeFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import test from 'node:test';
import {fileURLToPath} from 'node:url';

const require = createRequire(new URL('../tenant-workers/package.json', import.meta.url));
const {WebSocket, WebSocketServer} = require('ws');

async function listen(server, host, port = 0) {
  server.listen(port, host);
  await once(server, 'listening');
  return server.address().port;
}

test('hosted ingress relay keeps established WebSockets on their original upstream', {timeout: 15000}, async () => {
  const root = await mkdtemp(join(tmpdir(), 'bifrost-upgrade-relay-'));
  const token = randomBytes(32).toString('hex');
  const controlListener = createServer();
  const relayListener = createServer();
  const providerPort = await listen(controlListener, '127.0.0.1');
  const relayPort = await listen(relayListener, '127.0.0.1');
  controlListener.close();
  relayListener.close();
  const upstreams = [];
  const clients = [];
  let relay;
  try {
    for (const [address, label] of [['127.0.0.2', 'original'], ['127.0.0.3', 'successor']]) {
      const server = createServer();
      const sockets = new WebSocketServer({server});
      sockets.on('connection', (socket) => socket.on('message', () => socket.send(label)));
      upstreams.push({server, sockets});
      await listen(server, address, 8001);
    }
    const config = join(root, 'config.json');
    await writeFile(config, JSON.stringify({
      address: '127.0.0.1', subnet: '127.0.0.0/8', providerPort, relayPort,
      relayToken: token, ready: join(root, 'ready'),
    }), {mode: 0o600});
    relay = spawn(process.execPath, [fileURLToPath(new URL('./combined-provider.mjs', import.meta.url)),
      config, '--hosted-ingress-relay'], {stdio: 'ignore'});
    const control = (method, path, body, access = token) => fetch(`http://127.0.0.1:${providerPort}${path}`, {
      method, headers: {Authorization: `Bearer ${access}`, 'Content-Type': 'application/json'},
      ...(body === undefined ? {} : {body: JSON.stringify(body)}),
    });
    let ready = false;
    for (let attempt = 0; attempt < 75 && !ready; attempt++) {
      const response = await control('GET', '/local/evidence').catch(() => null);
      ready = response?.status === 200;
      if (!ready) await delay(100);
    }
    assert.ok(ready, 'the local relay did not start');
    assert.equal((await control('GET', '/local/evidence', undefined, 'other')).status, 403);
    assert.equal((await control('POST', '/local/interrupt', {})).status, 404);
    const target = (host) => control('POST', '/local/ingress', {host});
    assert.equal((await target('127.0.0.2')).status, 200);
    async function connect() {
      const client = new WebSocket(`ws://127.0.0.1:${relayPort}/api/worker-control`);
      clients.push(client);
      await once(client, 'open');
      return client;
    }
    async function exchange(client) {
      const answer = once(client, 'message');
      client.send('opaque fixture frame');
      return String((await answer)[0]);
    }
    const original = await connect();
    assert.equal(await exchange(original), 'original');
    assert.equal((await target('127.0.0.3')).status, 200);
    const successor = await connect();
    assert.equal(await exchange(successor), 'successor');
    assert.equal(await exchange(original), 'original');
  } finally {
    for (const client of clients) client.terminate();
    if (relay && relay.exitCode === null) {
      relay.kill();
      await once(relay, 'exit');
    }
    for (const {server, sockets} of upstreams) {
      for (const client of sockets.clients) client.terminate();
      sockets.close();
      server.close();
    }
    await rm(root, {recursive: true, force: true});
  }
});
