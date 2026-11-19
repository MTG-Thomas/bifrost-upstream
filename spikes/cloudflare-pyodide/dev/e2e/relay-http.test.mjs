import assert from 'node:assert/strict';
import {once} from 'node:events';
import {createServer, request as httpRequest} from 'node:http';
import {text} from 'node:stream/consumers';
import {test} from 'node:test';
import {hostedRelayPath, relayHttp} from './relay-http.mjs';

async function listen(t, handle) {
  const server = createServer(handle).listen(0, '127.0.0.1');
  t.after(() => server.close());
  await once(server, 'listening');
  return 'http://127.0.0.1:' + server.address().port;
}

test('allows the runtime log write and refuses log reads and peer routes', () => {
  assert.ok(hostedRelayPath('POST', '/api/sdk/runtime/logs'));
  for (const path of ['/api/sdk/runtime/logs/other', '/api/sdk/runtime/service-control',
    '/api/workflows/execute', '/auth/login']) {
    assert.equal(hostedRelayPath('POST', path), false);
  }
  assert.equal(hostedRelayPath('GET', '/api/sdk/runtime/logs'), false);
});

test('hosted qualification adds the model stub and the Linux fixture module reads', () => {
  assert.equal(hostedRelayPath('POST', '/__fixture/v1/chat/completions'), false);
  assert.equal(hostedRelayPath('POST', '/__fixture/v1/chat/completions', true), true);
  assert.equal(hostedRelayPath('GET', '/api/sdk/modules/workflows/phase6_linux_only.py', true), true);
  assert.equal(hostedRelayPath('GET', '/api/sdk/modules-resolve?name=packaging', true), true);
  assert.equal(hostedRelayPath('GET', '/api/sdk/modules/workflows/another.py', true), false);
  assert.equal(hostedRelayPath('POST', '/api/sdk/modules-resolve', true), false);
  assert.equal(hostedRelayPath('POST', '/auth/login', true), false);
});

test('relays a content-length SDK POST and preserves the API response', async (t) => {
  const api = await listen(t, async (request, response) => {
    assert.equal(request.url, '/api/sdk/tables/list');
    assert.equal(request.headers.authorization, 'Bearer test');
    assert.equal(request.headers['x-hop'], undefined);
    assert.equal(await text(request), '{}');
    response.writeHead(401, {'content-type': 'application/json'}).end('{"detail":"revoked"}');
  });
  const relay = await listen(t, async (request, response) => {
    const upstream = await relayHttp(request, api + request.url);
    response.writeHead(upstream.statusCode).end(await text(upstream));
  });
  const response = await new Promise((resolve, reject) => {
    const client = httpRequest(relay + '/api/sdk/tables/list', {
      method: 'POST',
      headers: {Authorization: 'Bearer test', Connection: 'x-hop', 'X-Hop': 'discard', 'Content-Length': 2},
    }, resolve);
    client.on('error', reject);
    client.end('{}');
  });
  assert.equal(response.statusCode, 401);
  assert.deepEqual(JSON.parse(await text(response)), {detail: 'revoked'});
});
