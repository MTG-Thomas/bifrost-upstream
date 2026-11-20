import assert from 'node:assert/strict';
import test from 'node:test';
import {expiryCapacityRelay, OVERFLOW_MEMORY_MIB, vmHoldMemoryMiB} from './containers-local.mjs';

test('VM hold leaves room for working-set growth while blocking the update stage', () => {
  const mib = 2 ** 20;
  const measured = 586376806;
  const hold = vmHoldMemoryMiB(measured) * mib;
  assert.ok(hold < measured - 64 * mib);
  assert.ok(measured + 64 * mib - hold < OVERFLOW_MEMORY_MIB * mib);
});

test('expiry relay forwards scoped reservations until independent observation begins', async () => {
  const outbound = [];
  const relay = expiryCapacityRelay('https://capacity.example.invalid', async (target, options) => {
    outbound.push({target: String(target), ...options});
    return Response.json({state: 'prepared'});
  });
  const path = 'https://local.invalid/v1/workers/incarnation';
  const headers = {authorization: 'Bearer fixture', 'content-type': 'application/json',
    'x-bifrost-capacity-scope': 'containers', host: 'local.invalid'};
  const prepared = await relay.fetch(new Request(path, {method: 'PUT', headers, body: '{}'}));
  assert.deepEqual(await prepared.json(), {state: 'prepared'});
  assert.equal(outbound[0].target, 'https://capacity.example.invalid/v1/workers/incarnation');
  assert.equal(outbound[0].headers.get('x-bifrost-capacity-scope'), 'containers');
  assert.equal(outbound[0].headers.has('host'), false);
  assert.equal(outbound[0].redirect, 'manual');

  relay.stopForwarding();
  assert.equal((await relay.fetch(new Request(path, {headers}))).status, 503);
  assert.equal((await relay.fetch(new Request(path, {method: 'DELETE', headers}))).status, 503);
  assert.equal(outbound.length, 1);
  assert.equal(relay.deletesForwarded, 0);
});
