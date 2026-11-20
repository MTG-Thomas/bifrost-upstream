import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {apiTarget, attemptPayload, SDK_ROUTE_POLICY} from './policy.mjs';

const id = '11111111-1111-4111-8111-111111111111';

test('the staged gateway policy matches the SDK analyzer policy', () => {
  const canonical = new URL('../../../../api/bifrost/cloudflare_sdk_routes.json', import.meta.url);
  assert.deepEqual(SDK_ROUTE_POLICY, JSON.parse(readFileSync(canonical, 'utf8')));
});
test('runtime API capability retains a fixed destination and query', () => {
  assert.equal(apiTarget('https://test-api.invalid/api/sdk/modules/a.py?hash=1', 'https://dev.invalid', {method: 'GET'}).href,
    'https://dev.invalid/api/sdk/modules/a.py?hash=1');
});
test('missing request method is refused in both scopes', () => {
  const url = 'https://test-api.invalid/api/tables';
  assert.throws(() => apiTarget(url, 'https://dev.invalid'), /API capability denied/);
  assert.throws(() => apiTarget(url, 'https://dev.invalid', {scope: 'all'}), /API capability denied/);
});
test('encoded email caret remains in the user identifier', () => {
  const url = 'https://test-api.invalid/api/users/a^b@example.test';
  assert.equal(apiTarget(url, 'https://dev.invalid', {method: 'GET'}).pathname, '/api/users/a%5Eb@example.test');
});
test('supervisor public metadata and SDK transport cannot be selected by the request', () => {
  const body = {execution_id: 'run', context: {parameters: {n: 7}},
    api_url: 'https://attacker.invalid', public_url: 'https://attacker.invalid'};
  assert.deepEqual(attemptPayload(body, 'https://platform.invalid/'), {
    action: 'execute', execution_id: 'run', context: body.context,
    api_url: 'https://test-api.invalid', public_url: 'https://platform.invalid',
  });
});
test('invalid trusted metadata is rejected before runtime construction', () => {
  for (const url of ['http://platform.invalid', 'https://user:secret@platform.invalid',
    'https://platform.invalid/path', 'https://platform.invalid?token=secret', 'https://platform.invalid/#x']) {
    assert.throws(() => attemptPayload({}, url), TypeError);
  }
});
test('agent run control plane rides the same authenticated bridge', () => {
  assert.equal(apiTarget('https://test-api.invalid/api/agent-runs/enqueue', 'https://dev.invalid', {method: 'GET'}).href,
    'https://dev.invalid/api/agent-runs/enqueue');
  assert.equal(apiTarget('https://test-api.invalid/api/agent-runs/0192dead-beef', 'https://dev.invalid', {method: 'GET'}).href,
    'https://dev.invalid/api/agent-runs/0192dead-beef');
});
test('SDK resource roots used by real workspace workflows are reachable', () => {
  for (const path of ['/api/organizations', '/api/organizations/0192dead-beef', '/api/configs/get',
    '/api/files/write', '/api/integrations', '/api/tables', '/api/tables/docs-file-org-grants/documents/x',
    '/api/events/emit', '/api/forms/submit', '/api/agents', '/api/agent-runs', '/api/sdk/modules/a.py',
    '/api/roles', `/api/roles/${id}/users`, '/api/users', `/api/users/${id}`, '/api/users/person@example.test',
    `/api/platform-jobs/${id}`]) {
    assert.equal(apiTarget(`https://test-api.invalid${path}`, 'https://dev.invalid', {method: 'GET'}).href,
      `https://dev.invalid${path}`);
  }
});
test('identity endpoints admit SDK method and path shapes, and jobs admit polling only', () => {
  for (const [path, method] of [
    ['/api/users', 'POST'], [`/api/users/${id}`, 'PATCH'], [`/api/roles/${id}/users`, 'POST'], ['/auth/refresh', 'POST'],
  ]) {
    assert.equal(apiTarget(`https://test-api.invalid${path}`, 'https://dev.invalid', {method}).pathname, path);
  }
  for (const [path, method] of [
    ['/api/users/bulk', 'PATCH'], [`/api/users/${id}/mfa/reset`, 'POST'],
    [`/api/users/${id}/role-assignments`, 'PUT'], [`/api/roles/${id}/permissions`, 'GET'],
    ['/api/platform-jobs', 'GET'], [`/api/platform-jobs/${id}`, 'POST'], [`/api/platform-jobs/${id}/cancel`, 'POST'],
  ]) {
    assert.throws(() => apiTarget(`https://test-api.invalid${path}`, 'https://dev.invalid', {method}));
  }
});
test('external fetch, control endpoints and origin ambiguity are rejected before effects', () => {
  for (const url of ['https://other.invalid/api/sdk/config/get', 'https://test-api.invalid/auth/login',
    'https://test-api.invalid/api/workflows', 'https://test-api.invalid/api/executions',
    'https://test-api.invalid/api/tables-admin', 'https://test-api.invalid/api/organizations-admin',
    'https://test-api.invalid/api/roles-other', 'https://test-api.invalid/api/users-other',
    'https://test-api.invalid/api/agent-runs-other', 'https://test-api.invalid/api']) {
    assert.throws(() => apiTarget(url, 'https://dev.invalid', {method: 'GET'}));
  }
  for (const origin of ['http://dev.invalid', 'https://user:password@dev.invalid', 'https://dev.invalid/path', 'https://dev.invalid?x=1']) {
    assert.throws(() => apiTarget('https://test-api.invalid/api/tables', origin, {method: 'GET'}));
  }
});

test('lab scope admits every /api/ path but still only the virtual host', () => {
  assert.throws(() => apiTarget('https://test-api.invalid/api/workflows', 'https://dev.invalid', {method: 'GET'}),
    /API capability denied/, 'default scope keeps dispatch routes out');
  assert.equal(apiTarget('https://test-api.invalid/api/workflows?x=1', 'https://dev.invalid',
    {scope: 'all', method: 'GET'}).href, 'https://dev.invalid/api/workflows?x=1');
  assert.throws(() => apiTarget('https://test-api.invalid/admin', 'https://dev.invalid', {scope: 'all', method: 'GET'}),
    /API capability denied/);
  assert.throws(() => apiTarget('https://evil.invalid/api/workflows', 'https://dev.invalid', {scope: 'all', method: 'GET'}),
    /API capability denied/);
});
