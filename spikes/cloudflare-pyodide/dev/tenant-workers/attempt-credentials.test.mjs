import {test} from 'node:test';
import assert from 'node:assert/strict';
import {AttemptCredentials, REPLAY_BODY_LIMIT, forwardWithRefresh} from './attempt-credentials.mjs';

const later = () => Date.now() + 60_000;

test('handles resolve only while the attempt is live', () => {
  let now = 1_000;
  const creds = new AttemptCredentials({refresh: async () => null, now: () => now});
  creds.open('h', {token: 'a1', expiresAt: 2_000});
  assert.equal(creds.token('h'), 'a1');
  now = 2_000;
  assert.equal(creds.token('h'), null, 'expires with the attempt deadline');
  creds.open('h2', {token: 'a2', expiresAt: 9_000});
  creds.close('h2');
  assert.equal(creds.token('h2'), null);
});

test('concurrent refreshes are single-flight and rotate both tokens', async () => {
  const sent = [];
  let finish;
  const creds = new AttemptCredentials({refresh: (refreshToken) => {
    sent.push(refreshToken);
    return new Promise((resolve) => { finish = resolve; });
  }});
  creds.open('h', {token: 'a1', refreshToken: 'r1', expiresAt: later()});
  const both = Promise.all([creds.refreshed('h', 'a1'), creds.refreshed('h', 'a1')]);
  await new Promise((r) => setImmediate(r));
  finish({access_token: 'a2', refresh_token: 'r2'});
  assert.deepEqual(await both, ['a2', 'a2']);
  assert.deepEqual(sent, ['r1'], 'a rotated refresh token is sent once');
  assert.equal(await creds.refreshed('h', 'a1'), 'a2', 'a stale observer gets the current token');
  creds.refreshed('h', 'a2');
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(sent, ['r1', 'r2']);
});

test('a refused refresh is never resent, and no refresh token means no refresh', async () => {
  let calls = 0;
  const creds = new AttemptCredentials({refresh: async () => { calls++; return null; }});
  creds.open('h', {token: 'a1', refreshToken: 'r1', expiresAt: later()});
  assert.equal(await creds.refreshed('h', 'a1'), null);
  assert.equal(await creds.refreshed('h', 'a1'), null);
  assert.equal(calls, 1);
  creds.open('legacy', {token: 'a', expiresAt: later()});
  assert.equal(await creds.refreshed('legacy', 'a'), null);
});

test('an attempt closed during a refresh never receives the new token', async () => {
  let finish;
  const creds = new AttemptCredentials({refresh: () => new Promise((resolve) => { finish = resolve; })});
  creds.open('h', {token: 'a1', refreshToken: 'r1', expiresAt: later()});
  const pending = creds.refreshed('h', 'a1');
  await new Promise((r) => setImmediate(r));
  creds.close('h');
  finish({access_token: 'a2', refresh_token: 'r2'});
  assert.equal(await pending, null);
});

function recordingFetch(statuses) {
  const seen = [];
  return {seen, fetchFn: async (request) => {
    seen.push({auth: request.headers.get('authorization'), cookie: request.headers.get('cookie'),
      body: request.body ? await request.text() : null});
    return new Response('x', {status: statuses[seen.length - 1]});
  }};
}

test('a 401 is replayed once with the refreshed token, body intact', async () => {
  const {seen, fetchFn} = recordingFetch([401, 200]);
  const body = JSON.stringify({key: 'v'});
  const request = new Request('https://test-api.invalid/api/config', {method: 'POST', body,
    headers: {'content-length': String(body.length), cookie: 'c=1', authorization: 'Bearer run_x'}});
  const out = await forwardWithRefresh({request, target: 'https://api.example/api/config', token: 'a1',
    fetchFn, refreshed: async (observed) => (observed === 'a1' ? 'a2' : null)});
  assert.equal(out.response.status, 200);
  assert.equal(out.refreshed, true);
  assert.deepEqual(seen.map((s) => s.auth), ['Bearer a1', 'Bearer a2']);
  assert.deepEqual(seen.map((s) => s.body), [body, body]);
  assert.ok(seen.every((s) => s.cookie === null), 'cookies never reach the API');
});

test('no replay without a newer token, or for an unsized or oversized body', async () => {
  const noNewer = recordingFetch([401]);
  const out = await forwardWithRefresh({request: new Request('https://v.invalid/x'), target: 'https://a/x',
    token: 'a1', fetchFn: noNewer.fetchFn, refreshed: async () => null});
  assert.equal(out.response.status, 401);
  assert.equal(noNewer.seen.length, 1);

  for (const headers of [{}, {'content-length': String(REPLAY_BODY_LIMIT + 1)}]) {
    const streamed = recordingFetch([401]);
    const request = new Request('https://v.invalid/upload', {method: 'PUT', body: 'payload', headers});
    if (!headers['content-length']) request.headers.delete('content-length');
    let asked = false;
    await forwardWithRefresh({request, target: 'https://a/upload', token: 'a1', fetchFn: streamed.fetchFn,
      refreshed: async () => { asked = true; return 'a2'; }});
    assert.equal(streamed.seen.length, 1, 'a non-replayable body is sent exactly once');
    assert.equal(asked, false);
  }
});
