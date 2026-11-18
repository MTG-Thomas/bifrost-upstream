import {test} from 'node:test';
import assert from 'node:assert/strict';
import {AttemptCredentials, REFRESH_BODY_LIMIT, isRefreshChallenge, refreshReply,
  refreshRequestHandle} from './attempt-credentials.mjs';

const later = () => Date.now() + 60_000;
const CHALLENGE = 'Bearer error="invalid_token", error_description="refresh_required"';

test('only the API refresh challenge counts, exactly as the SDK reads it', () => {
  assert.equal(isRefreshChallenge(new Response('', {status: 401, headers: {'WWW-Authenticate': CHALLENGE}})), true);
  // A handler-level 401 (e.g. an AI provider rejecting its key) is not one.
  assert.equal(isRefreshChallenge(new Response('', {status: 401, headers: {'WWW-Authenticate': 'Bearer'}})), false);
  assert.equal(isRefreshChallenge(new Response('', {status: 401})), false);
  assert.equal(isRefreshChallenge(new Response('', {status: 403, headers: {'WWW-Authenticate': CHALLENGE}})), false);
});

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

test('no upstream refresh unless the token in use was challenged', async () => {
  let calls = 0;
  const creds = new AttemptCredentials({refresh: async () => { calls++; return {access_token: 'a2', refresh_token: 'r2'}; }});
  creds.open('h', {token: 'a1', refreshToken: 'r1', expiresAt: later()});
  assert.equal(await creds.refresh('h'), 'current');
  creds.noteChallenge('h', 'stale-token');
  assert.equal(await creds.refresh('h'), 'current', 'a challenge for another token does not count');
  assert.equal(calls, 0);
  creds.noteChallenge('h', 'a1');
  assert.equal(await creds.refresh('h'), 'refreshed');
  assert.equal(creds.token('h'), 'a2');
  assert.equal(await creds.refresh('h'), 'current', 'the challenge is spent');
  assert.equal(calls, 1);
});

test('concurrent refreshes are single-flight and rotate both tokens', async () => {
  const sent = [];
  let finish;
  const creds = new AttemptCredentials({refresh: (refreshToken) => {
    sent.push(refreshToken);
    return new Promise((resolve) => { finish = resolve; });
  }});
  creds.open('h', {token: 'a1', refreshToken: 'r1', expiresAt: later()});
  creds.noteChallenge('h', 'a1');
  const both = Promise.all([creds.refresh('h'), creds.refresh('h')]);
  await new Promise((r) => setImmediate(r));
  finish({access_token: 'a2', refresh_token: 'r2'});
  assert.deepEqual(await both, ['refreshed', 'refreshed']);
  assert.deepEqual(sent, ['r1'], 'a rotating refresh token is sent once');
  creds.noteChallenge('h', 'a2');
  const third = creds.refresh('h');
  await new Promise((r) => setImmediate(r));
  finish({access_token: 'a3', refresh_token: 'r3'});
  assert.equal(await third, 'refreshed');
  assert.deepEqual(sent, ['r1', 'r2']);
});

test('a refused refresh is never resent; no refresh token means refused', async () => {
  let calls = 0;
  const creds = new AttemptCredentials({refresh: async () => { calls++; return null; }});
  creds.open('h', {token: 'a1', refreshToken: 'r1', expiresAt: later()});
  creds.noteChallenge('h', 'a1');
  assert.equal(await creds.refresh('h'), 'refused');
  assert.equal(await creds.refresh('h'), 'refused');
  assert.equal(calls, 1);
  creds.open('legacy', {token: 'a', expiresAt: later()});
  creds.noteChallenge('legacy', 'a');
  assert.equal(await creds.refresh('legacy'), 'refused');
});

test('an attempt that ends or expires during a refresh gets nothing', async () => {
  let now = 0;
  let finish;
  const creds = new AttemptCredentials({now: () => now,
    refresh: () => new Promise((resolve) => { finish = resolve; })});
  creds.open('closed', {token: 'a1', refreshToken: 'r1', expiresAt: 100});
  creds.noteChallenge('closed', 'a1');
  const closing = creds.refresh('closed');
  await new Promise((r) => setImmediate(r));
  creds.close('closed');
  finish({access_token: 'a2', refresh_token: 'r2'});
  assert.equal(await closing, 'gone');

  creds.open('expiring', {token: 'b1', refreshToken: 'r1', expiresAt: 100});
  creds.noteChallenge('expiring', 'b1');
  const expiring = creds.refresh('expiring');
  await new Promise((r) => setImmediate(r));
  now = 100;
  finish({access_token: 'b2', refresh_token: 'r2'});
  assert.equal(await expiring, 'gone');
  assert.equal(creds.token('expiring'), null);
});

test('the refresh request is read within its bound, and the reply carries only the handle', async () => {
  const request = (body) => new Request('https://test-api.invalid/auth/refresh', {method: 'POST', body});
  assert.equal(await refreshRequestHandle(request(JSON.stringify({refresh_token: 'run_x'}))), 'run_x');
  assert.equal(await refreshRequestHandle(request('{"refresh_token": 7}')), null);
  assert.equal(await refreshRequestHandle(request('not json')), null);
  assert.equal(await refreshRequestHandle(request('x'.repeat(REFRESH_BODY_LIMIT + 1))), null);
  const ok = await refreshReply('refreshed', 'run_x').json();
  assert.deepEqual(ok, {access_token: 'run_x', refresh_token: 'run_x', token_type: 'bearer'});
  const refused = refreshReply('refused', 'run_x');
  assert.equal(refused.status, 401);
  assert.equal(isRefreshChallenge(refused), false, 'the SDK must not loop on a refused refresh');
});

test('a temporarily unavailable refresh keeps the refresh token for a later try', async () => {
  const answers = ['unavailable', {access_token: 'a2', refresh_token: 'r2'}];
  const sent = [];
  const creds = new AttemptCredentials({refresh: async (refreshToken) => { sent.push(refreshToken); return answers.shift(); }});
  creds.open('h', {token: 'a1', refreshToken: 'r1', expiresAt: later()});
  creds.noteChallenge('h', 'a1');
  assert.equal(await creds.refresh('h'), 'unavailable');
  assert.equal(await creds.refresh('h'), 'refreshed', 'still challenged, still refreshable');
  assert.deepEqual(sent, ['r1', 'r1']);
  const reply = refreshReply('unavailable', 'h');
  assert.equal(reply.status, 503, 'not a 401: the SDK must not record the handle as rejected');
});

test('an explicit refusal ends refresh for the attempt', async () => {
  let calls = 0;
  const creds = new AttemptCredentials({refresh: async () => { calls++; return 'refused'; }});
  creds.open('h', {token: 'a1', refreshToken: 'r1', expiresAt: later()});
  creds.noteChallenge('h', 'a1');
  assert.equal(await creds.refresh('h'), 'refused');
  assert.equal(await creds.refresh('h'), 'refused');
  assert.equal(calls, 1);
});
