import assert from 'node:assert/strict';
import test from 'node:test';
import {summaryRunner} from './summary-provider.mjs';

const endpoint = 'https://provider.invalid/v1';
const context = {endpoint, body: {
  model: 'stub', store: false, stream: false,
  messages: [{role: 'system', content: 'summary'}, {role: 'user', content: 'run'}],
}};
const runner = summaryRunner({
  BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT: endpoint,
  BIFROST_CLOUDFLARE_SUMMARY_KEY: 'worker-test-key',
});

function run(payload) {
  return runner({deadline: new Date(Date.now() + 1000).toISOString()}, payload, new AbortController().signal);
}

test('a mismatched endpoint is refused before any provider request', async (t) => {
  const fetch = t.mock.method(globalThis, 'fetch');
  assert.equal((await run({...context, endpoint: 'https://different.invalid/v1'})).attempt_state, 'not_started');
  assert.equal(fetch.mock.callCount(), 0);
});

test('a redirect is terminal and never followed with the provider secret', async (t) => {
  const fetch = t.mock.method(globalThis, 'fetch', async (url, request) => {
    assert.equal(url, endpoint + '/chat/completions');
    assert.equal(request.redirect, 'manual');
    assert.equal(request.headers.Authorization, 'Bearer worker-test-key');
    return Response.redirect('https://different.invalid/v1');
  });
  const outcome = await run(context);
  assert.deepEqual(outcome, {
    attempt_state: 'completed', result: {success: false, error_type: 'SummaryProviderRejected'},
  });
  assert.equal(fetch.mock.callCount(), 1);
});

test('network failure after dispatch is uncertain and the error is redacted', async (t) => {
  t.mock.method(globalThis, 'fetch', async () => { throw new Error('worker-test-key'); });
  assert.deepEqual(await run(context), {
    attempt_state: 'uncertain', result: {success: false, error_type: 'SummaryInterrupted'},
  });
});
