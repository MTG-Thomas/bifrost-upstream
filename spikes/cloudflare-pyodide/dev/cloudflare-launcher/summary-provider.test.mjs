import assert from 'node:assert/strict';
import test from 'node:test';
import worker from './summary-provider.mjs';

const endpoint = 'https://provider.invalid/v1';
const context = {endpoint, api_key: 'worker-test-key', provider: 'openai', remaining_ms: 1000, body: {
  model: 'stub', store: false, stream: false,
  messages: [{role: 'system', content: 'summary'}, {role: 'user', content: 'run'}],
}};
async function run(payload, base = endpoint) {
  const response = await worker.fetch(new Request('https://summary.internal/complete', {
    method: 'POST', body: JSON.stringify(payload),
  }), {BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT: base});
  return response.json();
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


test('invalid provider configuration disables the summary call', async (t) => {
  const fetch = t.mock.method(globalThis, 'fetch');
  assert.equal((await run(context, 'not a URL')).attempt_state, 'not_started');
  assert.equal(fetch.mock.callCount(), 0);
});

test('an upstream echo of the bearer key cannot reach the terminal journal', async (t) => {
  t.mock.method(globalThis, 'fetch', async () => Response.json({
    model: 'stub', choices: [{message: {content: context.api_key}}],
    usage: {prompt_tokens: 3, completion_tokens: 1},
  }));
  const outcome = await run(context);
  assert.equal(outcome.result.success, false);
  assert.ok(!JSON.stringify(outcome).includes(context.api_key));
});
