import assert from 'node:assert/strict';
import {test} from 'node:test';
import {runLauncherWorkflow} from './launcher-host.mjs';

const START = Date.parse('2026-11-18T12:00:00Z');
const DEADLINE = '2026-11-18T12:00:05.123456+00:00';
const route = {workflow_deadline: DEADLINE, route_id: 'route', subject_id: 'subject',
  active_execution: {org_id: 'org'}};

test('warm-up retries use the same API deadline and approved snapshot', async () => {
  const calls = [];
  const context = {cloudflare_workspace: {sources: {'wf.py': 'pinned'}}, workflow_deadline: DEADLINE};
  const result = await runLauncherWorkflow(() => ({
    run: async (attempt) => {
      calls.push(attempt);
      return calls.length === 1 ? {needsWarm: true} : {output: {status: 'Success', result: 42}};
    },
    warm: async () => ({}),
  }), {route, context, signal: new AbortController().signal, now: () => START});
  assert.equal(calls.length, 2);
  assert.equal(calls[0].workflowDeadline, DEADLINE);
  assert.equal(calls[0].context, context);
  assert.deepEqual(calls[0], calls[1]);
  assert.deepEqual(result, {attempt_state: 'completed', result: {success: true, status: 'Success', result: 42}});
});

test('deadline during warm-up never dispatches after the needsWarm refusal', async () => {
  let now = START;
  let calls = 0;
  const result = await runLauncherWorkflow(() => ({
    run: async () => { calls++; return {needsWarm: true}; },
    warm: async () => { now = START + 6000; return {}; },
  }), {route, context: {}, signal: new AbortController().signal, now: () => now});
  assert.equal(calls, 1);
  assert.equal(result.attempt_state, 'not_started');
  assert.equal(result.result.error_type, 'TimeoutError');
});

test('a stop during warm-up prevents a later run RPC', async () => {
  const controller = new AbortController();
  let calls = 0;
  const result = await runLauncherWorkflow(() => ({
    run: async () => { calls++; return {needsWarm: true}; },
    warm: async () => { controller.abort(); return {}; },
  }), {route, context: {}, signal: controller.signal, now: () => START});
  assert.equal(calls, 1);
  assert.equal(result.attempt_state, 'not_started');
  assert.equal(result.result.error_type, 'CancelledError');
});

test('host transport failure stays uncertain and is never retried', async () => {
  let calls = 0;
  const result = await runLauncherWorkflow(() => ({
    run: async () => { calls++; throw new Error('reply lost'); },
  }), {route, context: {}, signal: new AbortController().signal, now: () => START});
  assert.equal(calls, 1);
  assert.equal(result.attempt_state, 'uncertain');
  assert.equal(result.result.success, false);
});
