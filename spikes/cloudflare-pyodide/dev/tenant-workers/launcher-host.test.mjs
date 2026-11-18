import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createLauncherWorkflowRunner, launcherTenantId} from './launcher-host.mjs';

const START = Date.parse('2026-11-18T12:00:00Z');
const DEADLINE = '2026-11-18T12:00:05.123456+00:00';
const route = {workflow_deadline: DEADLINE, route_id: 'route', subject_id: 'subject',
  active_execution: {org_id: 'org'}};

test('warm-up retries use the same API deadline and approved snapshot', async () => {
  const calls = [];
  const context = {cloudflare_workspace: {sources: {'wf.py': 'pinned'}}, workflow_deadline: DEADLINE};
  const result = await createLauncherWorkflowRunner(() => ({
    run: async (attempt) => {
      calls.push(attempt);
      return calls.length === 1 ? {needsWarm: true} : {output: {status: 'Success', result: 42}};
    },
    warm: async () => ({}),
  }))({route, context, signal: new AbortController().signal, now: () => START});
  assert.equal(calls.length, 2);
  assert.equal(calls[0].workflowDeadline, DEADLINE);
  assert.equal(calls[0].context, context);
  assert.deepEqual(calls[0], calls[1]);
  assert.deepEqual(result, {attempt_state: 'completed', result: {success: true, status: 'Success', result: 42}});
});

test('deadline during warm-up never dispatches after the needsWarm refusal', async () => {
  let now = START;
  let calls = 0;
  const result = await createLauncherWorkflowRunner(() => ({
    run: async () => { calls++; return {needsWarm: true}; },
    warm: async () => { now = START + 6000; return {}; },
  }))({route, context: {}, signal: new AbortController().signal, now: () => now});
  assert.equal(calls, 1);
  assert.equal(result.attempt_state, 'not_started');
  assert.equal(result.result.error_type, 'TimeoutError');
});

test('a stop during warm-up prevents a later run RPC', async () => {
  const controller = new AbortController();
  let calls = 0;
  const result = await createLauncherWorkflowRunner(() => ({
    run: async () => { calls++; return {needsWarm: true}; },
    warm: async () => { controller.abort(); return {}; },
  }))({route, context: {}, signal: controller.signal, now: () => START});
  assert.equal(calls, 1);
  assert.equal(result.attempt_state, 'not_started');
  assert.equal(result.result.error_type, 'CancelledError');
});

test('cold warm-ups serialize while already ready workflows keep running', async () => {
  const firstWarm = Promise.withResolvers();
  const finishWarm = Promise.withResolvers();
  const firstRun = Promise.withResolvers();
  const finishFirst = Promise.withResolvers();
  const secondRefusal = Promise.withResolvers();
  const secondReady = Promise.withResolvers();
  const a = {...route, workflow_deadline: null, active_execution: {org_id: 'a'}};
  const b = {...a, active_execution: {org_id: 'b'}};
  const ready = {...a, active_execution: {org_id: 'ready'}};
  const calls = [];
  let firstReady = false;
  let secondWarmed = false;
  const completed = {output: {status: 'Success'}};
  const hosts = new Map([
    [await launcherTenantId(a), {
      async run() {
        if (!firstReady) return {needsWarm: true};
        firstRun.resolve();
        await finishFirst.promise;
        return completed;
      },
      async warm() {
        calls.push('first');
        firstWarm.resolve();
        await finishWarm.promise;
        firstReady = true;
      },
    }],
    [await launcherTenantId(b), {
      async run() {
        if (secondWarmed) {
          secondReady.resolve();
          return completed;
        }
        secondRefusal.resolve();
        return {needsWarm: true};
      },
      async warm() {
        calls.push('second');
        secondWarmed = true;
      },
    }],
    [await launcherTenantId(ready), {run: async () => completed}],
  ]);
  const run = createLauncherWorkflowRunner((id) => hosts.get(id));
  const execute = (route) => run({route, context: {}, signal: new AbortController().signal});
  const first = execute(a);
  await firstWarm.promise;
  const second = execute(b);
  await secondRefusal.promise;
  assert.equal((await execute(ready)).attempt_state, 'completed');
  assert.deepEqual(calls, ['first']);

  finishWarm.resolve();
  await Promise.all([firstRun.promise, secondReady.promise]);
  assert.deepEqual(calls, ['first', 'second']);
  finishFirst.resolve();
  assert.deepEqual((await Promise.all([first, second])).map(({attempt_state}) => attempt_state),
    ['completed', 'completed']);
});

test('a cancelled queued warm-up never calls the tenant host again', async () => {
  const entered = Promise.withResolvers();
  const finish = Promise.withResolvers();
  const refused = Promise.withResolvers();
  const a = {...route, workflow_deadline: null, active_execution: {org_id: 'a'}};
  const b = {...a, active_execution: {org_id: 'b'}};
  let warmed = false;
  let secondCalls = 0;
  const firstHost = {
    run: async () => warmed ? {output: {status: 'Success'}} : {needsWarm: true},
    async warm() { entered.resolve(); await finish.promise; warmed = true; },
  };
  const secondHost = {
    run: async () => { secondCalls++; refused.resolve(); return {needsWarm: true}; },
    warm: async () => { secondCalls++; },
  };
  const firstId = await launcherTenantId(a);
  const run = createLauncherWorkflowRunner((id) => id === firstId ? firstHost : secondHost);
  const first = run({route: a, context: {}, signal: new AbortController().signal});
  await entered.promise;
  const controller = new AbortController();
  const second = run({route: b, context: {}, signal: controller.signal});
  await refused.promise;
  controller.abort();
  assert.equal((await second).result.error_type, 'CancelledError');
  finish.resolve();
  assert.equal((await first).attempt_state, 'completed');
  assert.equal(secondCalls, 1);
});

test('a hung warm RPC releases the queue even without a workflow deadline', {timeout: 1000}, async () => {
  const started = Promise.withResolvers();
  const a = {...route, workflow_deadline: null, active_execution: {org_id: 'a'}};
  const b = {...a, active_execution: {org_id: 'b'}};
  const firstId = await launcherTenantId(a);
  let ready = false;
  const run = createLauncherWorkflowRunner((id) => id === firstId ? {
    run: async () => ({needsWarm: true}),
    warm: async () => { started.resolve(); await new Promise(() => {}); },
  } : {
    run: async () => ready ? {output: {status: 'Success'}} : {needsWarm: true},
    warm: async (input) => { assert.equal(input.engineOnly, true); ready = true; },
  }, {warmRpcMs: 20});
  const execute = (route) => run({route, context: {}, signal: new AbortController().signal});
  const first = execute(a);
  await started.promise;
  const second = execute(b);
  assert.equal((await first).result.error_type, 'TenantHostNotReady');
  assert.equal((await second).attempt_state, 'completed');
});

test('host transport failure stays uncertain and is never retried', async () => {
  let calls = 0;
  const result = await createLauncherWorkflowRunner(() => ({
    run: async () => { calls++; throw new Error('reply lost'); },
  }))({route, context: {}, signal: new AbortController().signal, now: () => START});
  assert.equal(calls, 1);
  assert.deepEqual(result, {attempt_state: 'uncertain', result: {success: false,
    error_type: 'TenantHostOutcomeUnknown', error: 'TenantHost did not confirm the workflow outcome'}});
});

for (const [phase, error_type, error] of [
  ['cpu_limit', 'CpuLimitExceeded',
    'Stopped by Cloudflare: the executor exceeded its CPU time limit (cpu_budget_ms=1000).'],
  ['memory_limit', 'MemoryLimitExceeded',
    'Stopped by Cloudflare: the executor exceeded the platform memory limit.'],
]) {
  test(`launcher reports ${phase} with a safe message and uncertain effects`, async () => {
    const context = {cloudflare_workspace: {cpu_budget_ms: 1000}};
    const result = await createLauncherWorkflowRunner(() => ({
      run: async () => ({terminal: {phase, attempt_state: 'uncertain'}}),
    }))({route, context, signal: new AbortController().signal, now: () => START});
    assert.deepEqual(result, {attempt_state: 'uncertain', result: {success: false, error_type, error}});
  });
}
