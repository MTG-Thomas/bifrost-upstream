import assert from 'node:assert/strict';
import {test} from 'node:test';
import {LauncherCore} from './launcher-core.mjs';
import {CLOUDFLARE_INCARNATION_ENDED, FragmentAssembler, envelope, fragment, parseEnvelope, signHello} from './protocol.mjs';
import {createLauncherWorkflowRunner} from '../tenant-workers/launcher-host.mjs';

const INCARNATION = '0e9d8c7b-6a59-4847-b6a5-948372615041';
const LAUNCHER = '6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f';
const SUBJECT = '0e9d8c7b-6a59-4847-b6a5-948372615045';
const IDENTITY = {incarnationId: INCARNATION, launcherId: LAUNCHER,
  resourceUid: 'test:do', hostname: 'test-do', version: 'test-build'};
const SECRET = 'derived-test-key';
const START = Date.parse('2026-11-18T12:00:00Z');
const COMPLETED = {attempt_state: 'completed', result: {success: true, result: 42}};

function deferred() {
  return Promise.withResolvers();
}

function workflow(suffix = '1') {
  const workflow_deadline = '2026-11-18T12:00:05.001987+00:00';
  return {
    route_id: `workflow:${SUBJECT}:6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5${suffix}`,
    execution_id: SUBJECT, started_at: '2026-11-18T12:00:00.001987+00:00',
    workflow_deadline, timeout_seconds: 5, memory_request_bytes: 8192,
    active_execution: {execution_id: SUBJECT, sync: true, org_id: null},
    context: {workflow_deadline, file_path: 'workflows/test.py', engine_token: 'test-access',
      cloudflare_workspace: {manifest: {'workflows/test.py': 'a'.repeat(64)},
        sources: {'workflows/test.py': 'def run(): return 42'}, packages: [],
        https_hosts: ['example.com'], cpu_budget_ms: 10, subrequest_allowance: 5}},
  };
}

function peer({data = new Map(), runHost, prepareHost, runSummary, runAgent, beforePut, onSend, meter, slots = 1, deadlineMs,
  stopHost, tenantSlots = false} = {}) {
  let now = START;
  let order = 0;
  let session;
  let sequence = 0;
  let generation;
  const events = [];
  const background = [];
  const calls = [];
  const stopped = [];
  const entered = deferred();
  const assembler = new FragmentAssembler();
  const core = new LauncherCore({
    identity: IDENTITY, secret: SECRET, clock: () => now, deadlineMs, meter, slots, tenantSlots,
    newId: () => `id-${String(++order).padStart(24, '0')}`,
    storage: {
      get: async (key) => structuredClone(data.get(key)),
      put: async (key, value) => {
        if (beforePut) await beforePut(key, value);
        data.set(key, structuredClone(value));
      },
      delete: async (...keys) => keys.forEach(key => data.delete(key)),
    },
    send: (current, raw) => {
      assert.equal(current, session);
      const frame = parseEnvelope(raw);
      assert.equal(frame.session_generation, generation);
      const event = assembler.feed(frame);
      if (event) {
        events.push(event);
        onSend?.(event.payload);
      }
    },
    runHost: async (route, context, signal) => {
      assert.ok([...data.values()].some((entry) => entry.state === 'entering' &&
        entry.route.route_id === route.route_id));
      calls.push({route, context, signal});
      entered.resolve();
      return runHost ? runHost(route, context, signal) : COMPLETED;
    },
    prepareHost, runSummary, runAgent,
    stopHost: async (route, confirm) => {
      stopped.push(route.route_id);
      return stopHost?.(route, confirm);
    },
    background: (promise) => background.push(promise),
  });
  return {
    core, data, calls, stopped, entered: entered.promise, events,
    frames: () => events.map(({payload}) => payload),
    setClock: (value) => { now = value; },
    dial: async () => {
      const dial = await core.beginDial();
      if (!dial) return null;
      session = dial.session;
      sequence = 0;
      return JSON.parse(dial.hello);
    },
    welcome: async (value) => {
      generation = value;
      await core.receive(session, JSON.stringify({event: 'welcome', session_generation: value}));
    },
    command: async (action, payload = {}) => {
      const frames = fragment(envelope(generation, sequence + 1, 'command', {id: `req-${sequence + 1}`, action, payload}));
      for (const frame of frames) {
        sequence = frame.sequence;
        await core.receive(session, JSON.stringify(frame));
      }
    },
    ack: (event_id) => core.receive(session, JSON.stringify(envelope(generation, ++sequence, 'ack', {event_id}))),
    close: (code = 1006) => core.closed(session, code),
    receive: (frame) => core.receive(session, JSON.stringify(frame)),
    tick: () => core.tick(session),
    finish: () => Promise.all(background),
  };
}

async function connect(api, generation = 1) {
  await api.dial();
  await api.welcome(generation);
}

test('a preparation records its boot even when the workflow never arrives', async () => {
  const recorded = [];
  const receipt = [{kind: 'boot', cold: true}];
  const api = peer({
    meter: {hasRoute: () => false, recordRoute: (...args) => recorded.push(args)},
    async prepareHost({orgId, executionId}) {
      assert.equal(orgId, null);
      assert.equal(executionId, SUBJECT);
      return receipt;
    },
  });
  const routeId = `prepare:${SUBJECT}`;
  await connect(api);
  assert.equal(api.frames()[0].inventory.admission_prepare, true);
  await api.command('prepare_workflow', {route_id: routeId, org_id: null, execution_id: SUBJECT});
  await api.finish();
  assert.deepEqual(recorded, [[routeId, receipt]]);
  assert.deepEqual(api.calls, []);
});

test('a full preparation slot records a skipped hint without preempting the original receipt', async () => {
  const gate = deferred();
  const receipts = new Map();
  const invocation = {kind: 'boot', cold: true};
  const api = peer({
    meter: {hasRoute: id => receipts.has(id), recordRoute: (id, calls) => receipts.set(id, calls)},
    prepareHost: () => gate.promise,
  });
  const original = `prepare:${SUBJECT}`;
  const skipped = `prepare:${LAUNCHER}`;
  const payload = {org_id: null, execution_id: SUBJECT};
  await connect(api);
  await api.command('prepare_workflow', {...payload, route_id: original});
  await api.command('prepare_workflow', {...payload, route_id: original});
  await api.command('prepare_workflow', {...payload, route_id: skipped});
  assert.deepEqual([...receipts], [[skipped, []]]);
  gate.resolve([invocation]);
  await api.finish();
  assert.deepEqual(receipts.get(original), [invocation]);
});

test('an autonomous broker call replays across a lost reply without rerunning the loop', async () => {
  let runs = 0;
  const waiting = deferred();
  const api = peer({
    onSend: (event) => {
      if (event.event === 'cloudflare_agent_request') waiting.resolve(event);
    },
    async runAgent(_route, request, signal) {
      runs++;
      const receipt = await request('commit', {sequence: 1, kind: 'workflow_tool'}, signal);
      assert.equal(receipt.execution_id, SUBJECT);
      return COMPLETED;
    },
  });
  const hello = await api.dial();
  assert.equal(hello.agent_claim_protocol, 'autonomous-v1');
  assert.deepEqual(hello.handlers, [['workflow.execute', 1], ['agent.run', 1]]);
  await api.welcome(1);
  const route_id = `agent:${SUBJECT}:${LAUNCHER}`;
  await api.command('cloudflare_agent', {
    route_id, run_id: SUBJECT, claim_id: LAUNCHER,
    deadline: new Date(START + 3000).toISOString(), memory_request_bytes: 8192,
  });
  const first = await waiting.promise;
  await api.close();
  await connect(api, 2);
  assert.deepEqual(api.frames().filter((row) => row.event === first.event), [first, first]);
  await api.command('cloudflare_agent_reply', {
    route_id, request_id: first.request_id, ok: true, result: {execution_id: SUBJECT},
  });
  await api.finish();
  assert.equal(runs, 1);
  assert.equal(api.frames().filter((row) => row.event === 'cloudflare_agent_result').length, 1);
});

test('a negative authorization reply reaches the agent loop as a refusal', async () => {
  const waiting = deferred();
  const api = peer({
    onSend: (event) => {
      if (event.event === 'cloudflare_agent_request') waiting.resolve(event);
    },
    async runAgent(_route, request, signal) {
      assert.deepEqual(await request('commit', {sequence: 1, kind: 'workflow_tool'}, signal), {refused: true});
      return {attempt_state: 'completed', result: {success: false, error_type: 'AgentRequestRefused'}};
    },
  });
  await connect(api);
  const route_id = `agent:${SUBJECT}:${LAUNCHER}`;
  await api.command('cloudflare_agent', {
    route_id, run_id: SUBJECT, claim_id: LAUNCHER,
    deadline: new Date(START + 3000).toISOString(), memory_request_bytes: 8192,
  });
  const {request_id} = await waiting.promise;
  await api.command('cloudflare_agent_reply', {route_id, request_id, ok: false, result: {refused: true}});
  await api.finish();
  const terminal = api.frames().find((row) => row.event === 'cloudflare_agent_result');
  assert.equal(terminal.attempt_state, 'completed');
  assert.equal(terminal.result.error_type, 'AgentRequestRefused');
});

function tenantWorkflow(suffix, tenant) {
  const attempt = workflow(suffix);
  const subject = SUBJECT.slice(0, -1) + suffix;
  attempt.route_id = attempt.route_id.replace(SUBJECT, subject);
  attempt.execution_id = attempt.active_execution.execution_id = subject;
  attempt.active_execution.org_id = tenant;
  return attempt;
}

test('workspace entry supports hyphenated storage directories', async () => {
  const h = peer();
  await h.dial();
  await h.welcome(1);
  const payload = workflow();
  const file = 'workflows/halo-psa/sync.py';
  const bundle = payload.context.cloudflare_workspace;
  bundle.manifest = {[file]: 'a'.repeat(64)};
  bundle.sources = {[file]: 'def run(): return 42'};
  payload.context.file_path = file;
  await h.command('workflow', payload);
  await h.entered;
  assert.equal(h.calls[0].context.file_path, file);
});

test('launcher requires a matching Solution install and entry digest', async () => {
  const api = peer();
  await connect(api);
  const payload = workflow();
  const context = payload.context;
  const workspace = context.cloudflare_workspace;
  context.solution_id = INCARNATION;
  context.content_hash = workspace.manifest[context.file_path];
  workspace.solution_id = LAUNCHER;
  await api.command('workflow', payload);
  assert.equal(api.frames().at(-1).error_type, 'ValueError');
  workspace.solution_id = INCARNATION;
  context.content_hash = 'b'.repeat(64);
  await api.command('workflow', {...workflow('2'), context});
  assert.equal(api.frames().at(-1).error_type, 'ValueError');
  assert.equal(api.calls.length, 0);
  context.content_hash = workspace.manifest[context.file_path];
  await api.command('workflow', {...workflow('3'), context});
  await api.finish();
  assert.equal(api.calls[0].context.solution_id, INCARNATION);
});

test('an approved source snapshot alone cannot authorize an inherited artifact workspace', async () => {
  const api = peer();
  await connect(api);
  const payload = workflow();
  payload.context.artifact_workspace_id = SUBJECT;
  await api.command('workflow', payload);
  assert.equal(api.frames().at(-1).error_type, 'ValueError');
  assert.equal(api.calls.length, 0);
  delete payload.context.artifact_workspace_id;
  await api.command('workflow', {...workflow('2'), context: payload.context});
  await api.finish();
  assert.equal(api.calls.length, 1);
});

test('launcher accepts an operator wildcard and refuses a shared-hosting wildcard before entry', async () => {
  const api = peer();
  await connect(api);
  const denied = workflow('1');
  denied.context.cloudflare_workspace.https_hosts = ['*.github.io'];
  await api.command('workflow', denied);
  assert.equal(api.frames().at(-1).error_type, 'ValueError');
  assert.equal(api.calls.length, 0);

  const approved = workflow('2');
  approved.context.cloudflare_workspace.https_hosts = ['*.halopsa.com'];
  await api.command('workflow', approved);
  await api.finish();
  assert.deepEqual(api.calls[0].context.cloudflare_workspace.https_hosts, ['*.halopsa.com']);
});

test('distinct tenants run concurrently and ACK releases only its route', async () => {
  const first = deferred();
  const second = deferred();
  const api = peer({slots: 2, runHost: (route) =>
    route.active_execution.org_id === 'first' ? first.promise : second.promise});
  const a = tenantWorkflow('1', 'first');
  const b = tenantWorkflow('2', 'second');
  await connect(api);
  await api.command('workflow', a);
  await api.command('workflow', tenantWorkflow('3', 'first'));
  assert.equal(api.frames().at(-1).error_type, 'MemoryError');
  await api.command('workflow', b);
  await api.command('workflow', tenantWorkflow('4', 'third'));
  assert.equal(api.frames().at(-1).error_type, 'MemoryError');
  assert.deepEqual(api.calls.map(({route}) => route.route_id), [a.route_id, b.route_id]);

  await api.command('terminate', {route_id: a.route_id, error_type: 'CancelledError', error: 'cancelled'});
  const terminal = api.frames().at(-1);
  assert.equal(terminal.route_id, a.route_id);
  assert.equal(api.calls[0].signal.aborted, true);
  assert.equal(api.calls[1].signal.aborted, false);
  await api.close();
  await connect(api, 2);
  const inventory = api.frames().at(-2).inventory;
  assert.deepEqual(inventory.routes.map(({route_id}) => route_id), [b.route_id]);
  assert.deepEqual(inventory.terminal_routes.map(({event_id}) => event_id), [terminal._event_id]);
  assert.deepEqual(api.frames().at(-1), terminal);

  await api.ack(terminal._event_id);
  api.setClock(START + 1001);
  await api.tick();
  assert.deepEqual(api.frames().at(-1), {event: 'heartbeat', occupied_routes: [b.route_id], received_sequence: 1});
  first.resolve(COMPLETED);
  second.resolve(COMPLETED);
  await api.finish();
  const results = api.frames().filter(({event}) => event === 'result');
  assert.equal(results.length, 3); // Original terminal, replay, second route.
  assert.equal(results.at(-1).route_id, b.route_id);
});

test('completed terminals hold a slot without blocking the next route for that tenant', async () => {
  const api = peer({slots: 3});
  const first = tenantWorkflow('1', 'same');
  const second = tenantWorkflow('2', 'same');
  await connect(api);
  await api.command('workflow', first);
  await api.finish();
  const terminal = api.frames().at(-1);
  assert.equal(terminal.attempt_state, 'completed');
  await api.command('workflow', second);
  await api.finish();
  assert.deepEqual(api.calls.map(({route}) => route.route_id), [first.route_id, second.route_id]);

  await api.close();
  await connect(api, 2);
  const snapshot = api.frames().at(-3).inventory;
  assert.equal(snapshot.workflow_slots, 3);
  assert.deepEqual(snapshot.terminal_routes.map(({route_id}) => route_id), [first.route_id, second.route_id]);
});

test('an uncertain terminal blocks the same tenant until its ACK', async () => {
  const finish = deferred();
  const api = peer({slots: 3, runHost: () => finish.promise});
  const first = tenantWorkflow('1', 'same');
  await connect(api);
  await api.command('workflow', first);
  await api.entered;
  await api.command('terminate', {route_id: first.route_id, error_type: 'CancelledError', error: 'cancelled'});
  const terminal = api.frames().at(-1);
  assert.equal(terminal.attempt_state, 'uncertain');
  await api.command('workflow', tenantWorkflow('2', 'same'));
  assert.equal(api.frames().at(-1).error_type, 'MemoryError');
  await api.ack(terminal._event_id);
  await api.command('workflow', tenantWorkflow('3', 'same'));
  finish.resolve(COMPLETED);
  await api.finish();
  assert.equal(api.calls.length, 2);
});

test('managed cancellation keeps the acknowledged slot until child termination is confirmed', async () => {
  let ended = false;
  const finish = deferred();
  const reported = deferred();
  const api = peer({
    tenantSlots: true, runHost: () => finish.promise, stopHost: (_, confirm) => confirm && ended,
    onSend: payload => { if (payload.event === 'result') reported.resolve(payload); },
  });
  await connect(api);
  const attempt = {...workflow(), engine_slot: 0};
  await api.command('workflow', attempt);
  await api.entered;
  await api.command('terminate', {route_id: attempt.route_id, error_type: 'CancelledError', error: 'cancelled'});
  const terminal = await reported.promise;
  assert.equal(terminal.attempt_state, 'uncertain');
  await api.ack(terminal._event_id);
  assert.equal(api.data.get('slot').route.route_id, attempt.route_id);
  await assert.rejects(api.core.release(), /stop is unconfirmed/);
  assert.equal(api.data.get('slot').route.engine_slot, 0);
  const stopCalls = api.stopped.length;
  await assert.rejects(api.core.release(), /backing off/);
  assert.equal(api.stopped.length, stopCalls);
  ended = true;
  finish.resolve(COMPLETED);
  api.setClock(START + 2000);
  await api.core.release();
  await api.finish();
  assert.equal(await api.dial(), null);
});

test('a persisted stopping slot survives reset and serves control frames while awaiting proof', async () => {
  const run = deferred();
  const firstProof = deferred();
  const started = peer({tenantSlots: true, slots: 2, runHost: () => run.promise, stopHost: () => firstProof.promise});
  await connect(started);
  const attempt = {...workflow(), engine_slot: 0};
  const terminate = {route_id: attempt.route_id, error_type: 'CancelledError', error: 'cancelled'};
  await started.command('workflow', attempt);
  await started.entered;
  await started.command('terminate', terminate);
  assert.equal(started.data.get('slot').state, 'stopping');

  const restoredProof = deferred();
  const reset = peer({tenantSlots: true, slots: 2, data: new Map(started.data), stopHost: () => restoredProof.promise});
  await connect(reset, 2);
  await reset.command('terminate', terminate);
  assert.deepEqual(reset.frames().at(-1).result, {received: true});
  reset.setClock(START + 1000);
  await reset.tick();
  assert.deepEqual(reset.frames().at(-1).occupied_routes, [attempt.route_id]);
  assert.deepEqual(reset.stopped, [attempt.route_id]);
  restoredProof.resolve(true);
  await reset.finish();
  assert.equal(reset.frames().at(-1).attempt_state, 'stopped');
  await reset.ack(reset.frames().at(-1)._event_id);
  assert.equal(reset.data.has('slot'), false);
  firstProof.resolve(false);
  run.resolve(COMPLETED);
  await started.finish();
});

test('release of a stopping slot requires its own confirmed child stop', async () => {
  const run = deferred();
  const observation = deferred();
  const releaseProof = deferred();
  const releaseEntered = deferred();
  let confirmations = 0;
  const api = peer({tenantSlots: true, runHost: () => run.promise, stopHost: () => {
    if (++confirmations === 1) return observation.promise;
    releaseEntered.resolve();
    return releaseProof.promise;
  }});
  await connect(api);
  const attempt = {...workflow(), engine_slot: 0};
  await api.command('workflow', attempt);
  await api.entered;
  await api.command('terminate', {route_id: attempt.route_id, error_type: 'CancelledError', error: 'cancelled'});
  const releasing = api.core.release();
  await releaseEntered.promise;
  assert.equal(api.data.get('slot').state, 'stopping');
  releaseProof.resolve(false);
  await assert.rejects(releasing, /stop is unconfirmed/);
  assert.equal(api.data.get('slot').state, 'stopping');
  observation.resolve(true);
  run.resolve(COMPLETED);
  await api.finish();
});

test('reset fences every entering route and replays independently acknowledged terminals', async () => {
  const held = deferred();
  const api = peer({slots: 2, runHost: () => held.promise});
  const a = tenantWorkflow('1', 'first');
  const b = tenantWorkflow('2', 'second');
  await connect(api);
  await api.command('workflow', a);
  await api.command('workflow', b);
  await api.close();

  const reset = peer({slots: 2, data: new Map(api.data)});
  await connect(reset, 2);
  assert.deepEqual(reset.stopped, [a.route_id, b.route_id]);
  const [snapshot, ...replayed] = reset.frames();
  assert.deepEqual(snapshot.inventory.routes, []);
  assert.deepEqual(snapshot.inventory.terminal_routes.map(({attempt_state}) => attempt_state),
    ['uncertain', 'uncertain']);
  assert.deepEqual(replayed.map(({route_id}) => route_id), [a.route_id, b.route_id]);
  assert.notEqual(replayed[0]._event_id, replayed[1]._event_id);
  await reset.ack(replayed[1]._event_id);
  await reset.close();
  await connect(reset, 3);
  assert.deepEqual(reset.frames().at(-1), replayed[0]);
  held.resolve(COMPLETED);
  await api.finish();
});

test('terminal release stops the active route and fences late results after the ledger is cleared', async () => {
  const finish = deferred();
  const api = peer({runHost: () => finish.promise});
  await connect(api);
  const attempt = workflow();
  await api.command('workflow', attempt);
  await api.entered;
  await api.core.release();
  assert.deepEqual(api.stopped, [attempt.route_id]);
  assert.equal(api.calls[0].signal.aborted, true);
  api.data.clear();
  finish.resolve(COMPLETED);
  await api.finish();
  assert.equal(api.data.size, 0);
  assert.equal(await api.dial(), null);
  assert.equal(api.frames().some(({event}) => event === 'result'), false);
});

test('terminal release fences a concurrent retirement frame before it writes the ledger', async () => {
  const api = peer();
  await connect(api);
  const before = api.data.get('control');
  const releasing = api.core.release();
  const closing = api.close(CLOUDFLARE_INCARNATION_ENDED);
  await Promise.all([releasing, closing]);
  assert.deepEqual(api.data.get('control'), before);
  assert.equal(await api.dial(), null);
});

test('hello sequence is confirmed before every dial, including after a lost welcome', async () => {
  const first = peer();
  const initial = await first.dial();
  assert.equal(first.data.get('control').helloSequence, 1);
  assert.equal(initial.event, 'hello');
  assert.equal(initial.version, IDENTITY.version);
  assert.equal(initial.credential_protocol, 2);
  assert.equal(Object.hasOwn(initial, 'sample'), false);
  assert.deepEqual(initial.handlers, [['workflow.execute', 1]]);
  assert.equal(initial.hello.form, 'external');
  assert.equal(initial.hello.execution_lane, 'normal');
  assert.equal(initial.hello.runtime_profile, 'cloudflare-python-v1');
  assert.deepEqual(await signHello(SECRET, initial.hello), initial.hello);

  const reset = peer({data: first.data});
  assert.equal((await reset.dial()).hello.hello_sequence, 2);
  assert.equal(reset.data.get('control').helloSequence, 2);
});

test('inventory is first, then replay, then replies in each generation', async () => {
  const api = peer();
  await connect(api);
  assert.deepEqual(api.frames(), [{event: 'inventory', inventory: {version: 1, routes: [], terminal_routes: []}}]);
  const attempt = workflow();
  await api.command('workflow', attempt);
  await api.finish();
  const [, reply, terminal] = api.frames();
  assert.deepEqual(reply.result, {runtime_handle: terminal.runtime_handle});
  await api.close();
  const reset = peer({data: api.data});
  await connect(reset, 2);
  await reset.command('ready');
  const [inventory, replay, ready] = reset.frames();
  assert.equal(inventory.event, 'inventory');
  assert.deepEqual(inventory.inventory, {version: 1, routes: [], terminal_routes: [{
    kind: 'workflow', route_id: attempt.route_id, subject_id: SUBJECT, runtime_handle: terminal.runtime_handle,
    active_execution: attempt.active_execution, started_at: attempt.started_at,
    timeout_seconds: attempt.timeout_seconds, workflow_deadline: attempt.workflow_deadline,
    memory_request_bytes: attempt.memory_request_bytes, event_id: terminal._event_id, attempt_state: 'completed',
  }]});
  assert.deepEqual(replay, terminal);
  assert.deepEqual(ready, {reply: 'req-1', result: {}});
  assert.deepEqual(reset.events.map((frame) => frame.sequence), [1, 2, 3]);
});

test('a lost ack keeps the same result until the matching ack', async () => {
  const api = peer();
  await connect(api);
  await api.command('workflow', workflow());
  await api.finish();
  const terminal = api.frames().at(-1);
  await api.close();
  await connect(api, 2);
  assert.deepEqual(api.frames().at(-1), terminal);
  await api.ack('wrong-event');
  assert.ok(api.data.has('slot'));
  await api.ack(terminal._event_id);
  assert.equal(api.data.has('slot'), false);
});

test('a managed workflow retains only stage timestamps in its replayed terminal', async () => {
  const timings = {engine_ready_at: START + 10, entry_at: START + 20};
  const run = createLauncherWorkflowRunner(() => ({
    run: async () => ({
      output: {status: 'Success', result: 42},
      timings: {engine_ready_at: timings.engine_ready_at, child: {entry_at: timings.entry_at, other: 'ignored'}},
    }),
  }));
  const api = peer({tenantSlots: true, runHost: (route, context, signal) => run({
    route, context, signal, now: () => START,
  })});
  await connect(api);
  await api.command('workflow', {...workflow(), engine_slot: 0});
  await api.finish();
  const terminal = api.frames().at(-1);
  assert.deepEqual(terminal.timings, timings);
  assert.equal(terminal.result.timings, undefined);
  await api.close();
  await connect(api, 2);
  assert.deepEqual(api.frames().at(-1), terminal);
});

test('a CPU limit terminal retains its reason, budget, and uncertainty across reconnect', async () => {
  const outcome = {attempt_state: 'uncertain', result: {success: false, error_type: 'CpuLimitExceeded',
    error: 'Stopped by Cloudflare: the executor exceeded its CPU time limit (cpu_budget_ms=10).'}};
  const api = peer({runHost: async () => outcome});
  await connect(api);
  await api.command('workflow', workflow());
  await api.finish();
  const terminal = api.frames().at(-1);
  assert.equal(terminal.event, 'result');
  assert.equal(terminal.attempt_state, outcome.attempt_state);
  assert.deepEqual(terminal.result, outcome.result);

  const reset = peer({data: api.data});
  await connect(reset, 2);
  assert.deepEqual(reset.frames().at(-1), terminal);
  assert.equal(reset.calls.length, 0);
});

test('stale generations cannot acknowledge a retained terminal or welcome a new session', async () => {
  const api = peer();
  await connect(api, 2);
  await api.command('workflow', workflow());
  await api.finish();
  const ack = {event_id: api.frames().at(-1)._event_id};
  await assert.rejects(api.receive(envelope(1, 99, 'ack', ack)), /stale control generation/);
  assert.ok(api.data.has('slot'));
  await api.dial();
  await assert.rejects(api.welcome(2), /stale/);
});

test('a repeated command sequence is rejected on the current generation', async () => {
  const api = peer();
  await connect(api, 3);
  await api.command('ready');
  await assert.rejects(api.receive(envelope(3, 1, 'command', {id: 'repeat', action: 'ready', payload: {}})),
    /stale control generation or sequence/);
  assert.equal(api.frames().length, 2);
});

test('one slot holds through terminal settlement and refuses a second engine entry', async () => {
  const running = deferred();
  const api = peer({runHost: () => running.promise});
  await connect(api);
  const attempt = workflow();
  await api.command('workflow', attempt);
  await api.entered;
  assert.deepEqual(api.calls[0].context, attempt.context);
  assert.equal(api.calls[0].route.workflow_deadline, attempt.workflow_deadline);
  await api.command('workflow', workflow('2'));
  assert.equal(api.frames().at(-1).error_type, 'MemoryError');
  running.resolve(COMPLETED);
  await api.finish();
  const terminal = api.frames().at(-1);
  await api.command('workflow', workflow('3'));
  assert.equal(api.frames().at(-1).error_type, 'MemoryError');
  assert.equal(api.calls.length, 1);
  await api.ack(terminal._event_id);
  await api.command('workflow', workflow('4'));
  await api.finish();
  assert.equal(api.calls.length, 2);
});

test('launcher exhaustion preserves success and allows redial until the terminal ACK', async () => {
  const usage = {sqlite_rows_written: 31, do_requests: 7, log_events: 3, exhausted: true};
  const meter = {exhausted: null, log: () => false, snapshot: () => usage};
  const api = peer({meter, runHost() {
    meter.exhausted = 'do_requests';
    return COMPLETED;
  }});
  await connect(api);
  await api.command('workflow', workflow());
  await api.finish();
  const terminal = api.frames().at(-1);
  assert.deepEqual(terminal.result, COMPLETED.result);
  assert.deepEqual(terminal.usage, usage);
  assert.equal(await api.core.isStopped(), false);
  await api.close();
  assert.equal(await api.core.canReconnect(), true);
  await connect(api, 2);
  assert.deepEqual(api.frames().at(-1), terminal);
  assert.equal(await api.ack('other-event-id'), undefined);
  assert.equal(await api.ack(terminal._event_id), 'terminal_ack');
  assert.equal(await api.core.canReconnect(), false);
  assert.equal(await api.core.isStopped(), true);
});

test('an incarnation deadline drains the accepted workflow and stops after its ACK', async () => {
  const running = deferred();
  const api = peer({deadlineMs: START + 1000, slots: 2, runHost: () => running.promise});
  await connect(api);
  await api.command('workflow', workflow());
  await api.entered;
  api.setClock(START + 1000);
  await api.command('workflow', workflow('2'));
  assert.equal(api.frames().at(-1).error_type, 'LauncherRetired');
  api.setClock(START + 1001);
  await api.command('workflow', workflow('3'));
  assert.equal(api.frames().at(-1).error_type, 'LauncherRetired');
  assert.equal(api.data.has(`seen:${workflow('2').route_id}`), false);
  assert.equal(api.data.has(`seen:${workflow('3').route_id}`), false);
  assert.equal(await api.core.isStopped(), false);
  running.resolve(COMPLETED);
  await api.finish();
  await api.ack(api.frames().at(-1)._event_id);
  assert.equal(await api.core.isStopped(), true);
  assert.equal(await api.core.canReconnect(), false);
});

test('release completes while a stop RPC is still stalled', async () => {
  const run = deferred();
  const stop = deferred();
  const peerApi = peer({runHost: () => run.promise, stopHost: () => stop.promise});
  await connect(peerApi);
  await peerApi.command('workflow', workflow());
  await peerApi.entered;
  await peerApi.core.release();
  assert.equal(peerApi.core.retired, true);
  assert.equal(peerApi.stopped.length, 1);
  stop.resolve();
  run.resolve(COMPLETED);
  await peerApi.finish();
});

test('reset during a host call journals uncertain before reconnect', async () => {
  const running = deferred();
  const api = peer({runHost: () => running.promise});
  await connect(api);
  await api.command('workflow', workflow());
  await api.entered;
  const reset = peer({data: structuredClone(api.data)});
  await connect(reset, 2);
  assert.equal(reset.frames()[0].inventory.terminal_routes[0].attempt_state, 'uncertain');
  assert.equal(reset.frames()[1].result.success, false);
  assert.equal(reset.calls.length, 0);
  await reset.finish();
  assert.deepEqual(reset.stopped, [workflow().route_id]);
  running.resolve(COMPLETED);
  await api.finish();
});

test('a still-running route remains in live inventory on transport reconnect', async () => {
  const running = deferred();
  const api = peer({runHost: () => running.promise});
  const attempt = workflow();
  await connect(api);
  await api.command('workflow', attempt);
  await api.entered;
  await api.close();
  await connect(api, 2);
  const {inventory} = api.frames().at(-1);
  assert.deepEqual(inventory.routes, [api.calls[0].route]);
  assert.deepEqual(inventory.terminal_routes, []);
  running.resolve(COMPLETED);
  await api.finish();
  assert.equal(api.events.at(-1).session_generation, 2);
  assert.equal(api.frames().at(-1).route_id, attempt.route_id);
  assert.equal(api.calls.length, 1);
});

test('reset after the acceptance write but before engine entry journals not_started', async () => {
  let resetData;
  const api = peer({onSend: (payload) => {
    if (payload.result?.runtime_handle) resetData = structuredClone(api.data);
  }});
  await connect(api);
  await api.command('workflow', workflow());
  await api.finish();
  const reset = peer({data: resetData});
  await connect(reset, 2);
  assert.equal(reset.frames()[0].inventory.terminal_routes[0].attempt_state, 'not_started');
  assert.equal(reset.calls.length, 0);
});

test('a route omitted after confirmation of receipt is fenced across resets', async () => {
  const api = peer();
  const attempt = workflow();
  await connect(api);
  await api.command('workflow', attempt);
  await api.finish();
  await api.ack(api.frames().at(-1)._event_id);
  assert.equal(api.data.has(`seen:${attempt.route_id}`), false);
  const reset = peer({data: api.data});
  await connect(reset, 2);
  const snapshot = reset.frames();
  assert.deepEqual(snapshot[0].inventory.routes, []);
  assert.deepEqual(snapshot[0].inventory.terminal_routes, []);
  await assert.rejects(reset.command('workflow', attempt), /already seen/);
  assert.deepEqual(reset.frames(), snapshot);
  assert.equal(reset.calls.length, 0);
});

test('a route omitted after interrupted acceptance cannot enter or be reported', async () => {
  const attempt = workflow();
  let resetData;
  const api = peer({beforePut: (key, value) => {
    if (key === 'slot' && value.state === 'accepted') resetData = structuredClone(api.data);
  }});
  await connect(api);
  await api.command('workflow', attempt);
  await api.finish();
  const reset = peer({data: resetData});
  await connect(reset, 2);
  assert.deepEqual(reset.frames(), [{event: 'inventory', inventory: {version: 1, routes: [], terminal_routes: []}}]);
  await assert.rejects(reset.command('workflow', attempt), /already seen/);
  assert.equal(reset.frames().length, 1);
  assert.equal(reset.calls.length, 0);
});

test('terminate acknowledges repeated requests, stops once and ignores the late host result', async () => {
  const running = deferred();
  const api = peer({runHost: () => running.promise});
  const attempt = workflow();
  await connect(api);
  await api.command('workflow', attempt);
  await api.entered;
  const stop = {route_id: attempt.route_id, error_type: 'CancelledError', error: 'Cancelled'};
  await api.command('terminate', stop);
  await api.command('terminate', stop);
  running.resolve(COMPLETED);
  await api.finish();
  assert.equal(api.calls[0].signal.aborted, true);
  assert.deepEqual(api.stopped, [attempt.route_id]);
  const events = api.frames().slice(2);
  assert.deepEqual(events.map((event) => event.result?.received ?? event.attempt_state), [true, 'uncertain', true]);
  assert.equal(events[1].result.error_type, 'CancelledError');
});

test('the deadline is checked again after confirming engine intent', async () => {
  const api = peer({beforePut: (_, value) => {
    if (value.state === 'entering') api.setClock(START + 6000);
  }});
  await connect(api);
  await api.command('workflow', workflow());
  await api.finish();
  assert.equal(api.calls.length, 0);
  assert.equal(api.frames().at(-1).attempt_state, 'not_started');
  assert.equal(api.frames().at(-1).result.error_type, 'TimeoutError');
});

test('terminal retirement survives reset, while an ordinary refusal reconnects', async () => {
  const api = peer();
  await api.dial();
  await api.close(1008);
  assert.ok(await api.dial());
  await api.close(CLOUDFLARE_INCARNATION_ENDED);
  assert.equal(api.core.retired, true);
  assert.equal(await api.dial(), null);
  assert.equal(await peer({data: api.data}).dial(), null);
});

test('a replied route executes through disconnect and replays its terminal', async () => {
  const attempt = workflow();
  const api = peer({onSend: (payload) => {
    if (payload.result?.runtime_handle) void api.close();
  }});
  await connect(api);
  await api.command('workflow', attempt);
  await api.finish();
  assert.equal(api.calls.length, 1);
  assert.equal(api.calls[0].route.workflow_deadline, attempt.workflow_deadline);
  await connect(api, 2);
  assert.equal(api.frames().at(-2).inventory.terminal_routes[0].route_id, attempt.route_id);
  assert.equal(api.frames().at(-1).attempt_state, 'completed');
});

test('a persisted route whose reply could not be sent never enters the host', async () => {
  const api = peer({beforePut: (key, value) => {
    if (key === 'slot' && value.state === 'accepted') void api.close();
  }});
  await connect(api);
  await api.command('workflow', workflow());
  await api.finish();
  assert.equal(api.calls.length, 0);
  await connect(api, 2);
  assert.equal(api.frames().at(-1).attempt_state, 'not_started');
});

test('heartbeats are sequenced at one-second cadence after welcome', async () => {
  const api = peer();
  await api.dial();
  await api.tick();
  assert.deepEqual(api.frames(), []);
  await api.welcome(1);
  api.setClock(START + 999);
  await api.tick();
  assert.equal(api.frames().length, 1);
  for (const offset of [1000, 2000]) {
    api.setClock(START + offset);
    await api.tick();
    await api.tick();
  }
  const idle = {event: 'heartbeat', slot_occupied: false, received_sequence: 0};
  assert.deepEqual(api.frames().slice(1), [idle, idle]);
  assert.deepEqual(api.events.map(({sequence}) => sequence), [1, 2, 3]);
  await api.close();
  api.setClock(START + 3000);
  await api.tick();
  assert.equal(api.frames().length, 3);
});

test('heartbeat reports the slot occupied through terminal retention and empty after ACK', async () => {
  const api = peer();
  await connect(api);
  await api.command('workflow', workflow());
  await api.finish();
  const terminal = api.frames().at(-1);
  api.setClock(START + 1000);
  await api.tick();
  assert.deepEqual(api.frames().at(-1), {event: 'heartbeat', slot_occupied: true, received_sequence: 1});
  await api.ack(terminal._event_id);
  api.setClock(START + 2000);
  await api.tick();
  assert.deepEqual(api.frames().at(-1), {event: 'heartbeat', slot_occupied: false, received_sequence: 2});
});


function summaryCommand() {
  return {
    route_id: `summary:${SUBJECT}:${LAUNCHER}`, run_id: SUBJECT, claim_id: LAUNCHER,
    // An API clock well ahead cannot influence the launcher's deadline.
    deadline: '2040-01-01T00:00:00Z', timeout_seconds: 30, memory_request_bytes: 16 * 2 ** 20,
    context: {api_key: 'ephemeral-summary-key'},
  };
}

test('summary dispatch uses the launcher clock and journals no credential', async () => {
  const api = peer({runSummary: async (route, context) => {
    assert.equal(Date.parse(route.deadline), START + 30_000);
    assert.equal(context.api_key, 'ephemeral-summary-key');
    return COMPLETED;
  }});
  await connect(api);
  await api.command('cloudflare_summary', summaryCommand());
  await api.finish();
  assert.equal(api.frames().at(-1).event, 'summary_result');
  assert.equal(api.frames().at(-1).attempt_state, 'completed');
  assert.ok(!JSON.stringify([...api.data.values()]).includes('ephemeral-summary-key'));
});

test('terminate aborts a running summary and fences its late completion', async () => {
  const entered = deferred();
  const result = deferred();
  const api = peer({runSummary: (_route, _context, signal) => {
    entered.resolve(signal);
    return result.promise;
  }});
  await connect(api);
  const {route_id} = summaryCommand();
  await api.command('cloudflare_summary', summaryCommand());
  const signal = await entered.promise;
  await api.command('terminate', {route_id, error_type: 'OwnerLost', error: 'superseded'});
  assert.ok(signal.aborted);
  result.resolve(COMPLETED);
  await api.finish();
  assert.equal(api.frames().at(-1).attempt_state, 'uncertain');
  assert.equal(api.frames().filter((frame) => frame.event === 'summary_result').length, 1);
});

test('reset while a summary is running replays uncertainty without issuing the call again', async () => {
  const entered = deferred();
  const result = deferred();
  const original = peer({runSummary: () => { entered.resolve(); return result.promise; }});
  await connect(original);
  await original.command('cloudflare_summary', summaryCommand());
  await entered.promise;
  const reset = peer({data: structuredClone(original.data), runSummary: async () => assert.fail('reissued summary')});
  await connect(reset, 2);
  assert.equal(reset.frames().at(-1).event, 'summary_result');
  assert.equal(reset.frames().at(-1).attempt_state, 'uncertain');
  result.resolve(COMPLETED);
  await original.finish();
});
