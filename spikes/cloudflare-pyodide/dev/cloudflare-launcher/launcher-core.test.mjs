import assert from 'node:assert/strict';
import {test} from 'node:test';
import {LauncherCore} from './launcher-core.mjs';
import {CLOUDFLARE_INCARNATION_ENDED, FragmentAssembler, envelope, fragment, parseEnvelope, signHello} from './protocol.mjs';

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

function peer({data = new Map(), runHost, beforePut, onSend} = {}) {
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
    identity: IDENTITY, secret: SECRET, clock: () => now,
    newId: () => `id-${String(++order).padStart(24, '0')}`,
    storage: {
      get: async (key) => structuredClone(data.get(key)),
      put: async (key, value) => {
        if (beforePut) await beforePut(key, value);
        data.set(key, structuredClone(value));
      },
      delete: async (key) => data.delete(key),
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
      assert.equal(data.get('slot').state, 'entering');
      calls.push({route, context, signal});
      entered.resolve();
      return runHost ? runHost(route, context, signal) : COMPLETED;
    },
    stopHost: async (route) => stopped.push(route.route_id),
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

test('a lost ack keeps the same result until the matching ack, including while draining', async () => {
  const api = peer();
  await connect(api);
  await api.command('workflow', workflow());
  await api.finish();
  const terminal = api.frames().at(-1);
  await api.core.drain();
  await api.close();
  await connect(api, 2);
  assert.deepEqual(api.frames().at(-2), terminal);
  assert.deepEqual(api.frames().at(-1), {event: 'draining'});
  await api.ack('wrong-event');
  assert.ok(api.data.has('slot'));
  await api.ack(terminal._event_id);
  assert.equal(api.data.has('slot'), false);
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
  assert.deepEqual(api.frames().slice(1), [{event: 'heartbeat'}, {event: 'heartbeat'}]);
  assert.deepEqual(api.events.map(({sequence}) => sequence), [1, 2, 3]);
  await api.close();
  api.setClock(START + 3000);
  await api.tick();
  assert.equal(api.frames().length, 3);
});
