import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {Miniflare} from 'miniflare';
import {runAgentLoop} from './agent-loop.mjs';

const finish = {kind: 'finish', reason: 'stop', usage: {input_tokens: 2, output_tokens: 1}};
const toolCall = {id: 'call_1', name: 'lookup', arguments: '{"input":"stub"}'};

function fixture(turns) {
  const calls = [];
  const events = [];
  const ports = {
    grantTurn: async (turn, receipt) => {
      calls.push([turn, receipt]);
      return {granted: true, turn};
    },
    async *streamTurn({turn}) {
      yield* turns[turn - 1];
    },
    commit: async (event) => {
      events.push(event);
      return {
        sequence: event.sequence,
        ...(event.kind === 'workflow_tool' ? {execution_id: 'child-execution'} : {}),
      };
    },
  };
  return {ports, calls, events, controller: new AbortController()};
}

test('the model-only runner sequences deltas and an observed finish', async () => {
  const {ports, calls, events, controller} = fixture([[{kind: 'delta', text: 'hello'}, finish]]);
  assert.deepEqual(await runAgentLoop(ports, controller.signal), {
    attempt_state: 'completed', result: {success: true, through_sequence: 2},
  });
  assert.deepEqual(calls, [[1, null]]);
  assert.deepEqual(events, [
    {sequence: 1, turn: 1, kind: 'delta', text: 'hello'},
    {sequence: 2, turn: 1, ...finish},
  ]);
});

test('one workflow receipt authorizes the continuation', async () => {
  const {ports, calls, events, controller} = fixture([
    [{...finish, reason: 'tool_calls', toolCall}],
    [{kind: 'delta', text: 'tool answered'}, finish],
  ]);
  assert.deepEqual(await runAgentLoop(ports, controller.signal), {
    attempt_state: 'completed', result: {success: true, through_sequence: 4},
  });
  assert.deepEqual(calls, [[1, null], [2, 'child-execution']]);
  assert.deepEqual(events[1], {sequence: 2, turn: 1, kind: 'workflow_tool', toolCall});
});

test('a definitive refusal before the first grant is safe to hand back', async () => {
  const {ports, controller} = fixture([]);
  ports.grantTurn = async () => ({granted: false});
  ports.streamTurn = () => assert.fail('the provider must not be called');
  assert.deepEqual(await runAgentLoop(ports, controller.signal), {
    attempt_state: 'not_started',
    result: {success: false, through_sequence: 0, error_type: 'AgentTurnRefused'},
  });
});

test('a lost grant reply is uncertain and does not retry the grant', async () => {
  const {ports, calls, controller} = fixture([]);
  ports.grantTurn = async (turn) => {
    calls.push(turn);
    throw new Error('control disconnected');
  };
  assert.equal((await runAgentLoop(ports, controller.signal)).attempt_state, 'uncertain');
  assert.deepEqual(calls, [1]);
});

test('cancellation closes the provider stream and preserves the committed prefix', async () => {
  const {ports, events, controller} = fixture([]);
  let streamClosed = false;
  ports.streamTurn = async function* (_grant, signal) {
    try {
      yield {kind: 'delta', text: 'visible'};
      assert.equal(signal.aborted, true);
      yield {kind: 'delta', text: 'late'};
    } finally {
      streamClosed = true;
    }
  };
  const commit = ports.commit;
  ports.commit = async (event) => {
    const receipt = await commit(event);
    controller.abort();
    return receipt;
  };
  assert.deepEqual(await runAgentLoop(ports, controller.signal), {
    attempt_state: 'uncertain',
    result: {success: false, through_sequence: 1, error_type: 'AgentInterrupted'},
  });
  assert.equal(events.length, 1);
  assert.equal(streamClosed, true);
});

test('an ambiguous workflow acceptance never causes a second tool attempt', async () => {
  const {ports, calls, controller} = fixture([[{...finish, reason: 'tool_calls', toolCall}]]);
  const commit = ports.commit;
  ports.commit = async (event) => {
    if (event.kind === 'workflow_tool') throw new Error('receipt disconnected');
    return commit(event);
  };
  assert.deepEqual(await runAgentLoop(ports, controller.signal), {
    attempt_state: 'uncertain',
    result: {success: false, through_sequence: 1, error_type: 'AgentOutcomeUnknown'},
  });
  assert.deepEqual(calls, [[1, null]]);
});

test('an authorization refusal settles without an uncertain tool dispatch', async () => {
  const {ports, calls, controller} = fixture([[{...finish, reason: 'tool_calls', toolCall}]]);
  const commit = ports.commit;
  ports.commit = async (event) => event.kind === 'workflow_tool' ? {refused: true} : commit(event);
  assert.deepEqual(await runAgentLoop(ports, controller.signal), {
    attempt_state: 'completed',
    result: {success: false, through_sequence: 1, error_type: 'AgentRequestRefused'},
  });
  assert.deepEqual(calls, [[1, null]]);
});

for (const [refusal, errorType] of [
  [{refused: true}, 'AgentTurnRefused'],
  [{granted: false, error_type: 'AgentBudgetExceeded'}, 'AgentBudgetExceeded'],
  [{granted: false, error_type: 'AgentProviderRequestTooLarge'}, 'AgentProviderRequestTooLarge'],
]) {
  test(`a continuation refused with ${errorType} has a known outcome`, async () => {
    const {ports, controller} = fixture([[{...finish, reason: 'tool_calls', toolCall}]]);
    const grant = ports.grantTurn;
    ports.grantTurn = async (turn, receipt) => turn === 1 ? grant(turn, receipt) : refusal;
    assert.deepEqual(await runAgentLoop(ports, controller.signal), {
      attempt_state: 'completed',
      result: {success: false, through_sequence: 2, error_type: errorType},
    });
  });
}

test('workerd runs the same loop with a streamed turn and a workflow receipt', async () => {
  const mf = new Miniflare({cf: false, workers: [{
    config: {name: 'agent-loop-test', compatibilityDate: '2026-10-23',
      manifest: {mainModule: 'entry.mjs', modules: {
      'entry.mjs': {type: 'esm', contents: `
        import {runAgentLoop} from './agent-loop.mjs';
        export default {
          async fetch(request) {
            const events = [];
            const outcome = await runAgentLoop({
              grantTurn: async (turn, receipt) => ({granted: turn === 1 || receipt === 'child', turn}),
              async *streamTurn({turn}) {
                if (turn === 1) {
                  yield {kind: 'finish', reason: 'tool_calls',
                    toolCall: {id: 'call_1', name: 'lookup', arguments: '{}'}};
                } else {
                  yield {kind: 'delta', text: 'answer'};
                  yield {kind: 'finish', reason: 'stop'};
                }
              },
              commit: async ({sequence, kind}) => {
                events.push([sequence, kind]);
                return {sequence, ...(kind === 'workflow_tool' ? {execution_id: 'child'} : {})};
              },
            }, request.signal);
            return Response.json({outcome, events});
          },
        };
      `},
      'agent-loop.mjs': {type: 'esm',
        contents: await readFile(new URL('./agent-loop.mjs', import.meta.url), 'utf8')},
    }}},
  }]});
  try {
    const response = await mf.dispatchFetch('http://localhost/');
    assert.deepEqual(await response.json(), {
      outcome: {attempt_state: 'completed', result: {success: true, through_sequence: 4}},
      events: [[1, 'finish'], [2, 'workflow_tool'], [3, 'delta'], [4, 'finish']],
    });
  } finally {
    await mf.dispose();
  }
});
