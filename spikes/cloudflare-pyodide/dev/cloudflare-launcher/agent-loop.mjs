// Transport-independent runner for the bounded agent route. Broker methods
// return only after the API has confirmed the corresponding durable write.
const MAX_EVENTS = 1024;
const MAX_TURN_BYTES = 64 * 1024;
const encoder = new TextEncoder();

function failed(attempt_state, through_sequence, error_type) {
  return {attempt_state, result: {success: false, through_sequence, error_type}};
}

export async function runAgentLoop({grantTurn, streamTurn, commit}, signal) {
  let throughSequence = 0;
  let effectPossible = false;
  let toolReceipt = null;

  async function append(event) {
    signal.throwIfAborted();
    if (throughSequence >= MAX_EVENTS) throw new Error('Agent event budget exhausted');
    const sequence = throughSequence + 1;
    const receipt = await commit({sequence, ...event}, signal);
    if (receipt?.sequence !== sequence) throw new Error('Invalid agent event acknowledgement');
    throughSequence = sequence;
    return receipt;
  }

  try {
    for (let turn = 1; ; turn++) {
      signal.throwIfAborted();
      // A lost reply may conceal a committed grant. Only an explicit refusal
      // before any grant can prove that the first turn did not start.
      effectPossible = true;
      const grant = await grantTurn(turn, toolReceipt, signal);
      if (grant?.granted === false) {
        return failed(turn === 1 ? 'not_started' : 'uncertain', throughSequence, 'AgentTurnRefused');
      }
      if (grant?.granted !== true) throw new Error('Invalid provider grant');

      signal.throwIfAborted();
      let bytes = 0;
      toolReceipt = null;
      for await (const event of streamTurn(grant, signal)) {
        signal.throwIfAborted();
        if (event?.kind === 'delta' && typeof event.text === 'string' && event.text) {
          bytes += encoder.encode(event.text).byteLength;
          if (bytes > MAX_TURN_BYTES) throw new Error('Agent turn byte budget exhausted');
          await append({turn, kind: 'delta', text: event.text});
          continue;
        }
        if (event?.kind !== 'finish') throw new Error('Invalid provider event');

        const {reason, usage, toolCall} = event;
        if (reason === 'error' || reason === 'stop' || reason === 'length') {
          if (toolCall !== undefined) throw new Error('Invalid terminal tool call');
          await append({turn, kind: 'finish', reason, usage});
          return reason === 'error' ?
            failed('completed', throughSequence, 'AgentProviderRejected') :
            {attempt_state: 'completed', result: {success: true, through_sequence: throughSequence}};
        }

        if (reason !== 'tool_calls' || turn !== 1 || typeof toolCall?.id !== 'string' ||
            !toolCall.id || typeof toolCall.name !== 'string' || !toolCall.name ||
            typeof toolCall.arguments !== 'string' ||
            bytes + encoder.encode(toolCall.arguments).byteLength > MAX_TURN_BYTES) {
          throw new Error('Unsupported agent tool call');
        }
        // Commit the model turn before asking the API to accept the tool.
        await append({turn, kind: 'finish', reason, usage, toolCall});
        const receipt = await append({turn, kind: 'workflow_tool', toolCall});
        if (typeof receipt.execution_id !== 'string' || !receipt.execution_id) {
          throw new Error('Missing workflow execution receipt');
        }
        toolReceipt = receipt.execution_id;
        break;
      }
      if (toolReceipt === null) throw new Error('Incomplete provider stream');
    }
  } catch {
    return failed(effectPossible ? 'uncertain' : 'not_started', throughSequence,
      signal.aborted ? 'AgentInterrupted' : 'AgentOutcomeUnknown');
  }
}
