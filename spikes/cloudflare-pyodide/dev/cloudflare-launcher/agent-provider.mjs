const MAX_BYTES = 256 * 1024;
const MAX_TIME = 60_000;

export async function openAgentStream(base, grant, cancellation) {
  const endpoint = base?.replace(/\/+$/, '');
  const dest = new URL(endpoint + '/chat/completions');
  const {api_key: key, body, remaining_ms: remaining} = grant;
  if (dest.protocol !== 'https:' || dest.username || dest.password || /[?#\\\s]/.test(endpoint) ||
      grant.endpoint !== endpoint || typeof key !== 'string' || !key || key.length > 2048 ||
      !Number.isSafeInteger(remaining) || remaining < 1 || remaining > MAX_TIME ||
      body?.stream !== true || body.store !== false || !Array.isArray(body.messages) ||
      typeof body.model !== 'string' || !body.model) throw new Error('Invalid provider grant');
  const payload = JSON.stringify(body);
  if (new TextEncoder().encode(payload).byteLength > MAX_BYTES) throw new Error('Agent request exceeds byte budget');

  const control = new AbortController();
  const cancel = cancellation.getReader();
  const abort = () => control.abort();
  const deadline = setTimeout(abort, remaining);
  void cancel.read().then(abort, abort);
  const close = () => {
    clearTimeout(deadline);
    void cancel.cancel().catch(() => {});
  };
  let reader;
  try {
    const response = await fetch(dest, {
      method: 'POST', redirect: 'manual', signal: control.signal,
      headers: {Authorization: 'Bearer ' + key, 'Content-Type': 'application/json'},
      body: payload,
    });
    if (!response.ok) {
      await response.body?.cancel();
      close();
      return new Response(null, {status: 502});
    }
    if (!response.headers.get('content-type')?.startsWith('text/event-stream') || !response.body) {
      await response.body?.cancel();
      throw new Error('Invalid provider response');
    }
    reader = response.body.getReader();
  } catch {
    close();
    throw new Error('Agent provider stream interrupted');
  }
  let bytes = 0;
  return new Response(new ReadableStream({
    async pull(controller) {
      try {
        const {done, value} = await reader.read();
        if (done) {
          controller.close();
          close();
        } else {
          bytes += value.byteLength;
          if (bytes > MAX_BYTES) throw new Error('Stream limit');
          controller.enqueue(value);
        }
      } catch {
        await reader.cancel().catch(() => {});
        close();
        controller.error(new Error('Agent provider stream interrupted'));
      }
    },
    async cancel() {
      abort();
      await reader.cancel().catch(() => {});
      close();
    },
  }));
}

export async function* agentStreamEvents(response) {
  if (!response.ok) {
    yield {kind: 'finish', reason: 'error'};
    return;
  }
  if (!response.body) throw new Error('Missing provider stream');
  const decoder = new TextDecoder('utf-8', {fatal: true});
  let buffered = '', reason, usage, toolCall;
  for await (const bytes of response.body) {
    buffered += decoder.decode(bytes, {stream: true});
    let newline;
    while ((newline = buffered.indexOf('\n')) !== -1) {
      const line = buffered.slice(0, newline).trimEnd();
      buffered = buffered.slice(newline + 1);
      if (!line.startsWith('data:')) continue;
      const value = line.slice(5).trimStart();
      if (value === '[DONE]') {
        if (!reason || !usage) throw new Error('Provider did not report a complete turn');
        yield {kind: 'finish', reason, usage, toolCall};
        return;
      }
      const part = JSON.parse(value);
      if (part.usage) {
        usage = {
          input_tokens: part.usage.prompt_tokens,
          output_tokens: part.usage.completion_tokens,
          cache_read_tokens: part.usage.prompt_tokens_details?.cached_tokens ?? 0,
          cache_write_tokens: 0,
          reasoning_tokens: part.usage.completion_tokens_details?.reasoning_tokens ?? 0,
        };
      }
      if (!Array.isArray(part.choices) || part.choices.length > 1) throw new Error('Unexpected provider choices');
      const choice = part.choices[0];
      if (!choice) continue;
      const {delta} = choice;
      if (delta?.content) yield {kind: 'delta', text: delta.content};
      for (const call of delta?.tool_calls ?? []) {
        if (call.index !== 0) throw new Error('Only one workflow-tool call is supported');
        toolCall ??= {id: '', name: '', arguments: ''};
        toolCall.id += call.id ?? '';
        toolCall.name += call.function?.name ?? '';
        toolCall.arguments += call.function?.arguments ?? '';
      }
      if (choice.finish_reason) reason = choice.finish_reason;
    }
  }
  throw new Error('Provider stream closed before completion');
}
