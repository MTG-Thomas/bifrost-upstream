const REQUEST_BYTES = 256 * 1024;
const RESPONSE_BYTES = 64 * 1024;

export default {
  async fetch(request, env) {
    const base = env.BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT;
    let url;
    try {
      if (!base || /[?#\\\s]/.test(base)) throw new Error();
      url = new URL(base);
      if (url.protocol !== 'https:' || url.username || url.password) throw new Error();
    } catch {
      return Response.json(failed('not_started', 'SummaryProviderDisabled'));
    }

    if (request.method !== 'POST' || new URL(request.url).pathname !== '/complete') {
      return new Response(null, {status: 404});
    }
    let context;
    try {
      context = await readJson(request, REQUEST_BYTES + 4096);
    } catch {
      return Response.json(failed('not_started', 'SummaryRequestRejected'));
    }
    const endpoint = base.replace(/\/+$/, '');
    return Response.json(await completeSummary(new URL(endpoint + '/chat/completions').href,
      endpoint, context, request.signal));
  },
};

function failed(attempt_state, error_type) {
  return {attempt_state, result: {success: false, error_type}};
}

async function readJson(response, maxBytes) {
  const reader = response.body?.getReader();
  if (!reader) throw new Error('Missing completion body');
  const decoder = new TextDecoder('utf-8', {fatal: true});
  let text = '';
  let size = 0;
  try {
    for (;;) {
      const {done, value} = await reader.read();
      if (done) return JSON.parse(text + decoder.decode());
      size += value.byteLength;
      if (size > maxBytes) throw new Error('Completion exceeds byte budget');
      text += decoder.decode(value, {stream: true});
    }
  } finally {
    await reader.cancel().catch(() => {});
  }
}

function tokens(value) {
  if (!Number.isInteger(value) || value < 0 || value > 0x7fffffff) {
    throw new Error('Invalid completion usage');
  }
  return value;
}

async function completeSummary(dest, endpoint, context, signal) {
  const {body, api_key: key, provider, remaining_ms} = context ?? {};
  if (context?.endpoint !== endpoint || typeof key !== 'string' || !key || key.length > 2048 ||
      !/^[A-Za-z0-9._-]{1,50}$/.test(provider ?? '') ||
      !Number.isSafeInteger(remaining_ms) || remaining_ms < 1 || remaining_ms > 30_000 ||
      typeof body?.model !== 'string' || !body.model ||
      body.model.length > 100 || body.stream !== false || body.store !== false ||
      !Array.isArray(body.messages) || body.messages.length !== 2 ||
      !body.messages.every((msg, index) =>
        msg?.role === (index === 0 ? 'system' : 'user') && typeof msg.content === 'string') ||
      Object.keys(body).some((key) => !['model', 'messages', 'stream', 'store', 'max_completion_tokens', 'max_tokens'].includes(key)) ||
      [body.max_tokens, body.max_completion_tokens].some((value) =>
        value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) ||
      (body.max_tokens !== undefined && body.max_completion_tokens !== undefined)) {
    return failed('not_started', 'SummaryRequestRejected');
  }
  const request = JSON.stringify(body);
  if (new TextEncoder().encode(request).byteLength > REQUEST_BYTES) {
    return failed('not_started', 'SummaryRequestTooLarge');
  }
  if (signal.aborted) return failed('not_started', 'SummaryDeadlineElapsed');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), remaining_ms);
  try {
    const response = await fetch(dest, {
      method: 'POST', redirect: 'manual',
      headers: {Authorization: 'Bearer ' + key, 'Content-Type': 'application/json'},
      body: request, signal: AbortSignal.any([signal, controller.signal]),
    });
    if (!response.ok) {
      await response.body?.cancel();
      return failed('completed', 'SummaryProviderRejected');
    }
    try {
      const completion = await readJson(response, RESPONSE_BYTES);
      const content = completion.choices?.[0]?.message?.content;
      if (typeof content !== 'string' || content.includes(key) || completion.model?.includes?.(key) ||
          typeof completion.model !== 'string' || !completion.model || completion.model.length > 100) {
        throw new Error('Malformed chat completion');
      }
      const usage = completion.usage;
      return {attempt_state: 'completed', result: {
        success: true, content, model: completion.model, provider,
        input_tokens: tokens(usage?.prompt_tokens),
        output_tokens: tokens(usage?.completion_tokens),
        cache_read_tokens: usage?.prompt_tokens_details?.cached_tokens == null ? null : tokens(usage.prompt_tokens_details.cached_tokens),
        reasoning_tokens: usage?.completion_tokens_details?.reasoning_tokens == null ? null : tokens(usage.completion_tokens_details.reasoning_tokens),
      }};
    } catch {
      if (signal.aborted || controller.signal.aborted) return failed('uncertain', 'SummaryInterrupted');
      return failed('completed', 'InvalidSummaryCompletion');
    }
  } catch {
    return failed('uncertain', 'SummaryInterrupted');
  } finally {
    clearTimeout(timer);
  }
}
