const REQUEST_BYTES = 256 * 1024;
const RESPONSE_BYTES = 64 * 1024;

export function summaryRunner(env) {
  const base = env.BIFROST_CLOUDFLARE_SUMMARY_ENDPOINT;
  if (!base) return undefined;
  const url = new URL(base);
  const key = env.BIFROST_CLOUDFLARE_SUMMARY_KEY;
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash ||
      typeof key !== 'string' || key.length === 0) {
    throw new Error('Invalid Cloudflare summary provider configuration');
  }
  const endpoint = base.replace(/\/+$/, '');
  const dest = new URL(endpoint + '/chat/completions').href;
  return (route, context, signal) => completeSummary(dest, endpoint, key, route, context, signal);
}

function failed(attempt_state, error_type) {
  return {attempt_state, result: {success: false, error_type}};
}

async function readCompletion(response) {
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
      if (size > RESPONSE_BYTES) throw new Error('Completion exceeds byte budget');
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

async function completeSummary(dest, endpoint, key, route, context, signal) {
  const body = context?.body;
  if (context?.endpoint !== endpoint || typeof body?.model !== 'string' || !body.model ||
      body.model.length > 100 || body.stream !== false || body.store !== false ||
      !Array.isArray(body.messages) || body.messages.length !== 2 ||
      !body.messages.every((msg, index) =>
        msg?.role === (index === 0 ? 'system' : 'user') && typeof msg.content === 'string') ||
      Object.keys(body).some((key) => !['model', 'messages', 'stream', 'store', 'max_completion_tokens'].includes(key)) ||
      (body.max_completion_tokens !== undefined &&
        (!Number.isSafeInteger(body.max_completion_tokens) || body.max_completion_tokens <= 0))) {
    return failed('not_started', 'SummaryRequestRejected');
  }
  const request = JSON.stringify(body);
  if (new TextEncoder().encode(request).byteLength > REQUEST_BYTES) {
    return failed('not_started', 'SummaryRequestTooLarge');
  }
  const remaining = Date.parse(route.deadline) - Date.now();
  if (signal.aborted || remaining <= 0) return failed('not_started', 'SummaryDeadlineElapsed');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), remaining);
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
      const completion = await readCompletion(response);
      const content = completion.choices?.[0]?.message?.content;
      if (typeof content !== 'string' ||
          typeof completion.model !== 'string' || !completion.model || completion.model.length > 100) {
        throw new Error('Malformed chat completion');
      }
      const usage = completion.usage;
      return {attempt_state: 'completed', result: {
        success: true, content, model: completion.model,
        input_tokens: tokens(usage?.prompt_tokens),
        output_tokens: tokens(usage?.completion_tokens),
        cache_read_tokens: tokens(usage?.prompt_tokens_details?.cached_tokens ?? 0),
        reasoning_tokens: tokens(usage?.completion_tokens_details?.reasoning_tokens ?? 0),
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
