const REQUEST_BYTES = 256 * 1024;
const RESPONSE_BYTES = 64 * 1024;

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

export async function completeSummary(base, context, cancellation) {
  let url;
  try {
    if (!base || /[?#\\\s]/.test(base)) throw new Error();
    url = new URL(base);
    if (url.protocol !== 'https:' || url.username || url.password) throw new Error();
  } catch {
    return failed('not_started', 'SummaryProviderDisabled');
  }
  const endpoint = base.replace(/\/+$/, '');
  const dest = new URL(endpoint + '/chat/completions').href;
  const {body, api_key: key, remaining_ms} = context ?? {};
  if (context?.endpoint !== endpoint || typeof key !== 'string' || !key || key.length > 2048 ||
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
  const controller = new AbortController();
  const cancelReader = cancellation.getReader();
  const abort = () => controller.abort();
  // The launcher closes its outbound byte stream on terminate, independent
  // of HTTP Request.signal support on either Worker.
  void cancelReader.read().then(abort, abort);
  const timer = setTimeout(() => controller.abort(), remaining_ms);
  try {
    const response = await fetch(dest, {
      method: 'POST', redirect: 'manual',
      headers: {Authorization: 'Bearer ' + key, 'Content-Type': 'application/json'},
      body: request, signal: controller.signal,
    });
    if (!response.ok) {
      await response.body?.cancel();
      return failed('completed', 'SummaryProviderRejected');
    }
    try {
      const completion = await readJson(response, RESPONSE_BYTES);
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
      if (controller.signal.aborted) return failed('uncertain', 'SummaryInterrupted');
      return failed('completed', 'InvalidSummaryCompletion');
    }
  } catch {
    return failed('uncertain', 'SummaryInterrupted');
  } finally {
    clearTimeout(timer);
    await cancelReader.cancel().catch(() => {});
  }
}
