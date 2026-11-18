// Run credentials of an attempt, held by its TenantHost and never by the
// engine child. Pure (no cloudflare:workers) so it is unit-tested in Node.
//
// Credential protocol 2 gives an attempt two tokens: a short-lived access
// token and a refresh token. The child only ever sees an opaque handle. When
// the API answers 401 (an expired access token: the protocol-2 "challenge"),
// the gateway refreshes here, once per handle at a time, and replays the
// request. Refresh tokens rotate, so two concurrent refreshes with the same
// token would revoke the session: refreshes are single-flight, and a caller
// that observed an older access token just gets the current one.

// Requests up to this size are buffered so a 401 can be replayed after a
// refresh. Larger or unsized bodies stream once and are never replayed.
export const REPLAY_BODY_LIMIT = 4 * 1024 * 1024;

export class AttemptCredentials {
  #records = new Map();
  #refresh;
  #now;

  // refresh(refreshToken) -> {access_token, refresh_token} | null
  constructor({refresh, now = () => Date.now()}) {
    this.#refresh = refresh;
    this.#now = now;
  }

  // An attempt's credentials live exactly as long as its deadline.
  open(handle, {token, refreshToken = null, expiresAt}) {
    this.#records.set(handle, {token, refreshToken, expiresAt, refreshing: null});
  }

  close(handle) {
    this.#records.delete(handle);
  }

  #live(handle) {
    const record = this.#records.get(handle);
    return record && this.#now() < record.expiresAt ? record : null;
  }

  token(handle) {
    return this.#live(handle)?.token ?? null;
  }

  // The access token to retry with after `observed` was refused, or null when
  // there is nothing better (no refresh token, refresh refused, attempt over).
  async refreshed(handle, observed) {
    const record = this.#live(handle);
    if (!record) return null;
    if (record.token !== observed) return record.token;
    if (!record.refreshToken) return null;
    record.refreshing ??= (async () => {
      try {
        const next = await this.#refresh(record.refreshToken);
        if (!next || typeof next.access_token !== 'string' || typeof next.refresh_token !== 'string') {
          // A refused refresh is terminal for this generation: never resend it.
          record.refreshToken = null;
          return null;
        }
        record.token = next.access_token;
        record.refreshToken = next.refresh_token;
        return record.token;
      } finally {
        record.refreshing = null;
      }
    })();
    const token = await record.refreshing;
    return this.#records.get(handle) === record ? token : null;
  }
}

// Whether a request body can be held for one replay.
export function replayableLength(request) {
  if (request.body === null) return 0;
  // An absent header is unsized, not empty (Number(null) would read as 0).
  const header = request.headers.get('content-length');
  if (header === null || !/^\d+$/.test(header)) return null;
  const length = Number(header);
  return Number.isInteger(length) && length >= 0 && length <= REPLAY_BODY_LIMIT ? length : null;
}

// Sends an SDK request with the attempt's real token. On 401, when the body
// is replayable and a newer token can be had, replays exactly once with it:
// a 401 is refused before any handler runs, so the first send had no effect
// (the SDK itself retries after refreshing on the same assumption).
export async function forwardWithRefresh({request, target, token, fetchFn, refreshed}) {
  const replayable = replayableLength(request) !== null;
  const body = request.body === null ? null : replayable ? await request.arrayBuffer() : request.body;
  const send = (bearer) => {
    const headers = new Headers(request.headers);
    headers.set('Authorization', `Bearer ${bearer}`);
    headers.delete('cookie');
    return fetchFn(new Request(target, {method: request.method, headers, body, redirect: 'manual',
      // Required by the Fetch standard for a streamed body; Workers accept it.
      ...(body instanceof ReadableStream ? {duplex: 'half'} : {})}));
  };
  const first = await send(token);
  if (first.status !== 401 || !replayable) return {response: first, refreshed: false};
  const next = await refreshed(token);
  if (!next || next === token) return {response: first, refreshed: false};
  await first.body?.cancel();
  return {response: await send(next), refreshed: true};
}
