// Run credentials of an attempt, held by its TenantHost and never by the
// engine child. Pure (no cloudflare:workers) so it is unit-tested in Node.
//
// Credential protocol 2 gives an attempt two tokens: a short-lived access
// token and a refresh token. The child only ever holds an opaque handle, in
// both of its SDK credential slots. The gateway never replays a request. The
// SDK already owns challenge-driven refresh (api/bifrost/_refreshing.py): it
// replays only after the API's `refresh_required` challenge, which the API
// raises before any handler runs, within a per-call budget. So the gateway:
//   1. forwards each SDK request once and, when the API answers with that
//      challenge, records it against the token that was refused;
//   2. answers the SDK's own POST /auth/refresh (whose body carries the
//      handle) by refreshing the real tokens here, single-flight, and only
//      when the current token was actually challenged. The reply hands the
//      same handle back, so the SDK replays under its own rules.
// /auth/refresh rotates the refresh token, so two concurrent refreshes with
// one token would invalidate the session: the single-flight is load-bearing.

// The API's refresh challenge, matched exactly as the SDK matches it.
export function isRefreshChallenge(response) {
  if (response.status !== 401) return false;
  const challenge = response.headers.get('www-authenticate') ?? '';
  return challenge.includes('error="invalid_token"') && challenge.includes('error_description="refresh_required"');
}

export class AttemptCredentials {
  #records = new Map();
  #refresh;
  #now;

  // refresh(refreshToken) -> {access_token, refresh_token} | 'refused' | 'unavailable'.
  // 'refused' (the API answered 401/403) ends refresh for the attempt;
  // 'unavailable' (5xx, network) keeps the refresh token for a later try.
  constructor({refresh, now = () => Date.now()}) {
    this.#refresh = refresh;
    this.#now = now;
  }

  // An attempt's credentials live exactly as long as its deadline.
  open(handle, {token, refreshToken = null, expiresAt}) {
    this.#records.set(handle, {token, refreshToken, expiresAt, challenged: null, refreshing: null});
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

  // The API challenged `refused`. Only the token still in use is marked, so a
  // late challenge for an already-replaced token cannot trigger a refresh.
  noteChallenge(handle, refused) {
    const record = this.#live(handle);
    if (record && record.token === refused) record.challenged = refused;
  }

  // The SDK asked to refresh. 'refreshed' and 'current' both let it replay
  // with the same handle; 'refused' and 'gone' end its refresh attempts;
  // 'unavailable' fails this call without poisoning later ones.
  async refresh(handle) {
    const record = this.#live(handle);
    if (!record) return 'gone';
    if (record.refreshing) return this.#settle(handle, record, await record.refreshing);
    // Nothing challenged the token in use: no upstream call, the token works.
    if (record.challenged !== record.token) return 'current';
    if (!record.refreshToken) return 'refused';
    record.refreshing = (async () => {
      try {
        const next = await this.#refresh(record.refreshToken);
        // A 200 without both tokens is the API misbehaving, not a refusal.
        if (next === 'unavailable' || (next !== 'refused' && (typeof next?.access_token !== 'string' ||
            typeof next?.refresh_token !== 'string'))) {
          return 'unavailable';
        }
        if (next === 'refused') {
          // A refused refresh is terminal for this generation: never resend it.
          record.refreshToken = null;
          return 'refused';
        }
        record.token = next.access_token;
        record.refreshToken = next.refresh_token;
        record.challenged = null;
        return 'refreshed';
      } finally {
        record.refreshing = null;
      }
    })();
    return this.#settle(handle, record, await record.refreshing);
  }

  // An attempt that ended or expired while its refresh was in flight gets
  // nothing: its credentials must not outlive its deadline.
  #settle(handle, record, outcome) {
    return this.#live(handle) === record ? outcome : 'gone';
  }
}

// The SDK's POST /auth/refresh body is {"refresh_token": "<handle>"}. Read at
// most REFRESH_BODY_LIMIT bytes, however the body is framed.
export const REFRESH_BODY_LIMIT = 4096;

export async function refreshRequestHandle(request) {
  if (request.body === null) return null;
  const reader = request.body.getReader();
  const chunks = [];
  let size = 0;
  for (;;) {
    const {done, value} = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > REFRESH_BODY_LIMIT) {
      await reader.cancel();
      return null;
    }
    chunks.push(value);
  }
  try {
    const body = JSON.parse(new TextDecoder().decode(concat(chunks)));
    return typeof body?.refresh_token === 'string' ? body.refresh_token : null;
  } catch {
    return null;
  }
}

function concat(chunks) {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

// The reply the SDK expects from /auth/refresh (api/src/routers/auth.py Token),
// carrying the handle instead of either real token.
export function refreshReply(outcome, handle) {
  if (outcome === 'refreshed' || outcome === 'current') {
    return Response.json({access_token: handle, refresh_token: handle, token_type: 'bearer'});
  }
  // Not a 401: the SDK only remembers a rejected generation on 401, so a
  // later call can still refresh once the API is back.
  if (outcome === 'unavailable') return Response.json({detail: 'Refresh temporarily unavailable'}, {status: 503});
  return Response.json({detail: 'Invalid or expired refresh token'},
    {status: 401, headers: {'WWW-Authenticate': 'Bearer'}});
}
