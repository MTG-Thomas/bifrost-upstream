// Supervisor dispatch helpers, kept out of tenant-entry.mjs on purpose: workerd
// treats every named export of the main module as an entrypoint and refuses to
// start when one is a plain value (observed: `WARM_ATTEMPTS` broke local
// `wrangler dev`). The entry module exports only its handler and classes.

// Deployed spike auth: X-Spike-Auth must equal SPIKE_AUTH inside an unexpired
// SPIKE_EXPIRES_AT window. Both sides are hashed first, so the comparison runs
// over two fixed-length digests in constant time and leaks neither the
// secret's content nor its length through response timing.
export async function spikeAuthorized(request, env) {
  if (typeof env.SPIKE_AUTH !== 'string' || env.SPIKE_AUTH === '') return false;
  if (!(Date.now() < Number(env.SPIKE_EXPIRES_AT))) return false;
  const presented = request.headers.get('X-Spike-Auth') ?? '';
  const [a, b] = await Promise.all([presented, env.SPIKE_AUTH].map((value) =>
    crypto.subtle.digest('SHA-256', new TextEncoder().encode(value))));
  const left = new Uint8Array(a);
  const right = new Uint8Array(b);
  let difference = 0;
  for (let i = 0; i < left.length; i++) difference |= left[i] ^ right[i];
  return difference === 0;
}

// Per-tenant serialization chains for the data-only (non-engine) actions.
// Module-level: one supervisor isolate holds one chain per tenant id. Engine
// attempts are serialized by their TenantHost instead.
const tails = new Map();
const seenTenants = new Set();

export function resetTenantState() {
  tails.clear();
  seenTenants.clear();
}

// Whether this supervisor isolate has already served the tenant (data-only
// mode's reuse signal), recording it as served.
export function markTenantSeen(tenantId) {
  const seen = seenTenants.has(tenantId);
  seenTenants.add(tenantId);
  return seen;
}

// Serializes fn behind the tenant's chain. Resolves with {result, queueWaitMs}
// where queueWaitMs is the time spent waiting for the previous attempt on the
// same tenant. A rejection in one attempt never breaks the chain for later ones.
export async function enqueueTenant(tenantId, fn) {
  const prev = tails.get(tenantId) ?? Promise.resolve();
  const enqueuedAt = Date.now();
  const cur = prev.then(async () => {
    const queueWaitMs = Date.now() - enqueuedAt;
    return {result: await fn(), queueWaitMs};
  });
  tails.set(tenantId, cur.catch(() => {}));
  return cur;
}

// Platform failure classes, matched against the runtime's own messages and
// reported as a fixed slug: diagnosable without ever echoing exception text.
const RUNTIME_ERROR_CLASSES = [
  ['cpu_limit', /exceeded CPU time limit/i],
  ['memory_limit', /exceeded (its )?memory limit/i],
  ['code_updated', /code (was|has been) updated/i],
  ['overloaded', /overloaded/i],
  ['connection_lost', /network connection lost|disconnected/i],
  ['internal', /internal error/i],
];

export function runtimeErrorClass(error) {
  const message = String(error?.message ?? '');
  const match = RUNTIME_ERROR_CLASSES.find(([, pattern]) => pattern.test(message));
  return {error_class: match ? match[0] : 'other',
    retryable: error?.retryable === true, overloaded: error?.overloaded === true};
}

// Warm-up is idempotent and carries no attempt data, so a retryable platform
// failure (observed: `code_updated` while a fresh host boots its child) is
// retried with a fresh stub, as broken DO stubs must not be reused.
export const WARM_ATTEMPTS = 3;
export const ENGINE_BOOT_TIMEOUT_MS = 15_000;
export async function warmHost(getHost, tenantId, {check = () => {}, engineOnly = false} = {}) {
  for (let attempt = 1; ; attempt++) {
    check();
    try {
      return {...await getHost().warm({tenantId, ...(engineOnly ? {engineOnly} : {})}), warm_attempts: attempt};
    } catch (error) {
      if (error?.retryable !== true || attempt >= WARM_ATTEMPTS) throw error;
    }
  }
}

// Every engine outcome carries attempt_state: 'not_started' (safe to
// dispatch again), 'completed', or 'uncertain' (it may have run, with effects;
// never blindly redispatch). An error thrown by the attempt RPC itself is
// uncertain unless it is tagged not_started here.
//
// An engine attempt is dispatched at most once. A host that has not confirmed
// its child refuses with needsWarm before doing any attempt work; only the
// warm-up is retried.
export async function hostAttempt(getHost, attempt, {beforeRun = () => null, warm = warmHost} = {}) {
  const first = beforeRun() ?? await getHost().run(attempt);
  if (!first.needsWarm) return first;
  let warmed;
  try {
    warmed = await warm(getHost, attempt.tenantId);
  } catch (error) {
    // The host refused the attempt before any work and the warm-up failed:
    // nothing was dispatched, so the caller may safely retry elsewhere.
    error.attemptState = 'not_started';
    throw error;
  }
  const result = beforeRun() ?? await getHost().run(attempt);
  if (result.needsWarm) return {notReady: true, warm: warmed};
  return {...result, warm: warmed};
}

// The supervisor's engine path: one attempt through the tenant's host, shaped
// into {status, body}. Every body names its attempt_state.
export async function engineAttempt(getHost, attempt, {echoErrors = false} = {}) {
  const workerId = attempt.tenantId;
  const mark = Date.now();
  let hosted;
  try {
    // One dispatch time across the warm-up retry: the host's deadline runs
    // from here, not from whichever run() call finally starts the attempt.
    hosted = await hostAttempt(getHost, {...attempt, dispatchedAt: mark});
  } catch (error) {
    // Never echo exception strings (may contain workflow data or
    // credentials) -- except on a capability-lab deploy that opts in.
    return {status: 502, body: {phase: 'dynamic-runtime', error_type: error?.name ?? 'Error',
      attempt_state: error?.attemptState ?? 'uncertain',
      ...runtimeErrorClass(error), tenant_worker_id: workerId, api_calls: [],
      ...(echoErrors ? {lab_error: String(error?.message ?? error).slice(-600)} : {})}};
  }
  if (hosted.notReady) {
    // Warmed, yet the host still has not confirmed its child; the attempt was
    // never started.
    return {status: 502, body: {phase: 'engine-not-ready', attempt_state: 'not_started',
      tenant_worker_id: workerId, api_calls: []}};
  }
  if (hosted.terminal) {
    // e.g. output-budget or attempt-deadline; the host names the state.
    return {status: 502, body: {...hosted.terminal, tenant_worker_id: workerId}};
  }
  // Reuse is the child's own view: the supervisor's memory is lost whenever
  // the supervisor isolate is recycled.
  return {status: 200, body: {tenant_worker_id: workerId, attempt_state: 'completed',
    worker_reused: hosted.timings.child?.first_request === false,
    queue_wait_ms: hosted.queueWaitMs, output: hosted.output, api_calls: [],
    timings: {host_ms: Date.now() - mark, ...hosted.timings,
      ...(hosted.warm ? {warm: hosted.warm} : {})}}};
}
