// Engine-mode tenant host and its outbound gateway.
//
// TenantHost is one Durable Object per tenant. Being a single instance, it is
// the only place where "one long-lived engine child per tenant" actually holds:
// LOADER.get caches isolates per machine, and two supervisor requests are never
// guaranteed to share one, so a supervisor-side LOADER.get keeps paying the
// 5-10 s Pyodide boot whenever a request lands elsewhere. The host:
//   * creates the child with LOADER.get keyed by the engine digest,
//   * serializes attempts for its tenant (Pyodide is single-threaded),
//   * holds each attempt's run token IN MEMORY under an opaque handle, for the
//     attempt's duration only (never written to storage),
//   * keeps the child warm with an alarm for WARM_WINDOW_MS after the last
//     attempt, so a re-boot after eviction happens off the request path.
//
// TenantApi is the child's globalOutbound. It is created with props naming the
// tenant, so a handle can only be resolved by that tenant's own host. It
// rewrites the virtual API host, strips cookies, and refuses any path outside
// the SDK capability surface (apiTarget).
import {DurableObject, WorkerEntrypoint} from 'cloudflare:workers';
import {apiTarget} from '../executor/policy.mjs';
import {engineDigest, mergePrimeList, tenantEngineCode} from './generic-worker-code.mjs';
import {readBudgetedJson} from '../executor/output-budget.mjs';
import {AttemptCredentials, isRefreshChallenge, refreshReply, refreshRequestHandle} from './attempt-credentials.mjs';

const HANDLE_RE = /^run_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const VIRTUAL_API_ORIGIN = 'https://test-api.invalid';
const MAX_NOTED_CALLS = 64;
// Warm-pool policy: a tenant that ran in the last WARM_WINDOW_MS keeps a
// resident child. This is the knob that trades idle cost for latency, the
// counterpart of the Linux process pool's always-warm workers.
export const WARM_WINDOW_MS = 20 * 60 * 1000;
export const WARM_PING_MS = 45 * 1000;
// Matches the Linux pool's default per-workflow timeout (src/config.py);
// ATTEMPT_DEADLINE_MS overrides it per deploy.
const DEFAULT_ATTEMPT_DEADLINE_MS = 1800 * 1000;
const DEADLINE = Symbol('attempt-deadline');
// Bookkeeping writes (warm window, alarms, warm-up list, retirement, marker
// cleanup) never hold outgoing messages at the output gate. A held send would
// leave after its liveness check had passed, possibly after the deadline.
// Losing one of them in a crash is safe: the in-flight marker, the one gated
// write, makes the next instance retire the generation anyway.
const UNGATED = Object.freeze({allowUnconfirmed: true});

export class TenantHost extends DurableObject {
  // Run credentials stay here; the child holds an opaque handle. Refresh is
  // the gateway's job (credential protocol 2), so the child never sees the
  // refresh token either.
  #credentials = new AttemptCredentials({refresh: (refreshToken) => this.#refreshUpstream(refreshToken)});
  #calls = new Map();
  #current = null;
  #generation;
  #tail = Promise.resolve();
  // The generation whose child last answered a ping. In memory on purpose: a
  // fresh or reset host instance must re-confirm its child before it accepts
  // an attempt (see run()), and a late answer from a retired generation can
  // never vouch for its replacement.
  #readyGeneration = null;

  async #engine(tenantId) {
    const engineId = `tenant-eng-${tenantId}-${(await engineDigest(this.env.RUNTIME_SHA256)).slice(0, 16)}`;
    // The 15 MB runtime is decoded only here, i.e. only when a child is
    // actually created, never on the warm path.
    const cpuMs = Number(this.env.ENGINE_CPU_MS) || undefined;
    const subRequests = Number(this.env.ENGINE_SUBREQUESTS) || undefined;
    // The generation changes when a child is retired (attempt deadline), so a
    // wedged isolate is never handed back by the loader cache. One value keys
    // the loader and rides in the outbound gateway's props, so a retired child
    // that is still running can never act for a later attempt.
    const generation = await this.#currentGeneration();
    return {generation, worker: this.env.LOADER.get(
      `${engineId}-cpu${cpuMs ?? 'default'}-sr${subRequests ?? 'default'}-g${generation}`,
      () => tenantEngineCode(this.ctx.exports.TenantApi({props: {tenantId, generation}}),
        {cpuMs, subRequests}))};
  }

  // A host instance that starts while storage still records an attempt in
  // flight was reset mid-attempt: that attempt's child may still be running,
  // so its generation is retired before any child is used again.
  async #currentGeneration() {
    if (this.#generation === undefined) {
      const state = await this.ctx.storage.get(['generation', 'inflight']);
      const stored = state.get('generation') ?? 0;
      const inflight = state.get('inflight');
      if (inflight) {
        this.#generation = Math.max(stored, inflight.generation) + 1;
        await this.ctx.storage.put('generation', this.#generation);
        await this.ctx.storage.delete('inflight', UNGATED);
      } else {
        this.#generation = stored;
      }
    }
    return this.#generation;
  }

  // A child that blew its attempt deadline may be wedged (observed: a
  // subrequest-limit exception lost inside the synchronous import bridge
  // deadlocks the isolate). Never reuse it: next attempt warms a new one.
  async #retireChild() {
    this.#generation = (await this.#currentGeneration()) + 1;
    await this.ctx.storage.put('generation', this.#generation, UNGATED);
    this.#readyGeneration = null;
  }

  async #primeList() {
    const engine = await engineDigest(this.env.RUNTIME_SHA256);
    const stored = await this.ctx.storage.get('prime');
    return stored?.engine === engine ? stored.modules : [];
  }

  async #learnImports(imported) {
    if (!Array.isArray(imported) || imported.length === 0) return;
    const engine = await engineDigest(this.env.RUNTIME_SHA256);
    const stored = await this.ctx.storage.get('prime');
    const merged = mergePrimeList(stored, engine, imported);
    if (stored?.engine !== engine || merged.modules.length !== stored.modules.length) {
      await this.ctx.storage.put('prime', merged, UNGATED);
    }
  }

  #childIsReady() {
    return this.#readyGeneration !== null && this.#readyGeneration === this.#generation;
  }

  async #refreshUpstream(refreshToken) {
    let response;
    try {
      response = await fetch(new URL('/auth/refresh', this.env.API_ORIGIN), {
        method: 'POST', headers: {'content-type': 'application/json'},
        body: JSON.stringify({refresh_token: refreshToken}), redirect: 'manual',
      });
    } catch {
      return 'unavailable';
    }
    if (response.status === 200) return response.json();
    await response.body?.cancel();
    // Only the API refusing the refresh token ends refresh for the attempt.
    return response.status === 401 || response.status === 403 ? 'refused' : 'unavailable';
  }

  // An SDK call from this tenant's child, sent to the API with the attempt's
  // real token. The liveness check and the start of the upstream fetch happen
  // in one synchronous turn of this object, so an attempt that has ended or
  // expired can never start a new request; one already started may finish.
  // Sent exactly once: replay after a refresh challenge is the SDK's call.
  async forward(handle, request, target) {
    const started = Date.now();
    const token = this.#credentials.token(handle);
    if (!token) {
      this.noteEgress({method: request.method, path: new URL(target).pathname, status: 401,
        denied: 'unknown handle'});
      return new Response('unknown handle', {status: 401});
    }
    const headers = new Headers(request.headers);
    headers.set('Authorization', `Bearer ${token}`);
    headers.delete('cookie');
    const response = await fetch(new Request(target, {method: request.method, headers,
      body: request.body, redirect: 'manual'}));
    const challenged = isRefreshChallenge(response);
    if (challenged) this.#credentials.noteChallenge(handle, token);
    this.noteCall(handle, {method: request.method, path: new URL(target).pathname,
      status: response.status, ms: Date.now() - started, ...(challenged ? {challenged: true} : {})});
    return response;
  }

  // The workflow's own third-party HTTPS request, started in the same turn
  // as its permission check (see forward()).
  async egress(request, generation) {
    const url = new URL(request.url);
    const note = {method: request.method, host: url.hostname, path: url.pathname};
    if (!this.#egressAllowed(url.hostname, generation)) {
      this.noteEgress({...note, status: 403, denied: 'egress not declared'});
      return new Response('egress not declared', {status: 403});
    }
    const started = Date.now();
    const response = await fetch(request);
    this.noteEgress({...note, status: response.status, ms: Date.now() - started});
    return response;
  }

  // The SDK's own POST /auth/refresh, answered for the handle (see
  // attempt-credentials.mjs): 'refreshed' | 'current' | 'refused' | 'gone'.
  refresh(handle) {
    return this.#credentials.refresh(handle);
  }

  // The outbound gateway reports each SDK call (path without query, status,
  // latency to response headers) so an attempt's API time is attributable.
  noteCall(handle, call) {
    const calls = this.#calls.get(handle);
    if (calls && calls.length < MAX_NOTED_CALLS) calls.push(call);
  }

  // Third-party egress carries no handle (it is the workflow's own request),
  // so it is attributed to the attempt in flight: attempts are serialized per
  // host, so there is at most one. Allowed only for hosts that attempt
  // declared; nothing is allowed between attempts.
  #egressAllowed(hostname, generation) {
    const current = this.#current;
    return current !== null && generation === current.generation && Date.now() < current.deadlineAt &&
      current.egress.has(String(hostname).toLowerCase());
  }

  noteEgress(call) {
    if (this.#current) this.noteCall(this.#current.handle, call);
  }

  async run({tenantId, executionId, context, publicUrl, egressHosts = [], dispatchedAt}) {
    // Booting a child inside a fresh host can reset the host
    // (`code_updated`, retryable) even though the boot itself completes.
    // An attempt must never be in flight when that happens, because it cannot
    // be safely retried. So a host that has not confirmed its child refuses
    // before doing anything; the supervisor warms it (idempotent, retryable)
    // and dispatches once.
    if (!this.#childIsReady()) return {needsWarm: true, attempt_state: 'not_started'};
    // The deadline runs from the supervisor's dispatch (one value across its
    // warm-up retry), so queueing and warm-up count against it.
    const deadlineMs = Number(this.env.ATTEMPT_DEADLINE_MS) || DEFAULT_ATTEMPT_DEADLINE_MS;
    const now = Date.now();
    const deadlineAt = (Number.isFinite(dispatchedAt) && dispatchedAt <= now ? dispatchedAt : now) + deadlineMs;
    const expiredInQueue = (queueWaitMs) => ({terminal: {phase: 'attempt-deadline',
      attempt_state: 'not_started', deadline_ms: deadlineMs, queue_wait_ms: queueWaitMs, api_calls: []}});
    // Waiting stops at the deadline. The next attempt still waits for the
    // predecessor, so no two attempts ever overlap in the child.
    const previous = this.#tail;
    let release;
    const mine = new Promise((resolve) => { release = resolve; });
    this.#tail = previous.then(() => mine);
    const queuedAt = Date.now();
    let queueTimer;
    const turn = await Promise.race([
      previous.then(() => 'turn'),
      new Promise((resolve) => { queueTimer = setTimeout(() => resolve('expired'), Math.max(0, deadlineAt - now)); }),
    ]).finally(() => clearTimeout(queueTimer));
    const queueWaitMs = Date.now() - queuedAt;
    if (turn === 'expired' || Date.now() >= deadlineAt) {
      // Expired in the queue: the child never saw it, so it is safe to retry.
      release();
      return expiredInQueue(queueWaitMs);
    }
    if (!this.#childIsReady()) {
      // The predecessor retired the child: this attempt needs a confirmed
      // warm-up like any other, and has done no work.
      release();
      return {needsWarm: true, attempt_state: 'not_started'};
    }
    // The child carries only an opaque handle. Neither run token (access or
    // refresh) enters it, and the handle dies with the attempt's deadline.
    const handle = `run_${crypto.randomUUID()}`;
    this.#credentials.open(handle, {token: context.engine_token,
      refreshToken: context.engine_refresh_token ?? null, expiresAt: deadlineAt});
    const {engine_refresh_token: _refresh, ...childContext} = context;
    this.#calls.set(handle, []);
    try {
      const {worker, generation} = await this.#engine(tenantId);
      if (Date.now() >= deadlineAt) return expiredInQueue(Date.now() - queuedAt);
      this.#current = {handle, generation, deadlineAt,
        egress: new Set(egressHosts.map((h) => h.toLowerCase()))};
      // Survives a host reset: the next instance retires this generation. The
      // only gated write: it is made durable before the final deadline check,
      // so nothing is left to hold the child dispatch at the output gate.
      await this.ctx.storage.put('inflight', {generation});
      await this.ctx.storage.sync();
      if (Date.now() >= deadlineAt) {
        // The marker write crossed the deadline; the child has seen nothing.
        this.#current = null;
        return expiredInQueue(Date.now() - queuedAt);
      }
      const mark = Date.now();
      let timer;
      const expired = new Promise((resolve) => {
        timer = setTimeout(() => resolve(DEADLINE), Math.max(0, deadlineAt - Date.now()));
      });
      const response = await Promise.race([
        worker.getEntrypoint().fetch(new Request('https://attempt.invalid/', {
          method: 'POST',
          body: JSON.stringify({execution_id: executionId,
            context: {...childContext, engine_token: handle},
            api_url: 'https://test-api.invalid',
            public_url: publicUrl ?? 'https://staging.invalid'}),
        })),
        expired,
      ]).finally(() => clearTimeout(timer));
      if (response === DEADLINE) {
        // The attempt started and may still be running in the old child: its
        // effects are unknown. The handle and the child's generation are
        // revoked below, so it can no longer reach the API or egress.
        await this.#retireChild();
        return {terminal: {phase: 'attempt-deadline', attempt_state: 'uncertain',
          deadline_ms: deadlineMs, api_calls: this.#calls.get(handle)}};
      }
      const childMs = Date.now() - mark;
      let output;
      try {
        output = await readBudgetedJson(response);
      } catch (error) {
        // Error subclasses do not survive RPC, so name the outcome here.
        if (error?.name !== 'ExecutorBodyTooLargeError') throw error;
        return {terminal: {phase: 'output-budget', attempt_state: 'completed',
          error_type: 'ExecutorBodyTooLargeError',
          response_bytes: error.observedBytes, retention_limit_bytes: error.limitBytes,
          api_calls: this.#calls.get(handle)}};
      }
      // The import trace feeds the next child's warm-up; only its size is
      // reported upward.
      const {imported, ...child} = output.child ?? {};
      await this.#learnImports(imported);
      return {output: output.upstream_result, queueWaitMs, attempt_state: 'completed',
        timings: {child_ms: childMs,
          child: output.child ? {...child, imported: Array.isArray(imported) ? imported.length : 0} : null,
          api_calls: this.#calls.get(handle)}};
    } finally {
      this.#current = null;
      this.#credentials.close(handle);
      this.#calls.delete(handle);
      await this.ctx.storage.delete('inflight', UNGATED);
      release();
      await this.ctx.storage.put({tenantId, lastAttempt: Date.now()}, UNGATED);
      if (await this.ctx.storage.getAlarm() === null) {
        await this.ctx.storage.setAlarm(Date.now() + WARM_PING_MS, UNGATED);
      }
    }
  }

  // Runs fn with no attempt in flight on this host, and holds attempts back
  // until it finishes: the child is single-threaded and the engine's SDK
  // credential is process-wide, so nothing that awaits inside the child may
  // interleave with an attempt.
  async #exclusive(fn) {
    const previous = this.#tail;
    let release;
    this.#tail = new Promise((resolve) => { release = resolve; });
    await previous;
    try {
      return await fn();
    } finally {
      release();
    }
  }

  // Boots (or touches) the child with no attempt, token or tenant data.
  // With exercise, the child also runs one dry engine attempt under a handle
  // this host never registered: TenantApi refuses its first SDK call (401,
  // "unknown handle") before anything reaches the API. That keeps the engine's
  // pre-first-call path hot between real attempts. Callers must hold
  // #exclusive when exercising.
  async #ping(tenantId, {exercise = false} = {}) {
    const {worker, generation} = await this.#engine(tenantId);
    const modules = await this.#primeList();
    const mark = Date.now();
    const response = await worker.getEntrypoint().fetch(new Request('https://attempt.invalid/', {
      method: 'POST', body: JSON.stringify({action: 'ping', modules,
        ...(exercise ? {exercise: {handle: `run_${crypto.randomUUID()}`}} : {})}),
    }));
    const {first_request: firstRequest, primed, prime_skipped: primeSkipped,
      exercise: exercised} = await response.json();
    // Only the generation that answered is ready; if it was retired while
    // this ping was in flight, its answer vouches for nothing.
    if (generation === this.#generation) this.#readyGeneration = generation;
    // Keep the connection to the API origin warm too: a fresh child's first
    // SDK calls measured ~2x slower (104-126 ms vs ~50 ms). Unauthenticated
    // health check, no tenant data; bounded by the warm window.
    const originMark = Date.now();
    const origin = await fetch(new URL('/health/ready', this.env.API_ORIGIN), {redirect: 'manual'})
      .then((r) => { r.body?.cancel(); return r.status; }, () => 0);
    return {child_ms: originMark - mark, first_request: firstRequest,
      prime_listed: modules.length, primed, prime_skipped: primeSkipped,
      ...(exercise ? {exercise: exercised ?? null} : {}),
      origin_status: origin, origin_ms: Date.now() - originMark};
  }

  // Diagnostic: one exercising ping on demand, serialized like the alarm's.
  async exercise({tenantId}) {
    return this.#exclusive(() => this.#ping(tenantId, {exercise: true}));
  }

  // Diagnostic: module names loaded in this tenant's child (no tenant data).
  async inventory({tenantId}) {
    const {worker} = await this.#engine(tenantId);
    const response = await worker.getEntrypoint().fetch(new Request('https://attempt.invalid/', {
      method: 'POST', body: JSON.stringify({action: 'inventory'}),
    }));
    return response.json();
  }

  // Pre-warm: a dispatcher that knows an attempt is coming can pay the boot
  // ahead of it. Opens the same warm window an attempt does.
  async warm({tenantId}) {
    try {
      return await this.#ping(tenantId);
    } finally {
      await this.ctx.storage.put({tenantId, lastAttempt: Date.now()}, UNGATED);
      if (await this.ctx.storage.getAlarm() === null) {
        await this.ctx.storage.setAlarm(Date.now() + WARM_PING_MS, UNGATED);
      }
    }
  }

  async alarm() {
    const state = await this.ctx.storage.get(['tenantId', 'lastAttempt']);
    const tenantId = state.get('tenantId');
    const lastAttempt = state.get('lastAttempt') ?? 0;
    // Outside the window the host lets the child (and itself) go cold.
    if (!tenantId || Date.now() - lastAttempt > WARM_WINDOW_MS) return;
    try {
      // Keep-warm pings run off the request path, so they also exercise the
      // engine. Warm-ups on the request path (warm()) do not: an attempt is
      // waiting on them.
      await this.#exclusive(() => this.#ping(tenantId, {exercise: true}));
    } finally {
      await this.ctx.storage.setAlarm(Date.now() + WARM_PING_MS, UNGATED);
    }
  }
}

export class TenantApi extends WorkerEntrypoint {
  async fetch(request) {
    const tenantId = this.ctx.props?.tenantId;
    if (typeof tenantId !== 'string' || tenantId === '') {
      return new Response('unknown tenant', {status: 401});
    }
    const host = this.ctx.exports.TenantHost.getByName(tenantId);
    const url = new URL(request.url);
    if (url.origin !== VIRTUAL_API_ORIGIN) return this.#egress(host, request, url);
    if (request.method === 'POST' && url.pathname === '/auth/refresh') return this.#refresh(host, request);

    // Every refusal is noted against the attempt in flight, so a call the
    // gateway turned away is visible in the breakdown instead of vanishing.
    const deny = async (status, reason) => {
      await host.noteEgress({method: request.method, path: url.pathname, status, denied: reason});
      return new Response(reason, {status});
    };
    const authorization = request.headers.get('Authorization') ?? '';
    const handle = authorization.startsWith('Bearer ') ? authorization.slice(7) : '';
    if (!HANDLE_RE.test(handle)) return deny(401, 'opaque handle required');
    let target;
    try {
      target = apiTarget(request.url, this.env.API_ORIGIN, {scope: this.env.SDK_SCOPE});
    } catch {
      return deny(403, 'API capability denied');
    }
    // RPC carries strings, not URL objects.
    return host.forward(handle, request, String(target));
  }

  // The SDK refreshing its "token", i.e. the handle it holds in both slots.
  // Never forwarded: the host refreshes the real tokens if the API challenged
  // the current one, and the reply hands back the same handle.
  async #refresh(host, request) {
    const handle = await refreshRequestHandle(request);
    const outcome = HANDLE_RE.test(handle ?? '') ? await host.refresh(handle) : 'gone';
    const reply = refreshReply(outcome, handle);
    if (outcome !== 'gone') {
      await host.noteCall(handle, {method: 'POST', path: '/auth/refresh', status: reply.status, refresh: outcome});
    }
    return reply;
  }

  // The workflow's own third-party HTTPS request: forwarded as-is (its own
  // headers, never the run token) when the attempt in flight declared the host.
  async #egress(host, request, url) {
    const note = {method: request.method, host: url.hostname, path: url.pathname};
    if (url.protocol !== 'https:' || url.username || url.password || url.port) {
      await host.noteEgress({...note, status: 403, denied: 'egress not declared'});
      return new Response('egress not declared', {status: 403});
    }
    return host.egress(request, this.ctx.props?.generation);
  }
}
