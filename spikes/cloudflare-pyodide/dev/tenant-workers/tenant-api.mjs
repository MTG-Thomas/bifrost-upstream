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
import {engineDigest, tenantEngineCode} from './generic-worker-code.mjs';
import {readBudgetedJson} from '../executor/output-budget.mjs';

const HANDLE_RE = /^run_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const VIRTUAL_API_ORIGIN = 'https://test-api.invalid';
const TOKEN_TTL_MS = 10 * 60 * 1000;
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

export class TenantHost extends DurableObject {
  #tokens = new Map();
  #calls = new Map();
  #current = null;
  #generation;
  #tail = Promise.resolve();
  // In memory on purpose: a fresh or reset host instance must re-confirm its
  // child before it accepts an attempt (see run()).
  #childReady = false;

  async #engine(tenantId) {
    const engineId = `tenant-eng-${tenantId}-${(await engineDigest(this.env.RUNTIME_SHA256)).slice(0, 16)}`;
    // The 15 MB runtime is decoded only here, i.e. only when a child is
    // actually created, never on the warm path.
    const cpuMs = Number(this.env.ENGINE_CPU_MS) || undefined;
    const subRequests = Number(this.env.ENGINE_SUBREQUESTS) || undefined;
    // The generation changes when a child is retired (attempt deadline), so a
    // wedged isolate is never handed back by the loader cache.
    this.#generation ??= (await this.ctx.storage.get('generation')) ?? 0;
    return this.env.LOADER.get(
      `${engineId}-cpu${cpuMs ?? 'default'}-sr${subRequests ?? 'default'}-g${this.#generation}`,
      () => tenantEngineCode(this.ctx.exports.TenantApi({props: {tenantId}}), {cpuMs, subRequests}));
  }

  // A child that blew its attempt deadline may be wedged (observed: a
  // subrequest-limit exception lost inside the synchronous import bridge
  // deadlocks the isolate). Never reuse it: next attempt warms a new one.
  async #retireChild() {
    this.#generation = ((await this.ctx.storage.get('generation')) ?? 0) + 1;
    await this.ctx.storage.put('generation', this.#generation);
    this.#childReady = false;
  }

  // Resolves an attempt handle for this tenant's outbound gateway only.
  token(handle) {
    const record = this.#tokens.get(handle);
    if (!record || Date.now() >= record.expires) return null;
    return record.token;
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
  egressAllowed(hostname) {
    return this.#current !== null && this.#current.egress.has(String(hostname).toLowerCase());
  }

  noteEgress(call) {
    if (this.#current) this.noteCall(this.#current.handle, call);
  }

  async run({tenantId, executionId, context, publicUrl, egressHosts = []}) {
    // Booting a child inside a fresh host can reset the host
    // (`code_updated`, retryable) even though the boot itself completes.
    // An attempt must never be in flight when that happens, because it cannot
    // be safely retried. So a host that has not confirmed its child refuses
    // before doing anything; the supervisor warms it (idempotent, retryable)
    // and dispatches once.
    if (!this.#childReady) return {needsWarm: true};
    const previous = this.#tail;
    let release;
    this.#tail = new Promise((resolve) => { release = resolve; });
    const queuedAt = Date.now();
    await previous;
    const queueWaitMs = Date.now() - queuedAt;
    // The child carries only an opaque handle; the run token stays here.
    const handle = `run_${crypto.randomUUID()}`;
    this.#tokens.set(handle, {token: context.engine_token, expires: Date.now() + TOKEN_TTL_MS});
    this.#calls.set(handle, []);
    this.#current = {handle, egress: new Set(egressHosts.map((h) => h.toLowerCase()))};
    try {
      const worker = await this.#engine(tenantId);
      const mark = Date.now();
      const deadlineMs = Number(this.env.ATTEMPT_DEADLINE_MS) || DEFAULT_ATTEMPT_DEADLINE_MS;
      let timer;
      const expired = new Promise((resolve) => { timer = setTimeout(() => resolve(DEADLINE), deadlineMs); });
      const response = await Promise.race([
        worker.getEntrypoint().fetch(new Request('https://attempt.invalid/', {
          method: 'POST',
          body: JSON.stringify({execution_id: executionId,
            context: {...context, engine_token: handle},
            api_url: 'https://test-api.invalid',
            public_url: publicUrl ?? 'https://staging.invalid'}),
        })),
        expired,
      ]).finally(() => clearTimeout(timer));
      if (response === DEADLINE) {
        await this.#retireChild();
        return {terminal: {phase: 'attempt-deadline', deadline_ms: deadlineMs,
          api_calls: this.#calls.get(handle)}};
      }
      const childMs = Date.now() - mark;
      let output;
      try {
        output = await readBudgetedJson(response);
      } catch (error) {
        // Error subclasses do not survive RPC, so name the outcome here.
        if (error?.name !== 'ExecutorBodyTooLargeError') throw error;
        return {terminal: {phase: 'output-budget', error_type: 'ExecutorBodyTooLargeError',
          response_bytes: error.observedBytes, retention_limit_bytes: error.limitBytes,
          api_calls: this.#calls.get(handle)}};
      }
      return {output: output.upstream_result, queueWaitMs,
        timings: {child_ms: childMs, child: output.child ?? null,
          api_calls: this.#calls.get(handle)}};
    } finally {
      this.#current = null;
      this.#tokens.delete(handle);
      this.#calls.delete(handle);
      release();
      await this.ctx.storage.put({tenantId, lastAttempt: Date.now()});
      if (await this.ctx.storage.getAlarm() === null) {
        await this.ctx.storage.setAlarm(Date.now() + WARM_PING_MS);
      }
    }
  }

  // Boots (or touches) the child with no attempt, token or tenant data.
  async #ping(tenantId) {
    const worker = await this.#engine(tenantId);
    const mark = Date.now();
    const response = await worker.getEntrypoint().fetch(new Request('https://attempt.invalid/', {
      method: 'POST', body: JSON.stringify({action: 'ping'}),
    }));
    const {first_request: firstRequest} = await response.json();
    this.#childReady = true;
    // Keep the connection to the API origin warm too: a fresh child's first
    // SDK calls measured ~2x slower (104-126 ms vs ~50 ms). Unauthenticated
    // health check, no tenant data; bounded by the warm window.
    const originMark = Date.now();
    const origin = await fetch(new URL('/health/ready', this.env.API_ORIGIN), {redirect: 'manual'})
      .then((r) => { r.body?.cancel(); return r.status; }, () => 0);
    return {child_ms: originMark - mark, first_request: firstRequest,
      origin_status: origin, origin_ms: Date.now() - originMark};
  }

  // Diagnostic: module names loaded in this tenant's child (no tenant data).
  async inventory({tenantId}) {
    const worker = await this.#engine(tenantId);
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
      await this.ctx.storage.put({tenantId, lastAttempt: Date.now()});
      if (await this.ctx.storage.getAlarm() === null) {
        await this.ctx.storage.setAlarm(Date.now() + WARM_PING_MS);
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
      await this.#ping(tenantId);
    } finally {
      await this.ctx.storage.setAlarm(Date.now() + WARM_PING_MS);
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
    const started = Date.now();
    const token = await host.token(handle);
    if (!token) return deny(401, 'unknown handle');
    const tokenMs = Date.now() - started;
    const headers = new Headers(request.headers);
    headers.set('Authorization', `Bearer ${token}`);
    headers.delete('cookie');
    const response = await fetch(new Request(target, {
      method: request.method,
      headers,
      body: request.body,
      redirect: 'manual',
    }));
    // Awaited, not waitUntil: the last call's note must land before run()
    // reports, or the breakdown silently drops it.
    await host.noteCall(handle, {method: request.method,
      path: new URL(target).pathname, status: response.status,
      token_ms: tokenMs, ms: Date.now() - started - tokenMs});
    return response;
  }

  // The workflow's own third-party HTTPS request: forwarded as-is (its own
  // headers, never the run token) when the attempt in flight declared the host.
  async #egress(host, request, url) {
    const note = {method: request.method, host: url.hostname, path: url.pathname};
    if (url.protocol !== 'https:' || url.username || url.password || url.port ||
        !(await host.egressAllowed(url.hostname))) {
      await host.noteEgress({...note, status: 403, denied: 'egress not declared'});
      return new Response('egress not declared', {status: 403});
    }
    const started = Date.now();
    const response = await fetch(request);
    await host.noteEgress({...note, status: response.status, ms: Date.now() - started});
    return response;
  }
}
