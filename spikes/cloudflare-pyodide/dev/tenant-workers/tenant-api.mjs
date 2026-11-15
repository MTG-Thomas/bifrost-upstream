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
const TOKEN_TTL_MS = 10 * 60 * 1000;
// Warm-pool policy: a tenant that ran in the last WARM_WINDOW_MS keeps a
// resident child. This is the knob that trades idle cost for latency, the
// counterpart of the Linux process pool's always-warm workers.
export const WARM_WINDOW_MS = 20 * 60 * 1000;
export const WARM_PING_MS = 45 * 1000;

export class TenantHost extends DurableObject {
  #tokens = new Map();
  #tail = Promise.resolve();
  // In memory on purpose: a fresh or reset host instance must re-confirm its
  // child before it accepts an attempt (see run()).
  #childReady = false;

  async #engine(tenantId) {
    const engineId = `tenant-eng-${tenantId}-${(await engineDigest(this.env.RUNTIME_SHA256)).slice(0, 16)}`;
    // The 15 MB runtime is decoded only here, i.e. only when a child is
    // actually created, never on the warm path.
    return this.env.LOADER.get(engineId,
      () => tenantEngineCode(this.ctx.exports.TenantApi({props: {tenantId}})));
  }

  // Resolves an attempt handle for this tenant's outbound gateway only.
  token(handle) {
    const record = this.#tokens.get(handle);
    if (!record || Date.now() >= record.expires) return null;
    return record.token;
  }

  async run({tenantId, executionId, context, publicUrl}) {
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
    try {
      const worker = await this.#engine(tenantId);
      const mark = Date.now();
      const response = await worker.getEntrypoint().fetch(new Request('https://attempt.invalid/', {
        method: 'POST',
        body: JSON.stringify({execution_id: executionId,
          context: {...context, engine_token: handle},
          api_url: 'https://test-api.invalid',
          public_url: publicUrl ?? 'https://staging.invalid'}),
      }));
      const childMs = Date.now() - mark;
      const output = await readBudgetedJson(response);
      return {output: output.upstream_result, queueWaitMs,
        timings: {child_ms: childMs, child: output.child ?? null}};
    } finally {
      this.#tokens.delete(handle);
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
    return {child_ms: Date.now() - mark, first_request: firstRequest};
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
    const authorization = request.headers.get('Authorization') ?? '';
    const handle = authorization.startsWith('Bearer ') ? authorization.slice(7) : '';
    if (!HANDLE_RE.test(handle)) {
      return new Response('opaque handle required', {status: 401});
    }
    let target;
    try {
      target = apiTarget(request.url, this.env.API_ORIGIN);
    } catch {
      return new Response('API capability denied', {status: 403});
    }
    const tenantId = this.ctx.props?.tenantId;
    if (typeof tenantId !== 'string' || tenantId === '') {
      return new Response('unknown handle', {status: 401});
    }
    const token = await this.ctx.exports.TenantHost.getByName(tenantId).token(handle);
    if (!token) return new Response('unknown handle', {status: 401});
    const headers = new Headers(request.headers);
    headers.set('Authorization', `Bearer ${token}`);
    headers.delete('cookie');
    return fetch(new Request(target, {
      method: request.method,
      headers,
      body: request.body,
      redirect: 'manual',
    }));
  }
}
