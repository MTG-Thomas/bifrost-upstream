// Per-tenant persistent Python worker supervisor (PROTOTYPE, local only).
//
// Pivot from fresh isolate per attempt (env.LOADER.load per attempt) to one
// long-lived generic worker PER TENANT via
// env.LOADER.get(<tenantWorkerId>, () => genericCode). Attempts for the same
// tenant are serialized through a per-tenant promise chain (Pyodide is
// single-threaded); different tenants run in different isolates.
//
// Module source path (PATH-keyed, gateway-mediated): the attempt supplies
// body.modules as storage path -> source and body.manifest as storage path ->
// sha256. The supervisor never forwards the raw store: every byte the child
// receives comes out of createTenantGateway responses to
// GET /api/sdk/modules/<path> (pin-verified) after
// GET /api/sdk/modules-resolve resolves the entry, matching how dev/executor
// fetches. Recorded workspace loads are therefore REAL storage paths and the
// import audit reconciles paths. The gateway holds the run token
// (body.context?.engine_token) in props only; callers present DUMMY_CREDENTIAL.
//
// Security posture (reused, not reimplemented):
// - capability-requirements.mjs: undeclared modules rejected BEFORE any
//   loader/network/SDK effect.
// - egress-allowlist.mjs + gateway-injection.mjs decideGatewayAction: egress
//   gate BEFORE any loader/network effect; child holds NO credentials (child
//   env is {} and attempt bodies carry only DUMMY_CREDENTIAL).
// - output-budget.mjs: oversized child output dropped as infra failure.
// - import-audit.mjs: recorded workspace paths reconciled post-hoc; undeclared
//   imports map to a terminal (effects may already exist, same as executor).
import {bootProbeCode, genericWorkerCode, genericCodeDigest, engineDigest} from './generic-worker-code.mjs';
import {tenantWorkerId} from './tenant-registry.mjs';
import {createTenantGateway, createRemoteTenantGateway, isStoragePath} from './tenant-gateway.mjs';
import {capabilityRejection} from '../executor/capability-requirements.mjs';
import {egressRejection, isUrlAllowed} from '../executor/egress-allowlist.mjs';
import {reconcileImports, deniedTerminal} from '../executor/import-audit.mjs';
import {readBudgetedJson} from '../executor/output-budget.mjs';
import {DUMMY_CREDENTIAL} from '../executor/gateway-injection.mjs';

// Node unit tests cannot resolve cloudflare:workers, so the exports are loaded
// only where the runtime provides them; ctx.exports cannot see them otherwise.
let TenantApi;
let TenantHost;
try {
  ({TenantApi, TenantHost} = await import('./tenant-api.mjs'));
} catch {
  TenantApi = undefined;
  TenantHost = undefined;
}
export {TenantApi, TenantHost};

// Per-tenant serialization chains + first-use tracking. Module-level: one
// supervisor isolate holds one chain per tenant id.
const tails = new Map();
const seenTenants = new Set();

export function resetTenantState() {
  tails.clear();
  seenTenants.clear();
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
export async function warmHost(getHost, tenantId) {
  for (let attempt = 1; ; attempt++) {
    try {
      return {...await getHost().warm({tenantId}), warm_attempts: attempt};
    } catch (error) {
      if (error?.retryable !== true || attempt >= WARM_ATTEMPTS) throw error;
    }
  }
}

// An engine attempt is dispatched at most once. A host that has not confirmed
// its child refuses with needsWarm before doing any attempt work; only the
// warm-up is retried.
export async function hostAttempt(getHost, attempt) {
  const first = await getHost().run(attempt);
  if (!first.needsWarm) return first;
  const warm = await warmHost(getHost, attempt.tenantId);
  const result = await getHost().run(attempt);
  if (result.needsWarm) return {notReady: true, warm};
  return {...result, warm};
}

function json(status, value) {
  return Response.json(value, {status});
}

// Attempt store as a record (storage path -> source); anything else behaves
// as an empty store so the gateway preflight fails closed after the gates.
function gatewaySources(modules) {
  return modules !== null && typeof modules === 'object' && !Array.isArray(modules) ? modules : {};
}

export default {
  async fetch(request, env, ctx) {
    // Local workerd sets LOCAL_PROBE=true. A deployed instance instead needs the
    // spike auth header inside an unexpired window (same shape as the other
    // spike entries); anything else fails closed before any tenant work.
    if (env.LOCAL_PROBE !== 'true' &&
        (!env.SPIKE_AUTH || request.headers.get('X-Spike-Auth') !== env.SPIKE_AUTH ||
         !(Date.now() < Number(env.SPIKE_EXPIRES_AT)))) {
      return new Response('Local prototype only', {status: 403});
    }
    const url = new URL(request.url);
    if (request.method === 'GET' && url.pathname === '/__code_digest') {
      return Response.json({generic_code_sha256: await genericCodeDigest(),
        engine_code_sha256: await engineDigest(env.RUNTIME_SHA256)});
    }
    if (request.method === 'GET' && url.pathname === '/__gateway_probe') {
      // Diagnostic: can this Worker reach the configured API origin at all?
      const origin = typeof env.API_ORIGIN === 'string' ? env.API_ORIGIN : '';
      if (!origin) return Response.json({configured: false});
      try {
        const response = await fetch(`${origin}/health/ready`, {redirect: 'manual'});
        return Response.json({configured: true, status: response.status});
      } catch (error) {
        return Response.json({configured: true, error_type: error?.name ?? 'Error',
          error: String(error?.message ?? error).slice(0, 200)});
      }
    }
    if (request.method !== 'POST') return new Response('POST required', {status: 405});
    let body;
    try {
      body = await request.json();
    } catch {
      return json(400, {phase: 'bad-request', error: 'Invalid JSON'});
    }

    let workerId;
    try {
      workerId = await tenantWorkerId(body.tenant_id);
    } catch {
      return json(400, {phase: 'bad-request', error: 'Invalid tenant_id'});
    }

    // Per-attempt posture control (diagnostic only): LOADER.load with no id
    // bills one Dynamic Worker per invocation, unlike the per-tenant
    // LOADER.get below. Never used by tenant routing; used to measure the
    // creation-meter difference in the billed account.
    if (body.action === 'legacy_load') {
      const worker = env.LOADER.load(genericWorkerCode());
      const response = await worker.getEntrypoint().fetch(new Request('https://attempt.invalid/', {
        method: 'POST', body: JSON.stringify({action: 'honest', credential: DUMMY_CREDENTIAL}),
      }));
      return Response.json({mode: 'legacy_load', output: await readBudgetedJson(response)});
    }

    // Capability gate BEFORE any loader/network/SDK effect.
    const capability = capabilityRejection(body.requirements, body.manifest);
    if (capability) {
      return json(422, {phase: 'capability-rejected',
        missing_modules: capability.missing_modules, api_calls: []});
    }
    // Egress gate BEFORE any loader/network/SDK effect.
    const egress = egressRejection(body.egress);
    if (egress) {
      return json(422, {phase: 'egress-rejected', reason: egress.reason, api_calls: []});
    }
    if (body.url !== undefined && !isUrlAllowed(body.url, body.egress)) {
      return json(422, {phase: 'egress-rejected', reason: 'unlisted', api_calls: []});
    }

    // Run entries are storage paths (e.g. 'workflows/wf.py'), validated BEFORE
    // any loader/network/SDK effect alongside the capability/egress gates.
    if (body.action === 'run' && !isStoragePath(body.entry)) {
      return json(400, {phase: 'bad-request', error: 'Invalid entry path', api_calls: []});
    }

    if (body.action === 'boot_probe') {
      // Diagnostic: always a brand-new child (unique loader id), so child_ms
      // is a cold boot of exactly the requested import set.
      let code;
      try {
        code = bootProbeCode({imports: body.imports, bare: body.bare === true});
      } catch {
        return json(400, {phase: 'bad-request', error: 'Invalid imports'});
      }
      const mark = Date.now();
      try {
        const worker = env.LOADER.get(`boot-probe-${crypto.randomUUID()}`, () => code);
        const response = await worker.getEntrypoint().fetch(new Request('https://probe.invalid/'));
        const report = await response.json();
        return Response.json({child_ms: Date.now() - mark, status: response.status, report});
      } catch (error) {
        // Safe to echo here only: a boot probe runs no tenant code, data or
        // credential, just imports of the packaged runtime.
        return json(502, {phase: 'dynamic-runtime', error_type: error?.name ?? 'Error',
          ...runtimeErrorClass(error), child_ms: Date.now() - mark,
          probe_error: String(error?.message ?? error).slice(-600)});
      }
    }

    if (body.action === 'inventory') {
      if (typeof TenantHost !== 'function') {
        return json(502, {phase: 'engine-unavailable', tenant_worker_id: workerId});
      }
      return Response.json(await ctx.exports.TenantHost.getByName(workerId).inventory({tenantId: workerId}));
    }

    if (body.action === 'warm') {
      // Boot the tenant's engine child ahead of an attempt. Carries no
      // attempt, token or workflow data.
      if (typeof TenantHost !== 'function') {
        return json(502, {phase: 'engine-unavailable', tenant_worker_id: workerId});
      }
      try {
        const warmed = await warmHost(() => ctx.exports.TenantHost.getByName(workerId), workerId);
        return Response.json({tenant_worker_id: workerId, timings: warmed});
      } catch (error) {
        return json(502, {phase: 'dynamic-runtime', error_type: error?.name ?? 'Error',
          ...runtimeErrorClass(error), tenant_worker_id: workerId});
      }
    }

    if (body.action === 'engine') {
      // Real-engine mode: the tenant's host Durable Object owns the child,
      // the per-tenant serialization and the attempt token (tenant-api.mjs).
      // The engine authenticates SDK calls with an opaque handle that only
      // this tenant's outbound gateway can resolve (dev/staging/README).
      if (typeof TenantHost !== 'function') {
        return json(502, {phase: 'engine-unavailable', tenant_worker_id: workerId, api_calls: []});
      }
      const mark = Date.now();
      let hosted;
      try {
        hosted = await hostAttempt(() => ctx.exports.TenantHost.getByName(workerId), {
          tenantId: workerId, executionId: body.execution_id, context: body.context,
          publicUrl: body.public_url,
          // Already validated by the egress gate above.
          egressHosts: body.egress?.hosts ?? []});
      } catch (error) {
        // Never echo exception strings (may contain workflow data or
        // credentials) -- except on a capability-lab deploy that opts in.
        return json(502, {phase: 'dynamic-runtime', error_type: error?.name ?? 'Error',
          ...runtimeErrorClass(error), tenant_worker_id: workerId, api_calls: [],
          ...(env.LAB_ECHO_ERRORS === 'true'
            ? {lab_error: String(error?.message ?? error).slice(-600)} : {})});
      }
      if (hosted.notReady) {
        // Warmed, yet the host still has not confirmed its child; the
        // attempt was never started.
        return json(502, {phase: 'engine-not-ready', tenant_worker_id: workerId, api_calls: []});
      }
      if (hosted.terminal) {
        // e.g. output-budget: the attempt ran; its effects may already exist.
        return json(502, {...hosted.terminal, tenant_worker_id: workerId});
      }
      // Reuse is the child's own view: the supervisor's memory is lost
      // whenever the supervisor isolate is recycled.
      return Response.json({tenant_worker_id: workerId,
        worker_reused: hosted.timings.child?.first_request === false,
        queue_wait_ms: hosted.queueWaitMs, output: hosted.output, api_calls: [],
        timings: {host_ms: Date.now() - mark, ...hosted.timings,
          ...(hosted.warm ? {warm: hosted.warm} : {})}});
    }

    const workerReused = seenTenants.has(workerId);
    let outcome;
    try {
      outcome = await enqueueTenant(workerId, async () => {
      seenTenants.add(workerId);
      // One long-lived generic worker PER TENANT. The factory closes over NO
      // per-attempt data: the script is constant. Workflow source travels as
      // PATH-keyed DATA below, but only bytes the TenantGateway served after
      // manifest-pin verification (never the raw attempt store).
      const apiCalls = [];
      let childBody;
      if (body.action === 'run') {
        // Staging mode: fetch through the real API with the run token held in
        // the parent. The attempt declares the module closure; anything outside
        // it is never fetched.
        const remote = typeof env.API_ORIGIN === 'string' && env.API_ORIGIN !== '';
        const gateway = remote
          ? createRemoteTenantGateway({apiOrigin: env.API_ORIGIN, manifest: body.manifest,
              runToken: body.context?.engine_token})
          : createTenantGateway({store: body.modules, manifest: body.manifest,
              runToken: body.context?.engine_token});
        const gatewayGet = async (path) => {
          const response = await gateway.fetch(new Request(`https://test-api.invalid${path}`, {
            headers: {Authorization: `Bearer ${DUMMY_CREDENTIAL}`},
          }));
          apiCalls.push({path, method: 'GET', status: response.status});
          return response;
        };
        // Resolve the entry through the gateway first (mirrors executor's
        // modules-resolve + modules/<path> preflight): declared modules must
        // resolve in the manifest BEFORE the loader effect below.
        const resolveName = body.entry.slice(0, -'.py'.length).replaceAll('/', '.');
        const resolvedResponse = await gatewayGet(
          `/api/sdk/modules-resolve?name=${encodeURIComponent(resolveName)}`);
        const resolved = await resolvedResponse.json();
        if ((resolved.kind !== 'module' && resolved.kind !== 'package') ||
            resolved.storage_path !== body.entry) {
          return {terminal: {phase: 'module-preflight', module_status: resolvedResponse.status,
            path: body.entry, resolution: resolved.kind, api_calls: apiCalls}};
        }
        // Fetch every declared path through the gateway. Local mode keeps the
        // attempt-supplied store (plus extra loadable paths for the audit);
        // remote mode fetches exactly the declared closure from the real API.
        const modulePaths = remote
          ? [...new Set((body.requirements?.modules ?? []).filter(id => isStoragePath(id)))]
          : Object.keys(gatewaySources(body.modules));
        const fetched = {};
        for (const path of modulePaths) {
          const moduleResponse = await gatewayGet(`/api/sdk/modules/${path}`);
          if (moduleResponse.status !== 200) {
            return {terminal: {phase: 'module-preflight', module_status: moduleResponse.status,
              path, api_calls: apiCalls}};
          }
          const served = await moduleResponse.json();
          fetched[served.path] = served.content;
        }
        if (!(body.entry in fetched)) {
          return {terminal: {phase: 'module-preflight', module_status: 404,
            path: body.entry, api_calls: apiCalls}};
        }
        childBody = {action: body.action, entry: body.entry,
          modules: fetched, credential: DUMMY_CREDENTIAL};
      } else {
        childBody = {action: body.action, marker: body.marker, ms: body.ms,
          credential: DUMMY_CREDENTIAL};
      }
      const worker = env.LOADER.get(workerId, () => genericWorkerCode());
      const response = await worker.getEntrypoint().fetch(new Request('https://attempt.invalid/', {
        method: 'POST', body: JSON.stringify(childBody),
      }));
      let output;
      try {
        output = await readBudgetedJson(response);
      } catch (error) {
        if (error?.name === 'ExecutorBodyTooLargeError') {
          return {terminal: {phase: 'output-budget', error_type: 'ExecutorBodyTooLargeError',
            error: 'Executor response exceeds retention budget; workflow effects may already exist',
            provider_outcome: {kind: 'terminal_body_too_large', response_eof: true,
              engine_outcome_delivered: false,
              response_bytes: error.observedBytes, retention_limit_bytes: error.limitBytes},
            api_calls: apiCalls}};
        }
        throw error;
      }
      if (body.action === 'run') {
        const audit = reconcileImports(body.requirements?.modules, output.recorded);
        const denied = deniedTerminal(audit);
        if (denied) return {terminal: {...denied, api_calls: apiCalls}, output};
      }
      return {output, apiCalls};
      });
    } catch (error) {
      // Never echo exception strings (may contain workflow data or credentials).
      return json(502, {phase: 'dynamic-runtime', error_type: error?.name ?? 'Error',
        tenant_worker_id: workerId, api_calls: []});
    }
    const {result, queueWaitMs} = outcome;

    if (result?.terminal) {
      return json(502, {...result.terminal, tenant_worker_id: workerId,
        queue_wait_ms: queueWaitMs});
    }
    return Response.json({tenant_worker_id: workerId, worker_reused: workerReused,
      queue_wait_ms: queueWaitMs, output: result.output, api_calls: result.apiCalls ?? []});
  },
};
