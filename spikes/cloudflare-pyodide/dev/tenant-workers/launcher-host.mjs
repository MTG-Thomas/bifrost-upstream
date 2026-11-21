import {ENGINE_BOOT_TIMEOUT_MS, hostAttempt, warmHost} from './tenant-dispatch.mjs';
import {tenantWorkerId} from './tenant-registry.mjs';

function terminal(attempt_state, error_type, error) {
  return {attempt_state, result: {success: false, error_type, error}};
}

async function whileDispatching(task, {deadline, signal, now}) {
  if (signal.aborted || now() >= deadline) throw new Error('Attempt ended before warm-up');
  let timer;
  let cancel;
  const abort = new Promise((_, reject) => {
    cancel = () => reject(new Error('Attempt ended during warm-up'));
    signal.addEventListener('abort', cancel, {once: true});
    if (deadline !== Infinity) timer = setTimeout(cancel, Math.max(0, deadline - now()));
  });
  try {
    return await Promise.race([task(), abort]);
  } finally {
    clearTimeout(timer);
    signal.removeEventListener('abort', cancel);
  }
}

export async function launcherTenantId(route) {
  const org = route.active_execution.org_id;
  const tenant = org == null ? 'launcher:platform' : `launcher:org:${org}`;
  return tenantWorkerId(route.engine_slot === undefined ? tenant :
    `${tenant}:slot:${route.engine_slot}:incarnation:${route.incarnation_id}`);
}

export function createLauncherWorkflowRunner(getHost, {warmRpcMs = ENGINE_BOOT_TIMEOUT_MS + 5000} = {}) {
  // The local workerd Python loader can fail when two children cold-start
  // together. While a boot is in flight, retain the queue until it confirms
  // or reaches the RPC limit, independently of the waiting route's deadline.
  let warmTail = Promise.resolve();
  const preparations = new Map();
  const warmRpc = async (task) => {
    let timer;
    let expired = false;
    const check = () => {
      if (expired) throw new Error('TenantHost warm-up RPC timed out');
    };
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        expired = true;
        reject(new Error('TenantHost warm-up RPC timed out'));
      }, warmRpcMs);
    });
    try {
      return await Promise.race([task(check), timeout]);
    } finally {
      clearTimeout(timer);
    }
  };
  const queueWarm = (task) => {
    const next = warmTail.then(() => warmRpc(task));
    warmTail = next.catch(() => {});
    return next;
  };
  async function run(input) {
    const {route} = input;
    const current = preparations.get(await launcherTenantId(route));
    const preparation = current && (current.queuePrewarm || (current.executionId === route.subject_id &&
      (route.engine_slot === undefined || route.route_id.endsWith(`:${current.fence}`)))) ? current.task : null;
    if (!input.receipt) return runLauncherWorkflow(getHost, queueWarm, input, preparation);
    const invocations = [];
    let complete = true;
    const traced = (tenant) => {
      const host = getHost(tenant);
      const call = async (method, request) => {
        try {
          const result = await host[method](request);
          if (result.invocation) invocations.push(result.invocation);
          else if (!result.needsWarm && result.terminal?.attempt_state !== 'not_started') complete = false;
          return result;
        } catch (error) {
          // A retry can succeed after the previous invocation consumed a
          // startup. A missing result never gives a discount.
          complete = false;
          throw error;
        }
      };
      return {run: request => call('run', request), warm: request => call('warm', request)};
    };
    const outcome = await runLauncherWorkflow(traced, queueWarm, input, preparation);
    return complete && outcome.attempt_state === 'completed' ? {...outcome, invocations} : outcome;
  }
  async function prepare({orgId, executionId, signal, reserveIdentity, engineSlot, incarnationId, fence,
    prepareBy, warmUntil}) {
    const tenantId = await launcherTenantId({active_execution: {org_id: orgId},
      engine_slot: engineSlot, incarnation_id: incarnationId});
    if (preparations.has(tenantId)) return [];
    const queuePrewarm = prepareBy !== undefined;
    const task = (queuePrewarm ? warmRpc : queueWarm)(async (check) => {
      check();
      if (signal.aborted) return {skipped: true};
      return getHost(tenantId).warm({tenantId, executionId, admission: true,
        ...(queuePrewarm ? {prepareBy, warmUntil} : {}),
        ...(reserveIdentity ? {reserveIdentity} : {})});
    });
    preparations.set(tenantId, {executionId, fence, task, queuePrewarm});
    try {
      const result = await task;
      if (result.invocation) return [result.invocation];
      if (result.reused || result.skipped) return [];
      return undefined;
    } finally {
      preparations.delete(tenantId);
    }
  }
  return Object.assign(run, {prepare});
}

// Warm-up may be retried, but a run RPC is repeated only after an explicit
// needsWarm refusal. Every call carries the same absolute wire deadline.
async function runLauncherWorkflow(getHost, queueWarm,
  {route, context, publicUrl, signal, reserveIdentity, now = Date.now}, preparation) {
  const deadline = route.workflow_deadline === null ? Infinity : Date.parse(route.workflow_deadline);
  const beforeRun = () => {
    if (now() >= deadline) return {terminal: {phase: 'attempt-deadline', attempt_state: 'not_started'}};
    if (signal.aborted) return {terminal: {phase: 'attempt-stopped', attempt_state: 'not_started'}};
    return null;
  };
  const tenantId = await launcherTenantId(route);
  const attempt = {tenantId, executionId: route.subject_id, context, publicUrl, routeId: route.route_id,
    workflowDeadline: route.workflow_deadline, ...(route.engine_slot === undefined ? {} : {trackStop: true}),
    ...(reserveIdentity ? {reserveIdentity} : {})};
  let hosted;
  try {
    hosted = await hostAttempt(() => getHost(tenantId), attempt, {
      beforeRun,
      join: preparation ? () => whileDispatching(async () => {
        const observed = await preparation.catch(() => null);
        return Boolean(observed && !observed.skipped);
      }, {deadline, signal, now}).catch(() => false) : undefined,
      warm: (get, tenant) => whileDispatching(() => queueWarm((rpcCheck) => warmHost(get, tenant, {
          check: () => {
            if (beforeRun()) throw new Error('Attempt ended before warm-up');
            rpcCheck();
          },
          engineOnly: true, reserveIdentity, executionId: route.subject_id,
        })), {deadline, signal, now}),
    });
  } catch (error) {
    if (error?.attemptState === 'not_started' && error?.name === 'CloudflareLauncherMeterLimitExceeded') {
      return terminal('not_started', error.name, 'Cloudflare launcher DO request budget reached before engine entry.');
    }
    if (error?.attemptState !== 'not_started') {
      return terminal('uncertain', 'TenantHostOutcomeUnknown', 'TenantHost did not confirm the workflow outcome');
    }
    hosted = beforeRun() ?? {notReady: true};
  }
  if (hosted.notReady) return terminal('not_started', 'TenantHostNotReady', 'TenantHost could not warm the engine');
  if (hosted.terminal) {
    const {phase, attempt_state, error_type} = hosted.terminal;
    if (phase === 'attempt-deadline') return terminal(attempt_state, 'TimeoutError', 'Workflow deadline elapsed');
    if (phase === 'attempt-stopped') return terminal(attempt_state, 'CancelledError', 'Workflow stopped');
    if (phase === 'sdk-meter-limit') {
      return terminal(attempt_state, 'CloudflareSDKRequestLimitExceeded',
        `Cloudflare release gateway request limit reached (${hosted.terminal.limit} requests per attempt).`);
    }
    if (phase === 'cpu_limit') {
      return terminal(attempt_state, 'CpuLimitExceeded',
        'Stopped by Cloudflare: the executor exceeded its effective CPU time limit.');
    }
    if (phase === 'memory_limit') {
      return terminal(attempt_state, 'MemoryLimitExceeded',
        'Stopped by Cloudflare: the executor exceeded the platform memory limit.');
    }
    return terminal(attempt_state, error_type, 'Workflow output exceeded the retention limit');
  }
  const output = hosted.output;
  if (typeof output?.status !== 'string') {
    return terminal('uncertain', 'TenantHostOutcomeUnknown', 'TenantHost returned no workflow status');
  }
  const success = output.status === 'Success';
  const {error_message, ...result} = output;
  return {attempt_state: 'completed', result: {...result, success, ...(success ? {} : {error: error_message})}};
}
