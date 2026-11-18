import {hostAttempt, warmHost} from './tenant-dispatch.mjs';
import {tenantWorkerId} from './tenant-registry.mjs';

function terminal(attempt_state, error_type, error) {
  return {attempt_state, result: {success: false, error_type, error}};
}

async function boundedWarm(getHost, tenantId, {deadline, signal, beforeRun, now}) {
  const check = () => {
    if (beforeRun()) throw new Error('Attempt ended before warm-up');
  };
  let timer;
  let cancel;
  const abort = new Promise((_, reject) => {
    cancel = () => reject(new Error('Attempt ended during warm-up'));
    signal.addEventListener('abort', cancel, {once: true});
    if (deadline !== Infinity) timer = setTimeout(cancel, Math.max(0, deadline - now()));
  });
  try {
    return await Promise.race([warmHost(getHost, tenantId, {check}), abort]);
  } finally {
    clearTimeout(timer);
    signal.removeEventListener('abort', cancel);
  }
}

export async function launcherTenantId(route) {
  const org = route.active_execution.org_id;
  return tenantWorkerId(org == null ? 'launcher:platform' : `launcher:org:${org}`);
}

// Warm-up may be retried, but a run RPC is repeated only after an explicit
// needsWarm refusal. Every call carries the same absolute wire deadline.
export async function runLauncherWorkflow(getHost, {route, context, publicUrl, signal, now = Date.now}) {
  const deadline = route.workflow_deadline === null ? Infinity : Date.parse(route.workflow_deadline);
  const beforeRun = () => {
    if (now() >= deadline) return {terminal: {phase: 'attempt-deadline', attempt_state: 'not_started'}};
    if (signal.aborted) return {terminal: {phase: 'attempt-stopped', attempt_state: 'not_started'}};
    return null;
  };
  const tenantId = await launcherTenantId(route);
  const attempt = {tenantId, executionId: route.subject_id, context, publicUrl, routeId: route.route_id,
    workflowDeadline: route.workflow_deadline};
  let hosted;
  try {
    hosted = await hostAttempt(() => getHost(tenantId), attempt, {
      beforeRun,
      warm: (get, tenant) => boundedWarm(get, tenant, {deadline, signal, beforeRun, now}),
    });
  } catch (error) {
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
