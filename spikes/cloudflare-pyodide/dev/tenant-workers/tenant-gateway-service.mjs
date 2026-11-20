import {apiTarget} from '../executor/policy.mjs';
import {refreshReply, refreshRequestHandle} from './attempt-credentials.mjs';

const HANDLE_RE = /^run_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const VIRTUAL_API_ORIGIN = 'https://test-api.invalid';

export async function routeTenantRequest(request, host, generation, {API_ORIGIN, SDK_SCOPE}) {
  const url = new URL(request.url);
  if (url.origin !== VIRTUAL_API_ORIGIN) return host.egress(request, generation);
  if (request.method === 'POST' && url.pathname === '/auth/refresh') {
    const handle = await refreshRequestHandle(request);
    const outcome = HANDLE_RE.test(handle ?? '') ? await host.refresh(handle) : 'gone';
    const reply = refreshReply(outcome, handle);
    if (outcome !== 'gone') {
      await host.noteCall(handle, {method: 'POST', path: '/auth/refresh', status: reply.status, refresh: outcome});
    }
    return reply;
  }
  const deny = async (status, reason) => {
    await host.noteEgress({method: request.method, path: url.pathname, status, denied: reason});
    return new Response(reason, {status});
  };
  const authorization = request.headers.get('Authorization') ?? '';
  const handle = authorization.startsWith('Bearer ') ? authorization.slice(7) : '';
  if (!HANDLE_RE.test(handle)) return deny(401, 'opaque handle required');
  let target;
  try {
    target = apiTarget(request.url, API_ORIGIN, {scope: SDK_SCOPE, method: request.method});
  } catch {
    return deny(403, 'API capability denied');
  }
  return host.forward(handle, request, String(target));
}
