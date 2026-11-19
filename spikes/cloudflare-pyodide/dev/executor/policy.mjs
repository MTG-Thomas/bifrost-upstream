// SDK resource roots a workflow child may reach through the gateway. Dispatch
// and execution-management routes stay out: the parent owns those. Authorization
// is still the run token's; this is a capability boundary, not the auth gate.
const CHILD_API_PREFIXES = [
  '/api/sdk/',
  '/api/organizations',
  '/api/roles',
  '/api/users',
  '/api/configs',
  '/api/files',
  '/api/integrations',
  '/api/tables',
  '/api/events',
  '/api/forms',
  '/api/agents',
  '/api/agent-runs',
];

// Segment-exact prefix match: `/api/tables` admits `/api/tables` and
// `/api/tables/...`, never `/api/tables-admin`.
function isChildApiPath(pathname) {
  return pathname === '/auth/refresh' ||
    CHILD_API_PREFIXES.some((prefix) =>
      pathname === prefix || pathname.startsWith(prefix.endsWith('/') ? prefix : `${prefix}/`));
}

// `scope: 'all'` admits every /api/ path (and /auth/refresh). It exists for the
// capability lab only, to separate "the platform cannot" from "this gateway's
// policy does not"; the default stays the narrow SDK surface above.
export function apiTarget(url, origin, {scope = 'sdk'} = {}) {
  const incoming = new URL(url);
  const allowed = scope === 'all'
    ? incoming.pathname === '/auth/refresh' || incoming.pathname.startsWith('/api/')
    : isChildApiPath(incoming.pathname);
  if (incoming.origin !== 'https://test-api.invalid' || !allowed) {
    throw new TypeError('API capability denied');
  }
  const target = new URL(origin);
  if (target.protocol !== 'https:' || target.username || target.password || target.pathname !== '/' || target.search || target.hash) {
    throw new TypeError('Fixed HTTPS API origin required');
  }
  target.pathname = incoming.pathname;
  target.search = incoming.search;
  return target;
}

export function attemptPayload(body, publicUrl) {
  const url = new URL(publicUrl);
  if (url.protocol !== 'https:' || url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
    throw new TypeError('Trusted root HTTPS public URL required');
  }
  return {action: 'execute', execution_id: body.execution_id, context: body.context,
    api_url: 'https://test-api.invalid', public_url: url.origin};
}
