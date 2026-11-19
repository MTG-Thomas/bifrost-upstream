// Generated from the SDK route policy by generate-sdk-route-policy.mjs.
// Kept inline so staged Workers and Workerd fixtures carry the same policy.
export const SDK_ROUTE_POLICY = {
  "prefixes": [
    "/api/sdk/",
    "/api/organizations",
    "/api/configs",
    "/api/files",
    "/api/integrations",
    "/api/tables",
    "/api/events",
    "/api/forms",
    "/api/agents",
    "/api/agent-runs"
  ],
  "identifiers": {
    "uuid": "[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}",
    "email": "[A-Za-z0-9.!$&'*+=^_~-]+(?:@|%40)[A-Za-z0-9.-]+"
  },
  "routes": {
    "/auth/refresh": [
      "POST"
    ],
    "/api/roles": [
      "GET",
      "POST"
    ],
    "/api/roles/{uuid}": [
      "GET",
      "PATCH",
      "DELETE"
    ],
    "/api/roles/{uuid}/users": [
      "GET",
      "POST"
    ],
    "/api/roles/{uuid}/forms": [
      "GET",
      "POST"
    ],
    "/api/users": [
      "GET",
      "POST"
    ],
    "/api/users/{uuid}": [
      "GET",
      "PATCH",
      "DELETE"
    ],
    "/api/users/{email}": [
      "GET",
      "PATCH",
      "DELETE"
    ],
    "/api/platform-jobs/{uuid}": [
      "GET"
    ]
  }
};
const CHILD_ROUTES = Object.entries(SDK_ROUTE_POLICY.routes).map(([path, methods]) => ({
  pattern: new RegExp(`^${path.replaceAll(/\{(\w+)\}/g, (_, name) => `(?:${SDK_ROUTE_POLICY.identifiers[name]})`)}$`),
  methods,
}));

// Segment-exact prefix match: `/api/tables` admits `/api/tables` and
// `/api/tables/...`, never `/api/tables-admin`.
function isChildApiPath(pathname, method) {
  return CHILD_ROUTES.some((route) => route.methods.includes(method) && route.pattern.test(pathname)) ||
    SDK_ROUTE_POLICY.prefixes.some((prefix) =>
      pathname === prefix || pathname.startsWith(prefix.endsWith('/') ? prefix : `${prefix}/`));
}

// `scope: 'all'` admits every /api/ path (and /auth/refresh). It exists for the
// capability lab only, to separate "the platform cannot" from "this gateway's
// policy does not"; the default stays the narrow SDK surface above.
export function apiTarget(url, origin, {scope = 'sdk', method = 'GET'} = {}) {
  const incoming = new URL(url);
  const allowed = scope === 'all'
    ? incoming.pathname === '/auth/refresh' || incoming.pathname.startsWith('/api/')
    : isChildApiPath(incoming.pathname, method);
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
