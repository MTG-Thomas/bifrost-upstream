// Workers for Platforms probe: the dynamic dispatch Worker. Forwards an
// authenticated request to one user script in the namespace, optionally with a
// per-invocation CPU limit (`?cpuMs=`), and reports how the dispatch ended.
// Constant time over fixed-length digests: leaks neither content nor length.
async function authorized(request, env) {
  if (typeof env.LAB_AUTH !== 'string' || env.LAB_AUTH === '') return false;
  if (!(Date.now() < Number(env.LAB_EXPIRES_AT))) return false;
  const [a, b] = await Promise.all([request.headers.get('x-lab-auth') ?? '', env.LAB_AUTH].map((value) =>
    crypto.subtle.digest('SHA-256', new TextEncoder().encode(value))));
  const left = new Uint8Array(a);
  const right = new Uint8Array(b);
  let difference = 0;
  for (let i = 0; i < left.length; i++) difference |= left[i] ^ right[i];
  return difference === 0;
}

export default {
  async fetch(request, env) {
    if (!(await authorized(request, env))) {
      return new Response('forbidden', {status: 403});
    }
    const url = new URL(request.url);
    const script = url.searchParams.get('script') || env.USER_SCRIPT;
    const cpuMs = url.searchParams.get('cpuMs');
    const options = cpuMs ? {limits: {cpuMs: Number(cpuMs)}} : {};
    try {
      const response = await env.DISPATCHER.get(script, {}, options).fetch(request);
      return new Response(response.body, {status: response.status, headers: response.headers});
    } catch (error) {
      return Response.json({dispatch_error: {name: error?.name, message: String(error?.message ?? error)}}, {status: 502});
    }
  },
};
