// Workers for Platforms probe: the dynamic dispatch Worker. Forwards an
// authenticated request to one user script in the namespace, optionally with a
// per-invocation CPU limit (`?cpuMs=`), and reports how the dispatch ended.
export default {
  async fetch(request, env) {
    if (request.headers.get('x-lab-auth') !== env.LAB_AUTH || Date.now() > Number(env.LAB_EXPIRES_AT || 0)) {
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
