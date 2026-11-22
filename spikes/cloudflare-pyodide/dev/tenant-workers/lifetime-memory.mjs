// Local-only diagnostics. Python reports live allocations and cache sizes from
// inside the reused child. The inspector reports V8 memory by isolate.
export const pythonMemoryProbe = `
    import gc, linecache, sys, logging
    from src.core.module_cache_sync import _get_resolution_cache
    from runtime_runner import recorded_workspace_paths
    try:
        import tracemalloc
    except ImportError:
        traced = None
    else:
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        traced = tracemalloc.get_traced_memory()[0]
    memory = {"modules": len(sys.modules), "linecache_entries": len(linecache.cache),
              "tracked_objects": len(gc.get_objects()), "allocated_blocks": sys.getallocatedblocks(),
              "resolution_cache_entries": len(_get_resolution_cache()), "globals": len(globals()),
              "workspace_imports": len(recorded_workspace_paths),
              "logging_handlers": len(logging._handlerList), "traced_bytes": traced}
`;

export function isolateSampler(inspector) {
  const identities = new Map();
  let sample = 0;
  return async () => {
    if (sample++ % 10 !== 1) return null;
    const list = new URL('/json/list', inspector);
    list.protocol = 'http:';
    const response = await fetch(list);
    if (!response.ok) throw new Error(`Inspector target list returned ${response.status}`);
    return Promise.all((await response.json()).map(async (target) => {
      if (!identities.has(target.id)) identities.set(target.id, identities.size + 1);
      const url = new URL(target.webSocketDebuggerUrl);
      url.host = inspector.host;
      const socket = new WebSocket(url);
      try {
        const reply = await new Promise((resolve, reject) => {
          socket.addEventListener('error', reject, {once: true});
          socket.addEventListener('close', () => reject(new Error('Inspector closed before replying')), {once: true});
          socket.addEventListener('open', () => socket.send(JSON.stringify({id: 1, method: 'Runtime.getHeapUsage'})), {once: true});
          socket.addEventListener('message', ({data}) => {
            const result = JSON.parse(data);
            if (result.id === 1) resolve(result);
          });
        });
        return {isolate: identities.get(target.id), ...reply.result, ...(reply.error ? {error: reply.error.code} : {})};
      } finally {
        socket.close();
      }
    }));
  };
}
