// Local-only diagnostics. Python reports live allocations and cache sizes from
// inside the reused child. The inspector reports V8 memory by isolate.
export function pythonMemoryProbe({collect}) {
  return `
    import gc, linecache, sys
    import logging as probe_logging
    from src.core.module_cache_sync import _get_resolution_cache
    from runtime_runner import recorded_workspace_paths
    from pyodide.ffi import create_proxy
    probe_view = None
    probe_proxy = create_proxy(memoryview(bytearray(1)))
    try:
        probe_view = probe_proxy.getBuffer("u8")
        probe_wasm = probe_view.data.buffer.byteLength
    finally:
        if probe_view is not None:
            probe_view.release()
        probe_proxy.destroy()
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
              "logging_handlers": len(probe_logging._handlerList), "traced_bytes": traced,
              "wasm_bytes": probe_wasm, "gc_enabled": gc.isenabled(),
              "gc_collections": [item["collections"] for item in gc.get_stats()]}
    sys._fixture_probe_attempts = getattr(sys, "_fixture_probe_attempts", 0) + 1
    if ${collect ? 'True' : 'False'} and sys._fixture_probe_attempts % 32 == 0 and traced is not None:
        memory["allocation_sites"] = [
            {"source": "/".join(site.traceback[0].filename.rsplit("/", 2)[-2:]),
             "line": site.traceback[0].lineno, "bytes": site.size, "count": site.count}
            for site in tracemalloc.take_snapshot().statistics("lineno")[:12]
        ]
        memory["collected"] = gc.collect()
        memory["traced_after_collect"] = tracemalloc.get_traced_memory()[0]
        memory["objects_after_collect"] = len(gc.get_objects())
`;
}

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
        const engine = [target.id, target.title, target.url].some(value => value?.includes('tenant-eng-'));
        return {isolate: identities.get(target.id), engine, ...reply.result,
          ...(reply.error ? {error: reply.error.code} : {})};
      } finally {
        socket.close();
      }
    }));
  };
}
