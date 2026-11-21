// The staging runtime builds from a pinned companion artifact. Move imports
// within its existing paths and fail the build if those paths changed.
function rewrite(source, anchor, replacement, count = 1) {
  if (source.split(anchor).length !== count + 1) {
    throw new Error(`Cold-start patch expected ${count} occurrence(s) of ${JSON.stringify(anchor)}`);
  }
  return source.replaceAll(anchor, replacement);
}

export function trimRuntime(packaged) {
  const moduleCache = 'src/core/module_cache_sync.py';
  const sdkClient = 'bifrost/client.py';
  const redis = packaged[moduleCache]?.py;
  const client = packaged[sdkClient]?.py;
  if (typeof redis !== 'string' || typeof client !== 'string') {
    throw new Error('Cold-start patches require the companion module cache and SDK client sources');
  }
  const redisConnection = '    global _sync_redis_client\n    _sync_redis_client = redis.Redis.from_url(';
  const redisRead = '    try:\n        client = _get_sync_redis()';
  const browserOpen = '                    webbrowser.open(full_url)';
  const runtime = Object.fromEntries(Object.entries(packaged).filter(([key]) =>
    !key.startsWith('python_modules/') || !(key.endsWith('.pyi') || key.endsWith('/py.typed'))));
  // The execution-child branch returns before the parent-only Redis reads.
  // Workflows can still import Redis explicitly.
  runtime[moduleCache] = {py: rewrite(
    rewrite(rewrite(redis, '\nimport redis\n', '\n'), redisConnection,
      '    import redis\n\n' + redisConnection), redisRead, '    import redis\n\n' + redisRead, 3)};
  runtime[sdkClient] = {py: rewrite(rewrite(client, '\nimport webbrowser\n', '\n'), browserOpen,
    '                    import webbrowser\n\n' + browserOpen)};
  return runtime;
}
