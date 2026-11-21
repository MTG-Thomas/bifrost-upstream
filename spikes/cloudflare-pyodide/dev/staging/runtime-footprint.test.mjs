import assert from 'node:assert/strict';
import {test} from 'node:test';
import {trimRuntime} from './runtime-footprint.mjs';

const cache = 'src/core/module_cache_sync.py';
const sdk = 'bifrost/client.py';

function sourceRuntime() {
  return {
    [cache]: {py: `
import redis

def _get_sync_redis():
    global _sync_redis_client
    _sync_redis_client = redis.Redis.from_url()

def _get_cached_module_resolution():
    try:
        client = _get_sync_redis()
    except redis.RedisError:
        pass

def _get_exact_scoped_module():
    try:
        client = _get_sync_redis()
    except redis.RedisError:
        pass

def get_module_sync():
    if _execution_child:
        return fetch_from_gateway()
    try:
        client = _get_sync_redis()
    except redis.RedisError:
        pass
`},
    [sdk]: {py: `
import webbrowser

def open_browser(enabled, full_url):
            if enabled:
                try:
                    webbrowser.open(full_url)
                except Exception:
                    pass
`},
    'python_modules/redis/__init__.py': {py: 'VERSION = "fixture"'},
    'python_modules/redis/py.typed': {base64: ''},
    'python_modules/pkg/example.pyi': {base64: ''},
    'python_modules/pkg/schema.json': {base64: 'e30='},
  };
}

test('defers Redis until parent-only paths and webbrowser until login', () => {
  const input = sourceRuntime();
  const output = trimRuntime(input);
  assert.doesNotMatch(output[cache].py, /^import redis$/m);
  assert.equal(output[cache].py.match(/^    import redis$/gm).length, 4);
  assert.match(output[cache].py, /return fetch_from_gateway\(\)\n    import redis/);
  assert.match(output[sdk].py, /try:\n +import webbrowser\n\n +webbrowser\.open\(full_url\)/);
  assert.match(input[sdk].py, /^import webbrowser$/m);
});

test('removes vendor typing files while retaining Python and runtime data', () => {
  const input = sourceRuntime();
  const output = trimRuntime(input);
  assert.ok(!Object.hasOwn(output, 'python_modules/redis/py.typed'));
  assert.ok(!Object.hasOwn(output, 'python_modules/pkg/example.pyi'));
  assert.equal(output['python_modules/redis/__init__.py'], input['python_modules/redis/__init__.py']);
  assert.equal(output['python_modules/pkg/schema.json'], input['python_modules/pkg/schema.json']);
});

test('refuses a companion source whose Redis paths changed', () => {
  const input = sourceRuntime();
  input[cache].py = input[cache].py.replace('client = _get_sync_redis()', 'client = other()');
  assert.throws(() => trimRuntime(input), /expected 3 occurrence/);
});
