// Pure unit: the generic worker script is CONSTANT per tenant.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DEFAULT_ENGINE_SUBREQUESTS, GENERIC_PYTHON, ENGINE_PYTHON, genericWorkerCode, genericCodeDigest,
  MAX_TRACED_MODULES, mergePrimeList} from './generic-worker-code.mjs';

test('generic code is identical across calls (no per-tenant bake)', async () => {
  const a = genericWorkerCode();
  const b = genericWorkerCode();
  assert.deepEqual(a, b);
  assert.equal(await genericCodeDigest(), await genericCodeDigest());
});

test('digest is a stable sha256 over the script', async () => {
  assert.match(await genericCodeDigest(), /^[0-9a-f]{64}$/);
});

test('script contains no tenant identity and no per-attempt data', () => {
  assert.ok(!GENERIC_PYTHON.includes('org-'), 'must not bake tenant ids');
  assert.ok(!GENERIC_PYTHON.includes('tenant-py-'), 'must not bake worker ids');
  assert.ok(!GENERIC_PYTHON.includes('Bearer'), 'must not carry credentials');
  assert.ok(!GENERIC_PYTHON.includes('<tenant-data>/'), 'name-keyed data map is gone');
  assert.ok(GENERIC_PYTHON.includes('class Default(WorkerEntrypoint)'));
});

test('importer is path-keyed: records storage paths, maps names like production', () => {
  assert.ok(GENERIC_PYTHON.includes('class _PathImporter'), 'path-keyed importer present');
  assert.ok(!GENERIC_PYTHON.includes('_DataImporter'), 'old name-keyed importer removed');
  assert.ok(GENERIC_PYTHON.includes('<tenant-path>/'), 'tracebacks name storage paths');
  assert.ok(GENERIC_PYTHON.includes("__init__.py"), 'package entries resolve by path');
  assert.ok(GENERIC_PYTHON.includes('entry_path'), 'entry loads by storage path, not import name');
});

test('child env is empty: the child holds NO credentials', () => {
  assert.deepEqual(genericWorkerCode().env, {});
});

test('engine child limits: generous subrequest default, explicit overrides', () => {
  assert.equal(DEFAULT_ENGINE_SUBREQUESTS, 1000);
  assert.ok(DEFAULT_ENGINE_SUBREQUESTS >= 50, 'below the measured real-workspace need');
});

test('engine child serializes results with the Linux persistence contract (default=str)', () => {
  assert.ok(ENGINE_PYTHON.includes('"imported": imported}}, default=str)'),
    'datetimes/UUIDs/Decimals in a result must not fail the attempt');
  assert.ok(!ENGINE_PYTHON.includes('Response.json({"upstream_result"'));
});

test('engine child re-establishes attempt-scoped SDK state at every attempt', () => {
  // One persistent child serves every attempt for a tenant. The SDK caches its
  // client per thread, so without this handoff a second attempt keeps presenting
  // the previous attempt's run handle and its SDK/module calls are refused.
  assert.ok(ENGINE_PYTHON.includes('from bifrost.client import _install_cached_access_token'));
  const handoff = ENGINE_PYTHON.indexOf('_install_cached_access_token(body["api_url"], token)');
  const run = ENGINE_PYTHON.search(/await run_authorized_attempt\(\s+body\["execution_id"\]/);
  assert.ok(run > 0, 'the attempt run is present');
  assert.ok(handoff > 0, 'credential handoff present');
  assert.ok(handoff < run, 'handoff happens before the engine run starts');
});

test('warm-up list: union of traced imports, per engine, names only, bounded', () => {
  const first = mergePrimeList(undefined, 'engine-a', ['src.sdk.context', 'bifrost._runtime_io']);
  assert.deepEqual(first, {engine: 'engine-a', modules: ['bifrost._runtime_io', 'src.sdk.context']});
  const second = mergePrimeList(first, 'engine-a', ['src.sdk.context', 'httpx._multipart',
    'os; import subprocess', '../x', 42, null]);
  assert.deepEqual(second.modules, ['bifrost._runtime_io', 'httpx._multipart', 'src.sdk.context']);
  assert.deepEqual(mergePrimeList(second, 'engine-b', ['zoneinfo']).modules, ['zoneinfo'],
    'a new engine digest starts a new list');
  const many = Array.from({length: MAX_TRACED_MODULES + 50}, (_, i) => `m${i}`);
  assert.equal(mergePrimeList(undefined, 'engine-a', many).modules.length, MAX_TRACED_MODULES);
});

test('engine child traces imports per attempt and primes from the host list', () => {
  assert.match(ENGINE_PYTHON, /loaded_before = set\(sys\.modules\)/);
  assert.match(ENGINE_PYTHON, /"imported": imported/);
  assert.match(ENGINE_PYTHON, /_prime\(body\.get\("modules"\) or \[\]\)/);
  assert.doesNotMatch(ENGINE_PYTHON, /_PRIME_MODULES/, 'no hand-kept list');
});

// A function-local `import x` makes x local to the WHOLE function, so any use
// of the module-level x earlier in that function raises UnboundLocalError.
// This shipped once (an `import sys` in the inventory branch broke every
// attempt as soon as the attempt path used sys).
function importedNames(line) {
  const from = line.match(/^\s*from\s+\S+\s+import\s+(.+)$/);
  if (from) return from[1].split(',').map((part) => part.trim().split(/\s+as\s+/).pop());
  const plain = line.match(/^\s*import\s+(.+)$/);
  if (plain) return plain[1].split(',').map((part) => part.trim().split(/\s+as\s+/).pop().split('.')[0]);
  return [];
}

test('engine child: no function-local import shadows a module-level one', () => {
  const lines = ENGINE_PYTHON.split('\n');
  const moduleLevel = new Set(lines.filter((line) => /^(import|from)\s/.test(line)).flatMap(importedNames));
  const local = lines.filter((line) => /^\s+(import|from)\s/.test(line)).flatMap(importedNames);
  assert.deepEqual(local.filter((name) => moduleLevel.has(name)), []);
});

test('keep-warm exercise: a dry attempt under the given handle, never attempt data', () => {
  const start = ENGINE_PYTHON.indexOf('async def _exercise(handle):');
  const exercise = ENGINE_PYTHON.slice(start, ENGINE_PYTHON.indexOf('def _prime(names):'));
  assert.ok(start > 0);
  assert.ok(exercise.includes('_install_cached_access_token(api_url, handle)'));
  assert.ok(exercise.includes('"engine_token": handle'));
  assert.ok(exercise.includes('api_url = "https://test-api.invalid"'),
    'only the virtual origin the outbound gateway owns');
  assert.ok(!exercise.includes('body['), 'no attempt data reaches the dry run');
  // The real attempt always installs its own credential first, so the dry
  // run's handle never survives into it.
  assert.ok(ENGINE_PYTHON.includes('if token:\n            _install_cached_access_token(body["api_url"], token)'));
});
