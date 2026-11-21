// Measure sequential requests against two packaged modules in Linux workerd.
// Usage: node qualify-cpu.mjs <package directory> [batches]
import assert from 'node:assert/strict';
import {randomUUID, createHash} from 'node:crypto';
import {readFileSync, readdirSync} from 'node:fs';
import {createRequire} from 'node:module';
import {join, resolve} from 'node:path';
import {capacityWorker} from '../e2e/capacity-miniflare.mjs';

const {Miniflare} = createRequire(import.meta.resolve('wrangler'))('miniflare');
const settings = {token: 'local-qualification-' + 'a'.repeat(40), scope: 'workers', version: 'test', maxWorkers: 1};
const headers = {Authorization: `Bearer ${settings.token}`, 'X-Bifrost-Capacity-Scope': settings.scope};
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const packagePath = resolve(process.argv[2]);
const manifest = JSON.parse(readFileSync(join(packagePath, 'manifest.json')));
const launcher = readFileSync(join(packagePath, 'launcher.mjs'));
const capacity = join(packagePath, 'capacity.mjs');
const bindings = Object.fromEntries(manifest.workers.launcher.bindings
  .filter(binding => binding.text !== undefined).map(binding => [binding.name, {type: 'text', value: binding.text}]));
const observations = {};
const lifecycleCases = Number(process.argv[3] ?? 4);
assert.ok(Number.isSafeInteger(lifecycleCases) && lifecycleCases > 0);

function workerdCpuClock() {
  const children = readFileSync(`/proc/${process.pid}/task/${process.pid}/children`, 'utf8').trim().split(/\s+/);
  const ids = children.filter(pid => readFileSync(`/proc/${pid}/comm`, 'utf8').trim() === 'workerd');
  assert.equal(ids.length, 1, 'CPU qualification isolates one workerd process');
  const task = `/proc/${ids[0]}/task`;
  return () => readdirSync(task).reduce((sum, tid) =>
    sum + Number(readFileSync(`${task}/${tid}/schedstat`, 'utf8').split(' ')[0]), 0);
}

function workflow(tenantId, solution) {
  const executionId = randomUUID();
  const routeId = `workflow:${executionId}:${randomUUID()}`;
  const workflowDeadline = new Date(Date.now() + 60_000).toISOString();
  const file = 'workflows/reference.py';
  const solutionId = solution ? randomUUID() : null;
  let source = readFileSync(new URL('../e2e/fixture.py', import.meta.url), 'utf8');
  const sources = {};
  if (solution) {
    sources['phase6_support/values.py'] = 'INSTALL_VALUE = "solution"\n';
    source = source.replace('import logging', 'import logging\nfrom phase6_support.values import INSTALL_VALUE')
      .replace('"table_count": len(rows)', '"table_count": len(rows), "install_value": INSTALL_VALUE');
  }
  sources[file] = source;
  return {tenantId, executionId, routeId, workflowDeadline, context: {
    workflow_deadline: workflowDeadline, file_path: file, function_name: 'phase5a_fixture',
    name: 'phase5a_fixture', content_hash: sha256(source),
    parameters: {case: solution ? 'solution' : 'loose'},
    engine_token: 'fixture-only-access', transient: true, no_cache: true, organization_id: tenantId, solution_id: solutionId,
    caller: {user_id: tenantId, email: 'fixture@example.invalid', name: 'Qualifying fixture'},
    cloudflare_workspace: {manifest: Object.fromEntries(Object.entries(sources).map(([path, code]) => [path, sha256(code)])),
      sources, packages: [], https_hosts: ['phase5-fixture.invalid'], cpu_budget_ms: 30_000,
      subrequest_allowance: 12, solution_id: solutionId},
  }};
}

for (let batch = 0; batch < lifecycleCases; batch++) {
  const mf = new Miniflare({cf: false, workers: [
    capacityWorker({...settings, bundle: capacity}),
    {dev: {outboundService: {type: 'worker', worker: 'origin'}}, config: {
      name: 'launcher', compatibilityDate: '2026-10-30', exports: {
        CloudflareLauncher: {type: 'durable-object', storage: 'sqlite'},
        TenantHost: {type: 'durable-object', storage: 'sqlite'}, TenantApi: {type: 'worker'},
      },
      env: {...bindings, API_ORIGIN: {type: 'text', value: 'https://test-api.invalid'},
        BIFROST_TENANT_LINGER_SECONDS: {type: 'text', value: '90'},
        // Qualify CPU in the host and child independently of control delivery.
        BIFROST_METER_GENERATIONS: {type: 'text', value: 'false'},
        TENANT_GATEWAY: {type: 'worker', worker: 'gateway'},
        LOADER: {type: 'worker-loader'}, HOSTS: {type: 'durable-object', worker: 'launcher', exportName: 'TenantHost'}},
      manifest: {mainModule: 'launcher.mjs', modules: {'launcher.mjs': {type: 'esm', contents: launcher.toString()}}},
    }},
    {config: {name: 'gateway', compatibilityDate: '2026-10-30',
      env: {API_ORIGIN: {type: 'text', value: 'https://test-api.invalid'}, SDK_SCOPE: {type: 'text', value: 'sdk'}},
      manifest: {mainModule: 'gateway.mjs', modules: {'gateway.mjs': {type: 'esm',
        contents: readFileSync(join(packagePath, 'gateway.mjs'), 'utf8')}}},
    }},
    {config: {name: 'origin', compatibilityDate: '2026-10-30', manifest: {
      mainModule: 'origin.mjs', modules: {'origin.mjs': {type: 'esm', contents: `
        export default {fetch(request) {
          const path = new URL(request.url).pathname;
          if (path === '/api/sdk/tables/list') return Response.json([]);
          if (path === '/health/ready' || path.startsWith('/hold/')) return new Response(null, {status: 200});
          return new Response(null, {status: 404});
        }};
      `}},
    }}},
  ]});
  try {
    const provider = await mf.getWorker('capacity');
    const hosts = await mf.getDurableObjectNamespace('HOSTS', 'launcher');
    const cpu = workerdCpuClock();
    const measure = async (path, call) => {
      const start = cpu();
      const result = await call();
      const us = Math.ceil((cpu() - start) / 1000);
      assert.ok(us > 0, `No sequential CPU sample for ${path}`);
      (observations[path] ??= []).push(us);
      return result;
    };
    const tenantId = randomUUID();
    const host = hosts.getByName(tenantId);
    const workerId = randomUUID();
    const workerPath = 'workers/' + workerId;
    const uid = randomUUID().replaceAll('-', '');
    const capacityCall = async (path, method, body) => {
      const response = await provider.fetch('https://capacity.invalid/v1/' + path,
        {headers, method, body: body && JSON.stringify(body)});
      assert.equal(response.status, 200, await response.clone().text());
      return response.json();
    };
    await measure('capacity_prepare', () => capacityCall(workerPath, 'PUT', {
      worker_id: workerId, worker_version: settings.version, memory_limit_bytes: 128 * 1024 ** 2,
      max_lifetime_seconds: 60, drain_grace_seconds: 5, resource_uid: uid,
      control_key: 'a'.repeat(64),
    }));
    for (let i = 0; i < 25; i++) {
      await measure('capacity_profile', () => capacityCall('capabilities', 'GET'));
      await measure('capacity_observe', () => capacityCall(workerPath, 'GET'));
    }
    await measure('capacity_start', () => capacityCall(workerPath + '/start', 'POST', {resource_uid: uid}));
    const deleted = await measure('capacity_delete', () => capacityCall(workerPath, 'DELETE', {resource_uid: uid}));
    assert.equal(deleted.state, 'deleted');
    const boot = await measure('boot', () => host.warm({tenantId}));
    assert.equal(boot.first_request, true);
    for (const solution of [false, true]) {
      const input = workflow(tenantId, solution);
      const outcome = await measure(solution ? 'solution' : 'loose', () => host.run(input));
      assert.equal(outcome.output?.status, 'Success', JSON.stringify(outcome));
    }
    for (let i = 0; i < 25; i++) {
      const warm = await measure('warm', () => host.warm({tenantId}));
      assert.equal(warm.first_request, false);
      const exercise = await measure('exercise', () => host.exercise({tenantId, readyOnly: true}));
      assert.equal(exercise.first_request, false);
      assert.ok(exercise.exercise);
    }
    const idle = randomUUID();
    const coldExercise = await measure('cold_exercise', () => hosts.getByName(idle).exercise({tenantId: idle}));
    assert.equal(coldExercise.first_request, true);
    assert.ok(coldExercise.exercise);
  } finally {
    await mf.dispose();
  }
}

const samples = Object.fromEntries(Object.entries(observations).map(([path, values]) => {
  const sorted = values.toSorted((a, b) => a - b);
  return [path, {samples: sorted.length, p99_us: sorted[Math.ceil(sorted.length * 0.99) - 1]}];
}));
console.info('qualified-cpu', JSON.stringify({run: randomUUID(), launcher: sha256(launcher),
  capacity: sha256(readFileSync(capacity)), samples}));
