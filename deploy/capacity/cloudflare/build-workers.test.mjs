import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {assertMeterInventory, buildWorkers, bundleWorker, qualifiedMeterLimits} from './build-workers.mjs';

test('a Workers release requires a positive sequence before reading artifacts', async () => {
  await assert.rejects(buildWorkers('unused-runtime', 'unused-output', 'a'.repeat(40), {}, '1.0.0', 0),
    /positive Workers release sequence/);
  await assert.rejects(buildWorkers('unused-runtime', 'unused-output', 'a'.repeat(40), {}, '1.0.0-rc.01', 1),
    /SemVer Bifrost release version/);
});

const source = new URL('../../../api/src/services/worker_pool/cloudflare_meter_bounds.json', import.meta.url);
const qualification = () => JSON.parse(readFileSync(source, 'utf8'));
const meterInventory = JSON.parse(readFileSync(new URL('./meter-inventory.json', import.meta.url), 'utf8'));

test('every input in the packaged Workers import graph has a reviewed billing classification', async () => {
  for (const role of ['launcher', 'gateway', 'capacity']) {
    const inputs = await bundleWorker(role, Buffer.from('export default {};'), {inventoryOnly: true});
    const embedded = 'bifrost-runtime:generated-runtime.mjs';
    // The production builder verifies this input from the real runtime artifact.
    if (embedded in inputs) inputs[embedded] = 'fd03ddf5bd8e87a4b11941678b526455f118eb7913b31a0a0be473521cd7f0d5';
    assertMeterInventory(role, inputs, meterInventory);
    assert.throws(() => assertMeterInventory(role, {...inputs, 'unreviewed.mjs': 'unknown'}, meterInventory),
      /Unclassified Cloudflare meter input/);
    const [first] = Object.keys(inputs);
    assert.throws(() => assertMeterInventory(role, {...inputs, [first]: 'changed charge site'}, meterInventory),
      /Review the Cloudflare meter inventory/);
  }
  const docs = new TextDecoder('utf-8', {fatal: true}).decode(readFileSync(new URL(
    '../../../spikes/cloudflare-pyodide/dev/cloudflare-launcher/PHASE1-USAGE.md', import.meta.url)));
  for (const id of meterInventory.paths) assert.ok(docs.includes(`| \`${id}\` |`), id);
});

test('build uses the pinned measured capacity CPU and request inputs with the reviewed margin', () => {
  assert.deepEqual(qualifiedMeterLimits(qualification()), {
    capacityCpuMs: 104, launcherCpuMs: 2133, sdkRequests: 40, engineCpuMs: 30_000,
    bootCpuMs: 30_000, warmCpuMs: 440, exerciseCpuMs: 672,
    launcherMeters: Object.fromEntries(Object.entries(qualification().qualification.launcher_reference.observed)
      .map(([meter, count]) => [meter, count * 10])),
  });
});

for (const [name, corrupt, expected] of [
  ['missing meter', bounds => delete bounds.per_launcher_second.log_events, /positive bound/],
  ['zero priced meter', bounds => { bounds.per_fleet_fixed.sqlite_rows_written = '0'; }, /positive bound/],
  ['missing qualification', bounds => delete bounds.qualification, /qualification measurements/],
  ['unmeasured CPU', bounds => { bounds.qualification.cpu_reference.samples.capacity_observe.p99_us = 0; },
    /positive qualification samples/],
  ['missing keep-warm samples', bounds => { delete bounds.qualification.cpu_reference.samples.exercise; },
    /positive qualification samples/],
  ['missing measured module', bounds => delete bounds.qualification.capacity_module_sha256, /qualification measurements/],
  ['missing launcher counter', bounds => delete bounds.qualification.launcher_reference.observed.log_events, /qualification measurements/],
  ['zero launcher counter', bounds => { bounds.qualification.launcher_reference.observed.do_requests = 0; }, /qualification measurements/],
  ['CPU bound below the enforced limit', bounds => { bounds.per_launcher_second.worker_cpu_ms = '30001'; },
    /measured deployment limits/],
]) {
  test(`the Workers build rejects ${name}`, () => {
    const bounds = qualification();
    corrupt(bounds);
    assert.throws(() => qualifiedMeterLimits(bounds), expected);
  });
}
