import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {qualifiedMeterLimits} from './build-workers.mjs';

const source = new URL('../../../api/src/services/worker_pool/cloudflare_meter_bounds.json', import.meta.url);
const qualification = () => JSON.parse(readFileSync(source, 'utf8'));

test('build uses the pinned measured capacity CPU and request inputs with the reviewed margin', () => {
  assert.deepEqual(qualifiedMeterLimits(qualification()), {
    capacityCpuMs: 400, sdkRequests: 40, engineCpuMs: 30_000,
    launcherMeters: Object.fromEntries(Object.entries(qualification().qualification.launcher_reference.observed)
      .map(([meter, count]) => [meter, count * 10])),
  });
});

for (const [name, corrupt, expected] of [
  ['missing meter', bounds => delete bounds.per_launcher_second.log_events, /positive bound/],
  ['zero priced meter', bounds => { bounds.per_fleet_fixed.sqlite_rows_written = '0'; }, /positive bound/],
  ['missing qualification', bounds => delete bounds.qualification, /qualification measurements/],
  ['unmeasured CPU', bounds => { bounds.qualification.capacity_cpu_us = 0; }, /qualification measurements/],
  ['missing measured module', bounds => delete bounds.qualification.capacity_module_sha256, /qualification measurements/],
  ['missing launcher counter', bounds => delete bounds.qualification.launcher_reference.observed.log_events, /qualification measurements/],
  ['zero launcher counter', bounds => { bounds.qualification.launcher_reference.observed.do_requests = 0; }, /qualification measurements/],
  ['CPU bound below the enforced limit', bounds => { bounds.per_launcher_second.worker_cpu_ms = '30001'; }, /measured limit/],
]) {
  test(`the Workers build rejects ${name}`, () => {
    const bounds = qualification();
    corrupt(bounds);
    assert.throws(() => qualifiedMeterLimits(bounds), expected);
  });
}
