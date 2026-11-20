import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {qualifiedMeterLimits} from './build-workers.mjs';

const source = new URL('../../../api/src/services/worker_pool/cloudflare_meter_bounds.json', import.meta.url);
const qualification = () => JSON.parse(readFileSync(source, 'utf8'));

test('build uses the pinned measured capacity CPU and request inputs with the reviewed margin', () => {
  assert.deepEqual(qualifiedMeterLimits(qualification()), {
    capacityCpuMs: 425, sdkRequests: 40, engineCpuMs: 30_000,
  });
});

for (const [name, corrupt, expected] of [
  ['missing meter', bounds => delete bounds.per_launcher_second.log_events, /positive bound/],
  ['zero priced meter', bounds => { bounds.per_fleet_fixed.sqlite_rows_written = '0'; }, /positive bound/],
  ['missing qualification', bounds => delete bounds.qualification, /qualification measurements/],
  ['unmeasured CPU', bounds => { bounds.qualification.capacity_cpu_us = 0; }, /qualification measurements/],
  ['CPU bound below the enforced limit', bounds => { bounds.per_launcher_second.worker_cpu_ms = '30001'; }, /measured limit/],
]) {
  test(`the Workers build rejects ${name}`, () => {
    const bounds = qualification();
    corrupt(bounds);
    assert.throws(() => qualifiedMeterLimits(bounds), expected);
  });
}
