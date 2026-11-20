import assert from 'node:assert/strict';
import test from 'node:test';
import {OVERFLOW_MEMORY_MIB, vmHoldMemoryMiB} from './containers-local.mjs';

test('VM hold leaves room for working-set growth while blocking the update stage', () => {
  const mib = 2 ** 20;
  const measured = 586376806;
  const hold = vmHoldMemoryMiB(measured) * mib;
  assert.ok(hold < measured - 64 * mib);
  assert.ok(measured + 64 * mib - hold < OVERFLOW_MEMORY_MIB * mib);
});
