import assert from 'node:assert/strict';
import test from 'node:test';
import {faultDirectory, mountFaultHook} from './local-guest-fault.mjs';

const image = 'ghcr.io/example/linux@sha256:' + 'a'.repeat(64);
const source = '/rehearsal/combined-guest-site';

test('normal or hosted launch receives neither a fault mount nor a hook environment', () => {
  const launch = {Image: image, Env: ['BIFROST_WORKER_RESOURCE_UID=fixture'], HostConfig: {Mounts: []}};
  const original = structuredClone(launch);
  assert.equal(mountFaultHook(launch, {mode: 'off', image, source}), false);
  assert.deepEqual(launch, original);
  assert.ok(!JSON.stringify(launch).includes(faultDirectory));
  assert.ok(!launch.Env.some((value) => value.startsWith('PYTHONPATH=')));
});

test('local fatal launch mounts the hook read-only and retains the exact image reference', () => {
  const launch = {Image: image, Env: [
    'BIFROST_WORKER_RESOURCE_UID=fixture', 'REHEARSAL_FAIL_GUEST_AFTER_HELLO=1', 'PYTHONPATH=/app',
  ]};
  assert.equal(mountFaultHook(launch, {mode: 'fatal', image, source}), true);
  assert.equal(launch.Image, image);
  assert.equal(launch.Env.filter((value) => value.startsWith('PYTHONPATH=')).length, 1);
  assert.ok(launch.Env.includes('PYTHONPATH=' + faultDirectory + ':/app'));
  assert.deepEqual(launch.HostConfig.Mounts, [{Type: 'bind', Source: source, Target: faultDirectory, ReadOnly: true}]);
});
