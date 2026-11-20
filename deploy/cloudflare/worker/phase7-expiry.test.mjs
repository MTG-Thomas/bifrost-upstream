import assert from 'node:assert/strict';
import {test} from 'node:test';
import {WorkerContainer} from './phase7-expiry.mjs';

test('expiry fixture sets the platform timer without starting an exit monitor', async () => {
    const timeouts = [];
    const worker = new WorkerContainer({
        storage: {get: async () => ({version: 1, start_requested: true})},
        blockConcurrencyWhile: async (callback) => callback(),
        container: {
            running: true,
            async setInactivityTimeout(ms) { timeouts.push(ms); },
            monitor() { assert.fail('the exit monitor would keep the fixture active'); },
        },
    }, {});
    await worker._ready;
    assert.deepEqual(timeouts, [120_000]);
});
