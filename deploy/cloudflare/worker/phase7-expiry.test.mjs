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

test('starting an expiry Container arms the timer without monitoring the process', async () => {
    const storage = new Map();
    const calls = [];
    const worker = new WorkerContainer({
        storage: {
            get: async (key) => storage.get(key),
            put: async (key, value) => storage.set(key, value),
        },
        blockConcurrencyWhile: async (callback) => callback(),
        container: {
            running: false,
            start() {
                calls.push('start');
                this.running = true;
            },
            async setInactivityTimeout(ms) { calls.push(ms); },
            monitor() { assert.fail('the exit monitor would keep the fixture active'); },
        },
    }, {});
    const binding = { worker_id: '01010101-0101-4101-8101-010101010101', resource_uid: 'expiry-fixture' };
    const response = await worker.fetch(new Request('https://do/start', {
        method: 'POST',
        body: JSON.stringify({binding}),
    }));
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { running: true, lifecycle: 'accepted' });
    assert.deepEqual(calls, ['start', 120_000]);
    assert.equal(storage.get('lifecycle').start_requested, true);
});
