import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import {createEngine, STATE, WORKERS_DEADLINE_SKEW_MS} from './capacity-core.mjs';

const id = 'afe91459-38bd-4c34-8ae5-13e67af6b649';
const profile = {scope: 'test', worker_version: 'test', max_workers: 1,
  memory_limit_bytes: 128 * 1024 ** 2, max_lifetime_seconds: 60, starting_grace_ms: 0};
const reservation = {worker_id: id, worker_version: 'test', memory_limit_bytes: profile.memory_limit_bytes,
  max_lifetime_seconds: 60, drain_grace_seconds: 0,
  resource_uid: 'a'.repeat(32), control_key: 'b'.repeat(64)};

function fixture(clock) {
  let row;
  const slots = new Set();
  const store = {
    read: async () => structuredClone(row),
    write: async (_, value) => { row = structuredClone(value); },
    reserve: async (workerId) => {
      if (!slots.has(workerId) && slots.size >= 1) return false;
      slots.add(workerId);
      return true;
    },
    discard: async () => { row = undefined; },
    release: async (workerId) => slots.delete(workerId),
  };
  const facts = {state: STATE.PREPARED, start_fact: 'never_started'};
  const runtime = {
    prepare: async () => {},
    observe: async () => facts,
    start: async () => {
      facts.state = STATE.RUNNING;
      facts.start_fact = 'accepted';
      return facts;
    },
    destroy: async () => ({state: STATE.DELETED}),
    clearStorage: async () => {},
  };
  const restart = () => createEngine({profile, store, runtime, clock});
  return {restart, runtime, facts, slots, store};
}

test('prepare accepts the clock skew budgeted by the API', async () => {
  const protocol = readFileSync(new URL('../../../../api/src/worker/remote/protocol.py', import.meta.url), 'utf8');
  const allowance = Number(protocol.match(/^WORKERS_DEADLINE_SKEW_SECONDS = (\d+)$/m)?.[1]);
  assert.equal(1000 * allowance, WORKERS_DEADLINE_SKEW_MS);

  const now = 1_000_000;
  const {restart} = fixture({now: () => now});
  const deadline_ms = now + reservation.max_lifetime_seconds * 1000 + WORKERS_DEADLINE_SKEW_MS;
  await assert.rejects(restart().prepare(id, {...reservation, deadline_ms: deadline_ms + 1}), /invalid reservation deadline/);
  assert.equal((await restart().prepare(id, {...reservation, deadline_ms})).state, STATE.PREPARED);
});

test('a lost storage-clear reply preserves the final receipt and the retirement fence', async () => {
  const {restart, runtime, slots} = fixture();
  const {resource_uid} = await restart().prepare(id, reservation);
  let seals = 0;
  const usage = {sqlite_rows_written: 21, do_requests: 3, log_events: 1};
  runtime.destroy = async () => {
    seals++;
    return {state: STATE.DELETED, usage};
  };
  runtime.clearStorage = async () => { throw new Error('lost clear response'); };
  await assert.rejects(restart().remove(id, {resource_uid}), /delete outcome unknown/);
  await assert.rejects(restart().prepare(id, reservation), /deletion is pending/);
  assert.equal(slots.size, 1);
  runtime.clearStorage = async () => {};
  const removed = await restart().remove(id, {resource_uid});
  assert.equal(removed.state, STATE.DELETED);
  assert.deepEqual(removed.usage, {...usage, launcher_storage_cleared: true, ledger_receipt_pending: true});
  assert.deepEqual(await restart().get(id), removed);
  assert.equal(seals, 1);
  assert.equal(slots.size, 0);
});

test('receipt acknowledgement compacts only the bound deleted worker and replays its fence', async () => {
  const {restart, runtime, store} = fixture();
  const {resource_uid} = await restart().prepare(id, reservation);
  const ack = {resource_uid, receipt_ack: true};
  await assert.rejects(restart().remove(id, ack), /deletion is unconfirmed/);
  runtime.destroy = async () => ({state: STATE.DELETED, usage: {routes: {fixture: ['trace']}}});
  await restart().remove(id, {resource_uid});
  await assert.rejects(restart().remove(id, {...ack, resource_uid: 'wrong'}), /UID does not match/);
  assert.ok((await restart().get(id)).usage);
  const compacted = await restart().remove(id, ack);
  assert.equal(compacted.usage, undefined);
  assert.deepEqual(await restart().remove(id, ack), compacted);
  assert.deepEqual(await restart().get(id), compacted);
  assert.deepEqual(Object.keys(await store.read(id)).sort(),
    ['out_of_memory', 'reservation', 'resource_uid', 'scope', 'state', 'worker_id']);
  await assert.rejects(restart().prepare(id, reservation), /already terminated/);
});

test('uncertain prepare releases its slot and readback leaves retry to start', async () => {
  const {restart, runtime, slots} = fixture();
  let uid;
  runtime.prepare = async (row) => {
    uid = row.resource_uid;
    throw new Error('reply lost');
  };
  await assert.rejects(restart().prepare(id, reservation));
  assert.equal(slots.size, 0);
  let retries = 0;
  runtime.prepare = async (row) => {
    assert.equal(row.resource_uid, uid);
    retries++;
  };
  assert.equal((await restart().get(id)).resource_uid, uid);
  assert.equal(retries, 0);
  assert.equal(slots.size, 0);
  assert.equal((await restart().start(id, {resource_uid: uid})).state, STATE.STARTING);
  assert.equal(retries, 1);
  assert.equal(slots.size, 1);
});

test('observe reconciles a lost start reply without a second start', async () => {
  const {restart, runtime} = fixture();
  const {resource_uid} = await restart().prepare(id, reservation);
  const start = runtime.start;
  let starts = 0;
  runtime.start = async () => {
    starts++;
    await start();
    throw new Error('reply lost');
  };
  await assert.rejects(restart().start(id, {resource_uid}), /outcome unknown/);
  const engine = restart();
  assert.equal((await engine.get(id)).state, STATE.RUNNING);
  assert.equal((await engine.start(id, {resource_uid})).state, STATE.RUNNING);
  assert.equal(starts, 1);
});

test('prepare replays only the original UID and key and never returns key bytes', async () => {
  const {restart, runtime} = fixture();
  let prepares = 0;
  runtime.prepare = async (row) => {
    prepares++;
    assert.equal(row.control_key, reservation.control_key);
  };
  const prepared = await restart().prepare(id, reservation);
  assert.equal(prepared.resource_uid, reservation.resource_uid);
  assert.deepEqual(await restart().prepare(id, reservation), prepared);
  await assert.rejects(restart().prepare(id, {...reservation, resource_uid: 'c'.repeat(32)}), /conflicting/);
  await assert.rejects(restart().prepare(id, {...reservation, control_key: 'd'.repeat(64)}), /conflicting/);
  assert.equal(prepares, 1);
  assert.equal(JSON.stringify(await restart().get(id)).includes(reservation.control_key), false);
});

test('a start without a durable witness is replayed only on affirmative never-started evidence', async () => {
  const {restart, runtime, facts} = fixture();
  const engine = restart();
  const {resource_uid} = await engine.prepare(id, reservation);
  const start = runtime.start;
  runtime.start = async () => { throw new Error('did not reach launcher'); };
  await assert.rejects(engine.start(id, {resource_uid}));
  facts.start_fact = 'unknown';
  runtime.start = start;
  await assert.rejects(restart().start(id, {resource_uid}), /physical launch is unproven/);
  assert.equal(facts.state, STATE.PREPARED);
  facts.start_fact = 'never_started';
  assert.equal((await restart().start(id, {resource_uid})).state, STATE.STARTING);
});

test('unconfirmed release holds the slot and fences start until deletion is confirmed', async () => {
  const {restart, runtime, slots} = fixture();
  const engine = restart();
  const {resource_uid} = await engine.prepare(id, reservation);
  runtime.destroy = async () => ({state: STATE.STARTING});
  await assert.rejects(engine.remove(id, {resource_uid}), /delete outcome unknown/);
  assert.equal(slots.size, 1);
  await assert.rejects(restart().start(id, {resource_uid}), /transition is pending/);
  runtime.destroy = async () => ({state: STATE.DELETED});
  assert.equal((await engine.remove(id, {resource_uid})).state, STATE.DELETED);
  assert.equal(slots.size, 0);
  await assert.rejects(restart().start(id, {resource_uid}), /terminated/);
});
