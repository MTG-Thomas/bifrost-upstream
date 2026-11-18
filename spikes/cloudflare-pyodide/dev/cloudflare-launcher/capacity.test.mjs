import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createEngine, STATE} from './capacity-core.mjs';

const id = 'afe91459-38bd-4c34-8ae5-13e67af6b649';
const profile = {scope: 'test', worker_version: 'test', max_workers: 1,
  memory_limit_bytes: 128 * 1024 ** 2, max_lifetime_seconds: 60, starting_grace_ms: 0};
const reservation = {worker_id: id, worker_version: 'test', memory_limit_bytes: profile.memory_limit_bytes,
  max_lifetime_seconds: 60, drain_grace_seconds: 0};

function fixture() {
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
  };
  const restart = () => createEngine({profile, store, runtime});
  return {restart, runtime, facts, slots};
}

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
