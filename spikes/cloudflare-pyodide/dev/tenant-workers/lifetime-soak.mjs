import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {globSync, readFileSync, writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {setTimeout as delay} from 'node:timers/promises';

const seconds = 900;

function processSampler() {
  const childPids = readFileSync(`/proc/${process.pid}/task/${process.pid}/children`, 'utf8').trim().split(/\s+/);
  const workerd = childPids.filter(pid => readFileSync(`/proc/${pid}/comm`, 'utf8').trim() === 'workerd');
  assert.equal(workerd.length, 1, 'Expected one local workerd process');
  const proc = `/proc/${workerd[0]}`;
  const ticksPerSecond = Number(execFileSync('getconf', ['CLK_TCK'], {encoding: 'utf8'}));
  return () => {
    const status = readFileSync(join(proc, 'status'), 'utf8');
    const [, peak, rss] = /VmHWM:\s+(\d+) kB\nVmRSS:\s+(\d+) kB/.exec(status);
    const stat = readFileSync(join(proc, 'stat'), 'utf8').split(') ')[1].split(' ');
    return {rss_kib: Number(rss), peak_rss_kib: Number(peak),
      cpu_ms: (Number(stat[11]) + Number(stat[12])) * 1000 / ticksPerSecond};
  };
}

function readMeter(storage) {
  for (const path of globSync('**/*.sqlite', {cwd: storage})) {
    const db = new DatabaseSync(join(storage, path), {readOnly: true});
    try {
      if (!db.prepare("SELECT 1 FROM sqlite_master WHERE name = 'meter'").get()) continue;
      const row = db.prepare('SELECT value FROM meter WHERE id = 1').get();
      if (!row) continue;
      const {counts, exhausted} = JSON.parse(row.value);
      return {...counts, exhausted: exhausted ?? null};
    } finally {
      db.close();
    }
  }
  return null;
}

export async function runLifetimeSoak({result, storage, run, verifySession, launcher, observe, cadence, settle}) {
  const processMetrics = processSampler();
  const started = performance.now();
  const baseline = processMetrics().cpu_ms;
  const elapsed = () => Math.round(performance.now() - started) / 1000;
  const receipt = {
    node: process.version, target_seconds: seconds,
    launcher_sha256: createHash('sha256').update(readFileSync(launcher)).digest('hex'),
    process_memory_is_aggregate: true, samples: [], workflows: [], hello_sequences: [1],
  };
  const save = () => writeFileSync(result, JSON.stringify(receipt, null, 2) + '\n');
  const sample = async () => {
    const state = await observe();
    const {cpu_ms, ...memory} = processMetrics();
    receipt.samples.push({at_seconds: elapsed(), ...memory, cpu_ms: cpu_ms - baseline,
      meter: readMeter(storage), state});
    save();
    assert.equal(state, 'running');
  };
  // Keep a frequently reused seat, use the other seats for four-way bursts,
  // and let those seats lapse between bursts. Every submission is distinct.
  const bursts = new Set([0, 180, 360, 540, 720, 870]);
  const routeIds = [];
  await sample();
  const controller = cadence?.().then(value => ({value}), error => ({error}));
  for (let at = 0; at < seconds; at += 30) {
    await delay(Math.max(0, at * 1000 - (performance.now() - started)));
    if (at === 450) receipt.hello_sequences.push(await verifySession());
    const batch = bursts.has(at) ? [0, 1, 2, 3] : [0];
    for (const workflow of await Promise.all(batch.map(async slot => {
      const outcome = await run(slot, (at / 30 + slot) % 2 === 1);
      return {at_seconds: elapsed(), slot, ...outcome};
    }))) {
      const {route_id, ...sanitized} = workflow;
      routeIds.push(route_id);
      receipt.workflows.push(sanitized);
    }
    await sample();
  }
  await delay(Math.max(0, seconds * 1000 - (performance.now() - started)));
  const observation = await controller;
  if (observation?.error) throw observation.error;
  receipt.cadence = observation?.value;
  await sample();
  const drainStart = elapsed();
  const {routes, ...seal} = await settle();
  receipt.cleanup_seconds = elapsed() - drainStart;
  receipt.seal = seal;
  const identities = new Map();
  for (const [index, workflow] of receipt.workflows.entries()) {
    workflow.invocations = (routes?.[routeIds[index]] ?? []).map(({identity, kind, cold, wall_ms}) => {
      if (!identities.has(identity)) identities.set(identity, identities.size + 1);
      return {identity: identities.get(identity), kind, cold, wall_ms};
    });
  }
  receipt.route_invocations = Object.values(routes ?? {}).flat().reduce((totals, {kind, cold}) => {
    const key = kind + (cold ? '_cold' : '_warm');
    totals[key] = (totals[key] ?? 0) + 1;
    return totals;
  }, {});
  save();
  assert.ok(receipt.workflows.every(({success, sdk}) => success && sdk), 'At least one scheduled workflow failed');
  assert.equal(seal.exhausted, false, 'Launcher exhausted its release meter during the soak');
}
