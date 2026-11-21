import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {test} from 'node:test';
import {launcherMeter, SQLITE_HEADROOM} from './launcher-meter.mjs';

test('the sealed route receipt stays bounded across meter reloads', (t) => {
  const db = new DatabaseSync(':memory:');
  t.after(() => db.close());
  const sql = {
    databaseSize: 4096,
    exec(query, ...args) {
      const statement = db.prepare(query);
      if (query.startsWith('SELECT')) return {toArray: () => statement.all(...args)};
      statement.run(...args);
    },
  };
  const caps = JSON.stringify({sqlite_rows_written: SQLITE_HEADROOM + 300, do_requests: 1000, log_events: 10});
  const meter = launcherMeter(sql, caps, {settle: true});
  const invocation = {identity: 'fixture', kind: 'workflow', cold: true, started_at: '2026-11-20T23:59:00Z'};
  for (let route = 0; route < 128; route++) {
    meter.recordRoute(`prepare:${route}`, []);
    meter.recordRoute(`route-${route}`, [invocation]);
  }
  const reloaded = launcherMeter(sql, caps, {settle: true});
  reloaded.recordRoute('route-128', [invocation]);
  reloaded.recordRoute('prepare:128', []);
  const {routes} = reloaded.seal();
  assert.equal(Object.keys(routes).length, 256);
  assert.deepEqual(routes['prepare:0'], []);
  assert.equal(routes['prepare:128'], undefined);
  assert.deepEqual(routes['route-0'], [invocation]);
  assert.equal(routes['route-128'], undefined);
});
