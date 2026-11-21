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
  const caps = JSON.stringify({sqlite_rows_written: SQLITE_HEADROOM + 200, do_requests: 1000, log_events: 10});
  const meter = launcherMeter(sql, caps, {settle: true});
  const invocation = {identity: 'fixture', kind: 'workflow', cold: true, started_at: '2026-11-20T23:59:00Z'};
  for (let route = 0; route < 128; route++) meter.recordRoute(`route-${route}`, [invocation]);
  const reloaded = launcherMeter(sql, caps, {settle: true});
  reloaded.recordRoute('route-128', [invocation]);
  const sealed = reloaded.seal(6);
  const {routes} = sealed;
  assert.equal(sealed.sqlite_rows_written, 136); // 128 records, seal, 6 ledger deletes, meter delete
  assert.deepEqual(launcherMeter(sql, caps, {settle: true}).seal(6), sealed);
  assert.equal(Object.keys(routes).length, 128);
  assert.deepEqual(routes['route-0'], [invocation]);
  assert.equal(routes['route-128'], undefined);
});
