import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {test} from 'node:test';
import {incarnationMeterBinding, launcherMeter, SQLITE_HEADROOM} from './launcher-meter.mjs';

test('incarnation limits scale the priced launcher rates by the reserved lifetime', () => {
  const rates = '{"sqlite_rows_written":128,"do_requests":64,"log_events":64}';
  assert.deepEqual(JSON.parse(incarnationMeterBinding(rates, 900)), {
    sqlite_rows_written: 115200, do_requests: 57600, log_events: 57600,
  });
  assert.deepEqual(JSON.parse(incarnationMeterBinding(rates, 120)), {
    sqlite_rows_written: 15360, do_requests: 7680, log_events: 7680,
  });
  assert.throws(() => incarnationMeterBinding(rates, 0), /Invalid launcher lifetime meter bounds/);
});

for (const [lifetimeSeconds, limit] of [[undefined, 128], [120, 128], [3600, 512]]) {
  test(`the sealed route receipt stays bounded across meter reloads at lifetime ${lifetimeSeconds}`, (t) => {
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
    const caps = JSON.stringify({sqlite_rows_written: SQLITE_HEADROOM + 2 * limit + 20, do_requests: 1000, log_events: 10});
    const options = {settle: true, lifetimeSeconds};
    const meter = launcherMeter(sql, caps, options);
    const invocation = {identity: 'fixture', kind: 'workflow', cold: true, started_at: '2026-11-20T23:59:00Z'};
    for (let route = 0; route < limit; route++) {
      meter.recordRoute(`prepare:${route}`, []);
      meter.recordRoute(`route-${route}`, [invocation]);
    }
    const reloaded = launcherMeter(sql, caps, options);
    reloaded.recordRoute(`route-${limit}`, [invocation]);
    reloaded.recordRoute(`prepare:${limit}`, []);
    const sealed = reloaded.seal(6);
    const {routes} = sealed;
    assert.equal(sealed.sqlite_rows_written, 2 * limit + 8);
    assert.deepEqual(launcherMeter(sql, caps, options).seal(6), sealed);
    assert.equal(Object.keys(routes).length, 2 * limit);
    assert.deepEqual(routes['prepare:0'], []);
    assert.equal(routes[`prepare:${limit}`], undefined);
    assert.deepEqual(routes['route-0'], [invocation]);
    assert.equal(routes[`route-${limit}`], undefined);
  });
}

test('stop confirmation uses reserved DO requests within the hard cap', (t) => {
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
  const meter = launcherMeter(sql, JSON.stringify({
    sqlite_rows_written: SQLITE_HEADROOM + 300, do_requests: 10, log_events: 10,
  }));
  meter.setSlots(1);
  assert.equal(meter.charge({do_requests: 9}), true);
  assert.equal(meter.charge({do_requests: 1}, true, true), true);
  assert.equal(meter.charge({do_requests: 1}, true, true), false);
  assert.equal(meter.snapshot().do_requests, 10);
});
