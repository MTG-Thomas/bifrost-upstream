import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync, unlinkSync, appendFileSync, readdirSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { State } from './state.mjs';
import { fork, upstream, synchronize, reconcile, githubApi } from './sync.mjs';

const a = 'a'.repeat(40), b = 'b'.repeat(40), c = 'c'.repeat(40);
function fixture({ before = a, after = b, status = 'ahead', moved, fail, accepted = false, wrongReadback = false,
  failReadback = false, onWrite } = {}) {
  let current = before, reads = 0;
  const writes = [];
  const target = { id: 1441017913, full_name: fork, fork: true, default_branch: 'main',
    parent: { id: 1074603568, full_name: upstream } };
  const api = async (path, method = 'GET', body) => {
    if (method === 'PATCH') {
      onWrite?.();
      writes.push({ path, body });
      if (!fail || accepted) current = after;
      if (wrongReadback) current = c;
      if (fail) throw new Error('Lost response or denied');
      return { object: { sha: current } };
    }
    if (path === `repos/${fork}`) return target;
    if (path === `repos/${upstream}`) return { id: 1074603568, full_name: upstream, default_branch: 'main' };
    if (path === `repos/${fork}/git/ref/heads/main`) {
      reads++;
      if (reads >= 3 && failReadback) throw new Error('Read transport unavailable');
      if (reads === 2 && moved) current = moved;
      return { object: { sha: current } };
    }
    if (path === `repos/${upstream}/git/ref/heads/main`) return { object: { sha: after } };
    if (path === `repos/${fork}/compare/${before}...${after}`) return {
      status, ahead_by: 9, behind_by: status === 'ahead' ? 0 : 1,
      base_commit: { sha: before }, merge_base_commit: { sha: before },
    };
    throw new Error(`Unexpected endpoint: ${path}`);
  };
  return { api, writes, target };
}

function durable(t) {
  const directory = mkdtempSync(join(tmpdir(), 'bifrost-sync-test-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return new State(directory);
}

test('dry run plans update without writing', async (t) => {
  const f = fixture();
  assert.equal((await synchronize(f.api, false, durable(t))).state, 'update');
  assert.deepEqual(f.writes, []);
});
test('identical commits are a no-op even with apply', async (t) => {
  const f = fixture({ after: a });
  assert.equal((await synchronize(f.api, true, durable(t))).state, 'noop');
  assert.deepEqual(f.writes, []);
});
test('update only main with server-enforced fast-forward and verify readback', async (t) => {
  const f = fixture();
  const state = durable(t);
  assert.equal((await synchronize(f.api, true, state)).state, 'synced');
  assert.deepEqual(f.writes, [{ path: `repos/${fork}/git/refs/heads/main`, body: { sha: b, force: false } }]);
  assert.equal((await synchronize(f.api, true, new State(state.directory))).state, 'noop');
  assert.equal(f.writes.length, 1);
});
for (const status of ['diverged', 'behind']) test(`${status} stops without mutation`, async (t) => {
  const f = fixture({ status });
  await assert.rejects(synchronize(f.api, true, durable(t)), /divergent/);
  assert.deepEqual(f.writes, []);
});
test('identity mismatch stops before branch operations', async (t) => {
  const f = fixture(); f.target.parent.id = 123;
  await assert.rejects(synchronize(f.api, true, durable(t)), /identity/);
  assert.deepEqual(f.writes, []);
});
test('concurrent fork edit stops without mutation', async (t) => {
  const f = fixture({ moved: c });
  await assert.rejects(synchronize(f.api, true, durable(t)), /moved/);
  assert.deepEqual(f.writes, []);
});
test('lost successful response reconciles without resending', async (t) => {
  const f = fixture({ fail: true, accepted: true });
  assert.equal((await synchronize(f.api, true, durable(t))).reconciled, true);
  assert.equal(f.writes.length, 1);
});
test('denial or unresolved response stops after a single write', async (t) => {
  const f = fixture({ fail: true });
  await assert.rejects(synchronize(f.api, true, durable(t)), /do not retry/);
  assert.equal(f.writes.length, 1);
});
test('unexpected readback is not reported as success', async (t) => {
  const f = fixture({ wrongReadback: true });
  await assert.rejects(synchronize(f.api, true, durable(t)), /Readback mismatch/);
});

test('intent is persisted and readable before the writer is called', async (t) => {
  const state = durable(t);
  const f = fixture({ onWrite: () => {
    assert.equal(new State(state.directory).outstanding().after, b);
    assert.equal(JSON.parse(readFileSync(state.journalPath, 'utf8').trim()).status, 'intent');
  } });
  await synchronize(f.api, true, state);
  assert.equal(state.outstanding(), null);
  assert.deepEqual(readFileSync(state.journalPath, 'utf8').trim().split('\n').map(line => JSON.parse(line).status), ['intent', 'synced']);
});

test('timeout plus failed readback holds next scheduled invocation before any API call', async (t) => {
  const state = durable(t);
  const f = fixture({ fail: true, accepted: true, failReadback: true });
  await assert.rejects(synchronize(f.api, true, state), /Readback failed/);
  assert.equal(state.outstanding().after, b);
  assert.equal(JSON.parse(readFileSync(state.journalPath, 'utf8').trim().split('\n').at(-1)).status, 'uncertain');
  let calls = 0;
  const api = () => { calls++; throw new Error('Must not query or write'); };
  await assert.rejects(synchronize(api, true, new State(state.directory)), /Unresolved prior/);
  await assert.rejects(synchronize(api, false, new State(state.directory)), /Unresolved prior/);
  assert.equal(calls, 0);
  assert.equal(f.writes.length, 1);
  const scheduled = spawnSync(process.execPath, ['--input-type=module', '-e', `
    import { State } from ${JSON.stringify(new URL('./state.mjs', import.meta.url).href)};
    import { synchronize } from ${JSON.stringify(new URL('./sync.mjs', import.meta.url).href)};
    try { await synchronize(() => process.exit(99), true, new State(${JSON.stringify(state.directory)})); }
    catch (error) { if (error.message.includes('Unresolved prior')) process.exit(74); throw error; }
  `], { encoding: 'utf8', timeout: 10000 });
  assert.equal(scheduled.status, 74, scheduled.stderr);
});

test('explicit reconciliation clears only an exactly observed intended commit; never writes', async (t) => {
  const state = durable(t);
  await state.locked(async () => state.intent({ before: a, after: b }));
  const unchanged = fixture();
  await assert.rejects(reconcile(unchanged.api, state), /hold retained/);
  assert.equal(state.outstanding().after, b);
  const accepted = fixture({ before: b });
  assert.equal((await reconcile(accepted.api, new State(state.directory))).state, 'reconciled');
  assert.equal(state.outstanding(), null);
  assert.deepEqual(accepted.writes, []);
  assert.deepEqual(unchanged.writes, []);
  assert.equal((await synchronize(accepted.api, true, new State(state.directory))).state, 'noop');
});

test('failed reconciliation read leaves the prior intent intact', async (t) => {
  const state = durable(t);
  await state.locked(async () => state.intent({ before: a, after: b }));
  const f = fixture();
  const unavailable = (path, ...args) => path.includes('/git/ref/')
    ? Promise.reject(new Error('Read unavailable')) : f.api(path, ...args);
  await assert.rejects(reconcile(unavailable, state), /Read unavailable/);
  assert.equal(new State(state.directory).outstanding().after, b);
});

test('actual child crash leaves lock and intent; clearing abandoned lock does not clear hold', async (t) => {
  const state = durable(t);
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', `
    import { State } from ${JSON.stringify(new URL('./state.mjs', import.meta.url).href)};
    const state = new State(${JSON.stringify(state.directory)});
    await state.locked(async () => {
      state.intent({ before: '${a}', after: '${b}' });
      process.exit(71);
    });
  `], { encoding: 'utf8', timeout: 10000 });
  assert.equal(child.status, 71, child.stderr);
  let calls = 0;
  await assert.rejects(synchronize(() => { calls++; }, true, new State(state.directory)), /Process lock exists/);
  assert.equal(calls, 0);
  const owner = JSON.parse(readFileSync(state.lockPath, 'utf8'));
  assert.equal(owner.pid, child.pid);
  assert.throws(() => process.kill(owner.pid, 0)); // child has exited; operator recovery rehearsal
  unlinkSync(state.lockPath);
  await assert.rejects(synchronize(() => { calls++; }, true, new State(state.directory)), /Unresolved prior/);
  assert.equal(calls, 0);
  assert.equal(state.outstanding().after, b);
  await assert.rejects(reconcile(fixture().api, state), /hold retained/);
  assert.equal((await reconcile(fixture({ before: b }).api, state)).state, 'reconciled');
});

test('crash after simulated server acceptance still holds across processes until read-only reconciliation', async (t) => {
  const state = durable(t);
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', `
    import { State } from ${JSON.stringify(new URL('./state.mjs', import.meta.url).href)};
    import { synchronize } from ${JSON.stringify(new URL('./sync.mjs', import.meta.url).href)};
    import { writeFileSync } from 'node:fs';
    const state = new State(${JSON.stringify(state.directory)});
    const api = async (path, method = 'GET') => {
      if (method === 'PATCH') {
        if (!state.outstanding()) throw new Error('Missing durable intent');
        writeFileSync(state.directory + '/simulated-server-head', '${b}');
        process.exit(72);
      }
      if (path.endsWith('/compare/${a}...${b}')) return {
        status: 'ahead', ahead_by: 1, behind_by: 0,
        base_commit: { sha: '${a}' }, merge_base_commit: { sha: '${a}' }
      };
      if (path.includes('/git/ref/')) return { object: { sha: path.includes('MTG-Thomas') ? '${a}' : '${b}' } };
      if (path.endsWith('MTG-Thomas/bifrost-upstream')) return {
        id: 1441017913, full_name: 'MTG-Thomas/bifrost-upstream', fork: true, default_branch: 'main',
        parent: { id: 1074603568, full_name: 'gobifrost/bifrost' }
      };
      return { id: 1074603568, full_name: 'gobifrost/bifrost', default_branch: 'main' };
    };
    await synchronize(api, true, state);
  `], { encoding: 'utf8', timeout: 10000 });
  assert.equal(child.status, 72, child.stderr);
  assert.equal(readFileSync(join(state.directory, 'simulated-server-head'), 'utf8'), b);
  assert.throws(() => process.kill(child.pid, 0));
  unlinkSync(state.lockPath);
  await assert.rejects(synchronize(() => { throw new Error('No new plan'); }, true, new State(state.directory)), /Unresolved prior/);
  const f = fixture({ before: b });
  assert.equal((await reconcile(f.api, state)).state, 'reconciled');
  assert.deepEqual(f.writes, []);
});

test('separate process cannot acquire the held process lock', async (t) => {
  const state = durable(t);
  await state.locked(async () => {
    const child = spawnSync(process.execPath, ['--input-type=module', '-e', `
      import { State } from ${JSON.stringify(new URL('./state.mjs', import.meta.url).href)};
      try { await new State(${JSON.stringify(state.directory)}).locked(async () => process.exit(99)); }
      catch (error) { if (error.message.includes('Process lock exists')) process.exit(73); throw error; }
    `], { encoding: 'utf8', timeout: 10000 });
    assert.equal(child.status, 73, child.stderr);
  });
});

test('torn journal blocks rather than discarding a potentially pending intent', async (t) => {
  const state = durable(t);
  await state.locked(async () => state.intent({ before: a, after: b }));
  appendFileSync(state.journalPath, '{"status":');
  let calls = 0;
  await assert.rejects(synchronize(() => { calls++; }, true, state), /Torn journal/);
  assert.equal(calls, 0);
});

test('operational logs are bounded separately from durable receipts', async (t) => {
  const state = durable(t);
  for (let i = 0; i < 34; i++) await state.locked(async () => state.log('noop'));
  const directory = join(state.directory, 'logs');
  const logs = readdirSync(directory);
  assert.equal(logs.length, 30);
  assert.ok(logs.every(name => statSync(join(directory, name)).size <= 16384));
  await state.locked(async () => {
    state.intent({ before: a, after: b });
    for (let i = 0; i < 1000; i++) {
      if (statSync(state.logPath).size > 16200) break;
      state.log('bounded');
    }
    assert.throws(() => { for (let i = 0; i < 1000; i++) state.log('bounded'); }, /size limit/);
  });
  assert.equal(state.outstanding().after, b);
});

test('CLI rejects --apply without reaching any remote writer', () => {
  const child = spawnSync(process.execPath, [fileURLToPath(new URL('./sync.mjs', import.meta.url)), '--apply'],
    { encoding: 'utf8', timeout: 10000 });
  assert.equal(child.status, 1);
  assert.match(child.stderr, /Remote writes remain disabled/);
});

test('failure to persist intent prevents the PATCH', async (t) => {
  const state = durable(t);
  state.intent = () => { throw new Error('Simulated disk/fsync failure'); };
  const f = fixture();
  await assert.rejects(synchronize(f.api, true, state), /disk\/fsync/);
  assert.deepEqual(f.writes, []);
});

test('failure to persist terminal receipt retains intent and blocks the next invocation', async (t) => {
  const state = durable(t);
  const receipt = state.receipt.bind(state);
  state.receipt = (intent, status) => {
    if (status === 'synced') throw new Error('Simulated terminal receipt disk failure');
    return receipt(intent, status);
  };
  const f = fixture();
  await assert.rejects(synchronize(f.api, true, state), /terminal receipt disk/);
  await assert.rejects(synchronize(() => { throw new Error('No new network'); }, true, new State(state.directory)), /Unresolved prior/);
  assert.equal(f.writes.length, 1);
  assert.equal((await reconcile(f.api, new State(state.directory))).state, 'reconciled');
});

test('compare transport requests ancestry metadata without buffering full patches', () => {
  const result = githubApi(`repos/${fork}/compare/${a}...${b}`, 'GET', undefined, (command, args) => {
    assert.equal(command, 'gh');
    const projection = args[args.indexOf('--jq') + 1];
    assert.ok(args.includes('--jq'));
    assert.match(projection, /merge_base_commit/);
    assert.doesNotMatch(projection, /files|patch|commits/);
    return JSON.stringify({ status: 'ahead', ahead_by: 9, behind_by: 0,
      base_commit: { sha: a }, merge_base_commit: { sha: a } });
  });
  assert.equal(result.ahead_by, 9);
});
