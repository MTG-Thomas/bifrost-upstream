import test from 'node:test';
import assert from 'node:assert/strict';
import { fork, upstream, synchronize } from './sync.mjs';

const a = 'a'.repeat(40), b = 'b'.repeat(40), c = 'c'.repeat(40);
function fixture({ before = a, after = b, status = 'ahead', moved, fail, accepted = false, wrongReadback = false } = {}) {
  let current = before, reads = 0;
  const writes = [];
  const target = { id: 1441017913, full_name: fork, fork: true, default_branch: 'main',
    parent: { id: 1074603568, full_name: upstream } };
  const api = async (path, method = 'GET', body) => {
    if (method === 'PATCH') {
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

test('dry run plans update without writing', async () => {
  const f = fixture();
  assert.equal((await synchronize(f.api)).state, 'update');
  assert.deepEqual(f.writes, []);
});
test('identical commits are a no-op even with apply', async () => {
  const f = fixture({ after: a });
  assert.equal((await synchronize(f.api, true)).state, 'noop');
  assert.deepEqual(f.writes, []);
});
test('update only main with server-enforced fast-forward and verify readback', async () => {
  const f = fixture();
  assert.equal((await synchronize(f.api, true)).state, 'synced');
  assert.deepEqual(f.writes, [{ path: `repos/${fork}/git/refs/heads/main`, body: { sha: b, force: false } }]);
  assert.equal((await synchronize(f.api, true)).state, 'noop');
  assert.equal(f.writes.length, 1);
});
for (const status of ['diverged', 'behind']) test(`${status} stops without mutation`, async () => {
  const f = fixture({ status });
  await assert.rejects(synchronize(f.api, true), /divergent/);
  assert.deepEqual(f.writes, []);
});
test('identity mismatch stops before branch operations', async () => {
  const f = fixture(); f.target.parent.id = 123;
  await assert.rejects(synchronize(f.api, true), /identity/);
  assert.deepEqual(f.writes, []);
});
test('concurrent fork edit stops without mutation', async () => {
  const f = fixture({ moved: c });
  await assert.rejects(synchronize(f.api, true), /moved/);
  assert.deepEqual(f.writes, []);
});
test('lost successful response reconciles without resending', async () => {
  const f = fixture({ fail: true, accepted: true });
  assert.equal((await synchronize(f.api, true)).reconciled, true);
  assert.equal(f.writes.length, 1);
});
test('denial or unresolved response stops after a single write', async () => {
  const f = fixture({ fail: true });
  await assert.rejects(synchronize(f.api, true), /do not retry/);
  assert.equal(f.writes.length, 1);
});
test('unexpected readback is not reported as success', async () => {
  const f = fixture({ wrongReadback: true });
  await assert.rejects(synchronize(f.api, true), /Readback mismatch/);
});
