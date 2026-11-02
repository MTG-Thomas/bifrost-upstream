import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { State } from './state.mjs';
import { definitionBlobs, verifyTriggerGate } from './trigger-gate.mjs';
import { checkOfflineLaunch, schedulerTemplate, snapshotApi } from './launcher.mjs';

const a = 'a'.repeat(40), b = 'b'.repeat(40), c = 'c'.repeat(40), d = 'd'.repeat(40);
const root = resolve(fileURLToPath(new URL('../../', import.meta.url)));

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'bifrost-launch-fixture-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const snapshot = {
    fixtureOnly: true, complete: true, capturedAt: new Date().toISOString(), before: a,
    targetCommit: b, targetTree: d, targetCommitMetadata: { sha: b, tree: { sha: d } },
    fork: { id: 1441017913, full_name: 'MTG-Thomas/bifrost-upstream', fork: true,
      parent: { id: 1074603568, full_name: 'gobifrost/bifrost' }, default_branch: 'main' },
    upstream: { id: 1074603568, full_name: 'gobifrost/bifrost', default_branch: 'main' },
    comparison: { status: 'ahead', ahead_by: 11, behind_by: 0,
      base_commit: { sha: a }, merge_base_commit: { sha: a } },
    tree: { sha: d, truncated: false, tree: [
      { path: '.github/workflows/ci.yml', type: 'blob', mode: '100644', sha: a },
      { path: '.github/actions/prepare/action.yml', type: 'blob', mode: '100644', sha: b },
    ] },
    actions: { enabled: true, allowed_actions: 'all', sha_pinning_required: false },
    workflowPermissions: { default_workflow_permissions: 'read', can_approve_pull_request_reviews: false },
    registeredWorkflows: { complete: true, total_count: 1,
      workflows: [{ id: 1, path: '.github/workflows/ci.yml', state: 'disabled_manually' }] },
    rulesets: [], protection: { httpStatus: 404, reason: 'Branch not protected' },
  };
  const policy = { status: 'approved', approvalReference: 'FIXTURE-ONLY-NOT-OWNER-AUTHORITY',
    safety: { decision: 'qualified', evidenceReference: 'SYNTHETIC-TEST-EVIDENCE', noProductionConsumersConfirmed: true },
    definitionBlobs: definitionBlobs(snapshot), actions: structuredClone(snapshot.actions),
    workflowPermissions: structuredClone(snapshot.workflowPermissions),
    registeredWorkflows: structuredClone(snapshot.registeredWorkflows.workflows),
    rulesets: [], protection: structuredClone(snapshot.protection) };
  const config = { version: 1, mode: 'offline-check',
    authority: { status: 'approved', approvalReference: 'FIXTURE-ONLY', noProductionConsumersConfirmed: true },
    host: 'LT002-H8WL8H4', windowsPrincipal: 'azuread\\thomasbray', githubActor: 'MTG-Thomas', githubActorId: 87775189,
    worktree: root, sourceCommit: c, controllerBranch: 'automation/upstream-sync-f2ab3925',
    stateDirectory: join(directory, 'state'), nodePath: 'C:/Program Files/nodejs/node.exe',
    gitPath: 'C:/Program Files/Git/cmd/git.exe', ghPath: 'C:/Program Files/GitHub CLI/gh.exe',
    maxCommandSeconds: 10, maxRunSeconds: 240, scheduler: { ...schedulerTemplate }, triggerPolicy: policy };
  const runtime = { host: config.host, principal: config.windowsPrincipal, interactiveSession: true,
    nodePath: config.nodePath, env: {} };
  let calls = 0;
  const run = (binary, args, options) => {
    calls++;
    assert.equal(binary, config.gitPath);
    assert.ok(Array.isArray(args));
    assert.equal(args[4], config.worktree);
    assert.equal(options.shell, false);
    assert.equal(options.windowsHide, true);
    assert.equal(options.timeout, 10000);
    assert.equal(options.maxBuffer, 65536);
    switch (args.slice(5).join(' ')) {
      case 'rev-parse --show-toplevel': return root;
      case 'rev-parse HEAD': return c;
      case 'branch --show-current': return config.controllerBranch;
      case 'status --porcelain=v1 --untracked-files=all': return '';
      case 'remote get-url personal': return 'git@github.com:MTG-Thomas/bifrost-upstream.git';
      case 'remote get-url origin': return 'git@github.com:gobifrost/bifrost.git';
      default: throw new Error('Unexpected Git arguments');
    }
  };
  return { snapshot, config, runtime, run, calls: () => calls };
}

test('offline launcher validates pinned fixture plan under the shared lock without credentials or writes', async t => {
  const f = fixture(t);
  const result = await checkOfflineLaunch(f.config, f.snapshot, f.runtime, f.run);
  assert.equal(result.candidate.after, b);
  assert.equal(result.writePerformed, false);
  assert.equal(result.schedulerRegistered, false);
  assert.equal(result.liveIdentityVerified, false);
  assert.equal(result.liveTriggerSafetyVerified, false);
  assert.equal(f.calls(), 6);
  assert.equal(new State(f.config.stateDirectory).outstanding(), null);
});

for (const [name, mutate, pattern] of [
  ['pending approval', f => { f.config.authority.status = 'pending'; }, /approval pending/],
  ['wrong host', f => { f.runtime.host = 'OTHER-HOST'; }, /host\/principal/],
  ['wrong principal', f => { f.runtime.principal = 'SYSTEM'; }, /host\/principal/],
  ['no interactive session', f => { f.runtime.interactiveSession = false; }, /host\/principal/],
  ['token override', f => { f.runtime.env.GH_TOKEN = 'FIXTURE-SECRET'; }, /override refused/],
  ['Git environment override', f => { f.runtime.env.GIT_DIR = '/other'; }, /override refused/],
  ['Node injection override', f => { f.runtime.env.NODE_OPTIONS = '--require arbitrary'; }, /override refused/],
  ['unreviewed source pin', f => { f.config.sourceCommit = b; }, /not pinned/],
  ['invalid SHA injection', f => { f.config.sourceCommit = `${c};echo INJECTED`; }, /Pinned source/],
  ['binary substitution', f => { f.config.gitPath = 'C:/Windows/System32/cmd.exe'; }, /Pinned source/],
  ['parallel scheduling', f => { f.config.scheduler.multipleInstances = 'Parallel'; }, /Scheduler/],
  ['catch-up storm', f => { f.config.scheduler.startWhenAvailable = true; }, /Scheduler/],
  ['failure retry', f => { f.config.scheduler.restartCount = 10; }, /Scheduler/],
  ['unbounded timeout', f => { f.config.maxRunSeconds = 0; }, /timeout bounds/],
  ['state in worktree', f => { f.config.stateDirectory = join(root, 'state'); }, /outside/],
  ['newline path injection', f => { f.config.gitPath += '\nmalicious'; }, /fixed absolute/],
  ['live snapshot input', f => { f.snapshot.fixtureOnly = false; }, /offline fixtures/],
]) test(`launcher refuses ${name}`, async t => {
  const f = fixture(t); mutate(f);
  await assert.rejects(checkOfflineLaunch(f.config, f.snapshot, f.runtime, f.run), pattern);
});

test('journal hold blocks launcher before Git/planning on a later invocation', async t => {
  const f = fixture(t), state = new State(f.config.stateDirectory);
  await state.locked(async () => state.intent({ before: a, after: b }));
  await assert.rejects(checkOfflineLaunch(f.config, f.snapshot, f.runtime, f.run), /Unresolved prior/);
  assert.equal(f.calls(), 0);
});

test('shared process lock prevents overlapping launcher invocation', async t => {
  const f = fixture(t), state = new State(f.config.stateDirectory);
  await state.locked(async () => {
    await assert.rejects(checkOfflineLaunch(f.config, f.snapshot, f.runtime, f.run), /Process lock exists/);
  });
  assert.equal(f.calls(), 0);
});

test('dirty source and remotes changed to a different repository are refused', async t => {
  const f = fixture(t);
  await assert.rejects(checkOfflineLaunch(f.config, f.snapshot, f.runtime,
    (binary, args, options) => args.includes('--porcelain=v1') ? ' M scripts/upstream-sync/launcher.mjs' : f.run(binary, args, options)), /dirty/);
  await assert.rejects(checkOfflineLaunch(f.config, f.snapshot, f.runtime,
    (binary, args, options) => args.at(-1) === 'personal' ? 'git@github.com:Midtown-Technology-Group/bifrost.git' : f.run(binary, args, options)), /remotes/);
});

for (const [name, mutate, pattern] of [
  ['new workflow', s => { s.tree.tree.push({ path: '.github/workflows/new.yml', type: 'blob', mode: '100644', sha: c }); }, /definitions/],
  ['modified workflow', s => { s.tree.tree[0].sha = c; }, /definitions/],
  ['removed workflow', s => { s.tree.tree.shift(); }, /Missing workflow/],
  ['modified local action', s => { s.tree.tree[1].sha = c; }, /definitions/],
  ['symlink definition', s => { s.tree.tree[0].mode = '120000'; }, /Unreviewable/],
  ['truncated tree', s => { s.tree.truncated = true; }, /Complete commit-bound/],
  ['tree from another commit', s => { s.targetCommitMetadata.tree.sha = a; }, /commit-bound/],
  ['duplicate paths', s => { s.tree.tree.push({ ...s.tree.tree[0] }); }, /duplicate/],
  ['incomplete pagination', s => { s.registeredWorkflows.total_count = 2; }, /inventory/],
  ['workflow 404', s => { s.registeredWorkflows = { status: 404 }; }, /404 is not inactivity/],
  ['Actions settings', s => { s.actions.enabled = false; }, /Actions settings/],
  ['workflow grants', s => { s.workflowPermissions.default_workflow_permissions = 'write'; }, /Workflow permissions/],
  ['registered workflow activation', s => { s.registeredWorkflows.workflows[0].state = 'active'; }, /Registered workflows/],
  ['unknown workflow state', s => { s.registeredWorkflows.workflows[0].state = 'unknown'; }, /unknown workflow/],
  ['new rules', s => { s.rulesets.push({ id: 99 }); }, /Repository rules/],
  ['main protection', s => { s.protection = { httpStatus: 200 }; }, /Main protection/],
  ['wrong repository', s => { s.fork.id = 999; }, /identity/],
  ['wrong default branch', s => { s.upstream.default_branch = 'dev'; }, /identity/],
  ['wrong target', s => { s.targetCommit = a; }, /target\/repository/],
  ['stale snapshot', s => { s.capturedAt = new Date(Date.now() - 600000).toISOString(); }, /stale/],
  ['future snapshot', s => { s.capturedAt = new Date(Date.now() + 600000).toISOString(); }, /future/],
]) test(`trigger gate holds on ${name}`, t => {
  const f = fixture(t); mutate(f.snapshot);
  assert.throws(() => verifyTriggerGate({ after: b }, f.snapshot, f.config.triggerPolicy), pattern);
});

test('an unregistered definition must still be in the approved manifest', t => {
  const f = fixture(t);
  f.snapshot.tree.tree.push({ path: '.github/workflows/unregistered.yml', type: 'blob', mode: '100644', sha: c });
  assert.throws(() => verifyTriggerGate({ after: b }, f.snapshot, f.config.triggerPolicy), /definitions/);
});

test('matching a manifest alone never supplies safety approval or consumer confirmation', t => {
  const f = fixture(t);
  f.config.triggerPolicy.safety.noProductionConsumersConfirmed = false;
  assert.throws(() => verifyTriggerGate({ after: b }, f.snapshot, f.config.triggerPolicy), /pending/);
});

test('a protected branch cannot be admitted by editing the fixture policy', t => {
  const f = fixture(t);
  f.snapshot.rulesets = [{ id: 1 }]; f.config.triggerPolicy.rulesets = [{ id: 1 }];
  assert.throws(() => verifyTriggerGate({ after: b }, f.snapshot, f.config.triggerPolicy), /protection\/review adapter/);
});

test('fixture API exposes no write operation', async t => {
  const f = fixture(t);
  await assert.rejects(snapshotApi(f.snapshot)('repos/MTG-Thomas/bifrost-upstream/git/refs/heads/main', 'PATCH'), /prohibits remote writes/);
});

test('launcher CLI refuses apply before invoking commands or credentials', () => {
  const result = spawnSync(process.execPath, [fileURLToPath(new URL('./launcher.mjs', import.meta.url)), '--apply'],
    { encoding: 'utf8', timeout: 10000 });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /No live or apply mode/);
});

test('checked-in template remains pending with no owner or trigger approval', () => {
  const template = JSON.parse(readFileSync(new URL('./launcher.template.json', import.meta.url), 'utf8'));
  assert.equal(template.authority.status, 'pending');
  assert.equal(template.authority.noProductionConsumersConfirmed, false);
  assert.equal(template.triggerPolicy.status, 'pending');
  assert.equal(template.triggerPolicy.safety.decision, 'pending');
});

test('actual offline CLI refuses the pending template before inspecting worktree or credentials', t => {
  const directory = mkdtempSync(join(tmpdir(), 'bifrost-cli-fixture-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const snapshot = join(directory, 'snapshot.json');
  writeFileSync(snapshot, JSON.stringify({ fixtureOnly: true }));
  const result = spawnSync(process.execPath, [fileURLToPath(new URL('./launcher.mjs', import.meta.url)), '--check',
    fileURLToPath(new URL('./launcher.template.json', import.meta.url)), snapshot], { encoding: 'utf8', timeout: 10000 });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /Launcher approval pending/);
});

test('a Git subprocess timeout stops once without a retry', async t => {
  const f = fixture(t);
  let calls = 0;
  await assert.rejects(checkOfflineLaunch(f.config, f.snapshot, f.runtime, (_binary, _args, options) => {
    calls++; assert.equal(options.timeout, 10000);
    throw new Error('Simulated command timeout');
  }), /command timeout/);
  assert.equal(calls, 1);
  assert.equal(new State(f.config.stateDirectory).outstanding(), null);
});

test('a new upstream commit with the identical reviewed definition map may pass the offline trigger plan', t => {
  const f = fixture(t), next = 'e'.repeat(40);
  f.snapshot.targetCommit = next;
  f.snapshot.targetCommitMetadata.sha = next;
  assert.equal(verifyTriggerGate({ after: next }, f.snapshot, f.config.triggerPolicy).target, next);
});
