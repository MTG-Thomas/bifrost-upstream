import { execFileSync } from 'node:child_process';
import { readFileSync, realpathSync, statSync } from 'node:fs';
import { isAbsolute, resolve, relative, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { State } from './state.mjs';
import { plan, fork, upstream } from './sync.mjs';
import { verifyTriggerGate } from './trigger-gate.mjs';

const controllerBranch = 'automation/upstream-sync-f2ab3925';
const sha = /^[a-f0-9]{40}$/;
const scheduler = { hourlyMinute: 17, logon: 'InteractiveToken', runLevel: 'LeastPrivilege',
  multipleInstances: 'IgnoreNew', executionTimeLimitSeconds: 300, restartCount: 0,
  startWhenAvailable: false, wakeToRun: false };

function samePath(a, b) {
  const normalize = path => process.platform === 'win32' ? resolve(path).toLowerCase() : resolve(path);
  return normalize(a) === normalize(b);
}

export function verifyLauncherConfiguration(config, runtime) {
  if (config?.version !== 1 || config.mode !== 'offline-check' || config.authority?.status !== 'approved' ||
      !config.authority.approvalReference || config.authority.noProductionConsumersConfirmed !== true) {
    throw new Error('Launcher approval pending or mode is not offline-check. No credentials may be used.');
  }
  if (typeof runtime?.host !== 'string' || typeof runtime.principal !== 'string' || !runtime.env ||
      config.host !== 'LT002-H8WL8H4' || config.windowsPrincipal !== 'azuread\\thomasbray' ||
      config.githubActor !== 'MTG-Thomas' || config.githubActorId !== 87775189 ||
      runtime.host.toLowerCase() !== config.host.toLowerCase() ||
      runtime.principal.toLowerCase() !== config.windowsPrincipal || !runtime.interactiveSession) {
    throw new Error('Execution host/principal/session is not the proposed approved identity.');
  }
  for (const name of Object.keys(runtime.env)) {
    if (/^(GH_|GITHUB_|GIT_|NODE_OPTIONS$|NODE_PATH$)/i.test(name)) {
      throw new Error('Credential/runtime environment override refused.');
    }
  }
  for (const key of ['worktree', 'stateDirectory', 'nodePath', 'gitPath', 'ghPath']) {
    if (typeof config[key] !== 'string' || !isAbsolute(config[key]) ||
        /^[\\/]{2}/.test(config[key]) || /[\r\n\0]/.test(config[key])) {
      throw new Error('Only fixed absolute local paths are permitted.');
    }
  }
  if (!samePath(config.nodePath, runtime.nodePath) ||
      !samePath(config.nodePath, 'C:/Program Files/nodejs/node.exe') ||
      !samePath(config.gitPath, 'C:/Program Files/Git/cmd/git.exe') ||
      !samePath(config.ghPath, 'C:/Program Files/GitHub CLI/gh.exe') || !sha.test(config.sourceCommit) ||
      config.controllerBranch !== controllerBranch || config.maxCommandSeconds !== 10 ||
      config.maxRunSeconds !== 240) throw new Error('Pinned source/runtime or timeout bounds changed.');
  if (Object.keys(scheduler).length !== Object.keys(config.scheduler ?? {}).length ||
      Object.entries(scheduler).some(([key, value]) => config.scheduler[key] !== value)) {
    throw new Error('Scheduler must have no retries, backlog catch-up, or parallel instances.');
  }
  const stateRelative = relative(config.worktree, config.stateDirectory);
  if (!stateRelative || (!stateRelative.startsWith(`..${sep}`) && stateRelative !== '..' && !isAbsolute(stateRelative))) {
    throw new Error('Durable state must remain outside the pinned source worktree.');
  }
}

export function verifyWorktree(config, run = execFileSync) {
  if (!samePath(realpathSync(config.worktree), config.worktree)) throw new Error('Worktree aliases/symlinks refused.');
  const git = args => run(config.gitPath,
    ['--no-optional-locks', '-c', 'core.fsmonitor=false', '-C', config.worktree, ...args], {
      encoding: 'utf8', timeout: config.maxCommandSeconds * 1000,
      maxBuffer: 65536, shell: false, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
    }).trim();
  if (!samePath(git(['rev-parse', '--show-toplevel']), config.worktree) ||
      git(['rev-parse', 'HEAD']) !== config.sourceCommit ||
      git(['branch', '--show-current']) !== controllerBranch ||
      git(['status', '--porcelain=v1', '--untracked-files=all']) !== '') {
    throw new Error('Controller worktree is dirty, moved, or not pinned to the reviewed commit.');
  }
  if (git(['remote', 'get-url', 'personal']) !== `git@github.com:${fork}.git` ||
      git(['remote', 'get-url', 'origin']) !== `git@github.com:${upstream}.git`) {
    throw new Error('Controller repository remotes do not match exact fork/upstream identities.');
  }
  const scriptRelative = relative(config.worktree, fileURLToPath(import.meta.url));
  if (scriptRelative !== ['scripts', 'upstream-sync', 'launcher.mjs'].join(sep)) {
    throw new Error('Launcher must execute from the approved pinned worktree.');
  }
}

// No network transport is imported or constructed. Evidence is data, never code.
// Deliberately allow only the GETs used by plan(); no writer endpoint exists.
export function snapshotApi(snapshot) {
  return async (path, method = 'GET') => {
    if (method !== 'GET') throw new Error('Offline launcher prohibits remote writes.');
    if (path === `repos/${fork}`) return snapshot.fork;
    if (path === `repos/${upstream}`) return snapshot.upstream;
    if (path === `repos/${fork}/git/ref/heads/main`) return { object: { sha: snapshot.before } };
    if (path === `repos/${upstream}/git/ref/heads/main`) return { object: { sha: snapshot.targetCommit } };
    if (path === `repos/${fork}/compare/${snapshot.before}...${snapshot.targetCommit}`) return snapshot.comparison;
    throw new Error('Snapshot endpoint is not in the fixed read-only plan.');
  };
}

export async function checkOfflineLaunch(config, snapshot, runtime, run = execFileSync) {
  if (snapshot.fixtureOnly !== true) throw new Error('Only explicitly marked offline fixtures are accepted.');
  verifyLauncherConfiguration(config, runtime);
  const state = new State(config.stateDirectory);
  if (!samePath(realpathSync(config.stateDirectory), config.stateDirectory)) throw new Error('State aliases/symlinks refused.');
  return state.locked(async () => {
    if (state.outstanding()) throw new Error('Unresolved prior attempt; launcher held before planning.');
    const started = Date.now();
    verifyWorktree(config, run);
    const candidate = await plan(snapshotApi(snapshot));
    const gate = verifyTriggerGate(candidate, snapshot, config.triggerPolicy);
    if (Date.now() - started > config.maxRunSeconds * 1000) throw new Error('Launcher deadline exceeded.');
    state.log('offline-plan-matches');
    return { mode: 'offline-check', state: 'candidate-only', candidate, gate, source: config.sourceCommit,
      liveIdentityVerified: false, liveTriggerSafetyVerified: false, schedulerRegistered: false, writePerformed: false };
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const args = process.argv.slice(2);
    if (args.length !== 3 || args[0] !== '--check') throw new Error('Only --check <config.json> <snapshot.json> is supported. No live or apply mode.');
    const readJson = path => {
      if (statSync(path).size > 4194304) throw new Error('Offline input exceeds four MiB.');
      return JSON.parse(readFileSync(path, 'utf8'));
    };
    const config = readJson(args[1]), snapshot = readJson(args[2]);
    // This validates a declared fixture identity, not a live session/keyring.
    // Environment and binary identity are still the actual current process.
    const runtime = { ...snapshot.runtimeFixture, nodePath: process.execPath, env: process.env };
    console.log(JSON.stringify(await checkOfflineLaunch(config, snapshot, runtime), null, 2));
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}

export const schedulerTemplate = Object.freeze(scheduler);
