import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { State } from './state.mjs';

export const fork = 'MTG-Thomas/bifrost-upstream';
export const upstream = 'gobifrost/bifrost';
const shaPattern = /^[0-9a-f]{40}$/;

export function githubApi(path, method = 'GET', body, run = execFileSync) {
  const args = ['api', '--hostname', 'github.com', path, '--method', method];
  // Compare responses contain entire commit/file diffs. Only transport the
  // ancestry metadata this controller consumes, rather than buffering patches.
  if (path.includes('/compare/')) args.push('--jq',
    '{status,ahead_by,behind_by,base_commit:{sha:.base_commit.sha},merge_base_commit:{sha:.merge_base_commit.sha}}');
  if (body) args.push('--input', '-');
  return JSON.parse(run('gh', args, {
    input: body ? JSON.stringify(body) : undefined,
    encoding: 'utf8', timeout: 30000, stdio: ['pipe', 'pipe', 'pipe'],
  }));
}

async function planIdentity(api) {
  const target = await api(`repos/${fork}`);
  const source = await api(`repos/${upstream}`);
  if (target.id !== 1441017913 || target.full_name !== fork || !target.fork ||
      target.parent?.id !== 1074603568 || target.parent?.full_name !== upstream ||
      target.default_branch !== 'main' || source.id !== 1074603568 ||
      source.full_name !== upstream || source.default_branch !== 'main') {
    throw new Error('Repository identity, parent, or default branch changed; stop for review.');
  }
}

export async function plan(api = githubApi) {
  await planIdentity(api);
  const before = (await api(`repos/${fork}/git/ref/heads/main`)).object.sha;
  const after = (await api(`repos/${upstream}/git/ref/heads/main`)).object.sha;
  if (!shaPattern.test(before) || !shaPattern.test(after)) throw new Error('Invalid commit identity.');
  if (before === after) return { state: 'noop', before, after };
  const comparison = await api(`repos/${fork}/compare/${before}...${after}`);
  if (comparison.status !== 'ahead' || comparison.behind_by !== 0 ||
      comparison.ahead_by < 1 || comparison.base_commit?.sha !== before ||
      comparison.merge_base_commit?.sha !== before) {
    throw new Error('Fork is ahead, divergent, or comparison is inconsistent; no mutation.');
  }
  return { state: 'update', before, after, commits: comparison.ahead_by };
}

// Candidate writer for an approved execution host. The command line below is
// intentionally read-only until the persistent identity and trigger gates clear.
export async function synchronize(api, apply = false, state) {
  if (!state) throw new Error('A stable durable state directory is required for every invocation.');
  return state.locked(async () => {
    if (state.outstanding()) throw new Error('Unresolved prior attempt; reconcile before planning or writing.');
    const candidate = await plan(api);
    if (!apply || candidate.state === 'noop') {
      state.log(candidate.state === 'noop' ? 'noop' : 'dry-run');
      return candidate;
    }
    const current = (await api(`repos/${fork}/git/ref/heads/main`)).object.sha;
    if (current !== candidate.before) throw new Error('Fork moved during preflight; stop and re-plan.');
    const intent = state.intent(candidate);
    let responseFailed = false;
    try {
      await api(`repos/${fork}/git/refs/heads/main`, 'PATCH', { sha: candidate.after, force: false });
    } catch { responseFailed = true; }
    let observed;
    try { observed = (await api(`repos/${fork}/git/ref/heads/main`)).object.sha; }
    catch {
      state.receipt(intent, 'uncertain');
      throw new Error('Readback failed; durable hold retained. Do not retry the write.');
    }
    if (observed !== candidate.after) {
      state.receipt(intent, 'uncertain');
      throw new Error('Readback mismatch or failed update; durable hold retained; do not retry blindly.');
    }
    state.receipt(intent, 'synced');
    return { ...candidate, state: 'synced', ...(responseFailed ? { reconciled: true } : {}) };
  });
}

// Explicit read-only remote reconciliation. An unchanged reference cannot prove
// that an in-flight request will not still complete, so it never clears a hold.
export async function reconcile(api, state) {
  return state.locked(async () => {
    const intent = state.outstanding();
    if (!intent) return { state: 'no-hold' };
    await planIdentity(api);
    const observed = (await api(`repos/${fork}/git/ref/heads/main`)).object.sha;
    if (observed !== intent.after) throw new Error('Prior outcome unresolved; hold retained. No write permitted.');
    state.receipt(intent, 'synced');
    return { state: 'reconciled', before: intent.before, after: intent.after };
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const args = process.argv.slice(2);
    if (args.length !== 3 || !['--dry-run', '--reconcile'].includes(args[0]) || args[1] !== '--state-dir') {
      throw new Error('Use --dry-run or --reconcile --state-dir <absolute path>. Remote writes remain disabled.');
    }
    const state = new State(args[2]);
    console.log(JSON.stringify(await (args[0] === '--reconcile'
      ? reconcile(githubApi, state) : synchronize(githubApi, false, state)), null, 2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
