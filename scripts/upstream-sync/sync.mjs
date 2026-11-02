import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

export const fork = 'MTG-Thomas/bifrost-upstream';
export const upstream = 'gobifrost/bifrost';
const shaPattern = /^[0-9a-f]{40}$/;

export function githubApi(path, method = 'GET', body) {
  const args = ['api', '--hostname', 'github.com', path, '--method', method];
  if (body) args.push('--input', '-');
  return JSON.parse(execFileSync('gh', args, {
    input: body ? JSON.stringify(body) : undefined,
    encoding: 'utf8', timeout: 30000, stdio: ['pipe', 'pipe', 'pipe'],
  }));
}

export async function plan(api = githubApi) {
  const target = await api(`repos/${fork}`);
  const source = await api(`repos/${upstream}`);
  if (target.id !== 1441017913 || target.full_name !== fork || !target.fork ||
      target.parent?.id !== 1074603568 || target.parent?.full_name !== upstream ||
      target.default_branch !== 'main' || source.id !== 1074603568 ||
      source.full_name !== upstream || source.default_branch !== 'main') {
    throw new Error('Repository identity, parent, or default branch changed; stop for review.');
  }
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
export async function synchronize(api, apply = false) {
  const candidate = await plan(api);
  if (!apply || candidate.state === 'noop') return candidate;
  const current = (await api(`repos/${fork}/git/ref/heads/main`)).object.sha;
  if (current !== candidate.before) throw new Error('Fork moved during preflight; stop and re-plan.');
  try {
    await api(`repos/${fork}/git/refs/heads/main`, 'PATCH', { sha: candidate.after, force: false });
  } catch {
    // A lost response is not a failed write. Reconcile once, never retry writes.
    const observed = (await api(`repos/${fork}/git/ref/heads/main`)).object.sha;
    if (observed === candidate.after) return { ...candidate, state: 'synced', reconciled: true };
    throw new Error(`Update failed or outcome uncertain; observed main=${observed}; do not retry blindly.`);
  }
  const observed = (await api(`repos/${fork}/git/ref/heads/main`)).object.sha;
  if (observed !== candidate.after) throw new Error(`Readback mismatch: main=${observed}; stop for reconciliation.`);
  return { ...candidate, state: 'synced' };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    if (process.argv.slice(2).some(arg => arg !== '--dry-run')) {
      throw new Error('Only --dry-run is enabled. Persistent writer and Actions trigger review are pending.');
    }
    console.log(JSON.stringify(await synchronize(githubApi), null, 2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
