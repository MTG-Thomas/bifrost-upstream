// Workspace eligibility census.
//
// Discovers workflows and runs the server's Python approval analyzer over
// local bytes. No workspace code is executed and no API is called.
import {readFile, readdir, stat, writeFile} from 'node:fs/promises';
import {join, relative, extname, dirname, delimiter} from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';

const run = promisify(execFile);
const analyzer = fileURLToPath(new URL('./analyze-file.py', import.meta.url));
const analyzerModule = fileURLToPath(new URL(
  '../../../../api/bifrost/', import.meta.url,
));

const args = process.argv.slice(2);
const flag = (name, fallback) => args.includes(`--${name}`) ? args[args.indexOf(`--${name}`) + 1] : fallback;
const allowance = Number(flag('allowance', '50'));
const outPath = flag('out', null);
const wsRoot = flag('workspace', null);
const repoRoot = flag('repo', null);

const SKIP_DIRS = new Set(['node_modules', '.git', '.venv', 'venv', 'python_modules', 'vendor',
  '.wrangler', '.cloudflare', 'dist', 'build', '__pycache__', 'site-packages', 'tests', 'test',
  '__tests__']);
const SKIP_PATH = /(^|\/)(tests?|__tests__|test-support)\//;

async function walk(dir, files = []) {
  let entries;
  try { entries = await readdir(dir, {withFileTypes: true}); } catch { return files; }
  for (const e of entries) {
    if (e.isDirectory()) {
      if (SKIP_DIRS.has(e.name)) continue;
      await walk(join(dir, e.name), files);
    } else if (e.isFile() && !/^(test_.*|.*_test)\.py$/.test(e.name)) {
      files.push(join(dir, e.name));
    }
  }
  return files;
}

async function exists(p) {
  try { await stat(p); return true; } catch { return false; }
}

async function censusFile(workspaceRoot, file, opts) {
  const path = relative(workspaceRoot, file).replaceAll('\\', '/');
  const python = process.env.PYTHON ?? (process.platform === 'win32' ? 'python' : 'python3');
  const args = [analyzer, workspaceRoot, path, '--allowance', String(opts.allowance),
    '--cpu', opts.cpuBudget];
  if (opts.functionName) args.push('--function', opts.functionName);
  if (opts.fallbackRoot) args.push('--fallback', opts.fallbackRoot);
  const {stdout} = await run(python, args, {
    env: {...process.env, PYTHONPATH: [process.env.BIFROST_ANALYZER_PATH ?? analyzerModule,
      process.env.PYTHONPATH].filter(Boolean).join(delimiter)},
  });
  const result = JSON.parse(stdout);
  if (result.error) {
    return {path, backend: 'linux', reasons: [result.error], packages: [],
      declared_modules: [], estimated_subrequests: null};
  }
  return {path, backend: 'cloudflare', reasons: [], ...result,
    declared_modules: Object.keys(result.module_sha256)};
}

function summarize(rows, label) {
  const eligible = rows.filter((r) => r.backend === 'cloudflare');
  const reasons = new Map();
  for (const r of rows) for (const reason of r.reasons) {
    const key = reason.split(':')[0];
    reasons.set(key, (reasons.get(key) ?? 0) + 1);
  }
  return {
    label,
    files: rows.length,
    eligible: eligible.length,
    ineligible: rows.length - eligible.length,
    eligible_pct: rows.length ? Math.round((1000 * eligible.length) / rows.length) / 10 : null,
    by_reason: Object.fromEntries([...reasons].sort((a, b) => b[1] - a[1])),
  };
}

function roleOf(p) {
  const n = p.replaceAll('\\', '/');
  if (n.startsWith('examples/')) return 'example workspace';
  if (n.startsWith('api/bifrost/commands/')) return 'cli';
  if (n.startsWith('api/bifrost/')) return 'sdk';
  if (n.startsWith('api/src/') || n.startsWith('api/shared/')) return 'platform';
  if (n.startsWith('spikes/')) return 'spike fixture';
  if (n.startsWith('docs/')) return 'docs';
  return 'other';
}

const report = {generated_at: new Date().toISOString(), allowance, workspaces: [], repo: null};

if (wsRoot) {
  const touched = (await walk(wsRoot)).filter((f) => f.replaceAll('\\', '/').endsWith('.bifrost/workflows.yaml'));
  for (const manifestPath of touched) {
    const root = dirname(dirname(manifestPath));
    const rel = relative(wsRoot, root).replaceAll('\\', '/');
    // `reference/` manifests point at community files that are not present in
    // this repo; they are reference material, not a deployable workspace.
    if (rel.startsWith('reference/')) {
      report.workspaces_skipped = report.workspaces_skipped ?? [];
      report.workspaces_skipped.push({root: rel, reason: 'reference manifest, files not present'});
      continue;
    }
    const yaml = await readFile(manifestPath, 'utf8');
    const entries = [];
    let cur = null;
    for (const line of yaml.split(/\r?\n/)) {
      const p = line.match(/^\s*path:\s*["']?([^"'\r\n]+?)["']?\s*$/);
      if (p) { if (cur?.path) entries.push(cur); cur = {path: p[1].trim()}; continue; }
      const f = line.match(/^\s*function_name:\s*["']?([^"'\r\n]+?)["']?\s*$/);
      if (f && cur) cur.function_name = f[1].trim();
    }
    if (cur?.path) entries.push(cur);
    const rows = [];
    for (const entry of entries) {
      let entryRoot = root;
      let file = join(root, entry.path);
      if (!(await exists(file)) && wsRoot && (await exists(join(wsRoot, entry.path)))) {
        entryRoot = wsRoot;
        file = join(wsRoot, entry.path);
      }
      if (!(await exists(file))) {
        rows.push({path: entry.path, workflow: entry.function_name ?? null, backend: 'linux',
          reasons: ['missing workflow file'], packages: [], declared_modules: [],
          estimated_subrequests: null});
        continue;
      }
      const row = await censusFile(entryRoot, file, {allowance,
        cpuBudget: flag('cpu', '1000'), functionName: entry.function_name, fallbackRoot: wsRoot});
      rows.push({...row, workflow: entry.function_name ?? null});
    }
    report.workspaces.push({root: relative(wsRoot, root).replaceAll('\\', '/') || '.', ...summarize(rows, relative(wsRoot, root)), rows});
  }
}

if (repoRoot) {
  const all = (await walk(repoRoot)).filter((f) => extname(f) === '.py' && !SKIP_PATH.test(f));
  const rows = [];
  for (const file of all) {
    const source = await readFile(file, 'utf8');
    const isWorkflow = /^\s*(?:from|import)\s+bifrost\b/m.test(source) && /^\s*async\s+def\s/m.test(source);
    if (!isWorkflow) continue;
    rows.push({...await censusFile(repoRoot, file, {allowance,
      cpuBudget: flag('cpu', '1000')}),
      path: relative(repoRoot, file).replaceAll('\\', '/'), role: roleOf(relative(repoRoot, file))});
  }
  report.repo = {...summarize(rows, 'repo workflow-shaped Python'), rows};
  report.repo.by_role = Object.fromEntries([...new Set(rows.map((r) => r.role))].map((role) => {
    const subset = rows.filter((r) => r.role === role);
    return [role, {files: subset.length, eligible: subset.filter((r) => r.backend === 'cloudflare').length}];
  }));

}

const lines = ['# Eligibility census', ''];
for (const w of report.workspaces) lines.push(`- workspace \`${w.root}\`: ${w.eligible}/${w.files} eligible (${w.eligible_pct}%)`);
if (report.repo) lines.push(`- repo workflow-shaped: ${report.repo.eligible}/${report.repo.files} eligible (${report.repo.eligible_pct}%)`);
lines.push('', `Subrequest allowance: ${allowance}. Static analysis only.`);
report.summary_text = lines.join('\n');
console.log(report.summary_text);
if (outPath) await writeFile(outPath, JSON.stringify(report, null, 2) + '\n');
