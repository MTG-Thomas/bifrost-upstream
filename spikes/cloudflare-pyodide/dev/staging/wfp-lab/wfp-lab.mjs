#!/usr/bin/env node
// Workers for Platforms probe driver. Settles, by measurement:
//   1. whether a Python user Worker in a dispatch namespace cold-starts from a
//      deploy-time memory snapshot (as a normally deployed Python Worker does);
//   2. whether its CPU per invocation can be raised past the 30 s default.
// The same user script is first deployed normally to calibrate both signals.
//
//   node wfp-lab.mjs build-user  --out <dir> --name <script> [--cpu-ms N] [--runtime <generated-runtime.mjs>]
//   node wfp-lab.mjs build-dispatch --out <dir> --name <worker> --namespace <ns> --user-script <script>
//   node wfp-lab.mjs deploy --dir <dir> [--dispatch-namespace <ns>]
//   node wfp-lab.mjs cold   --dir <dir> --url <url> [--rounds 4] [--wait-s 20] [--dispatch-namespace <ns>]
//   node wfp-lab.mjs burn   --url <url> --n <iterations> [--cpu-ms N]
//
// Secrets: --secrets-file (default %TEMP%/wfp-lab-secrets.json, {"LAB_AUTH": ...}).
// LAB_EXPIRES_AT (epoch ms) comes from the environment at build time; every
// lab Worker answers 403 after it.
import {spawnSync} from 'node:child_process';
import {appendFileSync, copyFileSync, existsSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync} from 'node:fs';
import {request as httpsRequest} from 'node:https';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const spike = resolve(here, '../../..');
const [command, ...rest] = process.argv.slice(2);
const args = {};
for (let i = 0; i < rest.length; i += 2) args[rest[i].replace(/^--/, '')] = rest[i + 1];
const secretsFile = args['secrets-file'] ?? join(tmpdir(), 'wfp-lab-secrets.json');

function need(name) {
  if (!args[name]) throw new Error(`--${name} is required`);
  return args[name];
}

function expiresAt() {
  const value = process.env.LAB_EXPIRES_AT;
  if (!/^\d+$/.test(value ?? '')) throw new Error('LAB_EXPIRES_AT (epoch ms) must be set');
  return value;
}

function linkModules(out) {
  const target = join(spike, 'dev/tenant-workers/node_modules');
  if (!existsSync(join(out, 'node_modules'))) symlinkSync(target, join(out, 'node_modules'), 'junction');
  copyFileSync(join(spike, 'dev/tenant-workers/package.json'), join(out, 'package.json'));
}

async function buildUser() {
  const out = resolve(need('out'));
  mkdirSync(out, {recursive: true});
  const runtime = resolve(args.runtime ?? join(spike, 'dev/runtime/generated-runtime.mjs'));
  const packaged = (await import(pathToFileURL(runtime).href)).default;
  for (const [key, value] of Object.entries(packaged)) {
    const path = join(out, key);
    mkdirSync(dirname(path), {recursive: true});
    writeFileSync(path, value.py ?? value.js ?? Buffer.from(value.base64, 'base64'));
  }
  copyFileSync(join(here, 'user.py'), join(out, 'index.py'));
  const limits = args['cpu-ms'] ? `\n    limits: {cpuMs: ${Number(args['cpu-ms'])}},` : '';
  writeFileSync(join(out, 'cloudflare.config.ts'), `import {defineConfig} from 'cf/config';

export default defineConfig({
  worker: {
    name: ${JSON.stringify(need('name'))},
    entrypoint: './index.py',
    compatibilityDate: '2026-10-30',
    compatibilityFlags: ['python_workers'],
    workersDev: true,
    previewUrls: false,${limits}
    env: {
      LAB_AUTH: {type: 'secret'},
      LAB_EXPIRES_AT: {type: 'text', value: ${JSON.stringify(expiresAt())}},
    },
    observability: {enabled: false},
  },
});
`);
  linkModules(out);
  console.log(`built user script ${args.name} in ${out} (${Object.keys(packaged).length} runtime files)`);
}

function buildDispatch() {
  const out = resolve(need('out'));
  mkdirSync(out, {recursive: true});
  copyFileSync(join(here, 'dispatch.mjs'), join(out, 'index.mjs'));
  writeFileSync(join(out, 'cloudflare.config.ts'), `import {defineConfig} from 'cf/config';

export default defineConfig({
  worker: {
    name: ${JSON.stringify(need('name'))},
    entrypoint: './index.mjs',
    compatibilityDate: '2026-10-30',
    workersDev: true,
    previewUrls: false,
    env: {
      DISPATCHER: {type: 'dispatch-namespace', namespace: ${JSON.stringify(need('namespace'))}},
      USER_SCRIPT: {type: 'text', value: ${JSON.stringify(need('user-script'))}},
      LAB_AUTH: {type: 'secret'},
      LAB_EXPIRES_AT: {type: 'text', value: ${JSON.stringify(expiresAt())}},
    },
    observability: {enabled: false},
  },
});
`);
  linkModules(out);
  console.log(`built dispatch worker ${args.name} in ${out}`);
}

function deploy(dir, namespace) {
  // The project root is the build dir, so a previous build's output would be
  // bundled into the next one (recursively). Always deploy from clean.
  for (const stale of ['.cloudflare', '.wrangler']) rmSync(join(dir, stale), {recursive: true, force: true});
  const cfArgs = ['deploy', '--secrets-file', secretsFile];
  if (namespace) cfArgs.push('--dispatch-namespace', namespace);
  const started = Date.now();
  const result = spawnSync('cf', cfArgs, {cwd: dir, encoding: 'utf8', shell: process.platform === 'win32'});
  const output = `${result.stdout}\n${result.stderr}`;
  const keep = output.split(/\r?\n/)
    .filter((line) => !line.trimStart().startsWith('│ ') || /\[\d{4,}\]/.test(line))
    .filter((line) => /Startup Time|Current Version|Total Upload|APIError|\[\d{4,}\]|ERROR/.test(line));
  return {ok: result.status === 0, ms: Date.now() - started, lines: keep.map((line) => line.trim()).slice(0, 8)};
}

function get(url, extraHeaders = {}) {
  const {LAB_AUTH} = JSON.parse(readFileSync(secretsFile, 'utf8'));
  return new Promise((resolveResult) => {
    const started = Date.now();
    const req = httpsRequest(url, {headers: {'x-lab-auth': LAB_AUTH, 'user-agent': 'bifrost-lab/1.0', ...extraHeaders}}, (res) => {
      let text = '';
      res.on('data', (chunk) => (text += chunk));
      res.on('end', () => {
        let body = text;
        try { body = JSON.parse(text); } catch {}
        resolveResult({status: res.statusCode, ms: Date.now() - started, body});
      });
    });
    req.setTimeout(400_000, () => req.destroy(new Error('client timeout')));
    req.on('error', (error) => resolveResult({status: 0, ms: Date.now() - started, body: String(error)}));
    req.end();
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function cold() {
  const dir = resolve(need('dir'));
  const url = need('url');
  const rounds = Number(args.rounds ?? 4);
  const waitMs = Number(args['wait-s'] ?? 20) * 1000;
  const rows = [];
  for (let round = 1; round <= rounds; round++) {
    appendFileSync(join(dir, 'index.py'), `\n# cold round ${round} ${new Date().toISOString()}\n`);
    const deployed = deploy(dir, args['dispatch-namespace']);
    if (!deployed.ok) { console.log(JSON.stringify({round, deploy: deployed})); process.exitCode = 1; return; }
    await sleep(waitMs);
    const first = await get(`${url}?action=ping`);
    const warm1 = await get(`${url}?action=ping`);
    const warm2 = await get(`${url}?action=ping`);
    const row = {
      round, deploy_ms: deployed.ms, deploy: deployed.lines.filter((l) => /Startup Time/i.test(l)),
      first: {status: first.status, ms: first.ms, served: first.body?.served, module_wall_ms: first.body?.module_wall_ms},
      warm_ms: [warm1.ms, warm2.ms],
      // Snapshot creation runs top-level code with the clock at the epoch
      // (calibrated: module_wall_ms 462 on a normal deploy). Without a
      // snapshot it runs inside the first request, at real wall time.
      snapshot: typeof first.body?.module_wall_ms === 'number' ? first.body.module_wall_ms < 86_400_000 : null,
    };
    rows.push(row);
    console.log(JSON.stringify(row));
  }
  console.log('SUMMARY', JSON.stringify({
    first_ms: rows.map((r) => r.first.ms), snapshot: rows.map((r) => r.snapshot),
  }));
}

async function burn() {
  const n = need('n');
  const cpu = args['cpu-ms'] ? `&cpuMs=${args['cpu-ms']}` : '';
  const result = await get(`${need('url')}?action=burn&n=${n}${cpu}`);
  const body = typeof result.body === 'string' ? result.body.slice(0, 400) : result.body;
  console.log(JSON.stringify({n: Number(n), cpu_ms: args['cpu-ms'] ?? null, status: result.status, wall_ms: result.ms, body}));
}

const commands = {
  'build-user': buildUser,
  'build-dispatch': buildDispatch,
  deploy: () => console.log(JSON.stringify(deploy(resolve(need('dir')), args['dispatch-namespace']))),
  cold,
  burn,
};
if (!commands[command]) {
  console.error(`usage: node wfp-lab.mjs ${Object.keys(commands).join('|')} ...`);
  process.exit(2);
}
await commands[command]();
