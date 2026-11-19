// Local physical runtime for the #1425 provider. The HTTP protocol, registry,
// ledger and lifecycle code are the actual provider modules.
import assert from 'node:assert/strict';
import {execFile, execFileSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {createServer} from 'node:https';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {buffer} from 'node:stream/consumers';
import {promisify} from 'node:util';

import provider, {
  ScopeRegistry, WorkerContainer, WorkerLedger,
} from '../../../../deploy/cloudflare/worker/capacity.mjs';

const docker = (args, options = {}) => execFileSync('docker', args, {
  encoding: 'utf8', timeout: 20_000, ...options,
}).trim();
const waitDocker = promisify(execFile);

export function localLinuxProvider({
  project, image, scope, token, version, archive, vmEnv = null, protocolOnly = false, gitFixture = false,
}) {
  const records = new Map();
  const resetActors = [];
  const status = (name) => docker(['inspect', '--format', '{{.State.Running}}', name]) === 'true';
  const env = {
    SCOPE: scope, CAPACITY_TOKEN: token, WORKER_VERSION: version,
    MEMORY_LIMIT_BYTES: String(1024 ** 3), MAX_WORKERS: '1',
    MAX_LIFETIME_SECONDS: '21600', STARTING_GRACE_MS: '30000',
    WORKER_CONTAINER_ENV: JSON.stringify({BIFROST_API_URL: 'http://api:8000', HOME: '/home/bifrost'}),
    ...(protocolOnly ? {WORKER_ENTRYPOINT_JSON: '["sleep","300"]'} : {}),
  };

  function runtime(workerId) {
    const name = project + '-' + scope + '-' + workerId;
    return {
      get running() {
        return records.has(workerId) && status(name);
      },
      start({env: launchEnv, entrypoint}) {
        const values = {...launchEnv};
        if (vmEnv) {
          delete values.BIFROST_WORKER_CONTROL_SECRET;
          Object.assign(values, vmEnv);
        }
        const args = ['run', '--detach', '--name', name, '--memory', '1g', '--memory-swap', '1g',
          '--network', project + '_default'];
        if (gitFixture) {
          args.push('--env', 'SSL_CERT_FILE=/app/.phase6-ca.pem',
            '--mount', `type=bind,src=${archive}/.phase6-ca.pem,dst=/app/.phase6-ca.pem,readonly`);
        }
        const childEnv = {...process.env};
        for (const [key, value] of Object.entries(values)) {
          args.push('--env', key);
          childEnv[key] = value;
        }
        for (const directory of ['src', 'shared', 'bifrost']) {
          args.push('--mount', `type=bind,src=${archive}/api/${directory},dst=/app/${directory},readonly`);
        }
        const record = {name, launches: []};
        records.set(workerId, record);
        const [program, ...programArgs] = entrypoint;
        record.launches.push(docker([...args, '--entrypoint', program, image, ...programArgs], {env: childEnv}));
        if (!vmEnv) {
          const names = JSON.parse(docker(['inspect', '--format', '{{json .Config.Env}}', name]))
            .map((item) => item.split('=', 1)[0]).sort();
          assert.ok(names.includes('BIFROST_WORKER_CONTROL_SECRET'));
          assert.ok(!names.some((key) => /DATABASE_URL|CAPACITY_TOKEN|BIFROST_SECRET_KEY|REDIS_URL|S3_SECRET_KEY/.test(key)));
          console.log(JSON.stringify({label: 'containers-env-names', worker: workerId, names}));
        }
      },
      async destroy() {
        await waitDocker('docker', ['stop', '--time', '5', name], {timeout: 15_000});
      },
      setInactivityTimeout() {
        // Docker has no Cloudflare inactivity timer. The API still retires idle workers.
      },
      async monitor() {
        const {stdout} = await waitDocker('docker', ['wait', name]);
        const exitCode = Number(stdout.trim());
        if (exitCode) throw Object.assign(new Error('Local container exited'), {exitCode});
      },
    };
  }

  function binding(Actor, physical = false) {
    const actors = new Map();
    const storage = new Map();
    resetActors.push(() => actors.clear());
    return {
      getByName(name) {
        if (!actors.has(name)) {
          if (!storage.has(name)) storage.set(name, new Map());
          const data = storage.get(name);
          const actor = new Actor({
            storage: {
              get: async (key) => structuredClone(data.get(key)),
              put: async (key, value) => data.set(key, structuredClone(value)),
            },
            blockConcurrencyWhile: (work) => work(),
            waitUntil: (work) => work.catch(() => {}),
            ...(physical ? {container: runtime(name)} : {}),
          }, env);
          let pending = Promise.resolve();
          actors.set(name, {
            fetch(input, init) {
              const request = input instanceof Request ? input : new Request(input, init);
              const result = pending.then(() => actor.fetch(request));
              pending = result.catch(() => {});
              return result;
            },
          });
        }
        return actors.get(name);
      },
    };
  }

  env.SCOPE_REGISTRY = binding(ScopeRegistry);
  env.WORKER_LEDGER = binding(WorkerLedger);
  env.WORKER_CONTAINER = binding(WorkerContainer, true);
  return {
    fetch: (request) => provider.fetch(request, env),
    running: (workerId) => records.has(workerId) && status(records.get(workerId).name),
    observe: (workerId) => {
      const record = records.get(workerId);
      return {launches: record?.launches ?? [], running: record && status(record.name) ? 1 : 0};
    },
    restart() {
      for (const reset of resetActors) reset();
    },
    async stop(workerId) {
      const record = records.get(workerId);
      assert.ok(record);
      await runtime(workerId).destroy();
    },
    close() {
      for (const {name} of records.values()) {
        if (docker(['ps', '--all', '--filter', 'name=^/' + name + '$', '--format', '{{.Names}}']) === name) {
          docker(['rm', '--force', name]);
        }
      }
    },
  };
}

export function localSolutionGit() {
  const root = mkdtempSync(join(tmpdir(), 'bifrost-containers-git-'));
  const repo = join(root, 'solution.git');
  const git = (...args) => execFileSync('git', args, {cwd: repo, stdio: 'pipe', timeout: 5000});
  try {
    mkdirSync(join(repo, 'solutions/demo'), {recursive: true});
    git('init', '--initial-branch=main');
    git('config', 'user.email', 'fixture@example.invalid');
    git('config', 'user.name', 'Git Stage Fixture');
    const descriptor = join(repo, 'solutions/demo/bifrost.solution.yaml');
    for (const version of ['1.0.0', '2.0.0']) {
      writeFileSync(descriptor, `slug: container-update\nname: Container Update\nversion: "${version}"\n`);
      git('add', 'solutions/demo/bifrost.solution.yaml');
      git('commit', '-m', version);
    }
    git('tag', 'update');
    git('reset', '--hard', 'HEAD^');
    return {
      fetches: 0,
      async fetch(request) {
        const {pathname, search} = new URL(request.url);
        if (!['/solution.git/info/refs', '/solution.git/git-upload-pack'].includes(pathname)) {
          return new Response(null, {status: 404});
        }
        if (request.method === 'POST') this.fetches += 1;
        const input = Buffer.from(await request.arrayBuffer());
        const output = execFileSync('git', ['http-backend'], {
          input, timeout: 5000, maxBuffer: 1024 ** 2,
          env: {
            PATH: process.env.PATH, GIT_PROJECT_ROOT: root, GIT_HTTP_EXPORT_ALL: '1',
            PATH_INFO: pathname, QUERY_STRING: search.slice(1), REQUEST_METHOD: request.method,
            CONTENT_TYPE: request.headers.get('content-type') ?? '',
            CONTENT_LENGTH: String(input.length),
            HTTP_GIT_PROTOCOL: request.headers.get('git-protocol') ?? '',
          },
        });
        const separator = output.indexOf('\r\n\r\n');
        assert.ok(separator > 0);
        const headers = new Headers();
        for (const line of output.subarray(0, separator).toString().split('\r\n')) {
          const colon = line.indexOf(':');
          headers.append(line.slice(0, colon), line.slice(colon + 1).trim());
        }
        const status = Number(headers.get('status')?.split(' ')[0] ?? 200);
        return new Response(output.subarray(separator + 4), {status, headers});
      },
      close() { rmSync(root, {recursive: true}); },
    };
  } catch (error) {
    rmSync(root, {recursive: true});
    throw error;
  }
}

export async function serveContainersProviders(host, providers) {
  const server = createServer({
    key: readFileSync('.phase6-key.pem'), cert: readFileSync('.phase6-ca.pem'),
  }, async (req, res) => {
    try {
      const [_, scope, path] = /^\/([^/]+)(\/.*)$/.exec(req.url) ?? [];
      if (!Object.hasOwn(providers, scope)) return res.writeHead(404).end();
      const body = req.method === 'GET' ? undefined : await buffer(req);
      const reply = await providers[scope].fetch(new Request('https://provider.invalid' + path, {
        method: req.method, headers: req.headers, body,
      }));
      res.writeHead(reply.status, Object.fromEntries(reply.headers));
      res.end(Buffer.from(await reply.arrayBuffer()));
    } catch (error) {
      console.error(JSON.stringify({label: 'containers-provider-error', type: error.name}));
      res.writeHead(502).end();
    }
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, host, resolve);
  });
  return server;
}
