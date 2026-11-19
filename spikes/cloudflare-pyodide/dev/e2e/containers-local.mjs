// Local physical runtime for the #1425 provider. The HTTP protocol, registry,
// ledger and lifecycle code are the actual provider modules.
import assert from 'node:assert/strict';
import {execFile, execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {createServer} from 'node:https';
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
  project, image, scope, token, version, archive, vmEnv = null, protocolOnly = false,
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
        record.launches.push(docker([...args, image, ...entrypoint], {env: childEnv}));
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
