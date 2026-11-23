// Workers capacity provider. Run in a separate Worker from CloudflareLauncher.
import {createEngine, ProtocolError, STATE} from './capacity-core.mjs';
import {MAX_WORKFLOW_SLOTS, SLOT_BYTES} from './protocol.mjs';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SCOPE_HEADER = 'X-Bifrost-Capacity-Scope';

const json = (status, body) => Response.json(body, {status});

function profileFor(env) {
  const maxWorkers = Number(env.MAX_WORKERS);
  const maxLifetime = Number(env.MAX_LIFETIME_SECONDS);
  const slots = Number(env.WORKFLOW_SLOTS ?? 1);
  if (!/^[a-zA-Z0-9._:-]{1,128}$/.test(env.SCOPE ?? '') ||
      !/^.{1,100}$/.test(env.WORKER_VERSION ?? '') ||
      !/^[!-~]{32,512}$/.test(env.CAPACITY_TOKEN ?? '') ||
      !Number.isInteger(maxWorkers) || maxWorkers < 1 || maxWorkers > 1000 ||
      !Number.isInteger(maxLifetime) || maxLifetime < 60 || maxLifetime > 604800 ||
      !Number.isInteger(slots) || slots < 1 || slots > MAX_WORKFLOW_SLOTS ||
      !env.CLOUDFLARE_LAUNCHER) {
    throw new Error('Invalid Workers capacity settings');
  }
  return {
    protocol_version: 2, scope: env.SCOPE, worker_version: env.WORKER_VERSION,
    runtime_profile: 'cloudflare-python-v1', memory_limit_bytes: slots * SLOT_BYTES,
    max_workers: maxWorkers, max_lifetime_seconds: maxLifetime,
  };
}

// SQLite synchronous KV makes reserve/release atomic within a scope. The
// per-incarnation ledger serializes all requests that can change its slot.
export class ScopeRegistry {
  constructor(ctx) {
    this.storage = ctx.storage;
  }

  async fetch(request) {
    const {workerId, ceiling} = await request.json();
    const slots = this.storage.kv.get('slots') ?? [];
    if (request.method === 'DELETE') {
      this.storage.kv.put('slots', slots.filter((id) => id !== workerId));
      return json(200, {ok: true});
    }
    if (slots.includes(workerId)) return json(200, {ok: true});
    if (slots.length >= ceiling) return json(409, {ok: false});
    this.storage.kv.put('slots', [...slots, workerId]);
    return json(200, {ok: true});
  }
}

export class WorkerLedger {
  #engine;

  constructor(ctx, env) {
    const profile = profileFor(env);
    const storage = ctx.storage;
    const registry = env.SCOPE_REGISTRY.getByName(profile.scope);
    const slot = async (method, workerId) => {
      const response = await registry.fetch('https://do/slot', {
        method, body: JSON.stringify({workerId, ceiling: profile.max_workers}),
      });
      if (method === 'PUT' && response.status === 409) return false;
      if (!response.ok) throw new Error('scope registry unavailable');
      return true;
    };
    const launcher = (row) => env.CLOUDFLARE_LAUNCHER.getByName(row.worker_id);
    this.#engine = createEngine({
      profile: {...profile, starting_grace_ms: 0},
      store: {
        read: async () => (await storage.get('row')) ?? null,
        write: async (_, row) => { await storage.put('row', row); },
        discard: () => storage.delete('row'),
        reserve: (workerId) => slot('PUT', workerId),
        release: (workerId) => slot('DELETE', workerId),
      },
      runtime: {
        async prepare(row) {
          const slots = await launcher(row).prepareManaged({
            incarnationId: row.worker_id, launcherId: row.worker_id, resourceUid: row.resource_uid,
            version: row.reservation.worker_version, memoryLimitBytes: row.reservation.memory_limit_bytes,
            ...(row.reservation.deadline_ms === undefined ? {} : {
              deadlineMs: row.reservation.deadline_ms, lifetimeSeconds: row.reservation.max_lifetime_seconds,
              drainGraceMs: row.reservation.drain_grace_seconds * 1000,
            }),
            secret: row.control_key,
          });
          const expected = row.reservation.memory_limit_bytes / SLOT_BYTES;
          if (expected > 1 && slots !== expected) {
            throw new ProtocolError(409, 'Launcher does not confirm the prepared slot count');
          }
        },
        async start(row) {
          await launcher(row).startManaged(row.resource_uid);
          return {state: STATE.STARTING};
        },
        async destroy(row) {
          const usage = await launcher(row).releaseManaged(row.resource_uid);
          return {state: STATE.DELETED, usage};
        },
        async clearStorage(row) {
          await launcher(row).clearReleasedStorage(row.resource_uid);
        },
        async observe(row) {
          return launcher(row).observeManaged(row.resource_uid);
        },
      },
    });
  }

  async fetch(request) {
    const [, , , workerId, step] = new URL(request.url).pathname.split('/');
    try {
      const engine = this.#engine;
      if (request.method === 'GET') {
        const doc = await engine.get(workerId);
        return doc ? json(200, doc) : json(404, {error: 'unknown worker'});
      }
      let body;
      try {
        body = await request.json();
      } catch {
        throw new ProtocolError(400, 'invalid JSON');
      }
      if (request.method === 'PUT') return json(200, await engine.prepare(workerId, body));
      if (request.method === 'DELETE') return json(200, await engine.remove(workerId, body));
      if (step === 'start') return json(200, await engine.start(workerId, body));
      return json(404, {error: 'not found'});
    } catch (error) {
      return error instanceof ProtocolError ? json(error.status, {error: error.message}) :
        json(502, {error: 'provider operation unconfirmed'});
    }
  }
}

export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path === '/healthz' && request.method === 'GET') return json(200, {ok: true});
    let profile;
    try {
      profile = profileFor(env);
    } catch {
      return json(503, {error: 'provider is not configured'});
    }
    if (request.headers.get('Authorization') !== `Bearer ${env.CAPACITY_TOKEN}`) {
      return json(401, {error: 'unauthorized'});
    }
    if (path === '/v1/capabilities' && request.method === 'GET') return json(200, profile);
    const match = /^\/v1\/workers\/([^/]+)(\/start)?$/.exec(path);
    if (!match || !UUID.test(match[1]) || request.headers.get(SCOPE_HEADER) !== profile.scope ||
        !(match[2] ? request.method === 'POST' : ['GET', 'PUT', 'DELETE'].includes(request.method))) {
      return json(404, {error: 'not found'});
    }
    return env.WORKER_LEDGER.getByName(`${profile.scope}:${match[1]}`).fetch(request);
  },
};
