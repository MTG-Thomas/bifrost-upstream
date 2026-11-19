import {DurableObject} from 'cloudflare:workers';
import {createLauncherWorkflowRunner, launcherTenantId} from '../tenant-workers/launcher-host.mjs';
import {LauncherCore} from './launcher-core.mjs';
import {MAX_WORKFLOW_SLOTS, SLOT_BYTES} from './protocol.mjs';

const DIAL_TIMEOUT_MS = 10_000;
const ALARM_WATCHDOG_MS = 15_000;
const MAX_BACKOFF_MS = 30_000;
const STORAGE_CHUNK = 1 << 20;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function readSettings(env) {
  const {BIFROST_WORKER_CONTROL_SECRET: secret, BIFROST_WORKER_INCARNATION_ID: incarnationId,
    BIFROST_WORKER_LAUNCHER_ID: launcherId, BIFROST_WORKER_RESOURCE_UID: resourceUid,
    BIFROST_WORKER_VERSION: version} = env;
  if (env.BIFROST_CLOUDFLARE_LAUNCHER_ENABLED !== 'true') {
    throw new Error('Incomplete Cloudflare launcher enrollment');
  }
  return enrollmentSettings({incarnationId, launcherId, resourceUid, version, secret}, env);
}

function enrollmentSettings({incarnationId, launcherId, resourceUid, version, secret,
  memoryLimitBytes = SLOT_BYTES}, env) {
  const slots = memoryLimitBytes / SLOT_BYTES;
  if (!/^[0-9a-f]{64}$/.test(secret ?? '') || !UUID.test(incarnationId ?? '') ||
      !UUID.test(launcherId ?? '') || !/^[A-Za-z0-9._:-]{1,255}$/.test(resourceUid ?? '') ||
      !Number.isInteger(slots) || slots < 1 || slots > MAX_WORKFLOW_SLOTS ||
      typeof version !== 'string' || !version) throw new Error('Invalid launcher enrollment');
  const origin = new URL(env.API_ORIGIN);
  if (origin.protocol !== 'https:' || origin.pathname !== '/' || origin.username || origin.password ||
      origin.search || origin.hash) throw new Error('Cloudflare launcher requires a fixed HTTPS API origin');
  return {identity: {incarnationId, launcherId, resourceUid, version, hostname: `cf-${launcherId}`}, secret, slots,
    controlUrl: new URL('/api/worker-control', origin).href, publicUrl: origin.origin};
}

// SQLite limits a single row to 2 MB. Put the slot's inventory and terminal
// into byte chunks in one transaction so replay cannot see a partial record.
function confirmedStorage(ctx) {
  const {storage} = ctx;
  const sql = storage.sql;
  sql.exec('CREATE TABLE IF NOT EXISTS ledger (key TEXT, part INTEGER, data BLOB, PRIMARY KEY (key, part))');
  const confirm = async (write) => {
    try {
      write();
      await storage.sync();
    } catch {
      // Re-read the ledger after a failed write. The commit may already have
      // happened, and the current instance cannot continue from its cache.
      ctx.abort('Cloudflare launcher ledger write failed');
    }
  };
  return {
    async get(key) {
      const rows = sql.exec('SELECT data FROM ledger WHERE key = ? ORDER BY part', key).toArray();
      if (rows.length === 0) return undefined;
      const decoder = new TextDecoder();
      const text = rows.map(({data}) => decoder.decode(data, {stream: true})).join('') + decoder.decode();
      return JSON.parse(text);
    },
    put(key, value) {
      const bytes = new TextEncoder().encode(JSON.stringify(value));
      return confirm(() => storage.transactionSync(() => {
        sql.exec('DELETE FROM ledger WHERE key = ?', key);
        for (let offset = 0; offset < bytes.length; offset += STORAGE_CHUNK) {
          const chunk = bytes.subarray(offset, offset + STORAGE_CHUNK);
          sql.exec('INSERT INTO ledger VALUES (?, ?, ?)', key, offset, chunk);
        }
      }));
    },
    delete(key) {
      return confirm(() => sql.exec('DELETE FROM ledger WHERE key = ?', key));
    },
    clearReleased() {
      return confirm(() => sql.exec("DELETE FROM ledger WHERE key != 'enrollment'"));
    },
  };
}

export class CloudflareLauncher extends DurableObject {
  #core;
  #settings;
  #connection = null;
  #backoff = 1000;
  #storage;
  #managed;
  #fact;
  #tail = Promise.resolve();

  constructor(ctx, env) {
    super(ctx, env);
    this.#storage = confirmedStorage(ctx);
    this.#managed = env.BIFROST_CLOUDFLARE_MANAGED === 'true';
    if (this.#managed) {
      if (env.CAPACITY_TOKEN || env.BIFROST_WORKER_CAPACITY_TOKEN || env.BIFROST_SECRET_KEY ||
          env.BIFROST_WORKER_CONTROL_SECRET) throw new Error('Managed launcher received a forbidden Worker secret');
      ctx.blockConcurrencyWhile(async () => {
        this.#fact = await this.#storage.get('enrollment');
        if (this.#fact?.enrollment) this.#initialize(enrollmentSettings(this.#fact.enrollment, this.env));
      });
    } else {
      this.#initialize(readSettings(env));
    }
  }

  #initialize(settings) {
    this.#settings = settings;
    const getHost = (id) => this.ctx.exports.TenantHost.getByName(id);
    const runWorkflow = createLauncherWorkflowRunner(getHost);
    const summary = this.env.SUMMARY_PROVIDER;
    const runSummary = !this.#managed && typeof summary?.complete === 'function' ?
      async (route, context, signal) => {
        let close;
        const cancellation = new ReadableStream({
          type: 'bytes',
          start(controller) {
            close = () => {
              close = undefined;
              controller.close();
            };
          },
          cancel() { close = undefined; },
        });
        const abort = () => close?.();
        signal.addEventListener('abort', abort, {once: true});
        if (signal.aborted) abort();
        try {
          return await summary.complete({...context, remaining_ms: Date.parse(route.deadline) - Date.now()},
            cancellation);
        } finally {
          signal.removeEventListener('abort', abort);
          close = undefined;
        }
      } : undefined;
    this.#core = new LauncherCore({
      ...settings, clock: Date.now, storage: this.#storage,
      runSummary,
      send: (session, frame) => {
        const connection = this.#connection;
        if (connection?.session !== session || connection.socket.readyState !== WebSocket.OPEN) {
          throw new Error('Control socket is closed');
        }
        connection.socket.send(frame);
      },
      runHost: (route, context, signal) => runWorkflow({
        route, context, publicUrl: this.#settings.publicUrl, signal,
      }),
      stopHost: async (route) => getHost(await launcherTenantId(route)).stopWorkflow(route.route_id),
      background: (task) => this.#observe(this.#connection, task),
    });
  }

  #serialize(fn) {
    const task = this.#tail.then(fn);
    this.#tail = task.catch(() => {});
    return task;
  }

  #binding(resourceUid) {
    if (!this.#managed || (this.#fact && this.#fact.resourceUid !== resourceUid)) {
      throw new Error('Launcher enrollment binding mismatch');
    }
    return this.#fact;
  }

  async #saveFact(fact) {
    await this.#storage.put('enrollment', fact);
    this.#fact = fact;
  }

  prepareManaged(enrollment) {
    return this.#serialize(async () => {
      const settings = enrollmentSettings(enrollment, this.env);
      if (enrollment.launcherId !== enrollment.incarnationId) throw new Error('Invalid managed enrollment');
      const fact = this.#binding(enrollment.resourceUid);
      if (fact) {
        if (!fact.enrollment || Object.keys(enrollment).some((key) => key !== 'memoryLimitBytes' &&
            enrollment[key] !== fact.enrollment[key]) || (fact.enrollment.memoryLimitBytes ?? SLOT_BYTES) !==
            settings.slots * SLOT_BYTES) {
          throw new Error('Launcher enrollment already bound');
        }
        return settings.slots;
      }
      await this.#saveFact({resourceUid: enrollment.resourceUid, enrollment, state: 'prepared'});
      this.#initialize(settings);
      return settings.slots;
    });
  }

  startManaged(resourceUid) {
    return this.#serialize(async () => {
      const fact = this.#binding(resourceUid);
      if (!fact || !['prepared', 'accepted'].includes(fact.state)) throw new Error('Launcher cannot start');
      if (fact.state === 'prepared') {
        // Set the wake before committing acceptance. A reset after the wake
        // cannot dial until the durable state says this start was accepted.
        await this.ctx.storage.setAlarm(Date.now());
        await this.#saveFact({...fact, state: 'accepted'});
      }
    });
  }

  observeManaged(resourceUid) {
    return this.#serialize(async () => {
      const fact = this.#binding(resourceUid);
      if (!fact || fact.state === 'prepared') return {state: 'prepared', start_fact: 'never_started'};
      if (fact.state === 'deleted') return {state: 'deleted', start_fact: 'destroyed'};
      if (await this.#core.isStopped()) return {state: 'stopped', start_fact: 'exited'};
      return {state: this.#connection?.session.generation ? 'running' : 'starting', start_fact: 'accepted'};
    });
  }

  releaseManaged(resourceUid) {
    return this.#serialize(async () => {
      const fact = this.#binding(resourceUid);
      if (fact?.state === 'deleted') {
        await this.#stopReleased();
        return;
      }
      // The API already fenced this incarnation. Release cannot deliver a
      // terminal or wait for an ACK from a successor owner.
      await this.#core?.release();
      await this.#finishRelease(resourceUid);
    });
  }

  async #finishRelease(resourceUid) {
    await this.#saveFact({resourceUid, state: 'deleted'});
    const connection = this.#connection;
    if (connection) await this.#disconnect(connection);
    this.#settings = null;
    this.#core = null;
    await this.#stopReleased();
  }

  async #stopReleased() {
    await this.#storage.clearReleased();
    await this.ctx.storage.deleteAlarm();
  }

  async start() {
    if (this.#managed) throw new Error('Managed launch requires the capacity provider');
    if (await this.#core.canReconnect() && await this.ctx.storage.getAlarm() === null) {
      await this.ctx.storage.setAlarm(Date.now());
    }
  }

  async alarm() {
    if (this.#managed && !await this.#serialize(() => this.#fact?.state === 'accepted')) return;
    // Keep a durable wake-up even if the DO resets while a connection is open.
    // A close schedules an earlier retry. Only the current alarm opens sockets.
    const core = this.#core;
    if (!core || core.retired) return;
    await this.ctx.storage.setAlarm(Date.now() + ALARM_WATCHDOG_MS);
    if (this.#connection) return;
    const dial = await core.beginDial();
    if (!dial || this.#core !== core || core.retired) {
      if (core.retired) await this.ctx.storage.deleteAlarm();
      return;
    }
    const controller = new AbortController();
    const connection = {session: dial.session, socket: null, welcomeTimer: null, heartbeat: null, controller};
    this.#connection = connection;
    console.info('launcher_control_dial', this.#settings.identity.incarnationId);
    const timeout = setTimeout(() => controller.abort(), DIAL_TIMEOUT_MS);
    try {
      const response = await fetch(this.#settings.controlUrl, {
        headers: {Upgrade: 'websocket'}, redirect: 'manual', signal: controller.signal,
      }).finally(() => clearTimeout(timeout));
      connection.socket = response.webSocket;
      if (this.#connection !== connection) {
        if (connection.socket) {
          connection.socket.accept();
          connection.socket.close(1000, 'launcher ended');
        } else {
          await response.body?.cancel();
        }
        return;
      }
      if (!connection.socket) {
        console.info('launcher_control_upgrade_refused', response.status);
        await response.body?.cancel();
        await this.#disconnect(connection);
        return;
      }
      const ws = connection.socket;
      ws.addEventListener('message', ({data}) => this.#observe(connection, this.#receive(connection, data)));
      ws.addEventListener('close', ({code}) => this.#observe(connection, this.#disconnect(connection, code)));
      ws.addEventListener('error', () => this.#observe(connection, this.#disconnect(connection)));
      ws.accept();
      ws.send(dial.hello);
      connection.welcomeTimer = setTimeout(() => this.#observe(connection, this.#disconnect(connection)), DIAL_TIMEOUT_MS);
    } catch {
      await this.#disconnect(connection);
    }
  }

  async #receive(connection, data) {
    if (this.#connection !== connection) return;
    await this.#core.receive(connection.session, data);
    if (this.#connection !== connection) return;
    if (connection.session.generation && connection.heartbeat === null) {
      console.info('launcher_control_welcome', connection.session.generation);
      clearTimeout(connection.welcomeTimer);
      this.#backoff = 1000;
      connection.heartbeat = setInterval(() =>
        this.#observe(connection, this.#core.tick(connection.session)), 1000);
    }
  }

  #observe(connection, task) {
    this.ctx.waitUntil(task.catch(async () => {
      if (connection) await this.#disconnect(connection);
    }));
  }

  async #disconnect(connection, code) {
    if (this.#connection !== connection) return;
    this.#connection = null;
    connection.controller.abort();
    clearTimeout(connection.welcomeTimer);
    clearInterval(connection.heartbeat);
    const ws = connection.socket;
    const core = this.#core;
    await core.closed(connection.session, code);
    console.info('launcher_control_closed', code ?? null, core.retired);
    if (ws?.readyState === WebSocket.OPEN) ws.close(1000, 'launcher control closed');
    if (core.retired || this.#fact?.state === 'deleted') {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    await this.ctx.storage.setAlarm(Date.now() + this.#backoff);
    this.#backoff = Math.min(this.#backoff * 2, MAX_BACKOFF_MS);
  }
}
