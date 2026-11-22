import {normalizeEgressPattern} from '../executor/egress-hosts.mjs';
import {isStoragePath} from '../tenant-workers/storage-path.mjs';
import {METER_ERROR} from './launcher-meter.mjs';
import {
  CLOUDFLARE_HANDLERS, CLOUDFLARE_INCARNATION_ENDED, CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
  CREDENTIAL_PROTOCOL, FragmentAssembler, ProtocolError, encodeEnvelope, envelope, fragment,
  newNonce, parseEnvelope, signHello, SLOT_BYTES,
} from './protocol.mjs';

const CONTROL = 'control';
const ACKNOWLEDGED = 'acknowledged';
const STOP_SETTLEMENT_MS = 2000;
const slotKey = (index) => index === 0 ? 'slot' : `slot:${index}`;
const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const UUID_RE = new RegExp(`^${UUID}$`);
const WORKFLOW_ROUTE = new RegExp(`^workflow:(${UUID}):${UUID}$`);
const PREPARE_ROUTE = new RegExp(`^prepare:${UUID}$`);
const SUMMARY_ROUTE = new RegExp(`^summary:(${UUID}):(${UUID})$`);
const AGENT_ROUTE = new RegExp(`^agent:(${UUID}):(${UUID})$`);
const INSTANT = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|[+-]\d\d:\d\d)$/;
const HEX64 = /^[0-9a-f]{64}$/;
const STATES = new Set(['not_started', 'completed', 'stopped', 'uncertain']);
const record = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const integer = (value) => Number.isSafeInteger(value) && value >= 0;
const slotOrdinal = (value) => integer(value) && value < 32;
const instant = (value) => typeof value === 'string' && INSTANT.test(value) && Number.isFinite(Date.parse(value));

function validWorkspace(workspace, entry) {
  if (!record(workspace) || !record(workspace.manifest) || !record(workspace.sources) ||
      !Object.hasOwn(workspace.manifest, entry) || !Array.isArray(workspace.packages) ||
      !workspace.packages.every((name) => typeof name === 'string') ||
      !Array.isArray(workspace.https_hosts) ||
      !workspace.https_hosts.every((host) => typeof host === 'string' && normalizeEgressPattern(host) === host) ||
      !integer(workspace.subrequest_allowance) || workspace.subrequest_allowance > 50 ||
      !integer(workspace.cpu_budget_ms) || workspace.cpu_budget_ms === 0) return false;

  const paths = Object.keys(workspace.manifest);
  return paths.length === Object.keys(workspace.sources).length && paths.every((path) =>
    isStoragePath(path) && path.length <= 512 && HEX64.test(workspace.manifest[path]) &&
    Object.hasOwn(workspace.sources, path) && typeof workspace.sources[path] === 'string');
}

function workflowRoute(payload, runtimeHandle, incarnationId) {
  const {route_id, execution_id, active_execution, started_at, timeout_seconds,
    workflow_deadline, memory_request_bytes, context} = payload;
  if (typeof route_id !== 'string' || WORKFLOW_ROUTE.exec(route_id)?.[1] !== execution_id ||
      !record(active_execution) || active_execution.execution_id !== execution_id ||
      typeof active_execution.sync !== 'boolean' ||
      (active_execution.org_id != null && typeof active_execution.org_id !== 'string') ||
      !instant(started_at) || !integer(timeout_seconds) ||
      !integer(memory_request_bytes) || memory_request_bytes > SLOT_BYTES ||
      (timeout_seconds === 0 ? workflow_deadline !== null :
        !instant(workflow_deadline) || Date.parse(workflow_deadline) - Date.parse(started_at) !== timeout_seconds * 1000) ||
      !record(context) || context.workflow_deadline !== workflow_deadline ||
      context.code != null || context.service != null || context.artifact_workspace_id != null ||
      context.solution_global_repo_access ||
      typeof context.engine_token !== 'string' || !context.engine_token ||
      !validWorkspace(context.cloudflare_workspace, context.file_path)) return null;
  const workspace = context.cloudflare_workspace;
  // Wire consistency only. The API verifies the approved source bytes before sending.
  if ((context.solution_id ?? null) !== (workspace.solution_id ?? null) ||
      (context.solution_id != null && (!UUID_RE.test(context.solution_id) ||
        workspace.manifest[context.file_path] !== context.content_hash))) return null;

  return {kind: 'workflow', route_id, subject_id: execution_id, runtime_handle: runtimeHandle,
    active_execution, started_at, timeout_seconds, workflow_deadline, memory_request_bytes,
    ...(incarnationId ? {engine_slot: payload.engine_slot, incarnation_id: incarnationId} : {})};
}

function summaryRoute(payload, runtimeHandle, now) {
  const {route_id, run_id, claim_id, timeout_seconds, memory_request_bytes, context} = payload;
  const identity = SUMMARY_ROUTE.exec(route_id);
  if (!identity || identity[1] !== run_id || identity[2] !== claim_id ||
      !integer(timeout_seconds) || timeout_seconds < 1 || timeout_seconds > 30 ||
      !integer(memory_request_bytes) || memory_request_bytes === 0 ||
      memory_request_bytes > SLOT_BYTES || !record(context)) return null;
  const deadline = new Date(now + timeout_seconds * 1000).toISOString();
  return {kind: 'agent_summary', route_id, subject_id: run_id, claim_id, deadline,
    runtime_handle: runtimeHandle, memory_request_bytes};
}

function failure(attemptState, errorType, error) {
  return {attempt_state: attemptState, result: {success: false, error_type: errorType, error}};
}

function agentRoute(payload, runtimeHandle) {
  const {route_id, run_id, claim_id, deadline, memory_request_bytes} = payload;
  const identity = AGENT_ROUTE.exec(route_id);
  if (!identity || identity[1] !== run_id || identity[2] !== claim_id || !instant(deadline) ||
      !integer(memory_request_bytes) || !memory_request_bytes || memory_request_bytes > SLOT_BYTES) return null;
  return {kind: 'agent_run', route_id, subject_id: run_id, claim_id, deadline,
    runtime_handle: runtimeHandle, memory_request_bytes};
}

function timeout(route) {
  const detail = route.kind === 'workflow' ? 'Workflow deadline elapsed before engine entry' :
    'Summary deadline elapsed before provider dispatch';
  return failure('not_started', 'TimeoutError', detail);
}

// Each slot holds one confirmed route record. A terminal and its metadata
// are written together and held until the API acknowledges that exact event.
export class LauncherCore {
  #acknowledged = new Set();
  #stops = new Set();
  #control;
  #slots;
  #session = null;
  #tail = Promise.resolve();
  #aborts = new Map();
  #preparations = new Map();
  #agentRequests = new Map();
  #released = false;
  #cleanupRetryAt = 0;
  #cleanupDelayMs = 2000;

  constructor({identity, secret, clock, storage, send, runHost, prepareHost, stopHost, runSummary, runAgent,
    runCapacityCanary, background, meter, slots = 1, tenantSlots = false, newId = newNonce}) {
    this.#slots = Array(slots).fill(null);
    this.identity = identity;
    this.tenantSlots = tenantSlots;
    this.secret = secret;
    this.clock = clock;
    this.storage = storage;
    this.send = send;
    this.runHost = runHost;
    this.prepareHost = prepareHost;
    this.runSummary = runSummary;
    this.runAgent = runAgent;
    this.runCapacityCanary = runCapacityCanary;
    this.stopHost = stopHost;
    this.background = background;
    this.newId = newId;
    this.meter = meter;
  }

  get retired() {
    return this.#released || this.#control?.state === 'retired';
  }

  status() {
    return this.#control?.state ?? 'starting';
  }

  canReconnect() {
    return this.#serialize(async () => {
      await this.#load();
      return !this.retired && (!this.meter?.exhausted || this.#slots.some(Boolean));
    });
  }

  isStopped() {
    return this.#serialize(async () => {
      await this.#load();
      return this.retired || Boolean(this.meter?.exhausted && this.#slots.every(slot => !slot));
    });
  }

  release() {
    return this.#serialize(async () => {
      if (this.#released && !this.tenantSlots) return;
      const slots = this.#control ? this.#slots :
        await Promise.all(this.#slots.map((_, index) => this.storage.get(slotKey(index))));
      this.#released = true;
      this.#session = null;
      for (const controller of this.#preparations.values()) controller.abort();
      if (this.tenantSlots) {
        // A delete witness must cover the child, even if canonical settlement
        // was already acknowledged or the launcher reset after dispatch.
        if (this.clock() < this.#cleanupRetryAt) throw new Error('Workflow stop confirmation is backing off');
        const proof = await Promise.all(slots.filter(slot => slot?.route.kind === 'workflow' &&
          (slot.state === 'entering' || slot.state === 'stopping' || slot.terminal?.attempt_state === 'uncertain')).map(({route}) =>
          this.#confirmStop(route)));
        if (proof.includes(false)) {
          this.#cleanupRetryAt = this.clock() + this.#cleanupDelayMs;
          this.#cleanupDelayMs = Math.min(this.#cleanupDelayMs * 2, 30_000);
          throw new Error('Workflow stop is unconfirmed');
        }
      }
      for (const slot of slots) {
        if (slot?.state === 'entering' && !(this.tenantSlots && slot.route.kind === 'workflow')) this.#stop(slot.route);
      }
      this.#slots.fill(null);
      if (this.#stops.size) {
        let timer;
        await Promise.race([
          Promise.all(this.#stops),
          new Promise(resolve => { timer = setTimeout(resolve, STOP_SETTLEMENT_MS); }),
        ]).finally(() => clearTimeout(timer));
      }
    });
  }

  #serialize(fn) {
    const task = this.#tail.then(fn);
    this.#tail = task.catch(() => {});
    return task;
  }

  async #saveControl(control) {
    await this.storage.put(CONTROL, control, 'control');
    this.#control = control;
  }

  async #saveSlot(index, slot) {
    await this.storage.put(slotKey(index), slot, slot.state === 'terminal' || slot.state === 'stopping' ? 'settlement' : 'workload');
    this.#slots[index] = slot;
  }

  async #load() {
    if (this.#control) return;
    const control = await this.storage.get(CONTROL);
    this.#acknowledged = new Set(await this.storage.get(ACKNOWLEDGED));
    for (let index = 0; index < this.#slots.length; index++) {
      const slot = this.#slots[index] = await this.storage.get(slotKey(index));
      if (!slot || slot.state === 'terminal') continue;
      if (slot.state === 'stopping') {
        this.#observeStop(index, slot);
        continue;
      }
      const entering = slot.state === 'entering';
      if (entering && !(this.tenantSlots && slot.route.kind === 'workflow')) this.#stop(slot.route);
      const outcome = this.#expired(slot.route) && !entering ? timeout(slot.route) :
        failure(entering ? 'uncertain' : 'not_started', 'LauncherReset', 'Launcher reset before a terminal was recorded');
      await this.#settle(index, slot.route, outcome);
    }
    this.#control = control ?? {helloSequence: 0, generation: 0, state: 'starting'};
  }

  beginDial() {
    return this.#serialize(async () => {
      if (this.#released) return null;
      await this.#load();
      if (this.retired || this.#session || (this.meter?.exhausted && !this.#slots.some(Boolean))) return null;
      const helloSequence = this.#control.helloSequence + 1;
      if (!Number.isSafeInteger(helloSequence)) throw new ProtocolError('hello sequence exhausted');
      await this.#saveControl({...this.#control, helloSequence});
      const hello = await signHello(this.secret, {
        launcher_id: this.identity.launcherId, incarnation_id: this.identity.incarnationId,
        resource_uid: this.identity.resourceUid, hostname: this.identity.hostname,
        form: 'external', execution_lane: 'normal', runtime_profile: CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
        issued_at: Math.floor(this.clock() / 1000), nonce: this.newId(), hello_sequence: helloSequence,
      });
      const session = {generation: 0, sent: 0, received: 0, nextHeartbeat: 0,
        assembler: new FragmentAssembler()};
      this.#session = session;
      const handlers = [...CLOUDFLARE_HANDLERS];
      if (this.runSummary) handlers.push(['agent.summarize', 1]);
      if (this.runAgent) handlers.push(['agent.run', 1]);
      return {session, hello: JSON.stringify({event: 'hello', hello, handlers,
        ...(this.runSummary ? {summary_claim_protocol: 1} : {}),
        ...(this.runAgent ? {agent_claim_protocol: 'autonomous-v1'} : {}),
        credential_protocol: CREDENTIAL_PROTOCOL, version: this.identity.version})};
    });
  }

  closed(session, code) {
    if (this.#session !== session) return Promise.resolve();
    this.#session = null;
    for (const controller of this.#preparations.values()) controller.abort();
    return this.#serialize(async () => {
      if (this.#released) return;
      if (code === CLOUDFLARE_INCARNATION_ENDED) {
        await this.#saveControl({...this.#control, state: 'retired'});
        for (const [index, slot] of this.#slots.entries()) {
          if (slot?.state !== 'entering') continue;
          if (!(this.tenantSlots && slot.route.kind === 'workflow')) this.#stop(slot.route);
          await this.#settle(index, slot.route, failure('uncertain', 'LauncherRetired', 'The incarnation ended'));
        }
      }
    });
  }

  receive(session, raw) {
    return this.#serialize(async () => {
      if (this.#session !== session) throw new ProtocolError('stale control connection');
      try {
        return await this.#receiveCurrent(session, raw);
      } catch (error) {
        // Fence queued messages before the adapter observes the failure and
        // closes the transport.
        if (this.#session === session) this.#session = null;
        throw error;
      }
    });
  }

  async #receiveCurrent(session, raw) {
    if (!session.generation) return this.#welcome(session, raw);
    const frame = parseEnvelope(raw);
    if (frame.session_generation !== session.generation || !Number.isSafeInteger(frame.sequence) ||
        frame.sequence <= session.received) throw new ProtocolError('stale control generation or sequence');
    session.received = frame.sequence;
    const message = session.assembler.feed(frame);
    if (!message) return;
    if (message.kind === 'ack') {
      const index = this.#slots.findIndex((slot) =>
        slot?.state === 'terminal' && slot.terminal._event_id === message.payload.event_id);
      if (index !== -1) {
        const {route, terminal, invocations} = this.#slots[index];
        const routeId = route.route_id;
        if (invocations) this.meter?.recordRoute(routeId, invocations);
        // Persist the compact replay fence before removing the bulky slot
        // and its tombstone. A reset between writes can only replay the ACK.
        const acknowledged = new Set(this.#acknowledged).add(routeId);
        await this.storage.put(ACKNOWLEDGED, [...acknowledged], 'settlement');
        this.#acknowledged = acknowledged;
        if (!(route.kind === 'workflow' && this.tenantSlots && terminal.attempt_state === 'uncertain')) {
          await this.storage.delete(slotKey(index), `seen:${routeId}`);
          this.#slots[index] = null;
        }
        return 'terminal_ack';
      }
      return;
    }
    if (message.kind !== 'command') throw new ProtocolError('expected control command or ack');
    return this.#command(session, message.payload);
  }

  async #welcome(session, raw) {
    let message;
    try {
      if (typeof raw !== 'string' || raw.length > 1024) throw new Error();
      message = JSON.parse(raw);
    } catch {
      throw new ProtocolError('invalid welcome');
    }
    const generation = message?.session_generation;
    if (message?.event !== 'welcome' || !Number.isSafeInteger(generation) ||
        generation <= this.#control.generation) throw new ProtocolError('stale or invalid welcome');
    await this.#saveControl({...this.#control, generation});
    if (this.#session !== session) return;
    session.generation = generation;
    const routes = [];
    const terminalRoutes = [];
    for (const slot of this.#slots) {
      if (!slot) continue;
      if (slot.state === 'terminal') {
        if (this.#acknowledged.has(slot.route.route_id)) continue;
        terminalRoutes.push({...slot.route, event_id: slot.terminal._event_id,
          attempt_state: slot.terminal.attempt_state});
      } else {
        routes.push(slot.route);
      }
    }
    this.#emit(session, {event: 'inventory', inventory: {
      version: 1, routes, terminal_routes: terminalRoutes,
      ...(this.prepareHost ? {admission_prepare: true, ...(this.tenantSlots ? {queue_prewarm: true} : {})} : {}),
      ...(this.tenantSlots ? {tenant_slot_protocol: 1} : {}),
      ...(this.meter?.exhausted ? {meter_exhausted: true} : {}),
      ...(this.#slots.length > 1 ? {workflow_slots: this.#slots.length} : {}),
    }});
    for (const slot of this.#slots) {
      if (slot?.state === 'terminal' && !this.#acknowledged.has(slot.route.route_id)) this.#emit(session, slot.terminal);
    }
    for (const request of this.#agentRequests.values()) {
      if (!PREPARE_ROUTE.test(request.event.route_id)) this.#emit(session, request.event);
    }
    session.nextHeartbeat = this.clock() + 1000;
  }

  tick(session) {
    return this.#serialize(() => {
      if (this.#session !== session || !session.generation || this.clock() < session.nextHeartbeat) return;
      const routes = this.#slots.flatMap(slot =>
        slot && !this.#acknowledged.has(slot.route.route_id) ? [slot.route.route_id] : []);
      const occupancy = this.#slots.length === 1 ? {slot_occupied: routes.length !== 0} : {occupied_routes: routes};
      this.#emit(session, {event: 'heartbeat', ...occupancy, received_sequence: session.received,
        ...(this.meter?.exhausted ? {meter_exhausted: true} : {})});
      session.nextHeartbeat += 1000;
      if (session.nextHeartbeat <= this.clock()) session.nextHeartbeat = this.clock() + 1000;
    });
  }

  #emitCurrent(payload) {
    if (this.#session?.generation) this.#emit(this.#session, payload);
  }

  #emit(session, payload) {
    if (this.#session !== session) return false;
    const frames = fragment(envelope(session.generation, session.sent + 1, 'event', payload));
    for (const frame of frames) {
      this.send(session, encodeEnvelope(frame));
      session.sent = frame.sequence;
    }
    return true;
  }

  async #command(session, {id, action, payload}) {
    if (typeof id !== 'string' || !id || !record(payload)) throw new ProtocolError('invalid command');
    const accept = (result) => this.#emit(session, {reply: id, result});
    const refuse = (error_type, error) => this.#emit(session, {reply: id, error_type, error});
    switch (action) {
      case 'ready':
        if (this.status() === 'starting') await this.#saveControl({...this.#control, state: 'ready'});
        accept({});
        return;
      case 'terminate': {
        if (typeof payload.route_id !== 'string' ||
            !(WORKFLOW_ROUTE.test(payload.route_id) || SUMMARY_ROUTE.test(payload.route_id) ||
              AGENT_ROUTE.test(payload.route_id)) ||
            typeof payload.error_type !== 'string' || typeof payload.error !== 'string') {
          refuse('ValueError', 'Invalid terminate command');
          return;
        }
        const index = this.#slots.findIndex((slot) => slot?.route.route_id === payload.route_id);
        const slot = this.#slots[index];
        if (!slot || slot.state === 'terminal' || slot.state === 'stopping') {
          accept({received: true});
          return;
        }
        const entering = slot.state === 'entering';
        if (entering && !(this.tenantSlots && slot.route.kind === 'workflow')) this.#stop(slot.route);
        try {
          accept({received: true});
        } finally {
          await this.#settle(index, slot.route, failure(entering ? 'uncertain' : 'not_started',
            payload.error_type, payload.error));
        }
        return;
      }
      case 'workflow': {
        if (this.tenantSlots && !slotOrdinal(payload.engine_slot)) {
          refuse('ValueError', 'Invalid workflow engine slot');
          return;
        }
        return this.#dispatch(session, payload, accept, refuse, WORKFLOW_ROUTE,
          (request, handle) => workflowRoute(request, handle, this.tenantSlots ? this.identity.incarnationId : null));
      }
      case 'prepare_workflow': {
        const {route_id, execution_id, org_id, engine_slot, prepare_by, warm_until} = payload;
        const queuePrewarm = prepare_by !== undefined || warm_until !== undefined;
        if (!this.prepareHost || typeof route_id !== 'string' || !PREPARE_ROUTE.test(route_id) ||
            typeof execution_id !== 'string' || !UUID_RE.test(execution_id) ||
            (this.tenantSlots && !slotOrdinal(engine_slot)) ||
            (queuePrewarm && (!this.tenantSlots || !Number.isSafeInteger(prepare_by) ||
              !Number.isSafeInteger(warm_until) || warm_until < prepare_by || warm_until - prepare_by > 300_000)) ||
            !(org_id === null || (typeof org_id === 'string' && UUID_RE.test(org_id)))) {
          refuse('ValueError', 'Invalid Cloudflare workflow preparation');
          return;
        }
        // The original in-flight call owns this route's observation.
        if (this.#preparations.has(route_id) || this.meter?.hasRoute(route_id)) {
          accept({});
          return;
        }
        const remaining = queuePrewarm ? Math.min(19_000, prepare_by - this.clock()) : 19_000;
        if (this.retired || remaining <= 0 || this.meter?.exhausted || this.#preparations.size >= this.#slots.length) {
          this.meter?.recordRoute(route_id, []);
          accept({});
          return;
        }
        const controller = new AbortController();
        this.#preparations.set(route_id, controller);
        const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(remaining)]);
        this.background((async () => {
          let invocations;
          try {
            invocations = await this.prepareHost({
              orgId: org_id, executionId: execution_id, signal,
              ...(queuePrewarm ? {prepareBy: prepare_by, warmUntil: warm_until} : {}),
              ...(this.tenantSlots ? {engineSlot: engine_slot, incarnationId: this.identity.incarnationId,
                fence: route_id.slice('prepare:'.length)} : {}),
              reserveIdentity: (identity) => this.#requestControl({route_id}, 'cloudflare_generation_request',
                {identity}, AbortSignal.any([signal, AbortSignal.timeout(4000)])),
            });
          } catch {
            // Missing observations keep their full reservation. The pending
            // workflow can still take its normal confirmed-warm path.
          }
          await this.#serialize(() => {
            this.#preparations.delete(route_id);
            if (invocations) this.meter?.recordRoute(route_id, invocations);
            accept({});
          });
        })());
        return;
      }
      case 'capacity_canary':
        if (!this.runCapacityCanary) {
          accept({failure: {stage: 'launcher', error_class: 'unavailable'}});
          return;
        }
        if (!/^[a-f0-9]{64}$/.test(payload.proof ?? '')) {
          accept({failure: {stage: 'proof', error_class: 'invalid_command'}});
          return;
        }
        this.background((async () => {
          let failure;
          try {
            failure = await this.runCapacityCanary(session.generation, payload.proof);
          } catch {
            failure = {stage: 'launcher', error_class: 'unexpected'};
          }
          accept(failure ? {failure} : {});
        })());
        return;
      case 'cloudflare_summary':
        if (!this.runSummary) {
          refuse('ValueError', 'Cloudflare summaries are disabled');
          return;
        }
        return this.#dispatch(session, payload, accept, refuse, SUMMARY_ROUTE, summaryRoute);
      case 'cloudflare_agent':
        if (!this.runAgent) {
          refuse('ValueError', 'Cloudflare autonomous agents are disabled');
          return;
        }
        return this.#dispatch(session, payload, accept, refuse, AGENT_ROUTE, agentRoute);
      case 'cloudflare_agent_reply':
      case 'cloudflare_generation_reply': {
        const request = this.#agentRequests.get(payload.request_id);
        const reply = action === 'cloudflare_agent_reply' ? 'cloudflare_agent_request' : 'cloudflare_generation_request';
        if (request?.event.route_id === payload.route_id && request.event.event === reply) {
          if (payload.ok === true || payload.result?.refused === true) request.resolve(payload.result);
          else request.reject(new Error('Cloudflare agent request refused'));
        }
        accept({});
        return;
      }
      default:
        refuse('ValueError', 'Unknown launcher command');
    }
  }

  async #dispatch(session, payload, accept, refuse, routePattern, makeRoute) {
    if (typeof payload.route_id !== 'string' || !routePattern.test(payload.route_id)) {
      refuse('ValueError', 'Invalid execution route identity');
      return;
    }
    const routeKey = `seen:${payload.route_id}`;
    if (this.#acknowledged.has(payload.route_id) || await this.storage.get(routeKey)) {
      throw new ProtocolError('execution route already seen');
    }
    const route = makeRoute(payload, this.newId(), this.clock());
    if (!route) {
      refuse('ValueError', 'Invalid Cloudflare dispatch');
      return;
    }
    const index = this.#slots.findIndex((slot) => !slot);
    if (index === -1) {
      refuse('MemoryError', 'No Cloudflare workflow slot available');
      return;
    }
    if (route.kind === 'workflow' && this.#slots.some((slot) => slot?.route.kind === 'workflow' &&
        (slot.state !== 'terminal' || slot.terminal.attempt_state === 'uncertain') &&
        (!this.tenantSlots || slot.route.engine_slot === route.engine_slot) &&
        (slot.route.active_execution.org_id ?? null) === (route.active_execution.org_id ?? null))) {
      refuse('MemoryError', 'This Cloudflare workflow slot is already held');
      return;
    }
    if (this.#session !== session) return;
    if (this.meter?.exhausted) {
      refuse(METER_ERROR, 'Cloudflare launcher meter limit reached before dispatch');
      return;
    }
    try {
      // A durable intent fences an interruption before the slot is saved.
      await this.storage.put(routeKey, true, 'workload');
      await this.#saveSlot(index, {route, state: 'accepted'});
    } catch (error) {
      if (error?.name !== METER_ERROR) throw error;
      refuse(METER_ERROR, 'Cloudflare launcher meter limit reached before dispatch');
      return;
    }
    let replied = false;
    try {
      replied = accept({runtime_handle: route.runtime_handle});
    } finally {
      if (replied) {
        this.background(this.#execute(index, route, payload.context));
      } else {
        await this.#settle(index, route, failure('not_started', 'ControlDisconnected', 'Workflow reply was not sent'));
      }
    }
  }

  #expired(route) {
    const deadline = route.kind === 'workflow' ? route.workflow_deadline : route.deadline;
    return deadline !== null && this.clock() >= Date.parse(deadline);
  }

  async #execute(index, route, context) {
    const work = await this.#serialize(async () => {
      const slot = this.#slots[index];
      if (slot?.route !== route || slot.state !== 'accepted') return null;
      if (this.#expired(route)) {
        await this.#settle(index, route, timeout(route));
        return null;
      }
      if (this.retired) {
        await this.#settle(index, route, failure('not_started', 'LauncherRetired', 'The incarnation ended'));
        return null;
      }
      try {
        await this.#saveSlot(index, {route, state: 'entering'});
      } catch (error) {
        if (error?.name !== METER_ERROR) throw error;
        await this.#settle(index, route, failure('not_started', METER_ERROR, 'Cloudflare launcher meter limit reached'));
        return null;
      }
      if (this.#expired(route)) {
        await this.#settle(index, route, timeout(route));
        return null;
      }
      const controller = new AbortController();
      this.#aborts.set(route.route_id, controller);
      // Start the RPC in this turn, but leave the control queue available for
      // heartbeat, terminate and acknowledgement while the host is busy.
      return {outcome: this.#hostOutcome(route, context, controller.signal)};
    });
    if (!work) return;
    const outcome = await work.outcome;
    await this.#serialize(async () => {
      this.#aborts.delete(route.route_id);
      const slot = this.#slots[index];
      if (!this.#released && slot?.route === route && slot.state === 'entering') {
        await this.#settle(index, route, outcome);
      }
    });
  }

  async #hostOutcome(route, context, signal) {
    try {
      const outcome = route.kind === 'workflow' ? await this.runHost(route, context, signal,
        (identity) => this.#requestControl(route, 'cloudflare_generation_request', {identity},
          AbortSignal.any([signal, AbortSignal.timeout(4000)]))) :
        route.kind === 'agent_run' ? await this.runAgent(route,
          (operation, data, active) => this.#requestAgent(route, operation, data, active), signal) :
          await this.runSummary(route, context, signal);
      if (STATES.has(outcome?.attempt_state) && typeof outcome.result?.success === 'boolean' &&
          (!outcome.result.success || outcome.attempt_state === 'completed')) return outcome;
    } catch {
      // The RPC may have started work even if its eventual reply failed.
    }
    if (route.kind === 'agent_summary') {
      return failure('uncertain', 'SummaryOutcomeUnknown', 'The provider call did not confirm an outcome');
    }
    if (route.kind === 'agent_run') {
      return failure('uncertain', 'AgentOutcomeUnknown', 'The agent did not confirm an outcome');
    }
    return failure('uncertain', 'TenantHostOutcomeUnknown', 'TenantHost did not confirm the workflow outcome');
  }

  #stop(route) {
    this.#aborts.get(route.route_id)?.abort();
    if (route.kind !== 'workflow') return;
    const stop = this.stopHost(route).catch(() => {
      // API fencing and the retained uncertain terminal cover a lost stop.
    }).finally(() => this.#stops.delete(stop));
    this.#stops.add(stop);
    this.background(stop);
  }

  async #confirmStop(route) {
    this.#aborts.get(route.route_id)?.abort();
    let timer;
    try {
      return await Promise.race([
        this.stopHost(route, true).then(value => value === true, () => false),
        new Promise(resolve => { timer = setTimeout(() => resolve(false), STOP_SETTLEMENT_MS); }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  #observeStop(index, stopping) {
    this.background(this.#confirmStop(stopping.route).then(confirmed => this.#serialize(async () => {
      if (this.#released || this.#slots[index] !== stopping) return;
      const outcome = stopping.outcome;
      await this.#journalTerminal(index, stopping.route, {
        ...outcome, attempt_state: confirmed ? 'stopped' : 'uncertain',
      });
    })));
  }

  async #settle(index, route, outcome) {
    if (this.tenantSlots && route.kind === 'workflow' && outcome.attempt_state === 'uncertain') {
      const stopping = {route, state: 'stopping', outcome};
      await this.#saveSlot(index, stopping);
      this.#observeStop(index, stopping);
      return;
    }
    await this.#journalTerminal(index, route, outcome);
  }

  async #journalTerminal(index, route, {attempt_state, result, invocations}) {
    const event = route.kind === 'workflow' ? 'result' : route.kind === 'agent_run' ? 'cloudflare_agent_result' : 'summary_result';
    if (this.meter?.log()) console.info('launcher_workload_terminal', route.kind, attempt_state);
    const terminal = {event, route_id: route.route_id, runtime_handle: route.runtime_handle,
      _event_id: this.newId(), attempt_state, result};
    if (this.meter) terminal.usage = this.meter.snapshot();
    await this.#saveSlot(index, {route, state: 'terminal', terminal, ...(invocations ? {invocations} : {})});
    this.#emitCurrent(terminal);
  }

  async #requestAgent(route, operation, data, signal) {
    return this.#requestControl(route, 'cloudflare_agent_request', {operation, data}, signal);
  }

  async #requestControl(route, kind, fields, signal) {
    signal.throwIfAborted();
    const id = this.newId();
    const event = {event: kind, request_id: id,
      route_id: route.route_id, runtime_handle: route.runtime_handle, ...fields};
    const reply = Promise.withResolvers();
    const abort = () => reply.reject(signal.reason);
    this.#agentRequests.set(id, {event, ...reply});
    signal.addEventListener('abort', abort, {once: true});
    try {
      this.background(this.#serialize(() => {
        if (this.#agentRequests.has(id)) this.#emitCurrent(event);
      }));
      return await reply.promise;
    } finally {
      signal.removeEventListener('abort', abort);
      this.#agentRequests.delete(id);
    }
  }
}
