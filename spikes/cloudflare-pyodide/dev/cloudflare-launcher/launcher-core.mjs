import {
  CLOUDFLARE_HANDLERS, CLOUDFLARE_INCARNATION_ENDED, CLOUDFLARE_PYTHON_RUNTIME_PROFILE,
  CREDENTIAL_PROTOCOL, FragmentAssembler, ProtocolError, encodeEnvelope, envelope, fragment,
  newNonce, parseEnvelope, signHello,
} from './protocol.mjs';

const CONTROL = 'control';
const SLOT = 'slot';
const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const ROUTE = new RegExp(`^workflow:(${UUID}):${UUID}$`);
const INSTANT = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|[+-]\d\d:\d\d)$/;
const HOSTNAME = /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const PYTHON_PATH = /^(?:[a-zA-Z_][a-zA-Z0-9_]*\/)*[a-zA-Z_][a-zA-Z0-9_]*\.py$/;
const HEX64 = /^[0-9a-f]{64}$/;
const STATES = new Set(['not_started', 'completed', 'uncertain']);
const record = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const integer = (value) => Number.isSafeInteger(value) && value >= 0;
const instant = (value) => typeof value === 'string' && INSTANT.test(value) && Number.isFinite(Date.parse(value));

function validWorkspace(workspace, entry) {
  if (!record(workspace) || !record(workspace.manifest) || !record(workspace.sources) ||
      !Object.hasOwn(workspace.manifest, entry) || !Array.isArray(workspace.packages) ||
      !workspace.packages.every((name) => typeof name === 'string') ||
      !Array.isArray(workspace.https_hosts) ||
      !workspace.https_hosts.every((host) => typeof host === 'string' && host.length <= 253 && HOSTNAME.test(host)) ||
      !integer(workspace.subrequest_allowance) || workspace.subrequest_allowance > 50 ||
      !integer(workspace.cpu_budget_ms) || workspace.cpu_budget_ms === 0) return false;

  const paths = Object.keys(workspace.manifest);
  return paths.length === Object.keys(workspace.sources).length && paths.every((path) =>
    PYTHON_PATH.test(path) && HEX64.test(workspace.manifest[path]) &&
    Object.hasOwn(workspace.sources, path) && typeof workspace.sources[path] === 'string');
}

function workflowRoute(payload, runtimeHandle) {
  const {route_id, execution_id, active_execution, started_at, timeout_seconds,
    workflow_deadline, memory_request_bytes, context} = payload;
  if (typeof route_id !== 'string' || ROUTE.exec(route_id)?.[1] !== execution_id ||
      !record(active_execution) || active_execution.execution_id !== execution_id ||
      typeof active_execution.sync !== 'boolean' ||
      (active_execution.org_id != null && typeof active_execution.org_id !== 'string') ||
      !instant(started_at) || !integer(timeout_seconds) ||
      !integer(memory_request_bytes) || memory_request_bytes > 128 * 2 ** 20 ||
      (timeout_seconds === 0 ? workflow_deadline !== null :
        !instant(workflow_deadline) || Date.parse(workflow_deadline) - Date.parse(started_at) !== timeout_seconds * 1000) ||
      !record(context) || context.workflow_deadline !== workflow_deadline ||
      context.code || context.solution_id != null || context.artifact_workspace_id != null ||
      typeof context.engine_token !== 'string' || !context.engine_token ||
      !validWorkspace(context.cloudflare_workspace, context.file_path)) return null;

  return {kind: 'workflow', route_id, subject_id: execution_id, runtime_handle: runtimeHandle,
    active_execution, started_at, timeout_seconds, workflow_deadline, memory_request_bytes};
}

function failure(attemptState, errorType, error) {
  return {attempt_state: attemptState, result: {success: false, error_type: errorType, error}};
}

function timeout() {
  return failure('not_started', 'TimeoutError', 'Workflow deadline elapsed before engine entry');
}

// Storage writes resolve only after confirmation. The slot holds either an
// accepted route or that route and its terminal. The latter is one write and
// stays in the slot until the API acknowledges that exact event.
export class LauncherCore {
  #control;
  #slot;
  #session = null;
  #tail = Promise.resolve();
  #abort = null;
  #released = false;

  constructor({identity, secret, clock, storage, send, runHost, stopHost, background, newId = newNonce}) {
    this.identity = identity;
    this.secret = secret;
    this.clock = clock;
    this.storage = storage;
    this.send = send;
    this.runHost = runHost;
    this.stopHost = stopHost;
    this.background = background;
    this.newId = newId;
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
      return !this.retired;
    });
  }

  isStopped() {
    return this.#serialize(async () => this.retired || (await this.storage.get(CONTROL))?.state === 'retired');
  }

  release() {
    return this.#serialize(async () => {
      if (this.#released) return;
      const slot = this.#slot ?? await this.storage.get(SLOT);
      this.#released = true;
      this.#session = null;
      this.#slot = null;
      if (slot?.state === 'entering') this.#stop(slot.route);
    });
  }

  #serialize(fn) {
    const task = this.#tail.then(fn);
    this.#tail = task.catch(() => {});
    return task;
  }

  async #saveControl(control) {
    await this.storage.put(CONTROL, control);
    this.#control = control;
  }

  async #saveSlot(slot) {
    await this.storage.put(SLOT, slot);
    this.#slot = slot;
  }

  async #load() {
    if (this.#control) return;
    const control = await this.storage.get(CONTROL);
    this.#slot = await this.storage.get(SLOT);
    if (this.#slot && this.#slot.state !== 'terminal') {
      const entering = this.#slot.state === 'entering';
      if (entering) this.#stop(this.#slot.route);
      const outcome = this.#expired(this.#slot.route) && !entering ? timeout() :
        failure(entering ? 'uncertain' : 'not_started', 'LauncherReset', 'Launcher reset before a terminal was recorded');
      await this.#settle(this.#slot.route, outcome);
    }
    this.#control = control ?? {helloSequence: 0, generation: 0, state: 'starting'};
  }

  beginDial() {
    return this.#serialize(async () => {
      if (this.#released) return null;
      await this.#load();
      if (this.retired || this.#session) return null;
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
      return {session, hello: JSON.stringify({event: 'hello', hello, handlers: CLOUDFLARE_HANDLERS,
        credential_protocol: CREDENTIAL_PROTOCOL, version: this.identity.version})};
    });
  }

  closed(session, code) {
    if (this.#session !== session) return Promise.resolve();
    this.#session = null;
    return this.#serialize(async () => {
      if (code === CLOUDFLARE_INCARNATION_ENDED) {
        await this.#saveControl({...this.#control, state: 'retired'});
        const slot = this.#slot;
        if (slot?.state === 'entering') {
          this.#stop(slot.route);
          await this.#settle(slot.route, failure('uncertain', 'LauncherRetired', 'The incarnation ended'));
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
      if (this.#slot?.state === 'terminal' && this.#slot.terminal._event_id === message.payload.event_id) {
        await this.storage.delete(SLOT);
        this.#slot = null;
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
    const slot = this.#slot;
    const terminal = slot?.terminal;
    this.#emit(session, {event: 'inventory', inventory: {
      version: 1, routes: slot && !terminal ? [slot.route] : [],
      terminal_routes: terminal ? [{...slot.route, event_id: terminal._event_id,
        attempt_state: terminal.attempt_state}] : [],
    }});
    if (terminal) this.#emit(session, terminal);
    session.nextHeartbeat = this.clock() + 1000;
  }

  tick(session) {
    return this.#serialize(() => {
      if (this.#session !== session || !session.generation || this.clock() < session.nextHeartbeat) return;
      this.#emit(session, {event: 'heartbeat'});
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
        const slot = this.#slot;
        if (typeof payload.route_id !== 'string' || !ROUTE.test(payload.route_id) ||
            typeof payload.error_type !== 'string' || typeof payload.error !== 'string') {
          refuse('ValueError', 'Invalid terminate command');
          return;
        }
        if (slot?.route.route_id !== payload.route_id || slot.state === 'terminal') {
          accept({received: true});
          return;
        }
        const entering = slot.state === 'entering';
        if (entering) this.#stop(slot.route);
        try {
          accept({received: true});
        } finally {
          await this.#settle(slot.route, failure(entering ? 'uncertain' : 'not_started',
            payload.error_type, payload.error));
        }
        return;
      }
      case 'workflow':
        return this.#workflow(session, payload, accept, refuse);
      default:
        refuse('ValueError', 'Unknown launcher command');
    }
  }

  async #workflow(session, payload, accept, refuse) {
    // A tombstone contains only identity. It fences refused and settled routes
    // across generations, including interruption between intent and slot writes.
    if (typeof payload.route_id !== 'string' || !ROUTE.test(payload.route_id)) {
      refuse('ValueError', 'Invalid workflow route identity');
      return;
    }
    const routeKey = `seen:${payload.route_id}`;
    if (await this.storage.get(routeKey)) throw new ProtocolError('workflow route already seen');
    await this.storage.put(routeKey, true);
    const route = workflowRoute(payload, this.newId());
    if (!route) {
      refuse('ValueError', 'Invalid Cloudflare workflow dispatch');
      return;
    }
    if (this.#slot) {
      refuse('MemoryError', 'No Cloudflare workflow slot available');
      return;
    }
    if (this.#session !== session) return;
    await this.#saveSlot({route, state: 'accepted'});
    let replied = false;
    try {
      replied = accept({runtime_handle: route.runtime_handle});
    } finally {
      if (replied) {
        this.background(this.#execute(route, payload.context));
      } else {
        await this.#settle(route, failure('not_started', 'ControlDisconnected', 'Workflow reply was not sent'));
      }
    }
  }

  #expired(route) {
    return route.workflow_deadline !== null && this.clock() >= Date.parse(route.workflow_deadline);
  }

  async #execute(route, context) {
    const work = await this.#serialize(async () => {
      if (this.#slot?.route !== route || this.#slot.state !== 'accepted') return null;
      if (this.#expired(route)) {
        await this.#settle(route, timeout());
        return null;
      }
      if (this.retired) {
        await this.#settle(route, failure('not_started', 'LauncherRetired', 'The incarnation ended'));
        return null;
      }
      await this.#saveSlot({route, state: 'entering'});
      if (this.#expired(route)) {
        await this.#settle(route, timeout());
        return null;
      }
      const controller = new AbortController();
      this.#abort = controller;
      // Start the RPC in this turn, but leave the control queue available for
      // heartbeat, terminate and acknowledgement while the host is busy.
      return {controller, outcome: this.#hostOutcome(route, context, controller.signal)};
    });
    if (!work) return;
    const outcome = await work.outcome;
    await this.#serialize(async () => {
      if (this.#abort === work.controller) this.#abort = null;
      if (!this.#released && this.#slot?.route === route && this.#slot.state === 'entering') {
        await this.#settle(route, outcome);
      }
    });
  }

  async #hostOutcome(route, context, signal) {
    try {
      const outcome = await this.runHost(route, context, signal);
      if (STATES.has(outcome?.attempt_state) && typeof outcome.result?.success === 'boolean' &&
          (!outcome.result.success || outcome.attempt_state === 'completed')) return outcome;
    } catch {
      // The RPC may have started work even if its eventual reply failed.
    }
    return failure('uncertain', 'TenantHostOutcomeUnknown', 'TenantHost did not confirm the workflow outcome');
  }

  #stop(route) {
    this.#abort?.abort();
    this.background(this.stopHost(route).catch(() => {
      // API fencing and the retained uncertain terminal cover a lost stop.
    }));
  }

  async #settle(route, {attempt_state, result}) {
    const terminal = {event: 'result', route_id: route.route_id, runtime_handle: route.runtime_handle,
      _event_id: this.newId(), attempt_state, result};
    await this.#saveSlot({route, state: 'terminal', terminal});
    this.#emitCurrent(terminal);
  }
}
