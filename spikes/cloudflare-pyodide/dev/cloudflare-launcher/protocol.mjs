// Launcher side of upstream Bifrost's remote-control protocol, for a launcher
// that runs on Cloudflare. Mirrors api/src/worker/remote/protocol.py on
// upstream main (hello signing, envelopes, fragments) and #1424's
// api/shared/worker_control_auth.py (derived external worker keys).
// conformance.test.mjs checks every function against vectors produced by the
// Python modules themselves (vectors.py), so the two cannot drift silently.
//
// Uses only Web Crypto and TextEncoder: runs unchanged in Workers and Node.

export const AUTH_DOMAIN = 'bifrost.remote-control.hello.v1\0';
export const EXTERNAL_CONTROL_DOMAIN = 'bifrost.external-worker.control.v1\0';
export const MAX_FRAME_BYTES = 1 << 20;
export const MAX_CLOCK_SKEW_SECONDS = 60;
export const CREDENTIAL_PROTOCOL = 2;
export const CLOUDFLARE_PYTHON_RUNTIME_PROFILE = 'cloudflare-python-v1';
// The only handler this launcher executes. Advertising anything else would let
// the API route agent runs, summaries or platform jobs here.
export const CLOUDFLARE_HANDLERS = Object.freeze([Object.freeze(['workflow.execute', 1])]);

const encoder = new TextEncoder();
const RESOURCE_UID = /^[a-zA-Z0-9._:-]{1,255}$/;
const RUNTIME_PROFILE = /^[a-z][a-z0-9._-]{0,63}$/;

export class ProtocolError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ProtocolError';
  }
}

// Python's json.dumps(..., separators=(",", ":")) with ensure_ascii=True:
// non-ASCII characters become \uXXXX escapes (UTF-16 code units, lowercase hex).
export function pythonJson(value, {sortKeys = false} = {}) {
  const normalize = (v) => {
    if (Array.isArray(v)) return v.map(normalize);
    if (v !== null && typeof v === 'object') {
      const keys = sortKeys ? Object.keys(v).sort() : Object.keys(v);
      return Object.fromEntries(keys.map((k) => [k, normalize(v[k])]));
    }
    return v;
  };
  return JSON.stringify(normalize(value)).replace(/[\u007f-￿]/g,
    (ch) => `\\u${ch.charCodeAt(0).toString(16).padStart(4, '0')}`);
}

function hex(buffer) {
  return [...new Uint8Array(buffer)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

async function hmacSha256Hex(key, message) {
  const cryptoKey = await crypto.subtle.importKey('raw', encoder.encode(key),
    {name: 'HMAC', hash: 'SHA-256'}, false, ['sign']);
  return hex(await crypto.subtle.sign('HMAC', cryptoKey, encoder.encode(message)));
}

// Validates a hello the same way Hello.__post_init__ does, for the external
// form this launcher uses (job identities are never claimed here).
export function validateHello(hello) {
  const required = ['launcher_id', 'incarnation_id', 'hostname', 'form', 'issued_at', 'nonce',
    'hello_sequence'];
  for (const name of required) {
    if (hello[name] === undefined || hello[name] === null || hello[name] === '') {
      throw new ProtocolError('hello identity fields are required');
    }
  }
  if (hello.runtime_profile !== undefined && hello.runtime_profile !== null &&
      (typeof hello.runtime_profile !== 'string' || !RUNTIME_PROFILE.test(hello.runtime_profile))) {
    throw new ProtocolError('invalid physical runtime profile');
  }
  if (hello.form !== 'external') throw new ProtocolError('the Cloudflare launcher is an external worker');
  if (hello.job_uid != null || hello.pod_uid != null) {
    throw new ProtocolError('non-Kubernetes hello cannot claim Kubernetes identities');
  }
  if (typeof hello.resource_uid !== 'string' || !RESOURCE_UID.test(hello.resource_uid)) {
    throw new ProtocolError('external hello requires a capacity resource identity');
  }
  if (hello.nonce.length < 24 || hello.nonce.length > 256) {
    throw new ProtocolError('hello nonce must be 24-256 characters');
  }
  if (!Number.isInteger(hello.hello_sequence) || hello.hello_sequence < 1) {
    throw new ProtocolError('hello sequence must be positive');
  }
  if ((hello.execution_lane ?? 'normal') !== 'normal') {
    throw new ProtocolError('External capacity workers only support the Normal lane');
  }
}

// Hello.canonical_bytes(): sorted keys, compact separators. Optional fields
// enter the signed bytes only when present, as in Python.
export function canonicalHello(hello) {
  const fields = {
    incarnation_id: String(hello.incarnation_id),
    issued_at: hello.issued_at,
    form: hello.form,
    job_uid: hello.job_uid ?? null,
    launcher_id: String(hello.launcher_id),
    nonce: hello.nonce,
    pod_uid: hello.pod_uid ?? null,
    hostname: hello.hostname,
    hello_sequence: hello.hello_sequence,
  };
  if ((hello.execution_lane ?? 'normal') !== 'normal') fields.execution_lane = hello.execution_lane;
  if (hello.form === 'external') fields.resource_uid = hello.resource_uid;
  if (hello.runtime_profile != null) fields.runtime_profile = hello.runtime_profile;
  return pythonJson(fields, {sortKeys: true});
}

export async function signHello(secret, hello) {
  validateHello(hello);
  const proof = await hmacSha256Hex(secret, AUTH_DOMAIN + canonicalHello(hello));
  const signed = {
    launcher_id: String(hello.launcher_id),
    incarnation_id: String(hello.incarnation_id),
    job_uid: null,
    pod_uid: null,
    hostname: hello.hostname,
    form: hello.form,
    issued_at: hello.issued_at,
    nonce: hello.nonce,
    hello_sequence: hello.hello_sequence,
    resource_uid: hello.resource_uid,
    ...(hello.runtime_profile != null ? {runtime_profile: hello.runtime_profile} : {}),
    execution_lane: hello.execution_lane ?? 'normal',
    proof,
  };
  return signed;
}

// #1424 shared/worker_control_auth.py::external_worker_control_key.
export async function externalWorkerControlKey(providerKey, workerId, resourceUid) {
  if (typeof resourceUid !== 'string' || !RESOURCE_UID.test(resourceUid)) {
    throw new ProtocolError('Invalid external worker resource identity');
  }
  return hmacSha256Hex(providerKey, EXTERNAL_CONTROL_DOMAIN + pythonJson([String(workerId), resourceUid]));
}

export function newNonce() {
  // secrets.token_urlsafe(32): 32 random bytes, base64url without padding.
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return btoa(String.fromCharCode(...bytes)).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '');
}

// ControlEnvelope: field order session_generation, sequence, kind, payload.
export function envelope(sessionGeneration, sequence, kind, payload) {
  if (!Number.isInteger(sessionGeneration) || sessionGeneration < 1 ||
      !Number.isInteger(sequence) || sequence < 1) {
    throw new ProtocolError('session generation and sequence must be positive');
  }
  if (typeof kind !== 'string' || kind.length === 0 || kind.length > 64) {
    throw new ProtocolError('invalid control message kind');
  }
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new ProtocolError('invalid control envelope');
  }
  return {session_generation: sessionGeneration, sequence, kind, payload};
}

export function encodeEnvelope(value) {
  return pythonJson(envelope(value.session_generation, value.sequence, value.kind, value.payload));
}

export function parseEnvelope(raw) {
  const text = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
  if (encoder.encode(text).length > MAX_FRAME_BYTES) throw new ProtocolError('control frame exceeds limit');
  let value;
  try {
    value = JSON.parse(text);
  } catch {
    throw new ProtocolError('control frame is not JSON');
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new ProtocolError('control frame must be a JSON object');
  }
  return envelope(Number(value.session_generation), Number(value.sequence), String(value.kind), value.payload);
}

const CHUNK_SIZE = Math.floor(((MAX_FRAME_BYTES - 1024) * 3) / 4);

function base64(bytes) {
  let binary = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(binary);
}

// fragment(): one logical envelope as ordered frames, each within the limit.
// Each fragment consumes one sequence number, as in Python.
export function fragment(value, messageId = newNonce().slice(0, 24)) {
  const raw = encoder.encode(encodeEnvelope(value));
  if (raw.length <= CHUNK_SIZE) return [value];
  const total = Math.ceil(raw.length / CHUNK_SIZE);
  const frames = [];
  for (let index = 0; index < total; index++) {
    frames.push(envelope(value.session_generation, value.sequence + index, 'fragment', {
      message_id: messageId, index, total,
      data: base64(raw.subarray(index * CHUNK_SIZE, (index + 1) * CHUNK_SIZE)),
    }));
  }
  return frames;
}

// FragmentAssembler: one bounded, ordered message buffer per connection.
export class FragmentAssembler {
  #messageId = null;
  #total = 0;
  #parts = [];

  feed(value) {
    if (value.kind !== 'fragment') {
      if (this.#messageId !== null) throw new ProtocolError('interleaved control message fragments');
      return value;
    }
    const {message_id: messageId, index, total, data} = value.payload ?? {};
    let bytes;
    try {
      if (typeof messageId !== 'string' || typeof data !== 'string' ||
          !/^[A-Za-z0-9+/]*={0,2}$/.test(data)) {
        throw new Error('shape');
      }
      bytes = Uint8Array.from(atob(data), (c) => c.charCodeAt(0));
    } catch {
      this.#reset();
      throw new ProtocolError('invalid control fragment');
    }
    if (!Number.isInteger(total) || !Number.isInteger(index) || total < 2 || total > 2 ** 31 ||
        index < 0 || index >= total) {
      this.#reset();
      throw new ProtocolError('invalid control fragment sequence');
    }
    if (this.#messageId === null) {
      if (index !== 0) throw new ProtocolError('fragment sequence must start at zero');
      this.#messageId = messageId;
      this.#total = total;
    }
    if (messageId !== this.#messageId || total !== this.#total || index !== this.#parts.length) {
      this.#reset();
      throw new ProtocolError('out-of-order control fragments');
    }
    this.#parts.push(bytes);
    if (this.#parts.length !== total) return null;
    const size = this.#parts.reduce((sum, part) => sum + part.length, 0);
    const joined = new Uint8Array(size);
    let offset = 0;
    for (const part of this.#parts) {
      joined.set(part, offset);
      offset += part.length;
    }
    this.#reset();
    let parsed;
    try {
      parsed = JSON.parse(new TextDecoder().decode(joined));
    } catch {
      throw new ProtocolError('reassembled control message is not JSON');
    }
    return envelope(Number(parsed.session_generation), Number(parsed.sequence), String(parsed.kind),
      parsed.payload);
  }

  #reset() {
    this.#messageId = null;
    this.#total = 0;
    this.#parts = [];
  }
}
