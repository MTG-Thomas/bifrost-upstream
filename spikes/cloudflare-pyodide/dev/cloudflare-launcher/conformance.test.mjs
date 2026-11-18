// protocol.mjs against vectors produced by upstream's Python protocol.py and
// #1424's worker_control_auth.py (vectors.py). Regenerate with vectors.py
// after fetching upstream; a changed vector means protocol.mjs must follow.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {
  CLOUDFLARE_HANDLERS, FragmentAssembler, MAX_FRAME_BYTES, ProtocolError, canonicalHello,
  encodeEnvelope, envelope, externalWorkerControlKey, fragment, parseEnvelope, signHello,
} from './protocol.mjs';

const vectors = JSON.parse(readFileSync(new URL('./vectors.json', import.meta.url), 'utf8'));

test('the phase 3 wire contract makes an inventory omission final', () => {
  const design = readFileSync(new URL('./DESIGN.md', import.meta.url), 'utf8');
  const [, phase3] = design.split('### Phase 3 opaque workflow wire contract');
  assert.ok(phase3, 'Phase 3 wire contract is missing');
  const contract = phase3.split(/\r?\n#/)[0].replace(/\s+/g, ' ');
  const omission = /sends an inventory omitting a workflow route[^.]*must never enter the engine for that route or report it in a later reply, event, or inventory/;
  assert.match(contract, omission);
});

for (const [set, cases] of [['main', vectors.hello_main], ['branch+profile', vectors.hello_branch_profile]]) {
  test(`hello canonical bytes and proof match Python (${set})`, async () => {
    for (const vector of cases) {
      assert.equal(canonicalHello(vector.input), vector.canonical);
      assert.deepEqual(await signHello(vector.secret, vector.input), vector.signed);
    }
  });
}

test('derived external worker keys match #1424', async () => {
  for (const v of vectors.control_keys) {
    assert.equal(await externalWorkerControlKey(v.provider_key, v.worker_id, v.resource_uid), v.control_key);
  }
  await assert.rejects(externalWorkerControlKey('k', 'w', 'bad uid!'), ProtocolError);
});

test('envelope encoding matches Python byte for byte', () => {
  const expected = vectors.envelopes.small.json;
  assert.equal(encodeEnvelope(JSON.parse(expected)), expected);
  assert.deepEqual(parseEnvelope(expected), JSON.parse(expected));
});

test('fragments match Python and reassemble', () => {
  const big = vectors.envelopes.big;
  const value = envelope(big.session_generation, big.sequence, big.kind,
    {id: 'req-1', action: 'workflow',
      payload: {blob: 'x'.repeat(big.payload_spec.x) + 'é'.repeat(big.payload_spec.e_acute)}});
  const frames = fragment(value, big.message_id);
  assert.equal(frames.length, big.frame_count);
  assert.deepEqual(frames.map((f) => f.sequence), big.frame_sequences);
  const encoded = frames.map(encodeEnvelope);
  assert.equal(createHash('sha256').update(encoded.join('\n')).digest('hex'), big.frames_sha256);
  assert.ok(encoded.every((f) => Buffer.byteLength(f) <= MAX_FRAME_BYTES));
  const assembler = new FragmentAssembler();
  assert.equal(assembler.feed(parseEnvelope(encoded[0])), null);
  assert.deepEqual(assembler.feed(parseEnvelope(encoded[1])), value);
});

test('assembler refuses interleaving and out-of-order fragments', () => {
  const value = envelope(1, 1, 'command', {blob: 'y'.repeat(800_000)});
  const [first, second] = fragment(value, 'm1');
  const interleaved = new FragmentAssembler();
  interleaved.feed(first);
  assert.throws(() => interleaved.feed(envelope(1, 9, 'ack', {event_id: 'e'})), /interleaved/);
  const outOfOrder = new FragmentAssembler();
  assert.throws(() => outOfOrder.feed(second), /start at zero/);
});

test('hello refuses identities a Cloudflare launcher must never claim', async () => {
  const base = {...vectors.hello_branch_profile[0].input};
  for (const [field, value] of [['form', 'static'], ['job_uid', 'j'], ['execution_lane', 'fast_path'],
    ['nonce', 'short'], ['runtime_profile', 'Bad Profile'], ['resource_uid', '']]) {
    await assert.rejects(signHello('k', {...base, [field]: value}), ProtocolError, field);
  }
});

test('the launcher advertises workflow execution only', () => {
  assert.deepEqual(CLOUDFLARE_HANDLERS, [['workflow.execute', 1]]);
});
