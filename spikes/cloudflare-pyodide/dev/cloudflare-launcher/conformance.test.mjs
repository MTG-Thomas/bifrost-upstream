// protocol.mjs against vectors produced by upstream's Python protocol.py.
// Regenerate with vectors.py
// after fetching upstream; a changed vector means protocol.mjs must follow.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {
  CLOUDFLARE_HANDLERS, CLOUDFLARE_INCARNATION_ENDED, CLOUDFLARE_MAX_LOGICAL_BYTES,
  FragmentAssembler, MAX_FRAME_BYTES, ProtocolError, canonicalHello,
  encodeEnvelope, envelope, fragment, parseEnvelope, signHello,
} from './protocol.mjs';

const vectors = JSON.parse(readFileSync(new URL('./vectors.json', import.meta.url), 'utf8'));

for (const [set, cases] of [['main', vectors.hello_main], ['branch+profile', vectors.hello_branch_profile]]) {
  test(`hello canonical bytes and proof match Python (${set})`, async () => {
    for (const vector of cases) {
      assert.equal(canonicalHello(vector.input), vector.canonical);
      assert.deepEqual(await signHello(vector.secret, vector.input), vector.signed);
    }
  });
}

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

test('Cloudflare wire limits and retirement code match Python', () => {
  assert.equal(CLOUDFLARE_MAX_LOGICAL_BYTES, vectors.cloudflare_wire.max_logical_bytes);
  assert.equal(CLOUDFLARE_INCARNATION_ENDED, vectors.cloudflare_wire.incarnation_ended);
});

test('reassembly rejects fragments once their decoded total exceeds the logical limit', () => {
  const assembler = new FragmentAssembler();
  const bytesPerFrame = Math.floor(((MAX_FRAME_BYTES - 1024) * 3) / 4);
  const total = Math.floor(CLOUDFLARE_MAX_LOGICAL_BYTES / bytesPerFrame) + 1;
  const data = Buffer.alloc(bytesPerFrame).toString('base64');
  const frame = (index) => envelope(1, index + 1, 'fragment', {message_id: 'large', index, total, data});
  for (let index = 0; index < total - 1; index++) assert.equal(assembler.feed(frame(index)), null);
  assert.throws(() => assembler.feed(frame(total - 1)), /logical limit/);
});

test('fragments cannot substitute a different logical generation or sequence', () => {
  const frames = fragment(envelope(2, 3, 'command', {blob: 'x'.repeat(800_000)}));
  const assembler = new FragmentAssembler();
  const outer = frames.map((frame) => ({...frame, session_generation: 3}));
  assembler.feed(outer[0]);
  assert.throws(() => assembler.feed(outer[1]), /invalid reassembled control envelope/);
});
