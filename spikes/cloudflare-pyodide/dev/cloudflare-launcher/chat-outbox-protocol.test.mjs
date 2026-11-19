import assert from 'node:assert/strict';
import test from 'node:test';

// Protocol fixture. The journal mutations represent PostgreSQL transactions;
// project() represents one Lua operation on the existing conversation stream.
function commit(journal, sequence, payload) {
  const existing = journal.rows[sequence - 1];
  if (existing) {
    assert.deepEqual(existing.payload, payload, 'conflicting replay');
    return existing.eventId;
  }
  assert.equal(sequence, journal.rows.length + 1, 'sequence gap');
  const eventId = `chat-event-${sequence}`;
  journal.rows.push({eventId, payload, redisSequence: null});
  return eventId;
}

function project(redis, eventId, payload) {
  if (redis.receipts.has(eventId)) return redis.receipts.get(eventId);
  const sequence = ++redis.conversationSequence;
  redis.stream.push({eventId, sequence, payload});
  redis.receipts.set(eventId, sequence);
  return sequence;
}

function publish(journal, redis, afterProjection = () => {}) {
  const row = journal.rows.find(({redisSequence}) => redisSequence === null);
  if (!row) return;
  const sequence = project(redis, row.eventId, row.payload);
  afterProjection();
  row.redisSequence = sequence;
}

function fixture() {
  return {
    journal: {rows: []},
    redis: {conversationSequence: 0, receipts: new Map(), stream: []},
  };
}

test('a committed chunk survives a publisher restart and a lost Worker ACK', () => {
  const {journal, redis} = fixture();
  const chunk = {type: 'delta', content: 'hello'};
  const id = commit(journal, 1, chunk);
  assert.equal(commit(journal, 1, chunk), id);
  assert.throws(() => commit(journal, 1, {type: 'delta', content: 'changed'}), /conflicting replay/);
  assert.deepEqual(redis.stream, []);
  const recoveredJournal = structuredClone(journal);
  publish(recoveredJournal, redis);
  assert.deepEqual(redis.stream, [{eventId: id, sequence: 1, payload: chunk}]);
  assert.equal(recoveredJournal.rows[0].redisSequence, 1);
});

test('recovery after Redis publication reuses the sequence alongside Linux events', () => {
  const {journal, redis} = fixture();
  project(redis, 'linux-start', {type: 'run_status'});
  const first = commit(journal, 1, {type: 'message_start'});
  const second = commit(journal, 2, {type: 'delta', content: 'hello'});
  assert.throws(() => publish(journal, redis, () => { throw new Error('publisher stopped'); }),
    /publisher stopped/);
  assert.equal(journal.rows[0].redisSequence, null);
  project(redis, 'linux-update', {type: 'run_status'});
  publish(journal, redis);
  publish(journal, redis);
  assert.deepEqual(journal.rows.map(({redisSequence}) => redisSequence), [2, 4]);
  assert.deepEqual(redis.stream.map(({eventId, sequence}) => [eventId, sequence]),
    [['linux-start', 1], [first, 2], ['linux-update', 3], [second, 4]]);
});
