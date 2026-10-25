import test from 'node:test';
import assert from 'node:assert/strict';
import { generateRecords } from '../pipeline/fake-data.mjs';
import { validateRecord } from '../pipeline/validate.mjs';
import { createMemoryStore, processRecords } from '../pipeline/process.mjs';

// Plan criterion 3: 1,000 fake records with 50 bad ones and 20 duplicates must give
// exactly 930 valid, 50 rejected and 20 skipped as duplicates, with an empty
// dead-letter queue. The expected counts are written here first, before the code.
const EXPECTED = { valid: 930, rejected: 50, duplicate: 20 };

test('P1: the fake data has 1,000 records and its manifest states the expected counts', () => {
  const { records, expected } = generateRecords(1);
  assert.equal(records.length, 1000);
  assert.deepEqual(expected, EXPECTED);
});

test('P2: the same seed gives the same records, another seed gives different ones', () => {
  assert.deepEqual(generateRecords(7).records, generateRecords(7).records);
  assert.notDeepEqual(generateRecords(7).records, generateRecords(8).records);
});

test('P3: a good record is valid', () => {
  const good = { id: 'rec-0001', customer: 'cust-12', amount: 12.5, currency: 'USD', timestamp: '2026-01-02T03:04:05Z' };
  assert.deepEqual(validateRecord(good), { ok: true, errors: [] });
});

for (const [name, bad, error] of [
  ['missing id', { customer: 'c', amount: 1, currency: 'USD', timestamp: '2026-01-02T03:04:05Z' }, 'id'],
  ['negative amount', { id: 'rec-1', customer: 'c', amount: -5, currency: 'USD', timestamp: '2026-01-02T03:04:05Z' }, 'amount'],
  ['unknown currency', { id: 'rec-1', customer: 'c', amount: 1, currency: 'XXX', timestamp: '2026-01-02T03:04:05Z' }, 'currency'],
  ['bad timestamp', { id: 'rec-1', customer: 'c', amount: 1, currency: 'USD', timestamp: 'not a date' }, 'timestamp'],
  ['not an object', 'garbage', 'record'],
  ['null', null, 'record'],
]) {
  test(`P4: ${name} is rejected and names the field`, () => {
    const result = validateRecord(bad);
    assert.equal(result.ok, false);
    assert.ok(result.errors.includes(error), `errors ${JSON.stringify(result.errors)} should name ${error}`);
  });
}

test('P5: the 1,000-record run gives exactly 930 valid, 50 rejected, 20 duplicates and an empty dead-letter queue', async () => {
  const { records } = generateRecords(1);
  const store = createMemoryStore();
  const run = await processRecords(records, { store });
  assert.deepEqual(run.counts, { ...EXPECTED, deadLettered: 0 });
  assert.deepEqual(run.deadLetter, []);
});

test('P6: arrival order does not change the counts (out-of-order delivery)', async () => {
  const { records } = generateRecords(1);
  const run = await processRecords([...records].reverse(), { store: createMemoryStore() });
  assert.deepEqual(run.counts, { ...EXPECTED, deadLettered: 0 });
});

test('P7: sending the same batch again skips every valid record and re-rejects the bad ones', async () => {
  const { records } = generateRecords(1);
  const store = createMemoryStore();
  await processRecords(records, { store });
  const again = await processRecords(records, { store });
  assert.deepEqual(again.counts, { valid: 0, rejected: 50, duplicate: 950, deadLettered: 0 });
});

test('P8: each record keeps its result (valid, rejected or duplicate) and its path', async () => {
  const { records } = generateRecords(1);
  const store = createMemoryStore();
  const run = await processRecords(records, { store });
  assert.equal(run.results.length, 1000);
  for (const r of run.results) {
    assert.ok(['valid', 'rejected', 'duplicate'].includes(r.status), r.status);
    assert.ok(Array.isArray(r.path) && r.path[0] === 'received', 'path starts with received');
    assert.equal(r.path.at(-1), r.status);
  }
});

test('P9: a record that fails twice then succeeds is valid, retried, and not dead-lettered', async () => {
  const { records } = generateRecords(1);
  const one = records.find((r) => validateRecord(r).ok);
  let calls = 0;
  const handler = async () => { calls += 1; if (calls < 3) throw new Error('temporary'); };
  const run = await processRecords([one], { store: createMemoryStore(), handler, maxAttempts: 3 });
  assert.deepEqual(run.counts, { valid: 1, rejected: 0, duplicate: 0, deadLettered: 0 });
  assert.equal(run.results[0].attempts, 3);
  assert.deepEqual(run.deadLetter, []);
});

test('P10: a message that always fails goes to the dead-letter queue after maxAttempts', async () => {
  const { records } = generateRecords(1);
  const one = records.find((r) => validateRecord(r).ok);
  const handler = async () => { throw new Error('always'); };
  const run = await processRecords([one], { store: createMemoryStore(), handler, maxAttempts: 3 });
  assert.equal(run.counts.deadLettered, 1);
  assert.equal(run.counts.valid, 0);
  assert.equal(run.deadLetter.length, 1);
  assert.equal(run.deadLetter[0].id, one.id);
  assert.equal(run.deadLetter[0].attempts, 3);
});

test('P11: a handler that runs past the timeout counts as a failure and is dead-lettered with the reason', async () => {
  const { records } = generateRecords(1);
  const one = records.find((r) => validateRecord(r).ok);
  const handler = () => new Promise(() => {});
  const run = await processRecords([one], { store: createMemoryStore(), handler, maxAttempts: 2, timeoutMs: 20 });
  assert.equal(run.deadLetter.length, 1);
  assert.equal(run.deadLetter[0].reason, 'timeout');
});

test('P12: a dead-lettered record is not marked as seen, so a later clean retry can succeed', async () => {
  const { records } = generateRecords(1);
  const one = records.find((r) => validateRecord(r).ok);
  const store = createMemoryStore();
  await processRecords([one], { store, handler: async () => { throw new Error('x'); }, maxAttempts: 1 });
  const retry = await processRecords([one], { store });
  assert.deepEqual(retry.counts, { valid: 1, rejected: 0, duplicate: 0, deadLettered: 0 });
});

test('P13: an id that is already stored is skipped before any work runs', async () => {
  const { records } = generateRecords(1);
  const one = records.find((r) => validateRecord(r).ok);
  const store = createMemoryStore();
  await store.put(one.id);
  let calls = 0;
  const handler = async () => { calls += 1; };
  const run = await processRecords([one], { store, handler });
  assert.equal(calls, 0, 'the work handler must not run for a duplicate');
  assert.deepEqual(run.counts, { valid: 0, rejected: 0, duplicate: 1, deadLettered: 0 });
});
