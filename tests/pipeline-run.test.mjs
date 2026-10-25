import test from 'node:test';
import assert from 'node:assert/strict';
import { generateRecords } from '../pipeline/fake-data.mjs';
import { buildSendBatches, countStatuses, judgeRun } from '../pipeline/run-pipeline.mjs';

// Plan step 5: send the 1,000 fake records, then judge the run. The pass rule is the
// one in criterion 3: 930 valid, 50 rejected, 20 duplicates, dead-letter queue empty.
const EXPECTED = { valid: 930, rejected: 50, duplicate: 20 };

test('R1: 1,000 records become 100 SQS batches of 10 with unique ids and bodies that parse back', () => {
  const { records } = generateRecords(1);
  const batches = buildSendBatches(records);
  assert.equal(batches.length, 100);
  for (const batch of batches) {
    assert.equal(batch.length, 10);
    assert.equal(new Set(batch.map((e) => e.Id)).size, 10);
  }
  const back = batches.flat().map((e) => JSON.parse(e.MessageBody));
  assert.deepEqual(back, JSON.parse(JSON.stringify(records)));
});

test('R2: the last batch can be smaller, and SQS never gets more than 10 entries', () => {
  const batches = buildSendBatches(Array.from({ length: 25 }, (_, i) => ({ id: `rec-${i}` })));
  assert.deepEqual(batches.map((b) => b.length), [10, 10, 5]);
});

test('R3: log counting reads the JSON part of Lambda log lines and ignores the rest', () => {
  const lines = [
    'START RequestId: abc Version: $LATEST',
    '2026-10-19T20:00:00.000Z\tabc\tINFO\t{"level":"info","id":"rec-0001","status":"valid","path":["received"]}',
    '2026-10-19T20:00:00.001Z\tabc\tINFO\t{"level":"info","id":"rec-0002","status":"rejected","path":["received"]}',
    '2026-10-19T20:00:00.002Z\tabc\tINFO\t{"level":"info","id":"rec-0003","status":"duplicate","path":["received"]}',
    '{"level":"error","status":"error","error":"db down"}',
    'REPORT RequestId: abc Duration: 3.1 ms',
    'not json at all {oops',
  ];
  assert.deepEqual(countStatuses(lines), { valid: 1, rejected: 1, duplicate: 1, error: 1 });
});

const clean = { logCounts: { ...EXPECTED, error: 0 }, tableCount: 930, queueDepth: 0, deadLetterDepth: 0 };

test('R4: a clean run passes', () => {
  assert.deepEqual(judgeRun(clean), { pass: true, problems: [] });
});

test('R5: a wrong count, a stuck queue or a non-empty dead-letter queue each fail with a clear reason', () => {
  const cases = [
    [{ ...clean, tableCount: 929 }, /table/],
    [{ ...clean, logCounts: { ...clean.logCounts, valid: 929 } }, /valid/],
    [{ ...clean, logCounts: { ...clean.logCounts, rejected: 49 } }, /rejected/],
    [{ ...clean, logCounts: { ...clean.logCounts, duplicate: 21 } }, /duplicate/],
    [{ ...clean, logCounts: { ...clean.logCounts, error: 1 } }, /error/],
    [{ ...clean, queueDepth: 3 }, /queue/],
    [{ ...clean, deadLetterDepth: 1 }, /dead-letter/],
  ];
  for (const [input, pattern] of cases) {
    const verdict = judgeRun(input);
    assert.equal(verdict.pass, false);
    assert.ok(verdict.problems.some((p) => pattern.test(p)), `${JSON.stringify(verdict.problems)} should match ${pattern}`);
  }
});
