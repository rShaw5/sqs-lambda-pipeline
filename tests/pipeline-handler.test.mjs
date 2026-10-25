import test from 'node:test';
import assert from 'node:assert/strict';
import { generateRecords } from '../pipeline/fake-data.mjs';
import { createMemoryStore } from '../pipeline/process.mjs';
import { createHandler, createDynamoStore, createWorkFromEnv } from '../pipeline/handler.mjs';

// The Lambda handler turns an SQS batch into pipeline results. SQS owns retries and the
// dead-letter queue, so a record that fails here is reported back in batchItemFailures
// and SQS redelivers it; after maxReceiveCount it lands in the dead-letter queue.
const sqsEvent = (records) => ({
  Records: records.map((r, i) => ({ messageId: `m-${i}`, body: typeof r === 'string' ? r : JSON.stringify(r) })),
});
const quiet = () => {};

test('H1: a clean batch reports no failures', async () => {
  const { records } = generateRecords(1);
  const handler = createHandler({ store: createMemoryStore(), log: quiet });
  const out = await handler(sqsEvent(records.slice(0, 10).filter((r) => r && typeof r === 'object' && r.id)));
  assert.deepEqual(out, { batchItemFailures: [] });
});

test('H2: the whole 1,000-record run through the handler gives 930/50/20 across batches of 10', async () => {
  const { records } = generateRecords(1);
  const seen = [];
  const handler = createHandler({ store: createMemoryStore(), log: (line) => seen.push(line) });
  for (let i = 0; i < records.length; i += 10) {
    const out = await handler(sqsEvent(records.slice(i, i + 10)));
    assert.deepEqual(out.batchItemFailures, []);
  }
  const counts = { valid: 0, rejected: 0, duplicate: 0 };
  for (const line of seen) counts[line.status] += 1;
  assert.deepEqual(counts, { valid: 930, rejected: 50, duplicate: 20 });
});

test('H3: a record that fails is reported by message id so SQS can retry it', async () => {
  const { records } = generateRecords(1);
  const good = records.filter((r) => r && typeof r === 'object' && r.id).slice(0, 3);
  const failing = good[1].id;
  const store = {
    ...createMemoryStore(),
    async put(id) { if (id === failing) throw new Error('db down'); return true; },
  };
  const handler = createHandler({ store, log: quiet });
  const out = await handler(sqsEvent(good));
  assert.deepEqual(out, { batchItemFailures: [{ itemIdentifier: 'm-1' }] });
});

test('H4: a body that is not JSON is rejected, not retried', async () => {
  const handler = createHandler({ store: createMemoryStore(), log: quiet });
  const lines = [];
  const logged = createHandler({ store: createMemoryStore(), log: (l) => lines.push(l) });
  assert.deepEqual(await handler(sqsEvent(['{not json'])), { batchItemFailures: [] });
  await logged(sqsEvent(['{not json']));
  assert.equal(lines[0].status, 'rejected');
});

test('H5: each record writes one log line with its id, status and path', async () => {
  const { records } = generateRecords(1);
  const one = records.find((r) => r && typeof r === 'object' && r.id);
  const lines = [];
  const handler = createHandler({ store: createMemoryStore(), log: (l) => lines.push(l) });
  await handler(sqsEvent([one]));
  assert.equal(lines.length, 1);
  assert.equal(lines[0].id, one.id);
  assert.equal(lines[0].status, 'valid');
  assert.deepEqual(lines[0].path, ['received', 'validated', 'handled', 'valid']);
});

test('H6: the DynamoDB store stores an id once and says false the second time', async () => {
  const table = new Map();
  class GetItemCommand { constructor(input) { this.input = input; } }
  class PutItemCommand { constructor(input) { this.input = input; } }
  const client = {
    async send(cmd) {
      const id = cmd.input.Key?.id?.S ?? cmd.input.Item?.id?.S;
      if (cmd instanceof GetItemCommand) return table.has(id) ? { Item: table.get(id) } : {};
      if (table.has(id)) { const e = new Error('exists'); e.name = 'ConditionalCheckFailedException'; throw e; }
      table.set(id, cmd.input.Item);
      return {};
    },
  };
  const store = createDynamoStore({ client, table: 'results', GetItemCommand, PutItemCommand });
  assert.equal(await store.has('rec-0001'), false);
  assert.equal(await store.put('rec-0001'), true);
  assert.equal(await store.has('rec-0001'), true);
  assert.equal(await store.put('rec-0001'), false);
});

test('H7: any other DynamoDB error is not swallowed', async () => {
  class PutItemCommand { constructor(input) { this.input = input; } }
  class GetItemCommand { constructor(input) { this.input = input; } }
  const client = { async send() { const e = new Error('throttled'); e.name = 'ProvisionedThroughputExceededException'; throw e; } };
  const store = createDynamoStore({ client, table: 'results', GetItemCommand, PutItemCommand });
  await assert.rejects(() => store.put('rec-0001'), /throttled/);
});

test('H8: work that throws is reported as a failed message, and the others in the batch still succeed', async () => {
  const { records } = generateRecords(1);
  const good = records.filter((r) => r && typeof r === 'object' && r.id).slice(0, 3);
  const work = async (record) => { if (record.id === good[0].id) throw new Error('boom'); };
  const handler = createHandler({ store: createMemoryStore(), work, log: quiet });
  assert.deepEqual(await handler(sqsEvent(good)), { batchItemFailures: [{ itemIdentifier: 'm-0' }] });
});

test('H9: work that runs past the per-record timeout is reported as a failed message', async () => {
  const { records } = generateRecords(1);
  const one = records.find((r) => r && typeof r === 'object' && r.id);
  const lines = [];
  const handler = createHandler({ store: createMemoryStore(), work: () => new Promise(() => {}), timeoutMs: 20, log: (l) => lines.push(l) });
  assert.deepEqual(await handler(sqsEvent([one])), { batchItemFailures: [{ itemIdentifier: 'm-0' }] });
  assert.equal(lines[0].status, 'dead-lettered');
});

test('H10: a failed record is not stored, so SQS can retry it and it then succeeds', async () => {
  const { records } = generateRecords(1);
  const one = records.find((r) => r && typeof r === 'object' && r.id);
  const store = createMemoryStore();
  let fail = true;
  const work = async () => { if (fail) throw new Error('first time'); };
  const handler = createHandler({ store, work, log: quiet });
  assert.equal((await handler(sqsEvent([one]))).batchItemFailures.length, 1);
  fail = false;
  assert.deepEqual(await handler(sqsEvent([one])), { batchItemFailures: [] });
  assert.equal(await store.has(one.id), true);
});

test('H11: FAIL_IDS makes work throw for the listed ids only; unset or empty means no failing ids', async () => {
  const work = createWorkFromEnv({ FAIL_IDS: 'rec-0001, rec-0002' });
  await assert.rejects(() => work({ id: 'rec-0001' }), /poison/);
  await assert.rejects(() => work({ id: 'rec-0002' }), /poison/);
  assert.equal(await work({ id: 'rec-0003' }), undefined);
  for (const env of [{}, { FAIL_IDS: '' }, { FAIL_IDS: ' , ' }]) {
    assert.equal(await createWorkFromEnv(env)({ id: 'rec-0001' }), undefined);
  }
});
