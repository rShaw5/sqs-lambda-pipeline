import { processRecords } from './process.mjs';

// DynamoDB store for the pipeline rules. The AWS SDK classes are passed in so the
// store can be tested with no account and no SDK.
export function createDynamoStore({ client, table, GetItemCommand, PutItemCommand }) {
  return {
    async has(id) {
      const out = await client.send(new GetItemCommand({ TableName: table, Key: { id: { S: id } }, ConsistentRead: true }));
      return Boolean(out.Item);
    },
    async put(id) {
      try {
        await client.send(new PutItemCommand({
          TableName: table,
          Item: { id: { S: id }, status: { S: 'valid' }, at: { S: new Date().toISOString() } },
          ConditionExpression: 'attribute_not_exists(id)',
        }));
        return true;
      } catch (error) {
        if (error.name === 'ConditionalCheckFailedException') return false;
        throw error;
      }
    },
  };
}

// Test-only switch: ids listed in FAIL_IDS make the work step throw, so a real message can be
// pushed through the retry and dead-letter route. It is empty in normal use.
export function createWorkFromEnv(env) {
  const ids = new Set((env.FAIL_IDS ?? '').split(',').map((x) => x.trim()).filter(Boolean));
  return async (record) => {
    if (ids.has(record.id)) throw new Error(`poison record ${record.id}`);
  };
}

function parseBody(body) {
  try {
    return JSON.parse(body);
  } catch {
    return body;
  }
}

// SQS owns retries and the dead-letter queue: a record that fails here is returned in
// batchItemFailures, SQS delivers it again, and after maxReceiveCount it is moved aside.
// `work` is the step that does something with a good record. It is a no-op for now; the
// parameter lets the tests prove how failures and timeouts are reported.
export function createHandler({ store, work, timeoutMs, log = (line) => console.log(JSON.stringify(line)) }) {
  return async function handle(event) {
    const batchItemFailures = [];
    for (const message of event.Records ?? []) {
      const record = parseBody(message.body);
      try {
        const { results } = await processRecords([record], { store, handler: work, timeoutMs, maxAttempts: 1 });
        const [result] = results;
        log({ level: 'info', messageId: message.messageId, id: result.id, status: result.status, path: result.path });
        if (result.status === 'dead-lettered') batchItemFailures.push({ itemIdentifier: message.messageId });
      } catch (error) {
        log({ level: 'error', messageId: message.messageId, status: 'error', error: error.message });
        batchItemFailures.push({ itemIdentifier: message.messageId });
      }
    }
    return { batchItemFailures };
  };
}

let ready;
export async function handler(event) {
  ready ??= (async () => {
    const { DynamoDBClient, GetItemCommand, PutItemCommand } = await import('@aws-sdk/client-dynamodb');
    const store = createDynamoStore({ client: new DynamoDBClient({}), table: process.env.TABLE_NAME, GetItemCommand, PutItemCommand });
    return createHandler({ store, work: createWorkFromEnv(process.env) });
  })();
  return (await ready)(event);
}
