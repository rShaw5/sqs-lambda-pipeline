#!/usr/bin/env node
// Send the 1,000 fake records to the queue, then judge the run.
//   AWS_PROFILE=learning node pipeline/run-pipeline.mjs send    send the records (100 batches of 10)
//   AWS_PROFILE=learning node pipeline/run-pipeline.mjs check   count results and print pass or fail
// The login comes from the environment; nothing secret is written here.
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { generateRecords } from './fake-data.mjs';

const EXPECTED = { valid: 930, rejected: 50, duplicate: 20 };

export function buildSendBatches(records, size = 10) {
  const batches = [];
  for (let i = 0; i < records.length; i += size) {
    batches.push(records.slice(i, i + size).map((record, k) => ({ Id: `e${k}`, MessageBody: JSON.stringify(record) })));
  }
  return batches;
}

// Lambda log lines look like "<time>\t<request id>\tINFO\t{json}". Keep the JSON part.
export function countStatuses(lines) {
  const counts = { valid: 0, rejected: 0, duplicate: 0, error: 0 };
  for (const line of lines) {
    const start = line.indexOf('{');
    if (start < 0) continue;
    let entry;
    try {
      entry = JSON.parse(line.slice(start));
    } catch {
      continue;
    }
    if (entry && Object.hasOwn(counts, entry.status)) counts[entry.status] += 1;
  }
  return counts;
}

export function judgeRun({ logCounts, tableCount, queueDepth, deadLetterDepth }) {
  const problems = [];
  for (const key of ['valid', 'rejected', 'duplicate']) {
    if (logCounts[key] !== EXPECTED[key]) problems.push(`${key}: expected ${EXPECTED[key]}, logs show ${logCounts[key]}`);
  }
  if (logCounts.error !== 0) problems.push(`error: expected 0, logs show ${logCounts.error}`);
  if (tableCount !== EXPECTED.valid) problems.push(`table: expected ${EXPECTED.valid} stored results, found ${tableCount}`);
  if (queueDepth !== 0) problems.push(`queue: expected empty, ${queueDepth} messages left`);
  if (deadLetterDepth !== 0) problems.push(`dead-letter queue: expected empty, ${deadLetterDepth} messages`);
  return { pass: problems.length === 0, problems };
}

function aws(args) {
  const r = spawnSync('aws', [...args, '--output', 'json'], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  if (r.status !== 0) throw new Error(`aws ${args.slice(0, 2).join(' ')} failed: ${r.stderr.trim()}`);
  return r.stdout.trim() ? JSON.parse(r.stdout) : {};
}

const queueUrl = (name) => aws(['sqs', 'get-queue-url', '--queue-name', name]).QueueUrl;
const depth = (name) => {
  const a = aws(['sqs', 'get-queue-attributes', '--queue-url', queueUrl(name), '--attribute-names', 'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']).Attributes;
  return Number(a.ApproximateNumberOfMessages) + Number(a.ApproximateNumberOfMessagesNotVisible);
};

function send() {
  const url = queueUrl('ajr-records');
  const batches = buildSendBatches(generateRecords(1).records);
  batches.forEach((entries, i) => {
    const out = aws(['sqs', 'send-message-batch', '--queue-url', url, '--entries', JSON.stringify(entries)]);
    if (out.Failed?.length) throw new Error(`batch ${i} had ${out.Failed.length} failed entries`);
  });
  console.log(`sent ${batches.length} batches (${batches.flat().length} records)`);
}

function check() {
  const events = aws(['logs', 'filter-log-events', '--log-group-name', '/aws/lambda/ajr-handler', '--query', 'events[].message']);
  const items = aws(['dynamodb', 'scan', '--table-name', 'ajr-results', '--select', 'COUNT']);
  const verdict = judgeRun({
    logCounts: countStatuses(Array.isArray(events) ? events : []),
    tableCount: Number(items.Count),
    queueDepth: depth('ajr-records'),
    deadLetterDepth: depth('ajr-records-dlq'),
  });
  console.log(verdict.pass ? 'PASS: 930 valid, 50 rejected, 20 duplicates, queues empty' : `FAIL:\n- ${verdict.problems.join('\n- ')}`);
  process.exitCode = verdict.pass ? 0 : 1;
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  const command = process.argv[2];
  if (command === 'send') send();
  else if (command === 'check') check();
  else {
    console.error('usage: node pipeline/run-pipeline.mjs send|check');
    process.exitCode = 2;
  }
}
