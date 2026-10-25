# sqs-lambda-pipeline

A small AWS data pipeline that checks every record before it is saved. Records arrive on an SQS queue, a Lambda function validates each one, DynamoDB remembers what was already saved, and anything that keeps failing goes to a dead-letter queue for a person to inspect. Terraform sets it all up.

It runs on made-up data. I deployed it once to a personal AWS learning account and ran a 1,000-record test against it. It is a learning project, and it has never handled production traffic.

## The test it has to pass

Send 1,000 fake records: 930 good and unique, 50 bad, 20 repeats. The run passes only if the logs and the table show exactly 930 valid, 50 rejected and 20 duplicates, and both queues end empty.

The deployed run on 2026-10-19 matched all of it: 930 saved, 50 rejected, 20 duplicates, both queues empty, no timeouts. A second live test made one message fail on purpose; it was tried 3 times, moved to the dead-letter queue, and never saved.

## How a record moves

1. A record arrives as a message on the `ajr-records` queue. The queue absorbs bursts so the code is never flooded.
2. The `ajr-handler` Lambda takes up to 10 messages at a time.
3. For each record ([pipeline/process.mjs](pipeline/process.mjs)):
   - **Validate** ([pipeline/validate.mjs](pipeline/validate.mjs)). The id, customer, amount, currency and time must all make sense. A bad record is marked `rejected` and never retried, since retrying can't fix it.
   - **Skip repeats.** If DynamoDB already has the id, the record is marked `duplicate`.
   - **Save once.** The write only succeeds if the id doesn't exist yet, so when two copies race, one wins and the other becomes a `duplicate`.
4. Each record writes one JSON log line to CloudWatch with its id, its result and the steps it passed through.

## When things go wrong

| Case | What happens |
|---|---|
| Bad record | Marked `rejected`, logged with the fields that failed, not retried. |
| Repeat of a saved record | Marked `duplicate` and skipped. |
| Records arrive out of order | Counts come out the same; the first copy to arrive is the one saved. |
| Code or database fails on one record | That message alone is reported as failed (`ReportBatchItemFailures`), and SQS retries it. |
| It fails 3 times | SQS moves it to `ajr-records-dlq`, where it waits 14 days. |
| One record takes over 5 seconds | Cut off and reported as failed, then retried like any other failure. |
| A failed record was never saved | It isn't marked as seen, so a later clean retry can succeed. |

## Design choices

| Choice | Why | Cost |
|---|---|---|
| Queue in front of the code | Absorbs bursts and gives retries and a dead-letter queue for free. | Messages can arrive twice, so the code has to handle repeats. |
| DynamoDB to remember results | Fast lookup by id and an atomic "write only if new". | A second kind of storage to run. |
| Report failures per message | One bad message doesn't force the whole batch to retry. | Needs the `ReportBatchItemFailures` setting on the event source. |
| Rules kept apart from the AWS code | Validation and duplicate handling run and are tested with no AWS account. | Code that talks to AWS is only proven by a real run. |
| Pay-per-use services | A test run costs close to nothing. | Cost per record would be higher at large scale. |

## Known limits

- SQS delivers at least once, so a lucky redelivery could make a run show 21 duplicates instead of 20. A FIFO queue would fix that.
- If the whole Lambda runs past its 10-second limit, the entire batch goes back to the queue, and healthy messages use up a try. The per-record limit stays well under the Lambda limit to avoid this.
- Rejected records live only in the log, so a re-sent bad record is rejected again.
- Only one deliberate failure was run live. The rest of the failure handling is proven by the tests.

## Run it

The tests need only Node (version 20 or later) and no AWS account:

```bash
node --test
```

To deploy, you need your own AWS account and Terraform. Run `terraform init` and `terraform apply` in `terraform/`, then send and judge the 1,000-record test:

```bash
node pipeline/run-pipeline.mjs send
node pipeline/run-pipeline.mjs check
```

Run `terraform destroy` afterwards so nothing keeps running.

## Layout

| Path | What it is |
|---|---|
| [pipeline/](pipeline/) | Validation, duplicate handling, the Lambda handler, the fake-data generator, and the send-and-judge script. |
| [terraform/](terraform/) | The queue, dead-letter queue, table, log group, IAM role and Lambda. |
| [tests/](tests/) | 34 tests covering the rules, the handler's failure paths and the run judge. |

## About the author

Built by Robin Shaw, a software engineer. More of my work is on [my portfolio](https://rshaw5.github.io) and [GitHub](https://github.com/rShaw5), and the best way to reach me is [LinkedIn](https://www.linkedin.com/in/rshaw5/).
