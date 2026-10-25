# The record pipeline: a queue in front, a dead-letter queue for messages that keep
# failing, a table that remembers each record's result, and a logged Lambda.

# Test-only: record ids (comma separated) whose work step is made to fail, to prove the retry
# and dead-letter route on the real queue. Leave empty in normal use.
variable "fail_ids" {
  type    = string
  default = ""
}

resource "aws_sqs_queue" "dead_letter" {
  name                      = "ajr-records-dlq"
  message_retention_seconds = 1209600 # 14 days, to leave time to look at what failed
}

resource "aws_sqs_queue" "records" {
  name                       = "ajr-records"
  visibility_timeout_seconds = 60 # six times the Lambda timeout, as AWS advises

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter.arn
    maxReceiveCount     = 3
  })
}

resource "aws_dynamodb_table" "results" {
  name         = "ajr-results"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }
}

resource "aws_cloudwatch_log_group" "handler" {
  name              = "/aws/lambda/ajr-handler"
  retention_in_days = 7
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "handler" {
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.handler.arn}:*"]
  }

  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.records.arn]
  }

  statement {
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem"]
    resources = [aws_dynamodb_table.results.arn]
  }
}

resource "aws_iam_role" "handler" {
  name               = "ajr-handler"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy" "handler" {
  name   = "ajr-handler"
  role   = aws_iam_role.handler.id
  policy = data.aws_iam_policy_document.handler.json
}

# The zip holds the pipeline code and leaves out the fake-data generator and the test runner.
data "archive_file" "handler" {
  type        = "zip"
  source_dir  = "${path.module}/../pipeline"
  excludes    = ["fake-data.mjs", "run-pipeline.mjs"]
  output_path = "${path.module}/.build/handler.zip"
}

resource "aws_lambda_function" "handler" {
  function_name    = "ajr-handler"
  role             = aws_iam_role.handler.arn
  runtime          = "nodejs22.x"
  handler          = "handler.handler"
  filename         = data.archive_file.handler.output_path
  source_code_hash = data.archive_file.handler.output_base64sha256
  timeout          = 10
  memory_size      = 128

  environment {
    variables = {
      TABLE_NAME = aws_dynamodb_table.results.name
      FAIL_IDS   = var.fail_ids
    }
  }

  depends_on = [aws_cloudwatch_log_group.handler, aws_iam_role_policy.handler]
}

resource "aws_lambda_event_source_mapping" "records" {
  event_source_arn        = aws_sqs_queue.records.arn
  function_name           = aws_lambda_function.handler.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
}
