# The two SQS -> ClickHouse consumers, and the event-source mappings that give
# each the topology its queue wants.
#
# The .zip files are built by `npm run build:lambdas` (esbuild) BEFORE apply —
# Terraform reads them off disk, it does not build them. `source_code_hash`
# makes Terraform notice a rebuilt bundle and update the function.

# LocalStack does not enforce IAM, but a role ARN is still required to create a
# function. In real AWS this role needs sqs:Receive/Delete on the queues and
# logs:* — add those policies when you point this at a real account.
resource "aws_iam_role" "consumer" {
  name = "lambda-sqs-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

locals {
  bundle_dir = "${path.module}/../dist-lambda"
}

# --- pending consumer: ONE consumer, batch over a window ---------------------
resource "aws_lambda_function" "pending" {
  function_name    = "signals-pending-consumer"
  runtime          = "nodejs22.x"
  handler          = "index.handler"
  role             = aws_iam_role.consumer.arn
  filename         = "${local.bundle_dir}/pending.zip"
  source_code_hash = filebase64sha256("${local.bundle_dir}/pending.zip")
  timeout          = 30

  # Reserved concurrency 1 is what makes it a single consumer.
  reserved_concurrent_executions = var.pending_concurrency

  environment {
    variables = {
      CLICKHOUSE_URL      = var.clickhouse_url
      CLICKHOUSE_DATABASE = var.clickhouse_database
      CLICKHOUSE_USER     = "default"
    }
  }
}

resource "aws_lambda_event_source_mapping" "pending" {
  event_source_arn                   = aws_sqs_queue.pending.arn
  function_name                      = aws_lambda_function.pending.arn
  batch_size                         = var.pending_batch_size
  maximum_batching_window_in_seconds = var.pending_batch_window
  function_response_types            = ["ReportBatchItemFailures"]
}

# --- accepted consumer: N concurrent consumers, no window --------------------
resource "aws_lambda_function" "accepted" {
  function_name    = "signals-accepted-consumer"
  runtime          = "nodejs22.x"
  handler          = "index.handler"
  role             = aws_iam_role.consumer.arn
  filename         = "${local.bundle_dir}/accepted.zip"
  source_code_hash = filebase64sha256("${local.bundle_dir}/accepted.zip")
  timeout          = 30

  environment {
    variables = {
      CLICKHOUSE_URL         = var.clickhouse_url
      CLICKHOUSE_DATABASE    = var.clickhouse_database
      CLICKHOUSE_USER        = "default"
      PAYMENTS_URL           = var.payments_url
      INTERNAL_SETTLE_SECRET = var.internal_settle_secret
    }
  }
}

resource "aws_lambda_event_source_mapping" "accepted" {
  event_source_arn                   = aws_sqs_queue.accepted.arn
  function_name                      = aws_lambda_function.accepted.arn
  batch_size                         = var.accepted_batch_size
  maximum_batching_window_in_seconds = var.accepted_batch_window
  function_response_types            = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = var.accepted_concurrency
  }
}
