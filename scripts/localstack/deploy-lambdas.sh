#!/bin/bash
# Deploys the two SQS consumers as Lambdas into LocalStack, with the topology
# each queue wants:
#
#   signals_pending  -> pending consumer   : ONE consumer, 60s batch window
#                       reserved concurrency 1, writes raw_signals + signal_status
#   signals_accepted -> accepted consumer  : N concurrent consumers, no window
#                       MaximumConcurrency=N, writes raw_signals + a batch_id
#
# Idempotent: re-running updates code and re-creates the event-source mappings.
# Prereqrequisites: LocalStack up WITH LAMBDA_DOCKER_NETWORK set (so the Lambda
# containers can reach ClickHouse by name), and ClickHouse on that same network.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="000000000000"
# How many accepted consumers may run at once. SQS event-source mappings floor
# MaximumConcurrency at 2.
ACCEPTED_CONCURRENCY="${ACCEPTED_CONCURRENCY:-5}"
# ClickHouse as seen FROM the Lambda container: its name on the shared network.
CH_URL="${LAMBDA_CLICKHOUSE_URL:-http://clickhouse:8123}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"

awsl() { aws --endpoint-url "$ENDPOINT" --region "$REGION" "$@"; }

echo "==> building bundles"
node "$ROOT/scripts/lambda/build.mjs"

ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/lambda-sqs-role"
awsl iam get-role --role-name lambda-sqs-role >/dev/null 2>&1 || awsl iam create-role \
  --role-name lambda-sqs-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  >/dev/null

# $1 function name   $2 zip   $3 handler file base (index)   $4 extra env pairs
deploy_fn() {
  local name="$1" zip="$2"
  # JSON, not the key=value shorthand: an empty CLICKHOUSE_PASSWORD trips the
  # shorthand parser. Omitted entirely here — config defaults it to ''.
  local env="{\"Variables\":{\"CLICKHOUSE_URL\":\"${CH_URL}\",\"CLICKHOUSE_DATABASE\":\"signals\",\"CLICKHOUSE_USER\":\"default\"}}"

  if awsl lambda get-function --function-name "$name" >/dev/null 2>&1; then
    awsl lambda update-function-code --function-name "$name" \
      --zip-file "fileb://$zip" >/dev/null
    awsl lambda wait function-updated --function-name "$name"
    awsl lambda update-function-configuration --function-name "$name" \
      --environment "$env" --timeout 30 >/dev/null
  else
    awsl lambda create-function --function-name "$name" \
      --runtime nodejs22.x --handler index.handler --role "$ROLE_ARN" \
      --zip-file "fileb://$zip" --environment "$env" --timeout 30 >/dev/null
  fi
  awsl lambda wait function-active-v2 --function-name "$name" 2>/dev/null || \
    awsl lambda wait function-active --function-name "$name"
  echo "==> deployed $name"
}

# Removes any existing mapping between a function and a queue, so create is clean.
drop_mappings() {
  local name="$1"
  awsl lambda list-event-source-mappings --function-name "$name" \
    --query 'EventSourceMappings[].UUID' --output text 2>/dev/null \
    | tr '\t' '\n' | while read -r uuid; do
      [ -n "$uuid" ] && awsl lambda delete-event-source-mapping --uuid "$uuid" >/dev/null 2>&1 || true
    done
}

deploy_fn signals-pending-consumer  "$ROOT/dist-lambda/pending.zip"
deploy_fn signals-accepted-consumer "$ROOT/dist-lambda/accepted.zip"

# --- signals_pending: ONE consumer, batch over 60s -----------------------------
# Reserved concurrency 1 makes it a single consumer; the 60s window lets a large
# batch accumulate per invocation. ReportBatchItemFailures so one poison row does
# not replay the whole batch.
awsl lambda put-function-concurrency --function-name signals-pending-consumer \
  --reserved-concurrent-executions 1 >/dev/null
drop_mappings signals-pending-consumer
awsl lambda create-event-source-mapping \
  --function-name signals-pending-consumer \
  --event-source-arn "arn:aws:sqs:${REGION}:${ACCOUNT}:signals_pending" \
  --batch-size 100 \
  --maximum-batching-window-in-seconds 60 \
  --function-response-types ReportBatchItemFailures >/dev/null
echo "==> mapped signals_pending -> signals-pending-consumer (1 consumer, 60s window)"

# --- signals_accepted: N concurrent consumers, no window -----------------------
drop_mappings signals-accepted-consumer
awsl lambda create-event-source-mapping \
  --function-name signals-accepted-consumer \
  --event-source-arn "arn:aws:sqs:${REGION}:${ACCOUNT}:signals_accepted" \
  --batch-size 10 \
  --scaling-config "MaximumConcurrency=${ACCEPTED_CONCURRENCY}" \
  --function-response-types ReportBatchItemFailures >/dev/null
echo "==> mapped signals_accepted -> signals-accepted-consumer (${ACCEPTED_CONCURRENCY} concurrent)"

echo "done."
