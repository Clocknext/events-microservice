#!/bin/bash
# Creates the signal pipeline's queues inside LocalStack. Mounted at
# /etc/localstack/init/ready.d, which LocalStack runs once the gateway is up, so
# `docker compose up` is all it takes — nothing in the service creates queues.
#
# Real AWS gets the same queues from whatever provisions it. The attributes
# below are the contract, not the tool.
#
# TWO PIPELINES, one per outcome of a signal:
#
#   signals_pending    every response of 400 or above, as two ClickHouse rows
#                      (status PENDING). Published to today by plugins/vent.ts.
#   signals_accepted   every 202. NOT PUBLISHED TO YET — the queue is created so
#                      the shape of the system is visible and provisioning does
#                      not change when the publisher lands.
#
# ALL FOUR QUEUES ARE STANDARD, NOT FIFO. Deliberate, and three things depend
# on it:
#
#   · FIFO is capped at 300 requests/sec (3000 batched); Standard is not. The
#     vent fires on every 4xx and 5xx the service produces, including 404s from
#     bots probing paths — that is a rate nobody controls.
#   · FIFO would demand a MessageGroupId on every send, and there is no natural
#     grouping here. One group serialises the whole queue; per-signal groups buy
#     ordering nobody reads.
#   · Exactly-once is not needed. Duplicates collapse on `signal_id` in
#     ReplacingMergeTree, which the at-least-once redelivery path already
#     requires — so paying FIFO's price would buy a guarantee twice.
#
# A FIFO queue also has to be named with a `.fifo` suffix, so the names below
# are themselves part of the contract.
set -euo pipefail

# Creates one queue with a dead-letter queue behind it.
#   $1 queue name   $2 DelaySeconds
create_with_dlq() {
  local name="$1" delay="$2" dlq="$1_dlq" dlq_arn

  awslocal sqs create-queue --queue-name "$dlq" >/dev/null

  dlq_arn="$(awslocal sqs get-queue-attributes \
    --queue-url "$(awslocal sqs get-queue-url --queue-name "$dlq" --output text --query QueueUrl)" \
    --attribute-names QueueArn --output text --query 'Attributes.QueueArn')"

  # maxReceiveCount=5 sends a message that keeps failing to the DLQ instead of
  # letting it circle forever. A message that cannot be inserted is a bug to
  # look at, not traffic to retry.
  awslocal sqs create-queue \
    --queue-name "$name" \
    --attributes "$(cat <<JSON
{
  "DelaySeconds": "${delay}",
  "MessageRetentionPeriod": "1209600",
  "ReceiveMessageWaitTimeSeconds": "20",
  "VisibilityTimeout": "60",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${dlq_arn}\",\"maxReceiveCount\":\"5\"}"
}
JSON
)" >/dev/null

  echo "sqs ready: $(awslocal sqs get-queue-url --queue-name "$name" --output text --query QueueUrl) (delay ${delay}s, dlq ${dlq})"
}

# DelaySeconds=60 is the hold: a message is invisible to any consumer for a
# minute after it is sent, so they pile up and a worker drains them in batches
# rather than one at a time. It is a QUEUE attribute — the publisher sets no
# delay per message and does not need to know this number.
create_with_dlq signals_pending 60

# No hold on the accepted side: a settled signal is money, and money waits for
# nobody. Its worker batches by polling, not by letting messages accumulate.
create_with_dlq signals_accepted 0
