# The four SQS queues, one pipeline per outcome of a signal, each with a
# dead-letter queue behind it.
#
# Standard, not FIFO: duplicates collapse on signal_id in ReplacingMergeTree,
# which at-least-once redelivery needs anyway, so FIFO's throughput ceiling and
# mandatory MessageGroupId would cost for nothing.

locals {
  retention_14d = 1209600 # seconds; SQS max
}

# --- pending: every response >= 400 ------------------------------------------
resource "aws_sqs_queue" "pending_dlq" {
  name                      = "signals_pending_dlq"
  message_retention_seconds = local.retention_14d
}

resource "aws_sqs_queue" "pending" {
  name                       = "signals_pending"
  message_retention_seconds  = local.retention_14d
  visibility_timeout_seconds = 60
  receive_wait_time_seconds  = 20 # long-poll
  # The hold: a message is invisible for a minute after it is sent, so it piles
  # up for the single batching consumer. A queue attribute — the publisher sets
  # no per-message delay.
  delay_seconds = 60

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.pending_dlq.arn
    maxReceiveCount     = 5
  })
}

# --- accepted: every accepted 202 --------------------------------------------
resource "aws_sqs_queue" "accepted_dlq" {
  name                      = "signals_accepted_dlq"
  message_retention_seconds = local.retention_14d
}

resource "aws_sqs_queue" "accepted" {
  name                       = "signals_accepted"
  message_retention_seconds  = local.retention_14d
  visibility_timeout_seconds = 60
  receive_wait_time_seconds  = 20
  # No hold: a settled signal is money, drained fast by N consumers.
  delay_seconds = 0

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.accepted_dlq.arn
    maxReceiveCount     = 5
  })
}
