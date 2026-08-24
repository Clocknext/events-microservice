output "pending_queue_url" {
  value = aws_sqs_queue.pending.url
}

output "accepted_queue_url" {
  value = aws_sqs_queue.accepted.url
}

output "pending_dlq_url" {
  value = aws_sqs_queue.pending_dlq.url
}

output "accepted_dlq_url" {
  value = aws_sqs_queue.accepted_dlq.url
}
