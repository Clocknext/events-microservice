variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "localstack_endpoint" {
  type    = string
  default = "http://localhost:4566"
}

# ClickHouse as seen FROM the Lambda container — its name on the shared docker
# network, not localhost. The Lambda only resolves this because LocalStack runs
# it on that network (LAMBDA_DOCKER_NETWORK in docker-compose.yml).
variable "clickhouse_url" {
  type    = string
  default = "http://clickhouse:8123"
}

variable "clickhouse_database" {
  type    = string
  default = "signals"
}

# --- consumer sizing ---------------------------------------------------------
# accepted: N concurrent consumers, each up to accepted_batch_size signals.
variable "accepted_batch_size" {
  type    = number
  default = 50
}

variable "accepted_concurrency" {
  type    = number
  default = 5
}

# AWS requires a batching window > 0 whenever batch_size > 10. 1s is effectively
# no hold — a batch of accepted_batch_size fills near-instantly under load — but
# it satisfies the rule. Raise it only to trade latency for fewer invocations.
variable "accepted_batch_window" {
  type    = number
  default = 1
}

# pending: ONE consumer, draining a batch that accumulates over the window.
variable "pending_batch_size" {
  type    = number
  default = 100
}

variable "pending_batch_window" {
  type    = number
  default = 60
}

variable "pending_concurrency" {
  type    = number
  default = 1
}
