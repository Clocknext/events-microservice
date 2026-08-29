variable "aws_region" {
  description = "Region holding the VPC and the MSK Serverless cluster."
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "The existing VPC. Read-only here — Terraform never owns it."
  type        = string
  default     = "vpc-0f1c34ce751397c5d"
}

variable "public_subnet_ids" {
  description = <<-EOT
    The two existing public subnets, one per AZ. The ALB needs both; the edge
    instance lands in `edge_subnet_id` alone.
  EOT
  type        = list(string)
  default     = ["subnet-042341bb0261d3cc0", "subnet-09a7111365492a3f6"]
}

variable "edge_subnet_id" {
  description = "Public subnet the edge instance boots into (us-east-1b)."
  type        = string
  default     = "subnet-09a7111365492a3f6"
}

variable "edge_security_group_id" {
  description = <<-EOT
    The pre-existing `event-tasks` SG. MSK's own SG already allows 9098 inbound
    from precisely this group, which is why the edge joins it rather than
    getting one of its own. Terraform takes ownership of exactly ONE ingress
    rule on it (port 3000 from the ALB) and nothing else.
  EOT
  type        = string
  default     = "sg-0c5bd459dd43c87fe"
}

variable "msk_cluster_name" {
  description = "MSK Serverless cluster the edge produces to."
  type        = string
  default     = "clocknext-signals"
}

variable "kafka_bootstrap_brokers" {
  description = "MSK Serverless IAM bootstrap endpoint (port 9098)."
  type        = string
  default     = "boot-pd2cdyql.c1.kafka-serverless.us-east-1.amazonaws.com:9098"
}

variable "kafka_topic" {
  description = "The single topic every gate-passing signal is produced to."
  type        = string
  default     = "signals"
}

variable "instance_type" {
  description = "arm64 — free through 2026-12-31, then ~$12/mo. See docs/COSTS.md."
  type        = string
  default     = "t4g.small"
}

variable "edge_port" {
  description = "Port the edge listens on, and the target group's port."
  type        = number
  default     = 3000
}

variable "artifact_path" {
  description = <<-EOT
    Local path to the build tarball uploaded to S3 and pulled by user-data.
    Produced by `npm run build && tar -czf ... dist package.json package-lock.json`.
  EOT
  type        = string
  default     = "../../signal-edge.tar.gz"
}
