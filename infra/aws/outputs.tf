output "alb_dns_name" {
  description = "Where to POST a signal. Stable across instance replacement, which is why the ALB is here before the domain is."
  value       = aws_lb.edge.dns_name
}

output "edge_instance_id" {
  description = "For `aws ssm start-session --target <id>` — there is no key pair."
  value       = aws_instance.edge.id
}

output "deploy_bucket" {
  description = "Where the build tarball lives."
  value       = aws_s3_bucket.deploy.id
}

output "signal_endpoint" {
  description = "The full URL the runbook curls."
  value       = "http://${aws_lb.edge.dns_name}/api/v1/signal"
}
