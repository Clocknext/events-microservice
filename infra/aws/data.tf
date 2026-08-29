# Everything here is READ-ONLY. Terraform never takes ownership of these, so
# there is no import step and no way for an `apply` to alter them.

data "aws_caller_identity" "current" {}

data "aws_vpc" "main" {
  id = var.vpc_id
}

data "aws_subnet" "public" {
  for_each = toset(var.public_subnet_ids)
  id       = each.value
}

data "aws_security_group" "edge" {
  id = var.edge_security_group_id
}

# The AMI alias resolves to the current Amazon Linux 2023 arm64 image. Pinning a
# literal ami-… would go stale; this re-resolves on every plan.
data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}
