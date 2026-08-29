# The ALB's own security group. This is the only thing on the public internet.
resource "aws_security_group" "alb" {
  name        = "edge-alb"
  description = "Internet-facing ALB in front of the signal edge"
  vpc_id      = data.aws_vpc.main.id
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  security_group_id = aws_security_group.alb.id
  description       = "HTTP from anywhere. TLS is the deferred domain step."
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "alb_all" {
  security_group_id = aws_security_group.alb.id
  description       = "To the targets."
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# A STANDALONE rule on the pre-existing `event-tasks` SG. Terraform owns this one
# rule and nothing else about that group — importing the whole SG would put its
# existing MSK wiring at the mercy of this config.
resource "aws_vpc_security_group_ingress_rule" "edge_from_alb" {
  security_group_id            = data.aws_security_group.edge.id
  description                  = "Signal edge: ALB to Fastify"
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = var.edge_port
  to_port                      = var.edge_port
  ip_protocol                  = "tcp"
}

resource "aws_instance" "edge" {
  # nonsensitive() because aws_ssm_parameter marks every value sensitive, which
  # would blank the AMI id out of `terraform plan` — the one line a reviewer most
  # needs to see before launching an instance. A public AMI alias is not a secret.
  ami                    = nonsensitive(data.aws_ssm_parameter.al2023_arm64.value)
  instance_type          = var.instance_type
  subnet_id              = var.edge_subnet_id
  vpc_security_group_ids = [data.aws_security_group.edge.id]
  iam_instance_profile   = aws_iam_instance_profile.edge.name

  # The subnet does not auto-assign, and msk-ladder got its address the same way.
  associate_public_ip_address = true

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/user-data.sh.tpl", {
    bucket  = aws_s3_bucket.deploy.id
    key     = aws_s3_object.artifact.key
    region  = var.aws_region
    brokers = var.kafka_bootstrap_brokers
    topic   = var.kafka_topic
    port    = var.edge_port
  })

  metadata_options {
    http_tokens   = "required" # IMDSv2 only
    http_endpoint = "enabled"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 8
    encrypted             = true
    delete_on_termination = true
  }

  # Without this the box can boot, find no artifact, and sit there healthy-looking
  # with nothing listening on 3000.
  depends_on = [aws_s3_object.artifact]

  tags = { Name = "signal-edge" }
}
