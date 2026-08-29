# The edge's identity. There is no access key anywhere in this config, and none
# in `src/config.ts` either — the SDK's default chain picks up the instance role,
# which is the whole reason `KAFKA_USE_IAM=true` needs no secret.

locals {
  # MSK ARNs carry a random suffix after the cluster name, so the exact cluster
  # ARN cannot be constructed. The wildcard covers only that suffix — the cluster
  # NAME is still pinned, so this does not widen to other clusters.
  msk_cluster_arn_pattern = "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:cluster/${var.msk_cluster_name}/*"
  msk_topic_arn_pattern   = "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:topic/${var.msk_cluster_name}/*/${var.kafka_topic}"
}

data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "edge" {
  name               = "signal-edge"
  description        = "The signal edge on EC2: produce to MSK, read its own build from S3."
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

# Deliberately NARROWER than the msk-admin-temp role on the bastion: no ReadData,
# no CreateTopic, no consumer-group actions. The edge only ever produces.
data "aws_iam_policy_document" "edge" {
  statement {
    sid       = "ConnectToCluster"
    actions   = ["kafka-cluster:Connect"]
    resources = [local.msk_cluster_arn_pattern]
  }

  statement {
    sid = "ProduceToSignalsTopic"
    actions = [
      "kafka-cluster:WriteData",
      "kafka-cluster:WriteDataIdempotently",
      "kafka-cluster:DescribeTopic",
    ]
    resources = [local.msk_topic_arn_pattern]
  }

  statement {
    sid       = "ReadOwnBuildArtifact"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.deploy.arn}/*"]
  }
}

resource "aws_iam_role_policy" "edge" {
  name   = "signal-edge"
  role   = aws_iam_role.edge.id
  policy = data.aws_iam_policy_document.edge.json
}

# Session Manager instead of a key pair — the same access pattern msk-ladder
# already uses. No SSH, no port 22, nothing to rotate.
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.edge.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "edge" {
  name = "signal-edge"
  role = aws_iam_role.edge.name
}
