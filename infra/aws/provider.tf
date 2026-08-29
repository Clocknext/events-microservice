# Real AWS. No LocalStack block, no fake credentials — the default credential
# chain resolves them, the same way the edge itself resolves its MSK IAM token.
#
# This directory is deliberately a SEPARATE root from `infra/`, which only ever
# tracked the deleted LocalStack SQS/Lambda pipeline. A fresh state file avoids
# inheriting a lineage that describes resources in an account this config has
# nothing to do with.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "events-microservice"
      Component = "signal-edge"
      ManagedBy = "terraform"
    }
  }
}
