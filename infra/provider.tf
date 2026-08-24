# AWS provider, pointed at LocalStack instead of real AWS.
#
# This is the only thing that differs from a real-AWS config: fake credentials,
# the validation skips, and every service endpoint aimed at the LocalStack
# gateway on :4566. Point these at real AWS (or just delete the block) and the
# same resources below provision a real account — which is the whole reason to
# use Terraform here rather than a bash script.
#
# `tflocal` is a wrapper that injects exactly this block; doing it by hand means
# one fewer tool to install.

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
  region     = var.aws_region
  access_key = "test"
  secret_key = "test"

  # LocalStack accepts anything; these skips stop the provider from trying to
  # reach the real AWS metadata/STS endpoints to validate.
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    sqs    = var.localstack_endpoint
    lambda = var.localstack_endpoint
    iam    = var.localstack_endpoint
    sts    = var.localstack_endpoint
  }
}
