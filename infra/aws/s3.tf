# Where the build tarball is parked for user-data to pull. Private in every
# sense — the instance role is the only reader.

resource "aws_s3_bucket" "deploy" {
  bucket = "signal-edge-deploy-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_public_access_block" "deploy" {
  bucket                  = aws_s3_bucket.deploy.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "deploy" {
  bucket = aws_s3_bucket.deploy.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "deploy" {
  bucket = aws_s3_bucket.deploy.id
  versioning_configuration {
    status = "Enabled"
  }
}

# The instance's user-data fetches this on first boot. Uploading it as a real
# resource (rather than a local-exec) is what lets `aws_instance` depend on it:
# an instance that boots before the artifact exists would fail to start the
# service and leave a healthy-looking box serving nothing.
resource "aws_s3_object" "artifact" {
  bucket = aws_s3_bucket.deploy.id
  key    = "signal-edge.tar.gz"
  source = var.artifact_path

  # The etag is what makes a rebuilt tarball a real diff, which in turn triggers
  # the instance's user_data_replace_on_change. Without it a redeploy is a no-op.
  etag = fileexists(var.artifact_path) ? filemd5(var.artifact_path) : ""

  lifecycle {
    precondition {
      condition     = fileexists(var.artifact_path)
      error_message = "Build artifact not found at ${var.artifact_path}. From the repo root, run: npm run build && tar -czf signal-edge.tar.gz dist package.json package-lock.json"
    }
  }
}
