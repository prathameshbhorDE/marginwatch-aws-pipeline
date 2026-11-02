# Who am I? Used to make the bucket name globally unique without typing the account id anywhere.
data "aws_caller_identity" "current" {}

locals {
  name_prefix    = "marginwatch-${var.env}"
  landing_bucket = "${local.name_prefix}-landing-${data.aws_caller_identity.current.account_id}-${var.region}"
}

# The receiving dock: raw files exactly as they arrived.
resource "aws_s3_bucket" "landing" {
  bucket        = local.landing_bucket
  force_destroy = var.allow_destroy_with_data
}

# Nobody outside the account can ever read it.
resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

# Keep every version: a counterparty's corrected re-send never overwrites the original.
resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Encrypt everything at rest (SSE-S3). In a bank: switch to aws:kms with a customer-managed key.
resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Only the bucket owner owns objects; ACLs switched off (AWS's recommended setting).
resource "aws_s3_bucket_ownership_controls" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Move old raw files to cheaper storage; clean up old versions and failed uploads.
resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket     = aws_s3_bucket.landing.id
  depends_on = [aws_s3_bucket_versioning.landing]

  rule {
    id     = "landing-raw-tiering"
    status = "Enabled"
    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER"
    }
    noncurrent_version_expiration {
      noncurrent_days = 365
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
