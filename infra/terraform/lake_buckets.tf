# The data lake: one bucket per layer, all encrypted with our own KMS key.
#
#   landing   -> raw files exactly as received        (already built, s3_landing.tf)
#   bronze    -> the same data as Parquet tables, values still as text
#   silver    -> cleaned, typed, de-duplicated
#   gold      -> star schema the business reads (Athena / Power BI)
#   artifacts -> Glue scripts, Python libraries, Athena query results
#
# Separate buckets (not folders in one bucket) so each layer gets its own access rules:
# e.g. Power BI's role can read gold and nothing else.
locals {
  lake_buckets = {
    bronze    = "Parquet copy of every source file with values kept as text"
    silver    = "Cleaned typed and de-duplicated tables"
    gold      = "Star schema for Athena and Power BI"
    artifacts = "Glue scripts libraries and Athena query results"
  }
}

resource "aws_s3_bucket" "lake" {
  for_each      = local.lake_buckets
  bucket        = "${local.name_prefix}-${each.key}-${data.aws_caller_identity.current.account_id}-${var.region}"
  force_destroy = var.allow_destroy_with_data
  tags          = { Layer = each.key, Purpose = each.value }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  for_each                = aws_s3_bucket.lake
  bucket                  = each.value.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  for_each = aws_s3_bucket.lake
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Every new object is encrypted with OUR key (SSE-KMS).
# bucket_key_enabled: S3 makes one short-lived bucket-level key from our KMS key and reuses it,
# instead of calling KMS for every single file. Glue writes many files -> far fewer KMS calls, lower cost.
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  for_each = aws_s3_bucket.lake
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true
  }
}

# Versioning only on artifacts (roll back a bad Glue script).
# Bronze/silver/gold are rebuilt from the layer below, so old versions would just cost storage.
resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.lake["artifacts"].id
  versioning_configuration {
    status = "Enabled"
  }
}

# Refuse any request that is not over HTTPS (encryption in transit, not only at rest).
data "aws_iam_policy_document" "lake_tls_only" {
  for_each = aws_s3_bucket.lake
  statement {
    sid     = "DenyNonTLS"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [each.value.arn, "${each.value.arn}/*"]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  for_each   = aws_s3_bucket.lake
  bucket     = each.value.id
  policy     = data.aws_iam_policy_document.lake_tls_only[each.key].json
  depends_on = [aws_s3_bucket_public_access_block.lake]
}

# Housekeeping: clean failed uploads everywhere; Athena results are throw-away after 30 days.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  for_each   = aws_s3_bucket.lake
  bucket     = each.value.id
  depends_on = [aws_s3_bucket_versioning.artifacts]

  rule {
    id     = "abort-failed-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  dynamic "rule" {
    for_each = each.key == "artifacts" ? [1] : []
    content {
      id     = "expire-athena-results"
      status = "Enabled"
      filter {
        prefix = "athena-results/"
      }
      expiration {
        days = 30
      }
      noncurrent_version_expiration {
        noncurrent_days = 30
      }
    }
  }
}
