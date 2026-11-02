# MarginWatch's own encryption key (a "customer-managed KMS key").
#
# SSE-S3 (what the landing bucket uses) = AWS holds the key; we can't control or audit who uses it.
# Customer-managed KMS key               = WE own the key: IAM decides who may use it, every use is
#                                          logged in CloudTrail, and disabling the key locks the data.
#
# Used by: bronze / silver / gold / artifacts buckets and the agent bank secret.
resource "aws_kms_key" "lake" {
  description             = "${local.name_prefix} data lake and secrets key"
  enable_key_rotation     = true # AWS makes new key material every year; old data still decrypts
  rotation_period_in_days = 365
  deletion_window_in_days = 7 # a deleted key can be recovered for 7 days (minimum allowed)
  # No explicit key policy: AWS's default policy lets IAM policies in this account control the key.
  # In a bank the key policy would split key ADMINS (security team) from key USERS (job roles).
}

# A friendly name, so policies and the console show "alias/marginwatch-dev-lake" instead of a UUID.
resource "aws_kms_alias" "lake" {
  name          = "alias/${local.name_prefix}-lake"
  target_key_id = aws_kms_key.lake.key_id
}
