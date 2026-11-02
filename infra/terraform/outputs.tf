output "landing_bucket" {
  description = "Name of the S3 landing bucket (pass it to upload_to_landing.py --bucket)"
  value       = aws_s3_bucket.landing.bucket
}

output "account_id" {
  value     = data.aws_caller_identity.current.account_id
  sensitive = true
}

output "watermark_table" {
  description = "DynamoDB table holding the incremental-load watermarks"
  value       = aws_dynamodb_table.watermarks.name
}
