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

output "file_register_table" {
  value = aws_dynamodb_table.file_register.name
}

output "cutoff_check_function" {
  value = aws_lambda_function.cutoff_check.function_name
}

output "lake_buckets" {
  description = "Bronze / silver / gold / artifacts bucket names"
  value       = { for k, b in aws_s3_bucket.lake : k => b.bucket }
}

output "lake_kms_alias" {
  value = aws_kms_alias.lake.name
}

output "agent_bank_secret_name" {
  value = aws_secretsmanager_secret.agent_bank_oauth.name
}

output "bronze_glue_job" {
  value = aws_glue_job.bronze.name
}

output "bronze_database" {
  value = aws_glue_catalog_database.bronze.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.marginwatch.name
}
