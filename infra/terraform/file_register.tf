# ---------------------------------------------------------------------------------------------
# Step 3: file register + 12:30 Dublin cut-off check
#   S3 landing --(Object Created)--> EventBridge rule --> Lambda file_register --> DynamoDB
#   EventBridge Scheduler 12:30 Europe/Dublin --> Lambda cutoff_check --> SNS email (one summary)
# ---------------------------------------------------------------------------------------------

# 1. Let the landing bucket announce every new file to EventBridge
resource "aws_s3_bucket_notification" "landing" {
  bucket      = aws_s3_bucket.landing.id
  eventbridge = true
}

# 2. The register: one row per file, grouped by business date
resource "aws_dynamodb_table" "file_register" {
  name         = "${local.name_prefix}-file-register"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk" # business date, e.g. 2026-10-26
  range_key    = "sk" # S3 key of the file

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
}

# 3. Where the daily summary goes
resource "aws_sns_topic" "alerts" {
  name = "${local.name_prefix}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# 4. The Lambda code, zipped by Terraform (both functions share one small package)
data "archive_file" "lambdas" {
  type        = "zip"
  output_path = "${path.module}/build/file_register_lambdas.zip"

  source {
    content  = file("${path.module}/../../src/lambdas/file_register.py")
    filename = "file_register.py"
  }
  source {
    content  = file("${path.module}/../../src/lambdas/cutoff_check.py")
    filename = "cutoff_check.py"
  }
  source {
    content  = file("${path.module}/../../src/lambdas/dublin_time.py")
    filename = "dublin_time.py"
  }
  source {
    content  = file("${path.module}/../../config/counterparties.csv")
    filename = "counterparties.csv"
  }
}

# 5. Staff badge for both Lambdas: only what they need
resource "aws_iam_role" "file_register_lambda" {
  name = "${local.name_prefix}-file-register-lambda"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "file_register_logs" {
  role       = aws_iam_role.file_register_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "file_register_access" {
  name = "access"
  role = aws_iam_role.file_register_lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Sid = "ReadLandingFiles", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.landing.arn}/*" },
      { Sid = "ListLanding", Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.landing.arn },
      { Sid = "Register", Effect = "Allow", Action = ["dynamodb:PutItem", "dynamodb:Query"], Resource = aws_dynamodb_table.file_register.arn },
      { Sid = "Alert", Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.alerts.arn },
    ]
  })
}

# 6. Log groups with a retention period (otherwise logs are kept forever and slowly cost money)
resource "aws_cloudwatch_log_group" "file_register" {
  name              = "/aws/lambda/${local.name_prefix}-file-register"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "cutoff_check" {
  name              = "/aws/lambda/${local.name_prefix}-cutoff-check"
  retention_in_days = 14
}

# 7. The two Lambdas
resource "aws_lambda_function" "file_register" {
  function_name    = "${local.name_prefix}-file-register"
  role             = aws_iam_role.file_register_lambda.arn
  runtime          = "python3.13"
  handler          = "file_register.handler"
  filename         = data.archive_file.lambdas.output_path
  source_code_hash = data.archive_file.lambdas.output_base64sha256
  timeout          = 30
  memory_size      = 256
  environment {
    variables = { FILE_REGISTER_TABLE = aws_dynamodb_table.file_register.name }
  }
  depends_on = [aws_cloudwatch_log_group.file_register]
}

resource "aws_lambda_function" "cutoff_check" {
  function_name    = "${local.name_prefix}-cutoff-check"
  role             = aws_iam_role.file_register_lambda.arn
  runtime          = "python3.13"
  handler          = "cutoff_check.handler"
  filename         = data.archive_file.lambdas.output_path
  source_code_hash = data.archive_file.lambdas.output_base64sha256
  timeout          = 60
  memory_size      = 256
  environment {
    variables = {
      FILE_REGISTER_TABLE = aws_dynamodb_table.file_register.name
      ALERT_TOPIC_ARN     = aws_sns_topic.alerts.arn
    }
  }
  depends_on = [aws_cloudwatch_log_group.cutoff_check]
}

# 8. EventBridge rule: every new file under source=... -> file_register Lambda
resource "aws_cloudwatch_event_rule" "file_landed" {
  name = "${local.name_prefix}-file-landed"
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.landing.id] }
      object = { key = [{ prefix = "source=" }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "file_landed" {
  rule = aws_cloudwatch_event_rule.file_landed.name
  arn  = aws_lambda_function.file_register.arn
  retry_policy {
    maximum_retry_attempts       = 10
    maximum_event_age_in_seconds = 3600
  }
}

resource "aws_lambda_permission" "eventbridge_invoke" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.file_register.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.file_landed.arn
}

# 9. The 12:30 Dublin alarm clock (Mon-Fri), time-zone aware so summer time is handled
resource "aws_iam_role" "scheduler" {
  name = "${local.name_prefix}-scheduler"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "scheduler.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name = "invoke-cutoff-check"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "lambda:InvokeFunction", Resource = aws_lambda_function.cutoff_check.arn }]
  })
}

resource "aws_scheduler_schedule" "cutoff" {
  name                         = "${local.name_prefix}-cutoff-1230-dublin"
  schedule_expression          = "cron(30 12 ? * MON-FRI *)"
  schedule_expression_timezone = "Europe/Dublin"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_lambda_function.cutoff_check.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})
  }
}
