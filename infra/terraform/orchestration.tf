# ---------------------------------------------------------------------------------------------
# Orchestration: one Step Functions state machine runs the whole day
#   cut-off check (Lambda) -> gate -> Bronze -> Silver -> Gold (Glue) -> report (Lambda) -> SNS email
#   any failure -> alert email -> execution FAILED (visible in the console, step by step)
# Triggered by the 12:30 Europe/Dublin schedule (file_register.tf) or by hand for a re-run.
# ---------------------------------------------------------------------------------------------

# 1. Report Lambda ------------------------------------------------------------------------------
data "archive_file" "run_report" {
  type        = "zip"
  output_path = "${path.module}/build/run_report.zip"
  source {
    content  = file("${path.module}/../../src/lambdas/run_report.py")
    filename = "run_report.py"
  }
}

resource "aws_iam_role" "run_report" {
  name = "${local.name_prefix}-run-report-lambda"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "run_report_logs" {
  role       = aws_iam_role.run_report.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "run_report_access" {
  name = "read-run-summaries"
  role = aws_iam_role.run_report.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "ListRuns", Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.lake["artifacts"].arn,
        Condition = { StringLike = { "s3:prefix" = ["runs/*"] } }
      },
      { Sid = "ReadRuns", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.lake["artifacts"].arn}/runs/*" },
      { Sid = "UseOurKey", Effect = "Allow", Action = ["kms:Decrypt"], Resource = aws_kms_key.lake.arn },
    ]
  })
}

resource "aws_cloudwatch_log_group" "run_report" {
  name              = "/aws/lambda/${local.name_prefix}-run-report"
  retention_in_days = 14
}

resource "aws_lambda_function" "run_report" {
  function_name    = "${local.name_prefix}-run-report"
  role             = aws_iam_role.run_report.arn
  runtime          = "python3.13"
  handler          = "run_report.handler"
  filename         = data.archive_file.run_report.output_path
  source_code_hash = data.archive_file.run_report.output_base64sha256
  timeout          = 30
  memory_size      = 256
  environment {
    variables = { ARTIFACTS_BUCKET = aws_s3_bucket.lake["artifacts"].bucket }
  }
  depends_on = [aws_cloudwatch_log_group.run_report]
}

# 1b. Amazon SES for the formatted (HTML) daily report. SNS stays for alerts.
#     Creating the identity makes AWS send a verification email: click the link once.
#     New SES accounts are in the "sandbox": they can only send to verified addresses - fine here (we send to ourselves).
resource "aws_sesv2_email_identity" "report" {
  email_identity = var.alert_email
}

# 2. The state machine's own role: start these 3 Glue jobs, call these 2 Lambdas, email ---------
resource "aws_iam_role" "pipeline" {
  name = "${local.name_prefix}-daily-pipeline"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "states.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "pipeline" {
  name = "run-pipeline"
  role = aws_iam_role.pipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "InvokeLambdas", Effect = "Allow", Action = ["lambda:InvokeFunction"],
        Resource = [aws_lambda_function.cutoff_check.arn, aws_lambda_function.run_report.arn]
      },
      {
        Sid      = "RunGlueJobs", Effect = "Allow",
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
        Resource = [aws_glue_job.bronze.arn, aws_glue_job.silver.arn, aws_glue_job.gold.arn]
      },
      { Sid = "Alerts", Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.alerts.arn },
      { Sid = "ReportEmail", Effect = "Allow", Action = ["ses:SendEmail"], Resource = aws_sesv2_email_identity.report.arn },
      {
        Sid = "ExecutionLogs", Effect = "Allow",
        Action = ["logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"]
        Resource = "*" # these log-delivery actions do not support resource-level permissions
      },
    ]
  })
}

resource "aws_cloudwatch_log_group" "pipeline" {
  name              = "/aws/vendedlogs/states/${local.name_prefix}-daily-pipeline"
  retention_in_days = 14
}

# 3. The state machine (definition kept as a separate JSON file, readable on its own) -------------
resource "aws_sfn_state_machine" "daily" {
  name     = "${local.name_prefix}-daily-pipeline"
  role_arn = aws_iam_role.pipeline.arn
  type     = "STANDARD" # exactly-once, full history kept for 90 days; EXPRESS is for high-volume short runs
  definition = templatefile("${path.module}/statemachine/daily_pipeline.asl.json", {
    cutoff_check_arn = aws_lambda_function.cutoff_check.arn
    run_report_arn   = aws_lambda_function.run_report.arn
    bronze_job       = aws_glue_job.bronze.name
    silver_job       = aws_glue_job.silver.name
    gold_job         = aws_glue_job.gold.name
    topic_arn        = aws_sns_topic.alerts.arn
    report_email     = aws_sesv2_email_identity.report.email_identity
  })
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.pipeline.arn}:*"
    include_execution_data = false # step names and errors only, no business data in the logs
    level                  = "ERROR"
  }
}

output "daily_pipeline_arn" {
  value = aws_sfn_state_machine.daily.arn
}

# 4. 15:00 Dublin UPDATE run: same pipeline, picks up statements that arrived after the 12:30 cut-off.
#    Every layer replaces the day's data, so late files are added and nothing is doubled.
#    notify=false: the cut-off Lambda stays quiet; the report email says what changed since 12:30.
resource "aws_scheduler_schedule" "update_1500" {
  name                         = "${local.name_prefix}-update-1500-dublin"
  schedule_expression          = "cron(0 15 ? * MON-FRI *)"
  schedule_expression_timezone = "Europe/Dublin"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_sfn_state_machine.daily.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ run_type = "UPDATE_1500", notify = false })
  }
}
