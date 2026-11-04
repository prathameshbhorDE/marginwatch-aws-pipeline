# ---------------------------------------------------------------------------------------------
# Gold: Glue PySpark job  silver (Iceberg)  ->  gold (Iceberg): reconciliation, then margin calls
# ---------------------------------------------------------------------------------------------

# 1. Code ---------------------------------------------------------------------------------------
data "archive_file" "gold_lib" {
  type        = "zip"
  output_path = "${path.module}/build/gold_lib.zip"
  source {
    content  = file("${path.module}/../../src/gold/gold_recon.py")
    filename = "gold_recon.py"
  }
  source {
    content  = file("${path.module}/../../src/gold/gold_margin.py")
    filename = "gold_margin.py"
  }
}

resource "aws_s3_object" "gold_script" {
  bucket      = local.artifacts_bucket
  key         = "glue/scripts/gold_job.py"
  source      = "${path.module}/../../src/glue/gold_job.py"
  source_hash = filemd5("${path.module}/../../src/glue/gold_job.py")
}

# Business rules are config, not code: tolerances, approval limits, prime brokers, counterparty master
resource "aws_s3_object" "gold_tolerances" {
  bucket      = local.artifacts_bucket
  key         = "glue/config/gold/gold_tolerances.csv"
  source      = "${path.module}/../../config/gold_tolerances.csv"
  source_hash = filemd5("${path.module}/../../config/gold_tolerances.csv")
}

resource "aws_s3_object" "gold_config" {
  for_each    = toset(["margin_rules.csv", "prime_brokers.csv", "counterparties.csv"])
  bucket      = local.artifacts_bucket
  key         = "glue/config/gold/${each.value}"
  source      = "${path.module}/../../config/${each.value}"
  source_hash = filemd5("${path.module}/../../config/${each.value}")
}

resource "aws_s3_object" "gold_lib" {
  bucket      = local.artifacts_bucket
  key         = "glue/lib/gold_lib.zip"
  source      = data.archive_file.gold_lib.output_path
  source_hash = data.archive_file.gold_lib.output_md5
}

# 2. Catalog database for Gold Iceberg tables, stored in the gold bucket
resource "aws_glue_catalog_database" "gold" {
  name         = "${replace(local.name_prefix, "-", "_")}_gold" # marginwatch_dev_gold
  description  = "MarginWatch Gold: reconciliation and margin facts (Iceberg), read by Athena and Power BI"
  location_uri = "s3://${aws_s3_bucket.lake["gold"].bucket}"
}

# 3. Its own role: read silver + tolerances, write gold ------------------------------------------
resource "aws_iam_role" "glue_gold" {
  name = "${local.name_prefix}-glue-gold"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_gold_service" {
  role       = aws_iam_role.glue_gold.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_gold_access" {
  name = "data-access"
  role = aws_iam_role.glue_gold.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBuckets", Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"],
        Resource = [aws_s3_bucket.lake["silver"].arn, aws_s3_bucket.lake["gold"].arn, aws_s3_bucket.lake["artifacts"].arn]
      },
      { Sid = "ReadSilver", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.lake["silver"].arn}/*" },
      {
        Sid      = "WriteSilver", Effect = "Allow",
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
        Resource = "${aws_s3_bucket.lake["gold"].arn}/*"
      },
      { Sid = "ReadCode", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.lake["artifacts"].arn}/glue/*" },
      {
        Sid    = "WriteTempLogsRuns", Effect = "Allow",
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
        Resource = [
          "${aws_s3_bucket.lake["artifacts"].arn}/glue/temp/*",
          "${aws_s3_bucket.lake["artifacts"].arn}/glue/sparkui/*",
          "${aws_s3_bucket.lake["artifacts"].arn}/runs/gold/*",
        ]
      },
      { Sid = "UseOurKey", Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.lake.arn },
    ]
  })
}

# 4. The job ----------------------------------------------------------------------------------
resource "aws_glue_job" "gold" {
  name              = "${local.name_prefix}-gold"
  description       = "silver -> gold Iceberg: trade reconciliation, margin call per agreement, star schema; one business date per run"
  role_arn          = aws_iam_role.glue_gold.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${local.artifacts_bucket}/${aws_s3_object.gold_script.key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--business_date"         = "2026-10-26"
    "--mode"                  = "iceberg"
    "--catalog"               = "glue_catalog"
    "--silver_database"       = aws_glue_catalog_database.silver.name
    "--gold_database"         = aws_glue_catalog_database.gold.name
    "--silver_path"           = "s3://${local.silver_bucket}"
    "--gold_path"             = "s3://${aws_s3_bucket.lake["gold"].bucket}"
    "--config_path"           = "s3://${local.artifacts_bucket}/glue/config/gold"
    "--summary_path"          = "s3://${local.artifacts_bucket}/runs/gold"
    "--extra-py-files"        = "s3://${local.artifacts_bucket}/${aws_s3_object.gold_lib.key}"
    "--datalake-formats"      = "iceberg"
    "--conf"                  = local.iceberg_conf
    "--TempDir"               = "s3://${local.artifacts_bucket}/glue/temp/"
    "--enable-spark-ui"       = "true"
    "--spark-event-logs-path" = "s3://${local.artifacts_bucket}/glue/sparkui/"
    "--enable-metrics"        = "true"
    "--job-bookmark-option"   = "job-bookmark-disable"
    "--job-language"          = "python"
  }
}

output "gold_glue_job" {
  value = aws_glue_job.gold.name
}

output "gold_database" {
  value = aws_glue_catalog_database.gold.name
}
