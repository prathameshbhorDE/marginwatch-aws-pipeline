# ---------------------------------------------------------------------------------------------
# Silver: Glue PySpark job  bronze (Parquet, text)  ->  silver (Iceberg tables, typed, checked)
# ---------------------------------------------------------------------------------------------

locals {
  silver_bucket = aws_s3_bucket.lake["silver"].bucket
  iceberg_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lake["silver"].bucket}/",
  ])
}

# 1. Code ---------------------------------------------------------------------------------------
data "archive_file" "silver_lib" {
  type        = "zip"
  output_path = "${path.module}/build/silver_lib.zip"
  source {
    content  = file("${path.module}/../../src/silver/silver_transforms.py")
    filename = "silver_transforms.py"
  }
}

resource "aws_s3_object" "silver_script" {
  bucket      = local.artifacts_bucket
  key         = "glue/scripts/silver_job.py"
  source      = "${path.module}/../../src/glue/silver_job.py"
  source_hash = filemd5("${path.module}/../../src/glue/silver_job.py")
}

resource "aws_s3_object" "silver_lib" {
  bucket      = local.artifacts_bucket
  key         = "glue/lib/silver_lib.zip"
  source      = data.archive_file.silver_lib.output_path
  source_hash = data.archive_file.silver_lib.output_md5
}

# 2. Catalog database: Iceberg tables are created inside it by the job, stored in the silver bucket
resource "aws_glue_catalog_database" "silver" {
  name         = "${replace(local.name_prefix, "-", "_")}_silver" # marginwatch_dev_silver
  description  = "MarginWatch Silver: typed, de-duplicated, quality-checked Iceberg tables"
  location_uri = "s3://${local.silver_bucket}"
}

# 3. Its own role: read bronze, write silver (a Silver bug can never touch landing or bronze) ---
resource "aws_iam_role" "glue_silver" {
  name = "${local.name_prefix}-glue-silver"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_silver_service" {
  role       = aws_iam_role.glue_silver.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_silver_access" {
  name = "data-access"
  role = aws_iam_role.glue_silver.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBuckets", Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"],
        Resource = [aws_s3_bucket.lake["bronze"].arn, aws_s3_bucket.lake["silver"].arn, aws_s3_bucket.lake["artifacts"].arn]
      },
      { Sid = "ReadBronze", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.lake["bronze"].arn}/*" },
      {
        Sid      = "WriteSilver", Effect = "Allow",
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
        Resource = "${aws_s3_bucket.lake["silver"].arn}/*"
      },
      { Sid = "ReadCode", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.lake["artifacts"].arn}/glue/*" },
      {
        Sid    = "WriteTempLogsRuns", Effect = "Allow",
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
        Resource = [
          "${aws_s3_bucket.lake["artifacts"].arn}/glue/temp/*",
          "${aws_s3_bucket.lake["artifacts"].arn}/glue/sparkui/*",
          "${aws_s3_bucket.lake["artifacts"].arn}/runs/silver/*",
        ]
      },
      { Sid = "UseOurKey", Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.lake.arn },
    ]
  })
}

# 4. The job ----------------------------------------------------------------------------------
resource "aws_glue_job" "silver" {
  name              = "${local.name_prefix}-silver"
  description       = "bronze Parquet -> silver Iceberg (types, USD, de-dup, SCD2 agreements, DQ gate), one business date per run"
  role_arn          = aws_iam_role.glue_silver.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${local.artifacts_bucket}/${aws_s3_object.silver_script.key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1 # one writer at a time for the SCD2 MERGE
  }

  default_arguments = {
    "--business_date"         = "2026-10-26"
    "--bronze_path"           = "s3://${local.bronze_bucket}"
    "--mode"                  = "iceberg"
    "--catalog"               = "glue_catalog"
    "--database"              = aws_glue_catalog_database.silver.name
    "--silver_path"           = "s3://${local.silver_bucket}"
    "--summary_path"          = "s3://${local.artifacts_bucket}/runs/silver"
    "--extra-py-files"        = "s3://${local.artifacts_bucket}/${aws_s3_object.silver_lib.key}"
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

output "silver_glue_job" {
  value = aws_glue_job.silver.name
}

output "silver_database" {
  value = aws_glue_catalog_database.silver.name
}
