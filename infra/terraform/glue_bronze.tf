# ---------------------------------------------------------------------------------------------
# Bronze: Glue PySpark job  landing (raw Excel/PDF/CSV)  ->  bronze (Parquet)  ->  Athena tables
# ---------------------------------------------------------------------------------------------

locals {
  bronze_bucket    = aws_s3_bucket.lake["bronze"].bucket
  artifacts_bucket = aws_s3_bucket.lake["artifacts"].bucket
  contract         = jsondecode(file("${path.module}/../../config/bronze_tables.json"))
  bronze_tables    = { for k, v in local.contract : k => v if !startswith(k, "_") }
}

# 1. The code Glue runs, stored in the artifacts bucket --------------------------------------
data "archive_file" "bronze_lib" { # our parsers, shipped to every Spark executor
  type        = "zip"
  output_path = "${path.module}/build/bronze_lib.zip"
  source {
    content  = file("${path.module}/../../src/bronze/parsers.py")
    filename = "parsers.py"
  }
  source {
    content  = file("${path.module}/../../src/bronze/bronze_core.py")
    filename = "bronze_core.py"
  }
}

resource "aws_s3_object" "bronze_script" {
  bucket      = local.artifacts_bucket
  key         = "glue/scripts/bronze_job.py"
  source      = "${path.module}/../../src/glue/bronze_job.py"
  source_hash = filemd5("${path.module}/../../src/glue/bronze_job.py") # re-upload when the file changes
}

resource "aws_s3_object" "bronze_lib" {
  bucket      = local.artifacts_bucket
  key         = "glue/lib/bronze_lib.zip"
  source      = data.archive_file.bronze_lib.output_path
  source_hash = data.archive_file.bronze_lib.output_md5
}

resource "aws_s3_object" "bronze_contract" {
  bucket      = local.artifacts_bucket
  key         = "glue/config/bronze_tables.json"
  source      = "${path.module}/../../config/bronze_tables.json"
  source_hash = filemd5("${path.module}/../../config/bronze_tables.json")
}

# 2. The Glue job's staff badge: read landing, write bronze, nothing else --------------------
resource "aws_iam_role" "glue_bronze" {
  name = "${local.name_prefix}-glue-bronze"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" { # logs, metrics, Glue basics
  role       = aws_iam_role.glue_bronze.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_bronze_access" {
  name = "data-access"
  role = aws_iam_role.glue_bronze.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListLanding", Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"],
        Resource = aws_s3_bucket.landing.arn
      },
      { Sid = "ReadLanding", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.landing.arn}/source=*" },
      {
        Sid      = "ListLakeBuckets", Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"],
        Resource = [aws_s3_bucket.lake["bronze"].arn, aws_s3_bucket.lake["artifacts"].arn]
      },
      {
        Sid      = "WriteBronze", Effect = "Allow",
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
        Resource = "${aws_s3_bucket.lake["bronze"].arn}/*"
      },
      { Sid = "ReadCode", Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.lake["artifacts"].arn}/glue/*" },
      {
        Sid    = "WriteTempLogsRuns", Effect = "Allow",
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
        Resource = [
          "${aws_s3_bucket.lake["artifacts"].arn}/glue/temp/*",
          "${aws_s3_bucket.lake["artifacts"].arn}/glue/sparkui/*",
          "${aws_s3_bucket.lake["artifacts"].arn}/runs/bronze/*",
        ]
      },
      { Sid = "UseOurKey", Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.lake.arn },
    ]
  })
}

# 3. The Glue job -----------------------------------------------------------------------------
resource "aws_glue_job" "bronze" {
  name              = "${local.name_prefix}-bronze"
  description       = "landing (Excel/PDF/CSV) -> bronze Parquet, one business date per run"
  role_arn          = aws_iam_role.glue_bronze.arn
  glue_version      = "5.0" # Spark 3.5, Python 3.11
  worker_type       = "G.1X"
  number_of_workers = 2  # the minimum: ~3 MB of files a day
  timeout           = 15 # minutes; a stuck run is killed instead of billing for hours
  max_retries       = 0  # a failed run is investigated, not silently retried

  command {
    name            = "glueetl"
    script_location = "s3://${local.artifacts_bucket}/${aws_s3_object.bronze_script.key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1 # two runs for the same day must never write at the same time
  }

  default_arguments = {
    "--business_date"             = "2026-10-26" # overridden on every real run
    "--landing_bucket"            = aws_s3_bucket.landing.bucket
    "--bronze_path"               = "s3://${local.bronze_bucket}"
    "--contract_path"             = "s3://${local.artifacts_bucket}/${aws_s3_object.bronze_contract.key}"
    "--summary_path"              = "s3://${local.artifacts_bucket}/runs/bronze"
    "--extra-py-files"            = "s3://${local.artifacts_bucket}/${aws_s3_object.bronze_lib.key}"
    "--additional-python-modules" = "openpyxl==3.1.5,pdfplumber==0.11.10"
    "--TempDir"                   = "s3://${local.artifacts_bucket}/glue/temp/"
    "--enable-spark-ui"           = "true"
    "--spark-event-logs-path"     = "s3://${local.artifacts_bucket}/glue/sparkui/"
    "--enable-metrics"            = "true"
    "--job-bookmark-option"       = "job-bookmark-disable" # we pick the day ourselves (--business_date)
    "--job-language"              = "python"
  }
}

# 4. Glue Data Catalog: the "table of contents" Athena reads ----------------------------------
resource "aws_glue_catalog_database" "bronze" {
  name        = "${replace(local.name_prefix, "-", "_")}_bronze" # marginwatch_dev_bronze (no hyphens in SQL names)
  description = "MarginWatch Bronze: every source file as Parquet, values kept as text"
}

# One table per entry in config/bronze_tables.json.
# Partition projection: Athena works out the business_date folders itself, so no crawler and no
# "ADD PARTITION" step is needed when a new day is loaded.
resource "aws_glue_catalog_table" "bronze" {
  for_each      = local.bronze_tables
  name          = each.key
  database_name = aws_glue_catalog_database.bronze.name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification                           = "parquet"
    "projection.enabled"                     = "true"
    "projection.business_date.type"          = "date"
    "projection.business_date.format"        = "yyyy-MM-dd"
    "projection.business_date.range"         = "2026-01-01,NOW"
    "projection.business_date.interval"      = "1"
    "projection.business_date.interval.unit" = "DAYS"
    "storage.location.template"              = "s3://${local.bronze_bucket}/${each.key}/business_date=$${business_date}/"
  }

  partition_keys {
    name = "business_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bronze_bucket}/${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    dynamic "columns" {
      for_each = concat(each.value.file_level, keys(each.value.columns), local.contract["_lineage"])
      content {
        name = columns.value
        type = "string"
      }
    }
  }
}

# 5. Athena workgroup: where query results go, encrypted, with a cost guard -------------------
resource "aws_athena_workgroup" "marginwatch" {
  name          = local.name_prefix
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824 # 1 GB: a runaway query is stopped, not billed
    result_configuration {
      output_location = "s3://${local.artifacts_bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.lake.arn
      }
    }
  }
}
