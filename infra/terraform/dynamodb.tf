# Bookmark for incremental pulls: one item per source table, e.g.
#   { pk = "collateral_hub.agreements", watermark = "2026-10-27T05:59:58Z", last_rows = 1, last_extract = "s3 key" }
resource "aws_dynamodb_table" "watermarks" {
  name         = "${local.name_prefix}-watermarks"
  billing_mode = "PAY_PER_REQUEST" # pay per read/write; a few calls a day costs nothing
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true # undo a bad watermark write by restoring the table to an earlier minute
  }
}
