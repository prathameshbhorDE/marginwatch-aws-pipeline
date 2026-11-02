"""
MarginWatch - incremental (watermark-based CDC) pull from the Collateral Hub PostgreSQL database.

For each table:
  1. read the last watermark (the last_modified value we safely reached last time) from DynamoDB
  2. read the database clock ONCE -> this run's upper bound
     SELECT ... WHERE last_modified >  (watermark - overlap)
                  AND last_modified <= upper_bound
     (first run: no watermark -> full load)
  3. write the rows as a CSV extract and land it in S3 (same idempotent landing as every other source)
  4. ONLY AFTER the landing succeeded, move the watermark forward to upper_bound

Why the overlap: a row's last_modified is stamped when its transaction runs, but other sessions
only see it at COMMIT. A long transaction can commit a row whose timestamp is already behind our
watermark, and a strict "> watermark" would skip it forever. Re-reading a small window (default 5 min)
costs a few duplicate rows, which Silver removes with a MERGE on the business key.

Usage:
  python src/ingestion/collateral_hub_pull.py --bucket <landing bucket> --business-date 2026-10-26
"""
import argparse, csv, io, json, os, tempfile
from datetime import timedelta

import boto3
import psycopg

from upload_to_landing import land_file

TABLES = {  # table -> columns to extract (business key first)
    "agreements": ["agreement_id", "fund_id", "cpty_id", "cpty_short_name", "cpty_legal_name", "agreement_type",
                   "base_ccy", "threshold", "mta", "im_fund", "im_cpty", "last_modified"],
    "collateral_balances": ["balance_id", "agreement_id", "as_of_date", "collateral_type", "isin", "quantity",
                            "price", "haircut_pct", "currency", "market_value_usd", "post_haircut_value_usd",
                            "last_modified"],
}


class WatermarkStore:
    """One DynamoDB item per source table: {pk: 'collateral_hub.agreements', watermark: '2026-...'}"""

    def __init__(self, table_name, region):
        self.table = boto3.resource("dynamodb", region_name=region).Table(table_name)

    def get(self, key):
        item = self.table.get_item(Key={"pk": key}).get("Item")
        return item["watermark"] if item else None

    def set(self, key, value, rows, s3_key):
        self.table.put_item(Item={"pk": key, "watermark": value, "last_rows": rows, "last_extract": s3_key})


def pull_table(conn, s3, bucket, wm_store, table, business_date, overlap_min, log=print):
    cols = TABLES[table]
    key = f"collateral_hub.{table}"
    watermark = wm_store.get(key)
    upper = conn.execute("SELECT clock_timestamp()").fetchone()[0]          # one fixed upper bound per run

    if watermark is None:
        sql, params, mode = (f"SELECT {', '.join(cols)} FROM {table} WHERE last_modified <= %s ORDER BY last_modified",
                             (upper,), "FULL LOAD")
    else:
        sql = (f"SELECT {', '.join(cols)} FROM {table} "
               f"WHERE last_modified > %s::timestamptz - %s::interval AND last_modified <= %s ORDER BY last_modified")
        params, mode = (watermark, f"{overlap_min} minutes", upper), "INCREMENTAL"
    rows = conn.execute(sql, params).fetchall()

    stamp = upper.strftime("%Y%m%dT%H%M%S%fZ")
    file_name = f"{table}_cdc_{stamp}.csv"
    s3_key = f"source=collateral_hub/business_date={business_date}/{file_name}"
    if rows:                                                                 # nothing changed -> no empty file
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(cols)
        for r in rows:
            w.writerow([v.isoformat() if hasattr(v, "isoformat") else v for v in r])
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, file_name)
            with open(path, "w", newline="", encoding="utf-8") as f:
                f.write(buf.getvalue())
            land_file(s3, bucket, {"local_path": path, "key": s3_key, "metadata": {
                "source-system": "collateral_hub", "business-date": business_date,
                "received-at-utc": upper.isoformat(), "message-id": f"{table}:{stamp}",
                "sender": "collateral-hub-postgres"}})
    wm_store.set(key, upper.isoformat(), len(rows), s3_key if rows else "-")   # only after landing succeeded
    log(f"  {table:20} {mode:11} from={watermark or '-'}  rows={len(rows):4}  "
        f"-> {s3_key if rows else '(nothing new, no file)'}")
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.environ.get("CH_DSN", "postgresql://localhost/collateral_hub"))
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--business-date", required=True)
    ap.add_argument("--watermark-table", default="marginwatch-dev-watermarks")
    ap.add_argument("--region", default="ap-south-1")
    ap.add_argument("--overlap-minutes", type=int, default=5)
    a = ap.parse_args()
    s3 = boto3.client("s3", region_name=a.region)
    store = WatermarkStore(a.watermark_table, a.region)
    with psycopg.connect(a.dsn, options="-c TimeZone=UTC") as conn:   # all timestamps in UTC
        total = sum(pull_table(conn, s3, a.bucket, store, t, a.business_date, a.overlap_minutes) for t in TABLES)
    print(json.dumps({"rows_extracted": total}))


if __name__ == "__main__":
    main()
