"""
MarginWatch - Glue PySpark job: landing -> Bronze (Parquet), for ONE business date.

  landing (raw Excel / PDF / CSV, 401 files)                 bronze (Parquet tables, values as text)
  source=email/business_date=2026-10-26/...xlsx   ---->      bronze_cpty_mtm/business_date=2026-10-26/part-....parquet
  source=agent_bank/business_date=2026-10-26/...csv  --->    bronze_bank_trades_mtm/business_date=2026-10-26/...
  bad file                                        ---->      bronze_quarantine/business_date=2026-10-26/...

How the work is split:
  - the driver lists the day's files in landing (one S3 LIST per source)
  - Spark spreads the file NAMES over the executors; each executor opens its own files from S3 and
    parses them (Excel/PDF parsing is plain Python, so we parallelise by file)
  - one Parquet file per table per day (coalesce(1)): the data is small, and many tiny files make
    Athena slow
  - re-running a day replaces only that day's folder (dynamic partition overwrite) -> idempotent

Runs on AWS Glue 5.0 (Spark 3.5, Python 3.11), and on a laptop for testing (pip install pyspark).
"""
import json, os, sys, uuid
from collections import Counter

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bronze"))  # laptop runs
from bronze_core import QUARANTINE, list_landing_keys, load_contract, now_utc, process_object, table_columns

try:                                    # on Glue
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    ON_GLUE = True
except ImportError:                     # on a laptop
    ON_GLUE = False

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

ARGS = ["JOB_NAME", "business_date", "landing_bucket", "bronze_path", "contract_path", "summary_path"]


def get_args():
    if ON_GLUE:
        return getResolvedOptions(sys.argv, ARGS)
    import argparse
    ap = argparse.ArgumentParser()
    for a in ARGS:
        ap.add_argument(f"--{a}", default="local-bronze-job" if a == "JOB_NAME" else None)
    return vars(ap.parse_args())


def read_text(path):
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return open(path, encoding="utf-8").read()


def write_text(path, text):
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, "w", encoding="utf-8").write(text)


def clear_partition(path):
    """A table with no rows today: remove an old copy of today's folder, so a re-run never leaves stale rows."""
    if path.startswith("s3://"):
        bucket, prefix = path[5:].split("/", 1)
        s3 = boto3.client("s3")
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
    else:
        import shutil
        shutil.rmtree(path, ignore_errors=True)


def main():
    args = get_args()
    business_date, landing = args["business_date"], args["landing_bucket"]
    bronze_path = args["bronze_path"].rstrip("/")

    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")   # replace only this day's folder
    if ON_GLUE:
        job = Job(GlueContext(spark.sparkContext))
        job.init(args["JOB_NAME"], args)

    tables, lineage = load_contract(read_text(args["contract_path"]))
    run_id = f"{business_date}-{uuid.uuid4().hex[:8]}"
    ingested_at = now_utc()

    keys = list_landing_keys(boto3.client("s3"), landing, business_date)
    if not keys:
        raise RuntimeError(f"no files in landing for business_date={business_date}: nothing to load")
    print(f"[bronze] run {run_id}: {len(keys)} files in landing for {business_date}")

    def parse_partition(key_iter):                # runs on the executors
        s3 = boto3.client("s3")                   # one client per partition, not per file
        for key in key_iter:
            for table, row in process_object(s3, landing, key, tables, lineage, run_id, ingested_at):
                yield table, row

    parsed = spark.sparkContext.parallelize(keys, min(len(keys), 32)).mapPartitions(parse_partition).cache()
    rows_per_table = dict(parsed.map(lambda tr: tr[0]).countByValue())

    for table in tables:
        cols = table_columns(tables, lineage, table)
        part_path = f"{bronze_path}/{table}/business_date={business_date}"
        n = rows_per_table.get(table, 0)
        if n == 0:
            clear_partition(part_path)
            continue
        schema = StructType([StructField(c, StringType(), True) for c in cols + ["business_date"]])
        data = parsed.filter(lambda tr, t=table: tr[0] == t) \
                     .map(lambda tr, c=cols: tuple(tr[1].get(x) for x in c) + (business_date,))
        (spark.createDataFrame(data, schema)
              .coalesce(1)
              .write.mode("overwrite")
              .partitionBy("business_date")
              .parquet(f"{bronze_path}/{table}/"))
        print(f"[bronze]   {table:26} {n:6} rows -> {part_path}/")

    quarantined = parsed.filter(lambda tr: tr[0] == QUARANTINE) \
                        .map(lambda tr: {"file": tr[1]["source_file"], "error": tr[1]["error"]}).collect()
    files_per_table = Counter(t for t, _ in parsed.map(lambda tr: (tr[0], tr[1]["source_key"])).distinct().collect())
    summary = {
        "business_date": business_date, "run_id": run_id, "finished_at_utc": now_utc(),
        "files_in_landing": len(keys), "files_quarantined": len(quarantined),
        "files_loaded": len(keys) - len(quarantined),
        "tables": {t: {"rows": rows_per_table.get(t, 0), "files": files_per_table.get(t, 0)} for t in tables},
        "quarantined": quarantined,
    }
    print(json.dumps(summary, indent=2))
    if args.get("summary_path"):
        write_text(f"{args['summary_path'].rstrip('/')}/business_date={business_date}/{run_id}.json",
                   json.dumps(summary, indent=2))
    parsed.unpersist()
    if ON_GLUE:
        job.commit()
    spark.stop()


if __name__ == "__main__":
    main()
