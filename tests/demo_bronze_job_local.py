"""
Run the Bronze Glue job on a laptop: fake S3 (moto server) + local Spark + output to a local folder.

  RUN 1  land the 401 sample files, run the job             -> 8 Bronze tables, 0 quarantined
  RUN 2  a broken file arrives (garbage bytes as a PDF)       -> job still succeeds, 1 file quarantined,
                                                                every other table has the SAME row count
  RUN 3  the broken file is removed and the job re-runs     -> quarantine folder for the day is cleared
Then read the Parquet back like Athena would and check the planted scenarios.

Needs: pip install pyspark==3.5.4 "moto[server]" (and Java 17)
"""
import json, os, shutil, subprocess, sys, tempfile

import boto3
from moto.server import ThreadedMotoServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "ingestion"))
from upload_to_landing import run as land

DATE, BUCKET = "2026-10-26", "marginwatch-dev-landing-123456789012-ap-south-1"
import logging
logging.getLogger("werkzeug").setLevel(logging.ERROR)   # quiet the fake S3 server
server = ThreadedMotoServer(port=5055, verbose=False)
server.start()
os.environ.update(AWS_ENDPOINT_URL="http://127.0.0.1:5055", AWS_ACCESS_KEY_ID="test",
                  AWS_SECRET_ACCESS_KEY="test", AWS_DEFAULT_REGION="ap-south-1")
s3 = boto3.client("s3")
s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "ap-south-1"})
print("landed:", {r["status"] for r in land(s3, BUCKET, os.path.join(ROOT, "sample_data", "landing"), DATE)},
      len(s3.list_objects_v2(Bucket=BUCKET, MaxKeys=1000)["Contents"]), "files")

out = tempfile.mkdtemp(prefix="bronze_")
env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src", "bronze"), PYSPARK_PYTHON=sys.executable)


def run_job(label):
    cmd = [sys.executable, os.path.join(ROOT, "src", "glue", "bronze_job.py"),
           "--business_date", DATE, "--landing_bucket", BUCKET, "--bronze_path", f"{out}/bronze",
           "--contract_path", os.path.join(ROOT, "config", "bronze_tables.json"), "--summary_path", f"{out}/runs"]
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if p.returncode:
        print(p.stdout[-3000:], p.stderr[-3000:]); sys.exit(1)
    summary = json.loads(p.stdout[p.stdout.index("{"):p.stdout.rindex("}") + 1])
    print(f"\n{label}")
    print(f"  files in landing={summary['files_in_landing']}  loaded={summary['files_loaded']}  "
          f"quarantined={summary['files_quarantined']}")
    for t, v in summary["tables"].items():
        print(f"    {t:26} {v['rows']:5} rows from {v['files']:3} files")
    for q in summary["quarantined"]:
        print(f"  QUARANTINED {q['file']}: {q['error']}")
    return summary


run_job("RUN 1 - normal day")

bad_key = f"source=email/business_date={DATE}/CP999_BADCO_ISDA_MTM_{DATE.replace('-', '')}.pdf"
s3.put_object(Bucket=BUCKET, Key=bad_key, Body=b"this is not really a PDF")
run_job("RUN 2 - a broken file arrives (job must NOT fail, other tables must not change)")

s3.delete_object(Bucket=BUCKET, Key=bad_key)
run_job("RUN 3 - broken file removed, same day re-run")
print("\nquarantine folder for the day still there?",
      os.path.exists(f"{out}/bronze/bronze_quarantine/business_date={DATE}"))

print("\nParquet files written (one per table per day):")
for dirpath, _, files in sorted(os.walk(f"{out}/bronze")):
    for f in files:
        if f.endswith(".parquet"):
            print("  ", os.path.relpath(os.path.join(dirpath, f), out)[:110])

# Read back like Athena would
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
for t in ("bronze_cpty_mtm", "bronze_cpty_margin_call"):
    spark.read.parquet(f"{out}/bronze/{t}").createOrReplaceTempView(t)

print("\nQ1 trades per counterparty file (top 3) - GS INTL should have 400:")
spark.sql("""SELECT cpty_id, agreement_type, source_file, count(*) AS trades FROM bronze_cpty_mtm
             GROUP BY 1,2,3 ORDER BY trades DESC LIMIT 3""").show(truncate=False)
print("Q2 the planted duplicate (Tokyo Tanshi GMRA): Bronze KEEPS it, Silver will remove it")
spark.sql("""SELECT trade_ref, notional, mtm_cpty_view, count(*) AS copies FROM bronze_cpty_mtm
             WHERE cpty_id = 'CP094' GROUP BY 1,2,3 HAVING count(*) > 1""").show(truncate=False)
print("Q3 values are still text exactly as sent (cleaning is Silver's job):")
spark.sql("""SELECT cpty_id, exposure_usd, collateral_held_usd, call_recall_usd FROM bronze_cpty_margin_call
             WHERE cpty_id = 'CP001'""").show(truncate=False)
print("Q4 lineage on every row:")
spark.sql("""SELECT source_key, substr(source_sha256,1,12) AS sha256, received_at_utc, source_row, ingest_run_id
             FROM bronze_cpty_mtm WHERE cpty_id = 'CP001' LIMIT 2""").show(truncate=False)
spark.stop()
server.stop()
shutil.rmtree(out, ignore_errors=True)
