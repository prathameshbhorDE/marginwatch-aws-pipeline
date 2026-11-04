"""
Run Bronze -> Silver on a laptop (fake S3 + local Spark, Silver in --mode parquet).

  DAY 1  2026-10-26  land 401 files -> Bronze -> Silver: types, USD, fund-view sign, de-dup, DQ checks
  DAY 2  2026-10-27  same files, but Collateral Hub says GS INTL's MTA went 250k -> 500k
                     -> SCD2: the old version is closed on 26 Oct, a new current version starts 27 Oct
  RE-RUN 2026-10-27  nothing changes (idempotent)

Needs: pip install pyspark==3.5.4 "moto[server]" (and Java 17)
"""
import glob, json, logging, os, shutil, subprocess, sys, tempfile

import boto3
from moto.server import ThreadedMotoServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "ingestion"))
from upload_to_landing import run as land

D1, D2, BUCKET = "2026-10-26", "2026-10-27", "marginwatch-dev-landing-123456789012-ap-south-1"
logging.getLogger("werkzeug").setLevel(logging.ERROR)
server = ThreadedMotoServer(port=5056, verbose=False)
server.start()
os.environ.update(AWS_ENDPOINT_URL="http://127.0.0.1:5056", AWS_ACCESS_KEY_ID="test",
                  AWS_SECRET_ACCESS_KEY="test", AWS_DEFAULT_REGION="ap-south-1")
s3 = boto3.client("s3")
s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "ap-south-1"})
land(s3, BUCKET, os.path.join(ROOT, "sample_data", "landing"), D1)

out = tempfile.mkdtemp(prefix="silver_")
BRONZE, SILVER = f"{out}/bronze", f"{out}/silver"
env = dict(os.environ, PYTHONPATH=os.pathsep.join([os.path.join(ROOT, "src", "bronze"), os.path.join(ROOT, "src", "silver")]),
           PYSPARK_PYTHON=sys.executable)


def run(script, *args):
    p = subprocess.run([sys.executable, os.path.join(ROOT, "src", "glue", script), *args], env=env,
                       capture_output=True, text=True)
    if p.returncode:
        print(p.stdout[-4000:], p.stderr[-4000:]); sys.exit(1)
    return json.loads(p.stdout[p.stdout.index("\n{") + 1:p.stdout.rindex("}") + 1])


def silver(day, label):
    s = run("silver_job.py", "--business_date", day, "--bronze_path", BRONZE, "--mode", "parquet",
            "--silver_path", SILVER, "--summary_path", f"{out}/runs")
    print(f"\n{label}")
    for t, v in s["tables"].items():
        print(f"    {t:30} silver {v['rows']:5}  (bronze {v['bronze_rows']:5}, duplicates removed {v['duplicates_removed']})")
    print(f"  DQ: {s['dq']['checks']} checks, {s['dq']['pass']} pass, {len(s['dq']['warn'])} warn, {len(s['dq']['fail'])} fail")
    for x in s["dq"]["warn"]:
        print("    WARN", x)
    for x in s["dq"]["fail"]:
        print("    FAIL", x)


run("bronze_job.py", "--business_date", D1, "--landing_bucket", BUCKET, "--bronze_path", BRONZE,
    "--contract_path", os.path.join(ROOT, "config", "bronze_tables.json"))
print("Bronze built for", D1)
silver(D1, f"DAY 1 - Silver for {D1}")

# Day 2: copy every Bronze table to 27 Oct; Collateral Hub now says GS INTL's MTA is 500,000
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
for tdir in glob.glob(f"{BRONZE}/bronze_*"):
    src, dst = f"{tdir}/business_date={D1}", f"{tdir}/business_date={D2}"
    if not os.path.exists(src):
        continue
    if tdir.endswith("bronze_ch_agreements"):
        (spark.read.parquet(src)
              .withColumn("mta", F.when(F.col("agreement_id") == "ORION-MF-CP001-ISDA", F.lit("500000.00")).otherwise(F.col("mta")))
              .withColumn("last_modified", F.lit("2026-10-27T07:30:00+00:00"))
              .coalesce(1).write.parquet(dst))
    else:
        shutil.copytree(src, dst)
silver(D2, f"DAY 2 - Silver for {D2} (GS INTL MTA changed in Collateral Hub)")
silver(D2, f"RE-RUN - Silver for {D2} again (must change nothing)")

S = lambda t: spark.read.parquet(f"{SILVER}/{t}")
print("\nSCD2 history for GS INTL (silver_agreements):")
S("silver_agreements").filter("agreement_id = 'ORION-MF-CP001-ISDA'") \
    .select("agreement_id", "mta", "threshold", "valid_from", "valid_to", "is_current").orderBy("valid_from").show(truncate=False)
h = S("silver_agreements")
print(f"silver_agreements: {h.count()} rows, {h.filter('is_current').count()} current\n")

print("Counterparty trades: their view -> our view, and USD (GS INTL, 2 rows, day 1):")
S("silver_trades_cpty").filter(f"business_date = '{D1}' AND agreement_id = 'ORION-MF-CP001-ISDA'") \
    .select("trade_ref", "notional", "ccy", "mtm_cpty_view", "mtm_fund_view", "mtm_fund_view_usd").orderBy("trade_ref").show(2)
print("Same trades from the agent bank (our golden source):")
S("silver_trades_bank").filter(f"business_date = '{D1}' AND agreement_id = 'ORION-MF-CP001-ISDA'") \
    .select("trade_id", "notional", "ccy", "mtm", "mtm_usd").orderBy("trade_id").show(2)
print("Margin call notice, parsed (GS INTL):")
S("silver_margin_calls").filter(f"business_date = '{D1}' AND cpty_id = 'CP001'") \
    .select("agreement_id", "valuation_date", "exposure_usd", "collateral_held_usd", "call_amount_usd",
            "call_direction", "settlement_date").show(truncate=False)
print("The planted duplicate (Tokyo Tanshi) after Silver:")
S("silver_trades_cpty").filter(f"business_date = '{D1}' AND trade_ref = 'CP094-G0001'") \
    .select("agreement_id", "trade_ref", "mtm_fund_view", "duplicate_count").show()
print("Data quality results for day 1 (not PASS):")
S("silver_dq_results").filter(f"business_date = '{D1}' AND status <> 'PASS'") \
    .select("table_name", "check_name", "severity", "status", "failed_rows", "details").show(truncate=60)
spark.stop()
server.stop()
shutil.rmtree(out, ignore_errors=True)
