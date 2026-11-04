"""
Bronze -> Silver -> Gold on a laptop: fake S3 + local Spark, --mode parquet.

  RUN 1  default tolerances (5,000 USD and 5%)       -> 1 MTM break, 1 missing at cpty, 1 unknown at us
         + margin call per agreement, compared cell by cell with the answer key
  RUN 2  a one-line override for GS INTL: 15,000 USD  -> GS INTL's 10,892 USD difference is now MATCHED
         (shows: limits are config, not code; most specific rule wins)

Needs: pip install pyspark==3.5.4 "moto[server]" (and Java 17)
"""
import json, logging, os, shutil, subprocess, sys, tempfile

import boto3
from moto.server import ThreadedMotoServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "ingestion"))
from upload_to_landing import run as land

D, BUCKET = "2026-10-26", "marginwatch-dev-landing-123456789012-ap-south-1"
logging.getLogger("werkzeug").setLevel(logging.ERROR)
server = ThreadedMotoServer(port=5057, verbose=False)
server.start()
os.environ.update(AWS_ENDPOINT_URL="http://127.0.0.1:5057", AWS_ACCESS_KEY_ID="test",
                  AWS_SECRET_ACCESS_KEY="test", AWS_DEFAULT_REGION="ap-south-1")
s3 = boto3.client("s3")
s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "ap-south-1"})
land(s3, BUCKET, os.path.join(ROOT, "sample_data", "landing"), D)

out = tempfile.mkdtemp(prefix="gold_")
env = dict(os.environ, PYSPARK_PYTHON=sys.executable,
           PYTHONPATH=os.pathsep.join(os.path.join(ROOT, "src", p) for p in ("bronze", "silver", "gold")))


def run(script, *args):
    p = subprocess.run([sys.executable, os.path.join(ROOT, "src", "glue", script), *args], env=env,
                       capture_output=True, text=True)
    if p.returncode:
        print(p.stdout[-4000:], p.stderr[-4000:]); sys.exit(1)
    start = 0 if p.stdout.startswith("{") else p.stdout.index("\n{") + 1
    return json.loads(p.stdout[start:p.stdout.rindex("}") + 1])


run("bronze_job.py", "--business_date", D, "--landing_bucket", BUCKET, "--bronze_path", f"{out}/bronze",
    "--contract_path", os.path.join(ROOT, "config", "bronze_tables.json"))
run("silver_job.py", "--business_date", D, "--bronze_path", f"{out}/bronze", "--silver_path", f"{out}/silver")
print("Bronze and Silver built for", D)


def gold(label, config_dir):
    s = run("gold_job.py", "--business_date", D, "--silver_path", f"{out}/silver", "--gold_path", f"{out}/gold",
            "--config_path", config_dir, "--summary_path", f"{out}/runs")
    f = s["fact_trade_mtm"]
    print(f"\n{label}")
    print(f"  fact_trade_mtm rows={f['rows']}  (silver bank trades {f['silver_bank_trades']}, cpty trades {f['silver_cpty_trades']})")
    for st, n in f["by_status"].items():
        print(f"    {st:16} {n:5}")
    for b in s["breaks"]:
        print(f"  BREAK {b['agreement_id']} {b['trade_id']:12} {b['recon_status']:16} ours={b['mtm_ours_usd']} "
              f"theirs={b['mtm_theirs_usd']} diff={b['difference_usd']}")
    m = s["fact_margin_call"]
    print(f"  fact_margin_call rows={m['rows']}")
    for k, v in m["by_summary_action"].items():
        print(f"    {k:55} {v:4}")
    for x in m["client_approval_required"]:
        print(f"  APPROVAL {x['agreement_id']:22} {x['cpty_short_name']:14} net={x['net_excess_deficit_usd']:>14}  {x['summary']}")
    for x in m["calls_not_agreed"]:
        print(f"  CALL NOT AGREED {x['agreement_id']}: ours net={x['net_excess_deficit_usd']} theirs={x['cpty_call_direction']} "
              f"{x['cpty_call_amount_usd']} diff={x['call_difference_usd']} trade breaks={x['trade_breaks']}")
    print(f"  checks: {s['dq']['checks']}, fail: {s['dq']['fail']}")


gold("RUN 1 - default tolerances (5,000 USD and 5%)", os.path.join(ROOT, "config"))
print("\nCompare RUN 1 with the answer key:")
subprocess.run([sys.executable, os.path.join(ROOT, "tests", "check_gold_vs_answer_key.py"), "--date", D,
                "--local-gold-path", f"{out}/gold"], env=env)

override = f"{out}/config_override"
shutil.copytree(os.path.join(ROOT, "config"), override)
with open(f"{override}/gold_tolerances.csv", "a") as f:
    f.write("AGREEMENT,ORION-MF-CP001-ISDA,15000,0.05,example: bigger limit for one large client\n")
gold("RUN 2 - one-line override: GS INTL ISDA limit 15,000 USD", override)

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
from pyspark.sql import functions as F
print("\nBiggest differences in GS INTL and CP018 after RUN 2, and why each is or isn't a break:")
(spark.read.parquet(f"{out}/gold/fact_trade_mtm")
      .filter("agreement_id IN ('ORION-MF-CP001-ISDA', 'ORION-MF-CP018-ISDA')")
      .orderBy(F.abs("difference_usd").desc_nulls_last())
      .select("trade_id", "mtm_ours_usd", "mtm_theirs_usd", "difference_usd", "difference_pct",
              "tolerance_abs_usd", "tolerance_source", "recon_status").show(4, truncate=False))
spark.stop()
server.stop()
shutil.rmtree(out, ignore_errors=True)
