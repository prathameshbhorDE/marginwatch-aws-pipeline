"""
MarginWatch - Glue PySpark job: Silver (Iceberg) -> Gold (Iceberg), one business date per run.

Step 1: fact_trade_mtm = every trade, our MTM vs the counterparty's, with a recon status
        (MATCHED / MTM_BREAK / MISSING_AT_CPTY / UNKNOWN_AT_US / NO_STATEMENT), tolerances from config.
Step 2: fact_margin_call = one row per agreement / PB account: exposure, requirement, collateral,
        net excess/deficit, summary and action for the client, plus the counterparty's own call.
        Star schema around it: dim_counterparty, dim_agreement (SCD2 versions), dim_date.

Checks (written to gold_dq_results; an ERROR fails the job after writing):
  - every Silver bank trade and counterparty trade appears exactly once in fact_trade_mtm
  - every agreement valid that day (and every PB account) has exactly one margin row; maths adds up
Breaks and calls are NOT errors: they are the business output ops act on.

--mode iceberg (AWS)  /  --mode parquet (laptop test, same logic)
"""
import json, os, sys, uuid
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "gold"))

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    ON_GLUE = True
except ImportError:
    ON_GLUE = False

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

import gold_margin as M
import gold_recon as R

ARGS = ["JOB_NAME", "business_date", "mode", "catalog", "silver_database", "gold_database",
        "silver_path", "gold_path", "config_path", "summary_path"]


def get_args():
    if ON_GLUE:
        return getResolvedOptions(sys.argv, ARGS)
    import argparse
    ap = argparse.ArgumentParser()
    for a in ARGS:
        ap.add_argument(f"--{a}", default={"JOB_NAME": "local-gold-job", "mode": "parquet", "catalog": "local",
                                            "silver_database": "marginwatch_dev_silver",
                                            "gold_database": "marginwatch_dev_gold"}.get(a))
    return vars(ap.parse_args())


def main():
    a = get_args()
    d = a["business_date"]
    spark = SparkSession.builder.appName(a["JOB_NAME"]).getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if ON_GLUE:
        job = Job(GlueContext(spark.sparkContext))
        job.init(a["JOB_NAME"], a)
    iceberg = a["mode"] == "iceberg"
    gold_ns = f"{a['catalog']}.{a['gold_database']}"
    if iceberg:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {gold_ns}")
    run_id = f"{d}-gold-{uuid.uuid4().hex[:8]}"

    def silver_all(t):
        return (spark.table(f"{a['catalog']}.{a['silver_database']}.{t}") if iceberg
                else spark.read.parquet(f"{a['silver_path'].rstrip('/')}/{t}"))

    def silver(t):
        return silver_all(t).filter(F.col("business_date") == F.lit(d).cast("date"))

    def config(name):
        return spark.read.option("header", True).csv(f"{a['config_path'].rstrip('/')}/{name}")

    def replace_all(df, t):
        """Small dimension tables: replace every row (Iceberg keeps the old version as a snapshot)."""
        if not iceberg:
            df.write.mode("overwrite").parquet(f"{a['gold_path'].rstrip('/')}/{t}")
            return
        name = f"{gold_ns}.{t}"
        try:
            spark.sql(f"DESCRIBE TABLE {name}")
            df.writeTo(name).overwrite(F.lit(True))
        except AnalysisException:
            df.writeTo(name).using("iceberg").tableProperty("format-version", "2").create()

    def replace_day(df, t):
        df = (df.withColumn("business_date", F.lit(d).cast("date")).withColumn("gold_run_id", F.lit(run_id))
                .withColumn("processed_at_utc", F.lit(datetime.now(timezone.utc).replace(microsecond=0)).cast("timestamp")))
        if not iceberg:
            df.write.mode("overwrite").partitionBy("business_date").parquet(f"{a['gold_path'].rstrip('/')}/{t}")
            return
        name = f"{gold_ns}.{t}"
        try:
            spark.sql(f"DESCRIBE TABLE {name}")
            df.writeTo(name).overwritePartitions()
        except AnalysisException:
            df.writeTo(name).using("iceberg").partitionedBy(F.col("business_date")) \
              .tableProperty("format-version", "2").create()

    bank, cpty = silver("silver_trades_bank").cache(), silver("silver_trades_cpty").cache()
    n_bank, n_cpty = bank.count(), cpty.count()
    if n_bank == 0:
        raise RuntimeError(f"no Silver bank trades for {d}: run Silver first")
    tol = config("gold_tolerances.csv")

    fact = R.build_fact_trade_mtm(bank, cpty, tol).cache()
    status = {r["recon_status"]: r["count"] for r in fact.groupBy("recon_status").count().collect()}

    dq = []

    def check(name, failed, total, details=""):
        dq.append({"table_name": "fact_trade_mtm", "check_name": name, "severity": "ERROR",
                   "status": "PASS" if failed == 0 else "FAIL", "failed_rows": int(failed), "total_rows": int(total),
                   "details": details})

    ours_in_gold = fact.filter(F.col("mtm_ours_usd").isNotNull()).count()
    theirs_in_gold = fact.filter(F.col("mtm_theirs_usd").isNotNull()).count()
    check("every_bank_trade_once", abs(ours_in_gold - n_bank), n_bank, f"silver {n_bank}, gold {ours_in_gold}")
    check("every_cpty_trade_once", abs(theirs_in_gold - n_cpty), n_cpty, f"silver {n_cpty}, gold {theirs_in_gold}")
    check("unique_agreement_trade", fact.groupBy("agreement_id", "trade_id").count().filter("count > 1").count(), fact.count())
    check("tolerance_resolved", fact.filter(F.col("tolerance_abs_usd").isNull()).count(), fact.count())

    # ---- step 2: margin call per agreement + star schema ----
    hist = silver_all("silver_agreements").cache()
    on_day = M.agreements_on(hist, d).cache()
    pb = silver("silver_pb_margin")
    prime_brokers = config("prime_brokers.csv")
    margin = M.build_fact_margin_call(d, on_day, bank, silver("silver_fx_rates"), silver("silver_collateral_balances"),
                                      fact, silver("silver_margin_calls"), pb, config("margin_rules.csv"),
                                      prime_brokers)
    margin = margin.withColumn("agreement_version_key",
                               F.when(F.col("agreement_valid_from").isNotNull(),
                                      F.concat_ws("|", "agreement_id", F.col("agreement_valid_from").cast("string")))).cache()
    n_margin, n_expected = margin.count(), on_day.count() + pb.count()

    def check2(name, failed, total, details=""):
        dq.append({"table_name": "fact_margin_call", "check_name": name, "severity": "ERROR",
                   "status": "PASS" if failed == 0 else "FAIL", "failed_rows": int(failed), "total_rows": int(total),
                   "details": details})

    check2("one_row_per_agreement_or_pb_account", abs(n_margin - n_expected), n_expected,
           f"expected {n_expected} (agreements valid on {d} + PB accounts), got {n_margin}")
    check2("unique_agreement", margin.groupBy("agreement_id").count().filter("count > 1").count(), n_margin)
    check2("requirement_not_negative", margin.filter("total_requirement_usd < 0").count(), n_margin)
    check2("net_equals_collateral_minus_requirement",
           margin.filter("net_excess_deficit_usd <> collateral_post_haircut_usd - total_requirement_usd").count(), n_margin)
    check2("summary_and_action_set", margin.filter("summary IS NULL OR action IS NULL").count(), n_margin)

    replace_day(fact, "fact_trade_mtm")
    replace_day(margin, "fact_margin_call")
    replace_all(M.build_dim_counterparty(config("counterparties.csv"), prime_brokers), "dim_counterparty")
    replace_all(M.build_dim_agreement(hist), "dim_agreement")
    replace_all(M.build_dim_date(spark), "dim_date")
    replace_day(spark.createDataFrame(dq).select("table_name", "check_name", "severity", "status", "failed_rows",
                                                 "total_rows", "details"), "gold_dq_results")

    breaks = [r.asDict() for r in fact.filter("is_break")
              .select("agreement_id", "trade_id", "recon_status", "mtm_ours_usd", "mtm_theirs_usd", "difference_usd")
              .orderBy("agreement_id", "trade_id").limit(50).collect()]
    fails = [c for c in dq if c["status"] == "FAIL"]
    by_summary = {f"{r['summary']} / {r['action']}": r["count"]
                  for r in margin.groupBy("summary", "action").count().orderBy("summary", "action").collect()}
    approvals = [r.asDict() for r in margin.filter("action = 'Client approval required'")
                 .select("agreement_id", "cpty_short_name", "net_excess_deficit_usd", "summary")
                 .orderBy("net_excess_deficit_usd").collect()]
    disputes = [r.asDict() for r in margin.filter("call_agreed = false")
                .select("agreement_id", "net_excess_deficit_usd", "cpty_call_amount_usd", "cpty_call_direction",
                        "call_difference_usd", "trade_breaks").collect()]
    summary = {"business_date": d, "run_id": run_id, "mode": a["mode"],
               "fact_trade_mtm": {"rows": fact.count(), "silver_bank_trades": n_bank, "silver_cpty_trades": n_cpty,
                                  "by_status": {s: status.get(s, 0) for s in R.STATUSES}},
               "breaks": breaks,
               "fact_margin_call": {"rows": n_margin, "by_summary_action": by_summary,
                                    "client_approval_required": approvals, "calls_not_agreed": disputes},
               "dq": {"checks": len(dq), "pass": len(dq) - len(fails), "fail": [f"{c['check_name']}: {c['details']}" for c in fails]}}
    body = json.dumps(summary, indent=2, default=str)
    print(body)
    if a.get("summary_path"):
        path = f"{a['summary_path'].rstrip('/')}/business_date={d}/{run_id}.json"
        if path.startswith("s3://"):
            import boto3
            bkt, key = path[5:].split("/", 1)
            boto3.client("s3").put_object(Bucket=bkt, Key=key, Body=body.encode())
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            open(path, "w").write(body)
    if fails:
        raise RuntimeError(f"Gold checks FAILED for {d}: {[c['check_name'] for c in fails]}")
    if ON_GLUE:
        job.commit()
    spark.stop()


if __name__ == "__main__":
    main()
