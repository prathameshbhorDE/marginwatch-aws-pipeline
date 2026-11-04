"""
MarginWatch - Glue PySpark job: Bronze (Parquet, text) -> Silver (Iceberg, typed, de-duplicated, checked).

One run = one business date:
  1. read that day's Bronze folders (a missing source is logged, not fatal)
  2. transform (src/silver/silver_transforms.py): types, USD amounts, fund-view sign, agreement keys, de-dup
  3. write each daily table: replace ONLY that day's partition (idempotent re-runs)
  4. agreements: SCD2 history with an Iceberg MERGE (close the old version, insert the new one)
  5. data quality checks -> silver_dq_results; any ERROR fails the job AFTER writing the results,
     so Gold never runs on bad data, and the reason is queryable in Athena

--mode iceberg  (AWS Glue: tables in the Glue Data Catalog, Iceberg format, readable/writable by Athena)
--mode parquet  (laptop test: same logic, plain Parquet folders; Iceberg's library can't be downloaded there)
"""
import json, os, sys, uuid
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "silver"))          # laptop runs; on Glue: --extra-py-files

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

import silver_transforms as T

ARGS = ["JOB_NAME", "business_date", "bronze_path", "mode", "catalog", "database", "silver_path", "summary_path"]

DAILY = {  # silver table -> (bronze table it comes from, business key that must never be NULL)
    "silver_fx_rates": ("bronze_fx_rates", ["ccy", "to_usd"]),
    "silver_trades_bank": ("bronze_bank_trades_mtm", ["agreement_id", "trade_id", "mtm"]),
    "silver_trades_cpty": ("bronze_cpty_mtm", ["agreement_id", "trade_ref", "mtm_cpty_view"]),
    "silver_margin_calls": ("bronze_cpty_margin_call", ["agreement_id", "call_amount_usd", "call_direction"]),
    "silver_pb_margin": ("bronze_pb_margin", ["account", "excess_deficit_usd"]),
    "silver_holdings": ("bronze_bank_holdings", ["account", "isin", "nominal"]),
    "silver_collateral_balances": ("bronze_ch_balances", ["agreement_id", "collateral_type", "post_haircut_value_usd"]),
}
NUMERIC_TEXT = {  # bronze columns that must turn into numbers (a value that doesn't = ERROR)
    "bronze_fx_rates": ["to_usd"], "bronze_bank_trades_mtm": ["notional", "mtm"],
    "bronze_cpty_mtm": ["notional", "mtm_cpty_view"],
    "bronze_cpty_margin_call": ["exposure_usd", "im_client_usd", "im_cpty_usd", "collateral_held_usd"],
    "bronze_pb_margin": ["total_equity_usd", "total_requirement_usd", "excess_deficit_usd"],
    "bronze_bank_holdings": ["nominal", "price", "market_value"],
    "bronze_ch_balances": ["quantity", "price", "haircut_pct", "market_value_usd", "post_haircut_value_usd"],
    "bronze_ch_agreements": ["threshold", "mta", "im_fund", "im_cpty"],
}


def get_args():
    if ON_GLUE:
        return getResolvedOptions(sys.argv, ARGS)
    import argparse
    ap = argparse.ArgumentParser()
    for a in ARGS:
        ap.add_argument(f"--{a}", default={"JOB_NAME": "local-silver-job", "mode": "parquet", "catalog": "local",
                                            "database": "marginwatch_dev_silver"}.get(a))
    return vars(ap.parse_args())


class Writer:
    """Hides the one difference between AWS (Iceberg tables) and a laptop (Parquet folders)."""

    def __init__(self, spark, a):
        self.spark, self.mode = spark, a["mode"]
        self.ns, self.path = f"{a['catalog']}.{a['database']}", (a.get("silver_path") or "").rstrip("/")

    def name(self, t):
        return f"{self.ns}.{t}"

    def exists(self, t):
        if self.mode == "parquet":
            return os.path.exists(f"{self.path}/{t}")
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.name(t)}")
            return True
        except AnalysisException:
            return False

    def read(self, t):
        return self.spark.read.parquet(f"{self.path}/{t}") if self.mode == "parquet" else self.spark.table(self.name(t))

    def replace_day(self, df, t):
        """Replace only this business date's partition."""
        if self.mode == "parquet":
            df.write.mode("overwrite").partitionBy("business_date").parquet(f"{self.path}/{t}")
        elif not self.exists(t):
            df.writeTo(self.name(t)).using("iceberg").partitionedBy(F.col("business_date")) \
              .tableProperty("format-version", "2").create()
        else:
            df.writeTo(self.name(t)).overwritePartitions()

    def scd2_agreements(self, staged, run_id):
        t = "silver_agreements"
        if self.mode == "parquet":                    # laptop: same rule, as DataFrame code
            history = self.read(t).cache() if self.exists(t) else None
            result = T.scd2_apply(history, staged, run_id).cache()
            result.count()
            tmp = f"{self.path}/_tmp_{t}"
            result.write.mode("overwrite").parquet(tmp)
            self.spark.read.parquet(tmp).write.mode("overwrite").parquet(f"{self.path}/{t}")
            return
        cols = ["agreement_id"] + T.AGREEMENT_ATTRS
        if not self.exists(t):
            ddl = ", ".join(f"{c} {'decimal(20,2)' if c in ('threshold', 'mta', 'im_fund', 'im_cpty') else 'string'}"
                            for c in cols)
            self.spark.sql(f"""CREATE TABLE {self.name(t)} ({ddl}, valid_from date, valid_to date,
                               is_current boolean, row_hash string, created_run_id string, updated_run_id string)
                               USING iceberg TBLPROPERTIES ('format-version'='2')""")
        staged.withColumn("run_id", F.lit(run_id)).createOrReplaceTempView("staged_agreements")
        target = self.name(t)
        # Classic SCD2 MERGE: each changed agreement appears twice in the source -
        #   with its key   -> matches the current row -> close it
        #   with NULL key  -> never matches            -> inserted as the new current version
        self.spark.sql(f"""
            MERGE INTO {target} t
            USING (
                SELECT s.agreement_id AS merge_key, s.* FROM staged_agreements s
                UNION ALL
                SELECT NULL AS merge_key, s.* FROM staged_agreements s
                JOIN {target} c ON c.agreement_id = s.agreement_id AND c.is_current AND c.row_hash <> s.row_hash
            ) u
            ON t.agreement_id = u.merge_key AND t.is_current
            WHEN MATCHED AND t.row_hash <> u.row_hash THEN
                UPDATE SET t.is_current = false, t.valid_to = date_sub(u.business_date, 1), t.updated_run_id = u.run_id
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(cols)}, valid_from, valid_to, is_current, row_hash, created_run_id, updated_run_id)
                VALUES ({", ".join("u." + c for c in cols)},
                        CASE WHEN u.merge_key IS NULL THEN u.business_date ELSE u.valid_from_if_new END,
                        DATE '9999-12-31', true, u.row_hash, u.run_id, u.run_id)
        """)


def main():
    a = get_args()
    d, bronze = a["business_date"], a["bronze_path"].rstrip("/")
    builder = SparkSession.builder.appName(a["JOB_NAME"])
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.ansi.enabled", "false")       # an unreadable number becomes NULL, then DQ counts it
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if ON_GLUE:
        job = Job(GlueContext(spark.sparkContext))
        job.init(a["JOB_NAME"], a)
    w = Writer(spark, a)
    if a["mode"] == "iceberg":
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {w.ns}")
    run_id = f"{d}-silver-{uuid.uuid4().hex[:8]}"
    fs = spark._jvm.org.apache.hadoop.fs.Path(bronze).getFileSystem(spark._jsc.hadoopConfiguration())

    def bronze_day(table):
        p = f"{bronze}/{table}/business_date={d}"
        if not fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p)):
            return None
        return spark.read.parquet(p).cache()

    dq, stats, written = [], {}, {}

    def check(table, name, severity, failed, total, details=""):
        dq.append({"table_name": table, "check_name": name, "severity": severity, "failed_rows": int(failed),
                   "total_rows": int(total), "status": "PASS" if failed == 0 else ("FAIL" if severity == "ERROR" else "WARN"),
                   "details": details[:500]})

    src = {t: bronze_day(t) for t in {v[0] for v in DAILY.values()} | {"bronze_ch_agreements"}}
    for t, df in src.items():
        if df is None:
            check(t, "source_present_for_day", "WARN", 1, 1, f"no {t} partition for {d}: Silver tables from it are skipped")
        else:
            for c in NUMERIC_TEXT.get(t, []):
                bad = df.filter(F.trim(F.col(c)) != "").filter(T.to_num(c).isNull())
                n = bad.count()
                check(t, f"numeric_parse:{c}", "ERROR", n, df.count(),
                      "; ".join(r[c] for r in bad.select(c).limit(3).collect()) if n else "")

    # FX first: every other table converts to USD with it
    fx = None
    if src["bronze_fx_rates"] is not None:
        fx, stats["silver_fx_rates"] = T.build_fx(src["bronze_fx_rates"])
        fx = fx.cache()
        written["silver_fx_rates"] = fx
    agreements = None
    if src["bronze_ch_agreements"] is not None:
        agreements, stats["silver_agreements(staged)"] = T.build_agreements_staged(src["bronze_ch_agreements"], d)
        agreements = agreements.cache()

    builders = {
        "silver_trades_bank": lambda b: T.build_bank_trades(b, fx),
        "silver_trades_cpty": lambda b: T.build_cpty_trades(b, fx, agreements),
        "silver_margin_calls": T.build_margin_calls,
        "silver_pb_margin": T.build_pb_margin,
        "silver_holdings": lambda b: T.build_holdings(b, fx),
        "silver_collateral_balances": T.build_collateral_balances,
    }
    needs = {"silver_trades_bank": [fx], "silver_trades_cpty": [fx, agreements], "silver_holdings": [fx]}
    for t, build in builders.items():
        b = src[DAILY[t][0]]
        if b is None or any(x is None for x in needs.get(t, [])):
            continue
        df, stats[t] = build(b)
        written[t] = df.cache()

    processed_at = datetime.now(timezone.utc).replace(microsecond=0)
    for t, df in written.items():
        n = df.count()
        s = stats[t]
        check(t, "rows_reconcile_bronze", "ERROR", abs(s["bronze_rows"] - n - s["duplicates_removed"]), s["bronze_rows"],
              f"bronze {s['bronze_rows']} = silver {n} + duplicates removed {s['duplicates_removed']}")
        if s["duplicates_removed"]:
            check(t, "duplicates_removed", "WARN", s["duplicates_removed"], s["bronze_rows"],
                  "kept one row per business key")
        for k in DAILY[t][1]:
            check(t, f"not_null:{k}", "ERROR", df.filter(F.col(k).isNull()).count(), n)
        for usd in [c for c in df.columns if c.endswith("_usd") and c not in DAILY[t][1]]:
            if t in ("silver_trades_bank", "silver_trades_cpty", "silver_holdings"):
                check(t, f"fx_rate_found:{usd}", "ERROR", df.filter(F.col(usd).isNull()).count(), n)
        if "isin_valid" in df.columns:
            bad = df.filter(F.col("isin_valid") == False)  # noqa: E712
            check(t, "isin_check_digit", "WARN", bad.count(), n,
                  ", ".join(f"{r['isin']}" for r in bad.select("isin").limit(5).collect()))
        out = (df.withColumn("business_date", F.lit(d).cast("date"))
                 .withColumn("silver_run_id", F.lit(run_id))
                 .withColumn("processed_at_utc", F.lit(processed_at).cast("timestamp")))
        w.replace_day(out, t)
        print(f"[silver]   {t:28} {n:6} rows")

    if agreements is not None:
        s, n = stats["silver_agreements(staged)"], agreements.count()
        check("silver_agreements", "rows_reconcile_bronze", "ERROR", abs(s["bronze_rows"] - n - s["duplicates_removed"]),
              s["bronze_rows"], f"bronze {s['bronze_rows']} = staged {n} + older versions removed {s['duplicates_removed']}")
        w.scd2_agreements(agreements, run_id)
        hist = w.read("silver_agreements")
        cur = hist.filter("is_current")
        check("silver_agreements", "one_current_row_per_agreement", "ERROR",
              cur.groupBy("agreement_id").count().filter("count > 1").count(), cur.count())
        print(f"[silver]   silver_agreements (SCD2)     {hist.count():6} rows, {cur.count()} current")

    dq_df = spark.createDataFrame(dq).select("table_name", "check_name", "severity", "status", "failed_rows",
                                             "total_rows", "details") \
                 .withColumn("business_date", F.lit(d).cast("date")).withColumn("silver_run_id", F.lit(run_id))
    w.replace_day(dq_df, "silver_dq_results")

    errors = [c for c in dq if c["status"] == "FAIL"]
    tables = {t: {"rows": written[t].count(), **stats[t]} for t in written}
    if agreements is not None:
        tables["silver_agreements(staged)"] = {"rows": agreements.count(), **stats["silver_agreements(staged)"]}
    summary = {"business_date": d, "run_id": run_id, "mode": a["mode"], "tables": tables,
               "dq": {"checks": len(dq), "pass": sum(c["status"] == "PASS" for c in dq),
                      "warn": [f"{c['table_name']}.{c['check_name']}: {c['failed_rows']} ({c['details']})"
                               for c in dq if c["status"] == "WARN"],
                      "fail": [f"{c['table_name']}.{c['check_name']}: {c['failed_rows']} ({c['details']})" for c in errors]}}
    print(json.dumps(summary, indent=2, default=str))
    if a.get("summary_path"):
        body = json.dumps(summary, indent=2, default=str)
        path = f"{a['summary_path'].rstrip('/')}/business_date={d}/{run_id}.json"
        if path.startswith("s3://"):
            import boto3
            bkt, key = path[5:].split("/", 1)
            boto3.client("s3").put_object(Bucket=bkt, Key=key, Body=body.encode())
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            open(path, "w").write(body)
    if errors:
        raise RuntimeError(f"Silver quality gate FAILED for {d}: {len(errors)} check(s) - see silver_dq_results")
    if ON_GLUE:
        job.commit()
    spark.stop()


if __name__ == "__main__":
    main()
