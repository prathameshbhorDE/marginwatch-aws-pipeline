"""
Compare Gold's fact_margin_call with the answer key, row by row and column by column.

The generator wrote the expected margin summary BEFORE any pipeline code existed
(sample_data/expected/expected_margin_summary_<yyyymmdd>.csv). If Gold reproduces it exactly,
the whole chain - parsing, cleaning, de-duplication, FX, sign flip, SCD2 terms, margin maths - is right.

  AWS:     AWS_PROFILE=marginwatch python tests/check_gold_vs_answer_key.py --date 2026-10-26
  laptop:  python tests/check_gold_vs_answer_key.py --date 2026-10-26 --local-gold-path <folder>/gold
"""
import argparse, csv, os, sys, time
from decimal import Decimal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NUMS = ["exposure_usd", "im_fund_usd", "im_cpty_usd", "total_requirement_usd", "collateral_post_haircut_usd",
        "net_excess_deficit_usd", "mta_usd"]
TEXT = ["cpty_id", "agreement_type", "summary", "action"]


def from_athena(date, workgroup, database, region):
    import boto3
    ath = boto3.client("athena", region_name=region)
    q = (f"SELECT agreement_id, {', '.join(NUMS + TEXT)} FROM fact_margin_call "
         f"WHERE business_date = DATE '{date}'")
    qid = ath.start_query_execution(QueryString=q, WorkGroup=workgroup,
                                    QueryExecutionContext={"Database": database})["QueryExecutionId"]
    while True:
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if st["State"] != "SUCCEEDED":
        raise SystemExit(f"Athena query {st['State']}: {st.get('StateChangeReason')}")
    rows, header = [], None
    for page in ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            vals = [c.get("VarCharValue") for c in r["Data"]]
            if header is None:
                header = vals
                continue
            rows.append(dict(zip(header, vals)))
    return rows


def from_local(path, date):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(f"{path}/fact_margin_call").filter(f"business_date = '{date}'")
    rows = [{k: (None if v is None else str(v)) for k, v in r.asDict().items()} for r in df.collect()]
    spark.stop()
    return rows


def num(v):
    return None if v in (None, "") else Decimal(v)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--local-gold-path")
    ap.add_argument("--workgroup", default="marginwatch-dev")
    ap.add_argument("--database", default="marginwatch_dev_gold")
    ap.add_argument("--region", default="ap-south-1")
    a = ap.parse_args()

    key_file = os.path.join(ROOT, "sample_data", "expected", f"expected_margin_summary_{a.date.replace('-', '')}.csv")
    expected = {r["agreement_id"]: r for r in csv.DictReader(open(key_file))}
    got_rows = from_local(a.local_gold_path, a.date) if a.local_gold_path else \
        from_athena(a.date, a.workgroup, a.database, a.region)
    got = {r["agreement_id"]: r for r in got_rows}

    missing, extra = sorted(set(expected) - set(got)), sorted(set(got) - set(expected))
    mismatches, col_ok = [], {c: 0 for c in NUMS + TEXT}
    for k in sorted(set(expected) & set(got)):
        e, g = expected[k], got[k]
        e_cols = {"im_fund_usd": "im_fund_usd", "im_cpty_usd": "im_cpty_usd"}
        for c in NUMS:
            ev, gv = num(e.get(e_cols.get(c, c))), num(g.get(c))
            if ev is None and gv in (None, Decimal(0)) or (ev is not None and gv is not None and abs(ev - gv) <= Decimal("0.01")):
                col_ok[c] += 1
            else:
                mismatches.append((k, c, e.get(c), g.get(c)))
        for c in TEXT:
            if (e.get(c) or "") == (g.get(c) or ""):
                col_ok[c] += 1
            else:
                mismatches.append((k, c, e.get(c), g.get(c)))

    both = len(set(expected) & set(got))
    print(f"answer key rows: {len(expected)}   Gold rows: {len(got)}   compared: {both}")
    print(f"missing in Gold: {missing or 'none'}   extra in Gold: {extra or 'none'}")
    for c in NUMS + TEXT:
        print(f"  {c:30} {col_ok[c]:4} / {both} match")
    for m in mismatches[:20]:
        print("  MISMATCH", m)
    ok = not missing and not extra and not mismatches
    print("\nGOLD MATCHES THE ANSWER KEY EXACTLY" if ok else f"\n{len(mismatches)} cell(s) differ")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
