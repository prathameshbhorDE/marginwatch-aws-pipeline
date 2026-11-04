"""
MarginWatch - Silver transformations (pure PySpark functions, no reading/writing here).

Bronze keeps every value as text exactly as sent. Silver makes it trustworthy:
  - text -> real types       "6,873,353.25" -> 6873353.25    "26 October 2026" -> 2026-10-26
  - split mixed fields       "3,213,233.89 CALL - please deliver" -> amount 3213233.89, direction CALL
  - one view of the sign     counterparty MTM is THEIR view -> flip to the fund's view
  - one key everywhere       counterparty files get the agreement_id the bank and Collateral Hub use
  - one currency             every amount also in USD using the day's FX rates
  - de-duplicate             keep one row per business key, count what was removed
  - flag, don't drop         invalid ISINs are kept with isin_valid = false
Each build_* function returns (silver_df, stats) so the job can reconcile Bronze vs Silver row counts.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

MONEY = "decimal(20,2)"
RATE = "decimal(18,8)"
PRICE = "decimal(18,6)"
QTY = "decimal(20,4)"
PCT = "decimal(9,4)"

LINEAGE = ["source_file", "source_sha256", "ingest_run_id"]

AGREEMENT_ATTRS = ["fund_id", "cpty_id", "cpty_short_name", "cpty_legal_name", "agreement_type", "base_ccy",
                   "threshold", "mta", "im_fund", "im_cpty"]


# ---------- small helpers --------------------------------------------------------------------------
def to_num(col, dtype=MONEY):
    """'5,000,000.00' / ' 37,684.97 ' / '1000000' -> decimal. Anything unreadable -> NULL (counted by DQ)."""
    cleaned = F.regexp_replace(F.trim(F.col(col)), r"[,\s]", "")
    return F.when(cleaned == "", None).otherwise(cleaned).cast(dtype)


def to_day(col, fmt=None):
    c = F.trim(F.col(col))
    return F.to_date(c, fmt) if fmt else F.to_date(c)


def isin_is_valid(isin):
    """ISIN = 2 letters + 9 alphanumerics + 1 check digit (Luhn over the letters converted to numbers)."""
    if isin is None or len(isin) != 12 or not isin[:2].isalpha() or not isin[-1].isdigit() or not isin.isalnum():
        return False
    digits = "".join(str(int(ch, 36)) for ch in isin[:11].upper())
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 0:
            n *= 2
            n = n - 9 if n > 9 else n
        total += n
    return (10 - total % 10) % 10 == int(isin[-1])


isin_valid_udf = F.udf(isin_is_valid, BooleanType())


def add_usd(df, fx, amount_col, ccy_col, out_col):
    """Join the day's FX rate (to_usd) and add an amount in USD."""
    rates = fx.select(F.col("ccy").alias("_fx_ccy"), F.col("to_usd").alias("_to_usd"))
    return (df.join(F.broadcast(rates), F.col(ccy_col) == F.col("_fx_ccy"), "left")
              .withColumn(out_col, F.round(F.col(amount_col) * F.col("_to_usd"), 2).cast(MONEY))
              .drop("_fx_ccy", "_to_usd"))


def dedupe(df, keys, order_cols):
    """Keep the first row per business key (by order_cols); return (deduped, rows_removed)."""
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    ranked = df.withColumn("_rn", F.row_number().over(w)).withColumn("duplicate_count", F.count("*").over(Window.partitionBy(*keys)) - 1)
    kept = ranked.filter("_rn = 1").drop("_rn")
    return kept, ranked.filter("_rn > 1").count()


# ---------- one function per Silver table -----------------------------------------------------------
def build_fx(b):
    out = b.select(to_day("rate_date").alias("rate_date"), F.upper(F.trim("ccy")).alias("ccy"),
                   to_num("to_usd", RATE).alias("to_usd"), *LINEAGE)
    return out, {"bronze_rows": b.count(), "duplicates_removed": 0}


def build_agreements_staged(b, business_date):
    """Collateral Hub agreement terms for the day, one row per agreement (latest version wins).
    Full extracts and CDC extracts can both arrive on one day: the row with the newest last_modified wins."""
    typed = b.select(
        F.trim("agreement_id").alias("agreement_id"), F.trim("fund_id").alias("fund_id"),
        F.trim("cpty_id").alias("cpty_id"), F.trim("cpty_short_name").alias("cpty_short_name"),
        F.trim("cpty_legal_name").alias("cpty_legal_name"), F.upper(F.trim("agreement_type")).alias("agreement_type"),
        F.upper(F.trim("base_ccy")).alias("base_ccy"),
        to_num("threshold").alias("threshold"), to_num("mta").alias("mta"),
        to_num("im_fund").alias("im_fund"), to_num("im_cpty").alias("im_cpty"),
        to_day("effective_from").alias("effective_from"),
        F.to_timestamp("last_modified").alias("last_modified"), *LINEAGE)
    typed = typed.withColumn("effective_from", F.max("effective_from").over(Window.partitionBy("agreement_id")))
    kept, removed = dedupe(typed, ["agreement_id"],
                           [F.col("last_modified").desc_nulls_last(), F.col("source_file").desc()])
    staged = (kept.withColumn("business_date", F.lit(business_date).cast("date"))
                  .withColumn("valid_from_if_new", F.coalesce("effective_from", "business_date"))
                  .withColumn("row_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~"))
                                                                    for c in AGREEMENT_ATTRS]), 256)))
    return staged.drop("duplicate_count"), {"bronze_rows": b.count(), "duplicates_removed": removed}


def build_bank_trades(b, fx):
    """Agent bank = golden source for OUR side. MTM is already in the fund's view, in trade currency."""
    typed = b.select(
        F.trim("fund_id").alias("fund_id"), F.trim("agreement_id").alias("agreement_id"),
        F.trim("cpty_id").alias("cpty_id"), F.trim("trade_id").alias("trade_id"),
        F.upper(F.trim("product")).alias("product"), to_num("notional").alias("notional"),
        F.upper(F.trim("ccy")).alias("ccy"), to_num("mtm").alias("mtm"),
        to_day("valuation_date").alias("valuation_date"), *LINEAGE)
    kept, removed = dedupe(typed, ["agreement_id", "trade_id"], [F.col("source_file")])
    return add_usd(kept, fx, "mtm", "ccy", "mtm_usd"), {"bronze_rows": b.count(), "duplicates_removed": removed}


def build_cpty_trades(b, fx, agreements):
    """Counterparty MTM statements: their view of the same trades.
    - agreement_id: from the file's Agreement column, else looked up in Collateral Hub by (cpty_id, agreement_type)
    - mtm_fund_view = -mtm_cpty_view  (their gain is our loss), so it compares 1:1 with the agent bank
    - duplicates (same agreement + trade sent twice) are removed and counted"""
    lookup = agreements.select(F.col("cpty_id").alias("_cp"), F.col("agreement_type").alias("_at"),
                               F.col("agreement_id").alias("_agr"))
    typed = b.select(
        F.trim("cpty_id").alias("cpty_id"), F.upper(F.trim("agreement_type")).alias("agreement_type"),
        F.when(F.trim("agreement") != "", F.trim("agreement")).alias("_agr_in_file"),
        F.trim("trade_ref").alias("trade_ref"), F.upper(F.trim("product")).alias("product"),
        to_num("notional").alias("notional"), F.upper(F.trim("currency")).alias("ccy"),
        to_num("mtm_cpty_view").alias("mtm_cpty_view"), F.col("source_row"), *LINEAGE)
    joined = (typed.join(F.broadcast(lookup), (F.col("cpty_id") == F.col("_cp")) & (F.col("agreement_type") == F.col("_at")), "left")
                   .withColumn("agreement_id", F.coalesce("_agr_in_file", "_agr"))
                   .drop("_cp", "_at", "_agr", "_agr_in_file")
                   .withColumn("mtm_fund_view", (-F.col("mtm_cpty_view")).cast(MONEY)))
    kept, removed = dedupe(joined, ["agreement_id", "trade_ref"], [F.col("source_file"), F.col("source_row")])
    return add_usd(kept, fx, "mtm_fund_view", "ccy", "mtm_fund_view_usd").drop("source_row"), \
        {"bronze_rows": b.count(), "duplicates_removed": removed}


def build_margin_calls(b):
    """'ORION-MF-CP001-ISDA (ISDA)' -> agreement_id; '3,213,233.89 CALL - please deliver' -> 3213233.89 + CALL."""
    raw = F.trim("call_recall_usd")
    out = b.select(
        F.regexp_extract(F.trim("agreement"), r"^(\S+)", 1).alias("agreement_id"),
        F.trim("cpty_id").alias("cpty_id"), F.upper(F.trim("agreement_type")).alias("agreement_type"),
        to_day("valuation_date", "d MMMM yyyy").alias("valuation_date"),
        to_num("exposure_usd").alias("exposure_usd"), to_num("im_client_usd").alias("im_client_usd"),
        to_num("im_cpty_usd").alias("im_cpty_usd"), to_num("collateral_held_usd").alias("collateral_held_usd"),
        F.regexp_replace(F.regexp_extract(raw, r"^\s*([-0-9,\.]+)", 1), ",", "").cast(MONEY).alias("call_amount_usd"),
        F.when(F.regexp_extract(raw, r"\b(RECALL|CALL)\b", 1) != "", F.regexp_extract(raw, r"\b(RECALL|CALL)\b", 1))
         .alias("call_direction"),
        F.col("call_recall_usd").alias("call_text_as_sent"),
        F.to_date(F.regexp_extract(F.col("settlement"), r"\((\d{1,2} \w+ \d{4})\)", 1), "d MMMM yyyy").alias("settlement_date"),
        F.trim("sender_name").alias("sender_name"), *LINEAGE)
    kept, removed = dedupe(out, ["agreement_id"], [F.col("source_file")])
    return kept.drop("duplicate_count"), {"bronze_rows": b.count(), "duplicates_removed": removed}


def build_pb_margin(b):
    out = b.select(F.trim("account").alias("account"), F.upper(F.trim("account_type")).alias("account_type"),
                   to_num("total_equity_usd").alias("total_equity_usd"),
                   to_num("total_requirement_usd").alias("total_requirement_usd"),
                   to_num("excess_deficit_usd").alias("excess_deficit_usd"), *LINEAGE)
    return out, {"bronze_rows": b.count(), "duplicates_removed": 0}


def build_holdings(b, fx):
    typed = b.select(
        F.trim("account").alias("account"), F.upper(F.trim("isin")).alias("isin"),
        F.trim("security_name").alias("security_name"), to_num("nominal", QTY).alias("nominal"),
        to_num("price", PRICE).alias("price"), to_num("market_value").alias("market_value"),
        F.upper(F.trim("currency")).alias("ccy"), F.trim("pledged_to").alias("pledged_to"),
        to_day("statement_date").alias("statement_date"), *LINEAGE)
    typed = typed.withColumn("isin_valid", isin_valid_udf("isin"))
    kept, removed = dedupe(typed, ["account", "isin"], [F.col("source_file")])
    return add_usd(kept.drop("duplicate_count"), fx, "market_value", "ccy", "market_value_usd"), \
        {"bronze_rows": b.count(), "duplicates_removed": removed}


def build_collateral_balances(b):
    """Collateral Hub balances. Full + CDC extracts can both arrive: newest last_modified wins per item."""
    typed = b.select(
        F.trim("agreement_id").alias("agreement_id"), F.upper(F.trim("collateral_type")).alias("collateral_type"),
        F.when(F.trim("isin") != "", F.upper(F.trim("isin"))).alias("isin"),
        to_num("quantity", QTY).alias("quantity"), to_num("price", PRICE).alias("price"),
        to_num("haircut_pct", PCT).alias("haircut_pct"), F.upper(F.trim("currency")).alias("ccy"),
        to_num("market_value_usd").alias("market_value_usd"),
        to_num("post_haircut_value_usd").alias("post_haircut_value_usd"),
        to_day("as_of_date").alias("as_of_date"), F.to_timestamp("last_modified").alias("last_modified"), *LINEAGE)
    typed = typed.withColumn("isin_valid", F.when(F.col("isin").isNull(), None).otherwise(isin_valid_udf("isin")))
    kept, removed = dedupe(typed, ["agreement_id", "collateral_type", F.coalesce("isin", F.lit(""))],
                           [F.col("last_modified").desc_nulls_last(), F.col("source_file").desc()])
    return kept.drop("duplicate_count"), {"bronze_rows": b.count(), "duplicates_removed": removed}


# ---------- SCD2 for agreements (used on a laptop; on AWS the same rule runs as an Iceberg MERGE) ---
def scd2_apply(history, staged, run_id):
    """history: existing silver_agreements rows (or None). staged: today's agreements (one per id).
    Changed agreement  -> close the current row (valid_to = day before), add a new current row.
    New agreement      -> add a current row.   Unchanged / not in today's extract -> untouched."""
    cols = ["agreement_id"] + AGREEMENT_ATTRS
    new_rows = lambda df, vf: df.select(*cols, vf.alias("valid_from"), F.lit("9999-12-31").cast("date").alias("valid_to"),
                                        F.lit(True).alias("is_current"), "row_hash",
                                        F.lit(run_id).alias("created_run_id"), F.lit(run_id).alias("updated_run_id"))
    if history is None:
        return new_rows(staged, F.col("valid_from_if_new"))
    cur = history.filter("is_current").select("agreement_id", F.col("row_hash").alias("_cur_hash"))
    s = staged.join(cur, "agreement_id", "left")
    changed = s.filter(F.col("_cur_hash").isNotNull() & (F.col("_cur_hash") != F.col("row_hash")))
    brand_new = s.filter(F.col("_cur_hash").isNull())
    close = changed.select("agreement_id", F.date_sub("business_date", 1).alias("_close_to"))
    h = (history.join(close, "agreement_id", "left")
                .withColumn("valid_to", F.when(F.col("is_current") & F.col("_close_to").isNotNull(), F.col("_close_to"))
                            .otherwise(F.col("valid_to")))
                .withColumn("updated_run_id", F.when(F.col("is_current") & F.col("_close_to").isNotNull(), F.lit(run_id))
                            .otherwise(F.col("updated_run_id")))
                .withColumn("is_current", F.col("is_current") & F.col("_close_to").isNull())
                .drop("_close_to"))
    return h.unionByName(new_rows(changed, F.col("business_date"))).unionByName(new_rows(brand_new, F.col("valid_from_if_new")))
