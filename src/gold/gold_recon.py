"""
MarginWatch - Gold step 1: trade-level MTM reconciliation (pure PySpark, no reading/writing here).

Puts OUR view (agent bank) and THEIR view (counterparty statement, already flipped to the fund's view in
Silver) side by side, one row per trade, and gives every trade a status:

  MATCHED          both sides have it, difference inside tolerance
  MTM_BREAK        both sides have it, difference >= abs_usd AND >= pct of |our MTM|
  MISSING_AT_CPTY  we have the trade, the counterparty's statement doesn't
  UNKNOWN_AT_US    the counterparty lists a trade we don't have
  NO_STATEMENT     we have the trade, but no statement arrived for that agreement at all

Tolerances come from config/gold_tolerances.csv, most specific wins:
  AGREEMENT (one agreement)  >  AGREEMENT_TYPE (ISDA / GMRA)  >  DEFAULT
"""
from pyspark.sql import functions as F

STATUSES = ["MATCHED", "MTM_BREAK", "MISSING_AT_CPTY", "UNKNOWN_AT_US", "NO_STATEMENT"]


def resolve_tolerances(pairs, tol):
    """pairs: one row per (agreement_id, agreement_type). Adds abs_usd, pct, tolerance_source."""
    t = tol.select(F.upper(F.trim("scope_type")).alias("scope_type"), F.trim("scope_value").alias("scope_value"),
                   F.col("abs_usd").cast("decimal(20,2)").alias("abs_usd"), F.col("pct").cast("decimal(9,6)").alias("pct"))
    by_agr = t.filter("scope_type = 'AGREEMENT'").select(F.col("scope_value").alias("_a"),
                                                         F.col("abs_usd").alias("_a_abs"), F.col("pct").alias("_a_pct"))
    by_type = t.filter("scope_type = 'AGREEMENT_TYPE'").select(F.col("scope_value").alias("_t"),
                                                               F.col("abs_usd").alias("_t_abs"), F.col("pct").alias("_t_pct"))
    dflt = t.filter("scope_type = 'DEFAULT'").select(F.col("abs_usd").alias("_d_abs"), F.col("pct").alias("_d_pct")).limit(1)
    return (pairs.join(F.broadcast(by_agr), F.col("agreement_id") == F.col("_a"), "left")
                 .join(F.broadcast(by_type), F.col("agreement_type") == F.col("_t"), "left")
                 .crossJoin(F.broadcast(dflt))
                 .withColumn("tolerance_abs_usd", F.coalesce("_a_abs", "_t_abs", "_d_abs"))
                 .withColumn("tolerance_pct", F.coalesce("_a_pct", "_t_pct", "_d_pct"))
                 .withColumn("tolerance_source", F.when(F.col("_a").isNotNull(), F.concat(F.lit("AGREEMENT:"), "_a"))
                             .when(F.col("_t").isNotNull(), F.concat(F.lit("AGREEMENT_TYPE:"), "_t"))
                             .otherwise(F.lit("DEFAULT")))
                 .select("agreement_id", "tolerance_abs_usd", "tolerance_pct", "tolerance_source"))


def build_fact_trade_mtm(bank, cpty, tol):
    """bank = silver_trades_bank, cpty = silver_trades_cpty (one business date each)."""
    ours = bank.select("agreement_id", F.col("trade_id"), F.col("cpty_id").alias("_cp_o"),
                       F.col("product").alias("_prod_o"), F.col("ccy").alias("_ccy_o"),
                       F.col("notional").alias("notional_ours"), F.col("mtm_usd").alias("mtm_ours_usd"))
    theirs = cpty.select("agreement_id", F.col("trade_ref").alias("trade_id"), F.col("cpty_id").alias("_cp_t"),
                         F.col("agreement_type").alias("_type_t"), F.col("product").alias("_prod_t"),
                         F.col("ccy").alias("_ccy_t"), F.col("notional").alias("notional_theirs"),
                         F.col("mtm_fund_view_usd").alias("mtm_theirs_usd"), F.col("source_file").alias("cpty_source_file"))
    with_statement = cpty.select("agreement_id").distinct().withColumn("_has_stmt", F.lit(True))

    j = (ours.join(theirs, ["agreement_id", "trade_id"], "full_outer")
             .join(F.broadcast(with_statement), "agreement_id", "left")
             .withColumn("cpty_id", F.coalesce("_cp_o", "_cp_t"))
             # agreement type: from the counterparty file, else from the id (ORION-MF-CP001-ISDA -> ISDA)
             .withColumn("agreement_type", F.coalesce("_type_t", F.regexp_extract("agreement_id", r"-(ISDA|GMRA)$", 1)))
             .withColumn("product", F.coalesce("_prod_o", "_prod_t"))
             .withColumn("ccy", F.coalesce("_ccy_o", "_ccy_t"))
             .withColumn("has_statement", F.coalesce("_has_stmt", F.lit(False)))
             .drop("_cp_o", "_cp_t", "_type_t", "_prod_o", "_prod_t", "_ccy_o", "_ccy_t", "_has_stmt"))

    tols = resolve_tolerances(j.select("agreement_id", "agreement_type").distinct(), tol)
    j = j.join(F.broadcast(tols), "agreement_id", "left")

    diff = F.col("mtm_theirs_usd") - F.col("mtm_ours_usd")
    j = (j.withColumn("difference_usd", diff.cast("decimal(20,2)"))
          .withColumn("difference_pct", F.when(F.col("mtm_ours_usd") != 0,
                                               F.round(F.abs(diff) / F.abs(F.col("mtm_ours_usd")), 6)).cast("decimal(12,6)"))
          .withColumn("recon_status",
                      F.when(F.col("mtm_ours_usd").isNotNull() & ~F.col("has_statement"), "NO_STATEMENT")
                       .when(F.col("mtm_theirs_usd").isNull(), "MISSING_AT_CPTY")
                       .when(F.col("mtm_ours_usd").isNull(), "UNKNOWN_AT_US")
                       .when((F.abs(diff) >= F.col("tolerance_abs_usd")) &
                             (F.abs(diff) >= F.col("tolerance_pct") * F.abs(F.col("mtm_ours_usd"))), "MTM_BREAK")
                       .otherwise("MATCHED"))
          .withColumn("is_break", F.col("recon_status").isin("MTM_BREAK", "MISSING_AT_CPTY", "UNKNOWN_AT_US")))
    return j.select("agreement_id", "cpty_id", "agreement_type", "trade_id", "product", "ccy",
                    "notional_ours", "notional_theirs", "mtm_ours_usd", "mtm_theirs_usd", "difference_usd",
                    "difference_pct", "tolerance_abs_usd", "tolerance_pct", "tolerance_source",
                    "recon_status", "is_break", "has_statement", "cpty_source_file")
