"""
MarginWatch - Gold step 2: the daily margin call per agreement, and the star schema around it.

For every agreement valid on the business date (terms taken point-in-time from the SCD2 history):

  exposure            = sum of OUR trade MTM in USD (agent bank = golden source; negative = fund owes)
  total requirement   = max( max(-exposure - threshold, 0) + IM_fund - IM_cpty , 0 )
  net excess/deficit  = collateral held (Collateral Hub, post-haircut) - total requirement
  summary / action    = |net| < MTA                    -> Below MTA / No action
                        net < 0                        -> Margin deficit - pay
                        net > 0                        -> Margin excess - recall
                        |net| >= approval limit        -> Client approval required (ISDA 2.5m, GMRA 5m)
                        otherwise                      -> Inform client
Prime broker accounts: the PB calculates equity and requirement itself; we report its excess/deficit.

Extra columns ops need: the counterparty's own call (from its notice) and the difference to ours,
number of trade breaks behind the agreement, and which version of the agreement terms was used.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

MONEY = "decimal(20,2)"


def agreements_on(silver_agreements, d):
    """Point-in-time: the version of each agreement's terms that was valid on business date d."""
    day = F.lit(d).cast("date")
    return silver_agreements.filter((F.col("valid_from") <= day) & (F.col("valid_to") >= day))


def build_fact_margin_call(d, agreements, bank, fx, balances, fact_trade_mtm, margin_calls, pb, rules, prime_brokers):
    rules = rules.select(F.upper(F.trim("agreement_type")).alias("agreement_type"),
                         F.col("approval_limit_usd").cast(MONEY).alias("approval_limit_usd"))

    # exposure: sum in USD at full precision, rounded once (not a sum of already-rounded trade amounts)
    rates = fx.select(F.col("ccy").alias("_c"), "to_usd")
    exposure = (bank.join(F.broadcast(rates), F.col("ccy") == F.col("_c"), "left")
                    .groupBy("agreement_id")
                    .agg(F.round(F.sum(F.col("mtm") * F.col("to_usd")), 2).cast(MONEY).alias("exposure_usd"),
                         F.count("*").alias("trades_ours")))
    collateral = balances.groupBy("agreement_id").agg(
        F.sum("post_haircut_value_usd").cast(MONEY).alias("collateral_post_haircut_usd"))
    recon = fact_trade_mtm.groupBy("agreement_id").agg(
        F.sum(F.when(F.col("is_break"), 1).otherwise(0)).cast("int").alias("trade_breaks"),
        F.round(F.sum("mtm_theirs_usd"), 2).cast(MONEY).alias("exposure_theirs_usd"),
        F.max(F.col("has_statement").cast("int")).cast("boolean").alias("statement_received"))
    notices = margin_calls.select("agreement_id", F.col("call_amount_usd").alias("cpty_call_amount_usd"),
                                  F.col("call_direction").alias("cpty_call_direction"))

    a = (agreements.select("agreement_id", "fund_id", "cpty_id", "cpty_short_name", "agreement_type",
                           F.col("threshold").alias("threshold_usd"), F.col("mta").alias("mta_usd"),
                           F.col("im_fund").alias("im_fund_usd"), F.col("im_cpty").alias("im_cpty_usd"),
                           F.col("valid_from").alias("agreement_valid_from"))
         .join(exposure, "agreement_id", "left").join(collateral, "agreement_id", "left")
         .join(recon, "agreement_id", "left").join(notices, "agreement_id", "left")
         .join(F.broadcast(rules), "agreement_type", "left")
         .fillna(0, ["exposure_usd", "collateral_post_haircut_usd", "trades_ours", "trade_breaks"]))

    zero = F.lit(0).cast(MONEY)
    uncollateralised = F.greatest(-F.col("exposure_usd") - F.col("threshold_usd"), zero)
    a = (a.withColumn("total_requirement_usd",
                      F.greatest(uncollateralised + F.col("im_fund_usd") - F.col("im_cpty_usd"), zero).cast(MONEY))
          .withColumn("net_excess_deficit_usd",
                      (F.col("collateral_post_haircut_usd") - F.col("total_requirement_usd")).cast(MONEY)))
    net = F.col("net_excess_deficit_usd")
    a = (a.withColumn("summary", F.when(F.abs(net) < F.col("mta_usd"), "Below MTA")
                                  .when(net < 0, "Margin deficit - pay").otherwise("Margin excess - recall"))
          .withColumn("action", F.when(F.col("summary") == "Below MTA", "No action")
                                 .when(F.abs(net) >= F.col("approval_limit_usd"), "Client approval required")
                                 .otherwise("Inform client")))
    # The counterparty's call, signed like ours (CALL = they ask us to pay = negative for the fund).
    # Agreed if the difference is below the MTA: a gap smaller than the MTA would not move collateral anyway,
    # so it is not worth a dispute. A bigger gap = dispute, explained by the trade breaks behind it.
    cpty_signed = F.when(F.col("cpty_call_direction") == "CALL", -F.col("cpty_call_amount_usd")) \
                   .when(F.col("cpty_call_direction") == "RECALL", F.col("cpty_call_amount_usd"))
    a = (a.withColumn("call_difference_usd", (net - cpty_signed).cast(MONEY))
          .withColumn("call_agreed", F.when(cpty_signed.isNull(), None)
                      .otherwise(F.abs(net - cpty_signed) < F.col("mta_usd"))))

    # prime broker accounts: the PB's own numbers
    pbk = prime_brokers.select("cpty_id", "cpty_short_name", "account_prefix")
    p = (pb.crossJoin(F.broadcast(pbk)).filter(F.col("account").startswith(F.col("account_prefix")))
           .select(F.regexp_extract("account", r"^(.*)-PB-", 1).alias("fund_id"), F.col("account").alias("agreement_id"),
                   "cpty_id", "cpty_short_name", F.lit("PB").alias("agreement_type"),
                   F.col("total_requirement_usd").cast(MONEY).alias("total_requirement_usd"),
                   F.col("total_equity_usd").cast(MONEY).alias("collateral_post_haircut_usd"),
                   F.col("excess_deficit_usd").cast(MONEY).alias("net_excess_deficit_usd"))
           .withColumn("mta_usd", zero)
           .withColumn("summary", F.when(F.col("net_excess_deficit_usd") < 0, "Account in margin deficit")
                                   .otherwise("Account in margin excess"))
           .withColumn("action", F.lit("Inform client")))

    cols = ["fund_id", "agreement_id", "cpty_id", "cpty_short_name", "agreement_type", "exposure_usd", "im_fund_usd",
            "im_cpty_usd", "total_requirement_usd", "collateral_post_haircut_usd", "net_excess_deficit_usd", "mta_usd",
            "summary", "action",
            # extra columns beyond the client summary
            "threshold_usd", "approval_limit_usd", "agreement_valid_from", "trades_ours", "trade_breaks",
            "statement_received", "exposure_theirs_usd", "cpty_call_amount_usd", "cpty_call_direction",
            "call_difference_usd", "call_agreed"]
    return a.unionByName(p, allowMissingColumns=True).select(*cols)


# ---------- dimensions -----------------------------------------------------------------------------
def build_dim_counterparty(counterparties_csv, prime_brokers):
    """One row per legal entity (SCD1: names are corrected in place)."""
    c = counterparties_csv.select("cpty_id", F.col("short_name").alias("cpty_short_name"),
                                  F.col("legal_name").alias("cpty_legal_name"), "country", "group_name",
                                  F.lit(False).alias("is_prime_broker"))
    p = prime_brokers.select("cpty_id", "cpty_short_name", "cpty_legal_name", F.lit(None).cast("string").alias("country"),
                             F.lit(None).cast("string").alias("group_name"), F.lit(True).alias("is_prime_broker"))
    return c.unionByName(p)


def build_dim_agreement(silver_agreements):
    """Every version of every agreement (SCD2), with a key per version for the fact table to point at."""
    return silver_agreements.select(
        F.concat_ws("|", "agreement_id", F.col("valid_from").cast("string")).alias("agreement_version_key"),
        "agreement_id", "fund_id", "cpty_id", "agreement_type", "base_ccy",
        F.col("threshold").alias("threshold_usd"), F.col("mta").alias("mta_usd"),
        F.col("im_fund").alias("im_fund_usd"), F.col("im_cpty").alias("im_cpty_usd"),
        "valid_from", "valid_to", "is_current")


def build_dim_date(spark, start="2026-01-01", end="2027-12-31"):
    d = spark.sql(f"SELECT explode(sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY)) AS date")
    return d.select("date", F.year("date").alias("year"), F.quarter("date").alias("quarter"),
                    F.month("date").alias("month"), F.date_format("date", "MMMM").alias("month_name"),
                    F.dayofmonth("date").alias("day"), F.date_format("date", "EEEE").alias("weekday_name"),
                    (F.dayofweek("date").between(2, 6)).alias("is_business_day"))
