-- MarginWatch Silver checks. Athena -> Workgroup: marginwatch-dev -> Database: marginwatch_dev_silver
-- Silver tables are Iceberg: typed columns, business_date is a real DATE. Run one query at a time.

-- 1. Rows per Silver table for the day
SELECT 'silver_trades_bank' AS tbl, count(*) AS rows FROM silver_trades_bank WHERE business_date = DATE '2026-10-26'
UNION ALL SELECT 'silver_trades_cpty', count(*) FROM silver_trades_cpty WHERE business_date = DATE '2026-10-26'
UNION ALL SELECT 'silver_margin_calls', count(*) FROM silver_margin_calls WHERE business_date = DATE '2026-10-26'
UNION ALL SELECT 'silver_collateral_balances', count(*) FROM silver_collateral_balances WHERE business_date = DATE '2026-10-26'
UNION ALL SELECT 'silver_holdings', count(*) FROM silver_holdings WHERE business_date = DATE '2026-10-26'
UNION ALL SELECT 'silver_pb_margin', count(*) FROM silver_pb_margin WHERE business_date = DATE '2026-10-26'
UNION ALL SELECT 'silver_fx_rates', count(*) FROM silver_fx_rates WHERE business_date = DATE '2026-10-26'
UNION ALL SELECT 'silver_agreements (current)', count(*) FROM silver_agreements WHERE is_current;

-- 2. Data quality results that are not PASS (expect only WARNs)
SELECT table_name, check_name, severity, status, failed_rows, total_rows, details
FROM silver_dq_results WHERE business_date = DATE '2026-10-26' AND status <> 'PASS';

-- 3. A margin call notice, parsed from "3,213,233.89 CALL - please deliver"
SELECT agreement_id, valuation_date, exposure_usd, collateral_held_usd, call_amount_usd, call_direction, settlement_date
FROM silver_margin_calls WHERE business_date = DATE '2026-10-26' AND cpty_id = 'CP001';

-- 4. Real numbers now: total MTM in USD per agreement type, our side vs counterparty side
SELECT a.agreement_type, round(sum(b.mtm_usd), 2) AS ours_usd
FROM silver_trades_bank b JOIN silver_agreements a ON a.agreement_id = b.agreement_id AND a.is_current
WHERE b.business_date = DATE '2026-10-26' GROUP BY 1;

-- 5. Preview of Gold: same GS INTL trades, our MTM vs theirs (the planted 25% break should be at the top)
SELECT c.trade_ref, b.mtm_usd AS ours_usd, c.mtm_fund_view_usd AS theirs_usd,
       round(c.mtm_fund_view_usd - b.mtm_usd, 2) AS difference_usd
FROM silver_trades_cpty c
JOIN silver_trades_bank b ON b.business_date = c.business_date AND b.agreement_id = c.agreement_id AND b.trade_id = c.trade_ref
WHERE c.business_date = DATE '2026-10-26' AND c.agreement_id = 'ORION-MF-CP001-ISDA'
ORDER BY abs(c.mtm_fund_view_usd - b.mtm_usd) DESC LIMIT 5;

-- 6. SCD2 history for one agreement
SELECT agreement_id, threshold, mta, valid_from, valid_to, is_current, created_run_id
FROM silver_agreements WHERE agreement_id = 'ORION-MF-CP001-ISDA' ORDER BY valid_from;

-- 7. Iceberg's own register: every write is a snapshot you can time-travel to
SELECT made_current_at, snapshot_id, is_current_ancestor FROM "silver_agreements$history";
