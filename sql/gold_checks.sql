-- MarginWatch Gold checks. Athena -> Workgroup: marginwatch-dev -> Database: marginwatch_dev_gold

-- 1. Reconciliation result for the day: how many trades in each status
SELECT recon_status, count(*) AS trades
FROM fact_trade_mtm WHERE business_date = DATE '2026-10-26'
GROUP BY 1 ORDER BY 2 DESC;

-- 2. The breaks ops must chase today
SELECT agreement_id, trade_id, recon_status, mtm_ours_usd, mtm_theirs_usd, difference_usd, difference_pct,
       tolerance_abs_usd, tolerance_pct, tolerance_source
FROM fact_trade_mtm WHERE business_date = DATE '2026-10-26' AND is_break
ORDER BY agreement_id, trade_id;

-- 3. Why the 5% rule matters: big USD differences that are NOT breaks (small % of a large MTM)
SELECT agreement_id, trade_id, mtm_ours_usd, difference_usd, difference_pct, recon_status
FROM fact_trade_mtm WHERE business_date = DATE '2026-10-26' AND abs(difference_usd) >= 5000 AND NOT is_break
ORDER BY abs(difference_usd) DESC LIMIT 5;

-- 4. Per agreement: is the counterparty's statement clean? (input for the margin calculation in step 2)
SELECT agreement_id, count(*) AS trades,
       sum(CASE WHEN is_break THEN 1 ELSE 0 END) AS breaks,
       round(sum(mtm_ours_usd), 2) AS exposure_ours_usd,
       round(sum(mtm_theirs_usd), 2) AS exposure_theirs_usd
FROM fact_trade_mtm WHERE business_date = DATE '2026-10-26'
GROUP BY 1 HAVING sum(CASE WHEN is_break THEN 1 ELSE 0 END) > 0;
