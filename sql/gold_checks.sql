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

-- 5. The client's daily margin summary (what used to be the Excel at 13:00 Dublin)
SELECT agreement_id, cpty_short_name, agreement_type, exposure_usd, total_requirement_usd,
       collateral_post_haircut_usd, net_excess_deficit_usd, summary, action
FROM fact_margin_call WHERE business_date = DATE '2026-10-26'
ORDER BY CASE action WHEN 'Client approval required' THEN 1 WHEN 'Inform client' THEN 2 ELSE 3 END,
         abs(net_excess_deficit_usd) DESC;

-- 6. Calls in dispute: the counterparty's call differs from ours by at least the MTA
SELECT agreement_id, net_excess_deficit_usd AS our_net, cpty_call_direction, cpty_call_amount_usd,
       call_difference_usd, trade_breaks
FROM fact_margin_call WHERE business_date = DATE '2026-10-26' AND call_agreed = false;

-- 7. Star schema join: margin calls with counterparty country and the agreement version used
SELECT c.country, count(*) AS agreements, round(sum(f.net_excess_deficit_usd), 2) AS net_usd
FROM fact_margin_call f
JOIN dim_counterparty c ON c.cpty_id = f.cpty_id
JOIN dim_agreement a ON a.agreement_version_key = f.agreement_version_key
WHERE f.business_date = DATE '2026-10-26'
GROUP BY 1 ORDER BY abs(sum(f.net_excess_deficit_usd)) DESC LIMIT 10;
