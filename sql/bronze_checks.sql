-- MarginWatch Bronze checks. Athena -> Query editor -> Workgroup: marginwatch-dev -> Database: marginwatch_dev_bronze
-- Run one query at a time.

-- 1. Rows and files per table for the day (should match the Glue run summary)
SELECT 'bronze_cpty_mtm' AS tbl, count(*) AS rows, count(DISTINCT source_file) AS files FROM bronze_cpty_mtm WHERE business_date = '2026-10-26'
UNION ALL SELECT 'bronze_cpty_margin_call', count(*), count(DISTINCT source_file) FROM bronze_cpty_margin_call WHERE business_date = '2026-10-26'
UNION ALL SELECT 'bronze_pb_margin', count(*), count(DISTINCT source_file) FROM bronze_pb_margin WHERE business_date = '2026-10-26'
UNION ALL SELECT 'bronze_bank_trades_mtm', count(*), count(DISTINCT source_file) FROM bronze_bank_trades_mtm WHERE business_date = '2026-10-26'
UNION ALL SELECT 'bronze_bank_holdings', count(*), count(DISTINCT source_file) FROM bronze_bank_holdings WHERE business_date = '2026-10-26'
UNION ALL SELECT 'bronze_ch_agreements', count(*), count(DISTINCT source_file) FROM bronze_ch_agreements WHERE business_date = '2026-10-26'
UNION ALL SELECT 'bronze_ch_balances', count(*), count(DISTINCT source_file) FROM bronze_ch_balances WHERE business_date = '2026-10-26'
UNION ALL SELECT 'bronze_fx_rates', count(*), count(DISTINCT source_file) FROM bronze_fx_rates WHERE business_date = '2026-10-26'
UNION ALL SELECT 'bronze_quarantine', count(*), count(DISTINCT source_file) FROM bronze_quarantine WHERE business_date = '2026-10-26';

-- 2. Biggest counterparty files: GS INTL ISDA should have 400 trades
SELECT cpty_id, agreement_type, source_file, count(*) AS trades
FROM bronze_cpty_mtm WHERE business_date = '2026-10-26'
GROUP BY 1, 2, 3 ORDER BY trades DESC LIMIT 5;

-- 3. The planted duplicate (Tokyo Tanshi GMRA): Bronze keeps it, Silver removes it
SELECT trade_ref, notional, mtm_cpty_view, count(*) AS copies
FROM bronze_cpty_mtm WHERE business_date = '2026-10-26' AND cpty_id = 'CP094'
GROUP BY 1, 2, 3 HAVING count(*) > 1;

-- 4. Values are still text exactly as sent (commas, words): cleaning is Silver's job
SELECT cpty_id, agreement_type, exposure_usd, collateral_held_usd, call_recall_usd
FROM bronze_cpty_margin_call WHERE business_date = '2026-10-26' AND cpty_id IN ('CP001', 'CP094');

-- 5. Lineage: every row can be traced back to its file, checksum, arrival time and run
SELECT source_key, substr(source_sha256, 1, 12) AS sha256, received_at_utc, source_row, ingest_run_id
FROM bronze_cpty_mtm WHERE business_date = '2026-10-26' AND cpty_id = 'CP001' LIMIT 3;
