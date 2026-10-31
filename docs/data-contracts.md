# Data contracts — source files (synthetic)

**Disclaimer:** counterparty names in `config/counterparties.csv` are real institutions used only as labels.
Every trade, MTM, balance, holding and call amount is randomly generated. No real client, fund or
counterparty data is used anywhere in this repository.

Business date = the COB date values refer to. Files arrive the next morning; all timestamps are stored in UTC.
Processing cut-off 12:30 Europe/Dublin; client SLA 13:00 Europe/Dublin.

## Reference: counterparty master (`config/counterparties.csv`)
200 legal entities, 1 row each: `cpty_id, short_name, legal_name, country, group_name, agreements`
(`agreements` = `ISDA`, `GMRA` or `ISDA|GMRA`). Generates 271 agreements. Japanese tanshi firms
and Shinkin Central Bank are GMRA-only. Note `legal_name` contains commas (e.g. "JPMorgan Chase Bank, N.A."),
so CSV parsers must honour quoting.

## Source files
| Channel | File pattern | Format | Grain | Key fields | Deliberate quirks |
|---|---|---|---|---|---|
| Counterparty email | `CPnnn_<SHORT>_<ISDA/GMRA>_MTM_<yyyymmdd>.xlsx` or `.pdf` (~25% PDF) | Excel / PDF | 1 row per trade | Trade Ref, Product, Notional, Currency, MTM (counterparty view), Agreement | Banner rows above header (header on row 5), merged title, footer "Total" row, sign flipped vs our view, multi-page PDFs for large books |
| Counterparty email | `CPnnn_<SHORT>_<ISDA/GMRA>_MarginCall_<yyyymmdd>.pdf` | PDF key/value table | 1 per agreement, **only when the counterparty calls** | Exposure, IM – client, IM – counterparty, Collateral held, Call / Recall (amount + direction) | Never sent for recalls: those must be derived from the MTM file |
| Prime broker email | `PB01_MSPB_MarginReport_<yyyymmdd>.xlsx` | Excel | 1 row per PB account | Account, Account Type, Total Equity, Total Requirement, Excess/Deficit | Header on row 2 |
| Email metadata | `_email_metadata.jsonl` | JSON lines | 1 per attachment | message_id, from, subject, received_at_utc, attachment, size_bytes, business_date | |
| Agent bank portal | `trades_mtm_<yyyymmdd>.csv` | CSV | 1 row per trade | fund_id, agreement_id, cpty_id, trade_id, product, notional, ccy, mtm (fund view, trade ccy), valuation_date | Golden source for our side |
| Agent bank portal | `securities_holdings_<yyyymmdd>.csv` | CSV | 1 row per ISIN per pledged account | account, isin, security_name, nominal, price, market_value (pre-haircut), pledged_to | |
| Collateral Hub | `ch_balances_<yyyymmdd>.csv` | CSV | 1 row per collateral item per agreement | agreement_id, collateral_type, isin, quantity, price, haircut_pct, market_value_usd, post_haircut_value_usd | |
| Collateral Hub | `agreements.csv` | CSV | 1 row per agreement version | agreement_id, agreement_type, threshold, mta, im_fund, im_cpty, effective_from, effective_to | SCD2-ready |
| Reference | `fx_rates_<yyyymmdd>.csv` | CSV | 1 row per currency | rate_date, ccy, to_usd | USD, EUR, GBP, JPY |
| Answer key | `expected/expected_margin_summary_<yyyymmdd>.csv` | CSV | 1 row per agreement / PB account | requirement, collateral held, net_excess_deficit_usd, summary, action | Used to test pipeline output |

## Business rules encoded in the answer key
- Requirement (owed by fund) = max( max(−exposure − threshold, 0) + IM_fund − IM_cpty, 0 )
- Net excess/deficit = collateral held (post-haircut) − requirement
- |net| < MTA → "Below MTA / No action"
- otherwise deficit → pay, excess → recall; |net| ≥ 2.5m (ISDA) or 5m (GMRA) → "Client approval required", else "Inform client"
- The client sees every row; only approval-required rows need a pay/recall decision.

## Planted scenarios and data issues
| Agreement / file | Scenario | Caught in |
|---|---|---|
| GS INTL – ISDA | ~3.2m call (approval required), 400 trades (data skew), +25% MTM break on one trade | Pieces 6–7 |
| JPM PLC – ISDA | ~0.8m call; trade missing at counterparty + unknown trade `CP005-X901` | Piece 7 position breaks |
| MS INTL – ISDA | ~0.12m deficit, below MTA | Piece 7 |
| DB LDN – ISDA | ~1.4m recall, MTM sent as PDF | Pieces 5, 7 |
| TOKYO TANSHI – GMRA | ~6.1m call (approval required), duplicate row | Pieces 5, 7 |
| UEDA TANSHI – GMRA | ~2.3m recall, file after 12:30 cut-off (plus ~7 random late counterparties) | Piece 3 |
| First pledged bond in holdings | 50,000 nominal short vs Collateral Hub | Piece 7 balance recon |
| Third pledged bond in holdings | Invalid ISIN check digit | Piece 8 data quality |
| Every counterparty Excel | Footer "Total" row | Piece 5 parsing |
