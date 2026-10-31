# MarginWatch — daily collateral margin pipeline on AWS

> **Status: work in progress.** Ingestion layer built and tested locally against mocked AWS.
> Processing (Bronze/Silver/Gold), orchestration and the Power BI dashboard are **planned**.
> All data is synthetic — see [Data disclaimer](#data-disclaimer).

## The business problem
A fund administrator runs daily collateral for a hedge fund with **~200 counterparties** under
ISDA/CSA (derivatives) and GMRA (repo) agreements. Every morning it must:

1. collect counterparty statements (Excel/PDF by email, 03:00 to after 13:00 Dublin time),
   agent bank reports and internal collateral balances,
2. work out, per agreement, whether the fund **must pay** collateral (margin call) or can **get some back** (recall),
3. send the client a daily margin summary by **13:00 Dublin**. The client sees every agreement but only
   decides on calls above **2.5m (ISDA) / 5m (GMRA)**.

Done by hand in Excel this is slow and fragile: late files, changing formats, corrected re-sends,
disputes. MarginWatch automates it as a data pipeline.

## Architecture (target)
```
 Counterparty emails (Excel/PDF) ─┐
 Agent bank REST API (CSV)  ──────┼─► S3 landing (raw, versioned) ─► file register ─► 12:30 cut-off
 Collateral Hub DB (planned) ─────┘        (DynamoDB, planned)            │
                                                                          ▼
          Power BI ◄── Athena ◄── Gold (margin summary) ◄── Silver ◄── Bronze   (Glue PySpark, planned)
```
Ingestion is **event-driven** (every file is recorded as it lands); processing is **batch**
(at the 12:30 Dublin cut-off, plus update runs for late files).

## What is built so far
| Part | What it does | Files | Status |
|---|---|---|---|
| Synthetic data generator | One business day: 200 counterparties, 271 agreements, ~2,350 trades, 396 email attachments (Excel/PDF MTM files + margin call notices), agent bank CSVs, Collateral Hub extracts, and an **answer key** of the expected margin summary. Same seed → byte-identical files. | `src/generators/`, `config/counterparties.csv` | ✅ Built |
| S3 landing | Partitioned by `source=` / `business_date=`, SHA-256 idempotency (re-runs skip, corrected re-sends become new S3 versions), lineage metadata, encryption | `src/ingestion/upload_to_landing.py`, `infra/s3/landing_lifecycle.json` | ✅ Built, tested on mocked S3 |
| Agent bank REST pull | OAuth2 client credentials, cursor pagination, retry policy (429 / 5xx / 401), checksum verification, checkpointing | `src/ingestion/agent_bank_pull.py` | ✅ Built, tested against a mock bank API |
| Credentials | Secrets Manager + KMS, least-privilege IAM role per job, 5-minute cache, redacted logging | `src/common/secret_store.py`, `src/lambdas/agent_bank_puller_handler.py`, `infra/iam/` | ✅ Built, tested on mocked AWS |
| Counterparty matching (MDM) | Maps name variants to one counterparty id; exact first, fuzzy only as a suggestion for review | `src/mdm/counterparty_matching.py` | 🟡 First version |
| Collateral Hub DB pull (watermark/CDC) | | | ⏳ Planned |
| File register + cut-off gate | | | ⏳ Planned |
| Step Functions orchestration | | | ⏳ Planned |
| Bronze / Silver / Gold (Glue PySpark, Delta) | | | ⏳ Planned |
| Athena + Power BI dashboard | | | ⏳ Planned |
| CI/CD (GitHub Actions), Terraform | | | ⏳ Planned |

## How to run it locally
Requires **Python 3.10 or newer** (macOS ships 3.9, which is end-of-life; `reportlab` refuses to import on it).
```bash
python3 -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python src/generators/generate_daily_files.py --out sample_data   # one day of source files
python tests/demo_landing_moto.py        # S3 landing on mocked S3 (run 1 uploads, run 2 skips, run 3 re-send)
python tests/demo_agent_bank_pull.py     # REST pull vs mock bank (429, 503, expired token, 404)
python tests/demo_secrets.py             # where credentials live, caching, rotation
python tests/demo_lambda_secrets.py      # how the Lambda reads its secret (cold vs warm start)
python src/mdm/counterparty_matching.py  # name matching demo
```
No AWS account is needed for the demos: they use `moto`, a library that imitates AWS locally.

## Repository layout
```
config/            counterparty master (200 legal entities)
docs/              data contracts, design decisions, issues log
infra/             S3 lifecycle rules, IAM policy
sample_data/       one generated business day (2026-10-26) + expected answer key
src/common/        shared helpers (secrets)
src/generators/    synthetic data
src/ingestion/     S3 landing, agent bank REST pull
src/lambdas/       Lambda entry points
src/mdm/           counterparty identity matching
tests/             runnable demos against mocked AWS and a mock bank API
```

## Further reading in this repo
- [Data contracts](docs/data-contracts.md): every source file, its columns and its deliberate quirks
- [Design decisions](docs/decisions.md): why X over Y
- [Issues log](docs/issues-log.md): real problems hit while building, and the fixes

## Data disclaimer
Counterparty names are real institutions used **only as labels**. Every trade, valuation, balance,
holding and margin amount is randomly generated. No real client, fund or counterparty data is used.

## Metrics
To be filled in with numbers measured on real AWS runs (not estimates).
