# MarginWatch — daily collateral margin pipeline on AWS

> **Status: work in progress.** Ingestion, **Bronze** and **Silver** run on real AWS (Mumbai, `ap-south-1`), built with Terraform.
> Gold (reconciliation built, margin calculation next), orchestration and the Power BI dashboard are in progress.
> All data is synthetic: see [Data disclaimer](#data-disclaimer).

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

## Architecture
```
 Counterparty emails (Excel/PDF) ─┐
 Agent bank REST API (CSV) ───────┼─► S3 landing (raw, versioned) ─► file register (DynamoDB) ─► 12:30 Dublin cut-off alert (SNS)
 Collateral Hub DB (PostgreSQL) ──┘   watermark CDC, bookmark in DynamoDB
                                             │
                                             ▼
   Bronze (Glue → Parquet) ─► Silver (Glue → Iceberg, SCD2, DQ gate) ─► Gold (Glue → Iceberg: recon, margin calls) ─► Athena ─► Power BI (planned)
```
Ingestion is **event-driven** (every file is recorded as it lands); processing is **batch**
(at the 12:30 Dublin cut-off, plus update runs for late files). Every lake bucket and secret is encrypted
with a customer-managed KMS key.

## What is built so far
| Part | What it does | Files | Status |
|---|---|---|---|
| Synthetic data generator | One business day: 200 counterparties, 271 agreements, ~2,350 trades, 396 email attachments (Excel/PDF MTM files + margin call notices), agent bank CSVs, Collateral Hub extracts, and an **answer key** of the expected margin summary. Same seed → byte-identical files. | `src/generators/`, `config/counterparties.csv` | ✅ Built |
| S3 landing | Partitioned by `source=` / `business_date=`, SHA-256 idempotency (re-runs skip, corrected re-sends become new S3 versions), lineage metadata, versioning, lifecycle rules | `src/ingestion/upload_to_landing.py`, `infra/terraform/s3_landing.tf` | ✅ Running on AWS |
| Collateral Hub pull (watermark CDC) | PostgreSQL source with a trigger-maintained `last_modified`; incremental pull with a fixed upper bound and a 5-minute overlap; the bookmark (watermark) lives in DynamoDB and moves only after the extract has landed | `src/collateral_hub/`, `src/ingestion/collateral_hub_pull.py`, `infra/terraform/dynamodb.tf` | ✅ Running on AWS |
| File register + 12:30 cut-off alert | S3 → EventBridge → Lambda records every file in DynamoDB (on time / late); EventBridge Scheduler (Europe/Dublin) runs a Lambda at 12:30 that sends **one** SNS email: READY or INCOMPLETE, with missing and late counterparties | `src/lambdas/file_register.py`, `src/lambdas/cutoff_check.py`, `infra/terraform/file_register.tf` | ✅ Running on AWS |
| Encryption and secrets | Customer-managed KMS key (yearly rotation); bronze/silver/gold/artifacts buckets SSE-KMS with S3 Bucket Keys, HTTPS-only; agent bank OAuth secret in Secrets Manager locked with the same key | `infra/terraform/kms.tf`, `lake_buckets.tf`, `secrets.tf`, `tests/check_lake_security.py` | ✅ Running on AWS |
| Bronze (Glue PySpark) | Parses every landed Excel/PDF/CSV into 9 Parquet tables, values kept as text, lineage on every row, bad files to a quarantine table, idempotent re-runs per business date; Athena tables with partition projection | `config/bronze_tables.json`, `src/bronze/`, `src/glue/bronze_job.py`, `infra/terraform/glue_bronze.tf`, `sql/bronze_checks.sql` | ✅ Running on AWS |
| Agent bank REST pull | OAuth2 client credentials, cursor pagination, retry policy (429 / 5xx / 401), checksum verification, checkpointing | `src/ingestion/agent_bank_pull.py` | ✅ Built, tested against a mock bank API |
| Credentials helper | Reads secrets at runtime, least-privilege IAM per job, 5-minute cache, redacted logging | `src/common/secret_store.py`, `src/lambdas/agent_bank_puller_handler.py`, `infra/iam/` | ✅ Built, tested on mocked AWS |
| Counterparty matching (MDM) | Maps name variants to one counterparty id; exact first, fuzzy only as a suggestion for review | `src/mdm/counterparty_matching.py` | 🟡 First version |
| Silver (Iceberg) | Text → decimals and dates, counterparty MTM flipped to the fund's view, USD via FX, agreement keys, de-duplication (latest version wins), invalid ISINs flagged, SCD2 agreement history via Iceberg MERGE, ~59 data quality checks with a gate (any ERROR stops the pipeline) | `src/silver/`, `src/glue/silver_job.py`, `infra/terraform/glue_silver.tf`, `sql/silver_checks.sql` | ✅ Running on AWS |
| Gold step 1: trade reconciliation | Our MTM vs the counterparty's per trade (full outer join): MATCHED / MTM_BREAK / MISSING_AT_CPTY / UNKNOWN_AT_US, tolerance 5,000 USD and 5% from a config file | `src/gold/gold_recon.py`, `src/glue/gold_job.py`, `config/gold_tolerances.csv`, `infra/terraform/glue_gold.tf` | ✅ Built, tested on local Spark |
| Gold step 2: margin calculation + star schema (checked against the answer key) | | | ⏳ Next |
| Step Functions orchestration | | | ⏳ Planned |
| Power BI dashboard | | | ⏳ Planned |
| CI/CD (GitHub Actions) | | | ⏳ Planned |

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
python tests/demo_bronze_parsers.py      # Bronze parsers on one day of files (no Spark, no AWS)
```
No AWS account is needed for the demos: they use `moto`, a library that imitates AWS locally.
`tests/demo_bronze_job_local.py` runs the full Glue job on local Spark (needs `pip install pyspark==3.5.4 "moto[server]"` and Java 17).

## How to deploy and run on AWS
```bash
cd infra/terraform && terraform init && terraform plan -out=plan.tfplan && terraform apply plan.tfplan
cd ../.. && python src/ingestion/upload_to_landing.py --bucket <landing bucket> --date 2026-10-26
AWS_PROFILE=<profile> python scripts/run_bronze_job.py --date 2026-10-26     # then sql/bronze_checks.sql in Athena
```

## Repository layout
```
config/            counterparty master (200 legal entities), Bronze data contract, break tolerances
docs/              data contracts, design decisions, issues log, screenshots (docs/images)
infra/terraform/   every AWS resource (S3, KMS, DynamoDB, Lambda, EventBridge, SNS, Glue, Athena)
infra/iam/         example least-privilege policy for the agent bank puller
sample_data/       one generated business day (2026-10-26) + expected answer key
scripts/           run the Bronze Glue job and print its summary
sql/               Athena check queries
src/bronze/        Bronze parsers (Excel/PDF/CSV) and row mapping
src/collateral_hub/  Collateral Hub schema and loader (PostgreSQL)
src/silver/        Silver transformations (types, USD, de-dup, SCD2)
src/gold/          Gold reconciliation and margin logic
src/common/        shared helpers (secrets)
src/generators/    synthetic data
src/glue/          Glue PySpark jobs
src/ingestion/     S3 landing, agent bank REST pull, Collateral Hub watermark pull
src/lambdas/       Lambda functions (file register, cut-off check, agent bank puller)
src/mdm/           counterparty identity matching
tests/             runnable demos against mocked AWS, a mock bank API, local Spark; AWS security check
```

## Further reading in this repo
- [Data contracts](docs/data-contracts.md): every source file, its columns and its deliberate quirks
- [Design decisions](docs/decisions.md): why X over Y
- [Issues log](docs/issues-log.md): real problems hit while building, and the fixes

## Data disclaimer
Counterparty names are real institutions used **only as labels**. Every trade, valuation, balance,
holding and margin amount is randomly generated. No real client, fund or counterparty data is used.

## Metrics (measured on real AWS runs)
| What | Result |
|---|---|
| Landing one business day | 401 files uploaded; re-run: 401 skipped (idempotent) |
| Collateral Hub watermark pull | full load 1,084 rows; next run after a simulated day: 11 changed rows |
| 12:30 cut-off alert | INCOMPLETE (10 missing) before the late files, READY (10 late) after |
| Bronze Glue job, one business day | 403 files → 9 tables, 0 quarantined, **188 s** on 2 × G.1X (includes start-up and library install) |
| Athena on Bronze | count over all 9 tables: **6.40 KB scanned, 851 ms** |
| Silver Glue job (Iceberg), one business date | 59 data quality checks: 56 pass, 3 expected warnings, 0 fail; **158 s** on 2 × G.1X |
