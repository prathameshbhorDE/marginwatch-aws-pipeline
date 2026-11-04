# Design decisions

Each entry: what we chose, what we didn't, and why.

| # | Decision | Alternatives considered | Why |
|---|---|---|---|
| 1 | **Synthetic data with a fixed seed and an answer key** | Real or "anonymised" files | Real client/counterparty data is confidential. A seed gives byte-identical files on every run, and the answer key turns "looks right" into an automated test. |
| 2 | **Event-driven ingestion, batch processing** | Pure streaming (Kinesis/Kafka); pure scheduled batch | Files arrive continuously from 03:00 to after 13:00 Dublin, but the margin decision is made once a day at a 12:30 cut-off. Streaming adds cost with no business benefit; a plain cron job can't see which files are missing. |
| 3 | **Separate landing bucket; raw files never changed** | Clean files in place | Any parser bug can be fixed and re-run from the original bytes; full audit of what each counterparty sent. |
| 4 | **Partition by business date, not arrival date** | Partition by load date | A late file belongs to the day it describes; every query filters on business date, so Athena reads only that folder (partition pruning). |
| 5 | **SHA-256 content hash as the idempotency key** | S3 ETag | ETag is only an MD5 of the content for simple uploads, not multipart or SSE-KMS uploads. Our own hash is reliable. |
| 6 | **S3 versioning on** | Overwrite | Corrected re-sends keep the earlier version, so we know which version each call was based on. |
| 7 | **Agent bank via REST API** | SFTP (host-to-host), manual portal download | Small files and a hard cut-off: REST lets us ask for exactly today's reports, check size/checksum first, and get clear errors ("not ready", "rate limited"). SFTP stays the fallback for large files or banks that only offer SFTP; it does not "clutter storage" if pulls are selective. |
| 8 | **Retry only retryable errors** (429 with Retry-After, 5xx, timeouts; 401 → refresh token once) with bounded exponential backoff + jitter | Retry everything; retry forever | Retrying a 404 or bad credentials can never succeed; unbounded retries hide failures. |
| 9 | **Lambda for the bank pull** | Glue Python shell, ECS Fargate, EC2 cron | A ~10-second job pulling two small files: no servers, pay per run. Fargate if a pull ever exceeds 15 minutes. |
| 10 | **Credentials in Secrets Manager (KMS-encrypted); one IAM role per job** | Code, Git, Lambda environment variables | Rotation, versioning, audit in CloudTrail, and a hacked job can only read its own secrets. Only the secret's *name* is configured. |
| 11 | **OAuth access tokens kept in memory only** | Store the token | It expires in minutes; storing it only creates another thing to protect. |
| 12 | **Counterparty master as a config file; legal entity is the key** | Hard-coded names; match on group name | A new counterparty is a data change, not a code change. Different legal entities of one group have different agreements and calls. |
| 13 | **Fuzzy name matches are suggestions, never auto-applied** | Auto-accept above a score | "Morgan Stanley & Co" scored 0.93 against the US LLC but could be the London plc: a wrong match books a call against the wrong agreement. |
| 14 | **Customer-managed KMS key for the lake and secrets, yearly rotation** | SSE-S3 (AWS-owned key); AWS-managed `aws/s3` key | We control who can use the key with IAM, every use is logged in CloudTrail, and disabling the key locks the data. About $1/month. |
| 15 | **S3 Bucket Keys on** | One KMS call per object | Glue writes many Parquet files; a bucket-level key cuts KMS requests (cost and throttling) sharply. |
| 16 | **One bucket per layer (bronze / silver / gold / artifacts)** | One bucket with folders | Each layer gets its own access rules (e.g. Power BI reads gold only) and its own lifecycle. |
| 17 | **Secret created empty by Terraform, value set with the CLI** | Secret value in Terraform | Anything in Terraform is also stored in plain text in the state file. |
| 18 | **No versioning on bronze/silver/gold** | Versioning everywhere | Each layer can be rebuilt from the one below; old versions would only add storage cost. Landing (raw originals) and artifacts keep versions. |
| 19 | **Bronze in a Glue PySpark job (glueetl, Glue 5.0)** | Lambda; Glue Python shell; EMR | Today's ~3 MB fits anywhere, but Spark scales to hundreds of counterparties and years of backfill without a rewrite, and Silver/Gold need Spark anyway. 2 x G.1X workers is the minimum size. |
| 20 | **Spark parallelises by file (list keys on the driver, parse in executors with mapPartitions)** | `spark.read.csv` / Excel/PDF readers | Spark has no built-in Excel/PDF reader; each file must be opened by Python libraries. One S3 client per partition, not per file. |
| 21 | **Bronze keeps every value as text; only column names are made safe (snake_case)** | Cast types in Bronze | "5,000,000.00" or "3,213,233.89 CALL - please deliver" are kept exactly as sent, so nothing is lost if a cast rule is wrong. Parquet/Athena don't allow spaces or brackets in column names. |
| 22 | **One data contract file (`config/bronze_tables.json`) drives both the Glue job and the Athena tables** | Column lists repeated in code and Terraform | One place to change; the job and the table definitions can never drift apart. |
| 23 | **Bad files go to `bronze_quarantine`, the job does not fail** | Fail the whole run | One broken counterparty file must not stop the other 400 before a 13:00 SLA. The quarantine table says which file and why. |
| 24 | **Idempotent re-runs: `--business_date` argument + dynamic partition overwrite; Glue job bookmarks off** | Job bookmarks; append mode | Re-running a day replaces exactly that day's folder; a late or corrected file is picked up by simply re-running the day. Bookmarks track "new files", which fights with re-runs. |
| 25 | **One Parquet file per table per day (`coalesce(1)`)** | Spark's default many small files | Tiny files make Athena slow and cost more S3 requests. At larger volumes: size files to ~128 MB-1 GB instead. |
| 26 | **Athena partition projection on `business_date`** | Glue crawler; `MSCK REPAIR` / `ADD PARTITION` after each load | Tables are defined once in Terraform from the contract; Athena computes the day's folder itself. No crawler cost, no schema surprises from a crawler guessing types. |
| 27 | **Athena workgroup: results encrypted with our KMS key, 1 GB scan limit per query** | Default workgroup | Query results can hold client data; a runaway query is stopped instead of billed. |
