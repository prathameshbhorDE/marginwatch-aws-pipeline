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
