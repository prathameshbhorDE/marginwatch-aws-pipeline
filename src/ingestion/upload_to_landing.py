"""
MarginWatch - Ingestion step 1: land raw source files in the S3 landing zone.

What it does for every file of one business date:
  1. works out WHICH source system it came from (email / agent_bank / collateral_hub / reference)
  2. builds a partitioned S3 key:  source=<src>/business_date=<yyyy-mm-dd>/<original file name>
  3. computes a SHA-256 of the bytes (our idempotency key - not the S3 ETag)
  4. skips the upload if the same key already holds the same bytes (safe re-runs)
     or uploads a new version if the bytes changed (a counterparty re-sent a corrected file)
  5. attaches lineage metadata: source, business date, received time, message id, checksum

Usage (real AWS, credentials from your AWS profile):
  python src/ingestion/upload_to_landing.py --bucket marginwatch-dev-landing-<account-id>-ap-south-1 --date 2026-10-26
"""
import argparse, hashlib, json, os, sys
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

SOURCES = ("email", "agent_bank", "collateral_hub", "reference")
PORTAL_PULL_UTC = "T06:00:00+00:00"     # when our job pulls agent bank / CH / reference files (next day)


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):   # stream 1 MB at a time: works for huge files
            h.update(chunk)
    return h.hexdigest()


def load_email_metadata(email_dir):
    """attachment file name -> its email metadata (message id, sender, received time)."""
    meta = {}
    path = os.path.join(email_dir, "_email_metadata.jsonl")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                meta[rec["attachment"]] = rec
    return meta


def plan_uploads(local_root, business_date, next_day):
    """Return one dict per file to land, in ARRIVAL order (emails sorted by received time)."""
    email_meta = load_email_metadata(os.path.join(local_root, "email"))
    plan = []
    for source in SOURCES:
        folder = os.path.join(local_root, source)
        if not os.path.isdir(folder):
            continue
        for name in sorted(os.listdir(folder)):
            if name.startswith("_"):            # control files (email metadata) are not data files
                continue
            m = email_meta.get(name, {})
            plan.append({
                "local_path": os.path.join(folder, name),
                "key": f"source={source}/business_date={business_date}/{name}",
                "metadata": {
                    "source-system": source,
                    "business-date": business_date,
                    "received-at-utc": m.get("received_at_utc", next_day + PORTAL_PULL_UTC),
                    "message-id": m.get("message_id", "n/a"),
                    "sender": m.get("from", "portal-pull"),
                },
            })
    return sorted(plan, key=lambda p: p["metadata"]["received-at-utc"])


def land_file(s3, bucket, item):
    """Idempotent upload. Returns UPLOADED, SKIPPED_SAME_CONTENT or NEW_VERSION."""
    digest = sha256_of(item["local_path"])
    try:
        head = s3.head_object(Bucket=bucket, Key=item["key"])
        if head["Metadata"].get("sha256") == digest:
            return "SKIPPED_SAME_CONTENT", digest
        status = "NEW_VERSION"                   # same name, different bytes: keep both (versioning)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
        status = "UPLOADED"
    with open(item["local_path"], "rb") as body:
        s3.put_object(Bucket=bucket, Key=item["key"], Body=body,
                      Metadata={**item["metadata"], "sha256": digest,
                                "landed-at-utc": datetime.now(timezone.utc).isoformat(timespec="seconds")},
                      ServerSideEncryption="AES256")   # SSE-S3; swap for "aws:kms" + SSEKMSKeyId in a bank
    return status, digest


def run(s3, bucket, local_root, business_date):
    from datetime import date, timedelta
    next_day = (date.fromisoformat(business_date) + timedelta(days=1)).isoformat()
    results = []
    for item in plan_uploads(local_root, business_date, next_day):
        status, digest = land_file(s3, bucket, item)
        results.append({"key": item["key"], "status": status, "sha256": digest[:12],
                        "received_at_utc": item["metadata"]["received-at-utc"]})
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--date", required=True, help="business date YYYY-MM-DD")
    ap.add_argument("--local-root", default="sample_data/landing")
    ap.add_argument("--region", default="ap-south-1")
    args = ap.parse_args()
    out = run(boto3.client("s3", region_name=args.region), args.bucket, args.local_root, args.date)
    counts = {}
    for r in out:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(json.dumps(counts))
    sys.exit(0)
