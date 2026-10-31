"""Runs ingestion step 1 against a fake S3 (moto) so it can be tested without an AWS account."""
import collections, os, shutil, sys, tempfile
import boto3
from moto import mock_aws
sys.path.insert(0, "src/ingestion")
from upload_to_landing import run

BUCKET, DATE = "marginwatch-landing-123456789012-euw1", "2026-10-26"

with mock_aws():
    s3 = boto3.client("s3", region_name="eu-west-1")
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    s3.put_bucket_versioning(Bucket=BUCKET, VersioningConfiguration={"Status": "Enabled"})
    s3.put_public_access_block(Bucket=BUCKET, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})

    work = tempfile.mkdtemp()                       # copy so the demo never edits sample_data
    shutil.copytree("sample_data/landing", f"{work}/landing")

    r1 = run(s3, BUCKET, f"{work}/landing", DATE)
    print("RUN 1 (first landing):", dict(collections.Counter(r["status"] for r in r1)))
    print("  first 4 arrivals:")
    for r in r1[:4]: print("   ", r["received_at_utc"], r["status"], r["key"])
    print("  last arrival:  ", r1[-1]["received_at_utc"], r1[-1]["key"])

    r2 = run(s3, BUCKET, f"{work}/landing", DATE)
    print("\nRUN 2 (same files re-run):", dict(collections.Counter(r["status"] for r in r2)))

    notice = f"{work}/landing/email/CP001_GSINTL_ISDA_MarginCall_20261026.pdf"
    with open(notice, "ab") as f: f.write(b"\n% corrected re-send")   # GS re-sends a corrected notice
    r3 = run(s3, BUCKET, f"{work}/landing", DATE)
    print("\nRUN 3 (GS INTL re-sends notice):", dict(collections.Counter(r["status"] for r in r3)))
    print("  ", [r for r in r3 if r["status"] != "SKIPPED_SAME_CONTENT"])

    key = "source=email/business_date=2026-10-26/CP001_GSINTL_ISDA_MarginCall_20261026.pdf"
    vers = s3.list_object_versions(Bucket=BUCKET, Prefix=key)["Versions"]
    print(f"\nVersions kept for that key: {len(vers)}  (latest={[v['IsLatest'] for v in vers]})")
    head = s3.head_object(Bucket=BUCKET, Key=key)
    print("Object metadata:", head["Metadata"], "| encryption:", head.get("ServerSideEncryption"))

    print("\nObjects per prefix:")
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET)
    c = collections.Counter("/".join(o["Key"].split("/")[:2]) for p in pages for o in p["Contents"])
    for k, v in sorted(c.items()): print(f"   {v:4}  {k}/")
