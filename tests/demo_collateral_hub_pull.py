"""
Collateral Hub watermark pull, end to end: real local PostgreSQL + fake S3/DynamoDB (moto).
  CH_DSN=postgresql://localhost/collateral_hub python tests/demo_collateral_hub_pull.py
WARNING: drops and recreates the two Collateral Hub tables in that database.
"""
import os, subprocess, sys
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
import boto3, psycopg
from moto import mock_aws
sys.path[:0] = ["src/ingestion", "src/collateral_hub"]
from collateral_hub_pull import WatermarkStore, pull_table, TABLES
import load_collateral_hub as ch

DSN = os.environ.get("CH_DSN", "postgresql://localhost/collateral_hub")
BUCKET, WM = "marginwatch-dev-landing-123456789012-ap-south-1", "marginwatch-dev-watermarks"

with psycopg.connect(DSN) as conn:
    conn.execute("DROP TABLE IF EXISTS collateral_balances; DROP TABLE IF EXISTS agreements;")
    conn.commit()
    ch.init(conn, "sample_data/landing/collateral_hub")

with mock_aws():
    s3 = boto3.client("s3", region_name="ap-south-1")
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "ap-south-1"})
    s3.put_bucket_versioning(Bucket=BUCKET, VersioningConfiguration={"Status": "Enabled"})
    boto3.client("dynamodb", region_name="ap-south-1").create_table(
        TableName=WM, BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}])
    store = WatermarkStore(WM, "ap-south-1")

    def run(title, bdate, overlap=0):
        print(f"\n{title}")
        with psycopg.connect(DSN, options="-c TimeZone=UTC") as conn:
            return sum(pull_table(conn, s3, BUCKET, store, t, bdate, overlap) for t in TABLES)

    run("RUN 1 - first ever run: no watermark, so a full load", "2026-10-26")
    run("RUN 2 - nothing changed in Collateral Hub since run 1", "2026-10-26")

    print("\n... next day, operations staff work in Collateral Hub:")
    with psycopg.connect(DSN) as conn:
        ch.simulate_day(conn, "2026-10-27")
    run("RUN 3 - only the rows changed since run 2", "2026-10-27")

    print("\nWhat RUN 3 extracted for agreements (the MTA change):")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="source=collateral_hub/business_date=2026-10-27/agreements")["Contents"]]
    body = s3.get_object(Bucket=BUCKET, Key=keys[0])["Body"].read().decode()
    for line in body.splitlines():
        print("   ", line)

    run("RUN 4 - same as run 2, but with a 5-minute overlap window (production default)", "2026-10-27", overlap=5)

    print("\nWatermark table after the runs:")
    for item in boto3.resource("dynamodb", region_name="ap-south-1").Table(WM).scan()["Items"]:
        print(f"   {item['pk']:32} watermark={item['watermark']}  last_rows={item['last_rows']}")
