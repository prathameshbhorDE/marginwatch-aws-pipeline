"""
File register + cut-off check, end to end on fake AWS (moto):
  1. files that arrived by 12:30 Dublin land in S3 -> each one is registered
  2. the 12:30 cut-off check runs -> ONE summary (INCOMPLETE: late counterparties still missing)
  3. the late files arrive -> registered as LATE
  4. the check runs again -> READY, with the late list
In AWS, S3 sends an EventBridge event per file; here we build the same event and call the Lambda directly.
"""
import os, shutil, sys
os.environ.update(AWS_DEFAULT_REGION="ap-south-1", FILE_REGISTER_TABLE="marginwatch-dev-file-register",
                  COUNTERPARTY_FILE="config/counterparties.csv")
import boto3
from moto import mock_aws
sys.path[:0] = ["src/ingestion", "src/lambdas"]

BUCKET, DATE = "marginwatch-dev-landing-123456789012-ap-south-1", "2026-10-26"

with mock_aws():
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "ap-south-1"})
    boto3.client("dynamodb").create_table(
        TableName="marginwatch-dev-file-register", BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}])
    topic = boto3.client("sns").create_topic(Name="marginwatch-dev-alerts")["TopicArn"]
    os.environ["ALERT_TOPIC_ARN"] = topic

    import upload_to_landing, file_register, cutoff_check

    def land_and_register(arrived_by=None):
        res = upload_to_landing.run(s3, BUCKET, "sample_data/landing", DATE, arrived_by)
        new = [r for r in res if r["status"] != "SKIPPED_SAME_CONTENT"]
        counts = {}
        for r in new:   # what S3 + EventBridge would deliver to the Lambda, one event per file
            event = {"time": "2026-10-27T00:00:00Z",
                     "detail": {"bucket": {"name": BUCKET}, "object": {"key": r["key"], "size": 0}}}
            out = file_register.handler(event, None)
            counts[out["status"]] = counts.get(out["status"], 0) + 1
        return len(new), counts

    print("STEP 1 - files that arrived by the 12:30 Dublin cut-off")
    n, c = land_and_register("2026-10-27T12:30:00+00:00")
    print(f"  landed and registered {n} files: {c}")

    print("\nSTEP 2 - 12:30 cut-off check (one alert instead of hundreds)")
    s = cutoff_check.handler({"business_date": DATE}, None)

    print("\nSTEP 3 - the late counterparties' files arrive in the afternoon")
    n, c = land_and_register()
    print(f"  landed and registered {n} more files: {c}")

    print("\nSTEP 4 - cut-off check again")
    s = cutoff_check.handler({"business_date": DATE}, None)

    print("\nOne row of the file register (DynamoDB):")
    item = boto3.resource("dynamodb").Table("marginwatch-dev-file-register").get_item(
        Key={"pk": DATE, "sk": "source=email/business_date=2026-10-26/CP001_GSINTL_ISDA_MTM_20261026.pdf"})["Item"]
    for k in ("pk", "sk", "source", "file_type", "cpty_id", "agreement_type", "received_dublin", "status", "sha256"):
        print(f"   {k:16} {str(item[k])[:70]}")
