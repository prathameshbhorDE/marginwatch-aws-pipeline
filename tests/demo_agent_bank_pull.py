"""End-to-end local test: mock bank REST API -> client -> mock S3."""
import os, sys, tempfile
import boto3
from moto import mock_aws
sys.path[:0] = ["src/ingestion", "tests"]
import mock_agent_bank_api as bank_api
from agent_bank_pull import BankApiClient, BankApiError, pull_agent_bank

DATE, BUCKET = "2026-10-26", "marginwatch-landing-123456789012-euw1"
srv, url = bank_api.start("sample_data/landing/agent_bank", DATE)
state = os.path.join(tempfile.mkdtemp(), "agent_bank_state.json")

with mock_aws():
    s3 = boto3.client("s3", region_name="eu-west-1")
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    s3.put_bucket_versioning(Bucket=BUCKET, VersioningConfiguration={"Status": "Enabled"})
    client = BankApiClient(url, bank_api.CLIENT_ID, bank_api.CLIENT_SECRET)

    print("RUN 1 - first pull of the day")
    print("  result:", pull_agent_bank(client, s3, BUCKET, DATE, state))
    print("\nRUN 2 - scheduler fires again (or someone re-runs it)")
    print("  result:", pull_agent_bank(client, s3, BUCKET, DATE, state))

    print("\nRUN 3 - wrong secret (e.g. rotated in the bank portal but not in Secrets Manager)")
    try:
        BankApiClient(url, bank_api.CLIENT_ID, "old-secret").list_reports(DATE).__next__()
    except Exception as e:
        print("  failed fast:", type(e).__name__, str(e)[:80])

    print("\nRUN 4 - report id that does not exist (404 is NOT retried)")
    try:
        client.download({"report_id": "RPT-XXX", "sha256": "", "size_bytes": 0})
    except BankApiError as e:
        print("  failed fast:", str(e)[:70])

    print("\nState store after runs:")
    print(open(state).read())
srv.shutdown()
