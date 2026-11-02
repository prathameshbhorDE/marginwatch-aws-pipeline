"""
MarginWatch - prove the KMS / data lake setup on REAL AWS (run after terraform apply).

Checks:
  1. our KMS key exists and yearly rotation is ON
  2. bronze / silver / gold / artifacts are encrypted with OUR key (SSE-KMS) with S3 Bucket Keys on
  3. a test file written to bronze really comes back as aws:kms + our key, then is deleted
  4. the bucket policy refuses non-HTTPS requests
  5. the agent bank secret is locked with our key and can be read (values hidden)

Usage:  python tests/check_lake_security.py            (uses your AWS_PROFILE / aws login session)
"""
import json, os, sys

os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "common"))

import boto3
from secret_store import get_secret, redact

ENV, REGION = "dev", os.environ["AWS_DEFAULT_REGION"]
ALIAS = f"alias/marginwatch-{ENV}-lake"
SECRET = f"marginwatch/{ENV}/agent-bank/oauth"


def ok(flag):
    return "OK " if flag else "FAIL"


def main():
    account = boto3.client("sts").get_caller_identity()["Account"]
    kms, s3, sm = boto3.client("kms"), boto3.client("s3"), boto3.client("secretsmanager")
    fails = 0

    print("1) KMS key")
    key = kms.describe_key(KeyId=ALIAS)["KeyMetadata"]
    key_arn = key["Arn"]
    rot = kms.get_key_rotation_status(KeyId=key["KeyId"])
    good = key["KeyState"] == "Enabled" and key["KeyManager"] == "CUSTOMER" and rot["KeyRotationEnabled"]
    fails += not good
    print(f"   [{ok(good)}] {ALIAS}  state={key['KeyState']}  manager={key['KeyManager']}  "
          f"rotation={rot['KeyRotationEnabled']} every {rot.get('RotationPeriodInDays', '?')} days")

    print("2) Bucket default encryption")
    buckets = {layer: f"marginwatch-{ENV}-{layer}-{account}-{REGION}" for layer in ("bronze", "silver", "gold", "artifacts")}
    for layer, name in buckets.items():
        rule = s3.get_bucket_encryption(Bucket=name)["ServerSideEncryptionConfiguration"]["Rules"][0]
        algo = rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
        kid = rule["ApplyServerSideEncryptionByDefault"].get("KMSMasterKeyID", "-")
        good = algo == "aws:kms" and kid == key_arn and rule.get("BucketKeyEnabled")
        fails += not good
        print(f"   [{ok(good)}] {layer:9} {algo}  our key={kid == key_arn}  bucket key={rule.get('BucketKeyEnabled')}")

    print("3) Write a test file to bronze and read its encryption back")
    test_key = "_healthcheck/kms_test.txt"
    s3.put_object(Bucket=buckets["bronze"], Key=test_key, Body=b"hello from marginwatch")   # no encryption header sent
    head = s3.head_object(Bucket=buckets["bronze"], Key=test_key)
    good = head["ServerSideEncryption"] == "aws:kms" and head.get("SSEKMSKeyId") == key_arn
    fails += not good
    print(f"   [{ok(good)}] stored as {head['ServerSideEncryption']}  our key={head.get('SSEKMSKeyId') == key_arn}  "
          f"bucket key={head.get('BucketKeyEnabled')}")
    s3.delete_object(Bucket=buckets["bronze"], Key=test_key)
    print("          (test file deleted)")

    print("4) HTTPS-only bucket policy")
    for layer, name in buckets.items():
        stmts = json.loads(s3.get_bucket_policy(Bucket=name)["Policy"])["Statement"]
        good = any(s["Effect"] == "Deny" and s.get("Condition", {}).get("Bool", {}).get("aws:SecureTransport") == "false"
                   for s in stmts)
        fails += not good
        print(f"   [{ok(good)}] {layer:9} denies non-HTTPS requests")

    print("5) Agent bank secret")
    desc = sm.describe_secret(SecretId=SECRET)
    locked = desc.get("KmsKeyId") in (key_arn, key["KeyId"], ALIAS)
    value = get_secret(SECRET)
    fails += not locked
    print(f"   [{ok(locked)}] {SECRET} locked with our key={locked}")
    print(f"          read OK, fields: {redact(value)}")

    print("\nALL CHECKS PASSED" if fails == 0 else f"\n{fails} CHECK(S) FAILED")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
