import importlib, json, os, sys, time
os.environ.update(AWS_DEFAULT_REGION="eu-west-1", MW_ENV="prod")
import boto3
from moto import mock_aws
sys.path[:0] = ["src/lambdas", "src/common"]

with mock_aws():
    boto3.client("secretsmanager").create_secret(
        Name="marginwatch/prod/agent-bank/oauth",
        SecretString=json.dumps({"client_id": "orion-collateral-ops", "client_secret": "s3cr3t-v1"}))

    import secret_store, agent_bank_puller_handler as h
    print("CONTAINER 1 (cold start, then warm re-use)")
    for i, d in enumerate(["2026-10-26", "2026-10-26", "2026-10-27"], 1):
        r = h.lambda_handler({"business_date": d}, None)
        print(f"  invocation {i}: Secrets Manager calls in this container = "
              f"{r['secrets_manager_calls_so_far_in_this_container']}  logged as {r['creds_logged_as']}")
        time.sleep(0.2)

    print("\nCONTAINER 2 (AWS starts a fresh container: new cold start)")
    secret_store.clear_cache(); importlib.reload(secret_store); h = importlib.reload(h)
    r = h.lambda_handler({"business_date": "2026-10-27"}, None)
    print(f"  invocation 1: Secrets Manager calls in this container = {r['secrets_manager_calls_so_far_in_this_container']}")

    print("\nWRONG ENVIRONMENT (MW_ENV=dev, but only prod secret exists)")
    os.environ["MW_ENV"] = "dev"
    secret_store.clear_cache(); importlib.reload(secret_store); h = importlib.reload(h)
    try:
        h.lambda_handler({"business_date": "2026-10-27"}, None)
    except Exception as e:
        print("  failed:", type(e).__name__, "->", h.SECRET_NAME)
