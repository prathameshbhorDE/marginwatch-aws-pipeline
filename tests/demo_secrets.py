"""Where every credential lives, shown against a fake AWS (moto)."""
import json, os, sys
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
import boto3
from moto import mock_aws
sys.path.insert(0, "src/common")
import secret_store as mw

SECRETS = {   # name -> what is inside (all fake values)
  "marginwatch/prod/agent-bank/oauth":   {"client_id": "orion-collateral-ops", "client_secret": "s3cr3t-v1",
                                          "token_url": "https://api.agentbank.example/oauth2/token"},
  "marginwatch/prod/agent-bank/mtls":    {"client_cert_pem": "FAKE-CLIENT-CERT-PLACEHOLDER", "client_key_pem": "FAKE-CLIENT-KEY-PLACEHOLDER"},
  "marginwatch/prod/sftp/cp-dzbank":     {"host": "sftp.dzbank.example", "port": 22, "username": "orion_coll",
                                          "ssh_private_key": "FAKE-SSH-PRIVATE-KEY-PLACEHOLDER",
                                          "host_key_fingerprint": "SHA256:Xq3...fake", "pgp_private_key": "FAKE-PGP-PRIVATE-KEY-PLACEHOLDER",
                                          "pgp_passphrase": "fake-passphrase"},
  "marginwatch/prod/collateral-hub/db":  {"host": "ch-db.internal.example", "port": 5432, "dbname": "collateral_hub",
                                          "username": "mw_reader", "password": "fake-db-pass-v1"},
  "marginwatch/prod/email/graph-app":    {"tenant_id": "fake-tenant", "client_id": "fake-app-id", "client_secret": "fake-graph-secret",
                                          "mailbox": "collateral.ops@orion.example"},
  "marginwatch/prod/email/pdf-passwords":{"CP001": "gs-pdf-pass", "CP094": "tokyo-pdf-pass"},
}
with mock_aws():
    sm = boto3.client("secretsmanager")
    kms = boto3.client("kms")
    key_id = kms.create_key(Description="marginwatch secrets key")["KeyMetadata"]["KeyId"]
    for name, val in SECRETS.items():
        sm.create_secret(Name=name, SecretString=json.dumps(val), KmsKeyId=key_id)
    ssm = boto3.client("ssm")   # NON-secret settings go to Parameter Store
    ssm.put_parameter(Name="/marginwatch/prod/agent-bank/base_url", Value="https://api.agentbank.example", Type="String")

    print("1) What is stored where")
    for s in sm.list_secrets()["SecretList"]:
        print(f"   Secrets Manager  {s['Name']:42} fields: {list(SECRETS[s['Name']].keys())}")
    print(f"   Parameter Store  /marginwatch/prod/agent-bank/base_url -> "
          f"{ssm.get_parameter(Name='/marginwatch/prod/agent-bank/base_url')['Parameter']['Value']}")

    print("\n2) A job reads the same secret 3 times -> how many AWS calls?")
    stats = {}
    for _ in range(3):
        creds = mw.get_secret("marginwatch/prod/agent-bank/oauth", stats)
    print("   AWS API calls:", stats["api_calls"], "| safe to log:", mw.redact(creds))

    print("\n3) Rotation: the bank issues a new client secret")
    sm.put_secret_value(SecretId="marginwatch/prod/agent-bank/oauth",
                        SecretString=json.dumps({**SECRETS["marginwatch/prod/agent-bank/oauth"], "client_secret": "s3cr3t-v2"}))
    for v in sm.list_secret_version_ids(SecretId="marginwatch/prod/agent-bank/oauth")["Versions"]:
        val = json.loads(sm.get_secret_value(SecretId="marginwatch/prod/agent-bank/oauth", VersionId=v["VersionId"])["SecretString"])
        print(f"   version {v['VersionId'][:8]}  stages={v['VersionStages']}  client_secret ends with ...{val['client_secret'][-2:]}")
    print("   job still uses cached v1 until cache expires:", mw.get_secret("marginwatch/prod/agent-bank/oauth")["client_secret"][-2:])
    mw.clear_cache()
    print("   after cache refresh the job uses:", mw.get_secret("marginwatch/prod/agent-bank/oauth")["client_secret"][-2:])

    print("\n4) Secret that does not exist (typo in name / wrong environment)")
    try:
        mw.get_secret("marginwatch/dev/agent-bank/oauth")
    except sm.exceptions.ResourceNotFoundException as e:
        print("   ", type(e).__name__)
