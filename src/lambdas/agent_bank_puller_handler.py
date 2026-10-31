"""
Lambda entry point for the agent bank pull. Shows HOW a Lambda reads its secrets.

Two parts of a Lambda file run at different times:
  - code at the TOP of the file (outside the handler) runs once per "cold start"
    = when AWS sets up a fresh container (the staff member's first shift of the day)
  - lambda_handler() runs on EVERY invocation
    = every time the scheduler rings the bell

So we create the AWS client and the secret cache at the top: a warm container reuses them.
"""
import json, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "common"))
import secret_store

ENV = os.environ.get("MW_ENV", "prod")                         # only the NAME of things lives in env vars
SECRET_NAME = f"marginwatch/{ENV}/agent-bank/oauth"           # never the secret value itself
COLD_START_AT = time.time()                                    # set once per container
STATS = {"api_calls": 0}

def lambda_handler(event, context):
    started = time.time()
    creds = secret_store.get_secret(SECRET_NAME, STATS)        # cached for 5 min inside this container
    # ... here the real job would: get OAuth token with creds, list reports, download, land in S3 ...
    return {
        "business_date": event.get("business_date"),
        "secret_used": SECRET_NAME,
        "client_id": creds["client_id"],                       # safe to show
        "creds_logged_as": secret_store.redact(creds),          # never log raw values
        "secrets_manager_calls_so_far_in_this_container": STATS["api_calls"],
        "container_age_s": round(started - COLD_START_AT, 2),
    }
