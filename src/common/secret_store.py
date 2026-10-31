"""
MarginWatch - one place to read credentials. Every job uses this; no job ever holds a password in code.

  get_secret("marginwatch/prod/agent-bank/oauth")  -> {"client_id": ..., "client_secret": ...}

- secrets live in AWS Secrets Manager (encrypted with a KMS key)
- the job's IAM role decides WHICH secrets it may read
- values are cached in memory for a few minutes (fewer API calls, still picks up rotations)
- never print a secret: use redact() in logs
"""
import json, time
import boto3

_CACHE, TTL_SECONDS = {}, 300
_client = None

def _sm():
    global _client
    if _client is None:
        _client = boto3.client("secretsmanager")
    return _client

def get_secret(name, stats=None):
    hit = _CACHE.get(name)
    if hit and time.time() - hit[0] < TTL_SECONDS:
        return hit[1]
    value = json.loads(_sm().get_secret_value(SecretId=name)["SecretString"])
    if stats is not None:
        stats["api_calls"] = stats.get("api_calls", 0) + 1
    _CACHE[name] = (time.time(), value)
    return value

def clear_cache():
    _CACHE.clear()

def redact(d):
    """For logging: show which fields exist, never their values."""
    return {k: ("***" if any(w in k.lower() for w in ("secret", "password", "key", "passphrase", "token")) else v)
            for k, v in d.items()}
