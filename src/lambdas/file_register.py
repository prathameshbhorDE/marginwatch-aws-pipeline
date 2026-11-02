"""
MarginWatch - Lambda: record every file that lands in the S3 landing bucket.

Trigger: S3 "Object Created" event -> EventBridge rule -> this Lambda (one call per file).
Writes one row per file into the DynamoDB file register:
    pk = business date (e.g. "2026-10-26")      sk = the S3 key of the file
plus who sent it, what kind of file it is, when it arrived, and whether it was ON_TIME or LATE
against the 12:30 Dublin cut-off on the next day.
"""
import os
from datetime import date, datetime, time, timedelta, timezone
from urllib.parse import unquote_plus
from dublin_time import DUBLIN

import boto3


CUTOFF = time(12, 30)
TABLE = os.environ.get("FILE_REGISTER_TABLE", "marginwatch-dev-file-register")

_s3 = boto3.client("s3")
_table = None


def table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(TABLE)
    return _table


def describe_file(key):
    """source=email/business_date=2026-10-26/CP001_GSINTL_ISDA_MTM_20261026.xlsx -> what is this file?"""
    parts = key.split("/")
    source = parts[0].split("=", 1)[1]
    business_date = parts[1].split("=", 1)[1]
    name = parts[-1]
    info = {"source": source, "business_date": business_date, "file_name": name,
            "file_type": "OTHER", "cpty_id": "-", "agreement_type": "-"}
    bits = name.split("_")
    if source == "email" and name.startswith("CP"):
        info.update(cpty_id=bits[0], agreement_type=bits[2],
                    file_type="MARGIN_CALL" if "_MarginCall_" in name else "MTM")
    elif source == "email" and name.startswith("PB"):
        info.update(cpty_id=bits[0], agreement_type="PB", file_type="PB_REPORT")
    elif source == "agent_bank":
        info["file_type"] = "BANK_TRADES_MTM" if name.startswith("trades_mtm") else "BANK_HOLDINGS"
    elif source == "collateral_hub":
        info["file_type"] = "CH_AGREEMENTS" if name.startswith("agreements") else "CH_BALANCES"
    elif source == "reference":
        info["file_type"] = "FX_RATES"
    return info


def cutoff_utc(business_date):
    """12:30 Dublin time on the day AFTER the business date, as a UTC datetime (handles summer/winter time)."""
    next_day = date.fromisoformat(business_date) + timedelta(days=1)
    return datetime.combine(next_day, CUTOFF, tzinfo=DUBLIN).astimezone(timezone.utc)


def handler(event, context):
    detail = event["detail"]
    bucket = detail["bucket"]["name"]
    key = unquote_plus(detail["object"]["key"])
    if not key.startswith("source="):
        return {"skipped": key}

    head = _s3.head_object(Bucket=bucket, Key=key)
    meta = head.get("Metadata", {})
    info = describe_file(key)
    received = datetime.fromisoformat(meta.get("received-at-utc", event["time"]).replace("Z", "+00:00"))
    status = "ON_TIME" if received <= cutoff_utc(info["business_date"]) else "LATE"

    item = {
        "pk": info["business_date"], "sk": key, **info,
        "received_at_utc": received.astimezone(timezone.utc).isoformat(),
        "received_dublin": received.astimezone(DUBLIN).strftime("%Y-%m-%d %H:%M"),
        "status": status,
        "size_bytes": int(detail["object"].get("size", head["ContentLength"])),
        "sha256": meta.get("sha256", "-"),
        "version_id": detail["object"].get("version-id", "-"),
        "registered_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    table().put_item(Item=item)          # same key again (a re-send) simply updates the row
    return {"registered": key, "status": status}
