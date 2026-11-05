"""
MarginWatch - Lambda: at the 12:30 Dublin cut-off, check what has arrived for the business date
and send ONE summary alert (SNS email) - instead of one email per file.

Trigger: EventBridge Scheduler at 12:30 Europe/Dublin on weekdays (or run by hand with
         {"business_date": "2026-10-26"}).
Expected files come from counterparties.csv: one MTM file per counterparty agreement,
plus 2 agent bank reports, Collateral Hub extracts and the prime broker report.
"""
import csv, json, os
from datetime import date, datetime, timedelta
from dublin_time import DUBLIN

import boto3
from boto3.dynamodb.conditions import Key

TABLE = os.environ.get("FILE_REGISTER_TABLE", "marginwatch-dev-file-register")
TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")
CPTY_FILE = os.environ.get("COUNTERPARTY_FILE", os.path.join(os.path.dirname(__file__), "counterparties.csv"))



def expected_agreements():
    """{'CP001_ISDA': 'GS INTL', ...} - one MTM file is expected for each."""
    out = {}
    with open(CPTY_FILE, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            for agr in r["agreements"].split("|"):
                out[f"{r['cpty_id']}_{agr}"] = r["short_name"]
    return out


def previous_business_day(today):
    d = today - timedelta(days=1)
    while d.weekday() >= 5:          # skip Saturday/Sunday (a real bank would also use a holiday calendar)
        d -= timedelta(days=1)
    return d


def registered_files(business_date):
    tbl = boto3.resource("dynamodb").Table(TABLE)
    items, kwargs = [], {"KeyConditionExpression": Key("pk").eq(business_date)}
    while True:
        page = tbl.query(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def handler(event, context):
    business_date = (event or {}).get("business_date") or \
        previous_business_day(datetime.now(DUBLIN).date()).isoformat()
    files = registered_files(business_date)
    expected = expected_agreements()

    mtm = {f"{f['cpty_id']}_{f['agreement_type']}": f for f in files if f["file_type"] == "MTM"}
    on_time = sorted(k for k, f in mtm.items() if f["status"] == "ON_TIME")
    late = sorted(k for k, f in mtm.items() if f["status"] == "LATE")
    missing = sorted(set(expected) - set(mtm))
    types = {}
    for f in files:
        types[f["file_type"]] = types.get(f["file_type"], 0) + 1

    summary = {
        "business_date": business_date,
        "mtm_expected": len(expected), "mtm_on_time": len(on_time),
        "mtm_late": len(late), "mtm_missing": len(missing),
        "margin_call_notices": types.get("MARGIN_CALL", 0),
        "agent_bank_files": types.get("BANK_TRADES_MTM", 0) + types.get("BANK_HOLDINGS", 0),
        "collateral_hub_extracts": types.get("CH_AGREEMENTS", 0) + types.get("CH_BALANCES", 0),
        "pb_reports": types.get("PB_REPORT", 0),
        "late": [f"{expected.get(k, k)} ({k}) at {mtm[k]['received_dublin']} Dublin" for k in late],
        "missing": [f"{expected[k]} ({k})" for k in missing],
    }
    ready = summary["mtm_missing"] == 0 and summary["agent_bank_files"] >= 2 and summary["collateral_hub_extracts"] >= 1
    summary["status"] = "READY" if ready else "INCOMPLETE"

    lines = [f"MarginWatch cut-off check - business date {business_date} (12:30 Dublin)",
             f"Status: {summary['status']}",
             f"Counterparty MTM files: {len(on_time)} on time, {len(late)} late, {len(missing)} missing "
             f"(of {len(expected)} agreements)",
             f"Margin call notices: {summary['margin_call_notices']} | Agent bank files: {summary['agent_bank_files']}/2 | "
             f"Collateral Hub extracts: {summary['collateral_hub_extracts']} | PB reports: {summary['pb_reports']}"]
    if late:
        lines += ["", "Late (after 12:30 Dublin):"] + [f"  - {x}" for x in summary["late"]]
    if missing:
        lines += ["", "Missing:"] + [f"  - {x}" for x in summary["missing"][:30]]
        if len(missing) > 30:
            lines.append(f"  ... and {len(missing) - 30} more")
    message = "\n".join(lines)

    if TOPIC_ARN and (event or {}).get("notify", True):      # the 15:00 update run sends one combined email instead
        boto3.client("sns").publish(TopicArn=TOPIC_ARN, Subject=f"MarginWatch {business_date}: {summary['status']}",
                                    Message=message)
    print(message)
    return summary
