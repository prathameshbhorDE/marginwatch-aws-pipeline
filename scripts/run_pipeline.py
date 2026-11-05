"""
Start the MarginWatch daily pipeline (Step Functions) and follow it step by step.

  Re-run an old day (skips the cut-off gate, like an operator would):
      AWS_PROFILE=marginwatch python scripts/run_pipeline.py --date 2026-10-26 --skip-gate
  Run it exactly as the 12:30 Dublin schedule does (cut-off check decides the date and the gate):
      AWS_PROFILE=marginwatch python scripts/run_pipeline.py
  Simulate the 15:00 update run for a day (late files included, email lists changes since the last run):
      AWS_PROFILE=marginwatch python scripts/run_pipeline.py --date 2026-10-26 --skip-gate --run-type UPDATE_1500
"""
import argparse, json, time
from datetime import datetime

import boto3

ap = argparse.ArgumentParser()
ap.add_argument("--date")
ap.add_argument("--skip-gate", action="store_true")
ap.add_argument("--run-type", choices=["CUTOFF_1230", "UPDATE_1500", "MANUAL_RERUN"])
ap.add_argument("--name", default="marginwatch-dev-daily-pipeline")
ap.add_argument("--region", default="ap-south-1")
a = ap.parse_args()

sfn = boto3.client("stepfunctions", region_name=a.region)
arn = next(m["stateMachineArn"] for m in sfn.list_state_machines()["stateMachines"] if m["name"] == a.name)
payload = {}
if a.date:
    payload["business_date"] = a.date
if a.skip_gate:
    payload["skip_cutoff_gate"] = True
if a.run_type or a.skip_gate:
    payload["run_type"] = a.run_type or "MANUAL_RERUN"
if payload.get("run_type") == "UPDATE_1500":
    payload["notify"] = False
run = f"manual-{a.date or 'auto'}-{datetime.now().strftime('%H%M%S')}"
ex = sfn.start_execution(stateMachineArn=arn, name=run, input=json.dumps(payload))["executionArn"]
print(f"started {a.name} with {payload or '{}'}  (execution {run})")

seen = set()
while True:
    d = sfn.describe_execution(executionArn=ex)
    for e in sfn.get_execution_history(executionArn=ex, maxResults=1000)["events"]:
        if e["id"] in seen:
            continue
        seen.add(e["id"])
        if e["type"].endswith("StateEntered"):
            print(f"  {e['timestamp'].strftime('%H:%M:%S')}  -> {e['stateEnteredEventDetails']['name']}")
        elif e["type"] in ("TaskFailed", "ExecutionFailed"):
            det = e.get("taskFailedEventDetails") or e.get("executionFailedEventDetails") or {}
            print(f"  {e['timestamp'].strftime('%H:%M:%S')}  !! {det.get('error')}: {str(det.get('cause'))[:300]}")
    if d["status"] != "RUNNING":
        break
    time.sleep(15)

took = (d["stopDate"] - d["startDate"]).total_seconds()
print(f"\nstatus={d['status']}  total time={took:.0f} s")
if d["status"] == "SUCCEEDED":
    out = json.loads(d["output"])
    print("step times (s):", {k: out.get(k, {}).get("seconds") for k in ("bronze", "silver", "gold")})
    print("email subject :", out["report"]["subject"])
    print("\n" + out["report"]["message"])
