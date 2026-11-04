"""
Start the Bronze Glue job for one business date, wait for it, and print the run summary.

Usage:  AWS_PROFILE=marginwatch python scripts/run_bronze_job.py --date 2026-10-26
"""
import argparse, json, time

import boto3

ap = argparse.ArgumentParser()
ap.add_argument("--date", required=True)
ap.add_argument("--job", default="marginwatch-dev-bronze")
ap.add_argument("--region", default="ap-south-1")
a = ap.parse_args()

glue = boto3.client("glue", region_name=a.region)
s3 = boto3.client("s3", region_name=a.region)
run_id = glue.start_job_run(JobName=a.job, Arguments={"--business_date": a.date})["JobRunId"]
print(f"started {a.job} for business_date={a.date}: {run_id}")

while True:
    run = glue.get_job_run(JobName=a.job, RunId=run_id, PredecessorsIncluded=False)["JobRun"]
    state = run["JobRunState"]
    print(f"  {time.strftime('%H:%M:%S')}  {state}")
    if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"):
        break
    time.sleep(20)

print(f"\nstate={state}  execution time={run.get('ExecutionTime', 0)} s  workers={run.get('NumberOfWorkers')} x {run.get('WorkerType')}")
if state != "SUCCEEDED":
    print("error:", run.get("ErrorMessage"))
    print("logs : CloudWatch -> Log groups -> /aws-glue/jobs/error  (stream name = the run id)")
    raise SystemExit(1)

args = glue.get_job(JobName=a.job)["Job"]["DefaultArguments"]
bucket, prefix = args["--summary_path"][5:].split("/", 1)
objs = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/business_date={a.date}/").get("Contents", [])
latest = max(objs, key=lambda o: o["LastModified"])
summary = json.loads(s3.get_object(Bucket=bucket, Key=latest["Key"])["Body"].read())
print(f"\nrun summary ({latest['Key']}):")
print(f"  files in landing={summary['files_in_landing']}  loaded={summary['files_loaded']}  "
      f"quarantined={summary['files_quarantined']}")
for t, v in summary["tables"].items():
    print(f"    {t:26} {v['rows']:5} rows from {v['files']:3} files")
for q in summary["quarantined"]:
    print(f"  QUARANTINED {q['file']}: {q['error']}")
