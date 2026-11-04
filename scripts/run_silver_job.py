"""
Start the Silver Glue job for one business date, wait, and print its summary and data quality result.

Usage:  AWS_PROFILE=marginwatch python scripts/run_silver_job.py --date 2026-10-26
"""
import argparse, json, time

import boto3

ap = argparse.ArgumentParser()
ap.add_argument("--date", required=True)
ap.add_argument("--job", default="marginwatch-dev-silver")
ap.add_argument("--region", default="ap-south-1")
a = ap.parse_args()

glue, s3 = boto3.client("glue", region_name=a.region), boto3.client("s3", region_name=a.region)
run_id = glue.start_job_run(JobName=a.job, Arguments={"--business_date": a.date})["JobRunId"]
print(f"started {a.job} for business_date={a.date}: {run_id}")
while True:
    run = glue.get_job_run(JobName=a.job, RunId=run_id)["JobRun"]
    print(f"  {time.strftime('%H:%M:%S')}  {run['JobRunState']}")
    if run["JobRunState"] in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"):
        break
    time.sleep(20)
print(f"\nstate={run['JobRunState']}  execution time={run.get('ExecutionTime', 0)} s  "
      f"workers={run.get('NumberOfWorkers')} x {run.get('WorkerType')}")
if run["JobRunState"] != "SUCCEEDED":
    print("error:", run.get("ErrorMessage"))
    print("(a quality-gate failure still writes its summary - shown below if it exists)")

args = glue.get_job(JobName=a.job)["Job"]["DefaultArguments"]
bucket, prefix = args["--summary_path"][5:].split("/", 1)
objs = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/business_date={a.date}/").get("Contents", [])
if not objs:
    raise SystemExit("no run summary written - see CloudWatch logs: /aws-glue/jobs/error, stream = the run id")
s = json.loads(s3.get_object(Bucket=bucket, Key=max(objs, key=lambda o: o["LastModified"])["Key"])["Body"].read())
print(f"\nrun summary {s['run_id']}:")
for t, v in s["tables"].items():
    print(f"    {t:30} silver {v['rows']:5}  (bronze {v['bronze_rows']:5}, duplicates removed {v['duplicates_removed']})")
print(f"  DQ: {s['dq']['checks']} checks, {s['dq']['pass']} pass, {len(s['dq']['warn'])} warn, {len(s['dq']['fail'])} fail")
for x in s["dq"]["warn"]:
    print("    WARN", x)
for x in s["dq"]["fail"]:
    print("    FAIL", x)
