#!/usr/bin/env python3
"""Creates (or updates) the hourly pipeline job and starts a run.

    .venv/bin/python tools/create_job.py            # create/update and run
    .venv/bin/python tools/create_job.py --no-run   # create/update only

I used this instead of `databricks bundle deploy` because I didn't have the CLI set up.
"""
import os
import sys

import requests

JOB_NAME = "calgary-transit-pipeline"
host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
token = os.environ.get("DATABRICKS_TOKEN", "")
if not host or not token:
    sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN first: set -a; source .env; set +a")
s = requests.Session()
s.headers["Authorization"] = f"Bearer {token}"


def api(method, path, **kw):
    r = s.request(method, f"{host}/api/2.1/{path}", timeout=60, **kw)
    if not r.ok:
        sys.exit(f"{method} {path} failed: HTTP {r.status_code} {r.text[:500]}")
    return r.json()


user = s.get(f"{host}/api/2.0/preview/scim/v2/Me", timeout=30).json()["userName"]
nb = f"/Users/{user}/calgary-transit-lakehouse/notebooks"

steps = [
    ("bronze_realtime", "01_bronze_realtime", {}),
    ("schedule", "02_schedule", {}),
    ("silver_realtime", "03_silver_realtime", {}),
    ("stop_events", "04_silver_stop_events", {"lookback_days": "2"}),
    ("gold", "05_gold", {}),
]
tasks = []
for i, (key, notebook, params) in enumerate(steps):
    t = {
        "task_key": key,
        "notebook_task": {"notebook_path": f"{nb}/{notebook}", "source": "WORKSPACE", "base_parameters": params},
        "max_retries": 1,
        "min_retry_interval_millis": 60000,
        "timeout_seconds": 3600,
    }
    if i > 0:
        t["depends_on"] = [{"task_key": steps[i - 1][0]}]
    tasks.append(t)

settings = {
    "name": JOB_NAME,
    "max_concurrent_runs": 1,
    "schedule": {"quartz_cron_expression": "0 0 * * * ?", "timezone_id": "America/Edmonton", "pause_status": "UNPAUSED"},
    "email_notifications": {"on_failure": [user]},
    "tasks": tasks,
}

existing = [j for j in api("GET", "jobs/list", params={"name": JOB_NAME}).get("jobs", [])]
if existing:
    job_id = existing[0]["job_id"]
    api("POST", "jobs/reset", json={"job_id": job_id, "new_settings": settings})
    print(f"Updated job {job_id}")
else:
    job_id = api("POST", "jobs/create", json=settings)["job_id"]
    print(f"Created job {job_id}")

print(f"Job page: {host}/jobs/{job_id}")
if "--no-run" not in sys.argv:
    run = api("POST", "jobs/run-now", json={"job_id": job_id})
    print(f"Started run {run['run_id']}: watch it on the job page (Runs tab)")
