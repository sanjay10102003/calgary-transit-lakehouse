#!/usr/bin/env python3
"""Copies notebooks/ into the workspace under /Users/<you>/calgary-transit-lakehouse/notebooks.

    set -a; source .env; set +a
    .venv/bin/python tools/deploy_notebooks.py
"""
import base64
import os
import sys
from pathlib import Path

import requests

host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
token = os.environ.get("DATABRICKS_TOKEN", "")
if not host or not token:
    sys.exit("Set DATABRICKS_HOST and DATABRICKS_TOKEN first: set -a; source .env; set +a")

s = requests.Session()
s.headers["Authorization"] = f"Bearer {token}"

me = s.get(f"{host}/api/2.0/preview/scim/v2/Me", timeout=30)
me.raise_for_status()
user = me.json()["userName"]
target_dir = f"/Users/{user}/calgary-transit-lakehouse/notebooks"

r = s.post(f"{host}/api/2.0/workspace/mkdirs", json={"path": target_dir}, timeout=30)
r.raise_for_status()

src = Path(__file__).resolve().parent.parent / "notebooks"
lang = {".py": "PYTHON", ".sql": "SQL"}
for nb in sorted(src.iterdir()):
    if nb.suffix not in lang:
        continue
    path = f"{target_dir}/{nb.stem}"
    r = s.post(f"{host}/api/2.0/workspace/import", timeout=60, json={
        "path": path,
        "format": "SOURCE",
        "language": lang[nb.suffix],
        "content": base64.b64encode(nb.read_bytes()).decode(),
        "overwrite": True,
    })
    print(("OK   " if r.ok else f"FAIL {r.status_code} ") + path + ("" if r.ok else f"  {r.text[:200]}"))

print(f"\nOpen in Databricks: Workspace -> Users -> {user} -> calgary-transit-lakehouse -> notebooks")
