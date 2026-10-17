#!/usr/bin/env bash
# loads .env and keeps the mac awake while polling
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [ -f .env ] && source .env; set +a
mkdir -p logs
exec caffeinate -i .venv/bin/python poller/poller.py "$@"
