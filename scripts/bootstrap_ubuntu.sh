#!/usr/bin/env bash
set -euo pipefail
if [[ ! -f /etc/os-release ]] || ! command -v apt-get >/dev/null; then
  printf '%s\n' 'This bootstrap targets Ubuntu 24.04; install the README prerequisites for other Linux distributions.' >&2
  exit 2
fi
if [[ "${EUID}" -eq 0 ]]; then
  printf '%s\n' 'Run this as your normal Ubuntu user; the PostgreSQL tests refuse root.' >&2
  exit 2
fi
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"
sudo apt-get update
sudo apt-get install -y g++-13 cmake libssl-dev libsqlite3-dev nlohmann-json3-dev libmosquitto-dev mosquitto mosquitto-clients postgresql-16 python3-venv
python3 -I -m venv .venv
.venv/bin/python -I -m pip install -r ingestion/requirements.txt -r ingestion/requirements-cloud.txt
.venv/bin/python -I -m pip check
printf '%s\n' 'Prerequisites installed. Run scripts/verify_local.sh as this same normal user.'
