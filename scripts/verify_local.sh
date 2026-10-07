#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"
PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  printf '%s\n' 'Create .venv and install ingestion/requirements.txt first.' >&2
  exit 2
fi
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j2
ctest --test-dir build --output-on-failure
"$PYTHON_BIN" tests/cli_checks.py --gateway build/gateway
PYTHONPATH=ingestion "$PYTHON_BIN" -m unittest discover -s tests -p 'test_*.py'
timeout 60s "$PYTHON_BIN" tests/mqtt_smoke.py --gateway build/gateway
timeout 60s "$PYTHON_BIN" tests/broker_faults.py --gateway build/gateway
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DTELEMETRY_SANITIZE=ON
cmake --build build-asan -j2
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1:halt_on_error=1}"
export UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1}"
ctest --test-dir build-asan --output-on-failure
"$PYTHON_BIN" tests/cli_checks.py --gateway build-asan/gateway
timeout 60s "$PYTHON_BIN" tests/broker_faults.py --gateway build-asan/gateway --output results/broker-faults-asan.json
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DTELEMETRY_TSAN=ON
cmake --build build-tsan --target handoff_test -j2
if [[ "${TELEMETRY_TSAN_NO_ASLR:-0}" == 1 ]]; then
  setarch "$(uname -m)" -R ctest --test-dir build-tsan -R '^handoff$' --output-on-failure
else
  ctest --test-dir build-tsan -R '^handoff$' --output-on-failure
fi
timeout 200s "$PYTHON_BIN" tests/integration.py --gateway build/gateway
timeout 200s "$PYTHON_BIN" tests/load.py --gateway build/gateway --count 200
"$PYTHON_BIN" scripts/storage_probe.py --gateway build/gateway
