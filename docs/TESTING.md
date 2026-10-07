# Testing

Install the native prerequisites listed in README and create `.venv` with
`ingestion/requirements.txt`. Run the suite on Ubuntu 24.04 as a normal user:

```sh
env -u PYTHONPATH -u PYTHONHOME scripts/verify_local.sh
```

The runner creates isolated temporary Mosquitto/PostgreSQL services on ephemeral
loopback ports and stops only its own processes. Runtime results and failure logs
go to ignored `results/`; credentials are excluded. Exit 77 means a required
environment capability is unavailable.

## Test coverage

| Check | What it exercises |
| --- | --- |
| CTest + Python core | Canonical wire fixtures, validation, persistent outbox, ACK handoff |
| `tests/cli_checks.py` | Timestamp-series bounds, port/window bounds, option allowlists and rejection without state changes |
| `tests/mqtt_smoke.py` | Real broker delivery with injected application ACKs |
| `tests/broker_faults.py` | Reconnect, crash, retry identity, malformed/mismatched ACKs, capacity and shutdown |
| `tests/integration.py` | Eleven real MQTT/PostgreSQL fault groups, including commit-before-ACK, post-commit crash, two workers, outages, overload and ACLs |
| `tests/postgres_checks.py` | Independent-process insertion arbitration, conflict preservation, role restrictions and indexed range queries |
| `tests/load.py` | Healthy profiles, slow ingestion and a backlog alert/recovery drill |
| `scripts/storage_probe.py` | Durable SQLite enqueue cost and sampled RSS/database/WAL sizes |
| `tests/cloud_unit.py` | DynamoDB conditional writes, duplicate/conflict semantics and Lambda validation using SDK models |
| `tests/cloud_resources_unit.py` | Recorded ownership and lifecycle rejection before destructive SDK calls |
| `tests/cloud_teardown_unit.py` | Bucket identity, paginated rule absence and rejection of API errors |

Transport tests inject ACKs through an authorized publisher; PostgreSQL tests
establish backend durability. A pre-commit hook checks that no row or success ACK
is visible before COMMIT. A post-commit crash preserves one stored row and retries
as a duplicate. Hooks require `--test-hooks DIRECTORY` and are disabled in normal
worker operation.

Capacity tests first accept an event, then reach item/payload/quarantine, SQLite
BUSY or FULL guards. Rejection preserves existing rows and sequences. Invalid ACK
tests check counters and durable state before sending a matching positive ACK.

## Sanitizers

`scripts/verify_local.sh` builds ASan/UBSan host and broker checks, and a separate
TSan build for the handoff target. Third-party libraries are not instrumented.
Leak detection is enabled by default. If the host cannot support a sanitizer,
record that limitation separately from functional test results.

If TSan reports `unexpected memory mapping` at startup, this optional wrapper
disables ASLR only for the test process:

```sh
env -u PYTHONPATH -u PYTHONHOME TELEMETRY_TSAN_NO_ASLR=1 scripts/verify_local.sh
```

It leaves race reporting and system-wide ASLR settings unchanged.

## Cloud and CI

The [AWS runbook](../cloud/README.md) provides offline SDK tests and SAM lint/build,
then a separate bounded live deployment. Live checks exercise mutual TLS,
cross-device publish/subscribe denial, IoT Rule dispatch, DynamoDB originals,
application ACKs, duplicate/conflict arbitration, a Lambda error alarm and failure
destination. Read-only teardown checks query the recorded resources after cleanup.
IoT Rule dispatch-error injection is outside that run's scope.

Hosted CI execution, Compose runtime, host power loss and sustained production
loads require separate execution. Live AWS results are pending.
