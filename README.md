# Reliable telemetry ingestion

A Linux C++ gateway and ingestion service for synthetic sensor events. The wire
contract uses stable event identities and content fingerprints so a backend can
distinguish a retry from a conflicting delivery.

## Build and test

Ubuntu 24.04 prerequisites: `g++`, `cmake`, `libssl-dev`, `nlohmann-json3-dev`,
`libsqlite3-dev`, `libmosquitto-dev`, `mosquitto-clients`, Python 3.12; Docker Compose is optional.

```sh
python3 -m venv .venv
.venv/bin/pip install -r ingestion/requirements.txt
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j2
ctest --test-dir build --output-on-failure
.venv/bin/python tests/cli_checks.py --gateway build/gateway
PYTHONPATH=ingestion python3 -m unittest discover -s tests -p 'test_*.py'
```

The C++ and Python implementations check the same canonical JSON/SHA-256 fixtures.
See [the wire contract](docs/CONTRACT.md) and [outbox policy](docs/OUTBOX.md).

```sh
mkdir -p .runtime
build/gateway enqueue --db .runtime/sensor-a.db --device sensor-a --count 3
build/gateway status --db .runtime/sensor-a.db --device sensor-a
```

Enqueue returns one JSON result per input; exit 3 means at least one input was
rejected. Status reads durable state.

## Local services and delivery

```sh
python3 scripts/local_setup.py
docker compose up -d --build
```

Set `MQTT_USERNAME=sensor-a` and `MQTT_PASSWORD` from the generated private
`.runtime/credentials.json`, then:

```sh
build/gateway run --db .runtime/sensor-a.db --device sensor-a --port 18883 --count 10 --duration-ms 15000
docker compose down -v
```

The worker inserts an immutable event and publishes `stored` after PostgreSQL
COMMIT. A repeated identical delivery receives `duplicate`. A database error
produces no success ACK. See [delivery ownership and limits](docs/DELIVERY.md).
Two independent workers share the event subscription and arbitrate through the
database primary key; see [database and query commands](docs/DATABASE.md).
Use the native PostgreSQL tests to exercise database durability and queries.

The transport-only check uses a real isolated Mosquitto broker and deliberately
injected ACKs; it does not claim PostgreSQL verification:

```sh
.venv/bin/python tests/mqtt_smoke.py --gateway build/gateway
.venv/bin/python tests/broker_faults.py --gateway build/gateway
```

The complete native runner is `scripts/verify_local.sh`; see
[verification scope](docs/TESTING.md). Use a normal user for
PostgreSQL; initdb refuses root.

Use `python3 scripts/telemetry_status.py status --db .runtime/sensor-a.db` for
read-only backlog and file-size diagnostics. See the [runbook](docs/RUNBOOK.md)
and [measurement boundaries](docs/MEASUREMENTS.md).

The GitHub workflow configures real native-service verification and offline SAM
checks. Hosted CI and AWS deployment have not been run.
