# Reliable telemetry ingestion

A Linux C++ gateway and ingestion service for synthetic sensor events. The wire
contract uses stable event identities and content fingerprints so a backend can
distinguish a retry from a conflicting delivery.

## Build and test

Ubuntu 24.04 prerequisites: `g++`, `cmake`, `libssl-dev`, `nlohmann-json3-dev`, `libsqlite3-dev`.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j2
ctest --test-dir build --output-on-failure
python3 tests/cli_checks.py --gateway build/gateway
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
rejected. Status reads durable state. This stage provides persistence; MQTT
delivery is introduced in the next stage.
