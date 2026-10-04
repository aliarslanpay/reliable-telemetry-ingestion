# PostgreSQL arbitration and retrieval

The only event index is the composite primary key `(device_id, stream_id, sequence)`.
It serves exact lookup and ordered bounded range retrieval. Count scans one stream;
query timeout prevents an unlimited wait, not unlimited logical table growth.
Stored identities are retained until explicit teardown. No automatic deletion or
TTL is part of correctness.

`INSERT ... ON CONFLICT DO NOTHING RETURNING fingerprint` arbitrates first
insertion. A losing writer runs a new READ COMMITTED `SELECT` after the insert
statement has waited for the competing transaction. It compares canonical data
and fingerprint. It never updates the original. The adapter returns its result
only after the transaction context has committed; the MQTT worker constructs and
publishes the ACK afterward. Transient connection/query/commit failures produce
no success ACK. Connection timeout is 2 seconds; statement/lock timeouts are
1500/1000 ms. Worker credentials allow SELECT/INSERT only. A trigger also blocks
UPDATE/DELETE through the administrative owner.

Two independent workers use the same MQTT shared subscription. The database,
not worker-local memory or subscription routing, enforces uniqueness. The real
integration runner checks subscription grants and observes work in both processes.

With `PYTHONPATH=ingestion` and `DATABASE_URL` set privately:

```sh
python -m telemetry_ingestion.query get --device sensor-a --stream STREAM --first 1
python -m telemetry_ingestion.query range --device sensor-a --stream STREAM --first 1 --last 100 --limit 100
python -m telemetry_ingestion.query count --device sensor-a --stream STREAM
python -m telemetry_ingestion.query plan --device sensor-a --stream STREAM --first 10 --last 20 --limit 20
```

Replace `STREAM` with a real outbox stream ID. `plan` uses real EXPLAIN ANALYZE;
there is no assumed query-plan result. `tests/postgres_checks.py` races independent
processes, checks the committed original and role restrictions, seeds 2001 events,
and asserts the selective range uses `events_pkey`. This execution requires a real
isolated PostgreSQL service; it is not part of the dependency-free host test suite.
