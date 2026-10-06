# Measurement procedure

`tests/load.py` uses real Mosquitto/PostgreSQL, two workers and bounded profiles:
one/two devices with application window 1/8, followed by slow ingestion and a
backlog alert drill. Defaults are 200 events per device/profile. It records
runtime versions, wire payload, duration, unique commit throughput, ACK
percentiles/sample counts, sampled peak backlog, gateway RSS, database/WAL sizes
and outage recovery time in `results/load.json`.

Healthy duration starts before gateway process creation and ends after the last
application ACK drains the outbox. It includes connection, subscription, SQLite
acceptance and persistence. Throughput is verified new backend rows divided by
end-to-end duration. ACK latency runs from the monotonic first publish attempt to
the validated application ACK. Percentiles use nearest rank. Slow-backend and
outage results are separate; host/cloud wall timestamps are never subtracted.

RSS/file peaks use 20 ms samples and can miss brief peaks. Repeat profiles on a
specified host to compare them; one run does not establish maximum throughput.
If PostgreSQL cannot start, the runner records a blocked result and exits 77.

`scripts/storage_probe.py --gateway build/gateway` measures 1000 durable SQLite
enqueues without network delivery. It samples RSS/file sizes every 2 ms; the
enqueue loop has no 2 ms pacing. This measures storage overhead rather than
backend throughput or ACK latency.

FULL synchronization entails an fsync per enqueue. Batching could amortize this
cost but would change acceptance granularity and crash windows. The gateway uses
individual transactional acceptance and resets read statements before COMMIT so
checkpoint work can release WAL frames.
