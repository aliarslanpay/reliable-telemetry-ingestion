# Local operator runbook

`python3 scripts/telemetry_status.py status --db .runtime/sensor-a.db` reads actual
SQLite state without creating or changing the outbox. It reports pending/terminal
counts, oldest pending age, payload capacity and database/WAL/SHM sizes. Exit 8
means pending age meets the alert threshold; 0 means healthy at this instant;
2 means diagnostics failed. `--alert-age-ms 5000` is the default.

If backlog age rises, check gateway status for `connected`, `mqtt_transport_code`,
`retries`, `wire_outstanding`, `callback_dropped` and `checkpoint_error_code`.
Check broker authentication/topic ACL logs, worker `transient` and persistence
logs, then PostgreSQL readiness. Broker PUBACK is not storage evidence. Restore
the failed component and check pending reaches zero with accepted IDs in the
backend. A `stored`/`duplicate` ACK must carry the matching fingerprint.

If input is rejected, distinguish `item_capacity`, `payload_capacity`,
`storage_busy`, `storage_full` and `storage_io`. Preserve database and WAL. Never
delete pending rows just to clear an alert. Terminal inspection:

```sh
python3 scripts/telemetry_status.py quarantine --db .runtime/sensor-a.db --limit 100
build/gateway discard --db .runtime/sensor-a.db --device sensor-a --stream STREAM --sequence 1
```

`discard` explicitly removes only the selected terminal record. Export/review it
first. Quarantine overflow stays visible as `blocked_terminal`; those records
remain charged to storage and stop retrying. Long-lived readers can hold WAL;
close readers and stop the gateway gracefully before investigating checkpoint
failures. Use a filesystem quota for a hard total disk limit.

Reproduce the operational drill with `.venv/bin/python tests/load.py --gateway
build/gateway --count 200`. It stops both owned workers, accepts a bounded backlog,
observes an age alert (exit 8), captures diagnosis, restores workers, reconciles
accepted IDs and checks alert recovery (exit 0). The real PostgreSQL drill is
blocked in the development runtime.

For Compose, `docker compose stop worker-1 worker-2` / `docker compose start
worker-1 worker-2` reproduce the same interruption. `docker compose down -v` removes
the selected project's containers/volume and ends its deduplication retention.
Keep runtime files if retrying against preserved backend storage.
