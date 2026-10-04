#!/usr/bin/env python3
"""Run against an isolated project PostgreSQL database, never a mock store."""
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import secrets
import time
import psycopg
from psycopg.types.json import Jsonb
from telemetry_ingestion.core import canonical, fingerprint
from telemetry_ingestion.postgres import PgStore


def store_process(event, barrier, results):
    store = PgStore(os.environ["DATABASE_URL"])
    try:
        barrier.wait(timeout=4)
        results.put(store.store(event))
    except Exception as exc:
        results.put("error:" + type(exc).__name__)
    finally:
        store.close()


def race(events):
    context = mp.get_context("spawn")
    barrier = context.Barrier(len(events))
    results = context.Queue(maxsize=len(events))
    children = [context.Process(target=store_process, args=(event, barrier, results)) for event in events]
    try:
        for child in children:
            child.start()
        returned = [results.get(timeout=6) for _ in children]
        for child in children:
            child.join(timeout=2)
            assert child.exitcode == 0, "database process failed"
        return returned
    finally:
        for child in children:
            if child.is_alive():
                child.terminate()
                child.join(timeout=2)


def checks():
    stream = secrets.token_hex(16)
    original = dict(schema_version=1, device_id="sensor-a", stream_id=stream,
        sequence=1, timestamp_ms=1791100000000, temperature_mc=20000, pressure_pa=101325)
    original["fingerprint"] = fingerprint(original)
    identical = race([original, dict(reversed(list(original.items())))])
    assert sorted(identical) == ["duplicate", "stored"], identical
    conflict = {**original, "temperature_mc":21000}
    conflict["fingerprint"] = fingerprint(conflict)
    assert sorted(race([original, conflict])) == ["conflict", "duplicate"]
    with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=2) as conn:
        row = conn.execute("SELECT fingerprint,data FROM events WHERE device_id=%s AND stream_id=%s AND sequence=1", ("sensor-a", stream)).fetchone()
        assert row == (original["fingerprint"], json.loads(canonical(original))), "original changed"
        try:
            with conn.transaction():
                conn.execute("UPDATE events SET fingerprint=%s WHERE device_id=%s AND stream_id=%s", (conflict["fingerprint"], "sensor-a", stream))
        except psycopg.Error:
            pass
        else:
            raise AssertionError("worker role/immutability guard did not reject update")
    # A selective real workload makes the index choice meaningful.
    sample = []
    for seq in range(2, 2002):
        event = {**original, "sequence":seq}
        event["fingerprint"] = fingerprint(event)
        sample.append((event["device_id"],stream,seq,event["fingerprint"],Jsonb(json.loads(canonical(event)))))
    with psycopg.connect(os.environ["DATABASE_ADMIN_URL"], connect_timeout=2) as admin:
        with admin.cursor() as cur:
            cur.executemany("INSERT INTO events(device_id,stream_id,sequence,fingerprint,data) VALUES(%s,%s,%s,%s,%s)", sample)
        admin.execute("ANALYZE events")
    with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=2) as conn:
        total = conn.execute("SELECT count(*) FROM events WHERE device_id=%s AND stream_id=%s", ("sensor-a",stream)).fetchone()[0]
        assert total == 2001
        subset = conn.execute("SELECT sequence FROM events WHERE device_id=%s AND stream_id=%s AND sequence BETWEEN 990 AND 1000 ORDER BY sequence LIMIT 20", ("sensor-a", stream)).fetchall()
        assert subset == [(seq,) for seq in range(990, 1001)]
        plan = conn.execute("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT sequence FROM events WHERE device_id=%s AND stream_id=%s AND sequence BETWEEN 990 AND 1000 ORDER BY sequence LIMIT 20", ("sensor-a",stream)).fetchone()[0]
        assert "events_pkey" in json.dumps(plan), "selective range did not use primary index"
    return {"concurrent_process_results":identical, "immutable_conflict":"passed", "restricted_update":"passed", "sample_rows":2001, "query_plan":plan}


if __name__ == "__main__":
    if "DATABASE_URL" not in os.environ or "DATABASE_ADMIN_URL" not in os.environ:
        raise SystemExit("Use the isolated integration runner or set both disposable database DSNs.")
    print(json.dumps(checks(), separators=(",", ":")))
