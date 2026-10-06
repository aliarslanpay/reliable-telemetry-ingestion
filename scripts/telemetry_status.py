#!/usr/bin/env python3
"""Read-only outbox diagnostics; exit 8 means a real pending-age alert."""
import argparse
import json
from pathlib import Path
import sqlite3
import time


def inspect(path, alert_age_ms=5000, limit=100, terminal=False):
    path = Path(path).resolve()
    if not path.is_file():
        raise ValueError("outbox file does not exist")
    started = time.monotonic()
    with sqlite3.connect(path.as_uri()+"?mode=ro", uri=True, timeout=0.15) as conn:
        conn.execute("PRAGMA query_only=ON")
        conn.set_progress_handler(lambda:time.monotonic()-started>1, 1000)
        meta = conn.execute("SELECT device,stream,next_seq,max_items,max_bytes,max_quarantine,max_pages FROM meta WHERE singleton=1").fetchone()
        if meta is None:
            raise ValueError("outbox metadata missing")
        if terminal:
            records = conn.execute("SELECT wire,state,reason FROM outbox WHERE state!='pending' ORDER BY enqueued_ms,stream,seq LIMIT ?", (limit,)).fetchall()
            return {"device_id":meta[0],"terminal":[{"event":json.loads(r[0]),"state":r[1],"reason":r[2]} for r in records]},False
        count,payload,pending,quarantine,blocked,oldest = conn.execute("SELECT count(*),coalesce(sum(length(CAST(wire AS BLOB))),0),coalesce(sum(state='pending'),0),coalesce(sum(state='quarantine'),0),coalesce(sum(state='blocked'),0),coalesce(min(CASE WHEN state='pending' THEN enqueued_ms END),0) FROM outbox").fetchone()
    age = max(0,int(time.time()*1000)-oldest) if oldest else 0
    alert = pending>0 and age>=alert_age_ms
    sizes = {name:(Path(str(path)+suffix).stat().st_size if Path(str(path)+suffix).exists() else 0)
        for name,suffix in (('database_bytes',''),('wal_bytes','-wal'),('shm_bytes','-shm'))}
    return {"device_id":meta[0],"stream_id":meta[1],"next_sequence":meta[2],"items":count,
        "payload_bytes":payload,"pending":pending,"quarantine":quarantine,"blocked_terminal":blocked,
        "oldest_pending_age_ms":age,"alert":alert,"alert_age_ms":alert_age_ms,
        "max_items":meta[3],"max_payload_bytes":meta[4],"max_quarantine":meta[5],"max_database_pages":meta[6],**sizes},alert


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("command",choices=("status","quarantine"))
    parser.add_argument("--db",required=True,type=Path)
    parser.add_argument("--alert-age-ms",type=int,default=5000)
    parser.add_argument("--limit",type=int,default=100)
    args=parser.parse_args()
    if not 1<=args.limit<=100 or not 1<=args.alert_age_ms<=3600000:parser.error("limit/alert range")
    try:
        result,alert=inspect(args.db,args.alert_age_ms,args.limit,args.command=="quarantine")
    except (ValueError,sqlite3.Error,OSError) as exc:
        print(json.dumps({"kind":"diagnostic_error","error_type":type(exc).__name__}))
        return 2
    print(json.dumps(result,separators=(",",":")))
    return 8 if alert else 0


if __name__=="__main__":raise SystemExit(main())
