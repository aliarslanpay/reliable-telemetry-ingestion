#!/usr/bin/env python3
"""Measure acceptance-only storage; this is not backend throughput."""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0,str(Path(__file__).resolve().parent))
from telemetry_status import inspect


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--gateway",required=True,type=Path)
    parser.add_argument("--count",type=int,default=1000)
    parser.add_argument("--output",type=Path,default=Path("results/storage.json"))
    args=parser.parse_args()
    if not 1<=args.count<=3000:parser.error("count range 1..3000")
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="telemetry-storage-") as temp:
        db=Path(temp)/"outbox.db"
        peaks={"database_bytes":0,"wal_bytes":0,"combined_bytes":0,"gateway_rss_kib":None}
        started=time.monotonic()
        with (Path(temp)/"receipts.jsonl").open("w") as output:
            proc=subprocess.Popen([str(args.gateway.resolve()),"enqueue","--db",str(db),"--device","sensor-a","--count",str(args.count),"--timestamp-ms","1791100000000"],stdout=output,stderr=subprocess.PIPE)
            while proc.poll() is None:
                sizes=[Path(str(db)+suffix).stat().st_size if Path(str(db)+suffix).exists() else 0 for suffix in ("","-wal")]
                peaks["database_bytes"]=max(peaks["database_bytes"],sizes[0]);peaks["wal_bytes"]=max(peaks["wal_bytes"],sizes[1])
                peaks["combined_bytes"]=max(peaks["combined_bytes"],sum(sizes))
                try:
                    status=Path(f"/proc/{proc.pid}/status").read_text().splitlines()
                    if "Name:\t"+args.gateway.name[:15] in status:
                        for line in status:
                            if line.startswith("VmRSS:"):peaks["gateway_rss_kib"]=max(peaks["gateway_rss_kib"] or 0,int(line.split()[1]))
                except FileNotFoundError:pass
                if time.monotonic()-started>20:
                    proc.kill();proc.wait();raise RuntimeError("storage probe deadline")
                time.sleep(0.002)
            _,error=proc.communicate(timeout=2)
            if proc.returncode!=0:raise RuntimeError("enqueue failed; storage probe incomplete")
        duration=time.monotonic()-started
        receipts=[json.loads(line) for line in (Path(temp)/"receipts.jsonl").read_text().splitlines()]
        state,_=inspect(db)
        assert len(receipts)==args.count and all(r["kind"]=="accepted" for r in receipts) and state["items"]==args.count
        peaks["database_bytes"]=max(peaks["database_bytes"],state["database_bytes"])
        result={"scope":"SQLite durable enqueue only; no backend throughput or ACK latency", "status":"passed",
            "date_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"accepted":args.count,
            "payload_bytes":state["payload_bytes"],"duration_seconds":duration,"sampling_interval_ms":2,
            "observed_high_water":peaks,"final_database_bytes":state["database_bytes"],"final_wal_bytes":state["wal_bytes"]}
        if peaks["gateway_rss_kib"] is None:result["rss_status"]="not observable: reported child PID does not identify the gateway in /proc"
        args.output.write_text(json.dumps(result,indent=2)+"\n");print(json.dumps(result,separators=(",",":")))


if __name__=="__main__":main()
