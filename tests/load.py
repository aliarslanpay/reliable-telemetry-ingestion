#!/usr/bin/env python3
"""Bounded real-backend profiles and a backlog alert/recovery drill."""
import argparse
import datetime
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
from support import ROOT, RuntimeBlocked, rows, wait_for
from integration import Context
sys.path.insert(0,str(ROOT/"scripts"))
from telemetry_status import inspect


def percentile(samples, percentile):
    if not samples:return None
    values=sorted(samples)
    return values[max(0,(len(values)*percentile+99)//100-1)]


def profile(c, name, devices, window, count):
    before=len(c.pg.records())
    entries=[]
    peaks={"backlog":0,"gateway_rss_kib":None,"database_bytes":0,"wal_bytes":0,"combined_bytes":0}
    started=time.monotonic()
    for device in devices:
        proc,db=c.spawn(name+"-"+device,device,count=count,window=window,ack_timeout_ms=1000)
        entries.append((proc,db))
    sampling_stop=threading.Event()
    sampling_errors=[]
    def sample():
        try:
            while not sampling_stop.is_set():
                backlog=rss=dbsize=walsize=0
                rss_visible=True
                for proc,db in entries:
                    backlog+=sum(r[4]=="pending" for r in rows(db))
                    status=Path(f"/proc/{proc.process.pid}/status").read_text().splitlines()
                    if "Name:\t"+c.binary.name[:15] not in status:rss_visible=False
                    for line in status:
                        if line.startswith("VmRSS:"):rss+=int(line.split()[1])
                    dbsize+=db.stat().st_size if db.exists() else 0
                    wal=Path(str(db)+"-wal");walsize+=wal.stat().st_size if wal.exists() else 0
                if rss_visible:peaks["gateway_rss_kib"]=max(peaks["gateway_rss_kib"] or 0,rss)
                for key,value in (("backlog",backlog),("database_bytes",dbsize),("wal_bytes",walsize),("combined_bytes",dbsize+walsize)):
                    peaks[key]=max(peaks[key],value)
                sampling_stop.wait(0.02)
        except Exception as exc:sampling_errors.append(type(exc).__name__)
    monitor=threading.Thread(target=sample)
    monitor.start()
    try:
        for proc,db in entries:c.drain(proc,db,count,timeout=25)
        duration=time.monotonic()-started
    finally:
        sampling_stop.set();monitor.join(timeout=2)
    assert not monitor.is_alive() and not sampling_errors, "measurement sampler failed"
    latencies=[];payload=0
    for proc,db in entries:
        accepted=c.accepted(proc)
        payload+=sum(len(json.dumps(e,sort_keys=True,separators=(",",":"))) for e in accepted)
        latencies += [e["delivery_latency_ms"] for e in proc.entries() if e.get("kind")=="acknowledged" and "delivery_latency_ms" in e]
        c.stop(proc)
    unique=len(c.pg.records())-before
    assert unique==count*len(devices) and len(latencies)==unique
    return {"profile":name,"devices":len(devices),"window":window,"unique_commits":unique,
        "wire_payload_bytes":payload,"duration_seconds":duration,"unique_commits_per_second":unique/duration,
        "ack_latency_ms":{"samples":len(latencies),"p50":percentile(latencies,50),"p95":percentile(latencies,95),"p99":percentile(latencies,99)},
        "observed_high_water":peaks,"sampling_interval_ms":20}


def execute(c,count):
    c.start()
    profiles=[]
    for devices in (("sensor-a",),("sensor-a","sensor-b")):
        for window in (1,8):profiles.append(profile(c,f"healthy-{len(devices)}dev-w{window}",devices,window,count))
    c.stop_workers();c.start_workers(delay=60)
    slow=profile(c,"slow-backend",("sensor-a","sensor-b"),8,min(count,60))
    c.stop_workers()
    proc,db=c.spawn("operational-drill",count=30,window=4,ack_timeout_ms=150)
    accepted=c.submitted(proc,30)
    wait_for(lambda:inspect(db,300)[1],label="real backlog-age alert")
    command=[sys.executable,str(ROOT/"scripts/telemetry_status.py"),"status","--db",str(db),"--alert-age-ms","300"]
    diagnostic=subprocess.run(command,capture_output=True,text=True,timeout=2)
    assert diagnostic.returncode==8 and json.loads(diagnostic.stdout)["pending"]==30
    restored=time.monotonic();c.start_workers()
    c.drain(proc,db,accepted=accepted);recovery=time.monotonic()-restored
    recovered=subprocess.run(command,capture_output=True,text=True,timeout=2)
    assert recovered.returncode==0 and not json.loads(recovered.stdout)["alert"]
    c.stop(proc)
    return {"healthy_profiles":profiles,"slow_backend":slow,"operational_drill":{"status":"passed",
        "accepted_backlog":30,"alert_exit_code":8,"recovery_exit_code":0,"recovery_seconds":recovery,
        "diagnostic":json.loads(diagnostic.stdout)}}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--gateway",required=True,type=Path)
    parser.add_argument("--count",type=int,default=200)
    parser.add_argument("--output",type=Path,default=Path("results/load.json"))
    args=parser.parse_args()
    if not 20<=args.count<=1000:parser.error("count range 20..1000 per device/profile")
    args.output.parent.mkdir(parents=True,exist_ok=True)
    result={"scope":"real PostgreSQL commit/ACK measurements and operational drill",
        "date_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "gateway_versions":json.loads(subprocess.check_output([str(args.gateway.resolve()),"version"],text=True)),"python":sys.version.split()[0]}
    def timeout(*_):raise TimeoutError("load budget exceeded")
    signal.signal(signal.SIGALRM,timeout);signal.alarm(180)
    code=1
    with tempfile.TemporaryDirectory(prefix="telemetry-load-") as temp:
        context=None
        try:
            context=Context(args.gateway.resolve(),temp)
            result.update(execute(context,args.count),status="passed");code=0
        except RuntimeBlocked as exc:
            result.update(status="blocked",reason=str(exc),healthy_profiles=[],operational_drill={"status":"blocked"});code=77
        except Exception as exc:result.update(status="failed",error_type=type(exc).__name__)
        finally:
            signal.alarm(0)
            if context:
                try:context.close()
                except Exception as exc:result.update(cleanup_error=type(exc).__name__);code=1
            args.output.write_text(json.dumps(result,indent=2)+"\n");print(json.dumps(result,separators=(",",":")))
    return code


if __name__=="__main__":raise SystemExit(main())
