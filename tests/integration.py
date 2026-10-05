#!/usr/bin/env python3
"""Real Mosquitto/PostgreSQL faults with durable identity reconciliation."""
import argparse
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

from support import ROOT, Broker, Peer, Postgres, RuntimeBlocked, gateway, rows, wait_for, worker
sys.path.insert(0, str(ROOT / "ingestion"))
from telemetry_ingestion.core import canonical, fingerprint
from broker_faults import ack, collect
import postgres_checks

SCENARIOS = ["normal devices and streams", "broker disconnect/restart", "gateway SIGKILL/restart",
    "commit-before-ACK and post-commit crash", "two-worker duplicates/conflicts", "database outage/recovery",
    "capacity and slow ingestion", "invalid input and ACK guards", "storage failure and quarantine limits",
    "graceful shutdown/restart", "device ACL isolation"]


def identity(event):
    return tuple(event[k] for k in ("device_id", "stream_id", "sequence"))


class Context:
    def __init__(self, binary, directory):
        self.binary = binary
        self.directory = Path(directory)
        self.pg = Postgres(self.directory / "postgres")
        self.broker = Broker(self.directory / "broker")
        self.workers = []
        self.gateways = []
        self.results = []

    def start(self):
        self.pg.initialize()
        self.broker.start()
        self.start_workers()

    def start_workers(self, **hooks):
        for name in ("worker-1", "worker-2"):
            proc = worker(self.broker, self.pg, self.directory / (name + "-" + secrets.token_hex(3)), name, **hooks)
            self.workers.append(proc)

    def stop_workers(self):
        for proc in self.workers:
            proc.stop()
        self.workers.clear()

    def spawn(self, name, device="sensor-a", **options):
        case = self.directory / name
        proc = gateway(self.binary, self.broker, case, device, **options)
        self.gateways.append(proc)
        return proc, case / "outbox.db"

    def stop(self, proc, **options):
        duration = proc.stop(**options)
        if proc in self.gateways:
            self.gateways.remove(proc)
        return duration

    @staticmethod
    def accepted(proc):
        return [e["event"] for e in proc.entries() if e.get("kind") == "accepted"]

    def submitted(self, proc, count):
        wait_for(lambda:sum(e.get("kind") in ("accepted", "rejected") for e in proc.entries()) == count,
            label="bounded producer completion")
        return self.accepted(proc)

    def drain(self, proc, db, expected=None, accepted=None, timeout=15, terminal=()):
        if expected is not None:
            wait_for(lambda:len(self.accepted(proc)) == expected, label="durable acceptance results")
        accepted = accepted if accepted is not None else self.accepted(proc)
        wait_for(lambda:all(r[4] != "pending" for r in rows(db)), timeout=timeout, label="eventual pending drain")
        self.reconcile(accepted, db, terminal)
        return accepted

    def reconcile(self, accepted, db, terminal=()):
        expected = {identity(e):e for e in accepted}
        durable_rows = self.pg.records()
        durable = {r[:3]:r for r in durable_rows}
        assert len(durable_rows) == len(durable), "more than one row per identity"
        terminal = set(terminal)
        local = rows(db)
        pending = {(accepted[0]["device_id"],r[0],r[1]) for r in local if r[4] == "pending"} if accepted else set()
        actual_terminal = {(accepted[0]["device_id"],r[0],r[1]) for r in local if r[4] != "pending"} if accepted else set()
        assert not pending and actual_terminal == terminal, "pending/terminal reconciliation"
        assert set(expected) - terminal <= set(durable), "accepted valid identity absent from durable backend"
        for key, event in expected.items():
            if key not in terminal:
                assert durable[key][3:] == (event["fingerprint"],json.loads(canonical(event))), "immutable data mismatch"

    def passed(self, number, **evidence):
        self.results.append({"scenario":number, "name":SCENARIOS[number-1], "status":"passed", **evidence})

    def close(self):
        errors = []
        for proc in self.gateways + self.workers:
            try:
                proc.stop()
            except Exception as exc:
                errors.append(type(exc).__name__)
        try:
            self.broker.stop()
        finally:
            self.pg.stop()
        if errors:
            raise AssertionError("owned process cleanup failed: " + ",".join(errors))


def execute(c):
    c.start()
    a, da = c.spawn("normal-a", count=6, window=2)
    b, db = c.spawn("normal-b", "sensor-b", count=6, window=2)
    accepted_a = c.drain(a,da,6);c.drain(b,db,6)
    c.stop(a);c.stop(b)
    subprocess.run([str(c.binary),"new-stream","--db",str(da),"--device","sensor-a"],check=True,capture_output=True,timeout=3)
    a, da = c.spawn("normal-a", count=4, window=2)
    new = c.drain(a,da,4);c.stop(a)
    assert new[0]["stream_id"] != accepted_a[0]["stream_id"] and new[0]["sequence"] == 1
    c.passed(1,valid_events=16,devices=2,streams=3)

    proc, db = c.spawn("broker-outage",count=30,interval_ms=20,window=4,ack_timeout_ms=150)
    wait_for(lambda:len(c.accepted(proc)) >= 5,label="sending before broker interruption")
    c.broker.stop()
    accepted = c.submitted(proc,30)
    assert any(r[4] == "pending" for r in rows(db)), "outage did not leave a backlog"
    c.broker.start()
    c.drain(proc,db,accepted=accepted);c.stop(proc)
    c.passed(2,valid_events=len(accepted))

    c.stop_workers()
    peer = Peer(c.broker,"worker-1",["telemetry/v1/sensor-a/events"])
    try:
        proc, db = c.spawn("crash",count=4,window=4,ack_timeout_ms=150)
        accepted = c.submitted(proc,4)
        delivered = collect(peer,4)
        before = rows(db)
        assert len(before) == 4 and {identity(e) for e in accepted} == set(delivered)
        c.stop(proc,kill=True)
        assert rows(db) == before
        proc, db = c.spawn("crash",window=4,ack_timeout_ms=150)
        c.start_workers()
        c.drain(proc,db,accepted=accepted);c.stop(proc)
    finally:
        peer.close()
    c.passed(3,valid_events=4,wire_identity_preserved=True)

    c.stop_workers()
    crash_dir = c.directory / "commit-crash-worker"
    crash = worker(c.broker,c.pg,crash_dir,after="crash")
    try:
        proc, db = c.spawn("commit-crash",count=1,window=1,ack_timeout_ms=150)
        accepted = c.submitted(proc,1)
        wait_for(lambda:(crash_dir/"hooks/after-commit.json").exists(),label="post-commit crash marker")
        wait_for(lambda:crash.process.poll() == 71,label="worker exited after commit")
        assert identity(accepted[0]) in {r[:3] for r in c.pg.records()} and len(rows(db)) == 1
        crash.stop(expected_codes=(71,))
        c.start_workers()
        c.drain(proc,db,accepted=accepted)
        assert any(e.get("kind")=="acknowledged" and e.get("result")=="duplicate" for e in proc.entries())
        c.stop(proc)
    finally:
        if crash.process.poll() is None: crash.stop()
    c.stop_workers()
    pause_dir = c.directory / "before-commit-worker"
    paused = worker(c.broker,c.pg,pause_dir,pause=True)
    ack_peer = Peer(c.broker,"sensor-a",["telemetry/v1/sensor-a/acks"])
    try:
        proc, db = c.spawn("before-commit",count=1,window=1,ack_timeout_ms=150)
        accepted = c.submitted(proc,1)
        wait_for(lambda:(pause_dir/"hooks/before-commit.json").exists(),label="uncommitted transaction marker")
        assert identity(accepted[0]) not in {r[:3] for r in c.pg.records()}
        assert ack_peer.messages.empty() and len(rows(db)) == 1, "premature success ACK"
        (pause_dir/"hooks/release").write_text("release\n")
        c.drain(proc,db,accepted=accepted);c.stop(proc)
    finally:
        ack_peer.close();paused.stop()
    c.start_workers()
    c.passed(4,post_commit_duplicate=True,negative_commit_guard=True)

    original = dict(schema_version=1,device_id="sensor-a",stream_id=secrets.token_hex(16),sequence=1,
        timestamp_ms=1791100000000,temperature_mc=20000,pressure_pa=101325)
    original["fingerprint"] = fingerprint(original)
    conflict = {**original,"temperature_mc":21000};conflict["fingerprint"] = fingerprint(conflict)
    p1=Peer(c.broker,"sensor-a",["telemetry/v1/sensor-a/acks"]);p2=Peer(c.broker,"sensor-a")
    try:
        p1.publish("telemetry/v1/sensor-a/events",original)
        wait_for(lambda:identity(original) in {r[:3] for r in c.pg.records()},label="committed immutable original")
        errors=[]
        def publish(peer,event):
            try:
                for _ in range(10): peer.publish("telemetry/v1/sensor-a/events",dict(reversed(list(event.items()))))
            except Exception as exc: errors.append(type(exc).__name__)
        threads=[threading.Thread(target=publish,args=(p1,original)),threading.Thread(target=publish,args=(p2,conflict))]
        for t in threads:t.start()
        for t in threads:t.join(timeout=8)
        assert not errors and not any(t.is_alive() for t in threads)
        wait_for(lambda:all(any(e.get("stream_id")==original["stream_id"] for e in w.entries()) for w in c.workers),label="both shared-subscription worker processes handled events")
        results=set()
        def ack_results():
            while not p1.messages.empty():results.add(json.loads(p1.messages.get_nowait()[1])["result"])
            return {"duplicate","conflict"} <= results
        wait_for(ack_results,label="duplicate and conflict ACKs")
        originals=[r for r in c.pg.records() if r[:3] == identity(original)]
        assert len(originals)==1 and originals[0][3:]==(original["fingerprint"],json.loads(canonical(original)))
    finally:p1.close();p2.close()
    c.passed(5,two_independent_mqtt_workers=True,original_immutable=True)

    c.pg.stop()
    proc, db = c.spawn("database-outage",count=12,window=3,ack_timeout_ms=150)
    accepted=c.submitted(proc,12)
    wait_for(lambda:any(e.get("kind")=="transient" for w in c.workers for e in w.entries()),label="actual database failure reached worker")
    assert len(rows(db))==12 and not any(e.get("kind")=="acknowledged" for e in proc.entries())
    restored=time.monotonic();c.pg.start()
    c.drain(proc,db,accepted=accepted);recovery=time.monotonic()-restored;c.stop(proc)
    c.passed(6,valid_events=12,recovery_seconds=recovery,no_premature_success_ack=True)

    c.stop_workers();c.start_workers(delay=150)
    proc, db = c.spawn("slow-overload",count=80,window=4,max_items=8,max_quarantine=1,ack_timeout_ms=500)
    accepted=c.submitted(proc,80)
    rejected=sum(e.get("kind")=="rejected" for e in proc.entries())
    assert rejected>0 and 0<len(accepted)<80 and len(rows(db))<=8
    c.drain(proc,db,accepted=accepted);c.stop(proc)
    c.stop_workers();c.start_workers()
    c.passed(7,accepted=len(accepted),rejected=rejected,item_limit=8)

    c.stop_workers()
    pause_dir=c.directory/"guard-worker"
    paused=worker(c.broker,c.pg,pause_dir,pause=True)
    publisher=Peer(c.broker,"worker-1")
    try:
        proc,db=c.spawn("ack-guards",count=1,window=1,ack_timeout_ms=150)
        accepted=c.submitted(proc,1)
        wait_for(lambda:(pause_dir/"hooks/before-commit.json").exists(),label="guard event paused before commit")
        original_rows=rows(db)
        for change in ({"device_id":"sensor-b"},{"fingerprint":"0"*64},{"sequence":999}):
            publisher.publish("telemetry/v1/sensor-a/acks",ack(accepted[0],**change))
        publisher.publish("telemetry/v1/sensor-a/acks","{invalid")
        publisher.publish("telemetry/v1/sensor-a/acks","x"*513)
        wait_for(lambda:any(e.get("ignored_acks",0)>=4 and e.get("callback_dropped",0)>0 for e in proc.entries()),label="identity and size guards actually executed")
        assert rows(db)==original_rows
        (pause_dir/"hooks/release").write_text("release\n")
        c.drain(proc,db,accepted=accepted);c.stop(proc)
    finally:publisher.close();paused.stop()
    c.start_workers()
    p=Peer(c.broker,"sensor-a",["telemetry/v1/sensor-a/acks"])
    try:
        invalid={**original,"stream_id":secrets.token_hex(16),"schema_version":2}
        invalid["fingerprint"]=fingerprint(invalid)
        p.publish("telemetry/v1/sensor-a/events",invalid)
        p.publish("telemetry/v1/sensor-a/events","x"*1025)
        def invalid_ack_seen():
            while not p.messages.empty():
                a=json.loads(p.messages.get_nowait()[1])
                if a.get("stream_id")==invalid["stream_id"] and a.get("result")=="invalid":return True
            return False
        wait_for(invalid_ack_seen,label="correlated invalid schema ACK")
        assert identity(invalid) not in {r[:3] for r in c.pg.records()}
    finally:p.close()
    c.passed(8,negative_ack_guards=5,invalid_schema_not_stored=True)

    subprocess.run([str(c.binary.parent/"outbox_test")],check=True,capture_output=True,timeout=15)
    c.stop_workers()
    proc,db=c.spawn("terminal",count=3,window=3,max_items=3,max_quarantine=1,ack_timeout_ms=150)
    accepted=c.submitted(proc,3)
    wait_for(lambda:any(e.get("connected") for e in proc.entries()),label="terminal ACK subscription ready")
    publisher=Peer(c.broker,"worker-1")
    try:
        publisher.publish("telemetry/v1/sensor-a/acks",ack(accepted[0],"invalid"))
        publisher.publish("telemetry/v1/sensor-a/acks",ack(accepted[1],"conflict"))
        wait_for(lambda:sorted(r[4] for r in rows(db))==["blocked","pending","quarantine"],label="bounded terminal-state transfer")
        c.start_workers()
        c.drain(proc,db,accepted=accepted,terminal=[identity(e) for e in accepted[:2]]);c.stop(proc)
    finally:publisher.close()
    c.passed(9,sqlite_full_and_busy_verified=True,quarantine=1,blocked_terminal=1,unrelated_valid_stored=1,
        terminal_ack_scope="authorized test injection")

    proc,db=c.spawn("shutdown",count=20,window=4,interval_ms=50,ack_timeout_ms=200)
    wait_for(lambda:len(c.accepted(proc))>=3 and any(e.get("kind")=="acknowledged" for e in proc.entries()),label="live callbacks before shutdown")
    elapsed=c.stop(proc)
    accepted=c.accepted(proc)
    assert 3<=len(accepted)<=20
    proc,db=c.spawn("shutdown",window=4,ack_timeout_ms=200)
    c.drain(proc,db,accepted=accepted);c.stop(proc)
    c.passed(10,accepted_before_shutdown=len(accepted),shutdown_seconds=elapsed,restart_drain=True)

    a=Peer(c.broker,"sensor-a",["telemetry/v1/sensor-a/acks","telemetry/v1/sensor-b/acks"],client_id="integration-acl-a",allow_denied=True)
    publisher=Peer(c.broker,"worker-1")
    try:
        a.publish("telemetry/v1/sensor-b/events",{"marker":"denied"})
        wait_for(lambda:"Denied PUBLISH from integration-acl-a" in (c.broker.directory/"broker.log").read_text(),label="unauthorized publication denied")
        publisher.publish("telemetry/v1/sensor-b/acks",{"marker":"hidden"})
        publisher.publish("telemetry/v1/sensor-a/acks",{"marker":"visible"})
        topics=[]
        def witness():
            while not a.messages.empty():topics.append(a.messages.get_nowait()[0])
            return "telemetry/v1/sensor-a/acks" in topics
        wait_for(witness,label="authorized delivery after denied topic")
        assert "telemetry/v1/sensor-b/acks" not in topics
    finally:a.close();publisher.close()
    c.passed(11,publish_denied=True,cross_device_ack_hidden=True)

    os.environ["DATABASE_URL"]=c.pg.worker_url;os.environ["DATABASE_ADMIN_URL"]=c.pg.admin_url
    query=postgres_checks.checks()
    return {"scenarios":c.results,"query_verification":query,"unique_backend_rows":len(c.pg.records())}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--gateway",required=True,type=Path)
    parser.add_argument("--output",type=Path,default=Path("results/integration.json"))
    parser.add_argument("--budget-seconds",type=int,default=180)
    args=parser.parse_args()
    if not 30<=args.budget_seconds<=360:parser.error("budget range 30..360")
    args.output.parent.mkdir(parents=True,exist_ok=True)
    def timeout(*_):raise TimeoutError("integration scenario budget exceeded")
    signal.signal(signal.SIGALRM,timeout);signal.alarm(args.budget_seconds)
    result={"scope":"real MQTT/PostgreSQL process faults and durable reconciliation"}
    code=1
    with tempfile.TemporaryDirectory(prefix="telemetry-integration-") as directory:
        context=None
        try:
            context=Context(args.gateway.resolve(),directory)
            result.update(execute(context),status="passed",postgresql="verified")
            code=0
        except RuntimeBlocked as exc:
            result.update(status="blocked",postgresql="not_run",reason=str(exc),
                scenarios=[{"scenario":i+1,"name":name,"status":"blocked"} for i,name in enumerate(SCENARIOS)])
            code=77
        except Exception as exc:
            result.update(status="failed",error_type=type(exc).__name__,scenarios=context.results if context else [])
            for logfile in Path(directory).rglob("*.log"):
                target=args.output.parent/"integration-failure-logs"/logfile.relative_to(directory)
                target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(logfile,target)
        finally:
            signal.alarm(0)
            if context:
                try:context.close()
                except Exception as exc:result.update(cleanup_error=type(exc).__name__);code=1
            args.output.write_text(json.dumps(result,indent=2)+"\n")
            print(json.dumps(result,separators=(",",":")))
    return code


if __name__=="__main__":raise SystemExit(main())
