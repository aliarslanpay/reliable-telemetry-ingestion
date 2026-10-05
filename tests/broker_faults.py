#!/usr/bin/env python3
"""Real MQTT/process faults with injected application ACKs; no PostgreSQL claim."""
import argparse
import json
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading
import time
from support import Broker, Peer, gateway, rows, wait_for


def ack(event, result="stored", **changes):
    return {"schema_version":1, **{k:event[k] for k in ("device_id", "stream_id", "sequence", "fingerprint")}, "result":result, **changes}


def collect(peer, expected, timeout=8):
    found = {}
    def complete():
        while not peer.messages.empty():
            _, raw = peer.messages.get_nowait()
            event = json.loads(raw)
            found[(event["device_id"], event["stream_id"], event["sequence"])] = event
        return len(found) >= expected
    wait_for(complete, timeout=timeout, label="event delivery")
    return found


def checks(binary, directory):
    directory = Path(directory)
    broker = Broker(directory / "broker")
    broker.start()
    peer = proc = None
    results = []
    try:
        peer = Peer(broker, "worker-1", ["telemetry/v1/+/events"])
        case = directory / "crash"
        proc = gateway(binary, broker, case, count=4, window=4, ack_timeout_ms=150)
        events = collect(peer, 4)
        db = case / "outbox.db"
        before = rows(db)
        assert len(before) == 4
        proc.stop(kill=True)
        assert rows(db) == before, "SIGKILL lost committed rows"
        peer.close()
        peer = Peer(broker, "worker-1", ["telemetry/v1/+/events"])
        proc = gateway(binary, broker, case, window=4, ack_timeout_ms=150)
        restarted = collect(peer, 4)
        assert events == restarted, "restart regenerated IDs/data"
        for e in restarted.values():
            peer.publish("telemetry/v1/sensor-a/acks", ack(e, "duplicate"))
        wait_for(lambda:not rows(db), label="crash recovery drain")
        shutdown = proc.stop(); proc = None
        results.append({"scenario":"SIGKILL after enqueue/awaiting ACK", "status":"passed", "reconciled_events":4, "shutdown_seconds":shutdown})

        peer.close(); peer = None
        broker.stop()
        case = directory / "broker-outage"
        proc = gateway(binary, broker, case, count=5, window=2, ack_timeout_ms=150)
        db = case / "outbox.db"
        before = wait_for(lambda:rows(db) if len(rows(db)) == 5 else None, label="offline durable acceptance")
        broker.start()
        peer = Peer(broker, "worker-1", ["telemetry/v1/+/events"])
        recovered = {}
        def drain():
            while not peer.messages.empty():
                _, raw = peer.messages.get_nowait(); e = json.loads(raw)
                recovered[(e["stream_id"],e["sequence"])] = e
                peer.publish("telemetry/v1/sensor-a/acks", ack(e))
            return not rows(db)
        wait_for(drain, timeout=12, label="broker restart drain")
        assert {(r[0],r[1]) for r in before} == set(recovered), "offline identity reconciliation"
        proc.stop(); proc = None
        results.append({"scenario":"broker unavailable/restart", "status":"passed", "reconciled_events":5})

        case = directory / "active-disconnect"
        proc = gateway(binary, broker, case, count=4, window=2, ack_timeout_ms=150)
        collect(peer, 2)
        db = case / "outbox.db"
        before = wait_for(lambda:rows(db) if len(rows(db)) == 4 else None, label="active connection backlog")
        peer.close(); peer = None
        broker.stop(); broker.start()
        peer = Peer(broker, "worker-1", ["telemetry/v1/+/events"])
        recovered = {}
        def active_drain():
            while not peer.messages.empty():
                _, raw = peer.messages.get_nowait(); e = json.loads(raw)
                recovered[(e["stream_id"],e["sequence"])] = e
                peer.publish("telemetry/v1/sensor-a/acks", ack(e))
            return not rows(db)
        wait_for(active_drain, timeout=12, label="active reconnect drain")
        assert {(r[0],r[1]) for r in before} == set(recovered)
        proc.stop(); proc = None
        results.append({"scenario":"broker disconnect during sending", "status":"passed", "reconciled_events":4})

        case = directory / "saturation"
        proc = gateway(binary, broker, case, count=30, window=3, max_items=3, max_quarantine=1, ack_timeout_ms=150)
        db = case / "outbox.db"
        wait_for(lambda:sum(e.get("kind")=="rejected" for e in proc.entries()) == 27, label="visible capacity rejection")
        events = collect(peer, 3)
        assert len(rows(db)) == 3
        accepted = [e["event"] for e in proc.entries() if e.get("kind")=="accepted"]
        assert len(accepted) == 3
        wait_for(lambda:any(e.get("connected") for e in proc.entries()), label="ACK subscription before terminal injection")
        peer.publish("telemetry/v1/sensor-a/acks", ack(accepted[0], "invalid"))
        peer.publish("telemetry/v1/sensor-a/acks", ack(accepted[1], "conflict"))
        wait_for(lambda:sorted(r[4] for r in rows(db)) == ["blocked","pending","quarantine"], label="quarantine and overflow state")
        peer.publish("telemetry/v1/sensor-a/acks", "x" * 513)
        wait_for(lambda:any(e.get("callback_dropped",0)>0 for e in proc.entries()), label="oversized ACK callback guard")
        assert len(rows(db)) == 3
        peer.publish("telemetry/v1/sensor-a/acks", ack(accepted[2]))
        wait_for(lambda:len(rows(db)) == 2, label="unrelated valid event drain")
        proc.stop(); proc = None
        results.append({"scenario":"capacity/terminal limits/oversized ACK", "status":"passed", "accepted":3, "rejected":27, "terminal":2, "acknowledged":1})

        case = directory / "replay"
        case.mkdir()
        replay = case / "measurements.ndjson"
        valid = {"timestamp_ms":1791100000000,"temperature_mc":20000,"pressure_pa":101325}
        replay.write_text(json.dumps(valid)+"\n"+json.dumps({**valid,"temperature_mc":1.0})+"\n"+"x"*2048+"\n"+json.dumps(valid)+"\n")
        proc = gateway(binary, broker, case, replay=replay, window=2, ack_timeout_ms=150)
        db = case / "outbox.db"
        wait_for(lambda:sum(e.get("kind")=="rejected" for e in proc.entries())==2, label="replay negative guards")
        accepted = [e["event"] for e in proc.entries() if e.get("kind")=="accepted"]
        assert len(accepted)==2 and len(rows(db))==2
        wait_for(lambda:any(e.get("connected") for e in proc.entries()), label="ACK subscription before replay injection")
        for e in accepted:
            peer.publish("telemetry/v1/sensor-a/acks", ack(e))
        wait_for(lambda:not rows(db), label="valid replay recovery")
        proc.stop(); proc = None
        results.append({"scenario":"bounded replay mixed valid/invalid input", "status":"passed", "accepted":2, "rejected":2})

        case = directory / "shutdown-race"
        proc = gateway(binary, broker, case, count=1, window=1, ack_timeout_ms=150)
        db = case / "outbox.db"
        e = wait_for(lambda:rows(db) if len(rows(db))==1 else None, label="race acceptance")
        original = e[0]
        stop = threading.Event()
        errors = []
        def flood():
            try:
                event = json.loads(original[2])
                while not stop.is_set():
                    peer.publish("telemetry/v1/sensor-a/acks", ack(event, sequence=999))
            except Exception as exc:
                errors.append(type(exc).__name__)
        thread = threading.Thread(target=flood)
        thread.start()
        try:
            wait_for(lambda:any(e.get("ignored_acks",0)>0 for e in proc.entries()), label="callback race active")
            shutdown = proc.stop(); proc = None
        finally:
            stop.set();thread.join(timeout=3)
        assert not thread.is_alive() and not errors and rows(db)==[original]
        results.append({"scenario":"callback/shutdown race", "status":"passed", "pending_preserved":1, "shutdown_seconds":shutdown})

        peer.close(); peer = None
        a = Peer(broker,"sensor-a",["telemetry/v1/sensor-a/acks","telemetry/v1/sensor-b/acks"],client_id="acl-a",allow_denied=True)
        publisher = Peer(broker,"worker-1")
        try:
            a.publish("telemetry/v1/sensor-b/events", {"marker":"unauthorized"})
            wait_for(lambda:"Denied PUBLISH from acl-a" in (directory / "broker/broker.log").read_text(), label="ACL publish denial")
            publisher.publish("telemetry/v1/sensor-b/acks", {"marker":"hidden"})
            publisher.publish("telemetry/v1/sensor-a/acks", {"marker":"visible"})
            observed = []
            def visible():
                while not a.messages.empty(): observed.append(a.messages.get_nowait()[0])
                return "telemetry/v1/sensor-a/acks" in observed
            wait_for(visible,label="authorized ACL delivery witness")
            assert "telemetry/v1/sensor-b/acks" not in observed, "cross-device subscription delivered"
        finally:
            a.close();publisher.close()
        results.append({"scenario":"local two-device publication/subscription ACL", "status":"passed"})
        return results
    finally:
        if proc: proc.stop()
        if peer: peer.close()
        broker.stop()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--gateway",required=True,type=Path)
    parser.add_argument("--output",type=Path,default=Path("results/broker-faults.json"))
    args=parser.parse_args()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="telemetry-broker-faults-") as directory:
        result={"scope":"real MQTT + SQLite + process faults; injected application ACKs", "postgresql":"not_run"}
        try:
            result["scenarios"]=checks(args.gateway.resolve(),directory)
            result["status"]="passed"
        except Exception as exc:
            result.update(status="failed",error=type(exc).__name__+": "+str(exc))
            # Keep only logs; password/configuration files are never copied.
            import shutil
            for log in Path(directory).rglob("*.log"):
                target=args.output.parent/"failure-logs"/log.relative_to(directory)
                target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(log,target)
            raise
        finally:
            args.output.write_text(json.dumps(result,indent=2)+"\n")
            print(json.dumps(result,separators=(",",":")))


if __name__=="__main__":main()
