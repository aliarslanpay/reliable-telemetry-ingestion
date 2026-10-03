#!/usr/bin/env python3
"""Real MQTT ACK-identity checks. Does not verify database commit semantics."""
import argparse
import json
from pathlib import Path
import tempfile
from support import Broker, Peer, gateway, rows, wait_for


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", required=True, type=Path)
    args = parser.parse_args()
    binary = args.gateway.resolve()
    with tempfile.TemporaryDirectory(prefix="telemetry-mqtt-") as temp:
        directory = Path(temp)
        broker = Broker(directory / "broker")
        broker.start()
        peer = proc = None
        try:
            peer = Peer(broker, "worker-1", ["telemetry/v1/+/events"])
            proc = gateway(binary, broker, directory / "device", count=3, window=3, ack_timeout_ms=200)
            db = directory / "device/outbox.db"
            pending = wait_for(lambda:rows(db) if len(rows(db)) == 3 else None, label="durable enqueue")
            events = {}
            def delivered():
                while not peer.messages.empty():
                    _, raw = peer.messages.get_nowait()
                    e = json.loads(raw)
                    events[(e["stream_id"], e["sequence"])] = e
                return len(events) == 3
            wait_for(delivered, label="three real MQTT events")
            assert len(rows(db)) == 3, "PUBACK removed an outbox record"
            first = next(iter(events.values()))
            base = {k:first[k] for k in ("device_id", "stream_id", "sequence", "fingerprint")}
            base.update(schema_version=1, result="stored")
            for change in ({"device_id":"sensor-b"}, {"fingerprint":"0"*64}, {"sequence":999}):
                peer.publish("telemetry/v1/sensor-a/acks", {**base, **change})
            peer.publish("telemetry/v1/sensor-a/acks", "{bad json")
            wait_for(lambda:any(e.get("ignored_acks",0) >= 4 for e in proc.entries()), label="negative ACK guards executed")
            assert rows(db) == pending, "invalid ACK changed durable data"
            for i, event in enumerate(events.values()):
                ack = {k:event[k] for k in ("device_id", "stream_id", "sequence", "fingerprint")}
                ack.update(schema_version=1, result="stored" if i == 0 else "duplicate")
                peer.publish("telemetry/v1/sensor-a/acks", ack)
            wait_for(lambda:len(rows(db)) == 0, label="matching ACK drain")
            shutdown = proc.stop()
            proc = None
            print(json.dumps({"real_broker":"passed", "puback_preserves_outbox":True,
                "negative_ack_guards":4, "durable_ack_drain":"passed", "shutdown_seconds":round(shutdown,4),
                "postgresql":"not_run"}))
        finally:
            if proc: proc.stop()
            if peer: peer.close()
            broker.stop()


if __name__ == "__main__":
    main()
