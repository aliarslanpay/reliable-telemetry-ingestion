"""MQTT callback handoff; database work and ACK publication occur outside callbacks."""
import argparse
import json
import os
import queue
import signal
import threading
import time
import paho.mqtt.client as mqtt
from .core import MAX_EVENT_BYTES, Invalid, acknowledgement, invalid_ack, topic_device, validate
from .postgres import PgStore


def log(kind, **fields):
    print(json.dumps({"kind": kind, **fields}, separators=(",", ":")), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.getenv("MQTT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    parser.add_argument("--name", default="worker-1")
    args = parser.parse_args()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    handoff = queue.Queue(maxsize=64)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.name, protocol=mqtt.MQTTv311)
    client.username_pw_set(os.environ["MQTT_USERNAME"], os.environ["MQTT_PASSWORD"])
    client.max_inflight_messages_set(16)
    client.max_queued_messages_set(32)
    client.reconnect_delay_set(1, 4)

    def connected(c, _, __, reason, ___):
        if reason == 0:
            c.subscribe("$share/ingestors/telemetry/v1/+/events", qos=1)

    def subscribed(_, __, ___, reasons, ____):
        log("subscription", granted=all(not r.is_failure for r in reasons))

    def message(_, __, msg):
        if msg.retain or msg.qos != 1 or len(msg.payload) > MAX_EVENT_BYTES:
            return
        try:
            handoff.put_nowait((msg.topic, bytes(msg.payload)))
        except queue.Full:
            # No success ACK: the durable producer retries this identity.
            pass

    client.on_connect = connected
    client.on_subscribe = subscribed
    client.on_message = message
    store = PgStore(os.environ["DATABASE_URL"])
    counts = {"stored": 0, "duplicate": 0, "conflict": 0, "invalid": 0, "transient": 0}
    client.connect_async(args.host, args.port, keepalive=10)
    client.loop_start()
    try:
        while not stop.is_set():
            try:
                topic, raw = handoff.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                device = topic_device(topic)
                event = validate(raw, device)
            except Invalid:
                counts["invalid"] += 1
                ack = invalid_ack(raw, topic.split("/")[2] if len(topic.split("/")) == 4 else "")
            else:
                try:
                    result = store.store(event)
                    counts[result] += 1
                    ack = acknowledgement(event, result)
                    log("persistence", result=result, **{k:event[k] for k in ("device_id", "stream_id", "sequence")})
                except Exception as exc:
                    counts["transient"] += 1
                    log("transient", error_type=type(exc).__name__)
                    store.close()
                    stop.wait(0.1)
                    continue
            if ack is not None:
                info = client.publish("telemetry/v1/" + ack["device_id"] + "/acks", json.dumps(ack, separators=(",", ":")), qos=1, retain=False)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    log("ack_not_queued", rc=int(info.rc))
    finally:
        client.disconnect()
        client.loop_stop()
        store.close()
        log("shutdown", **counts)


if __name__ == "__main__":
    main()
