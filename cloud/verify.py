#!/usr/bin/env python3
"""Bounded real AWS checks; never runs without the explicit execution gate."""
import argparse
import datetime
import json
import os
from pathlib import Path
import queue
import sqlite3
import ssl
import subprocess
import time
import boto3
from botocore.config import Config
import paho.mqtt.client as mqtt
from telemetry_ingestion.core import fingerprint, canonical
from telemetry_ingestion.dynamo import keys
from resources import load_manifest, validate_stack


def wait_for(predicate, seconds=15):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError("cloud check deadline")


class Device:
    def __init__(self, name, endpoint, directory):
        self.name = name
        self.messages = queue.Queue(maxsize=16)
        self.ready = False
        self.denied = False
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=name, protocol=mqtt.MQTTv311
        )
        self.client.max_inflight_messages_set(2)
        self.client.max_queued_messages_set(8)
        self.client.tls_set(
            ca_certs=str(directory / "root-ca.pem"),
            certfile=str(directory / name / "certificate.pem"),
            keyfile=str(directory / name / "private.key"),
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        self.client.tls_insecure_set(False)

        def connected(client, _, __, reason, ___):
            if reason == 0:
                client.subscribe("telemetry/v1/" + name + "/acks", qos=1)

        def subscribed(_, __, ___, reasons, ____):
            if any(r.is_failure for r in reasons):
                self.denied = True
            else:
                self.ready = True

        def disconnected(_, __, ___, reason, ____):
            if reason != 0:
                self.denied = True
            self.ready = False

        def message(_, __, msg):
            if len(msg.payload) <= 512 and not msg.retain:
                try:
                    self.messages.put_nowait(json.loads(msg.payload))
                except (ValueError, queue.Full):
                    pass

        self.client.on_connect = connected
        self.client.on_subscribe = subscribed
        self.client.on_disconnect = disconnected
        self.client.on_message = message
        self.client.connect_async(endpoint, 8883, 30)
        self.client.loop_start()
        wait_for(lambda: self.ready)

    def event(self, event, topic_device=None, expect_ack=True):
        info = self.client.publish(
            "telemetry/v1/" + (topic_device or self.name) + "/events",
            json.dumps(event, separators=(",", ":")),
            qos=1,
            retain=False,
        )
        if not expect_ack:
            return
        info.wait_for_publish(timeout=5)
        if not info.is_published():
            raise AssertionError("cloud broker PUBACK missing")
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                a = self.messages.get(timeout=0.2)
            except queue.Empty:
                continue
            if all(
                a.get(k) == event[k]
                for k in ("device_id", "stream_id", "sequence", "fingerprint")
            ):
                return a
        raise AssertionError("durable application ACK missing")

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--account", required=True)
    parser.add_argument("--stack", required=True)
    parser.add_argument("--project-name", default="telemetry_demo")
    parser.add_argument("--gateway", required=True, type=Path)
    parser.add_argument("--directory", type=Path, default=Path(".runtime/cloud"))
    parser.add_argument("--output", type=Path, default=Path("results/aws-live.json"))
    args = parser.parse_args()
    if not args.execute:
        parser.error(
            "Use --execute after reviewing the account and bounded verification scope"
        )
    region = "eu-central-1"
    session = boto3.Session(region_name=region)
    config = Config(
        connect_timeout=2,
        read_timeout=3,
        retries={"mode": "standard", "total_max_attempts": 2},
    )
    if (
        session.client("sts", config=config).get_caller_identity()["Account"]
        != args.account
    ):
        raise SystemExit("Target account mismatch")
    cf = session.client("cloudformation", config=config)
    state = load_manifest(
        args.directory / "resources.json",
        args.account,
        args.stack,
        region,
        args.project_name,
    )
    stack = validate_stack(cf, state)
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
    client = session.client("dynamodb", config=config)
    devices = [outputs["DeviceAName"], outputs["DeviceBName"]]
    endpoint = outputs["Endpoint"]
    table = outputs["TableName"]
    accepted = {}
    for name in devices:
        env = dict(os.environ)
        env.pop("MQTT_USERNAME", None)
        env.pop("MQTT_PASSWORD", None)
        directory = args.directory.resolve()
        env.update(
            MQTT_CA_FILE=str(directory / "root-ca.pem"),
            MQTT_CERT_FILE=str(directory / name / "certificate.pem"),
            MQTT_KEY_FILE=str(directory / name / "private.key"),
        )
        db = directory / (name + ".db")
        logfile = directory / (name + ".jsonl")
        if db.exists():
            raise SystemExit(
                "Use a fresh cloud check directory/outbox, preserving prior results"
            )
        with logfile.open("w") as log:
            subprocess.run(
                [
                    str(args.gateway.resolve()),
                    "run",
                    "--db",
                    str(db),
                    "--device",
                    name,
                    "--host",
                    endpoint,
                    "--port",
                    "8883",
                    "--count",
                    "10",
                    "--window",
                    "2",
                    "--ack-timeout-ms",
                    "2000",
                    "--duration-ms",
                    "20000",
                ],
                env=env,
                stdout=log,
                check=True,
                timeout=25,
            )
        events = [
            e["event"]
            for line in logfile.read_text().splitlines()
            if (e := json.loads(line)).get("kind") == "accepted"
        ]
        assert len(events) == 10
        with sqlite3.connect(db) as conn:
            assert conn.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
        for event in events:
            stored = client.get_item(
                TableName=table,
                Key=keys(name, event["stream_id"], event["sequence"]),
                ConsistentRead=True,
            )["Item"]
            assert stored["fingerprint"]["S"] == event["fingerprint"] and stored[
                "data"
            ]["S"] == canonical(event)
        accepted[name] = events
    for name in devices:
        peer = Device(name, endpoint, args.directory.resolve())
        try:
            event = accepted[name][0]
            assert (
                peer.event(dict(reversed(list(event.items()))))["result"] == "duplicate"
            )
            conflict = {**event, "temperature_mc": event["temperature_mc"] + 1}
            conflict["fingerprint"] = fingerprint(conflict)
            assert peer.event(conflict)["result"] == "conflict"
            stored = client.get_item(
                TableName=table,
                Key=keys(name, event["stream_id"], event["sequence"]),
                ConsistentRead=True,
            )["Item"]
            assert stored["data"]["S"] == canonical(event)
        finally:
            peer.close()
    peer = Device(devices[0], endpoint, args.directory.resolve())
    try:
        bad = {**accepted[devices[1]][0], "sequence": 999}
        bad["fingerprint"] = fingerprint(bad)
        peer.event(bad, devices[1], expect_ack=False)
        wait_for(lambda: peer.denied)
        assert "Item" not in client.get_item(
            TableName=table,
            Key=keys(bad["device_id"], bad["stream_id"], bad["sequence"]),
            ConsistentRead=True,
        )
    finally:
        peer.close()
    peer = Device(devices[0], endpoint, args.directory.resolve())
    try:
        peer.client.subscribe("telemetry/v1/" + devices[1] + "/acks", qos=1)
        wait_for(lambda: peer.denied)
    finally:
        peer.close()
    # One bounded asynchronous failure exercises Lambda Errors and its destination.
    session.client("lambda", config=config).invoke(
        FunctionName=outputs["FunctionName"], InvocationType="Event", Payload=b"{}"
    )
    sqs = session.client("sqs", config=config)
    cw = session.client("cloudwatch", config=config)
    wait_for(
        lambda: bool(
            sqs.receive_message(
                QueueUrl=outputs["FailureQueueUrl"],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=1,
            ).get("Messages")
        ),
        seconds=150,
    )
    wait_for(
        lambda: cw.describe_alarms(AlarmNames=[outputs["AlarmName"]])["MetricAlarms"][
            0
        ]["StateValue"]
        == "ALARM",
        seconds=150,
    )
    logs = session.client("logs", config=config)
    lambda_logs = logs.filter_log_events(
        logGroupName=outputs["LambdaLogGroup"], limit=20
    )
    rule_logs = logs.filter_log_events(
        logGroupName=outputs["RuleErrorLogGroup"], limit=20
    )
    result = {
        "status": "passed",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "region": region,
        "sample_count": sum(len(events) for events in accepted.values()),
        "device_count": len(devices),
        "end_to_end": "passed",
        "application_acknowledgements": "passed",
        "mutual_tls": "passed",
        "device_isolation": "passed",
        "conditional_writes": "passed",
        "lambda_error_alarm": "passed",
        "failure_destination": "passed",
        "lambda_log_events": len(lambda_logs.get("events", [])),
        "rule_error_log_events": len(rule_logs.get("events", [])),
        "rule_dispatch_error_injection": "not_run",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
