"""IoT Rule envelope -> immutable DynamoDB write -> device ACK."""

import base64
import json
import os
import boto3
from botocore.config import Config
from .core import (
    DEVICE,
    MAX_EVENT_BYTES,
    Invalid,
    acknowledgement,
    invalid_ack,
    validate,
)
from .dynamo import DynamoStore

_clients = None


def envelope(value):
    if not isinstance(value, dict) or set(value) != {"event_b64", "authorized_device"}:
        raise Invalid("rule envelope fields")
    device = value["authorized_device"]
    encoded = value["event_b64"]
    if (
        not isinstance(device, str)
        or not DEVICE.fullmatch(device)
        or not isinstance(encoded, str)
        or len(encoded) > 1368
    ):
        raise Invalid("rule envelope bound")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, UnicodeError) as exc:
        raise Invalid("rule base64") from exc
    if not 0 < len(raw) <= MAX_EVENT_BYTES:
        raise Invalid("rule event size")
    return raw, device


def clients():
    global _clients
    if _clients is None:
        config = Config(
            connect_timeout=1,
            read_timeout=2,
            retries={"mode": "standard", "total_max_attempts": 2},
        )
        table = os.environ["EVENT_TABLE"]
        store = DynamoStore(boto3.client("dynamodb", config=config), table)
        publisher = boto3.client(
            "iot-data",
            endpoint_url="https://" + os.environ["IOT_DATA_ENDPOINT"],
            config=config,
        )
        _clients = (store, publisher)
    return _clients


def handler(value, _context):
    raw, device = envelope(value)
    if device not in os.environ["ALLOWED_DEVICES"].split(","):
        raise Invalid("unreviewed device")
    store, publisher = clients()
    try:
        event = validate(raw, device)
    except Invalid:
        ack = invalid_ack(raw, device)
        result = "invalid"
    else:
        result = store.store(event)
        ack = acknowledgement(event, result)
    if ack is not None:
        # A publication error propagates: asynchronous retry will safely deduplicate.
        publisher.publish(
            topic="telemetry/v1/" + device + "/acks",
            qos=1,
            retain=False,
            payload=json.dumps(ack, separators=(",", ":")).encode(),
        )
    print(
        json.dumps(
            {"kind": "ingestion", "result": result, "device_id": device},
            separators=(",", ":"),
        )
    )
    return {"result": result}
