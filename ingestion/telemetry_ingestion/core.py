"""Wire contract shared by local workers and the Lambda adapter."""
import hashlib
import json
import re

MAX_EVENT_BYTES = 1024
MAX_ACK_BYTES = 512
MAX_SEQUENCE = 9007199254740991
DEVICE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}\Z")
STREAM = re.compile(r"[0-9a-f]{32}\Z")
HASH = re.compile(r"[0-9a-f]{64}\Z")
FIELDS = {"schema_version", "device_id", "stream_id", "sequence", "timestamp_ms", "temperature_mc", "pressure_pa"}


class Invalid(ValueError):
    pass


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise Invalid("duplicate key")
        result[key] = value
    return result


def decode(raw, limit=MAX_EVENT_BYTES):
    if not isinstance(raw, (str, bytes)) or not 0 < len(raw.encode() if isinstance(raw, str) else raw) <= limit:
        raise Invalid("message size")
    try:
        return json.loads(raw, object_pairs_hook=_pairs, parse_constant=lambda _: (_ for _ in ()).throw(Invalid("non-finite number")))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise Invalid("invalid JSON") from exc


def integer(event, name, low, high):
    value = event.get(name)
    if type(value) is not int or not low <= value <= high:
        raise Invalid("invalid " + name)
    return value


def identity(event):
    if not isinstance(event, dict):
        raise Invalid("object required")
    if not isinstance(event.get("device_id"), str) or not DEVICE.fullmatch(event["device_id"]):
        raise Invalid("device ID")
    if not isinstance(event.get("stream_id"), str) or not STREAM.fullmatch(event["stream_id"]):
        raise Invalid("stream ID")
    integer(event, "sequence", 1, MAX_SEQUENCE)
    if not isinstance(event.get("fingerprint"), str) or not HASH.fullmatch(event["fingerprint"]):
        raise Invalid("fingerprint")


def canonical(event):
    return json.dumps({key: event[key] for key in FIELDS}, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def fingerprint(event):
    return hashlib.sha256(canonical(event).encode("ascii")).hexdigest()


def validate(raw, authorized_device):
    event = decode(raw)
    identity(event)
    if set(event) != FIELDS | {"fingerprint"}:
        raise Invalid("unexpected fields")
    if event["device_id"] != authorized_device:
        raise Invalid("topic identity mismatch")
    integer(event, "schema_version", 1, 1)
    integer(event, "timestamp_ms", 0, 253402300799999)
    integer(event, "temperature_mc", -100000, 200000)
    integer(event, "pressure_pa", 0, 2000000)
    if fingerprint(event) != event["fingerprint"]:
        raise Invalid("fingerprint mismatch")
    return event


def acknowledgement(event, result):
    if result not in {"stored", "duplicate", "conflict", "invalid"}:
        raise ValueError("not a terminal persistence result")
    identity(event)
    return {"schema_version": 1, **{k: event[k] for k in ("device_id", "stream_id", "sequence", "fingerprint")}, "result": result}


def invalid_ack(raw, authorized_device):
    """Only a bounded, correlatable invalid envelope can receive a terminal ACK."""
    try:
        event = decode(raw)
        identity(event)
        if event["device_id"] == authorized_device:
            return acknowledgement(event, "invalid")
    except Invalid:
        pass
    return None


def topic_device(topic):
    parts = topic.split("/")
    if len(parts) != 4 or parts[:2] != ["telemetry", "v1"] or parts[3] != "events" or not DEVICE.fullmatch(parts[2]):
        raise Invalid("event topic")
    return parts[2]
