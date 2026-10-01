# Event and ACK contract

Version 1 events are UTF-8 JSON objects of at most 1024 bytes, with exactly these fields:

| Field | Meaning and range |
| --- | --- |
| `schema_version` | Integer 1 |
| `device_id` | `[a-z0-9][a-z0-9_-]{0,31}` |
| `stream_id` | 32 lowercase hex characters; random 128-bit stream identity |
| `sequence` | Integer 1..9007199254740991, allocated durably within a stream |
| `timestamp_ms` | Unix milliseconds, 0..253402300799999; acquisition time |
| `temperature_mc` | Integer millidegrees Celsius, -100000..200000 |
| `pressure_pa` | Integer pascals, 0..2000000 |
| `fingerprint` | 64 lowercase hex characters, SHA-256 of canonical data |

Canonical data excludes `fingerprint`. Sort the seven field names lexicographically,
use double-quoted ASCII strings, decimal integers, no whitespace and no trailing
newline. Restricted IDs require no escaping. JSON input field order is irrelevant.
Duplicate keys, extra fields, booleans used as integers, floats and non-finite values
are invalid. Fixtures include negative temperature and upper numeric bounds.

Event topic: `telemetry/v1/{device_id}/events`. ACK topic:
`telemetry/v1/{device_id}/acks`. Both use QoS 1 without retain. An ACK is at most
512 bytes and has exactly `schema_version`, `device_id`, `stream_id`, `sequence`,
`fingerprint`, `result`. The result is `stored`, `duplicate`, `conflict` or `invalid`.
Only matching `stored`/`duplicate` ACKs permit deletion from the pending outbox.
Matching terminal errors are kept in quarantine. An invalid message without a valid
identity/fingerprint cannot be correlated and receives no ACK.

MQTT PUBACK confirms broker receipt, not a database commit. Storage is immutable
per `(device_id, stream_id, sequence)`. Identical data is a duplicate; changed data
under the same identity is a conflict. The backend recomputes the hash and checks
the authorized topic identity. Retries preserve event data; attempt timing/counters
belong to transport state outside that data. Sequence expresses stream order, not
arrival order. At-least-once retries require eventual connectivity, available
storage, a cooperating backend and retention of identities. Deleting backend rows
ends deduplication protection. There is no distributed exactly-once guarantee.
