# Delivery loop

One main thread owns SQLite, input parsing, the application in-flight map and
ACK decisions. libmosquitto owns its network thread. Callbacks copy ACKs (at most
512 bytes) into a mutex-protected 64-item handoff; overflow drops an ACK and
leaves the durable event available for retry. Database access never happens in
callbacks or while holding the handoff mutex.

The application window is configurable from 1 to 64. An independent reservation
counter bounds libmosquitto's outgoing QoS 1 messages to 16 even across reconnects;
an ACK timeout cannot grow an unlimited library offline buffer. PUBACK releases
only this transport reservation. It never removes an outbox record. No event
is sent before successful subscription to its ACK topic.

The application ACK timeout defaults to 1 second. Backoff is 100 ms doubling to
2 seconds, with bounded 80–120% jitter capped at 2 seconds. Scheduling and ACK
latency use `steady_clock`. Retry state is intentionally volatile: restart retries
the same stored event, not a new identity. The broker reconnect delay is separately
bounded to 1–4 seconds by libmosquitto.

SIGINT/SIGTERM only set a `sig_atomic_t` flag. The main loop stops input,
checkpoints, and stops/joins the library thread before destroying callback userdata.
The loop polls at 10 ms; SQLite lock waits are at most 150 ms. Local shutdown
timing is checked by the broker runner. DNS/TLS/system I/O are dependency/OS waits,
so there is no hard real-time shutdown guarantee for arbitrary hosts; a service
supervisor may use a 5-second stop timeout and SIGKILL, preserving committed data.

The local broker enforces packet/message limits. libmosquitto may allocate an
incoming packet before invoking the callback; the gateway's callback limit bounds
project-owned storage, not allocations from an untrusted broker. The local
loopback broker and AWS IoT are the intended trusted endpoints.

`run --count N` creates bounded deterministic synthetic measurements. `--replay`
reads a measurement-only NDJSON file (at most 4 MiB / 10000 inputs), allocating new
identities on acceptance. Retrying the outbox preserves existing identities.
Multiple enqueue processes are supported through SQLite; a file lock permits
only one delivery loop for each outbox.
