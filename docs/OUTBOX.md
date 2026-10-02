# Persistent outbox

Acceptance is the successful SQLite commit that inserts the wire event and advances
the stream sequence in one `BEGIN IMMEDIATE` transaction. Rejected input consumes
no sequence. Opening an existing database restores device, stream and limits;
changing a device or its persistent limits accidentally is an error. `new-stream`
explicitly starts another stream without changing existing records.

SQLite uses WAL, `synchronous=FULL`, a 150 ms busy timeout, automatic checkpoint
every 32 pages and a 256 KiB post-checkpoint journal size target. Process-crash
recovery is tested separately from power loss; filesystem/hardware flush guarantees
remain necessary for power-loss durability. Busy, full, read-only and I/O errors
reject input explicitly. A commit error leaves the event unaccepted.

Default capacity is 4096 records / 1 MiB of wire payload, including terminal rows.
Quarantine holds up to 64 records. Further terminal errors enter inspectable
`blocked` state, remain charged to capacity, and stop retrying until the operator
discards them explicitly. Accepted events are never evicted automatically.

The database has a configured maximum page count (4096 pages by default); this
does **not** bound the database + WAL + filesystem footprint exactly. Indexes,
page slack and WAL frames add overhead. Long-lived external readers can prevent
checkpoint progress and grow WAL. Keep operator reads short, monitor file sizes,
and use filesystem quotas for an operational hard disk limit. Checkpoint failure
must never turn an already committed enqueue into a rejected result.

Status and terminal inspection are bounded queries. Payload serialization and
batch reads are bounded. SQLite is owned by the calling event-loop thread;
external enqueue processes arbitrate through SQLite transactions.
