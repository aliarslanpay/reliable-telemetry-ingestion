"""Immutable insert and duplicate arbitration under READ COMMITTED."""
import psycopg
from psycopg.types.json import Jsonb
from .core import canonical
import json


class PgStore:
    def __init__(self, dsn):
        self.dsn = dsn
        self.connection = None

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def connect(self):
        if self.connection is None or self.connection.closed:
            self.connection = psycopg.connect(self.dsn, connect_timeout=2,
                options="-c statement_timeout=1500 -c lock_timeout=1000 -c idle_in_transaction_session_timeout=2000")
        return self.connection

    def store(self, event, before_commit=None):
        conn = self.connect()
        key = tuple(event[k] for k in ("device_id", "stream_id", "sequence"))
        data = json.loads(canonical(event))
        # Returning from this block happens only after COMMIT succeeds.
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("INSERT INTO events(device_id,stream_id,sequence,fingerprint,data) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING fingerprint",
                            (*key, event["fingerprint"], Jsonb(data)))
                if cur.fetchone() is not None:
                    result = "stored"
                else:
                    # A new READ COMMITTED statement sees the competing committed insert.
                    cur.execute("SELECT fingerprint,data FROM events WHERE device_id=%s AND stream_id=%s AND sequence=%s", key)
                    original = cur.fetchone()
                    if original is None:
                        raise RuntimeError("deduplication identity disappeared")
                    result = "duplicate" if original == (event["fingerprint"], data) else "conflict"
            if before_commit is not None:
                before_commit(event)
        return result
