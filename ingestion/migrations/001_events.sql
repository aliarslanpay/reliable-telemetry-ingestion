CREATE TABLE IF NOT EXISTS events (
    device_id text NOT NULL CHECK (device_id ~ '^[a-z0-9][a-z0-9_-]{0,31}$'),
    stream_id text NOT NULL CHECK (stream_id ~ '^[0-9a-f]{32}$'),
    sequence bigint NOT NULL CHECK (sequence BETWEEN 1 AND 9007199254740991),
    fingerprint text NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    data jsonb NOT NULL,
    stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (device_id, stream_id, sequence)
);
