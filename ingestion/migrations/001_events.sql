CREATE TABLE IF NOT EXISTS events (
    device_id text NOT NULL CHECK (device_id ~ '^[a-z0-9][a-z0-9_-]{0,31}$'),
    stream_id text NOT NULL CHECK (stream_id ~ '^[0-9a-f]{32}$'),
    sequence bigint NOT NULL CHECK (sequence BETWEEN 1 AND 9007199254740991),
    fingerprint text NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    data jsonb NOT NULL,
    stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (device_id, stream_id, sequence)
);

CREATE OR REPLACE FUNCTION reject_event_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'events are immutable; retention ends deduplication protection';
END;
$$;
DROP TRIGGER IF EXISTS events_immutable ON events;
CREATE TRIGGER events_immutable BEFORE UPDATE OR DELETE ON events
FOR EACH ROW EXECUTE FUNCTION reject_event_mutation();

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'telemetry_worker') THEN
        CREATE ROLE telemetry_worker NOLOGIN;
    END IF;
END $$;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO telemetry_worker;
GRANT SELECT, INSERT ON events TO telemetry_worker;
