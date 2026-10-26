BEGIN;
-- Serialize first-open DDL across independent processes, including an empty DB.
SELECT pg_advisory_xact_lock(hashtextextended('acyclic-stream-schema', 1937006962));
CREATE TABLE IF NOT EXISTS acyclic_stream_journal (
    revision bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace text NOT NULL,
    at_millis bigint NOT NULL CHECK (at_millis >= 0),
    -- Public command ceiling (1056768), plus oneof tag and length delimiter.
    command bytea NOT NULL CHECK (octet_length(command) <= 1056772)
);
CREATE INDEX IF NOT EXISTS acyclic_stream_journal_namespace_revision
    ON acyclic_stream_journal(namespace, revision);
COMMIT;
