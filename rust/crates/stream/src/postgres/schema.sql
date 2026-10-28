BEGIN ISOLATION LEVEL READ COMMITTED;
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

-- Physical append-if-revision only. The canonical Stream core decides paths,
-- conditions and retry identity; SQL never decodes a command. VOLATILE keeps
-- the head query's READ COMMITTED snapshot after the namespace lock wait.
CREATE OR REPLACE FUNCTION acyclic_stream_publish(
    stream_namespace text,
    expected_revision bigint,
    command_bytes bytea,
    deadline_millis bigint
) RETURNS TABLE (revision bigint, at_millis bigint, accepted boolean)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    current_revision bigint;
    accepted_millis bigint;
    published_revision bigint;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION USING ERRCODE = 'PST02',
            MESSAGE = 'STEP stream-publication failed: READ COMMITTED required',
            HINT = 'Recovery: reopen PostgresStream::connect with the same database and namespace';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(stream_namespace, 1937006962));
    SELECT COALESCE(max(j.revision), 0) INTO current_revision
        FROM acyclic_stream_journal AS j WHERE j.namespace = stream_namespace;
    IF current_revision <> expected_revision THEN
        RETURN QUERY SELECT current_revision, 0::bigint, false;
        RETURN;
    END IF;
    accepted_millis := floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint;
    IF deadline_millis IS NOT NULL AND accepted_millis >= deadline_millis THEN
        RAISE EXCEPTION USING ERRCODE = 'PST01',
            MESSAGE = 'STEP stream-publication refused: acceptance deadline elapsed',
            HINT = 'Recovery: reconcile the original same-key Stream request before submitting new work';
    END IF;
    INSERT INTO acyclic_stream_journal(namespace, at_millis, command)
        VALUES(stream_namespace, accepted_millis, command_bytes)
        RETURNING acyclic_stream_journal.revision INTO published_revision;
    PERFORM pg_notify('acyclic_stream_commits', stream_namespace);
    -- A refusal aborts the statement and its insertion/notification together.
    -- The durable acknowledgement may arrive later than this acceptance cut.
    IF deadline_millis IS NOT NULL AND
        floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint >= deadline_millis THEN
        RAISE EXCEPTION USING ERRCODE = 'PST01',
            MESSAGE = 'STEP stream-publication refused: acceptance deadline elapsed',
            HINT = 'Recovery: reconcile the original same-key Stream request before submitting new work';
    END IF;
    RETURN QUERY SELECT published_revision, accepted_millis, true;
END;
$$;
COMMIT;
