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
-- One revision incarnation per namespace journal, shared by every replica.
-- Deleting the row (or recreating the database) issues a new incarnation, so
-- tokens from an earlier journal refuse instead of matching reused indexes.
-- A restore from backup must delete it too.
CREATE TABLE IF NOT EXISTS acyclic_stream_generation (
    namespace text PRIMARY KEY,
    generation bytea NOT NULL CHECK (octet_length(generation) = 16)
);

-- Physical transaction entry: acquire the publication fence before reading a
-- bounded page, then return one head/clock row (NULL command). The
-- caller keeps this transaction open for canonical planning and publication.
CREATE OR REPLACE FUNCTION acyclic_stream_read_fenced(
    stream_namespace text,
    after_revision bigint,
    page_size bigint
) RETURNS TABLE (revision bigint, at_millis bigint, command bytea)
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION USING ERRCODE = 'PST02',
            MESSAGE = 'STEP stream-fenced-read failed: READ COMMITTED required',
            HINT = 'Recovery: reopen PostgresStream::connect with the same database and namespace';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(stream_namespace, 1937006962));
    -- VOLATILE obtains this snapshot after lock acquisition, including when a
    -- competing publisher committed while this statement waited for the lock.
    RETURN QUERY SELECT j.revision, j.at_millis, j.command
        FROM acyclic_stream_journal AS j
        WHERE j.namespace = stream_namespace AND j.revision > after_revision
        ORDER BY j.revision LIMIT page_size;
    RETURN QUERY SELECT COALESCE(max(j.revision), 0),
        floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint, NULL::bytea
        FROM acyclic_stream_journal AS j WHERE j.namespace = stream_namespace;
END;
$$;

-- Physical append-if-revision only. The canonical Stream core decides paths,
-- conditions and retry identity; SQL never decodes a command. VOLATILE keeps
-- the head query's READ COMMITTED snapshot after the namespace lock wait.
-- The core plans at the caller-read transaction clock; the row records exactly
-- that time so replay reproduces the same decision.
CREATE OR REPLACE FUNCTION acyclic_stream_publish(
    stream_namespace text,
    expected_revision bigint,
    command_bytes bytea[],
    planned_millis bigint[],
    deadline_millis bigint[]
) RETURNS TABLE (revision bigint, at_millis bigint, accepted boolean)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    current_revision bigint;
    accepted_millis bigint;
    published_revision bigint;
    frame record;
    earliest_deadline bigint;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION USING ERRCODE = 'PST02',
            MESSAGE = 'STEP stream-publication failed: READ COMMITTED required',
            HINT = 'Recovery: reopen PostgresStream::connect with the same database and namespace';
    END IF;
    IF cardinality(command_bytes) IS NULL OR cardinality(command_bytes) = 0 OR
        cardinality(command_bytes) IS DISTINCT FROM cardinality(planned_millis) OR
        cardinality(command_bytes) IS DISTINCT FROM cardinality(deadline_millis) THEN
        RAISE EXCEPTION USING ERRCODE = 'PST03',
            MESSAGE = 'STEP stream-publication refused: empty or mismatched frame arrays',
            HINT = 'Recovery: submit the original nonempty ordered journal frame batch';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(stream_namespace, 1937006962));
    SELECT COALESCE(max(j.revision), 0) INTO current_revision
        FROM acyclic_stream_journal AS j WHERE j.namespace = stream_namespace;
    IF current_revision <> expected_revision THEN
        RETURN QUERY SELECT current_revision, 0::bigint, false;
        RETURN;
    END IF;
    -- Array order is publication order. Identity values may have gaps; only
    -- RETURNING supplies the new opaque cursor, never expected_revision + count.
    FOR frame IN SELECT * FROM unnest(command_bytes, planned_millis, deadline_millis)
        AS f(command, stamp, deadline)
    LOOP
        accepted_millis := frame.stamp;
        IF frame.deadline IS NOT NULL AND accepted_millis >= frame.deadline THEN
            RAISE EXCEPTION USING ERRCODE = 'PST01',
                MESSAGE = 'STEP stream-publication refused: acceptance deadline elapsed',
                HINT = 'Recovery: reconcile the original same-key Stream request before submitting new work';
        END IF;
        INSERT INTO acyclic_stream_journal(namespace, at_millis, command)
            VALUES(stream_namespace, accepted_millis, frame.command)
            RETURNING acyclic_stream_journal.revision INTO published_revision;
        earliest_deadline := LEAST(earliest_deadline, frame.deadline);
    END LOOP;
    PERFORM pg_notify('acyclic_stream_commits', stream_namespace);
    -- A refusal aborts the statement and its insertion/notification together.
    -- The durable acknowledgement may arrive later than this acceptance cut.
    IF earliest_deadline IS NOT NULL AND
        floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint >= earliest_deadline THEN
        RAISE EXCEPTION USING ERRCODE = 'PST01',
            MESSAGE = 'STEP stream-publication refused: acceptance deadline elapsed',
            HINT = 'Recovery: reconcile the original same-key Stream request before submitting new work';
    END IF;
    RETURN QUERY SELECT published_revision, accepted_millis, true;
END;
$$;
COMMIT;
