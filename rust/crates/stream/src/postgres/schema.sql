BEGIN ISOLATION LEVEL READ COMMITTED;
-- Serialize first-open DDL across independent processes, including an empty DB.
SELECT pg_advisory_xact_lock(hashtextextended('acyclic-stream-schema', 1937006962));
-- One native key for indexes, fencing and notifications. It is not an identity:
-- every lookup retains the exact original spelling, including hash collisions.
CREATE OR REPLACE FUNCTION acyclic_stream_namespace_key(stream_namespace text)
RETURNS bigint LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT hashtextextended(stream_namespace COLLATE "C", 1937006962)
$$;
-- Keep the namespace itself when it fits the PostgreSQL NOTIFY representation.
-- Longer names use their index key; notification equality only requests a refresh.
CREATE OR REPLACE FUNCTION acyclic_stream_notification_payload(
    stream_namespace text, namespace_key bigint
) RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT CASE WHEN octet_length(stream_namespace) < 8000
        THEN stream_namespace ELSE namespace_key::text END
$$;
CREATE TABLE IF NOT EXISTS acyclic_stream_journal (
    revision bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace text NOT NULL,
    at_millis bigint NOT NULL CHECK (at_millis >= 0),
    -- Public command ceiling (1056768), plus oneof tag and length delimiter.
    command bytea NOT NULL CHECK (octet_length(command) <= 1056772)
);
CREATE INDEX IF NOT EXISTS acyclic_stream_journal_key_revision
    ON acyclic_stream_journal(acyclic_stream_namespace_key(namespace), revision);
DROP INDEX IF EXISTS acyclic_stream_journal_namespace_revision;
-- One revision incarnation per namespace journal, shared by every replica.
-- Deleting the row (or recreating the database) issues a new incarnation, so
-- tokens from an earlier journal refuse instead of matching reused indexes.
-- A restore from backup must delete it too.
CREATE TABLE IF NOT EXISTS acyclic_stream_generation (
    namespace text NOT NULL,
    generation bytea NOT NULL CHECK (octet_length(generation) = 16)
);

-- Only indexes change. Rows, generation bytes and revision identities retain
-- their original representation. Full names distinguish a shared native key.
CREATE INDEX IF NOT EXISTS acyclic_stream_generation_key
    ON acyclic_stream_generation(acyclic_stream_namespace_key(namespace));
ALTER TABLE acyclic_stream_generation
    DROP CONSTRAINT IF EXISTS acyclic_stream_generation_pkey;

CREATE OR REPLACE FUNCTION acyclic_stream_open(
    stream_namespace text, proposed_generation bytea
) RETURNS TABLE (generation bytea, notification_key text)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    namespace_key bigint := acyclic_stream_namespace_key(stream_namespace);
    retained bytea;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION USING ERRCODE = 'PST02',
            MESSAGE = 'namespace opening requires READ COMMITTED';
    END IF;
    PERFORM pg_advisory_xact_lock(namespace_key);
    -- Acquire the eventual insert lock before reading, so concurrent DDL
    -- cannot interpose an exclusive lock ahead of a later lock upgrade.
    LOCK TABLE acyclic_stream_generation IN ROW EXCLUSIVE MODE;
    SELECT g.generation INTO retained FROM acyclic_stream_generation g
        WHERE acyclic_stream_namespace_key(g.namespace) = namespace_key
            AND g.namespace COLLATE "C" = stream_namespace COLLATE "C";
    IF NOT FOUND THEN
        INSERT INTO acyclic_stream_generation(namespace, generation)
            VALUES (stream_namespace, proposed_generation)
            RETURNING acyclic_stream_generation.generation INTO retained;
    END IF;
    RETURN QUERY SELECT retained, acyclic_stream_notification_payload(stream_namespace, namespace_key);
END;
$$;

-- Physical append-if-revision only. The canonical Stream core decides paths,
-- conditions and retry identity; SQL never decodes a command. VOLATILE keeps
-- the head query's READ COMMITTED snapshot after the namespace lock wait.
-- The core binds the returned database clock to its original accepted plan;
-- replay uses the same persisted stamp, without a preceding clock read.
CREATE OR REPLACE FUNCTION acyclic_stream_publish(
    stream_namespace text,
    expected_revision bigint,
    command_bytes bytea[],
    deadline_millis bigint[]
) RETURNS TABLE (revision bigint, at_millis bigint, accepted boolean)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    namespace_key bigint := acyclic_stream_namespace_key(stream_namespace);
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
        cardinality(command_bytes) IS DISTINCT FROM cardinality(deadline_millis) THEN
        RAISE EXCEPTION USING ERRCODE = 'PST03',
            MESSAGE = 'STEP stream-publication refused: empty or mismatched frame arrays',
            HINT = 'Recovery: submit the original nonempty ordered journal frame batch';
    END IF;
    PERFORM pg_advisory_xact_lock(namespace_key);
    SELECT COALESCE(max(j.revision), 0) INTO current_revision
        FROM acyclic_stream_journal AS j WHERE acyclic_stream_namespace_key(j.namespace) = namespace_key
            AND j.namespace COLLATE "C" = stream_namespace COLLATE "C";
    IF current_revision <> expected_revision THEN
        RETURN QUERY SELECT current_revision, 0::bigint, false;
        RETURN;
    END IF;
    -- Array order is publication order. Identity values may have gaps; only
    -- RETURNING supplies the new opaque cursor, never expected_revision + count.
    accepted_millis := floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint;
    FOR frame IN SELECT * FROM unnest(command_bytes, deadline_millis)
        AS f(command, deadline)
    LOOP
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
    PERFORM pg_notify('acyclic_stream_commits', acyclic_stream_notification_payload(stream_namespace, namespace_key));
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
