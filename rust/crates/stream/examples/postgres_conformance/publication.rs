//! Real SQL publication controls, including rollback after a written prefix.
use acyclic_stream::postgres::PostgresStream;
use tokio_postgres::{Client, NoTls, Row};

const PUBLISH: &str = "SELECT revision,at_millis,accepted FROM acyclic_stream_publish(\
    $1,$2,$3::bytea[],$4::bigint[],$5::bigint[])";

async fn publish(
    client: &Client,
    namespace: &str,
    expected: i64,
    commands: &[Vec<u8>],
    stamps: &[i64],
    deadlines: &[Option<i64>],
) -> Result<Row, tokio_postgres::Error> {
    client
        .query_one(
            PUBLISH,
            &[&namespace, &expected, &commands, &stamps, &deadlines],
        )
        .await
}

async fn rows(client: &Client, namespace: &str) -> Result<Vec<Row>, tokio_postgres::Error> {
    client
        .query(
            "SELECT revision,at_millis,command FROM acyclic_stream_journal \
             WHERE namespace=$1 ORDER BY revision",
            &[&namespace],
        )
        .await
}

pub(super) async fn verify(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(database, NoTls).await?;
    let driver = tokio::spawn(connection);
    client
        .batch_execute("SET default_transaction_isolation='read committed'")
        .await?;
    for width in [1_i64, 16, 256] {
        let namespace = uuid::Uuid::new_v4().to_string();
        // Install through the public provider, then inspect its actual SQL port.
        let provider = PostgresStream::connect(database, &namespace).await?;
        drop(provider);
        // A consumed identity is a real gap, never an assumed sequence + count.
        client
            .query_one(
                "SELECT nextval(pg_get_serial_sequence('acyclic_stream_journal','revision'))",
                &[],
            )
            .await?;
        let commands: Vec<Vec<u8>> = (0..width).map(|i| i.to_le_bytes().to_vec()).collect();
        let stamps: Vec<i64> = (1..=width).collect();
        let deadlines = vec![None; commands.len()];
        let accepted = publish(&client, &namespace, 0, &commands, &stamps, &deadlines).await?;
        let head: i64 = accepted.get(0);
        assert!(accepted.get::<_, bool>(2));
        assert_eq!(accepted.get::<_, i64>(1), width);
        let retained = rows(&client, &namespace).await?;
        assert_eq!(retained.len(), commands.len());
        assert_eq!(retained.last().unwrap().get::<_, i64>(0), head);
        assert!(retained.first().unwrap().get::<_, i64>(0) > 1);
        for (i, row) in retained.iter().enumerate() {
            assert_eq!(row.get::<_, i64>(1), stamps[i]);
            assert_eq!(row.get::<_, Vec<u8>>(2), commands[i]);
        }
        // Negative control: stale publication adds no frames and returns the
        // storage cursor, rather than a canonical Stream sequence or batch size.
        let stale = publish(&client, &namespace, 0, &commands, &stamps, &deadlines).await?;
        assert!(!stale.get::<_, bool>(2));
        assert_eq!(stale.get::<_, i64>(0), head);
        assert_eq!(rows(&client, &namespace).await?.len(), commands.len());
        // The second frame refuses after the first INSERT. Both roll back.
        let refused = vec![b"first".to_vec(), b"second".to_vec()];
        for (times, deadlines) in [
            ([1, 2], [None, Some(2)]),
            // Planning time passes; the actual database acceptance clock fails.
            ([0, 1], [Some(2), None]),
        ] {
            let error = publish(&client, &namespace, head, &refused, &times, &deadlines)
                .await
                .unwrap_err();
            assert_eq!(error.as_db_error().unwrap().code().code(), "PST01");
            assert_eq!(rows(&client, &namespace).await?.len(), commands.len());
        }
        let error = publish(&client, &namespace, head, &refused, &[], &[None, None])
            .await
            .unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "PST03");
        let corrected =
            publish(&client, &namespace, head, &refused, &[3, 4], &[None, None]).await?;
        assert!(corrected.get::<_, bool>(2));
        assert!(corrected.get::<_, i64>(0) > head + 2);
        assert_eq!(rows(&client, &namespace).await?.len(), commands.len() + 2);
    }
    // The function can return a row before PostgreSQL attempts its implicit
    // COMMIT. A deferred trigger supplies a real commit-time failure: consuming
    // that row must never turn the aborted publication into an acknowledgement.
    client.batch_execute(
        "CREATE FUNCTION publication_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION USING ERRCODE='PST04', MESSAGE='publication commit control'; END; $$;
         CREATE CONSTRAINT TRIGGER publication_commit_failure AFTER INSERT ON acyclic_stream_journal
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
         WHEN (NEW.namespace = 'commit-failure-control') EXECUTE FUNCTION publication_commit_failure();"
    ).await?;
    let error = publish(
        &client,
        "commit-failure-control",
        0,
        &[b"aborted".to_vec()],
        &[0],
        &[None],
    )
    .await
    .unwrap_err();
    assert_eq!(error.as_db_error().unwrap().code().code(), "PST04");
    assert!(rows(&client, "commit-failure-control").await?.is_empty());
    client.batch_execute("DROP TRIGGER publication_commit_failure ON acyclic_stream_journal; DROP FUNCTION publication_commit_failure()").await?;
    // Progress control: dropping the actual protocol driver terminates a sent
    // request. No timer, readiness probe or replacement connection produces it.
    let namespace = uuid::Uuid::new_v4().to_string();
    let commands = [b"disconnected".to_vec()];
    let request = publish(&client, &namespace, 0, &commands, &[0], &[None]);
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    driver.abort();
    assert!(request.await.is_err());
    println!(
        "SQL publication batches 1/16/256 preserve order, opaque gaps, atomic refusals and driver-loss progress"
    );
    Ok(())
}
