//! Real SQL publication controls, including rollback after a written prefix.
use acyclic_stream::postgres::PostgresStream;
use tokio_postgres::{Client, NoTls, Row};

const PUBLISH: &str = "SELECT revision,at_millis,accepted FROM acyclic_stream_publish(\
    $1,$2,$3::bytea[],$4::bigint[])";

async fn publish(
    client: &Client,
    namespace: &str,
    expected: i64,
    commands: &[Vec<u8>],
    deadlines: &[Option<i64>],
) -> Result<Row, tokio_postgres::Error> {
    client
        .query_one(PUBLISH, &[&namespace, &expected, &commands, &deadlines])
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

/// The SQLSTATE a refused publication carries; an acknowledgement has none.
fn refused<T>(result: Result<T, tokio_postgres::Error>) -> Option<String> {
    result
        .err()?
        .as_db_error()
        .map(|error| error.code().code().to_owned())
}

async fn now_millis(client: &Client) -> Result<i64, tokio_postgres::Error> {
    Ok(client
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            &[],
        )
        .await?
        .get(0))
}

pub(super) async fn verify(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(database, NoTls).await?;
    let driver = tokio::spawn(connection);
    client
        .batch_execute("SET default_transaction_isolation='read committed'")
        .await?;
    for width in [1_i64, 16, 256] {
        batch(&client, database, width).await?;
    }
    commit_failure(&client).await?;
    // Progress control: dropping the actual protocol driver terminates a sent
    // request. No timer, readiness probe or replacement connection produces it.
    let namespace = uuid::Uuid::new_v4().to_string();
    let commands = [b"disconnected".to_vec()];
    let request = publish(&client, &namespace, 0, &commands, &[None]);
    tokio::pin!(request);
    assert!(futures::poll!(&mut request).is_pending());
    driver.abort();
    assert!(request.await.is_err());
    println!(
        "SQL publication batches 1/16/256 preserve order, opaque gaps, atomic refusals and driver-loss progress"
    );
    Ok(())
}

/// One batch of `width` frames publishes in order behind a real identity gap,
/// and every refusal rolls back its written prefix.
async fn batch(
    client: &Client,
    database: &str,
    width: i64,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let deadlines = vec![None; commands.len()];
    let before = now_millis(client).await?;
    let accepted = publish(client, &namespace, 0, &commands, &deadlines).await?;
    let after = now_millis(client).await?;
    let head: i64 = accepted.get(0);
    assert!(accepted.get::<_, bool>(2));
    let accepted_millis = accepted.get::<_, i64>(1);
    assert!((before..=after).contains(&accepted_millis));
    let retained = rows(client, &namespace).await?;
    assert_eq!(retained.len(), commands.len());
    assert_eq!(retained.last().map(|row| row.get::<_, i64>(0)), Some(head));
    assert!(retained.first().is_some_and(|row| row.get::<_, i64>(0) > 1));
    for (row, command) in retained.iter().zip(&commands) {
        assert_eq!(row.get::<_, i64>(1), accepted_millis);
        assert_eq!(&row.get::<_, Vec<u8>>(2), command);
    }
    // Negative control: stale publication adds no frames and returns the
    // storage cursor, rather than a canonical Stream sequence or batch size.
    let stale = publish(client, &namespace, 0, &commands, &deadlines).await?;
    assert!(!stale.get::<_, bool>(2));
    assert_eq!(stale.get::<_, i64>(0), head);
    assert_eq!(rows(client, &namespace).await?.len(), commands.len());
    // The second frame refuses after the first INSERT. Both roll back.
    let refused_frames = vec![b"first".to_vec(), b"second".to_vec()];
    for deadlines in [[None, Some(2)], [Some(2), None]] {
        let result = publish(client, &namespace, head, &refused_frames, &deadlines).await;
        assert_eq!(refused(result).as_deref(), Some("PST01"));
        assert_eq!(rows(client, &namespace).await?.len(), commands.len());
    }
    let result = publish(client, &namespace, head, &refused_frames, &[]).await;
    assert_eq!(refused(result).as_deref(), Some("PST03"));
    let corrected = publish(client, &namespace, head, &refused_frames, &[None, None]).await?;
    assert!(corrected.get::<_, bool>(2));
    assert!(corrected.get::<_, i64>(0) > head + 2);
    assert_eq!(rows(client, &namespace).await?.len(), commands.len() + 2);
    Ok(())
}

/// The function can return a row before PostgreSQL attempts its implicit
/// COMMIT. A deferred trigger supplies a real commit-time failure: consuming
/// that row must never turn the aborted publication into an acknowledgement.
async fn commit_failure(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    client.batch_execute(
        "CREATE FUNCTION publication_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION USING ERRCODE='PST04', MESSAGE='publication commit control'; END; $$;
         CREATE CONSTRAINT TRIGGER publication_commit_failure AFTER INSERT ON acyclic_stream_journal
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
         WHEN (NEW.namespace = 'commit-failure-control') EXECUTE FUNCTION publication_commit_failure();"
    ).await?;
    let result = publish(
        client,
        "commit-failure-control",
        0,
        &[b"aborted".to_vec()],
        &[None],
    )
    .await;
    assert_eq!(refused(result).as_deref(), Some("PST04"));
    assert!(rows(client, "commit-failure-control").await?.is_empty());
    client.batch_execute("DROP TRIGGER publication_commit_failure ON acyclic_stream_journal; DROP FUNCTION publication_commit_failure()").await?;
    Ok(())
}
