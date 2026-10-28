//! Observe actual SQL journal storage, rather than only logical Stream results.
use acyclic_stream::{postgres::PostgresStream, *};

async fn retained(
    client: &tokio_postgres::Client,
    namespace: &str,
) -> Result<(i64, i64), tokio_postgres::Error> {
    let row = client.query_one(
        "SELECT count(*), coalesce(sum(octet_length(command)),0)::bigint FROM acyclic_stream_journal WHERE namespace=$1",
        &[&namespace],
    ).await?;
    Ok((row.get(0), row.get(1)))
}

pub(super) async fn verify(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    let namespace = uuid::Uuid::new_v4().to_string();
    let provider = PostgresStream::connect(database, &namespace).await?;
    let (client, connection) = tokio_postgres::connect(database, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    let path = StreamPath::new("refusal/corrected")?;
    let mut append = AppendRequest {
        path: path.clone(),
        records: vec![OwnedBytes::from_static(b"accepted").into()],
        if_tail: Some(1),
        idempotency_key: IdempotencyKey::new(b"corrected-append".to_vec())?,
    };
    let mut fork = ForkRequest {
        source: path.clone(),
        destination: StreamPath::new("refusal/forked")?,
        at_tail: None,
        exclude: Vec::new(),
        idempotency_key: IdempotencyKey::new(b"corrected-fork".to_vec())?,
    };
    let commit = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: 1,
        }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"committed").into()],
        }],
        idempotency_key: IdempotencyKey::new(b"corrected-commit".to_vec())?,
    };
    for _ in 0..16 {
        assert!(matches!(
            provider.append(append.clone()).await?,
            AppendOutcome::TailConflict { actual_tail: 0 }
        ));
        assert!(matches!(
            provider.fork(fork.clone()).await,
            Err(StreamError::NotFound)
        ));
        assert!(matches!(
            provider.commit(commit.clone()).await?,
            CommitOutcome::Conflict(_)
        ));
    }
    assert!(matches!(
        provider.commit(CommitRequest { deadline_unix_millis: Some(0), ..commit.clone() }).await,
        Err(StreamError::DeadlineElapsed)
    ));
    for key in [
        append.idempotency_key.clone(),
        fork.idempotency_key.clone(),
        Some(commit.idempotency_key.clone()),
    ]
    .into_iter()
    .flatten()
    {
        assert!(provider.inspect_idempotency(key).await?.is_none());
    }
    assert_eq!(retained(&client, &namespace).await?, (0, 0));
    append.if_tail = Some(0);
    let appended = provider.append(append.clone()).await?;
    let committed = provider.commit(commit.clone()).await?;
    fork.at_tail = Some(2);
    let forked = provider.fork(fork.clone()).await?;
    let accepted = retained(&client, &namespace).await?;
    assert_eq!(accepted.0, 3);
    assert!(accepted.1 > 0);
    let reopened = PostgresStream::connect(database, &namespace).await?;
    assert_eq!(reopened.append(append).await?, appended);
    assert_eq!(reopened.commit(CommitRequest { deadline_unix_millis: Some(0), ..commit }).await?, committed);
    assert_eq!(reopened.fork(fork).await?, forked);
    assert_eq!(retained(&client, &namespace).await?, accepted);
    println!(
        "Refused commands retain zero SQL rows/bytes; corrected identities and all operation retries reopen with exactly three accepted rows ({} bytes)",
        accepted.1
    );
    driver.abort();
    Ok(())
}
