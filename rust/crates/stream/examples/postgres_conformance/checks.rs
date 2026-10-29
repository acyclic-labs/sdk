//! Runs the unchanged suite and cross-session durability/CAS/notification checks.
use acyclic_stream::{postgres::PostgresStream, *};
use futures::StreamExt;
use futures::TryFutureExt as _;
use std::time::{Duration, Instant};
#[path = "publication.rs"]
mod publication;
#[path = "refusals.rs"]
mod refusals;

pub async fn verify(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    let _diagnostics = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_writer(std::io::stderr)
            .finish(),
    );
    publication::verify(database).await?;
    let namespace = uuid::Uuid::new_v4().to_string();
    let start = Instant::now();
    let (provider, peer) = tokio::try_join!(
        PostgresStream::connect(database, &namespace),
        PostgresStream::connect(database, &namespace)
    )?;
    acyclic_stream::conformance::verify(&acyclic_stream::conformance::Fixture::new(
        provider.clone(),
    ))
    .await?;
    println!("Stream conformance passed in {:?}", start.elapsed());
    let path = StreamPath::new("cross-session")?;
    let first = AppendRequest {
        path: path.clone(),
        records: vec![OwnedBytes::from_static(b"first").into()],
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::new(b"cross-session-first".to_vec())?,
    };
    let receipt = provider.append(first.clone()).await?;
    assert_eq!(peer.append(first).await?, receipt);
    let mut follow = peer
        .follow(acyclic_stream::FollowRequest::new(path.clone(), 1))
        .map_ok(acyclic_stream::records_only)
        .await?;
    let request = AppendRequest {
        path: path.clone(),
        records: vec![OwnedBytes::from_static(b"second").into()],
        if_tail: Some(1),
        idempotency_key: acyclic_stream::IdempotencyKey::new(&b"checks-1"[..])?,
    };
    // Two distinct writers race the same tail: distinct retry identities.
    let rival = AppendRequest {
        idempotency_key: acyclic_stream::IdempotencyKey::new(&b"checks-rival"[..])?,
        ..request.clone()
    };
    let (left, right) = tokio::join!(provider.append(request), peer.append(rival));
    assert!(matches!(
        (left?, right?),
        (
            AppendOutcome::Committed(_),
            AppendOutcome::TailConflict { actual_tail: 2 }
        ) | (
            AppendOutcome::TailConflict { actual_tail: 2 },
            AppendOutcome::Committed(_)
        )
    ));
    let record = tokio::time::timeout(Duration::from_secs(1), follow.next())
        .await?
        .ok_or("notification stream closed")??;
    assert_eq!(record.sequence, 1);
    let observer = PostgresStream::connect(database, &namespace).await?;
    let mut remote = observer
        .follow(acyclic_stream::FollowRequest::new(path.clone(), 2))
        .map_ok(acyclic_stream::records_only)
        .await?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"notification-only").into()],
            if_tail: Some(2),
            idempotency_key: acyclic_stream::IdempotencyKey::new(&b"checks-2"[..])?,
        })
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), remote.next())
            .await?
            .ok_or("remote notification stream closed")??
            .sequence,
        2
    );
    let committed = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: 3,
        }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"deadline").into()],
        }],
        idempotency_key: IdempotencyKey::new(b"deadline-replay".to_vec())?,
    };
    let outcome = provider.commit(committed.clone()).await?;
    assert_eq!(
        peer.commit(CommitRequest {
            deadline_unix_millis: Some(0),
            ..committed
        })
        .await?,
        outcome
    );
    let reopened = PostgresStream::connect(database, &namespace).await?;
    assert_eq!(reopened.tail(path).await?, 4);
    println!("Cross-session replay, exact CAS, LISTEN/NOTIFY and reopen checks passed");
    refusals::verify(database).await?;
    subscription_lifetime(database).await?;
    listener_failure(database).await?;
    Ok(())
}

async fn subscription_lifetime(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    for (anchored, progress) in [(false, false), (true, false), (true, true)] {
        let namespace = uuid::Uuid::new_v4().to_string();
        let provider = PostgresStream::connect(database, &namespace).await?;
        let path = StreamPath::new("subscription-owner")?;
        let AppendOutcome::Committed(receipt) = provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![OwnedBytes::from_static(b"anchor").into()],
                if_tail: Some(0),
                idempotency_key: acyclic_stream::IdempotencyKey::new(&b"checks-3"[..])?,
            })
            .await?
        else {
            return Err("initial append unexpectedly conflicted".into());
        };
        let mut subscription = provider
            .follow(acyclic_stream::FollowRequest {
                when_absent: Default::default(),
                path: path.clone(),
                from: 1,
                preceding: anchored.then_some(receipt.commit_id),
                filter: FollowFilter::default(),
                progress,
                at_least: Default::default(),
            })
            .await?;
        drop(provider);
        // A distinct provider must wake this subscription through LISTEN after
        // its creator's last handle is gone, including after a previous yield.
        let publisher = PostgresStream::connect(database, &namespace).await?;
        for sequence in 1..=2 {
            publisher
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![OwnedBytes::from_static(b"still-following").into()],
                    if_tail: Some(sequence),
                    idempotency_key: acyclic_stream::IdempotencyKey::new(
                        format!("checks-4-{sequence}").into_bytes(),
                    )?,
                })
                .await?;
            let update = tokio::time::timeout(Duration::from_secs(1), subscription.next())
                .await?
                .ok_or("subscription closed after dropping provider")??;
            assert!(matches!(update, FollowUpdate::Record(record) if record.sequence == sequence));
        }
        println!(
            "follow anchored={anchored} progress={progress} retains shared connections after its provider handle is dropped"
        );
    }
    Ok(())
}

async fn listener_failure(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    let namespace = uuid::Uuid::new_v4().to_string();
    let provider = PostgresStream::connect(database, &namespace).await?;
    let path = StreamPath::new("listener-loss")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::new().into()],
            if_tail: Some(0),
            idempotency_key: acyclic_stream::IdempotencyKey::new(&b"checks-5"[..])?,
        })
        .await?;
    let mut records = provider
        .follow(acyclic_stream::FollowRequest::new(path.clone(), 1))
        .map_ok(acyclic_stream::records_only)
        .await?;
    let mut updates = provider
        .follow(acyclic_stream::FollowRequest {
            when_absent: Default::default(),
            path: path,
            from: 1,
            preceding: None,
            filter: FollowFilter::default(),
            progress: true,
            at_least: Default::default(),
        })
        .await?;
    let (client, connection) = tokio_postgres::connect(database, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    let killed = client
        .query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=$1",
            &[&format!("acyclic-stream-listener-{namespace}")],
        )
        .await?;
    assert_eq!(killed.len(), 1);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), records.next()).await?,
        Some(Err(StreamError::Unavailable))
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), updates.next()).await?,
        Some(Err(StreamError::Unavailable))
    ));
    assert!(records.next().await.is_none());
    assert!(updates.next().await.is_none());
    driver.abort();
    println!("Listener loss fails record and progress subscriptions closed");
    Ok(())
}
