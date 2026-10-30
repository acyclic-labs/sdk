use super::*;
use crate::{CommitId, ReadRequest};

pub(super) async fn anchor(
    provider: &MemoryStream,
    path: &str,
    sequence: u64,
) -> Result<CommitId, Box<dyn std::error::Error + Send + Sync>> {
    Ok(provider
        .read(ReadRequest {
            key: None,
            path: StreamPath::new(path)?,
            from: sequence,
            limit: 1,
            at_least: Default::default(),
        })
        .await?
        .next()
        .await
        .ok_or("anchor missing")??
        .commit_id)
}

#[tokio::test]
async fn checked_registration_fails_before_returning_a_cursor_and_keeps_siblings_live() -> TestResult
{
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = fixture.client()?;
    let path = StreamPath::new("events")?;
    let mut ordinary = client
        .follow(crate::FollowRequest::new(path.clone(), 1))
        .map_ok(crate::records_only)
        .await?;
    assert!(matches!(
        client
            .follow(crate::FollowRequest {
                when_absent: Default::default(),
                path: path.clone(),
                from: 1,
                preceding: Some(CommitId::from_bytes([42; 32])),
                filter: crate::FollowFilter::default(),
                progress: false,
                at_least: Default::default(),
            })
            .map_ok(crate::records_only)
            .await,
        Err(StreamError::PrefixNotRetained)
    ));
    let mut checked = client
        .follow(crate::FollowRequest {
            when_absent: Default::default(),
            path: path.clone(),
            from: 1,
            preceding: Some(anchor(&provider, "events", 0).await?),
            filter: crate::FollowFilter::default(),
            progress: false,
            at_least: Default::default(),
        })
        .map_ok(crate::records_only)
        .await?;
    append(&provider, "events", 1).await?;
    assert_eq!(next(&mut checked).await?.sequence, 1);
    assert_eq!(next(&mut ordinary).await?.sequence, 1);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn unconfirmed_history_is_rejected_without_granting_credit_or_closing_siblings() -> TestResult
{
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 3).await?;
    let fixture = Fixture::configured(Arc::clone(&provider), "legacy", false, false).await?;
    let client = fixture.client()?;
    let path = StreamPath::new("events")?;
    let mut ordinary = client
        .follow(crate::FollowRequest::new(path.clone(), 3))
        .map_ok(crate::records_only)
        .await?;
    assert!(matches!(
        client
            .follow(crate::FollowRequest {
                when_absent: Default::default(),
                path: path.clone(),
                from: 1,
                preceding: Some(anchor(&provider, "events", 0).await?),
                filter: crate::FollowFilter::default(),
                progress: false,
                at_least: Default::default(),
            })
            .map_ok(crate::records_only)
            .await,
        Err(StreamError::Unsupported)
    ));
    assert_eq!(fixture.counts.sent.load(Ordering::Relaxed), 0);
    append(&provider, "events", 1).await?;
    assert_eq!(next(&mut ordinary).await?.sequence, 3);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn checked_move_and_reconnect_use_the_yielded_commit_instead_of_buffered_records()
-> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 1).await?;
    let old = Fixture::open(Arc::clone(&provider), "old").await?;
    let new = Fixture::open(Arc::clone(&provider), "new").await?;
    let destination = new._channel.clone();
    let connect: Connect = Arc::new(move |_| {
        let channel = destination.clone();
        Box::pin(async move { Ok(channel) })
    });
    let client = Client::with_connector(
        old._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        connect,
    );
    client.register_credential(0, crate::session::testing::credential())?;
    let path = StreamPath::new("events")?;
    let mut records = client
        .follow(crate::FollowRequest {
            when_absent: Default::default(),
            path: path,
            from: 1,
            preceding: Some(anchor(&provider, "events", 0).await?),
            filter: crate::FollowFilter::default(),
            progress: false,
            at_least: Default::default(),
        })
        .map_ok(crate::records_only)
        .await?;
    append(&provider, "events", 1).await?;
    let yielded = next(&mut records).await?;
    append(&provider, "events", 2).await?;
    loop {
        let changed = old.counts.changed.notified();
        if old.counts.sent.load(Ordering::Relaxed) >= 3 {
            break;
        }
        changed.await;
    }
    old.controls.send(Ok(frame(
        1,
        1,
        Body::Moved(wire::SessionMoved {
            node: Some(wire::SessionNode {
                node_id: "new".into(),
                endpoint: "https://new.invalid".into(),
                placement_epoch: 1,
            }),
            resume: Some(cursor(999)),
        }),
    )))?;
    wait_guard(&new, 2, yielded.commit_id).await?;
    assert_eq!(next(&mut records).await?.sequence, 2);
    let yielded = next(&mut records).await?;
    assert_eq!(yielded.sequence, 3);
    new.controls
        .send(Err(Status::unavailable("disconnect checked session")))?;
    wait_guard(&new, 3, yielded.commit_id).await?;
    append(&provider, "events", 1).await?;
    assert_eq!(next(&mut records).await?.sequence, 4);
    Ok(())
}
async fn wait_guard(fixture: &Fixture, generation: u64, expected: CommitId) -> TestResult {
    loop {
        let changed = fixture.counts.changed.notified();
        let guard = fixture
            .counts
            .guards
            .lock()
            .map_err(|_| "guards poisoned")?
            .iter()
            .find(|(id, current, _)| *id == 1 && *current == generation)
            .cloned();
        if let Some((_, _, guard)) = guard {
            assert_eq!(guard.as_ref(), expected.as_bytes());
            return Ok(());
        }
        changed.await;
    }
}

#[tokio::test]
async fn checked_delivery_reports_history_replacement_after_registration() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = fixture.client()?;
    let mut records = client
        .follow(crate::FollowRequest {
            when_absent: Default::default(),
            path: StreamPath::new("events")?,
            from: 1,
            preceding: Some(anchor(&provider, "events", 0).await?),
            filter: crate::FollowFilter::default(),
            progress: false,
            at_least: Default::default(),
        })
        .map_ok(crate::records_only)
        .await?;
    let replacement = MemoryStream::default();
    replacement
        .append(AppendRequest {
            path: StreamPath::new("events")?,
            records: vec![(OwnedBytes::from_static(b"replacement")).into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    provider
        .install_state(&replacement.encode_state().await)
        .await?;
    assert!(matches!(
        records.next().await,
        Some(Err(StreamError::PrefixNotRetained))
    ));
    Ok(())
}
