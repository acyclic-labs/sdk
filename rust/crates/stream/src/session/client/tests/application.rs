use super::*;
use futures::FutureExt as _;

fn subscription() -> wire::SessionSubscription {
    wire::SessionSubscription {
        channel: None,
        resume: Some(cursor(0)),
        credential: 0,
        filter: None,
        kind: Some(Kind::Follow(wire::SessionFollow {
            resource: wire_codec::session::stream_resource("events"),
            delivery: wire::SessionDelivery::All as i32,
            ..Default::default()
        })),
    }
}

async fn data(source: &mut ApplicationSubscription) -> Result<ApplicationDelivery, StreamError> {
    loop {
        let delivery = source.next().await?.ok_or(StreamError::Unavailable)?;
        if matches!(delivery.frame().body, Some(Body::Data(_))) {
            return Ok(delivery);
        }
        delivery.discard()?;
    }
}

fn sequence(delivery: &ApplicationDelivery) -> Result<u64, StreamError> {
    let Some(Body::Data(data)) = &delivery.frame().body else {
        return Err(StreamError::InvalidResponse);
    };
    let Some(Item::Record(record)) = &data.item else {
        return Err(StreamError::InvalidResponse);
    };
    Ok(record.sequence)
}

#[tokio::test]
async fn crash_before_application_commit_replays_despite_returned_credit() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 3).await?;
    let client = Client::in_process(
        provider.clone(),
        crate::session::testing::authority(),
        Arc::new(crate::runtime::PlatformExecutor),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    let mut source = client.deliveries(subscription()).await?;
    let initial = source.checkpoint()?;
    let delivery = data(&mut source).await?;
    assert_eq!(sequence(&delivery)?, 0);
    let transport_cut = delivery.checkpoint().clone();
    assert_eq!(
        transport_cut.resume.as_ref().ok_or("no proposed cut")?.next,
        1
    );
    // Cancelling a blocked next must not remove its completion fence.
    assert!(source.next().now_or_never().is_none());
    assert!(source.next().now_or_never().is_none());
    assert_eq!(source.checkpoint()?, initial);
    drop(delivery); // Credit returns; application progress must remain zero.
    assert_eq!(source.checkpoint()?, initial);
    assert!(matches!(source.next().await, Err(StreamError::Unavailable)));
    drop(source);

    let mut recovered = client.deliveries(initial).await?;
    let delivery = data(&mut recovered).await?;
    assert_eq!(sequence(&delivery)?, 0);
    let checkpoint = delivery.checkpoint().encode_to_vec();
    let decided = provider
        .commit(crate::CommitRequest {
            deadline_unix_millis: None,
            conditions: Vec::new(),
            mutations: vec![
                crate::CommitMutation::Append {
                    path: StreamPath::new("consumer/effects")?,
                    records: vec!["effect".into()],
                },
                crate::CommitMutation::Append {
                    path: StreamPath::new("consumer/checkpoint")?,
                    records: vec![OwnedBytes::from(checkpoint).into()],
                },
            ],
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    assert!(matches!(decided, crate::CommitOutcome::Committed(_)));
    assert!(delivery.commit()?);
    let committed = recovered.checkpoint()?;
    assert_eq!(committed, transport_cut);
    let delivery = data(&mut recovered).await?;
    assert_eq!(sequence(&delivery)?, 1);
    assert!(delivery.discard()?);
    assert_eq!(recovered.checkpoint()?.resume.ok_or("no cut")?.next, 2);
    let abandoned_owner = data(&mut recovered).await?;
    drop(recovered);
    assert!(
        !abandoned_owner.commit()?,
        "a closed interest completed outstanding custody"
    );

    // Negative control: equating receive/credit with completion skips zero
    // even though no consumer decision for that first delivery was persisted.
    let mut premature = client.deliveries(transport_cut).await?;
    assert_eq!(sequence(&data(&mut premature).await?)?, 1);
    Ok(())
}

#[tokio::test]
async fn keyed_application_checkpoints_reopen_without_a_contiguous_anchor() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    crate::conformance::key_seek::verify(&*provider).await?;
    let client = Client::in_process(
        provider,
        crate::session::testing::authority(),
        Arc::new(crate::runtime::PlatformExecutor),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    let mut selected = subscription();
    let Some(Kind::Follow(follow)) = &mut selected.kind else {
        unreachable!()
    };
    follow.resource = wire_codec::session::stream_resource(crate::conformance::key_seek::SOURCE);
    follow.key = Some(wire::ReadKeySeek {
        key: Bytes::from_static(b"a"),
        before: 4,
    });
    follow.read_limit = 3;
    follow.until = 4;
    let mut source = client.deliveries(selected.clone()).await?;
    let first = data(&mut source).await?;
    assert_eq!(sequence(&first)?, 0);
    let proposed = first.checkpoint().clone();
    let Some(Body::Data(frame)) = &first.frame().body else {
        unreachable!()
    };
    let Some(Item::Record(record)) = &frame.item else {
        unreachable!()
    };
    let commit_id = record.commit_id.clone();
    drop(first);
    assert_eq!(source.checkpoint()?.resume, selected.resume);
    drop(source);

    // A crash before the application decision replays the first sparse item.
    let mut recovered = client.deliveries(selected).await?;
    let first = data(&mut recovered).await?;
    assert_eq!(sequence(&first)?, 0);
    assert!(first.commit()?);
    let committed = recovered.checkpoint()?;
    assert_eq!(committed, proposed);
    drop(recovered);

    // Both the proposed durable cut and the committed cut reopen at the next
    // matching key, skipping sequence one without inventing a prefix fence.
    let mut resumed = client.deliveries(committed.clone()).await?;
    let next = data(&mut resumed).await?;
    assert_eq!(sequence(&next)?, 2);
    assert!(next.discard()?);
    assert_eq!(resumed.checkpoint()?.resume.ok_or("no cut")?.next, 3);
    drop(resumed);
    let mut resumed = client.deliveries(proposed).await?;
    assert_eq!(sequence(&data(&mut resumed).await?)?, 2);

    // Negative control: the former checkpoint's contiguous anchor is invalid
    // for a key seek and would prevent recovery before any record is served.
    let mut invalid = committed;
    let Some(Kind::Follow(follow)) = &mut invalid.kind else {
        unreachable!()
    };
    follow.preceding_commit_id = commit_id;
    assert!(matches!(
        client.deliveries(invalid).await,
        Err(StreamError::InvalidArgument)
    ));
    Ok(())
}

#[tokio::test]
async fn reconnect_fences_an_outstanding_application_decision() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 4).await?;
    let fixture = Fixture::open(provider, "node").await?;
    let client = fixture.client()?.session_client()?;
    let mut source = client.deliveries(subscription()).await?;
    let old = data(&mut source).await?;
    assert_eq!(sequence(&old)?, 0);
    fixture.reset()?;
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.until(|counts| counts.follows.load(Ordering::Relaxed) >= 2),
    )
    .await?;
    assert!(
        !old.commit()?,
        "a fenced generation certified a later application cut"
    );
    assert_eq!(source.checkpoint()?.resume.ok_or("no cut")?.next, 0);
    let replayed = data(&mut source).await?;
    assert_eq!(sequence(&replayed)?, 0);
    assert!(replayed.commit()?);
    Ok(())
}
