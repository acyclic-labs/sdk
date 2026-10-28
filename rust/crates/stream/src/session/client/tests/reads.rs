//! Provider reads and children pages are finite subscriptions on the session.
use super::*;

fn local(provider: &Arc<MemoryStream>) -> Result<Client, StreamError> {
    let client = Client::in_process(
        provider.clone(),
        crate::session::testing::authority(),
        Arc::new(crate::runtime::PlatformExecutor),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    Ok(client)
}
fn read(path: &str, from: u64, limit: u32) -> Result<crate::ReadRequest, StreamError> {
    Ok(crate::ReadRequest {
        key: None,
        path: StreamPath::new(path)?,
        from,
        limit,
        at_least: Default::default(),
    })
}
#[tokio::test]
async fn indexed_reads_share_session_credit_and_preserve_sparse_keys() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let client = local(&provider)?;
    crate::conformance::key_seek::verify_with(&*provider, async |request| {
        client.read(request).await
    })
    .await?;
    let follow = wire::SessionFollow {
        key: Some(wire::ReadKeySeek {
            key: Bytes::from_static(b"a"),
            before: 4,
        }),
        read_limit: 1,
        until: 4,
        delivery: wire::SessionDelivery::All as i32,
        resource: wire_codec::session::stream_resource("conformance/keys"),
        ..Default::default()
    };
    let record = wire_codec::record_wire(
        provider
            .read(read("conformance/keys", 0, 1)?)
            .await?
            .next()
            .await
            .ok_or(StreamError::Unavailable)??,
    );
    let mut data = wire::SessionData {
        after: Some(wire::SessionCursor {
            next: 1,
            opaque: Bytes::new(),
        }),
        accepted: 4,
        item: Some(Item::Record(record)),
        ..Default::default()
    };
    wire_codec::session::follow_data_sequence(&follow, 0, &data)?;
    let Some(Item::Record(record)) = &mut data.item else {
        unreachable!()
    };
    record.keys = vec![Bytes::from_static(b"a\0")];
    assert_eq!(
        wire_codec::session::follow_data_sequence(&follow, 0, &data),
        Err(StreamError::InvalidResponse)
    );
    Ok(())
}

async fn sequences(
    mut records: RecordStream,
) -> Result<Vec<u64>, Box<dyn std::error::Error + Send + Sync>> {
    let mut sequences = Vec::new();
    while let Some(record) = tokio::time::timeout(Duration::from_secs(1), records.next()).await? {
        sequences.push(record?.sequence);
    }
    Ok(sequences)
}
fn bounded(path: &str, until: u64) -> wire::SessionFollow {
    wire::SessionFollow {
        key: None,
        read_limit: 0,
        bootstrap_head: false,
        when_absent: Default::default(),
        resource: wire_codec::session::stream_resource(path),
        delivery: wire::SessionDelivery::All as i32,
        preceding_commit_id: Bytes::new(),
        until,
        at_least: Default::default(),
    }
}
fn page(parent: &str, limit: u32) -> Result<crate::ChildrenPageRequest, StreamError> {
    Ok(crate::ChildrenPageRequest {
        tail_stream: Some(StreamPath::new("status")?),
        parent: Some(StreamPath::new(parent)?),
        after: None,
        hierarchy_version: None,
        limit,
    })
}
async fn tree(provider: &MemoryStream) -> Result<(), StreamError> {
    append(provider, "tree/a/status", 2).await?;
    append(provider, "tree/b/status", 1).await?;
    append(provider, "tree/c/other", 1).await
}

#[tokio::test]
async fn bounded_follow_delivers_exactly_the_read_range_and_closes() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let client = local(&provider)?;
    for (index, (from, limit)) in [(0, 5), (1, 3), (2, 100), (4, 1)].into_iter().enumerate() {
        let path = format!("events/{index}");
        append(&provider, &path, 5).await?;
        let expected = sequences(provider.read(read(&path, from, limit)?).await?).await?;
        assert_eq!(
            expected,
            (from..(from + u64::from(limit)).min(5)).collect::<Vec<_>>()
        );
        let session = client.read(read(&path, from, limit)?).await?;
        // Appends after open never extend the range: it ends at the tail at open.
        append(&provider, &path, 1).await?;
        assert_eq!(sequences(session).await?, expected);
    }
    // Errors resolve exactly as the provider's own read.
    for request in [
        read("events/0", 99, 1)?,
        read("absent", 0, 1)?,
        read("events/0", 0, 0)?,
    ] {
        assert_eq!(
            client.read(request.clone()).await.err(),
            provider.read(request).await.err()
        );
    }
    assert_eq!(client.active.load(super::super::Ordering::Acquire), 0);
    Ok(())
}

#[tokio::test]
async fn bounded_follow_at_or_past_its_end_closes_without_data() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 5).await?;
    let client = local(&provider)?;
    // until <= from, and a range that starts at the tail.
    for (from, until) in [(3, 2), (3, 3), (5, 9)] {
        let mut data = Box::pin(
            client
                .follow_data_with_ack(
                    bounded("events", until),
                    from,
                    true,
                    crate::FollowFilter::default(),
                )
                .await?,
        );
        let end = tokio::time::timeout(Duration::from_secs(1), data.next()).await?;
        assert!(end.is_none(), "{from}..{until} delivered data");
    }
    assert_eq!(
        sequences(client.read(read("events", 5, 1)?).await?).await?,
        Vec::<u64>::new()
    );
    // A bound is meaningful only for an ALL follow of a stream.
    let mut latest = bounded("events", 4);
    latest.delivery = wire::SessionDelivery::Latest as i32;
    assert!(matches!(
        wire_codec::session::follow_bound(&latest),
        Err(StreamError::InvalidArgument)
    ));
    assert_eq!(
        wire_codec::session::follow_bound(&bounded("events", 0))?,
        None
    );
    assert_eq!(client.active.load(super::super::Ordering::Acquire), 0);
    Ok(())
}

#[tokio::test]
async fn session_children_page_equals_the_provider_page() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    tree(&provider).await?;
    let client = local(&provider)?;
    for limit in [1, 2, 10] {
        let direct = provider.children_page(page("tree", limit)?).await?;
        assert_eq!(client.children_page(page("tree", limit)?, 0).await?, direct);
        if let Some(after) = direct.next_after.clone() {
            let next = crate::ChildrenPageRequest {
                after: Some(after),
                hierarchy_version: Some(direct.hierarchy_version),
                ..page("tree", limit)?
            };
            assert_eq!(
                client.children_page(next.clone(), 0).await?,
                provider.children_page(next).await?
            );
        }
    }
    let full = client.children_page(page("tree", 10)?, 0).await?;
    assert_eq!(full.children.len(), 3);
    assert!(
        full.children
            .iter()
            .take(2)
            .all(|child| child.tail_record.is_some())
    );
    assert_eq!(
        client.children_page(page("tree", 0)?, 0).await.err(),
        provider.children_page(page("tree", 0)?).await.err()
    );
    Ok(())
}

#[tokio::test]
async fn session_heads_keeps_named_absence_and_children_on_the_same_cut() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    tree(&provider).await?;
    let client = local(&provider)?;
    let request = crate::HeadsRequest {
        durable_through: None,
        projection: Default::default(),
        paths: [
            StreamPath::new("tree/a/status")?,
            StreamPath::new("tree/absent")?,
        ]
        .into_iter()
        .collect(),
        children: Some(page("tree", 10)?),
        limit: 4,
        at_least: Default::default(),
    };
    let expected = provider.heads(request.clone()).await?;
    let actual = client.heads(request.clone(), 0).await?;
    assert_eq!(actual, expected);
    assert!(actual.heads[1].is_none());
    let mut offsets = request.clone();
    offsets.projection = crate::HeadProjection::Watermarks;
    assert_eq!(
        client.heads(offsets.clone(), 0).await?,
        provider.heads(offsets).await?
    );
    assert_eq!(
        actual
            .children
            .as_ref()
            .ok_or("missing page")?
            .children
            .len(),
        2
    );
    // Even one named denial refuses the whole view; absence cannot hide it.
    use acyclic_auth::contract::{ResourceGrant, ResourcePattern};
    client.register_credential(
        1,
        crate::session::testing::credential_with_grants(vec![
            ResourceGrant {
                resource: ResourcePattern {
                    name: "streams:/tree".into(),
                    prefix: true,
                },
                actions: vec!["streams/list".into(), "streams/follow".into()],
            },
            ResourceGrant {
                resource: ResourcePattern {
                    name: "streams:/tree/a/status".into(),
                    prefix: false,
                },
                actions: vec!["streams/read".into()],
            },
        ]),
    )?;
    let mut allowed = request.clone();
    allowed.paths.pop();
    assert!(client.heads(allowed, 1).await?.heads[0].is_some());
    assert_eq!(
        client.heads(request, 1).await,
        Err(StreamError::AccessDenied)
    );
    Ok(())
}

#[tokio::test]
async fn remote_provider_reads_and_pages_never_use_unary_rpcs() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 4).await?;
    tree(&provider).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = fixture.client()?;
    assert_eq!(
        sequences(StreamProvider::read(&client, read("events", 1, 2)?).await?).await?,
        vec![1, 2]
    );
    assert_eq!(
        crate::StreamProviderExt::children_page(&client, page("tree", 10)?).await?,
        provider.children_page(page("tree", 10)?).await?
    );
    let view = crate::HeadsRequest {
        durable_through: None,
        projection: Default::default(),
        paths: [StreamPath::new("tree/a/status")?].into(),
        children: Some(page("tree", 10)?),
        limit: 3,
        at_least: Default::default(),
    };
    assert_eq!(
        StreamProvider::heads(&client, view.clone()).await?,
        provider.heads(view).await?
    );
    assert_eq!(fixture.counts.unary.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.follows.load(Ordering::Relaxed), 1);
    for request in [read("absent", 0, 1)?, read("events", 5, 1)?] {
        assert_eq!(
            StreamProvider::read(&client, request.clone()).await.err(),
            provider.read(request).await.err()
        );
    }
    assert!(
        sequences(StreamProvider::read(&client, read("events", 4, 10)?).await?)
            .await?
            .is_empty()
    );
    assert_eq!(fixture.counts.unary.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn partially_delivered_read_fails_instead_of_widening_after_reconnect() -> TestResult {
    interrupted_read(false).await
}

#[tokio::test]
async fn partially_delivered_read_fails_instead_of_widening_after_move() -> TestResult {
    interrupted_read(true).await
}

async fn interrupted_read(moved: bool) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 5).await?;
    append(&provider, "healthy", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
    let client = fixture.client()?;
    let mut healthy = client
        .follow(crate::FollowRequest::new(StreamPath::new("healthy")?, 1))
        .await
        .map(crate::records_only)?;
    let mut records = StreamProvider::read(&client, read("events", 0, 10)?).await?;
    assert_eq!(next(&mut records).await?.sequence, 0);
    assert_eq!(next(&mut records).await?.sequence, 1);
    // Two credited records remain buffered. The fifth cannot be sent yet,
    // so this interruption necessarily precedes the finite read's completion.
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.until(|counts| counts.sent.load(Ordering::Relaxed) == 4),
    )
    .await?;
    append(&provider, "events", 5).await?;
    if moved {
        fixture.controls.send(Ok(frame(
            3,
            1,
            Body::Moved(wire::SessionMoved {
                resume: None,
                node: Some(wire::SessionNode {
                    node_id: "node".into(),
                    endpoint: String::new(),
                    placement_epoch: 1,
                }),
            }),
        )))?;
    } else {
        fixture.reset()?;
    }
    let mut refused = false;
    while let Some(record) = tokio::time::timeout(Duration::from_secs(1), records.next()).await? {
        match record {
            Ok(record) => assert!(record.sequence < 5, "read crossed its original snapshot"),
            Err(error) => {
                assert_eq!(error, StreamError::Unavailable);
                refused = true;
                break;
            }
        }
    }
    assert!(
        refused,
        "a partially delivered finite read must fail on disconnect"
    );
    assert!(records.next().await.is_none());
    // A live follow on the same physical connection still recovers, and a
    // fresh read can explicitly choose the new snapshot.
    append(&provider, "healthy", 1).await?;
    assert_eq!(next(&mut healthy).await?.sequence, 1);
    assert_eq!(
        sequences(StreamProvider::read(&client, read("events", 0, 10)?).await?).await?,
        (0..10).collect::<Vec<_>>()
    );
    assert_eq!(fixture.counts.unary.load(Ordering::Relaxed), 0);
    assert_eq!(
        fixture.counts.follows.load(Ordering::Relaxed),
        if moved { 3 } else { 4 }
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_a_finite_read_releases_only_its_own_interest() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 10).await?;
    let client = local(&provider)?;
    let mut healthy = client
        .follow(crate::FollowRequest::new(StreamPath::new("events")?, 10))
        .await
        .map(crate::records_only)?;
    let records = client.read(read("events", 0, 10)?).await?;
    assert_eq!(client.active.load(super::super::Ordering::Acquire), 2);
    drop(records);
    assert_eq!(client.active.load(super::super::Ordering::Acquire), 1);
    append(&provider, "events", 1).await?;
    assert_eq!(next(&mut healthy).await?.sequence, 10);
    drop(healthy);
    assert_eq!(client.active.load(super::super::Ordering::Acquire), 0);
    Ok(())
}
