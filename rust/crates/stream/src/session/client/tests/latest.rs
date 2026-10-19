use super::*;
use crate::{
    AppendOutcome, ChildStream, ChildrenRequest, CommitId, CommitOutcome, CommitRequest,
    CommittedEnvelope, ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation,
    ReadRequest, StreamBounds,
};

fn subscription(path: &str, latest: bool) -> wire::SessionFollow {
    wire::SessionFollow {
        path: path.into(),
        delivery: if latest {
            wire::SessionDelivery::Latest
        } else {
            wire::SessionDelivery::All
        } as i32,
        ..Default::default()
    }
}
fn value(sequence: u64, attention: bool) -> wire::SessionLatestValue {
    wire::SessionLatestValue {
        key: Bytes::from_static(b"agent"),
        value: sequence.to_le_bytes().to_vec().into(),
        attention,
    }
}
async fn status(memory: &MemoryStream, sequence: u64, attention: bool) -> Result<(), StreamError> {
    memory
        .append(AppendRequest {
            path: StreamPath::new("status")?,
            records: vec![value(sequence, attention).encode_to_vec().into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    Ok(())
}
async fn delivery(
    data: &mut BoxStream<'static, Result<wire::SessionData, StreamError>>,
) -> Result<wire::SessionData, Box<dyn std::error::Error + Send + Sync>> {
    Ok(data.next().await.ok_or("data follow ended")??)
}
fn assert_status(data: &wire::SessionData, sequence: u64, attention: bool) {
    assert_eq!(
        data.item,
        Some(Item::LatestValue(value(sequence, attention)))
    );
    assert_eq!(data.after, Some(cursor(sequence + 1)));
    assert_eq!(data.accepted, sequence + 1);
    assert_eq!(data.durable, 0);
}
struct Counted {
    memory: Arc<MemoryStream>,
    pulled: AtomicUsize,
    changed: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl StreamProvider for Arc<Counted> {
    fn operation_claims(&self) -> &crate::OperationClaims {
        self.memory.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.memory.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.memory.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.memory.bounds(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.memory.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.memory.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.memory.read(request).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.memory.children(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.memory.commit(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.memory.read_commit(id).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let status = path.as_str() == "status";
        let counted = Arc::clone(self);
        Ok(self
            .memory
            .follow(path, from)
            .await?
            .inspect(move |_| {
                if status {
                    counted.pulled.fetch_add(1, Ordering::Relaxed);
                    counted.changed.notify_one();
                }
            })
            .boxed())
    }
}

#[tokio::test]
async fn saturated_latest_conflates_only_ordinary_values_and_shares_healthy_all() -> TestResult {
    let memory = Arc::new(MemoryStream::default());
    status(&memory, 0, false).await?;
    append(&memory, "events", 1).await?;
    let counted = Arc::new(Counted {
        memory: Arc::clone(&memory),
        pulled: AtomicUsize::new(0),
        changed: tokio::sync::Notify::new(),
    });
    let fixture = Fixture::open(Arc::new(Arc::clone(&counted)), "node").await?;
    let client = fixture.client()?;
    let mut overview = client.follow_data(subscription("status", true), 1).await?;
    let mut full = client.follow(StreamPath::new("events")?, 1).await?;
    for (sent, sequence) in (1..=2).enumerate() {
        status(&memory, sequence, false).await?;
        fixture
            .until(|counts| counts.sent.load(Ordering::Relaxed) == sent + 1)
            .await;
    }
    for sequence in 3..=9 {
        status(&memory, sequence, sequence == 8).await?;
    }
    loop {
        let changed = counted.changed.notified();
        if counted.pulled.load(Ordering::Relaxed) == 9 {
            break;
        }
        changed.await;
    }
    // The actual per-channel frame grant is exhausted before testing a sibling.
    let traffic = fixture
        .counts
        .traffic
        .lock()
        .map_err(|_| "traffic poisoned")?
        .get(&(1, 1, 1))
        .cloned()
        .ok_or("missing traffic")?;
    assert_eq!(traffic.granted_frames, 2);
    assert_eq!(traffic.positions, [2, 3]);
    append(&memory, "events", 1).await?;
    assert_eq!(next(&mut full).await?.sequence, 1);
    for sequence in [1, 2, 8, 9] {
        assert_status(&delivery(&mut overview).await?, sequence, sequence == 8);
    }
    assert!(super::tenk::quiescent(std::slice::from_mut(&mut overview)).await);
    assert_eq!(counted.pulled.load(Ordering::Relaxed), 9);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn canonical_all_data_preserves_the_exact_cursor_and_both_watermarks() -> TestResult {
    let memory = Arc::new(MemoryStream::default());
    append(&memory, "events", 1).await?;
    let fixture = Fixture::open(memory, "node").await?;
    let client = fixture.client()?.without_delivery_acknowledgements();
    let mut raw = client.follow_data(subscription("events", false), 1).await?;
    let expected = wire::SessionData {
        after: Some(cursor(2)),
        accepted: 7,
        durable: 6,
        item: Some(Item::Record(wire::Record {
            sequence: 1,
            value: Bytes::from_static(b"accepted"),
            commit_id: vec![7; 32].into(),
            committed_at_micros: 123,
        })),
    };
    fixture
        .controls
        .send(Ok(frame(1, 1, Body::Data(expected.clone()))))?;
    assert_eq!(delivery(&mut raw).await?, expected);
    Ok(())
}

#[tokio::test]
async fn invalid_follow_modes_and_checked_latest_are_rejected_before_session_rpc() -> TestResult {
    let fixture = Fixture::open(Arc::new(MemoryStream::default()), "node").await?;
    let client = fixture.client()?;
    for follow in [
        wire::SessionFollow::default(),
        wire::SessionFollow {
            delivery: 99,
            ..subscription("status", true)
        },
        wire::SessionFollow {
            path: "../bad".into(),
            ..subscription("status", true)
        },
        wire::SessionFollow {
            preceding_commit_id: vec![7; 32].into(),
            ..subscription("status", true)
        },
    ] {
        assert!(client.follow_data(follow, 1).await.is_err());
    }
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn latest_moves_and_reconnects_resume_consumed_cursors_and_keep_attention() -> TestResult {
    let memory = Arc::new(MemoryStream::default());
    status(&memory, 0, false).await?;
    let old = Fixture::open(Arc::clone(&memory), "old").await?;
    let new = Fixture::open(Arc::clone(&memory), "new").await?;
    let target = new._channel.clone();
    let connect: Connect = std::sync::Arc::new(move |node| {
        assert_eq!(node.node_id, "new");
        Box::pin(future::ready(Ok(target.clone())))
    });
    let client = Client::with_connector(
        old._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        connect,
    );
    let mut raw = client.follow_data(subscription("status", true), 1).await?;
    status(&memory, 1, false).await?;
    assert_status(&delivery(&mut raw).await?, 1, false);
    for sequence in 2..=4 {
        status(&memory, sequence, sequence == 3).await?;
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
    new.until(|counts| {
        counts
            .resumes
            .lock()
            .is_ok_and(|resumes| resumes.contains(&(1, 2, 2)))
    })
    .await;
    let mut cut = 2;
    let mut attention = Vec::new();
    while cut < 5 {
        let data = delivery(&mut raw).await?;
        let after = data.after.as_ref().ok_or("missing cursor")?.next;
        assert!(after > cut && after <= 5);
        if let Some(Item::LatestValue(value)) = data.item
            && value.attention
        {
            attention.push(after);
        }
        cut = after;
    }
    assert_eq!(attention, [4]);
    assert!(super::tenk::quiescent(std::slice::from_mut(&mut raw)).await);
    new.controls
        .send(Err(Status::unavailable("fixture reconnect")))?;
    new.until(|counts| {
        counts
            .resumes
            .lock()
            .is_ok_and(|resumes| resumes.contains(&(1, 3, 5)))
    })
    .await;
    status(&memory, 5, true).await?;
    assert_status(&delivery(&mut raw).await?, 5, true);
    assert_eq!(old.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(new.counts.sessions.load(Ordering::Relaxed), 2);
    assert_eq!(new.counts.connections.load(Ordering::Relaxed), 1);
    Ok(())
}
