
use super::*;
use futures::StreamExt as _;

struct Gapped {
    inner: MemoryStream,
    missing: u64,
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StreamProvider for Gapped {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        let missing = self.missing;
        let records = self.inner.read(request).await?;
        Ok(records
            .filter(move |record| {
                std::future::ready(!matches!(record, Ok(record) if record.sequence == missing))
            })
            .boxed())
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.inner.commit(request).await
    }
    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit_id).await
    }
}

async fn filled<P: StreamProvider>(provider: P, records: u64) -> Stream<P> {
    let stream = crate::Stream::new(Arc::new(provider), "replay")
        .unwrap();
    let mut next = 0;
    while next < records {
        let batch = (next..records.min(next + 500))
            .map(|sequence| OwnedBytes::from(sequence.to_be_bytes().to_vec()))
            .collect::<Vec<_>>();
        next += batch.len() as u64;
        stream.append_batch(batch, None, None).await.unwrap();
    }
    stream
}

async fn drain<P: StreamProvider>(replay: &mut Replay<P>) -> Result<Vec<u64>, StreamError> {
    let mut sequences = Vec::new();
    while let Some(page) = replay.next_page().await? {
        sequences.extend(page.into_iter().map(|record| record.sequence));
    }
    Ok(sequences)
}

#[tokio::test]
async fn missing_stream_replays_as_empty() {
    let stream = crate::Stream::new(Arc::new(MemoryStream::new(MemoryLimits::default())), "absent")
        .unwrap();
    let mut replay = stream.replay(0);
    assert!(drain(&mut replay).await.unwrap().is_empty());
    assert_eq!(replay.cursor(), 0);
    assert!(replay.next_page().await.unwrap().is_none());
}

#[tokio::test]
async fn replay_crosses_pages_in_order_from_any_cursor() {
    let stream = filled(MemoryStream::new(MemoryLimits::default()), 2_500).await;
    let mut replay = stream.replay(0);
    assert_eq!(
        drain(&mut replay).await.unwrap(),
        (0..2_500).collect::<Vec<_>>()
    );
    assert_eq!(replay.cursor(), 2_500);
    let mut replay = stream.replay(1_500);
    assert_eq!(
        drain(&mut replay).await.unwrap(),
        (1_500..2_500).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn gaps_within_and_between_pages_fail_closed() {
    for missing in [7, u64::from(REPLAY_PAGE)] {
        let inner = MemoryStream::new(MemoryLimits::default());
        let stream = filled(Gapped { inner, missing }, 2_100).await;
        let mut replay = stream.replay(0);
        assert_eq!(drain(&mut replay).await, Err(StreamError::Unavailable));
    }
}
