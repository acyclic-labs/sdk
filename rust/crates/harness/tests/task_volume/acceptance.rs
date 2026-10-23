use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommitOutcome,
    CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest, IdempotencyKey,
    IdempotencyObservation, RecordStream, StreamBounds, StreamError,
};

type StreamResult<T> = std::result::Result<T, StreamError>;

struct AckGate {
    inner: MemoryStream,
    accepted: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pending: std::sync::atomic::AtomicBool,
    settled: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl StreamProvider for AckGate {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> StreamResult<Option<IdempotencyObservation>> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> StreamResult<u64> {
        let settled = self.settled.notified();
        if self.pending.load(std::sync::atomic::Ordering::SeqCst) {
            settled.await;
        }
        self.inner.tail(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> StreamResult<acyclic_stream::StreamWatermarks> {
        self.inner.watermarks(path).await
    }
    async fn bounds(&self, path: StreamPath) -> StreamResult<StreamBounds> {
        self.inner.bounds(path).await
    }
    async fn append(&self, request: AppendRequest) -> StreamResult<AppendOutcome> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> StreamResult<ForkReceipt> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> StreamResult<RecordStream> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: acyclic_stream::FollowFilter,
    ) -> StreamResult<RecordStream> {
        self.inner.follow(path, from, filter).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
        filter: acyclic_stream::FollowFilter,
    ) -> StreamResult<RecordStream> {
        self.inner
            .follow_checked(path, from, preceding, filter)
            .await
    }
    async fn children(&self, request: ChildrenRequest) -> StreamResult<ChildStream> {
        self.inner.children(request).await
    }
    async fn read_commit(&self, id: CommitId) -> StreamResult<CommittedEnvelope> {
        self.inner.read_commit(id).await
    }
    async fn commit(&self, request: CommitRequest) -> StreamResult<CommitOutcome> {
        let outcome = self.inner.commit(request).await?;
        self.pending
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let signal = self
            .accepted
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .take();
        if let Some(signal) = signal {
            signal.send(()).map_err(|_| StreamError::Unavailable)?;
        }
        let release = self.release.lock().await.take();
        if let Some(release) = release {
            release.await.map_err(|_| StreamError::Unavailable)?;
        }
        self.pending
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.settled.notify_waiters();
        Ok(outcome)
    }
}

#[tokio::test]
async fn materialized_heads_do_not_wait_for_a_pending_append_acknowledgement() -> Result {
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let provider = Arc::new(AckGate {
        inner: MemoryStream::default(),
        accepted: std::sync::Mutex::new(Some(accepted_tx)),
        release: tokio::sync::Mutex::new(Some(release_rx)),
        pending: Default::default(),
        settled: Default::default(),
    });
    let volume = Arc::new(TaskVolume::open(provider, StreamPath::new("task-events")?).await?);
    let initial = volume.snapshot(&Query::Root, 0).await?;
    let mut follow = volume.follow(Query::Root, initial.cursor).await?;
    let appending = tokio::spawn({
        let volume = volume.clone();
        async move {
            volume
                .append(
                    initial.generation,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"active")),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), accepted_rx).await??;
    let head =
        tokio::time::timeout(Duration::from_secs(1), volume.heads(&Query::Root, None, 1)).await??;
    assert_eq!(
        head.files
            .first()
            .ok_or("missing accepted head")?
            .segments
            .concat(),
        b"active"
    );
    assert!(!appending.is_finished());
    let change = tokio::time::timeout(Duration::from_secs(1), follow.next())
        .await?
        .ok_or("follow ended")??;
    assert_eq!(change.file, Some(write("GOAL.md", "active")?));
    release_tx
        .send(())
        .map_err(|_| "append stopped before acknowledgement")?;
    assert_eq!(appending.await??, head.generation);
    Ok(())
}

#[tokio::test]
async fn reset_and_other_heads_stay_live_while_the_durable_tail_is_blocked() -> Result {
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let provider = Arc::new(AckGate {
        inner: MemoryStream::default(),
        accepted: std::sync::Mutex::new(Some(accepted_tx)),
        release: tokio::sync::Mutex::new(Some(release_rx)),
        pending: Default::default(),
        settled: Default::default(),
    });
    let path = StreamPath::new("task-events")?;
    let volume = Arc::new(TaskVolume::open(provider.clone(), path.clone()).await?);
    let initial = volume.snapshot(&Query::Root, 0).await?;
    let mut invalid = initial.cursor;
    invalid.query[0] ^= 1;
    let appending = tokio::spawn({
        let volume = volume.clone();
        async move {
            volume
                .append(
                    initial.generation,
                    &Write::new(FileKey::Goal, OwnedBytes::from_static(b"accepted")),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), accepted_rx).await??;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), provider.tail(path))
            .await
            .is_err()
    );
    let (reset, head) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            volume.observe(Query::Root, Some(invalid), 1),
            volume.heads(&Query::Root, None, 1)
        )
    })
    .await?;
    let reset = reset?;
    assert!(reset.reset);
    assert_eq!(
        reset.snapshot.ok_or("missing reset snapshot")?.files,
        vec![write("GOAL.md", "accepted")?]
    );
    assert_eq!(
        head?.files.first().ok_or("missing head")?.segments.concat(),
        b"accepted"
    );
    assert!(!appending.is_finished());
    release_tx
        .send(())
        .map_err(|_| "append stopped before acknowledgement")?;
    appending.await??;
    Ok(())
}
