//! Abstract, monotone durable prefix over the real modeled MemoryStream.
use acyclic_stream::*;
use loom::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

pub(super) struct Monitor {
    pub(super) prefix: Arc<AtomicU64>,
    pub(super) required: u64,
    pub(super) premature: Mutex<bool>,
}
impl Monitor {
    fn record(&self) {
        // The immutable observation is taken at provider commit admission,
        // before a later prefix advance can hide an early call.
        let early = self.prefix.load(Ordering::SeqCst) < self.required;
        *self.premature.lock().unwrap() |= early;
    }
}

pub(super) struct Provider {
    pub(super) inner: MemoryStream,
    pub(super) cap: Option<Arc<AtomicU64>>,
    // Installed once by the sole setup actor, before the publisher is spawned.
    // This is an observation sink, never an application synchronization path.
    pub(super) monitor: OnceLock<Arc<Monitor>>,
}
impl Provider {
    pub(super) fn new(cap: Option<Arc<AtomicU64>>) -> Self {
        Self {
            inner: MemoryStream::default(),
            cap,
            monitor: OnceLock::new(),
        }
    }
}

#[async_trait::async_trait]
impl StreamProvider for Provider {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }

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
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        let accepted = self.inner.tail(path).await?;
        let durable = self
            .cap
            .as_ref()
            .map_or(accepted, |cap| accepted.min(cap.load(Ordering::SeqCst)));
        Ok(StreamWatermarks { accepted, durable })
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: acyclic_stream::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.inner.follow(path, from, filter).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
        filter: acyclic_stream::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.inner
            .follow_checked(path, from, preceding, filter)
            .await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        if let Some(monitor) = self.monitor.get() {
            monitor.record();
        }
        // Lock order: the observer mutex is released before I/O. The wrapper
        // owns no protocol lock; TaskVolume releases its index before provider
        // I/O and MemoryStream uses its actual crate-local mutex. A model that
        // accidentally nested those real locks would be explored by Loom.
        self.inner.commit(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}
