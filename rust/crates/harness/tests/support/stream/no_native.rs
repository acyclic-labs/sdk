use super::{AppendOutcome, MemoryStream, StreamError, StreamProvider};

/// Ordinary-capable negative control: it deliberately inherits every real SDK native capability default, rather than simulating unsupported results.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[derive(Default)]
pub struct NoNative {
    inner: MemoryStream,
    pub ordinary_calls: std::sync::atomic::AtomicU64,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[async_trait::async_trait]
impl StreamProvider for NoNative {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.tail(path).await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<AppendOutcome, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.append(request).await
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.fork(request).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.children(request).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.commit(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.ordinary_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.read_commit(id).await
    }
}
