//! Scripted durability around the real memory provider; no extra records on save.
use acyclic_stream::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
pub(super) struct Provider {
    memory: MemoryStream,
    pub(super) saved: AtomicBool,
    pub(super) outbox_saved: AtomicU64,
}
impl Provider {
    fn durable(&self, path: &StreamPath, accepted: u64) -> u64 {
        if path.as_str() == "remote-outbox/files" {
            accepted.min(self.outbox_saved.load(Ordering::Acquire))
        } else if self.saved.load(Ordering::Acquire) {
            accepted
        } else {
            0
        }
    }
}
#[async_trait::async_trait]
impl StreamProvider for Provider {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        acyclic_stream::ProviderContext::new(self.memory.context().operation_claims())
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.memory.inspect_idempotency(key).await
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        let projection = request.projection;
        let paths = request.paths.clone();
        let mut view = self.memory.heads(request).await?;
        for (path, head) in paths.iter().zip(&mut view.heads) {
            if let Some(head) = head {
                head.durable = if projection == HeadProjection::Tail {
                    0
                } else {
                    self.durable(path, head.tail)
                };
            }
        }
        Ok(view)
    }
    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> acyclic_stream::BoxProviderFuture<
        '_,
        std::result::Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>,
    > {
        self.memory.write(request)
    }

    async fn read(&self, mut request: ReadRequest) -> Result<RecordStream, StreamError> {
        if request.path.as_str() == "browser/overlap" && request.from > 0 {
            request.from -= 1;
            request.limit += 1;
        }
        self.memory.read(request).await
    }
    async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
        self.memory.follow(request).await
    }

    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.memory.read_commit(id).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::NativeStreamProvider for Provider {}
