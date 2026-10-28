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
    async fn head(&self, path: StreamPath) -> Result<PathHead, StreamError> {
        let mut head = self.memory.head(path.clone()).await?;
        head.durable = self.durable(&path, head.tail);
        Ok(head)
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.memory.tail(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        let accepted = self.memory.tail(path.clone()).await?;
        Ok(StreamWatermarks {
            accepted,
            durable: self.durable(&path, accepted),
        })
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.memory.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.memory.fork(request).await
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

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.memory.children_page(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.memory.commit(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.memory.read_commit(id).await
    }
}
