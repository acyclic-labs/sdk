//! Publication authority, observed at the provider: which owner's settlement
//! commits actually landed. The model's authorizer names its owner with an
//! always-true `Absent` witness on every settlement grant.
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenPage, ChildrenPageRequest, ChildrenRequest,
    CommitCondition, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt,
    ForkRequest, IdempotencyKey, IdempotencyObservation, MemoryStream, ReadRequest, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, StreamWatermarks,
};
use loom::sync::atomic::{AtomicUsize, Ordering};

type Storage<T> = Result<T, StreamError>;

pub(super) fn witness(owner: u64) -> CommitCondition {
    CommitCondition::Absent {
        path: StreamPath::new(format!("settlement-{owner}")).unwrap(),
    }
}

#[derive(Default)]
pub(super) struct Audited {
    inner: MemoryStream,
    /// Landed settlement commits granted to the replaced owner (owner 1).
    replaced: AtomicUsize,
}

impl Audited {
    pub(super) fn replaced_settlements(&self) -> usize {
        self.replaced.load(Ordering::SeqCst)
    }

    async fn commit_with(
        &self,
        replaced: bool,
        commit: impl std::future::Future<Output = Storage<CommitOutcome>>,
    ) -> Storage<CommitOutcome> {
        let outcome = commit.await?;
        if replaced && matches!(outcome, CommitOutcome::Committed(_)) {
            self.replaced.fetch_add(1, Ordering::SeqCst);
        }
        Ok(outcome)
    }
}

fn replaced(request: &CommitRequest) -> bool {
    request.conditions.contains(&witness(1))
}

#[async_trait::async_trait]
impl StreamProvider for Audited {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Storage<Option<IdempotencyObservation>> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Storage<u64> {
        self.inner.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Storage<StreamBounds> {
        self.inner.bounds(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Storage<StreamWatermarks> {
        self.inner.watermarks(path).await
    }
    async fn append(&self, request: AppendRequest) -> Storage<AppendOutcome> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Storage<ForkReceipt> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Storage<RecordStream> {
        self.inner.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Storage<RecordStream> {
        self.inner.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Storage<RecordStream> {
        self.inner.follow_checked(path, from, preceding).await
    }
    async fn children(&self, request: ChildrenRequest) -> Storage<ChildStream> {
        self.inner.children(request).await
    }
    async fn children_page(&self, request: ChildrenPageRequest) -> Storage<ChildrenPage> {
        self.inner.children_page(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Storage<CommitOutcome> {
        self.commit_with(replaced(&request), self.inner.commit(request))
            .await
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Storage<CommitOutcome> {
        self.commit_with(
            replaced(&request),
            self.inner.commit_before(request, deadline_unix_millis),
        )
        .await
    }
    async fn read_commit(&self, id: CommitId) -> Storage<CommittedEnvelope> {
        self.inner.read_commit(id).await
    }
}
