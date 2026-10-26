use super::*;
use crate::workflow::{DurableWorkflowHost, WorkflowJournal};
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommitMutation,
    CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest,
    IdempotencyObservation, OperationClaims, ReadRequest, RecordStream, StreamBounds, StreamError,
    StreamPath, StreamProvider,
};
use std::sync::Mutex;

type Interleaving = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// A memory stream that runs one interleaving just before the first commit
/// appending to a workflow, as another task acting at that moment would.
#[derive(Default)]
struct Interleaved {
    stream: MemoryStream,
    before_workflow_commit: Mutex<Option<Interleaving>>,
    ran: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl StreamProvider for Interleaved {
    fn operation_claims(&self) -> &OperationClaims {
        self.stream.operation_claims()
    }
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        self.stream.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> std::result::Result<u64, StreamError> {
        self.stream.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
        self.stream.bounds(path).await
    }
    async fn append(
        &self,
        request: AppendRequest,
    ) -> std::result::Result<AppendOutcome, StreamError> {
        self.stream.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> std::result::Result<ForkReceipt, StreamError> {
        self.stream.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        self.stream.read(request).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: acyclic_stream::FollowFilter,
    ) -> std::result::Result<RecordStream, StreamError> {
        self.stream.follow(path, from, filter).await
    }
    async fn children(
        &self,
        request: ChildrenRequest,
    ) -> std::result::Result<ChildStream, StreamError> {
        self.stream.children(request).await
    }
    async fn commit(
        &self,
        request: CommitRequest,
    ) -> std::result::Result<CommitOutcome, StreamError> {
        let workflow = request.mutations.iter().any(|mutation| {
            matches!(mutation, CommitMutation::Append { path, .. }
                if path.as_str().starts_with("harness/v2/workflows/"))
        });
        let interleaving = workflow
            .then(|| {
                self.before_workflow_commit
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
            })
            .flatten();
        if let Some(interleaving) = interleaving {
            interleaving.await;
            self.ran.store(true, std::sync::atomic::Ordering::Release);
        }
        self.stream.commit(request).await
    }
    async fn read_commit(
        &self,
        id: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        self.stream.read_commit(id).await
    }
}

/// Another task's admission lands on the coordinator between a workflow
/// commit's ownership check and its append. The task still owns its
/// workflow, whose checkpoint is unchanged, so the transition commits.
#[tokio::test]
async fn another_tasks_event_does_not_conflict_a_workflow_transition() -> Result<()> {
    let stream = Arc::new(Interleaved::default());
    let f = Fixture::new(
        Arc::clone(&stream),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let owner = f.running(1).await?;
    let admission = owner.workflow_admission().clone();
    let workflow = f.workflow(owner)?;
    workflow.admit(admission.clone()).await?;
    let (host, other) = (Arc::clone(&f.host), f.admission(2, json!(0)));
    *stream
        .before_workflow_commit
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::pin(async move {
        host.admit(other).await.unwrap();
    }));
    DurableWorkflowHost::open(f.host.machines.clone(), admission.initial, workflow.clone())
        .await?
        .step(
            OperationId::from_bytes([2; 16]),
            IdempotencyKey::new("step")?,
            json!(0),
            |_| Ok(()),
        )
        .await?;
    assert!(stream.ran.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(workflow.replay(0, 1, None)?.await?.0.len(), 1);
    Ok(())
}
