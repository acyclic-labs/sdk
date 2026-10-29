//! Inject failures at the real Streams operation-index lookup, after workspace
//! and generation resolution. These exercise the complete hosted RPC boundary.
use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope,
    ForkReceipt, ForkRequest, IdempotencyObservation, MemoryStream, ReadRequest, RecordStream,
    StreamError, StreamPath, StreamProvider,
};
use std::sync::Mutex;
use tonic::Code;

type TestResult = Result<(), Box<dyn std::error::Error>>;
type Service = FilesystemWireService<
    crate::StreamAuthorityStore<FaultStream>,
    crate::LogicalObjectStore<acyclic_objects::MemoryObjects>,
>;

#[derive(Default)]
struct FaultStream {
    inner: MemoryStream,
    operation_error: Mutex<Option<StreamError>>,
    failed_paths: Mutex<Vec<StreamPath>>,
}

impl FaultStream {
    fn fail_operation(&self, error: StreamError) {
        *self.operation_error.lock().unwrap() = Some(error);
    }

    fn take_failure(&self, path: &StreamPath) -> Option<StreamError> {
        if path.as_str().starts_with("fs/operations/")
            && let Some(error) = self.operation_error.lock().unwrap().take()
        {
            self.failed_paths.lock().unwrap().push(path.clone());
            return Some(error);
        }
        None
    }
}

#[async_trait::async_trait]
impl StreamProvider for FaultStream {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        acyclic_stream::ProviderContext::new(self.inner.context().operation_claims())
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        for path in &request.paths {
            if let Some(error) = self.take_failure(path) {
                return Err(error);
            }
        }
        self.inner.heads(request).await
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }

    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> acyclic_stream::BoxProviderFuture<
        '_,
        Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>,
    > {
        self.inner.write(request)
    }

    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> Result<acyclic_stream::FollowStream, StreamError> {
        self.inner.follow(request).await
    }

    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

fn service() -> Result<(Service, Arc<FaultStream>), Status> {
    let stream = Arc::new(FaultStream::default());
    let objects = acyclic_objects::MemoryObjects::memory();
    let fs = Fs::new(
        crate::StreamAuthorityStore::new(stream.clone()),
        crate::LogicalObjectStore::new(Arc::new(objects)),
        crate::EmbeddedCapabilities::MEMORY,
    );
    Ok((
        FilesystemWireService::new(fs, FilesystemWireLimits::default())?,
        stream,
    ))
}

async fn workspace(service: &Service) -> Result<wire::Workspace, Box<dyn std::error::Error>> {
    Ok(service
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "main".into(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: operation(1),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing created workspace")?)
}

async fn observe_failure(error: StreamError, expected: Code) -> TestResult {
    let (service, stream) = service()?;
    let created = workspace(&service).await?;
    let request = wire::ObserveRequest {
        workspace: created.workspace,
        operation_id: operation_bytes(1),
    };
    stream.fail_operation(error.clone());
    let failure = service
        .observe(Request::new(request.clone()))
        .await
        .unwrap_err();
    assert_eq!(failure.code(), expected, "{error:?}");
    assert!(failure.message().contains(&error.to_string()));
    assert_eq!(stream.failed_paths.lock().unwrap().len(), 1);
    let retry = service.observe(Request::new(request)).await?.into_inner();
    assert_eq!(retry.state, "completed");
    Ok(())
}

#[tokio::test]
async fn observe_stream_unavailable_stays_retryable() -> TestResult {
    observe_failure(StreamError::Unavailable, Code::Unavailable).await
}

#[tokio::test]
async fn observe_stream_deadline_stays_retryable() -> TestResult {
    observe_failure(StreamError::DeadlineElapsed, Code::DeadlineExceeded).await
}

#[tokio::test]
async fn observe_preserves_other_stream_categories() -> TestResult {
    for (error, code) in [
        (StreamError::InvalidPath, Code::InvalidArgument),
        (StreamError::InvalidArgument, Code::InvalidArgument),
        (StreamError::LimitExceeded, Code::InvalidArgument),
        (StreamError::AlreadyExists, Code::AlreadyExists),
        (StreamError::PrefixNotRetained, Code::FailedPrecondition),
        (StreamError::HierarchyChanged, Code::FailedPrecondition),
        (StreamError::IdempotencyMismatch, Code::FailedPrecondition),
        (StreamError::OutOfRange, Code::OutOfRange),
        (StreamError::Capacity, Code::ResourceExhausted),
        (StreamError::AccessDenied, Code::PermissionDenied),
        (StreamError::Unsupported, Code::Unimplemented),
    ] {
        observe_failure(error, code).await?;
    }
    Ok(())
}

async fn join_failure(error: StreamError, expected: Code) -> TestResult {
    let (service, stream) = service()?;
    let created = workspace(&service).await?;
    let source = created.head.ok_or("missing source generation")?;
    let forked = service
        .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
            selection: None,
            source: Some(source.clone()),
            destination_name: "child".into(),
            operation: operation(2),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing forked workspace")?;
    let plan = service
        .plan_join(Request::new(wire::PlanJoinRequest {
            source: Some(source),
            target: forked.head,
            maximum_changes: 32,
            maximum_conflicts: 8,
            maximum_generations: 32,
            history: wire::JoinHistory::Merge as i32,
        }))
        .await?
        .into_inner();
    let request = wire::ApplyJoinRequest {
        plan: Some(plan),
        operation: operation(3),
    };
    // Only the operation-index tail fails; resolving source/target generations
    // above and inside ApplyJoin succeeds before join_target_head is reached.
    stream.fail_operation(error.clone());
    let failure = service
        .apply_join(Request::new(request.clone()))
        .await
        .unwrap_err();
    assert_eq!(failure.code(), expected, "{error:?}");
    assert!(failure.message().contains(&error.to_string()));
    assert_eq!(stream.failed_paths.lock().unwrap().len(), 1);
    let retry = service
        .apply_join(Request::new(request))
        .await?
        .into_inner();
    assert_eq!(retry.status, wire::JoinStatus::NoChanges as i32);
    Ok(())
}

#[tokio::test]
async fn apply_join_stream_unavailable_stays_retryable() -> TestResult {
    join_failure(StreamError::Unavailable, Code::Unavailable).await
}

#[tokio::test]
async fn apply_join_stream_deadline_stays_retryable() -> TestResult {
    join_failure(StreamError::DeadlineElapsed, Code::DeadlineExceeded).await
}
