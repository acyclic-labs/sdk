#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

//! Fault-window coverage for the production recursive local swarm.
//!
//! The provider below is only a deterministic model/fault adapter.  Fork
//! intent, seed publication, the registry, inherited filesystem grants, and
//! completion artifacts all use the production LocalStream/LocalFs
//! composition.  In particular, these tests never manufacture a child
//! session or replace the publication store with an in-memory test double.

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::AuthorityIssuer,
    filesystem::{
        FilesystemHost, LocalFilesystemForkResolver, LocalHarnessTools, LocalSessionPhase,
        LocalSwarmBindings, PersistentLocalHarness, PersistentLocalSwarm, WorkspaceMutation, workspace_ref,
    },
    fork::ForkSeed,
    model::{
        Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent, ModelProvider,
        ModelRequest,
    },
    resources::ProviderRef,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::{
    StreamExt as _,
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};
use tempfile::tempdir;
use tokio::time::{Duration, timeout};

const ROOT_FILE: &str = "fault fixture root file";

/// Fault locations are named after the durable boundary they surround.  The
/// provider can observe the model boundary; publication boundaries are
/// asserted through the real registry's seed/report state before a child
/// provider dispatch is allowed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum FaultWindow {
    None = 0,
    BeforeIntent = 1,
    AfterIntent = 2,
    // The production publisher exposes no callback between ForkPrepared and
    // seed append.  The first provider call is therefore the observable
    // after-seed boundary, and the tests assert both durable records before
    // exercising it.
    AfterPreparedSeed = 3,
    BeforeCompletion = 4,
}

impl FaultWindow {
    fn from_byte(value: u8) -> Self {
        match value {
            1 => Self::BeforeIntent,
            2 => Self::AfterIntent,
            3 => Self::AfterPreparedSeed,
            4 => Self::BeforeCompletion,
            _ => Self::None,
        }
    }
}

fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn task(operation: OperationId) -> TaskId {
    TaskId::from_bytes(operation.into_bytes())
}

fn model() -> Result<Model> {
    Model::new("mock", "local-swarm-faults", "1", json!({}))
}

fn message_contains(request: &ModelRequest, needle: &str) -> bool {
    request
        .messages
        .iter()
        .any(|message| match &message.content {
            ModelContent::Text(text) => text.contains(needle),
            ModelContent::Part(ModelContentPart::Text { text }) => text.contains(needle),
            ModelContent::Part(ModelContentPart::ToolResult { value, .. }) => {
                value.as_str().is_some_and(|text| text.contains(needle))
            }
            ModelContent::Part(ModelContentPart::ToolCall { arguments, .. }) => {
                arguments.to_string().contains(needle)
            }
            ModelContent::Part(ModelContentPart::File { .. }) => false,
            ModelContent::Parts(parts) => parts.iter().any(
                |part| matches!(part, ModelContentPart::Text { text } if text.contains(needle)),
            ),
        })
}

fn ordinary() -> Vec<ModelEvent> {
    vec![
        ModelEvent::Content {
            delta: "ordinary completion".into(),
        },
        ModelEvent::Completed {
            metadata: Value::Null,
        },
    ]
}

/// A model adapter with explicit provider disconnects.  The atomic fault
/// switches are changed only between independently opened production swarm
/// handles, which models a cold restart without mutating the registry.
struct ForkFaultProvider {
    requests: Mutex<Vec<ModelRequest>>,
    serialized_requests: Mutex<Vec<Vec<u8>>>,
    request_digests: Mutex<Vec<[u8; 32]>>,
    root_sent: AtomicBool,
    root_fault: AtomicU8,
    child_a_fault: AtomicU8,
    child_a_failed: AtomicBool,
    reconcile_completed: AtomicBool,
    child_a_blocked: AtomicBool,
    child_stream_dropped: Arc<AtomicBool>,
    child_stream_drop_notified: Arc<tokio::sync::Notify>,
    child_a_started: AtomicBool,
    child_a_dispatched: Arc<tokio::sync::Notify>,
    dispatches: AtomicUsize,
    child_a: OperationId,
    child_b: OperationId,
}

struct BlockedChildStreamGuard {
    dropped: Arc<AtomicBool>,
    notified: Arc<tokio::sync::Notify>,
}

impl Drop for BlockedChildStreamGuard {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        self.notified.notify_waiters();
    }
}

impl ForkFaultProvider {
    fn new(child_a: OperationId, child_b: OperationId) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            serialized_requests: Mutex::new(Vec::new()),
            request_digests: Mutex::new(Vec::new()),
            root_sent: AtomicBool::new(false),
            root_fault: AtomicU8::new(FaultWindow::None as u8),
            child_a_fault: AtomicU8::new(FaultWindow::None as u8),
            child_a_failed: AtomicBool::new(false),
            reconcile_completed: AtomicBool::new(false),
            child_a_blocked: AtomicBool::new(false),
            child_stream_dropped: Arc::new(AtomicBool::new(false)),
            child_stream_drop_notified: Arc::new(tokio::sync::Notify::new()),
            child_a_started: AtomicBool::new(false),
            child_a_dispatched: Arc::new(tokio::sync::Notify::new()),
            dispatches: AtomicUsize::new(0),
            child_a,
            child_b,
        })
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().expect("request lock").clone()
    }

    fn requests_matching(&self, needle: &str) -> Vec<ModelRequest> {
        self.requests()
            .into_iter()
            .filter(|request| message_contains(request, needle))
            .collect()
    }

    fn set_root_fault(&self, fault: FaultWindow) {
        self.root_fault.store(fault as u8, Ordering::SeqCst);
    }

    fn set_child_a_fault(&self, fault: FaultWindow) {
        self.child_a_fault.store(fault as u8, Ordering::SeqCst);
    }

    fn serialized(&self) -> Vec<Vec<u8>> {
        self.serialized_requests
            .lock()
            .expect("serialized request lock")
            .clone()
    }

    fn assert_request_digests(&self) {
        let bytes = self.serialized();
        let digests = self
            .request_digests
            .lock()
            .expect("request digest lock")
            .clone();
        assert_eq!(bytes.len(), digests.len());
        for (bytes, digest) in bytes.iter().zip(digests) {
            assert_eq!(*blake3::hash(bytes).as_bytes(), digest);
        }
    }

    fn tool_result_child_operations(request: &ModelRequest) -> Vec<String> {
        fn collect(content: &ModelContent, operations: &mut Vec<String>) {
            let parts = match content {
                ModelContent::Part(part) => std::slice::from_ref(part).to_vec(),
                ModelContent::Parts(parts) => parts.clone(),
                ModelContent::Text(_) => Vec::new(),
            };
            for part in parts {
                if let ModelContentPart::ToolResult { value, .. } = part
                    && let Some(operation) = value.get("child_operation").and_then(Value::as_str)
                {
                    operations.push(operation.to_owned());
                }
            }
        }

        let mut operations = Vec::new();
        for message in &request.messages {
            collect(&message.content, &mut operations);
        }
        operations
    }

    /// A completed recursive batch has one initial root request, one request
    /// for each selected child, and one fresh root continuation. The latter
    /// must carry both ordered tool results; counting prompt text alone would
    /// incorrectly count inherited child prefixes as root requests.
    fn assert_completed_dispatch_trace(&self, child_a: OperationId, child_b: OperationId) {
        let requests = self.requests();
        let digests = self
            .request_digests
            .lock()
            .expect("request digest lock")
            .clone();
        assert_eq!(requests.len(), 4, "unexpected provider request trace");
        assert_eq!(digests.len(), requests.len());

        let root_indices = requests
            .iter()
            .enumerate()
            .filter(|(_, request)| {
                !message_contains(request, "child task: child-a")
                    && !message_contains(request, "child task: child-b")
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        assert_eq!(root_indices.len(), 2);
        let initial_root = *root_indices
            .iter()
            .find(|index| Self::tool_result_child_operations(&requests[**index]).is_empty())
            .expect("initial root request");
        let continuation_root = *root_indices
            .iter()
            .find(|index| {
                Self::tool_result_child_operations(&requests[**index])
                    == vec![child_a.to_string(), child_b.to_string()]
            })
            .expect("root continuation request");
        assert_eq!(
            digests
                .iter()
                .filter(|digest| **digest == digests[initial_root])
                .count(),
            1,
            "initial root request was dispatched more than once"
        );
        assert_ne!(
            digests[initial_root], digests[continuation_root],
            "root continuation reused the initial request digest"
        );

        assert_eq!(
            requests
                .iter()
                .filter(|request| message_contains(request, "child task: child-a"))
                .count(),
            1,
            "child A request was dispatched more than once"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| message_contains(request, "child task: child-b"))
                .count(),
            1,
            "child B request was dispatched more than once"
        );
    }

    fn fork_events(&self) -> Vec<ModelEvent> {
        vec![
            ModelEvent::Content {
                delta: "ordinary completion".into(),
            },
            ModelEvent::ToolCall {
                call_id: "stage-root-file".into(),
                name: "acyclic.stage_file".into(),
                arguments: json!({
                    "path": "root.txt",
                    "text": ROOT_FILE,
                    "media_type": "text/plain",
                    "display_name": "root.txt"
                }),
            },
            ModelEvent::ToolCall {
                call_id: "fork-child-a".into(),
                name: "acyclic.fork_child".into(),
                arguments: json!({
                    "child_operation": self.child_a.to_string(),
                    "task": "child-a",
                    "prompt": "prepare child a"
                }),
            },
            ModelEvent::ToolCall {
                call_id: "fork-child-b".into(),
                name: "acyclic.fork_child".into(),
                arguments: json!({
                    "child_operation": self.child_b.to_string(),
                    "task": "child-b",
                    "prompt": "prepare child b"
                }),
            },
        ]
    }
}

impl ModelProvider for ForkFaultProvider {
    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request().clone();
        let bytes = prepared.bytes().to_vec();
        let digest = prepared.manifest().request_digest;
        self.requests
            .lock()
            .expect("request lock")
            .push(request.clone());
        self.serialized_requests
            .lock()
            .expect("serialized request lock")
            .push(bytes);
        self.request_digests
            .lock()
            .expect("request digest lock")
            .push(digest);
        self.dispatches.fetch_add(1, Ordering::SeqCst);

        let is_child_a = message_contains(&request, "child task: child-a");
        let is_child_b = message_contains(&request, "child task: child-b");
        let root = !is_child_a && !is_child_b;

        if root && !self.root_sent.swap(true, Ordering::SeqCst) {
            let root_fault = FaultWindow::from_byte(self.root_fault.load(Ordering::SeqCst));
            if root_fault == FaultWindow::BeforeIntent {
                return Box::pin(stream::once(async {
                    Err(Error::Storage(
                        "simulated disconnect before fork intent".into(),
                    ))
                }));
            }
            let events = self.fork_events();
            if root_fault == FaultWindow::AfterIntent {
                return Box::pin(
                    stream::iter(events.into_iter().map(Ok::<ModelEvent, Error>)).chain(
                        stream::once(async {
                            Err(Error::Storage(
                                "simulated disconnect after durable fork intents".into(),
                            ))
                        }),
                    ),
                );
            }
            return Box::pin(stream::iter(
                events.into_iter().map(Ok::<ModelEvent, Error>).chain([Ok(
                    ModelEvent::Completed {
                        metadata: Value::Null,
                    },
                )]),
            ));
        }

        if is_child_a {
            self.child_a_started.store(true, Ordering::SeqCst);
            // One observer must retain the permit when provider dispatch
            // occurs before its notification future is first polled.
            self.child_a_dispatched.notify_one();
            if self.child_a_blocked.load(Ordering::SeqCst) {
                let guard = BlockedChildStreamGuard {
                    dropped: self.child_stream_dropped.clone(),
                    notified: self.child_stream_drop_notified.clone(),
                };
                let first = stream::once(async move {
                    let _guard = guard;
                    futures::future::pending::<Result<ModelEvent>>().await
                });
                return Box::pin(first.chain(stream::once(async {
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    })
                })));
            }
            let child_fault = FaultWindow::from_byte(self.child_a_fault.load(Ordering::SeqCst));
            if child_fault == FaultWindow::AfterPreparedSeed
                && !self.child_a_failed.swap(true, Ordering::SeqCst)
            {
                return Box::pin(stream::once(async {
                    Err(Error::Storage(
                        "simulated disconnect after prepared seed publication".into(),
                    ))
                }));
            }
            if child_fault == FaultWindow::BeforeCompletion
                && !self.child_a_failed.swap(true, Ordering::SeqCst)
            {
                return Box::pin(stream::iter([
                    Ok(ModelEvent::Content {
                        delta: "child-a prefix".into(),
                    }),
                    Err(Error::Storage(
                        "simulated disconnect after child dispatch".into(),
                    )),
                ]));
            }
        }

        Box::pin(stream::iter(
            ordinary().into_iter().map(Ok::<ModelEvent, Error>),
        ))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        let completed = self.reconcile_completed.load(Ordering::SeqCst);
        Box::pin(async move {
            if completed {
                Ok(Some(vec![ModelEvent::Completed {
                    metadata: Value::Null,
                }]))
            } else {
                Ok(None)
            }
        })
    }
}

async fn local_project(
    root: &Path,
    initialize: bool,
) -> Result<(
    Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    StreamClient<LocalStream>,
    VolumeRef,
)> {
    let provider = ProviderRef::new("local", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(
        LocalFs::local(LocalOptions::new(root.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(root.join("conversation"), LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let project = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("local-swarm-faults".into()),
    )?;
    // `initialize == false` is the cold-open path: the workspace and its
    // generation already exist on disk, so reopening must not call
    // create_volume or reseed the project.
    if initialize {
        let head = host.create_volume(&project).await?;
        let workspace = workspace_ref(project.provider().clone(), &project.storage_name()?)?;
        host.apply(
            &workspace,
            Some(&head.generation),
            &[WorkspaceMutation::PutFile {
                path: "/project-root.txt".into(),
                bytes: ROOT_FILE.as_bytes().to_vec(),
            }],
            &acyclic_harness::IdempotencyKey::new("seed-fault-project-root")?,
        )
        .await?;
    }
    Ok((host, stream, project))
}

async fn open_swarm(
    root: &Path,
    provider: Arc<ForkFaultProvider>,
    host: Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    stream: StreamClient<LocalStream>,
    project: VolumeRef,
) -> Result<Arc<PersistentLocalSwarm>> {
    open_swarm_with_secret(root, provider, host, stream, project, [0x5A; 32]).await
}

async fn open_swarm_with_secret(
    root: &Path,
    provider: Arc<ForkFaultProvider>,
    host: Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    stream: StreamClient<LocalStream>,
    project: VolumeRef,
    secret: [u8; 32],
) -> Result<Arc<PersistentLocalSwarm>> {
    let resolver = Arc::new(
        LocalFilesystemForkResolver::new(
            host,
            stream,
            ProviderRef::new("local", "stream", "2")?,
            project,
        )?
        // The resolver's owner secret is part of the durable constructor
        // identity.  Every reopen in this fixture deliberately supplies the
        // same secret; changing it must reject the persisted fork bindings.
        .with_host_secret(secret)?,
    );
    PersistentLocalSwarm::open_shared_with_model_and_bindings(
        root,
        model()?,
        provider,
        Limits::default(),
        LocalSwarmBindings::default().with_filesystem_fork_resolver(resolver),
    )
    .await
}

struct AbortRun(tokio::task::AbortHandle);

impl Drop for AbortRun {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn wait_for_child_dispatch<T: std::fmt::Debug>(
    provider: &ForkFaultProvider,
    running: &mut tokio::task::JoinHandle<T>,
) {
    if provider.child_a_started.load(Ordering::SeqCst) {
        return;
    }
    let notified = provider.child_a_dispatched.notified();
    if provider.child_a_started.load(Ordering::SeqCst) {
        return;
    }
    if timeout(Duration::from_secs(30), notified).await.is_err() {
        running.abort();
        let stopped = running.await;
        panic!("child dispatch did not reach the provider; owned run stopped: {stopped:?}");
    }
}

async fn wait_for_child_stream_drop<T: std::fmt::Debug>(
    provider: &ForkFaultProvider,
    running: &mut tokio::task::JoinHandle<T>,
) {
    if provider.child_stream_dropped.load(Ordering::SeqCst) {
        return;
    }
    let notified = provider.child_stream_drop_notified.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if provider.child_stream_dropped.load(Ordering::SeqCst) {
        return;
    }
    if timeout(Duration::from_secs(2), notified).await.is_err() {
        running.abort();
        let stopped = running.await;
        panic!(
            "child provider stream did not drop within the cancellation bound; dispatches={}, started={}, dropped={}, owned run stopped: {stopped:?}",
            provider.dispatches.load(Ordering::SeqCst),
            provider.child_a_started.load(Ordering::SeqCst),
            provider.child_stream_dropped.load(Ordering::SeqCst),
        );
    }
}

async fn wait_for_dispatches(provider: &ForkFaultProvider, minimum: usize) -> Result<()> {
    timeout(Duration::from_secs(30), async {
        loop {
            if provider.dispatches.load(Ordering::SeqCst) >= minimum {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Error::Conflict(format!("provider dispatches did not reach {minimum}")))
}

async fn finish_owned_run<T: std::fmt::Debug>(
    running: &mut tokio::task::JoinHandle<T>,
    maximum: Duration,
) -> T {
    match timeout(maximum, &mut *running).await {
        Ok(result) => result.expect("owned swarm task panicked"),
        Err(error) => {
            running.abort();
            let stopped = running.await;
            panic!("owned swarm task did not stop: {error}; abort result: {stopped:?}");
        }
    }
}

#[tokio::test]
async fn fork_intent_disconnect_replays_publication_after_cold_restart() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path(), true).await?;
    let child_a = operation(0xA1);
    let child_b = operation(0xB1);
    let provider = ForkFaultProvider::new(child_a, child_b);
    provider.set_root_fault(FaultWindow::AfterIntent);
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let root_operation = operation(0x01);
    assert!(
        swarm
            .run_root(root_operation, "start faulted recursive swarm")
            .await
            .is_err()
    );
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 1);
    assert!(swarm.published_seed(task(child_a)).await.is_err());
    assert!(swarm.published_seed(task(child_b)).await.is_err());
    let durable_prefix = provider.serialized();
    assert_eq!(durable_prefix.len(), 1);
    drop(swarm);

    // The second handle only permits reconciliation.  It must publish the
    // already selected children without redispatching the root request.
    provider.reconcile_completed.store(true, Ordering::SeqCst);
    drop(host);
    drop(stream);
    drop(project);
    let (host, stream, project) = local_project(directory.path(), false).await?;
    let reopened = open_swarm(directory.path(), provider.clone(), host, stream, project).await?;
    let output = reopened
        .run_root(root_operation, "start faulted recursive swarm")
        .await?;
    assert_eq!(output.text, "ordinary completion");
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 4);
    assert_eq!(
        &provider.serialized()[..durable_prefix.len()],
        durable_prefix
    );
    provider.assert_request_digests();
    provider.assert_completed_dispatch_trace(child_a, child_b);

    for child in [child_a, child_b] {
        let task = task(child);
        assert_eq!(
            reopened.session(task).await?.phase,
            LocalSessionPhase::Completed
        );
        assert!(reopened.published_seed(task).await.is_ok());
        assert!(reopened.prepared_report(task).await.is_ok());
        assert_eq!(reopened.outcome(task).await?.text, "ordinary completion");
    }
    Ok(())
}

#[tokio::test]
async fn changed_issuer_binding_rejects_recovery_before_child_dispatch() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path(), true).await?;
    let child_a = operation(0xA7);
    let child_b = operation(0xB7);
    let provider = ForkFaultProvider::new(child_a, child_b);
    provider.set_root_fault(FaultWindow::AfterIntent);
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let root_operation = operation(0x07);
    assert!(swarm
        .run_root(root_operation, "reject changed owner binding")
        .await
        .is_err());
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 1);
    let requests_before_reopen = provider.serialized();
    let root_task = swarm.root_task().await?;
    drop(swarm);
    drop(host);
    drop(stream);
    drop(project);

    // Reconciliation may prove the model batch completed, but a resolver
    // reopened under another owner secret must reject the persisted issuer
    // binding before it can allocate or dispatch either child.
    provider.reconcile_completed.store(true, Ordering::SeqCst);
    let (host, stream, project) = local_project(directory.path(), false).await?;
    let reopened = open_swarm_with_secret(
        directory.path(),
        provider.clone(),
        host,
        stream,
        project,
        [0xA5; 32],
    )
    .await?;
    let result = reopened
        .run_root(root_operation, "reject changed owner binding")
        .await;
    assert!(matches!(
        result,
        Err(Error::Conflict(reason)) if reason.contains("issuer binding")
    ));
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(provider.serialized(), requests_before_reopen);
    assert!(!matches!(
        reopened.session(root_task).await?.phase,
        LocalSessionPhase::Failed(_)
    ));
    assert!(reopened.published_seed(task(child_a)).await.is_err());
    assert!(reopened.published_seed(task(child_b)).await.is_err());
    Ok(())
}

#[tokio::test]
async fn prepared_batch_failure_retries_without_duplicate_child_dispatch() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path(), true).await?;
    let child_a = operation(0xA2);
    let child_b = operation(0xB2);
    let provider = ForkFaultProvider::new(child_a, child_b);
    provider.set_child_a_fault(FaultWindow::AfterPreparedSeed);
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let root_operation = operation(0x02);
    let initial = swarm
        .run_root(root_operation, "publish a faulted child batch")
        .await?;
    assert_eq!(initial.text, "ordinary completion");
    wait_for_dispatches(&provider, 3).await?;
    // Both seeds are durable before the first child dispatch.  A retry must
    // therefore resume the batch boundary rather than republish either seed.
    for child in [child_a, child_b] {
        assert!(swarm.published_seed(task(child)).await.is_ok());
        assert!(swarm.prepared_report(task(child)).await.is_ok());
    }
    assert_eq!(
        swarm.session(task(child_a)).await?.phase,
        LocalSessionPhase::Activating,
        "child A provider disconnect retains its uncertain activation claim"
    );
    assert_eq!(
        swarm.session(task(child_b)).await?.phase,
        LocalSessionPhase::Completed,
        "sibling B completes independently while child A is uncertain"
    );
    assert_eq!(
        swarm.outcome(task(child_b)).await?.text,
        "ordinary completion"
    );
    drop(swarm);

    let durable_prefix = provider.serialized();
    provider.reconcile_completed.store(true, Ordering::SeqCst);
    drop(host);
    drop(stream);
    drop(project);
    let (host, stream, project) = local_project(directory.path(), false).await?;
    let reopened = open_swarm(directory.path(), provider.clone(), host.clone(), stream.clone(), project.clone()).await?;
    let root_task = reopened.root_task().await?;
    let root_harness = PersistentLocalHarness::open_with_tools_and_project_on_providers(
        directory.path().join("tasks").join(root_task.to_string()),
        model()?,
        provider.clone(),
        Limits::default(),
        LocalHarnessTools::new(),
        Some(project.clone()),
        host.clone(),
        stream.clone(),
        ProviderRef::new("local", "stream", "2")?,
    )
    .await?;
    let mut parent = root_harness
        .conversation_aggregate(Limits::default())
        .await?;
    let seed = reopened.published_seed(task(child_a)).await?;
    let recovered = reopened
        .retry_published_child(
            task(child_a),
            host,
            stream,
            child_issuer(&seed, child_a),
            &mut parent,
        )
        .await?;
    assert_eq!(recovered.output.text, "");
    let output = reopened
        .run_root(root_operation, "publish a faulted child batch")
        .await?;
    assert_eq!(output.text, "ordinary completion");
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 4);
    assert_eq!(
        &provider.serialized()[..durable_prefix.len()],
        durable_prefix
    );
    provider.assert_request_digests();
    provider.assert_completed_dispatch_trace(child_a, child_b);
    for child in [child_a, child_b] {
        assert_eq!(
            reopened.session(task(child)).await?.phase,
            LocalSessionPhase::Completed
        );
        let expected = if child == child_a {
            ""
        } else {
            "ordinary completion"
        };
        assert_eq!(reopened.outcome(task(child)).await?.text, expected);
    }
    Ok(())
}

#[tokio::test]
async fn before_intent_disconnect_remains_indeterminate_after_real_reopen() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path(), true).await?;
    let child_a = operation(0xA4);
    let child_b = operation(0xB4);
    let provider = ForkFaultProvider::new(child_a, child_b);
    provider.set_root_fault(FaultWindow::BeforeIntent);
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let root_operation = operation(0x04);
    assert!(
        swarm
            .run_root(root_operation, "fail before selecting children")
            .await
            .is_err()
    );
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 1);
    assert!(swarm.published_seed(task(child_a)).await.is_err());
    assert!(swarm.published_seed(task(child_b)).await.is_err());
    provider.assert_request_digests();
    drop(swarm);
    drop(host);
    drop(stream);
    drop(project);

    // Reopening the actual local providers must preserve the unknown outcome;
    // no provider retry is safe until the owner supplies reconciliation.
    let (host, stream, project) = local_project(directory.path(), false).await?;
    let reopened = open_swarm(directory.path(), provider.clone(), host, stream, project).await?;
    assert!(
        reopened
            .run_root(root_operation, "fail before selecting children")
            .await
            .is_err()
    );
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests_matching("child task:").len(), 0);
    Ok(())
}

#[tokio::test]
async fn precompletion_disconnect_replays_child_result_without_duplicate_dispatch() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path(), true).await?;
    let child_a = operation(0xA5);
    let child_b = operation(0xB5);
    let provider = ForkFaultProvider::new(child_a, child_b);
    provider.set_child_a_fault(FaultWindow::BeforeCompletion);
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let root_operation = operation(0x05);
    let initial = swarm
        .run_root(root_operation, "recover a child before completion")
        .await?;
    assert_eq!(initial.text, "ordinary completion");
    wait_for_dispatches(&provider, 4).await?;
    let durable_prefix = provider.serialized();
    assert_eq!(durable_prefix.len(), 4);
    for child in [child_a, child_b] {
        assert!(swarm.published_seed(task(child)).await.is_ok());
        assert!(swarm.prepared_report(task(child)).await.is_ok());
    }
    drop(swarm);
    drop(host);
    drop(stream);
    drop(project);

    provider.reconcile_completed.store(true, Ordering::SeqCst);
    let (host, stream, project) = local_project(directory.path(), false).await?;
    let reopened = open_swarm(directory.path(), provider.clone(), host.clone(), stream.clone(), project.clone()).await?;
    let root_task = reopened.root_task().await?;
    let root_harness = PersistentLocalHarness::open_with_tools_and_project_on_providers(
        directory.path().join("tasks").join(root_task.to_string()),
        model()?,
        provider.clone(),
        Limits::default(),
        LocalHarnessTools::new(),
        Some(project.clone()),
        host.clone(),
        stream.clone(),
        ProviderRef::new("local", "stream", "2")?,
    )
    .await?;
    let mut parent = root_harness
        .conversation_aggregate(Limits::default())
        .await?;
    let seed = reopened.published_seed(task(child_a)).await?;
    let recovered = reopened
        .retry_published_child(
            task(child_a),
            host,
            stream,
            child_issuer(&seed, child_a),
            &mut parent,
        )
        .await?;
    assert_eq!(recovered.output.text, "child-a prefix");
    let output = reopened
        .run_root(root_operation, "recover a child before completion")
        .await?;
    assert_eq!(output.text, "ordinary completion");
    assert_eq!(
        &provider.serialized()[..durable_prefix.len()],
        durable_prefix
    );
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 4);
    provider.assert_request_digests();
    provider.assert_completed_dispatch_trace(child_a, child_b);
    assert_eq!(
        reopened.outcome(task(child_a)).await?.text,
        "child-a prefix"
    );
    assert_eq!(
        reopened.outcome(task(child_b)).await?.text,
        "ordinary completion"
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_child_after_publication_cannot_be_reactivated() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path(), true).await?;
    let child_a = operation(0xA3);
    let child_b = operation(0xB3);
    let provider = ForkFaultProvider::new(child_a, child_b);
    provider.child_a_blocked.store(true, Ordering::SeqCst);
    let first = open_swarm(
        directory.path(),
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let second = open_swarm(
        directory.path(),
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let root_operation = operation(0x03);
    let first_for_run = first.clone();
    let mut running = tokio::spawn(async move {
        first_for_run
            .run_root(root_operation, "cancel child after publication")
            .await
    });
    let _abort_run = AbortRun(running.abort_handle());
    wait_for_child_dispatch(&provider, &mut running).await;
    let cancelled = second.cancel(task(child_a)).await?;
    assert_eq!(
        cancelled.phase,
        LocalSessionPhase::Cancelled,
        "the second handle must observe durable cancellation before the owner is joined"
    );
    wait_for_child_stream_drop(&provider, &mut running).await;
    let result = finish_owned_run(&mut running, Duration::from_secs(30)).await;
    assert!(result.is_ok(), "root admission should complete before child cancellation: {result:?}");
    first.shutdown_workers().await;
    assert!(provider.child_stream_dropped.load(Ordering::SeqCst),
        "cancellation returned while the child model stream was still live");
    // Close the owning composition before dropping the shared local providers;
    // otherwise the process-close/reopen check observes its live stream lock.
    drop(first);
    drop(second);

    drop(host);
    drop(stream);
    drop(project);
    let (host, stream, project) = local_project(directory.path(), false).await?;
    let reopened = open_swarm(directory.path(), provider.clone(), host, stream, project).await?;
    assert_eq!(
        reopened.session(task(child_a)).await?.phase,
        LocalSessionPhase::Cancelled
    );
    let dispatches_before_resume = provider.dispatches.load(Ordering::SeqCst);
    assert!(reopened
        .run_root(root_operation, "cancel child after publication")
        .await
        .is_err());
    assert_eq!(
        provider.dispatches.load(Ordering::SeqCst),
        dispatches_before_resume,
        "cold resume of a cancelled child must not redispatch any model turn"
    );
    assert_eq!(provider.requests_matching("child task: child-a").len(), 1);
    assert_eq!(provider.requests_matching("child task: child-b").len(), 1);
    provider.assert_request_digests();
    Ok(())
}

fn child_issuer(seed: &ForkSeed, operation: OperationId) -> AuthorityIssuer {
    let mut key = blake3::Hasher::new_keyed(&[0x5A; 32]);
    key.update(b"acyclic.local-swarm.child-authority.v1\0");
    key.update(seed.child.id.as_bytes());
    key.update(&operation.into_bytes());
    AuthorityIssuer::new(
        "local-swarm-fork",
        *key.finalize().as_bytes(),
        seed.child.clone(),
    )
}

#[tokio::test]
async fn concurrent_handle_reconciles_live_admission_without_duplicate_dispatch() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path(), true).await?;
    let child_a = operation(0xA6);
    let child_b = operation(0xB6);
    let provider = ForkFaultProvider::new(child_a, child_b);
    provider.child_a_blocked.store(true, Ordering::SeqCst);
    let first = open_swarm(
        directory.path(),
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let second = open_swarm(
        directory.path(),
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let root_operation = operation(0x06);
    let first_for_run = first.clone();
    let mut first_run = tokio::spawn(async move {
        first_for_run
            .run_root(root_operation, "reconcile a live child admission")
            .await
    });
    let _abort_run = AbortRun(first_run.abort_handle());
    wait_for_child_dispatch(&provider, &mut first_run).await;

    // Child A has a durable ModelStarted record while the first provider
    // stream is still blocked. Reopen its parent aggregate and ask a second
    // handle to retry the exact published child. The local live fence must
    // retain the claim until the first owner has finished, without another
    // provider dispatch or concurrent journal writer.
    provider.reconcile_completed.store(true, Ordering::SeqCst);
    let root_task = second.root_task().await?;
    let root_harness = PersistentLocalHarness::open_with_tools_and_project_on_providers(
        directory.path().join("tasks").join(root_task.to_string()),
        model()?,
        provider.clone(),
        Limits::default(),
        LocalHarnessTools::new(),
        Some(project.clone()),
        host.clone(),
        stream.clone(),
        ProviderRef::new("local", "stream", "2")?,
    )
    .await?;
    let mut parent = root_harness
        .conversation_aggregate(Limits::default())
        .await?;
    second.sessions().await?;
    let seed = second.published_seed(task(child_a)).await?;
    let recovered = second
        .retry_published_child(
            task(child_a),
            host.clone(),
            stream.clone(),
            child_issuer(&seed, child_a),
            &mut parent,
        )
        .await;
    assert!(
        matches!(recovered, Err(Error::Indeterminate(operation)) if operation == child_a),
        "live retry must retain the activation claim while the first handle owns the provider stream: {recovered:?}"
    );
    assert_eq!(provider.requests_matching("child task: child-a").len(), 1);

    second.cancel(task(child_a)).await?;
    wait_for_child_stream_drop(&provider, &mut first_run).await;
    let first_output = finish_owned_run(&mut first_run, Duration::from_secs(30)).await;
    assert!(first_output.is_ok(), "root admission should complete before cancellation: {first_output:?}");
    first.shutdown_workers().await;
    assert!(provider.child_stream_dropped.load(Ordering::SeqCst));
    assert_eq!(provider.requests_matching("child task: child-a").len(), 1);
    assert_eq!(provider.requests_matching("child task: child-b").len(), 1);
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 4);
    provider.assert_request_digests();
    Ok(())
}
