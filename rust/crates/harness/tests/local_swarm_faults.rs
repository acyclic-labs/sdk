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
    filesystem::{
        FilesystemHost, LocalFilesystemForkResolver, LocalSessionPhase, LocalSwarmBindings,
        PersistentLocalSwarm, WorkspaceMutation, workspace_ref,
    },
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
    release_child_a: Arc<AtomicBool>,
    child_a_started: AtomicBool,
    dispatches: AtomicUsize,
    child_a: OperationId,
    child_b: OperationId,
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
            release_child_a: Arc::new(AtomicBool::new(false)),
            child_a_started: AtomicBool::new(false),
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

    fn release_child(&self) {
        self.release_child_a.store(true, Ordering::SeqCst);
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
    fn output_token_limit_for_bytes(&self, max_output_bytes: u64) -> Option<u32> {
        u32::try_from(max_output_bytes).ok().filter(|bound| *bound > 0)
    }

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
            if self.child_a_blocked.load(Ordering::SeqCst) {
                let release = self.release_child_a.clone();
                let first = stream::once(async move {
                    while !release.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    Ok::<ModelEvent, Error>(ModelEvent::Content {
                        delta: "child-a completion".into(),
                    })
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
        .with_host_secret([0x5A; 32])?,
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

async fn wait_for_child_dispatch(provider: &ForkFaultProvider) {
    timeout(Duration::from_secs(2), async {
        while !provider.child_a_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("child dispatch did not reach the provider");
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
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 3);
    assert_eq!(
        &provider.serialized()[..durable_prefix.len()],
        durable_prefix
    );
    provider.assert_request_digests();
    assert_eq!(
        provider
            .requests_matching("start faulted recursive swarm")
            .len(),
        1
    );

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
    assert!(
        swarm
            .run_root(root_operation, "publish a faulted child batch")
            .await
            .is_err()
    );
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 2);
    // Both seeds are durable before the first child dispatch.  A retry must
    // therefore resume the batch boundary rather than republish either seed.
    for child in [child_a, child_b] {
        assert!(swarm.published_seed(task(child)).await.is_ok());
        assert!(swarm.prepared_report(task(child)).await.is_ok());
        assert_ne!(
            swarm.session(task(child)).await?.phase,
            LocalSessionPhase::Completed
        );
    }
    drop(swarm);

    let durable_prefix = provider.serialized();
    provider.reconcile_completed.store(true, Ordering::SeqCst);
    drop(host);
    drop(stream);
    drop(project);
    let (host, stream, project) = local_project(directory.path(), false).await?;
    let reopened = open_swarm(directory.path(), provider.clone(), host, stream, project).await?;
    let output = reopened
        .run_root(root_operation, "publish a faulted child batch")
        .await?;
    assert_eq!(output.text, "ordinary completion");
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 3);
    assert_eq!(
        &provider.serialized()[..durable_prefix.len()],
        durable_prefix
    );
    provider.assert_request_digests();
    assert_eq!(provider.requests_matching("child task: child-a").len(), 1);
    assert_eq!(provider.requests_matching("child task: child-b").len(), 1);
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
    assert!(
        swarm
            .run_root(root_operation, "recover a child before completion")
            .await
            .is_err()
    );
    let durable_prefix = provider.serialized();
    assert_eq!(durable_prefix.len(), 2);
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
    let reopened = open_swarm(directory.path(), provider.clone(), host, stream, project).await?;
    let output = reopened
        .run_root(root_operation, "recover a child before completion")
        .await?;
    assert_eq!(output.text, "ordinary completion");
    assert_eq!(
        &provider.serialized()[..durable_prefix.len()],
        durable_prefix
    );
    assert_eq!(provider.dispatches.load(Ordering::SeqCst), 3);
    provider.assert_request_digests();
    assert_eq!(provider.requests_matching("child task: child-a").len(), 1);
    assert_eq!(provider.requests_matching("child task: child-b").len(), 1);
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
    let running = tokio::spawn(async move {
        first
            .run_root(root_operation, "cancel child after publication")
            .await
    });
    wait_for_child_dispatch(&provider).await;
    second.cancel(task(child_a)).await?;
    provider.release_child();
    let result = timeout(Duration::from_secs(2), running)
        .await
        .expect("cancelled publication did not finish")
        .expect("publication task panicked");
    assert!(result.is_err());
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
    assert!(
        reopened
            .run_root(root_operation, "cancel child after publication")
            .await
            .is_err()
    );
    assert_eq!(provider.requests_matching("child task: child-a").len(), 1);
    assert_eq!(provider.requests_matching("child task: child-b").len(), 0);
    provider.assert_request_digests();
    Ok(())
}
