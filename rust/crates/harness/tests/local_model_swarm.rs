#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

//! Production local model-selected recursive swarm coverage.
//!
//! The fixture uses the same on-disk LocalStream and LocalFs providers as the
//! application composition. The model is deterministic, but every request
//! captured below is the actual serialized provider request produced by the
//! durable executor.

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    Error, OperationId, Result,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{
        FilesystemHost, LocalFilesystemForkResolver, LocalSessionPhase, LocalSwarmBindings,
        PersistentLocalSwarm, WorkspaceMutation, workspace_ref,
    },
    model::{
        Model, ModelContent, ModelContentPart, ModelEvent, ModelProvider, ModelRequest, ModelRole,
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
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tempfile::tempdir;

const ROOT_FILE: &str = "root private file survives the recursive fork";

fn id(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn message_contains(request: &ModelRequest, needle: &str) -> bool {
    request
        .messages
        .iter()
        .any(|message| match &message.content {
            ModelContent::Text(text) => text.contains(needle),
            ModelContent::Part(ModelContentPart::File { .. }) => false,
            ModelContent::Part(ModelContentPart::Text { text }) => text.contains(needle),
            ModelContent::Parts(parts) => parts.iter().any(
                |part| matches!(part, ModelContentPart::Text { text } if text.contains(needle)),
            ),
            ModelContent::Part(ModelContentPart::ToolResult { value, .. }) => {
                value.as_str().is_some_and(|text| text.contains(needle))
            }
            ModelContent::Part(ModelContentPart::ToolCall { arguments, .. }) => {
                arguments.to_string().contains(needle)
            }
        })
}

fn staged_file(request: &ModelRequest) -> Option<Value> {
    request.messages.iter().find_map(|message| {
        let ModelContent::Part(ModelContentPart::ToolResult { name, value, .. }) = &message.content
        else {
            return None;
        };
        (name == "acyclic.stage_file")
            .then(|| value.get("file").cloned())
            .flatten()
    })
}

fn has_read_result(request: &ModelRequest) -> bool {
    request.messages.iter().any(|message| {
        let ModelContent::Part(ModelContentPart::ToolResult { name, value, .. }) = &message.content
        else {
            return false;
        };
        name == "acyclic.read_file"
            && (value.as_str() == Some(ROOT_FILE)
                || value.get("text").and_then(Value::as_str) == Some(ROOT_FILE))
    })
}

struct DeterministicProvider {
    requests_decoded: Mutex<Vec<ModelRequest>>,
    requests: Mutex<Vec<Vec<u8>>>,
    root_fork_sent: AtomicBool,
    child_fork_sent: AtomicBool,
    child_read_verified: AtomicBool,
    grandchild_inherited_read: AtomicBool,
    sibling_fork_sent: AtomicBool,
    dispatches: AtomicUsize,
    swarm: Mutex<Option<Weak<PersistentLocalSwarm>>>,
    child_a: OperationId,
    child_b: OperationId,
    grandchild: OperationId,
}

impl DeterministicProvider {
    fn new(child_a: OperationId, child_b: OperationId, grandchild: OperationId) -> Arc<Self> {
        Arc::new(Self {
            requests_decoded: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
            root_fork_sent: AtomicBool::new(false),
            child_fork_sent: AtomicBool::new(false),
            child_read_verified: AtomicBool::new(false),
            grandchild_inherited_read: AtomicBool::new(false),
            sibling_fork_sent: AtomicBool::new(false),
            dispatches: AtomicUsize::new(0),
            swarm: Mutex::new(None),
            child_a,
            child_b,
            grandchild,
        })
    }

    fn bind_swarm(&self, swarm: &Arc<PersistentLocalSwarm>) {
        *self.swarm.lock().expect("swarm binding lock") = Some(Arc::downgrade(swarm));
    }

    fn serialized_requests(&self) -> Vec<Vec<u8>> {
        self.requests.lock().expect("request lock").clone()
    }

    fn decoded_requests(&self) -> Vec<ModelRequest> {
        self.requests_decoded.lock().expect("request lock").clone()
    }

    fn assert_request_round_trips(request: &ModelRequest) {
        let bytes = serde_json::to_vec(request).expect("serialize model request");
        let decoded: ModelRequest =
            serde_json::from_slice(&bytes).expect("deserialize serialized model request");
        assert_eq!(&decoded, request);
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
}

impl ModelProvider for DeterministicProvider {
    fn output_token_limit_for_bytes(&self, max_output_bytes: u64) -> Option<u32> {
        u32::try_from(max_output_bytes).ok().filter(|bound| *bound > 0)
    }

    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request().clone();
        Self::assert_request_round_trips(&request);
        // Capture the bytes from the actual production executor boundary. A
        // separately serialized clone could hide a provider-input rewrite or
        // canonicalization mismatch.
        let bytes = prepared.bytes().to_vec();
        assert_eq!(
            *blake3::hash(&bytes).as_bytes(),
            prepared.manifest().request_digest,
            "provider capture must retain the admitted request bytes"
        );
        let decoded: ModelRequest =
            serde_json::from_slice(&bytes).expect("captured request must decode");
        assert_eq!(decoded, request, "captured bytes must preserve the request");
        self.requests_decoded
            .lock()
            .expect("request lock")
            .push(request.clone());
        self.requests.lock().expect("request lock").push(bytes);
        let dispatch = self.dispatches.fetch_add(1, Ordering::SeqCst);
        let is_child_a = message_contains(&request, "child task: child-a");
        let is_child_b = message_contains(&request, "child task: child-b");
        let is_grandchild = message_contains(&request, "child task: grandchild");
        let sibling_fork_attempt = is_child_a && message_contains(&request, "attempt sibling fork");
        let root = !is_child_a && !is_child_b && !is_grandchild;

        if (is_child_a || is_child_b) && dispatch > 0 {
            let expected_a = self.child_a;
            let expected_b = self.child_b;
            let weak = self
                .swarm
                .lock()
                .expect("swarm binding lock")
                .clone()
                .expect("provider bound to swarm");
            let first_child_request = !self.grandchild_inherited_read.load(Ordering::SeqCst)
                && !self.child_read_verified.load(Ordering::SeqCst);
            let barrier = async move {
                if first_child_request {
                    let swarm = weak
                        .upgrade()
                        .ok_or_else(|| Error::Storage("swarm dropped during dispatch".into()))?;
                    let sessions = swarm.sessions().await;
                    for operation in [expected_a, expected_b] {
                        let task = acyclic_harness::TaskId::from_bytes(operation.into_bytes());
                        let session = sessions
                            .iter()
                            .find(|session| session.task == task)
                            .ok_or_else(|| {
                                Error::Conflict(
                                    "child request dispatched before all children were prepared"
                                        .into(),
                                )
                            })?;
                        assert_eq!(session.phase, LocalSessionPhase::Activating);
                    }
                }
                Ok::<ModelEvent, Error>(ModelEvent::Content {
                    delta: String::new(),
                })
            };
            let events =
                if sibling_fork_attempt && !self.sibling_fork_sent.swap(true, Ordering::SeqCst) {
                    vec![
                        ModelEvent::ToolCall {
                            call_id: "fork-sibling".into(),
                            name: "acyclic.fork_child".into(),
                            arguments: json!({
                                "child_operation": self.child_b.to_string(),
                                "task": "sibling-from-child-a",
                                "prompt": "sibling must be rejected"
                            }),
                        },
                        ModelEvent::Completed {
                            metadata: Value::Null,
                        },
                    ]
                } else if is_child_a && self.child_fork_sent.swap(true, Ordering::SeqCst) == false {
                    let file = staged_file(&request).ok_or_else(|| {
                        Error::Conflict("child request did not inherit root staged file".into())
                    });
                    let file = match file {
                        Ok(file) => file,
                        Err(error) => return Box::pin(stream::once(async move { Err(error) })),
                    };
                    vec![
                        ModelEvent::ToolCall {
                            call_id: "child-read-root".into(),
                            name: "acyclic.read_file".into(),
                            arguments: json!({"file": file}),
                        },
                        ModelEvent::ToolCall {
                            call_id: "fork-grandchild".into(),
                            name: "acyclic.fork_child".into(),
                            arguments: json!({
                                "child_operation": self.grandchild.to_string(),
                                "task": "grandchild",
                                "prompt": "read the inherited root file"
                            }),
                        },
                        ModelEvent::Completed {
                            metadata: Value::Null,
                        },
                    ]
                } else {
                    if is_child_a && has_read_result(&request) {
                        self.child_read_verified.store(true, Ordering::SeqCst);
                    }
                    if is_grandchild && has_read_result(&request) {
                        self.grandchild_inherited_read.store(true, Ordering::SeqCst);
                    }
                    Self::ordinary()
                };
            return Box::pin(stream::once(barrier).chain(stream::iter(events.into_iter().map(Ok))));
        }

        let events = if root && !self.root_fork_sent.swap(true, Ordering::SeqCst) {
            vec![
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
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]
        } else {
            Self::ordinary()
        };
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: acyclic_harness::model::ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

async fn local_project(
    root: &std::path::Path,
) -> Result<(
    Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    StreamClient<LocalStream>,
    VolumeRef,
)> {
    // PersistentLocalHarness pins private volumes to the production local
    // filesystem provider identity. The fixture host and project must use the
    // same identity so strict FilesystemHost ownership checks remain active.
    let provider = ProviderRef::new("local", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(
        LocalFs::local(LocalOptions::new(root.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let stream = StreamClient::new(Arc::new(
        // The shared swarm owns root/swarm as its registry stream. The
        // resolver's conversation stream is a separate provider-backed
        // stream, so opening the composition does not double-open the
        // registry path.
        LocalStream::open(root.join("conversation"), LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let project = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("local-model-swarm".into()),
    )?;
    let head = host.create_volume(&project).await?;
    let workspace = workspace_ref(project.provider().clone(), &project.storage_name()?)?;
    host.apply(
        &workspace,
        Some(&head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/project-root.txt".into(),
            bytes: ROOT_FILE.as_bytes().to_vec(),
        }],
        &acyclic_harness::IdempotencyKey::new("seed-project-root")?,
    )
    .await?;
    Ok((host, stream, project))
}

#[tokio::test]
async fn local_model_selected_swarm_is_recursive_durable_and_replays_without_dispatch() -> Result<()>
{
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path()).await?;
    let stream_provider = ProviderRef::new("local", "stream", "2")?;
    let child_a = id(0xA1);
    let child_b = id(0xB1);
    let grandchild = id(0xC1);
    let provider = DeterministicProvider::new(child_a, child_b, grandchild);
    let model = Model::new("mock", "local-model-swarm", "1", json!({}))?;
    let limits = Limits::default();
    let resolver = Arc::new(
        LocalFilesystemForkResolver::new(
            host.clone(),
            stream.clone(),
            stream_provider,
            project.clone(),
        )?
        .with_host_secret([0x5A; 32])?,
    );
    let bindings = LocalSwarmBindings::default().with_filesystem_fork_resolver(resolver);
    let swarm = PersistentLocalSwarm::open_shared_with_model_and_bindings(
        directory.path(),
        model.clone(),
        provider.clone(),
        limits,
        bindings,
    )
    .await?;
    provider.bind_swarm(&swarm);
    let root_operation = id(0x01);
    let root_output = swarm
        .run_root(root_operation, "start recursive local swarm")
        .await?;
    assert_eq!(root_output.text, "ordinary completion");
    assert!(provider.child_read_verified.load(Ordering::SeqCst));
    assert!(provider.grandchild_inherited_read.load(Ordering::SeqCst));
    let serialized_requests = provider.serialized_requests();
    assert!(!serialized_requests.is_empty());
    assert!(
        serialized_requests
            .iter()
            .all(|bytes| serde_json::from_slice::<ModelRequest>(bytes).is_ok())
    );

    // The child request sent to the provider must contain the exact frozen
    // parent wire prefix followed by the persisted declaration suffix. A
    // later child turn must retain those bytes before adding its own exchange.
    let decoded_requests = provider.decoded_requests();
    let child_a_requests = decoded_requests
        .iter()
        .filter(|request| message_contains(request, "child task: child-a"))
        .collect::<Vec<_>>();
    assert!(child_a_requests.len() >= 2);
    let root_task = swarm.root_task().await?;
    let child_a_task = acyclic_harness::TaskId::from_bytes(child_a.into_bytes());
    let declared_suffix = format!(
        "child task: child-a; parent: {root_task}; identity: {child_a_task}; fresh scratch: true"
    );
    let suffix_index = child_a_requests[0]
        .messages
        .iter()
        .position(|message| {
            message.role == ModelRole::System
                && message.content == ModelContent::Text(declared_suffix.clone())
        })
        .expect("child request must carry its declared model suffix");
    assert!(suffix_index > 0);
    for index in 0..=suffix_index {
        let first = serde_json::to_vec(&child_a_requests[0].messages[index])
            .expect("serialize first child wire message");
        let later = serde_json::to_vec(&child_a_requests[1].messages[index])
            .expect("serialize later child wire message");
        assert_eq!(
            first, later,
            "child wire prefix/suffix byte changed at {index}"
        );
    }

    let sessions = swarm.sessions().await;
    assert_eq!(sessions.len(), 4);
    for operation in [child_a, child_b, grandchild] {
        let task = acyclic_harness::TaskId::from_bytes(operation.into_bytes());
        assert_eq!(
            swarm.session(task).await?.phase,
            LocalSessionPhase::Completed
        );
        assert_eq!(swarm.outcome(task).await?.text, "ordinary completion");
    }

    // A completed child cannot select its sibling as a new child. The
    // authenticated parent binding and durable operation index reject the
    // forged sibling fork before another child is admitted.
    let sibling_error = swarm
        .run(child_a_task, id(0xA2), "attempt sibling fork")
        .await;
    assert!(sibling_error.is_err());
    assert!(provider.sibling_fork_sent.load(Ordering::SeqCst));

    let dispatches_before_restart = provider.dispatches.load(Ordering::SeqCst);
    let requests_before_restart = provider.serialized_requests();
    drop(swarm);

    let resolver = Arc::new(
        LocalFilesystemForkResolver::new(
            host,
            stream,
            ProviderRef::new("local", "stream", "2")?,
            project,
        )?
        .with_host_secret([0x5A; 32])?,
    );
    let reopened = PersistentLocalSwarm::open_shared_with_model_and_bindings(
        directory.path(),
        model,
        provider.clone(),
        limits,
        LocalSwarmBindings::default().with_filesystem_fork_resolver(resolver),
    )
    .await?;
    provider.bind_swarm(&reopened);
    assert_eq!(
        reopened
            .run_root(root_operation, "start recursive local swarm")
            .await?,
        root_output
    );
    assert_eq!(
        provider.dispatches.load(Ordering::SeqCst),
        dispatches_before_restart
    );
    assert_eq!(provider.serialized_requests(), requests_before_restart);

    // Reusing the root operation with changed settings is rejected by the
    // durable prompt receipt before a provider dispatch can occur.
    assert!(
        reopened
            .run_root(root_operation, "mutated root setting")
            .await
            .is_err()
    );
    assert_eq!(
        provider.dispatches.load(Ordering::SeqCst),
        dispatches_before_restart
    );
    Ok(())
}
