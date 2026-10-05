#![cfg(feature = "filesystem-local")]

//! Production local publisher recovery at the durable seed/activation gate.

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    Error, IdempotencyKey, OperationId, Result, TaskId,
    batch_publication::ModelBatchPublication,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::EffectGuarantee,
    filesystem::{
        FilesystemHost, LocalFilesystemForkResolver, LocalForkPublicationGate,
        LocalSessionPhase, LocalSwarmBindings, PersistentLocalSwarm, WorkspaceMutation,
        workspace_ref,
    },
    model::{Model, ModelContent, ModelContentPart, ModelEvent, ModelProvider, ModelRequest},
    resources::ProviderRef,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::{future::BoxFuture, stream::BoxStream};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn task(operation: OperationId) -> TaskId {
    TaskId::from_bytes(operation.into_bytes())
}

fn model() -> Result<Model> {
    Model::new("mock", "fork-publication-gate", "1", Value::Null)
}

fn contains_child_task(request: &ModelRequest) -> bool {
    request.messages.iter().any(|message| match &message.content {
        ModelContent::Text(text) => text.contains("child task: publication-child"),
        ModelContent::Part(ModelContentPart::Text { text }) => {
            text.contains("child task: publication-child")
        }
        ModelContent::Parts(parts) => parts.iter().any(|part| {
            matches!(part, ModelContentPart::Text { text } if text.contains("child task: publication-child"))
        }),
        _ => false,
    })
}

struct ForkModel {
    child_a: OperationId,
    child_b: OperationId,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for ForkModel {
    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request().clone();
        self.requests.lock().expect("model request lock").push(request.clone());
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        if first && !contains_child_task(&request) {
            return Box::pin(futures::stream::iter([
                Ok(ModelEvent::ToolCall {
                    call_id: "fork-publication-child-a".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": self.child_a.to_string(),
                        "task": "publication-child-a",
                        "prompt": "complete publication recovery child a"
                    }),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "fork-publication-child-b".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": self.child_b.to_string(),
                        "task": "publication-child-b",
                        "prompt": "complete publication recovery child b"
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]));
        }
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content {
                delta: "ordinary completion".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }

    fn reconcile<'a>(
        &'a self,
        _: acyclic_harness::model::ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

struct FailOnceAfterSeeds {
    calls: AtomicUsize,
    publications: Mutex<Vec<ModelBatchPublication>>,
}

impl LocalForkPublicationGate for FailOnceAfterSeeds {
    fn before_child_activation(&self, publication: &ModelBatchPublication) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.publications
            .lock()
            .expect("publication lock")
            .push(publication.clone());
        if self.calls.load(Ordering::SeqCst) == 1 {
            Err(Error::Storage(
                "simulated interruption after durable child seeds".into(),
            ))
        } else {
            Ok(())
        }
    }
}

async fn local_project(
    root: &std::path::Path,
    initialize: bool,
) -> Result<(
    Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    StreamClient<LocalStream>,
    VolumeRef,
)> {
    let filesystem_provider = ProviderRef::new("local", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(
        LocalFs::local(LocalOptions::new(root.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        filesystem_provider.clone(),
    )?);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(root.join("conversation"), LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let project = VolumeRef::new(
        filesystem_provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("fork-publication-gate".into()),
    )?;
    if initialize {
        let head = host.create_volume(&project).await?;
        let workspace = workspace_ref(project.provider().clone(), &project.storage_name()?)?;
        host.apply(
            &workspace,
            Some(&head.generation),
            &[WorkspaceMutation::PutFile {
                path: "/root.txt".into(),
                bytes: b"pinned root".to_vec(),
            }],
            &IdempotencyKey::new("seed-publication-gate-root")?,
        )
        .await?;
    }
    Ok((host, stream, project))
}

async fn open_swarm(
    root: &std::path::Path,
    provider: Arc<ForkModel>,
    gate: Arc<FailOnceAfterSeeds>,
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
        .with_host_secret([0x5A; 32])?,
    );
    PersistentLocalSwarm::open_shared_with_model_and_bindings(
        root,
        model()?,
        provider,
        Limits::default(),
        LocalSwarmBindings::default()
            .with_filesystem_fork_resolver(resolver)
            .with_publication_gate(gate),
    )
    .await
}

#[tokio::test]
async fn production_publisher_reuses_seed_and_publication_after_activation_gate_failure() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path(), true).await?;
    let child_a = operation(0xB8);
    let child_b = operation(0xB9);
    let provider = Arc::new(ForkModel {
        child_a,
        child_b,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let gate = Arc::new(FailOnceAfterSeeds {
        calls: AtomicUsize::new(0),
        publications: Mutex::new(Vec::new()),
    });
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        gate.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
    )
    .await?;
    let root_operation = operation(0x08);
    assert!(
        swarm
            .run_root(root_operation, "publish one child")
            .await
            .is_err()
    );
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    let first_seed_a = swarm.published_seed(task(child_a)).await?;
    let first_seed_b = swarm.published_seed(task(child_b)).await?;
    for child in [child_a, child_b] {
        assert_eq!(
            swarm.session(task(child)).await?.phase,
            LocalSessionPhase::Activating
        );
    }
    // The gate is after both durable seeds and before activation. A failed
    // publication must therefore leave no child model request in flight.
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests.lock().expect("model request lock").len(), 1);

    drop(swarm);
    drop(host);
    drop(stream);
    drop(project);
    let (host, stream, project) = local_project(directory.path(), false).await?;
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        gate.clone(),
        host,
        stream,
        project,
    )
    .await?;
    let output = swarm
        .run_root(root_operation, "publish one child")
        .await?;
    assert_eq!(output.text, "ordinary completion");
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    assert_eq!(first_seed_a, swarm.published_seed(task(child_a)).await?);
    assert_eq!(first_seed_b, swarm.published_seed(task(child_b)).await?);
    for child in [child_a, child_b] {
        assert_eq!(
            swarm.session(task(child)).await?.phase,
            LocalSessionPhase::Completed
        );
    }

    let publications = gate.publications.lock().expect("publication lock");
    assert_eq!(publications.len(), 2);
    assert_eq!(publications[0], publications[1]);
    assert_eq!(publications[0].guarantee, EffectGuarantee::IdempotentRetry);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
    assert_eq!(provider.requests.lock().expect("model request lock").len(), 4);
    Ok(())
}
