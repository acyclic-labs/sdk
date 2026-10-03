#![cfg(feature = "filesystem-local")]

//! Production local swarm budget coverage.
//!
//! The fixture uses the real LocalFs/LocalStream composition and the model
//! fork tool.  Its provider only supplies deterministic model events; child
//! admission, publication, metering, restart, and cancellation remain the
//! production paths.

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{
        FilesystemHost, LocalFilesystemForkResolver, LocalSessionPhase, LocalSwarmBindings,
        LocalSwarmConfig, PersistentLocalSwarm, WorkspaceMutation, workspace_ref,
    },
    model::{Model, ModelContent, ModelEvent, ModelProvider, ModelRequest},
    resources::ProviderRef,
    swarm_budget::SwarmBudgetLimits,
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
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tempfile::tempdir;
use tokio::time::{Duration, timeout};

fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn has_task(request: &ModelRequest, task: &str) -> bool {
    request.messages.iter().any(|message| {
        matches!(&message.content, ModelContent::Text(text) if text.contains(task))
    })
}

struct BudgetProvider {
    root_forked: AtomicBool,
    child_started: AtomicUsize,
    released: Arc<AtomicBool>,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}

impl BudgetProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            root_forked: AtomicBool::new(false),
            child_started: AtomicUsize::new(0),
            released: Arc::new(AtomicBool::new(false)),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        })
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
    }
}

impl ModelProvider for BudgetProvider {
    fn generate<'a>(
        &'a self,
        prepared: crate::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request().clone();
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().expect("request lock").push(request.clone());
        let is_child = has_task(&request, "child task:");
        if is_child {
            self.child_started.fetch_add(1, Ordering::SeqCst);
            let released = self.released.clone();
            let first = stream::once(async move {
                while !released.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
                Ok(ModelEvent::Content {
                    delta: "child result".into(),
                })
            });
            return Box::pin(first.chain(stream::iter([Ok(ModelEvent::Completed {
                metadata: Value::Null,
            })])));
        }
        if !self.root_forked.swap(true, Ordering::SeqCst) {
            return Box::pin(stream::iter([
                Ok(ModelEvent::ToolCall {
                    call_id: "budget-child-a".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": operation(0xA1).to_string(),
                        "task": "budget-child-a",
                        "prompt": "run child a"
                    }),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: "budget-child-b".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": operation(0xB1).to_string(),
                        "task": "budget-child-b",
                        "prompt": "run child b"
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]));
        }
        Box::pin(stream::iter([
            Ok(ModelEvent::Content {
                delta: "root result".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: acyclic_harness::model::ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

async fn local_project(
    root: &Path,
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
        VolumeOwner::Project("budget-integration".into()),
    )?;
    let head = host.create_volume(&project).await?;
    let workspace = workspace_ref(project.provider().clone(), &project.storage_name()?)?;
    host.apply(
        &workspace,
        Some(&head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/root.txt".into(),
            bytes: b"budget root".to_vec(),
        }],
        &IdempotencyKey::new("seed-budget-project")?,
    )
    .await?;
    Ok((host, stream, project))
}

fn bindings(
    host: Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    stream: StreamClient<LocalStream>,
    project: VolumeRef,
) -> Result<LocalSwarmBindings> {
    let resolver = Arc::new(
        LocalFilesystemForkResolver::new(
            host,
            stream,
            ProviderRef::new("local", "stream", "2")?,
            project,
        )?
        .with_host_secret([0x39; 32])?,
    );
    Ok(LocalSwarmBindings::default().with_filesystem_fork_resolver(resolver))
}

fn budget_limits() -> SwarmBudgetLimits {
    SwarmBudgetLimits {
        max_active_agents: 2,
        max_total_agents: 3,
        max_recursion_depth: 1,
        max_model_steps: 64,
        max_output_bytes: 64 * 1024,
        max_execution_time_ms: 60_000,
    }
}

#[tokio::test]
async fn persistent_local_swarm_budget_is_shared_across_children_handles_and_reopen() -> Result<()>
{
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path()).await?;
    let provider = BudgetProvider::new();
    let model = Model::new("mock", "budget-integration", "1", json!({}))?;
    let mut config = LocalSwarmConfig::new(model.clone(), Limits::default())?;
    config.swarm_budget = Some(budget_limits());
    let swarm = PersistentLocalSwarm::open_shared_with_bindings(
        directory.path(),
        config.clone(),
        provider.clone(),
        bindings(host.clone(), stream.clone(), project.clone())?,
    )
    .await?;
    let second = PersistentLocalSwarm::open_shared_with_bindings(
        directory.path(),
        config.clone(),
        provider.clone(),
        bindings(host.clone(), stream.clone(), project.clone())?,
    )
    .await?;
    assert_eq!(swarm.budget_usage().await?.unwrap().active_agents, 1);
    assert_eq!(second.budget_usage().await?.unwrap().total_agents, 1);

    let run = tokio::spawn({
        let swarm = swarm.clone();
        async move { swarm.run_root(operation(0x01), "start budget children").await }
    });
    timeout(Duration::from_secs(5), async {
        loop {
            if swarm.sessions().await.len() >= 3 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Error::Storage("children were not durably admitted".into()))?;
    provider.release();
    let _ = timeout(Duration::from_secs(10), run)
        .await
        .map_err(|_| Error::Storage("root budget run did not finish".into()))?
        .map_err(|error| Error::Storage(error.to_string()))??;

    let usage = second.budget_usage().await?.unwrap();
    assert_eq!(usage.active_agents, 1);
    assert!(usage.total_agents <= 3);
    assert_eq!(provider.child_started.load(Ordering::SeqCst), 1);
    assert!(usage.consumed.model_steps > 0);
    assert!(usage.consumed.output_bytes > 0);
    assert!(usage.consumed.execution_time_ms <= budget_limits().max_execution_time_ms);
    drop(second);
    drop(swarm);

    let reopened = PersistentLocalSwarm::open_shared_with_bindings(
        directory.path(),
        config,
        provider.clone(),
        bindings(host, stream, project)?,
    )
    .await?;
    let reopened_usage = reopened.budget_usage().await?.unwrap();
    assert_eq!(reopened_usage, usage);
    assert_eq!(reopened.session(reopened.root_task().await?).await?.phase, LocalSessionPhase::Completed);
    assert!(provider.calls.load(Ordering::SeqCst) <= 3);
    Ok(())
}
