#![cfg(feature = "filesystem-local")]

//! Root-only production budget checks.
//!
//! Recursive publication is covered by `local_model_swarm.rs`; this fixture
//! keeps budget admission and restart/cancellation evidence independent from
//! any recursive fork manifest failure.

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{
        workspace_ref, FilesystemHost, LocalFilesystemForkResolver, LocalSessionPhase,
        LocalSwarmBindings, LocalSwarmConfig, PersistentLocalSwarm, WorkspaceMutation,
    },
    model::{Model, ModelEvent, ModelProvider},
    resources::ProviderRef,
    swarm_budget::{SwarmBudgetLimits, SwarmOwnerFence, SwarmUsage, SwarmUsageSource},
    Error, IdempotencyKey, OperationId, Result,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tempfile::tempdir;
use tokio::sync::Notify;

const ROOT_FILE: &str = "root budget fixture";

#[derive(Default)]
struct MeasuredUsage {
    values: Mutex<BTreeMap<(OperationId, String), SwarmUsage>>,
}

impl MeasuredUsage {
    fn key(operation: OperationId, dispatch: &IdempotencyKey) -> (OperationId, String) {
        (operation, dispatch.0.clone())
    }
}

impl SwarmUsageSource for MeasuredUsage {
    fn provider_identity(&self) -> &str {
        "local.test.measured"
    }

    fn cumulative_usage(
        &self,
        operation: OperationId,
        dispatch: &IdempotencyKey,
    ) -> Result<SwarmUsage> {
        self.values
            .lock()
            .map_err(|_| Error::Storage("usage source lock poisoned".into()))
            .map(|values| {
                values
                    .get(&Self::key(operation, dispatch))
                    .copied()
                    .unwrap_or_default()
            })
    }

    fn record_runtime_usage(
        &self,
        operation: OperationId,
        dispatch: &IdempotencyKey,
        usage: SwarmUsage,
    ) -> Result<()> {
        let mut values = self
            .values
            .lock()
            .map_err(|_| Error::Storage("usage source lock poisoned".into()))?;
        let value = values.entry(Self::key(operation, dispatch)).or_default();
        value.model_steps = value.model_steps.max(usage.model_steps);
        value.output_bytes = value.output_bytes.max(usage.output_bytes);
        value.execution_time_ms = value.execution_time_ms.max(usage.execution_time_ms);
        Ok(())
    }
}

struct RecoveryProvider {
    calls: AtomicUsize,
    fail_first: AtomicBool,
}

impl RecoveryProvider {
    fn fail_once() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            fail_first: AtomicBool::new(true),
        })
    }
}

impl ModelProvider for RecoveryProvider {
    fn output_token_limit_for_bytes(&self, bytes: u64) -> Option<u32> {
        u32::try_from(bytes / 4).ok()
    }

    fn generate<'a>(
        &'a self,
        _prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_first.swap(false, Ordering::SeqCst) && call == 0 {
            return Box::pin(stream::once(async {
                Err(Error::Storage("simulated lost provider reply".into()))
            }));
        }
        Box::pin(stream::iter([
            Ok(ModelEvent::Content {
                delta: "completed budgeted root".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: serde_json::Value::Null,
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

struct CancellationProvider {
    calls: AtomicUsize,
    started: Arc<AtomicBool>,
    release: Arc<Notify>,
    released: Arc<AtomicBool>,
}

impl CancellationProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            started: Arc::new(AtomicBool::new(false)),
            release: Arc::new(Notify::new()),
            released: Arc::new(AtomicBool::new(false)),
        })
    }
}

impl ModelProvider for CancellationProvider {
    fn output_token_limit_for_bytes(&self, bytes: u64) -> Option<u32> {
        u32::try_from(bytes / 4).ok()
    }

    fn generate<'a>(
        &'a self,
        _prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.store(true, Ordering::SeqCst);
        let release = self.release.clone();
        let released = self.released.clone();
        Box::pin(stream::once(async move {
            while !released.load(Ordering::SeqCst) {
                release.notified().await;
            }
            Ok(ModelEvent::Completed {
                metadata: serde_json::Value::Null,
            })
        }))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: acyclic_harness::model::ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

fn budget_limits() -> SwarmBudgetLimits {
    SwarmBudgetLimits {
        max_active_agents: 2,
        max_total_agents: 2,
        max_recursion_depth: 1,
        max_model_steps: 128,
        max_output_bytes: 8 * 1024 * 1024,
        max_execution_time_ms: 60 * 60 * 1_000,
    }
}

async fn local_project(
    root: &std::path::Path,
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
        VolumeOwner::Project("local-budget-root".into()),
    )?;
    let head = host.create_volume(&project).await?;
    let workspace = workspace_ref(project.provider().clone(), &project.storage_name()?)?;
    host.apply(
        &workspace,
        Some(&head.generation),
        &[WorkspaceMutation::PutFile {
            path: "/root.txt".into(),
            bytes: ROOT_FILE.as_bytes().to_vec(),
        }],
        &IdempotencyKey::new("seed-budget-root")?,
    )
    .await?;
    Ok((host, stream, project))
}

fn model() -> Result<Model> {
    Model::new("mock", "persistent-local-budget", "1", json!({}))
}

async fn open_budget_swarm(
    root: &std::path::Path,
    model: Model,
    provider: Arc<dyn ModelProvider>,
    host: Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    stream: StreamClient<LocalStream>,
    project: VolumeRef,
    source: Arc<MeasuredUsage>,
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
    let mut config = LocalSwarmConfig::new(model, Limits::default())?;
    config.swarm_budget = Some(budget_limits());
    PersistentLocalSwarm::open_shared_with_bindings(
        root,
        config,
        provider,
        LocalSwarmBindings::default()
            .with_filesystem_fork_resolver(resolver)
            .with_swarm_budget(
                SwarmOwnerFence::new("local-budget-root-test", 0)?,
                budget_limits(),
                source,
            ),
    )
    .await
}

#[tokio::test]
async fn persistent_budget_root_restart_preserves_uncertainty() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path()).await?;
    let provider = RecoveryProvider::fail_once();
    let source = Arc::new(MeasuredUsage::default());
    let operation = OperationId::from_bytes([0xD1; 16]);
    let swarm = open_budget_swarm(
        directory.path(),
        model()?,
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
        source.clone(),
    )
    .await?;
    assert!(swarm
        .run_root(operation, "budgeted reply may be lost")
        .await
        .is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let before = swarm.budget_usage().await?.expect("budget is enabled");
    assert_eq!(before.active_agents, 1);
    assert_eq!(before.total_agents, 1);
    drop(swarm);

    let reopened = open_budget_swarm(
        directory.path(),
        model()?,
        provider.clone(),
        host,
        stream,
        project,
        source,
    )
    .await?;
    let error = reopened
        .run_root(operation, "budgeted reply may be lost")
        .await
        .expect_err("unknown provider outcome must remain fenced");
    assert!(
        matches!(error, Error::Indeterminate(_)),
        "unexpected recovery error: {error:?}"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        reopened.budget_usage().await?.expect("budget is enabled"),
        before
    );
    Ok(())
}

#[tokio::test]
async fn persistent_budget_root_cancellation_survives_reopen() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (host, stream, project) = local_project(directory.path()).await?;
    let provider = CancellationProvider::new();
    let swarm = open_budget_swarm(
        directory.path(),
        model()?,
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
        Arc::new(MeasuredUsage::default()),
    )
    .await?;
    let canceller = open_budget_swarm(
        directory.path(),
        model()?,
        provider.clone(),
        host.clone(),
        stream.clone(),
        project.clone(),
        Arc::new(MeasuredUsage::default()),
    )
    .await?;
    let task = swarm.root_task().await?;
    let operation = OperationId::from_bytes([0xD2; 16]);
    let run = tokio::spawn({
        let swarm = swarm.clone();
        async move { swarm.run_root(operation, "cancel budgeted root").await }
    });
    while !provider.started.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    canceller.cancel(task).await?;
    provider.released.store(true, Ordering::SeqCst);
    provider.release.notify_waiters();
    assert!(run.await.expect("root task join").is_err());
    assert_eq!(
        canceller.session(task).await?.phase,
        LocalSessionPhase::Cancelled
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    drop(canceller);
    drop(swarm);

    let reopened = open_budget_swarm(
        directory.path(),
        model()?,
        provider.clone(),
        host,
        stream,
        project,
        Arc::new(MeasuredUsage::default()),
    )
    .await?;
    assert!(reopened
        .run_root(operation, "cancel budgeted root")
        .await
        .is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        reopened.session(task).await?.phase,
        LocalSessionPhase::Cancelled
    );
    Ok(())
}
