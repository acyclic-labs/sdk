#![cfg(feature = "filesystem-local")]

//! Black-box budget checks for the production local swarm composition.
//!
//! These tests deliberately bind a real `LocalFs`/`LocalStream` resolver and
//! a host usage source through `LocalSwarmBindings::with_swarm_budget`.  The
//! model is deterministic, but fork publication, child admission, provider
//! metering, registry CAS, and restart recovery all use the production paths.

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{
        workspace_ref, FilesystemHost, LocalFilesystemForkResolver, LocalSessionPhase,
        LocalSwarmBindings, LocalSwarmConfig, PersistentLocalSwarm, WorkspaceMutation,
    },
    model::{Model, ModelContent, ModelEvent, ModelProvider, ModelRequest},
    resources::ProviderRef,
    swarm_budget::{
        SwarmBudgetLimits, SwarmForkRequest, SwarmResourceRequest, SwarmUsage, SwarmUsageSource,
    },
    swarm_budget_journal::SwarmBudgetJournal,
    Error, IdempotencyKey, OperationId, Result, TaskId,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
    StreamExt as _,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tempfile::tempdir;
use tokio::time::{timeout, Duration};

fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn task(byte: u8) -> TaskId {
    TaskId::from_bytes(operation(byte).into_bytes())
}

fn has_task(request: &ModelRequest, needle: &str) -> bool {
    request.messages.iter().any(
        |message| matches!(&message.content, ModelContent::Text(text) if text.contains(needle)),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderMode {
    Normal,
    BlockChildren,
    StorageFailureForChild,
    InvalidFailureForChild,
    Recursive,
}

/// A deterministic provider that exposes real dispatch concurrency and
/// restart behavior without introducing a fake scheduler or fake storage.
struct BudgetProvider {
    mode: ProviderMode,
    root_forked: AtomicBool,
    child_forked: AtomicBool,
    released: Arc<AtomicBool>,
    child_started: AtomicUsize,
    calls: AtomicUsize,
    requests: Mutex<Vec<Vec<u8>>>,
}

impl BudgetProvider {
    fn new(mode: ProviderMode) -> Arc<Self> {
        Arc::new(Self {
            mode,
            root_forked: AtomicBool::new(false),
            child_forked: AtomicBool::new(false),
            released: Arc::new(AtomicBool::new(false)),
            child_started: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        })
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
    }

    fn request_count(&self) -> usize {
        self.requests.lock().expect("request lock").len()
    }
}

impl ModelProvider for BudgetProvider {
    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        let request = prepared.request().clone();
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests
            .lock()
            .expect("request lock")
            .push(serde_json::to_vec(&request).expect("request bytes"));

        let is_child = has_task(&request, "child task:");
        let is_grandchild = has_task(&request, "child task: budget-grandchild");
        if is_child {
            self.child_started.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, ProviderMode::StorageFailureForChild) {
                return Box::pin(stream::once(async {
                    Err(Error::Storage("mock provider disconnected".into()))
                }));
            }
            if matches!(self.mode, ProviderMode::InvalidFailureForChild) {
                return Box::pin(stream::once(async {
                    Err(Error::Invalid("mock provider rejected turn".into()))
                }));
            }
            if matches!(self.mode, ProviderMode::Recursive)
                && !is_grandchild
                && !self.child_forked.swap(true, Ordering::SeqCst)
            {
                return Box::pin(stream::iter([
                    Ok(ModelEvent::ToolCall {
                        call_id: "budget-grandchild-call".into(),
                        name: "acyclic.fork_child".into(),
                        arguments: json!({
                            "child_operation": operation(0xC1).to_string(),
                            "task": "budget-grandchild",
                            "prompt": "complete the grandchild turn"
                        }),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]));
            }
            if matches!(self.mode, ProviderMode::BlockChildren) {
                let released = self.released.clone();
                return Box::pin(
                    stream::once(async move {
                        while !released.load(Ordering::SeqCst) {
                            tokio::task::yield_now().await;
                        }
                        Ok(ModelEvent::Content {
                            delta: "child result".into(),
                        })
                    })
                    .chain(stream::once(async {
                        Ok(ModelEvent::Completed {
                            metadata: Value::Null,
                        })
                    })),
                );
            }
            return Box::pin(stream::iter([
                Ok(ModelEvent::Content {
                    delta: "child result".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]));
        }

        if !self.root_forked.swap(true, Ordering::SeqCst) {
            let children = if matches!(self.mode, ProviderMode::Recursive) {
                vec![Ok(ModelEvent::ToolCall {
                    call_id: "budget-child-a".into(),
                    name: "acyclic.fork_child".into(),
                    arguments: json!({
                        "child_operation": operation(0xA1).to_string(),
                        "task": "budget-child-a",
                        "prompt": "prepare child a"
                    }),
                })]
            } else {
                vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "budget-child-a".into(),
                        name: "acyclic.fork_child".into(),
                        arguments: json!({
                            "child_operation": operation(0xA1).to_string(),
                            "task": "budget-child-a",
                            "prompt": "prepare child a"
                        }),
                    }),
                    Ok(ModelEvent::ToolCall {
                        call_id: "budget-child-b".into(),
                        name: "acyclic.fork_child".into(),
                        arguments: json!({
                            "child_operation": operation(0xB1).to_string(),
                            "task": "budget-child-b",
                            "prompt": "prepare child b"
                        }),
                    }),
                ]
            };
            return Box::pin(stream::iter(children.into_iter().chain(std::iter::once(
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ))));
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

#[derive(Default)]
struct RecordingUsageSource {
    counters: Mutex<BTreeMap<(OperationId, String), SwarmUsage>>,
    failing_operation: Mutex<Option<OperationId>>,
    reads: Mutex<Vec<(OperationId, String)>>,
}

impl RecordingUsageSource {
    fn fail_for(&self, operation: OperationId) {
        *self.failing_operation.lock().expect("failure lock") = Some(operation);
    }

    fn clear_failure(&self) {
        *self.failing_operation.lock().expect("failure lock") = None;
    }

    fn reads(&self) -> Vec<(OperationId, String)> {
        self.reads.lock().expect("reads lock").clone()
    }
}

impl SwarmUsageSource for RecordingUsageSource {
    fn provider_identity(&self) -> &str {
        "budget-e2e-measurement"
    }

    fn cumulative_usage(
        &self,
        operation_id: OperationId,
        dispatch_id: &IdempotencyKey,
    ) -> Result<SwarmUsage> {
        self.reads
            .lock()
            .map_err(|_| Error::Storage("reads lock poisoned".into()))?
            .push((operation_id, dispatch_id.0.clone()));
        if self
            .failing_operation
            .lock()
            .expect("failure lock")
            .as_ref()
            .is_some_and(|failed| *failed == operation_id)
        {
            return Err(Error::Storage("measurement store unavailable".into()));
        }
        Ok(self
            .counters
            .lock()
            .map_err(|_| Error::Storage("measurement lock poisoned".into()))?
            .get(&(operation_id, dispatch_id.0.clone()))
            .copied()
            .unwrap_or_default())
    }

    fn record_runtime_usage(
        &self,
        operation_id: OperationId,
        dispatch_id: &IdempotencyKey,
        usage: SwarmUsage,
    ) {
        if let Ok(mut counters) = self.counters.lock() {
            let entry = counters
                .entry((operation_id, dispatch_id.0.clone()))
                .or_default();
            entry.model_steps = entry.model_steps.max(usage.model_steps);
            entry.output_bytes = entry.output_bytes.max(usage.output_bytes);
            entry.execution_time_ms = entry.execution_time_ms.max(usage.execution_time_ms);
        }
    }

    fn restore_runtime_usage(
        &self,
        operation_id: OperationId,
        dispatch_id: &IdempotencyKey,
        usage: SwarmUsage,
    ) {
        self.record_runtime_usage(operation_id, dispatch_id, usage);
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
        VolumeOwner::Project("budget-e2e".into()),
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
        &IdempotencyKey::new("seed-budget-e2e-project")?,
    )
    .await?;
    Ok((host, stream, project))
}

fn limits(max_active_agents: u64, max_total_agents: u64, depth: u32) -> SwarmBudgetLimits {
    SwarmBudgetLimits {
        max_active_agents,
        max_total_agents,
        max_recursion_depth: depth,
        max_model_steps: 64,
        max_output_bytes: 64 * 1024,
        max_execution_time_ms: 60_000,
    }
}

fn harness_limits() -> Limits {
    let mut limits = Limits::default();
    limits.model_steps = 8;
    limits.render_bytes = 16 * 1024;
    limits
}

async fn bindings(
    host: Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    stream: StreamClient<LocalStream>,
    project: VolumeRef,
    owner: acyclic_harness::swarm_budget::SwarmOwnerFence,
    budget: SwarmBudgetLimits,
    source: Arc<RecordingUsageSource>,
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
    Ok(LocalSwarmBindings::default()
        .with_filesystem_fork_resolver(resolver)
        .with_swarm_budget(owner, budget, source))
}

async fn open_swarm(
    root: &Path,
    provider: Arc<BudgetProvider>,
    mode: ProviderMode,
    budget: SwarmBudgetLimits,
    owner: acyclic_harness::swarm_budget::SwarmOwnerFence,
    source: Arc<RecordingUsageSource>,
) -> Result<Arc<PersistentLocalSwarm>> {
    let (host, stream, project) = local_project(root).await?;
    let model = Model::new("mock", "budget-e2e", "1", json!({}))?;
    let mut config = LocalSwarmConfig::new(model, harness_limits())?;
    config.swarm_budget = Some(budget);
    let provider = if provider.mode == mode {
        provider
    } else {
        BudgetProvider::new(mode)
    };
    PersistentLocalSwarm::open_shared_with_bindings(
        root,
        config,
        provider,
        bindings(host, stream, project, owner, budget, source).await?,
    )
    .await
}

#[tokio::test]
async fn persistent_local_budget_binds_owner_source_and_reopens_without_redispatch() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let budget = limits(3, 4, 2);
    let owner = acyclic_harness::swarm_budget::SwarmOwnerFence::new("budget-e2e-owner", 0)?;
    let source = Arc::new(RecordingUsageSource::default());
    let provider = BudgetProvider::new(ProviderMode::Normal);
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        ProviderMode::Normal,
        budget,
        owner.clone(),
        source.clone(),
    )
    .await?;

    let output = swarm
        .run_root(operation(0x01), "start budget children")
        .await?;
    assert_eq!(output.text, "root result");
    let usage = swarm.budget_usage().await?.expect("budget projection");
    assert_eq!(usage.active_agents, 1);
    assert_eq!(usage.total_agents, 3);
    assert!(usage.consumed.model_steps >= 3);
    assert!(usage.consumed.output_bytes > 0);
    assert!(usage.consumed.execution_time_ms <= budget.max_execution_time_ms);
    assert_eq!(swarm.sessions().await.len(), 3);
    assert!(swarm
        .sessions()
        .await
        .into_iter()
        .all(|session| session.phase == LocalSessionPhase::Completed));
    let calls_before_reopen = provider.calls.load(Ordering::SeqCst);
    drop(swarm);

    let (host, stream, project) = local_project(directory.path()).await?;
    let model = Model::new("mock", "budget-e2e", "1", json!({}))?;
    let mut config = LocalSwarmConfig::new(model, harness_limits())?;
    config.swarm_budget = Some(budget);
    let reopened = PersistentLocalSwarm::open_shared_with_bindings(
        directory.path(),
        config,
        provider.clone(),
        bindings(host, stream, project, owner, budget, source).await?,
    )
    .await?;
    assert_eq!(reopened.budget_usage().await?.expect("budget"), usage);
    assert_eq!(
        reopened
            .run_root(operation(0x01), "start budget children")
            .await?,
        output
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls_before_reopen);
    Ok(())
}

#[tokio::test]
async fn persistent_local_budget_holds_one_sibling_active_and_releases_after_completion(
) -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let budget = limits(2, 3, 1);
    let owner = acyclic_harness::swarm_budget::SwarmOwnerFence::new("budget-capacity-owner", 0)?;
    let source = Arc::new(RecordingUsageSource::default());
    let provider = BudgetProvider::new(ProviderMode::BlockChildren);
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        ProviderMode::BlockChildren,
        budget,
        owner,
        source,
    )
    .await?;
    let run = tokio::spawn({
        let swarm = swarm.clone();
        async move {
            swarm
                .run_root(operation(0x02), "exercise sibling capacity")
                .await
        }
    });

    timeout(Duration::from_secs(5), async {
        loop {
            let usage = swarm.budget_usage().await?.expect("budget projection");
            if provider.child_started.load(Ordering::SeqCst) == 1 && usage.active_agents == 2 {
                break Ok::<(), Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Error::Storage("sibling admission barrier did not settle".into()))??;
    assert!(swarm.sessions().await.len() <= 2);
    provider.release();
    let _ = timeout(Duration::from_secs(10), run)
        .await
        .map_err(|_| Error::Storage("capacity fixture did not finish".into()))?;
    let usage = swarm.budget_usage().await?.expect("budget projection");
    assert_eq!(usage.active_agents, 1);
    assert_eq!(provider.child_started.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn persistent_local_budget_retains_capacity_when_measurement_is_uncertain() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let budget = limits(2, 3, 1);
    let owner = acyclic_harness::swarm_budget::SwarmOwnerFence::new("budget-uncertain-owner", 0)?;
    let source = Arc::new(RecordingUsageSource::default());
    source.fail_for(operation(0xA1));
    let provider = BudgetProvider::new(ProviderMode::Normal);
    let swarm = open_swarm(
        directory.path(),
        provider,
        ProviderMode::Normal,
        budget,
        owner,
        source.clone(),
    )
    .await?;
    let _ = swarm
        .run_root(operation(0x03), "retain uncertain child reservation")
        .await;
    let usage = swarm.budget_usage().await?.expect("budget projection");
    assert_eq!(usage.active_agents, 2);
    let child = swarm
        .sessions()
        .await
        .into_iter()
        .find(|session| session.task == task(0xA1))
        .ok_or_else(|| Error::NotFound("uncertain child session".into()))?;
    assert!(matches!(
        child.phase,
        LocalSessionPhase::Activating | LocalSessionPhase::Failed(_)
    ));

    source.clear_failure();
    let _ = swarm.cancel(child.task).await?;
    assert_eq!(
        swarm.budget_usage().await?.expect("budget").active_agents,
        1
    );
    Ok(())
}

#[tokio::test]
async fn persistent_local_budget_cancels_before_release_for_non_storage_provider_error(
) -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let budget = limits(2, 3, 1);
    let owner = acyclic_harness::swarm_budget::SwarmOwnerFence::new("budget-error-owner", 0)?;
    let source = Arc::new(RecordingUsageSource::default());
    let provider = BudgetProvider::new(ProviderMode::InvalidFailureForChild);
    let swarm = open_swarm(
        directory.path(),
        provider,
        ProviderMode::InvalidFailureForChild,
        budget,
        owner,
        source,
    )
    .await?;
    let _ = swarm
        .run_root(operation(0x04), "release after explicit provider denial")
        .await;
    let usage = swarm.budget_usage().await?.expect("budget projection");
    assert_eq!(usage.active_agents, 1);
    let child = swarm
        .sessions()
        .await
        .into_iter()
        .find(|session| session.task == task(0xA1))
        .ok_or_else(|| Error::NotFound("failed child session".into()))?;
    assert!(matches!(child.phase, LocalSessionPhase::Failed(_)));
    Ok(())
}

#[tokio::test]
async fn persistent_local_budget_recursive_reopen_does_not_charge_grandchild_again() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let budget = limits(4, 4, 2);
    let owner = acyclic_harness::swarm_budget::SwarmOwnerFence::new("budget-recursive-owner", 0)?;
    let source = Arc::new(RecordingUsageSource::default());
    let provider = BudgetProvider::new(ProviderMode::Recursive);
    let swarm = open_swarm(
        directory.path(),
        provider.clone(),
        ProviderMode::Recursive,
        budget,
        owner.clone(),
        source.clone(),
    )
    .await?;
    let output = swarm
        .run_root(operation(0x05), "recursive budget recovery")
        .await?;
    let first_usage = swarm.budget_usage().await?.expect("budget projection");
    assert_eq!(swarm.sessions().await.len(), 3);
    assert_eq!(
        swarm.session(task(0xC1)).await?.phase,
        LocalSessionPhase::Completed
    );
    let calls_before_reopen = provider.calls.load(Ordering::SeqCst);
    drop(swarm);

    let (host, stream, project) = local_project(directory.path()).await?;
    let model = Model::new("mock", "budget-e2e", "1", json!({}))?;
    let mut config = LocalSwarmConfig::new(model, harness_limits())?;
    config.swarm_budget = Some(budget);
    let reopened = PersistentLocalSwarm::open_shared_with_bindings(
        directory.path(),
        config,
        provider.clone(),
        bindings(host, stream, project, owner, budget, source).await?,
    )
    .await?;
    assert_eq!(reopened.budget_usage().await?.expect("budget"), first_usage);
    assert_eq!(
        reopened
            .run_root(operation(0x05), "recursive budget recovery")
            .await?,
        output
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls_before_reopen);
    assert!(provider.request_count() >= 3);
    Ok(())
}

#[tokio::test]
async fn persistent_local_budget_usage_failure_is_bound_to_exact_dispatch_identity() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let budget = limits(3, 4, 2);
    let owner = acyclic_harness::swarm_budget::SwarmOwnerFence::new("budget-identity-owner", 0)?;
    let source = Arc::new(RecordingUsageSource::default());
    source.fail_for(operation(0xA1));
    let provider = BudgetProvider::new(ProviderMode::Normal);
    let swarm = open_swarm(
        directory.path(),
        provider,
        ProviderMode::Normal,
        budget,
        owner,
        source.clone(),
    )
    .await?;

    let _ = swarm
        .run_root(operation(0x06), "fail only child A measurement")
        .await;
    let reads = source.reads();
    assert!(reads
        .iter()
        .any(|(operation_id, _)| *operation_id == operation(0xA1)));
    assert!(reads
        .iter()
        .any(|(operation_id, _)| *operation_id == operation(0xB1)));
    assert!(reads
        .iter()
        .filter(|(operation_id, _)| *operation_id == operation(0xB1))
        .all(|(_, dispatch_id)| !dispatch_id.is_empty()));
    assert!(
        swarm
            .sessions()
            .await
            .into_iter()
            .any(|session| session.task == task(0xB1)
                && session.phase == LocalSessionPhase::Completed)
    );
    Ok(())
}

#[tokio::test]
async fn local_stream_budget_cas_orders_concurrent_child_reservations() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (_host, stream, _project) = local_project(directory.path()).await?;
    let budget = limits(3, 3, 1);
    let owner = acyclic_harness::swarm_budget::SwarmOwnerFence::new("budget-cas-owner", 0)?;
    let session = operation(0xD1);
    let root_dispatch = IdempotencyKey::new("root-budget-cas")?;
    let _root = SwarmBudgetJournal::start_with_root_dispatch(
        &stream,
        session,
        owner.clone(),
        budget,
        root_dispatch,
    )
    .await?;
    let mut left = SwarmBudgetJournal::open(&stream, session).await?;
    let mut right = SwarmBudgetJournal::open(&stream, session).await?;
    let resources = SwarmResourceRequest {
        model_steps: 4,
        output_bytes: 1_024,
        execution_time_ms: 10_000,
    };
    let left_request = SwarmForkRequest {
        operation_id: operation(0xD2),
        idempotency_key: IdempotencyKey::new("budget-cas-left")?,
        parent_operation_id: Some(session),
        depth: 1,
        resources,
        admission_digest: None,
    };
    let right_request = SwarmForkRequest {
        operation_id: operation(0xD3),
        idempotency_key: IdempotencyKey::new("budget-cas-right")?,
        parent_operation_id: Some(session),
        depth: 1,
        resources,
        admission_digest: None,
    };
    let (left_result, right_result) = tokio::join!(
        left.reserve_child(left_request),
        right.reserve_child(right_request)
    );
    assert!(
        left_result.is_ok(),
        "left reservation failed: {left_result:?}"
    );
    assert!(
        right_result.is_ok(),
        "right reservation failed: {right_result:?}"
    );
    left.refresh().await?;
    assert_eq!(left.usage()?.active_agents, 3);
    assert_eq!(left.usage()?.total_agents, 3);
    Ok(())
}

#[tokio::test]
async fn local_stream_budget_fences_stale_owner_claim_and_cancel() -> Result<()> {
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (_host, stream, _project) = local_project(directory.path()).await?;
    let budget = limits(2, 2, 1);
    let owner = acyclic_harness::swarm_budget::SwarmOwnerFence::new("budget-stale-owner", 0)?;
    let session = operation(0xE1);
    let root_dispatch = IdempotencyKey::new("root-stale-owner")?;
    let mut primary = SwarmBudgetJournal::start_with_root_dispatch(
        &stream,
        session,
        owner.clone(),
        budget,
        root_dispatch,
    )
    .await?;
    let request = SwarmForkRequest {
        operation_id: operation(0xE2),
        idempotency_key: IdempotencyKey::new("stale-child")?,
        parent_operation_id: Some(session),
        depth: 1,
        resources: SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 1_024,
            execution_time_ms: 10_000,
        },
        admission_digest: None,
    };
    primary.reserve_child(request).await?;
    let mut stale = SwarmBudgetJournal::open(&stream, session).await?;
    let next_owner = primary
        .takeover(&owner, "budget-stale-owner-recovered")
        .await?;
    stale.refresh().await?;
    let claim = stale
        .claim_root_model_step(&owner, session, 0, [0xA5; 32])
        .await
        .expect_err("stale owner must lose root permit");
    assert!(matches!(claim, Error::Conflict(message) if message.contains("stale")));
    let cancel = stale
        .cancel(operation(0xE2), &owner)
        .await
        .expect_err("stale owner must not cancel the rebound child");
    assert!(matches!(cancel, Error::Conflict(message) if message.contains("stale")));
    primary.refresh().await?;
    assert_eq!(primary.descriptor()?.1, next_owner);
    assert_eq!(primary.usage()?.active_agents, 2);
    Ok(())
}
