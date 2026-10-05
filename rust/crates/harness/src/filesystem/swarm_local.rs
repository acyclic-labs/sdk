//! Durable-local recursive swarm composition.
//!
//! This module owns only the local application composition. Turn execution,
//! model-input admission, completed-batch publication, and provider recovery
//! remain the shared Harness paths. A swarm record is a small durable index of
//! task identities and activation outcomes; each task's journal and private
//! files are still owned by [`PersistentLocalHarness`].

use super::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemHost,
    InteractionApprovalAuthorization, InteractionOperatorAuthorizer, LocalHarnessTools,
    PersistentLocalHarness, workspace_ref,
};
use crate::{
    AgentId, Capabilities, Error, InteractionId, OperationId, Result, TaskId,
    batch_publication::ModelBatchPublication,
    communication::{DurableCommunication, MessageRequest, MessageTarget},
    communication_tools::{LocalTaskCancellationSource, WaitCancellationSource},
    conversation::{ConversationMessage, FileRef, Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, EffectGuarantee, SchemaRegistry, Scope},
    executor::{ExecutionEvent, TurnOutput},
    fork::{
        Capture, ForkPreparation, ForkRebindProof, ForkReport, ForkRequest, ForkSeed,
        ForkSelection, ResourceRevision,
    },
    interaction::{
        InteractionKind, InteractionOutcome, InteractionResolution, InteractionResponse,
        InteractionTicket,
    },
    model::{Model, ModelContent, ModelMessage, ModelProvider, ModelRole},
    model_input::{CompletedModelBoundary, InheritedModelContext},
    registry::ComponentIdentity,
    resources::{GenerationRef, ProviderRef, StreamRef},
    runtime::TaskRunLimits,
    store::StreamAggregate,
    swarm_budget::{
        SwarmAdmissionReceipt, SwarmBudgetLimits, SwarmDispatchToken, SwarmForkReservation,
        SwarmOwnerFence, SwarmResourceRequest, SwarmUsageSource, VerifiedForkPublication,
    },
    swarm_budget_journal::SwarmBudgetJournal,
    tool::{
        ModelToolContext, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection,
        ToolRegistry, ToolResult,
    },
};
use acyclic_fs::{LocalAuthorityBackend, LocalFs, LocalObjectBackend, LocalOptions};
use acyclic_stream::{AppendOutcome, LocalStream, LocalStreamLimits, StreamClient, StreamError};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
    task::{Context, Poll},
};
use tokio::sync::Mutex;

#[path = "swarm_read_projection.rs"]
mod read_projection;
#[path = "swarm_communication.rs"]
mod communication_host;
#[path = "swarm_workers.rs"]
mod child_workers;
pub use read_projection::{LocalSwarmAgent, LocalSwarmPage};
use read_projection::{
    page_by_cursor, page_from_sorted, recursive_agent_tree as project_recursive_agent_tree,
};

const REGISTRY_STREAM: &str = "swarm/records";
// The issuer-binding event gained a child-operation key and is no longer
// safely decodable as the original single-fork record. Keep recovery
// deliberately fenced at the registry boundary until an explicit migration
// can validate every legacy record.
const REGISTRY_VERSION: u32 = 2;
const LOCAL_DEPTH_LIMIT_ERROR: &str = "local swarm depth limit exceeded";
const LOCAL_DEPTH_LIMIT_REASON: &str = "depth_limit";
/// Completion payloads stay small enough for a Stream record. Larger outputs
/// are staged in the child agent-private volume and the registry retains only
/// their immutable reference and digest.
const MAX_INLINE_COMPLETION_BYTES: usize = 64 * 1024;
const MAX_SWARM_RECORD_BYTES: usize = 1024 * 1024;
const MAX_SWARM_ACTIVITY_EVENTS: usize = 65_536;

/// A spawned child turn remains owned by its activation future. Dropping the
/// activation must cancel the child task instead of detaching a model worker
/// that can continue dispatching effects after its caller has gone away.
struct AbortOnDrop<T> {
    handle: tokio::task::JoinHandle<T>,
}

impl<T> AbortOnDrop<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self { handle }
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = std::result::Result<T, tokio::task::JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().handle).poll(cx)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

type LocalFilesystemHost = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

/// Prepared through the typed fork path, with its live writer fence retained
/// until execution and durable completion have both finished.
struct LocalChildTurn {
    request: LocalForkRequest,
    stream: acyclic_stream::Stream<LocalStream>,
    harness: Arc<PersistentLocalHarness>,
    bundle: crate::Harness,
    admission: crate::runtime::TaskAdmissionRecord,
    max_steps: u32,
    cancelled: tokio::sync::watch::Receiver<bool>,
    _activation_guard: tokio::sync::OwnedMutexGuard<()>,
}

enum LocalChildActivation {
    Completed(LocalForkOutcome),
    Ready(Box<LocalChildTurn>),
}

/// LocalStream journals are process-exclusive. Keep one authenticated provider
/// handle per composition root so independently opened swarm handles observe
/// the same journal and CAS boundary.
static LOCAL_STREAM_CACHE: OnceLock<Mutex<BTreeMap<PathBuf, Weak<LocalStream>>>> = OnceLock::new();
static LOCAL_FILESYSTEM_CACHE: OnceLock<Mutex<BTreeMap<PathBuf, Weak<LocalFilesystemHost>>>> =
    OnceLock::new();
// Live cancellation is shared by handles of the same durable composition.
// This cache carries no authority: only a committed cancellation signals it.
#[derive(Default)]
struct LocalSwarmLiveState {
    cancellation: LocalTaskCancellationSource,
    task_gates: StdMutex<BTreeMap<TaskId, Weak<Mutex<()>>>>,
}

impl WaitCancellationSource for LocalSwarmLiveState {
    fn receiver(&self, task: TaskId) -> Option<tokio::sync::watch::Receiver<bool>> {
        self.cancellation.receiver(task)
    }

    fn cancel(&self, task: TaskId) -> Result<()> {
        self.cancellation.cancel(task)
    }
}

async fn cancellation_requested(receiver: &mut tokio::sync::watch::Receiver<bool>) -> Result<()> {
    while !*receiver.borrow_and_update() {
        receiver.changed().await.map_err(|_| {
            Error::Storage("task cancellation source closed".into())
        })?;
    }
    Ok(())
}

static LOCAL_LIVE_CACHE: OnceLock<
    StdMutex<BTreeMap<PathBuf, Weak<LocalSwarmLiveState>>>,
> = OnceLock::new();

fn shared_local_live_state(root: &Path) -> Result<Arc<LocalSwarmLiveState>> {
    let mut cache = LOCAL_LIVE_CACHE
        .get_or_init(|| StdMutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| Error::Storage("local swarm live-state cache lock poisoned".into()))?;
    let key = normalized_path(root);
    if let Some(source) = cache.get(&key).and_then(Weak::upgrade) {
        return Ok(source);
    }
    cache.retain(|_, source| source.strong_count() != 0);
    let source = Arc::new(LocalSwarmLiveState::default());
    cache.insert(key, Arc::downgrade(&source));
    Ok(source)
}

#[cfg(test)]
static EMPTY_REGISTRY_OPEN_BARRIER: OnceLock<
    StdMutex<Option<(PathBuf, Arc<tokio::sync::Barrier>)>>,
> = OnceLock::new();

#[cfg(test)]
async fn await_empty_registry_open_barrier(root: &Path) {
    let barrier = EMPTY_REGISTRY_OPEN_BARRIER
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .expect("empty-registry barrier lock")
        .as_ref()
        .filter(|(target, _)| target == root)
        .map(|(_, barrier)| barrier.clone());
    if let Some(barrier) = barrier {
        barrier.wait().await;
    }
}

#[cfg(not(test))]
async fn await_empty_registry_open_barrier(_root: &Path) {}

async fn shared_local_stream(root: PathBuf) -> Result<StreamClient<LocalStream>> {
    let root = normalized_path(&root);
    let cache = LOCAL_STREAM_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut cache = cache.lock().await;
    if let Some(provider) = cache.get(&root).and_then(Weak::upgrade) {
        return Ok(StreamClient::new(provider));
    }
    let provider = Arc::new(
        LocalStream::open(&root, LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );
    cache.retain(|_, provider| provider.strong_count() != 0);
    cache.insert(root, Arc::downgrade(&provider));
    Ok(StreamClient::new(provider))
}

async fn shared_local_filesystem(
    root: PathBuf,
    provider: ProviderRef,
) -> Result<Arc<LocalFilesystemHost>> {
    let root = normalized_path(&root);
    let cache = LOCAL_FILESYSTEM_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut cache = cache.lock().await;
    if let Some(host) = cache.get(&root).and_then(Weak::upgrade) {
        if host.provider == provider {
            return Ok(host);
        }
        return Err(Error::Conflict(
            "local filesystem root is already bound to another provider".into(),
        ));
    }
    let filesystem = LocalFs::local(LocalOptions::new(&root))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let host = Arc::new(FilesystemHost::new(filesystem, provider)?);
    cache.retain(|_, host| host.strong_count() != 0);
    cache.insert(root, Arc::downgrade(&host));
    Ok(host)
}

/// Durable session budget configuration for one local swarm.
#[derive(Clone, Debug)]
pub struct LocalSwarmBudgetConfig {
    /// Session-wide ceilings shared by root and recursive children.
    pub limits: SwarmBudgetLimits,
    /// Owner fence used for durable journal mutations and recovery takeover.
    pub owner: SwarmOwnerFence,
}

impl Default for LocalSwarmBudgetConfig {
    fn default() -> Self {
        Self {
            limits: SwarmBudgetLimits::default(),
            owner: SwarmOwnerFence {
                owner: "local-swarm".into(),
                generation: 0,
            },
        }
    }
}

impl LocalSwarmBudgetConfig {
    /// Validates session ceilings and owner identity before opening.
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.owner.validate()
    }
}

/// Configuration for one persistent local swarm.
#[derive(Clone, Debug)]
pub struct LocalSwarmConfig {
    /// Model identity pinned into every task descriptor.
    pub model: Model,
    /// Shared conversation and artifact bounds.
    pub limits: Limits,
    /// Maximum number of fork edges from one task.
    pub maximum_children: usize,
    /// Maximum recursive depth, where the root is depth zero.
    pub maximum_depth: usize,
    /// Optional task-local execution bounds retained with each child.
    pub run_limits: TaskRunLimits,
    /// Owner-selected source project for root and recursive child forks.
    /// When a concrete filesystem resolver is supplied, its project may fill
    /// this field during swarm open, but a persisted session still pins the
    /// resulting identity before any model work starts.
    pub project: Option<VolumeRef>,
    /// Durable session budget configuration. The journal remains the sole
    /// source of reservation and usage state.
    pub budget: LocalSwarmBudgetConfig,
}

/// Host-side observations for lazy local swarm qualification.
///
/// These events are diagnostic only. They are never persisted, exposed to a
/// model, or included in model request construction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum LocalSwarmObservation {
    /// A complete metadata session listing was returned.
    SessionList { returned: usize },
    /// A bounded metadata session page was returned.
    SessionPage { returned: usize },
    /// A metadata-only task snapshot was returned.
    SessionSnapshot { task: TaskId },
    /// A bounded event history page was returned.
    HistoryPage { task: TaskId, returned: usize },
    /// A bounded canonical message page was returned.
    MessagePage { task: TaskId, returned: usize },
    /// A bounded workspace directory page was returned.
    WorkspacePage { task: TaskId, returned: usize },
    /// One workspace file body was read explicitly.
    WorkspaceFile { task: TaskId, bytes: usize },
    /// A cold task harness was opened.
    HarnessOpened { task: TaskId },
    /// A model worker was about to be dispatched for a task.
    ModelWorkerStarted { task: TaskId },
}

/// Receives host-only local swarm observations for qualification and metrics.
pub trait LocalSwarmObserver: Send + Sync {
    /// Records one observation outside the model-visible request path.
    fn observe(&self, observation: LocalSwarmObservation);
}

/// Owner authenticated services shared by every local session.
///
/// Child sessions receive the same tool executors and durable wait store, but
/// each child still gets a fresh task context and private storage binding.
#[derive(Clone, Default)]
pub struct LocalSwarmBindings {
    /// Host used by the version pinned message and wait tools.
    pub communication_host: Option<Arc<dyn crate::runtime::DurableTaskHost>>,
    /// Owner retained wait journal, reconstructed by the caller on reopen.
    pub wait_store: Option<Arc<dyn crate::communication::DurableWaitStore>>,
    /// Live cancellation bridge for admitted tasks.
    pub cancellation: Option<Arc<dyn crate::communication_tools::WaitCancellationSource>>,
    /// Owner mediated publication of completed model/tool batches.
    pub model_batch_publisher: Option<Arc<dyn crate::batch_publication::ModelBatchPublisher>>,
    /// Owner-prepared model fork plans made available to the authenticated
    /// model-facing fork tool.
    pub model_fork_plans: Option<Arc<LocalModelForkPlans>>,
    /// Concrete local provider allocator. When supplied without an explicit
    /// plan index, the swarm builds one durable index around this resolver.
    pub filesystem_fork_resolver: Option<Arc<LocalFilesystemForkResolver>>,
    /// Optional host-only observation sink for lazy qualification metrics.
    pub observer: Option<Arc<dyn LocalSwarmObserver>>,
    /// Authenticated provider measurement source used for durable receipt
    /// issuance. The source must expose actual provider counters.
    pub budget_usage_source: Option<Arc<dyn SwarmUsageSource>>,
}

impl LocalSwarmBindings {
    /// Creates bindings for authenticated durable communication.
    #[must_use]
    pub fn communication(
        host: Arc<dyn crate::runtime::DurableTaskHost>,
        wait_store: Option<Arc<dyn crate::communication::DurableWaitStore>>,
        cancellation: Option<Arc<dyn crate::communication_tools::WaitCancellationSource>>,
    ) -> Self {
        Self {
            communication_host: Some(host),
            wait_store,
            cancellation,
            model_batch_publisher: None,
            model_fork_plans: None,
            filesystem_fork_resolver: None,
            observer: None,
            budget_usage_source: None,
        }
    }

    /// Adds the authenticated model batch publisher to these bindings.
    #[must_use]
    pub fn with_model_batch_publisher(
        mut self,
        publisher: Arc<dyn crate::batch_publication::ModelBatchPublisher>,
    ) -> Self {
        self.model_batch_publisher = Some(publisher);
        self
    }

    /// Adds owner-prepared recursive fork plans and the model-facing fork tool.
    #[must_use]
    pub fn with_model_fork_plans(mut self, plans: Arc<LocalModelForkPlans>) -> Self {
        self.model_fork_plans = Some(plans);
        self
    }

    /// Installs the concrete local Filesystem allocator used for dynamic
    /// model-selected children.
    #[must_use]
    pub fn with_filesystem_fork_resolver(
        mut self,
        resolver: Arc<LocalFilesystemForkResolver>,
    ) -> Self {
        self.filesystem_fork_resolver = Some(resolver);
        self
    }

    /// Installs a host-only observation sink. Observations never enter model
    /// requests or durable swarm state.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn LocalSwarmObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Binds the authenticated provider measurement source used for durable
    /// receipt issuance. Caller-supplied usage totals are not accepted.
    #[must_use]
    pub fn with_budget_usage_source(mut self, source: Arc<dyn SwarmUsageSource>) -> Self {
        self.budget_usage_source = Some(source);
        self
    }

    /// Reads the exact owner-retained admission for a task. Budget admission
    /// callers must use this record rather than reconstructing limits from a
    /// local fork request.
    pub async fn authenticated_admission(
        &self,
        task: TaskId,
    ) -> Result<crate::runtime::TaskAdmissionRecord> {
        self.communication_host
            .as_ref()
            .ok_or_else(|| {
                Error::Unauthorized("durable task admission host is not configured".into())
            })?
            .observe_admission(task)
            .await
    }

    fn tools_for(&self, parent: TaskId) -> Result<LocalHarnessTools> {
        let Some(host) = self.communication_host.clone() else {
            let mut tools = LocalHarnessTools::new();
            if let Some(plans) = &self.model_fork_plans {
                let mut registry = ToolRegistry::new();
                registry.register(local_fork_tool(parent, plans.clone()))?;
                tools = LocalHarnessTools::from_registry(registry);
            }
            return Ok(match &self.model_batch_publisher {
                Some(publisher) => tools.with_batch_publisher(publisher.clone()),
                None => tools,
            });
        };
        let mut registry =
            crate::communication_tools::communication_tools_with_wait_store_and_cancellation(
                host,
                self.wait_store.clone(),
                self.cancellation.clone(),
            )?;
        if let Some(plans) = &self.model_fork_plans {
            registry.register(local_fork_tool(parent, plans.clone()))?;
        }
        let tools = LocalHarnessTools::from_registry(registry).with_authenticated_task(parent);
        Ok(match &self.model_batch_publisher {
            Some(publisher) => tools.with_batch_publisher(publisher.clone()),
            None => tools,
        })
    }
}

/// One owner-prepared fork request that a model may select by stable
/// publication identity. Preparation and child allocation happen before the
/// model sees the tool; the completed model batch is the remaining admission
/// dependency.
#[derive(Clone)]
pub struct LocalModelForkPlan {
    /// Local parent task whose completed model step owns this fork.
    pub parent: TaskId,
    /// Completed model publication containing this child selection. Multiple
    /// children from one batch share this identity while each receives a
    /// distinct typed fork operation below.
    pub publication_operation: OperationId,
    /// Exact local request and child turn identity.
    pub request: LocalForkRequest,
    /// Provider-prepared report retained for publication and recovery.
    pub report: ForkReport,
    pub(crate) rebind_proof: Option<ForkRebindProof>,
    /// Exact recursive model boundary declaration.
    pub declaration: LocalInheritedModelDeclaration,
    /// Owner authenticated local filesystem host.
    pub host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    /// Shared local stream provider.
    pub stream: StreamClient<LocalStream>,
    /// Issuer used to bind the fresh child aggregate.
    pub issuer: AuthorityIssuer,
}

/// Model-selected child intent captured from an authenticated batch. The
/// publication operation is derived from the parent operation and step; the
/// model chooses only the fresh child operation and task content.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalForkIntent {
    /// Parent task that owns the completed model batch.
    pub parent: TaskId,
    /// Parent model turn identity.
    pub parent_operation: OperationId,
    /// Completed model step.
    pub parent_step: u32,
    /// Host-derived completed-batch publication identity. This remains the
    /// same for all children selected by one model batch.
    #[serde(default)]
    pub publication_operation: Option<OperationId>,
    /// Host-derived typed fork publication identity for this child.
    pub fork_operation: OperationId,
    /// Fresh child turn identity selected for the child task.
    pub child_operation: OperationId,
    /// Provider call identity used to replay an already selected tool result.
    #[serde(default)]
    pub call_id: Option<String>,
    /// Durable child task declaration.
    pub task: String,
    /// Fresh child user input.
    pub prompt: String,
}

impl LocalForkIntent {
    fn validate(&self) -> Result<()> {
        if self.parent.into_bytes() == [0; 16]
            || self.parent_operation.into_bytes() == [0; 16]
            || self.parent_step > 1_000_000
            || self.fork_operation.into_bytes() == [0; 16]
            || self.child_operation.into_bytes() == [0; 16]
            || self
                .publication_operation
                .is_some_and(|operation| operation.into_bytes() == [0; 16])
            || self.fork_operation == self.child_operation
            || self.publication_operation == Some(self.child_operation)
            || self.call_id.as_deref().is_some_and(str::is_empty)
            || self.task.trim().is_empty()
            || self.task.len() > 4 * 1024
            || self.prompt.len() > 64 * 1024
        {
            return Err(Error::Invalid(
                "model-selected fork intent is invalid".into(),
            ));
        }
        Ok(())
    }
}

fn child_fork_operation(publication: OperationId, child: OperationId) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.local-swarm.child-fork.v1\0");
    hasher.update(&publication.into_bytes());
    hasher.update(&child.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn rebind_report_history(
    report: &mut ForkReport,
    parent_revision: u64,
    proof: &ForkRebindProof,
) -> Result<()> {
    report.validate_with_rebind_proof(proof)?;
    let captured = report.captured_history_revision()?;
    if captured >= parent_revision {
        return Err(Error::Conflict(
            "fork report rebind requires an advanced publication revision".into(),
        ));
    }
    if report.original_request_digest.is_none() {
        report.original_request_digest =
            Some(crate::contract::canonical_json_digest(&report.request)?);
    }
    report.request.parent_revision = parent_revision;
    report.validate_with_rebind_proof(proof)
}

fn source_project_for_parent(
    parent_project: Option<VolumeRef>,
    parent_is_root: bool,
    root_project: &VolumeRef,
) -> Result<VolumeRef> {
    match parent_project {
        Some(project) => Ok(project),
        None if parent_is_root => Ok(root_project.clone()),
        None => Err(Error::Conflict(
            "local fork resolver is missing the direct parent project binding".into(),
        )),
    }
}

fn validate_recursive_depth(parent_depth: usize, maximum_depth: usize) -> Result<()> {
    if parent_depth >= maximum_depth {
        return Err(Error::Unauthorized(LOCAL_DEPTH_LIMIT_ERROR.into()));
    }
    Ok(())
}

fn is_depth_limit_denial(error: &Error) -> bool {
    matches!(error, Error::Unauthorized(message) if message == LOCAL_DEPTH_LIMIT_ERROR)
}

/// Owner allocator invoked only after the parent completed batch is
/// published. Implementations allocate child authorities/resources and return
/// the existing typed report/declaration plan used by activation.
pub trait LocalModelForkResolver: Send + Sync {
    /// Binds the resolver to the owning swarm after its durable registry has
    /// been opened. The default keeps custom resolvers independent of the
    /// application composition.
    fn bind_swarm(&self, _swarm: Weak<PersistentLocalSwarm>) -> Result<()> {
        Ok(())
    }

    /// Returns the non-secret fingerprint of the owner binding used to issue
    /// child authorities. The fingerprint is journaled with each intent so a
    /// reopen using a different host secret fails closed.
    fn issuer_binding_digest(&self) -> Option<[u8; 32]> {
        None
    }

    /// Returns the owner-selected source project when this resolver binds one.
    fn source_project(&self) -> Option<VolumeRef> {
        None
    }

    /// Performs deterministic policy checks before a model fork intent is
    /// retained. Implementations may use the authoritative parent session;
    /// the resolver repeats the check during publication for stale-policy
    /// protection.
    fn preflight_depth<'a>(&'a self, _parent: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    /// Resolves one authenticated model intent against its exact publication.
    fn resolve<'a>(
        &'a self,
        intent: LocalForkIntent,
        publication: ModelBatchPublication,
    ) -> BoxFuture<'a, Result<LocalModelForkPlan>>;
}

/// Filesystem-backed allocator used by the durable local swarm composition.
///
/// The resolver deliberately allocates only after a model-selected intent has
/// been matched to an admitted completed batch. The existing
/// [`FilesystemForkPreparer`] then claims both child volumes and records the
/// exact request before any child aggregate is activated. The project volume
/// is supplied by the owner so project read/write authority never comes from a
/// model argument or an ancestor volume-wide grant.
pub struct LocalFilesystemForkResolver {
    host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    stream: StreamClient<LocalStream>,
    stream_provider: ProviderRef,
    project: VolumeRef,
    /// Host-held secret used to derive child authority signing keys. The
    /// operation identity is only a domain input and is never itself a key.
    issuer_secret: Option<[u8; 32]>,
    target: StdMutex<Option<Weak<PersistentLocalSwarm>>>,
}

impl LocalFilesystemForkResolver {
    /// Binds the local provider pair and the owner-selected source project.
    pub fn new(
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        stream_provider: ProviderRef,
        project: VolumeRef,
    ) -> Result<Self> {
        stream_provider.validate()?;
        if stream_provider.family() != "stream"
            || project.class() != VolumeClass::Project
            || project.provider() != &host.provider
        {
            return Err(Error::Invalid(
                "local fork resolver requires a local stream and project provider".into(),
            ));
        }
        Ok(Self {
            host,
            stream,
            stream_provider,
            project,
            issuer_secret: None,
            target: StdMutex::new(None),
        })
    }

    /// Binds durable owner secret material used across restart and replay.
    #[must_use]
    pub fn with_host_secret(mut self, secret: [u8; 32]) -> Result<Self> {
        if secret == [0; 32] {
            return Err(Error::Invalid(
                "local fork issuer secret cannot be zero".into(),
            ));
        }
        self.issuer_secret = Some(secret);
        Ok(self)
    }

    fn target(&self) -> Result<Arc<PersistentLocalSwarm>> {
        let target = self
            .target
            .lock()
            .map_err(|_| Error::Storage("local fork resolver lock poisoned".into()))?;
        target
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| Error::Conflict("local fork resolver is not bound to a swarm".into()))
    }

    fn child_authority(intent: &LocalForkIntent) -> Authority {
        Authority {
            kind: AggregateKind::Conversation,
            id: format!("local-child-{}", intent.child_operation),
        }
    }

    fn child_issuer(
        child: &Authority,
        operation: OperationId,
        secret: [u8; 32],
    ) -> AuthorityIssuer {
        let mut key = blake3::Hasher::new_keyed(&secret);
        key.update(b"acyclic.local-swarm.child-authority.v1\0");
        key.update(child.id.as_bytes());
        key.update(&operation.into_bytes());
        AuthorityIssuer::new(
            "local-swarm-fork",
            *key.finalize().as_bytes(),
            child.clone(),
        )
    }

    /// Returns the authenticated filesystem host owned by this resolver.
    #[must_use]
    pub fn host(&self) -> Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>> {
        self.host.clone()
    }

    /// Returns the stream provider shared by all conversation aggregates in
    /// this local composition.
    #[must_use]
    pub fn stream(&self) -> StreamClient<LocalStream> {
        self.stream.clone()
    }

    /// Returns the provider identity used for stream history references.
    #[must_use]
    pub fn stream_provider(&self) -> ProviderRef {
        self.stream_provider.clone()
    }
}

impl LocalModelForkResolver for LocalFilesystemForkResolver {
    fn bind_swarm(&self, swarm: Weak<PersistentLocalSwarm>) -> Result<()> {
        let mut target = self
            .target
            .lock()
            .map_err(|_| Error::Storage("local fork resolver lock poisoned".into()))?;
        *target = Some(swarm);
        Ok(())
    }

    fn issuer_binding_digest(&self) -> Option<[u8; 32]> {
        self.issuer_secret.map(|secret| {
            *blake3::keyed_hash(&secret, b"acyclic.local-swarm.issuer-binding.v1").as_bytes()
        })
    }

    fn source_project(&self) -> Option<VolumeRef> {
        Some(self.project.clone())
    }

    fn preflight_depth<'a>(&'a self, parent: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let swarm = self.target()?;
            let parent_session = swarm.session(parent).await?;
            validate_recursive_depth(parent_session.depth, swarm.config.maximum_depth)
        })
    }

    fn resolve<'a>(
        &'a self,
        intent: LocalForkIntent,
        publication: ModelBatchPublication,
    ) -> BoxFuture<'a, Result<LocalModelForkPlan>> {
        Box::pin(async move {
            intent.validate()?;
            let swarm = self.target()?;
            let parent_harness = swarm.open_session(intent.parent).await?;
            let parent_session = swarm.session(intent.parent).await?;
            validate_recursive_depth(parent_session.depth, swarm.config.maximum_depth)?;
            let existing_task_ids = swarm
                .records
                .lock()
                .await
                .values()
                .filter(|session| session.parent == Some(intent.parent))
                .map(|session| session.task)
                .collect::<BTreeSet<_>>();
            let existing_children = existing_task_ids.len();
            let pending_children = if let Some(plans) = swarm.bindings.model_fork_plans.as_ref() {
                plans
                    .pending_for_parent(intent.parent, &existing_task_ids)
                    .await?
            } else {
                0
            };
            if existing_children + pending_children > swarm.config.maximum_children {
                return Err(Error::Unauthorized(
                    "local swarm child limit exceeded".into(),
                ));
            }
            let issuer_secret = self.issuer_secret.ok_or_else(|| {
                Error::Unauthorized("local fork resolver has no durable host secret".into())
            })?;
            let storage = parent_harness.storage();
            let inherited = swarm.declarations.lock().await.get(&intent.parent).cloned();
            let verified = match inherited {
                Some(declaration) => {
                    let context = declaration.context(swarm.config.limits)?;
                    storage
                        .verified_inherited_model_fork_boundary(
                            &publication,
                            swarm.config.limits,
                            &context,
                        )
                        .await?
                }
                None => {
                    storage
                        .verified_model_fork_boundary(&publication, swarm.config.limits)
                        .await?
                }
            };
            let boundary = verified.boundary().clone();
            let parent = verified.parent();
            let parent_revision = parent.reducer().revision();
            if parent.reducer().authority() != storage.conversation() || parent_revision == 0 {
                return Err(Error::Conflict(
                    "fork publication parent aggregate changed during allocation".into(),
                ));
            }

            // A restart may have committed the typed report and declaration
            // before the live plan cache was reconstructed. Reuse that exact
            // durable allocation after re-verifying the completed boundary;
            // never allocate another pair of child volumes for one operation.
            let child_task = TaskId::from_bytes(intent.child_operation.into_bytes());
            let existing_report = swarm.reports.lock().await.get(&child_task).cloned();
            let existing_declaration = swarm.declarations.lock().await.get(&child_task).cloned();
            if let (Some(report), Some(declaration)) = (existing_report, existing_declaration) {
                let source_project = report
                    .request
                    .selections
                    .iter()
                    .find_map(|selection| match &selection.revision {
                        ResourceRevision::Project { volume, .. } => Some(volume.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| Error::Invalid("fork report has no project selection".into()))?;
                let parent_reader = Arc::new(FilesystemContentVerifier::new(
                    self.host.clone(),
                    storage.verifier(),
                    storage.owner_scope().clone(),
                    swarm.config.limits.file_bytes,
                )?);
                let preparer = FilesystemForkPreparer::new(
                    self.host.clone(),
                    parent.reducer().clone(),
                    storage.verifier(),
                    storage.owner_scope().clone(),
                    source_project,
                    self.stream_provider.clone(),
                    parent_reader,
                )?;
                let mut original_request = report.request.clone();
                original_request.parent_revision = report.captured_history_revision()?;
                let rebind_proof = preparer
                    .authenticate_rebind_records(&original_request)
                    .await?;
                report.validate_with_rebind_proof(&rebind_proof)?;
                let seed = report.clone().into_seed_with_rebind_proof(&rebind_proof)?;
                if seed.operation_id != intent.fork_operation
                    || seed.parent != *storage.conversation()
                    || seed.child != Self::child_authority(&intent)
                    || seed.child_agent != AgentId::from_bytes(intent.child_operation.into_bytes())
                    || declaration.boundary != boundary
                {
                    return Err(Error::Conflict(
                        "durable fork allocation does not match the selected publication".into(),
                    ));
                }
                let request = LocalForkRequest {
                    parent: intent.parent,
                    parent_operation: intent.parent_operation,
                    parent_step: intent.parent_step,
                    fork_operation: Some(intent.fork_operation),
                    child_operation: intent.child_operation,
                    child_authority: Some(seed.child.clone()),
                    child_agent: Some(seed.child_agent),
                    task: intent.task,
                    prompt: intent.prompt,
                };
                request.validate()?;
                return Ok(LocalModelForkPlan {
                    parent: intent.parent,
                    publication_operation: publication.operation_id,
                    request,
                    report,
                    rebind_proof: Some(rebind_proof),
                    declaration,
                    host: self.host.clone(),
                    stream: self.stream.clone(),
                    issuer: Self::child_issuer(&seed.child, intent.child_operation, issuer_secret),
                });
            }

            let child_authority = Self::child_authority(&intent);
            let child_agent = AgentId::from_bytes(intent.child_operation.into_bytes());
            let child_issuer =
                Self::child_issuer(&child_authority, intent.child_operation, issuer_secret);
            let child_private = VolumeRef::new(
                self.host.provider.clone(),
                format!("local-private-{}", intent.child_operation),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(child_agent),
            )?;
            // A recursive child must fork from the project generation that
            // its direct parent received.  The root resolver's configured
            // project is only the fallback for the root task; reusing it at
            // every depth would silently discard edits made by an ancestor.
            let parent_project =
                swarm
                    .reports
                    .lock()
                    .await
                    .get(&intent.parent)
                    .and_then(|report| {
                        report
                            .request
                            .selections
                            .iter()
                            .zip(&report.captures)
                            .find_map(|(selection, capture)| {
                                if !matches!(&selection.revision, ResourceRevision::Project { .. })
                                {
                                    return None;
                                }
                                match capture {
                                    Capture::Captured(resource) => match &resource.revision {
                                        ResourceRevision::Project { volume, .. } => {
                                            Some(volume.clone())
                                        }
                                        _ => None,
                                    },
                                    _ => None,
                                }
                            })
                    });
            let source_project = source_project_for_parent(
                parent_project,
                parent_session.parent.is_none(),
                &self.project,
            )?;
            let project_ref = workspace_ref(
                source_project.provider().clone(),
                &source_project.storage_name()?,
            )?;
            let project_head = self.host.resolve(&project_ref).await?;
            let source_generation = project_head.generation;
            let project_owner = match source_project.owner() {
                VolumeOwner::Project(owner) => VolumeOwner::Project(owner.clone()),
                _ => {
                    return Err(Error::Invalid(
                        "local fork resolver source project has an invalid owner".into(),
                    ));
                }
            };
            let child_project = VolumeRef::new(
                self.host.provider.clone(),
                format!("local-project-{}", intent.child_operation),
                VolumeClass::Project,
                project_owner,
            )?;
            let history = ResourceRevision::History(StreamRef::new(
                self.stream_provider.clone(),
                parent.reducer().authority().stream_path()?.into_bytes(),
                Some(parent_revision.to_string()),
            )?);
            let inherited_through_sequence = parent
                .reducer()
                .conversation()
                .map(|conversation| conversation.messages.len() as u64)
                .ok_or_else(|| {
                    Error::Conflict(
                        "fork publication parent has no authoritative conversation".into(),
                    )
                })?;
            let mut request = ForkRequest {
                operation_id: intent.fork_operation,
                parent: parent.reducer().authority().clone(),
                parent_revision,
                child: child_authority.clone(),
                child_agent,
                attached_agents: Vec::new(),
                preparation: ForkPreparation {
                    child_project_volume: child_project,
                    child_private_volume: child_private,
                    inherited_through_sequence,
                    maximum_inherited_messages: swarm.config.limits.context_messages as u64,
                    maximum_inherited_bytes: swarm.config.limits.file_bytes,
                    maximum_inherited_references: swarm.config.limits.attachments as u32,
                },
                selections: vec![
                    ForkSelection {
                        required: true,
                        revision: history,
                    },
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::Project {
                            volume: source_project.clone(),
                            generation: source_generation,
                        },
                    },
                ],
                boundary: None,
                model_boundary: None,
            };
            storage
                .attach_model_fork_references(&verified, &mut request)
                .await?;
            // Admit the child before the filesystem preparer can claim either
            // workspace. The scheduler reserves the returned admission before
            // invoking this resolver; the exact prompt, parent, model,
            // grants, and limits are persisted in the owner registry first.
            let child_admission = swarm
                .admit_local_child_turn(
                    child_task,
                    intent.child_operation,
                    &intent.prompt,
                    intent.parent,
                    &parent_harness,
                )
                .await?;
            let _ = child_admission;
            let parent_reader = Arc::new(FilesystemContentVerifier::new(
                self.host.clone(),
                storage.verifier(),
                storage.owner_scope().clone(),
                swarm.config.limits.file_bytes,
            )?);
            let preparer = FilesystemForkPreparer::new(
                self.host.clone(),
                parent.reducer().clone(),
                storage.verifier(),
                storage.owner_scope().clone(),
                source_project.clone(),
                self.stream_provider.clone(),
                parent_reader,
            )?;
            let report = parent.prepare_fork(&preparer, request.clone()).await?;
            let declaration = LocalInheritedModelDeclaration {
                boundary,
                suffix: vec![ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(format!(
                        "child task: {}; parent: {}; identity: {}; fresh scratch: true",
                        intent.task,
                        intent.parent,
                        TaskId::from_bytes(intent.child_operation.into_bytes())
                    )),
                }],
            };
            declaration.context(swarm.config.limits)?;
            Ok(LocalModelForkPlan {
                parent: intent.parent,
                publication_operation: publication.operation_id,
                request: LocalForkRequest {
                    parent: intent.parent,
                    parent_operation: intent.parent_operation,
                    parent_step: intent.parent_step,
                    fork_operation: Some(intent.fork_operation),
                    child_operation: intent.child_operation,
                    child_authority: Some(child_authority),
                    child_agent: Some(child_agent),
                    task: intent.task,
                    prompt: intent.prompt,
                },
                report,
                rebind_proof: Some(preparer.authenticate_rebind(&request).await?),
                declaration,
                host: self.host.clone(),
                stream: self.stream.clone(),
                issuer: child_issuer,
            })
        })
    }
}

impl LocalModelForkPlan {
    /// Validates the owner prepared identities before exposing the plan to a
    /// model-facing tool.
    pub fn validate(&self) -> Result<()> {
        self.request.validate()?;
        if self.publication_operation.into_bytes() == [0; 16] {
            return Err(Error::Invalid(
                "model fork plan publication identity is empty".into(),
            ));
        }
        let seed = if let Some(proof) = &self.rebind_proof {
            self.report.validate_with_rebind_proof(proof)?;
            self.report.clone().into_seed_with_rebind_proof(proof)?
        } else {
            self.report.validate()?;
            self.report.clone().into_seed()?
        };
        let fork_operation = self.request.fork_operation.ok_or_else(|| {
            Error::Invalid("model fork plan requires a fork operation identity".into())
        })?;
        if seed.operation_id != fork_operation
            || self.request.child_authority.as_ref() != Some(&seed.child)
            || self.request.child_agent != Some(seed.child_agent)
        {
            return Err(Error::Conflict(
                "model fork plan identities do not match its prepared seed".into(),
            ));
        }
        Ok(())
    }
}

/// Durable in-process index of owner-prepared model fork plans. The plan
/// payload itself is also written to the swarm registry once selected, so a
/// publisher retry can recover it without trusting model output.
pub struct LocalModelForkPlans {
    plans: Mutex<BTreeMap<(OperationId, OperationId), LocalModelForkPlan>>,
    intents: Mutex<BTreeMap<(OperationId, OperationId), LocalForkIntent>>,
    /// Journal sequence for each selected tool call. BTreeMap ordering is a
    /// storage detail and does not represent the model's ordered batch.
    intent_order: Mutex<BTreeMap<(OperationId, OperationId), u64>>,
    issuer_bindings: Mutex<BTreeMap<(OperationId, OperationId), [u8; 32]>>,
    completed: Mutex<BTreeMap<OperationId, [u8; 32]>>,
    resolver: Option<Arc<dyn LocalModelForkResolver>>,
    journal: Mutex<Option<StreamClient<LocalStream>>>,
}

fn replay_fork_publication_completion(
    event: &StoredEvent,
    completed: &mut BTreeMap<OperationId, [u8; 32]>,
) -> Result<()> {
    let StoredEvent::ForkPublicationCompleted { operation, digest } = event else {
        return Ok(());
    };
    if operation.into_bytes() == [0; 16] || *digest == [0; 32] {
        return Err(Error::Conflict(
            "persisted model fork publication receipt is invalid".into(),
        ));
    }
    if let Some(existing) = completed.get(operation)
        && existing != digest
    {
        return Err(Error::Conflict(
            "durable model fork publication result changed on retry".into(),
        ));
    }
    completed.insert(*operation, *digest);
    Ok(())
}

fn replay_fork_intent(
    position: u64,
    intent: LocalForkIntent,
    issuer_digest: Option<[u8; 32]>,
    intents: &mut BTreeMap<(OperationId, OperationId), LocalForkIntent>,
    intent_order: &mut BTreeMap<(OperationId, OperationId), u64>,
    issuer_bindings: &mut BTreeMap<(OperationId, OperationId), [u8; 32]>,
) -> Result<()> {
    intent.validate()?;
    let key = (intent.fork_operation, intent.child_operation);
    if let Some(existing) = intents.get(&key)
        && existing != &intent
    {
        return Err(Error::Conflict(
            "durable model fork intent changed during recovery".into(),
        ));
    }
    intents.insert(key, intent);
    intent_order.entry(key).or_insert(position);
    if let Some(digest) = issuer_digest {
        if digest == [0; 32] {
            return Err(Error::Conflict(
                "persisted local fork issuer binding is empty".into(),
            ));
        }
        if let Some(existing) = issuer_bindings.get(&key)
            && existing != &digest
        {
            return Err(Error::Conflict(
                "durable model fork issuer binding changed during recovery".into(),
            ));
        }
        issuer_bindings.insert(key, digest);
    }
    Ok(())
}

fn replay_fork_intent_record(
    position: u64,
    record: StoredRecord,
    intents: &mut BTreeMap<(OperationId, OperationId), LocalForkIntent>,
    intent_order: &mut BTreeMap<(OperationId, OperationId), u64>,
    issuer_bindings: &mut BTreeMap<(OperationId, OperationId), [u8; 32]>,
) -> Result<()> {
    match record.event {
        StoredEvent::ForkIntent { intent } => replay_fork_intent(
            position,
            intent,
            None,
            intents,
            intent_order,
            issuer_bindings,
        )?,
        StoredEvent::ForkIntentSelected {
            intent,
            issuer_digest,
        } => replay_fork_intent(
            position,
            intent,
            issuer_digest,
            intents,
            intent_order,
            issuer_bindings,
        )?,
        StoredEvent::ForkIssuerBinding {
            operation,
            child_operation,
            digest,
        } => {
            if digest == [0; 32] {
                return Err(Error::Conflict(
                    "persisted local fork issuer binding is empty".into(),
                ));
            }
            let key = (operation, child_operation);
            if let Some(existing) = issuer_bindings.get(&key)
                && existing != &digest
            {
                return Err(Error::Conflict(
                    "durable model fork issuer binding changed during recovery".into(),
                ));
            }
            issuer_bindings.insert(key, digest);
        }
        _ => {}
    }
    Ok(())
}

impl Default for LocalModelForkPlans {
    fn default() -> Self {
        Self {
            plans: Mutex::new(BTreeMap::new()),
            intents: Mutex::new(BTreeMap::new()),
            intent_order: Mutex::new(BTreeMap::new()),
            issuer_bindings: Mutex::new(BTreeMap::new()),
            completed: Mutex::new(BTreeMap::new()),
            resolver: None,
            journal: Mutex::new(None),
        }
    }
}

impl LocalModelForkPlans {
    /// Creates an empty owner plan index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Installs the owner allocator used by model-selected fork intents.
    #[must_use]
    pub fn with_resolver(mut self, resolver: Arc<dyn LocalModelForkResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    async fn preflight_depth(&self, parent: TaskId) -> Result<()> {
        if let Some(resolver) = &self.resolver {
            resolver.preflight_depth(parent).await?;
        }
        Ok(())
    }

    /// Binds a concrete allocator to this opened swarm without exposing the
    /// swarm's mutable registry to the model-facing tool.
    pub fn bind_swarm(&self, swarm: Weak<PersistentLocalSwarm>) -> Result<()> {
        if let Some(resolver) = &self.resolver {
            resolver.bind_swarm(swarm)?;
        }
        Ok(())
    }

    /// Binds the owner registry used to persist model-selected intents before
    /// the provider publication callback is entered.
    pub async fn bind_journal(&self, registry: StreamClient<LocalStream>) -> Result<()> {
        let stream = registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let records = load_records(&stream).await?;
        let mut intents = self.intents.lock().await;
        let mut intent_order = self.intent_order.lock().await;
        let mut issuer_bindings = self.issuer_bindings.lock().await;
        let mut completed = self.completed.lock().await;
        for (position, record) in records.into_iter().enumerate() {
            replay_fork_publication_completion(&record.event, &mut completed)?;
            replay_fork_intent_record(
                position as u64,
                record,
                &mut intents,
                &mut intent_order,
                &mut issuer_bindings,
            )?;
        }
        *self.journal.lock().await = Some(registry);
        Ok(())
    }

    /// Counts distinct child operations selected for one parent. This index
    /// is consulted before provider allocation, so a restart cannot allocate
    /// a fresh child pair merely because the live plan cache is empty.
    pub async fn pending_for_parent(
        &self,
        parent: TaskId,
        existing_tasks: &BTreeSet<TaskId>,
    ) -> Result<usize> {
        // Each PersistentLocalSwarm handle owns its own plan index. Refresh
        // the selected intents from the shared registry before reserving a
        // child slot so a sibling process cannot allocate from a stale cache.
        self.refresh_journal_state().await?;
        let intents = self.intents.lock().await;
        Ok(intents
            .values()
            .filter(|intent| intent.parent == parent)
            .filter(|intent| {
                !existing_tasks.contains(&TaskId::from_bytes(intent.child_operation.into_bytes()))
            })
            .map(|intent| intent.child_operation)
            .collect::<BTreeSet<_>>()
            .len())
    }

    async fn refresh_journal_state(&self) -> Result<()> {
        let Some(registry) = self.journal.lock().await.clone() else {
            return Ok(());
        };
        let stream = registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let records = load_records(&stream).await?;
        let mut intents = self.intents.lock().await;
        let mut intent_order = self.intent_order.lock().await;
        let mut issuer_bindings = self.issuer_bindings.lock().await;
        let mut completed = self.completed.lock().await;
        for (position, record) in records.into_iter().enumerate() {
            replay_fork_publication_completion(&record.event, &mut completed)?;
            replay_fork_intent_record(
                position as u64,
                record,
                &mut intents,
                &mut intent_order,
                &mut issuer_bindings,
            )?;
        }
        Ok(())
    }

    async fn record_intent(&self, intent: LocalForkIntent) -> Result<()> {
        intent.validate()?;
        let issuer_digest = self
            .resolver
            .as_ref()
            .and_then(|resolver| resolver.issuer_binding_digest());
        // Reconcile before checking the live cache. A second swarm handle may
        // have selected this intent since this handle last observed the
        // registry.
        self.refresh_journal_state().await?;
        if self
            .existing_intent_status(&intent, issuer_digest)
            .await?
        {
            return Ok(());
        }
        let registry = self.journal.lock().await.clone();
        let Some(registry) = registry else {
            self.insert_live_intent(intent, issuer_digest).await;
            return Ok(());
        };
        let stream = registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let event = StoredEvent::ForkIntentSelected {
            intent: intent.clone(),
            issuer_digest,
        };
        let mut last_conflict = None;
        for _ in 0..4 {
            // The observed tail is paired with this exact event. A competing
            // selector therefore yields a CAS conflict instead of silently
            // appending behind it.
            let (observed_tail, _) = load_records_with_tail(&stream).await?;
            self.refresh_journal_state().await?;
            if self
                .existing_intent_status(&intent, issuer_digest)
                .await?
            {
                return Ok(());
            }
            match append_record_at(&stream, event.clone(), observed_tail).await {
                Ok(()) => {
                    self.refresh_journal_state().await?;
                    if self
                        .existing_intent_status(&intent, issuer_digest)
                        .await?
                    {
                        return Ok(());
                    }
                    return Err(Error::Conflict(
                        "durable model fork intent was not visible after append".into(),
                    ));
                }
                Err(error @ Error::Conflict(_)) => {
                    last_conflict = Some(error);
                    self.refresh_journal_state().await?;
                    if self
                        .existing_intent_status(&intent, issuer_digest)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Err(error) => {
                    // A storage error may be reported after the append was
                    // committed. Reconcile once before exposing uncertainty;
                    // never append a second selection blindly.
                    self.refresh_journal_state().await?;
                    if self
                        .existing_intent_status(&intent, issuer_digest)
                        .await?
                    {
                        return Ok(());
                    }
                    return Err(error);
                }
            }
        }
        Err(last_conflict.unwrap_or_else(|| {
            Error::Conflict("local fork intent append did not reach a stable tail".into())
        }))
    }

    async fn existing_intent_status(
        &self,
        intent: &LocalForkIntent,
        issuer_digest: Option<[u8; 32]>,
    ) -> Result<bool> {
        let key = (intent.fork_operation, intent.child_operation);
        let intents = self.intents.lock().await;
        if intents.values().any(|existing| {
            existing.child_operation == intent.child_operation && existing != intent
        }) {
            return Err(Error::Conflict(
                "child operation is already bound to another durable fork intent".into(),
            ));
        }
        if let Some(existing) = intents.get(&key)
            && existing != intent
        {
            return Err(Error::Conflict(
                "model fork publication identity is already bound to another intent".into(),
            ));
        }
        if intents.contains_key(&key) {
            let actual = self.issuer_bindings.lock().await.get(&key).copied();
            if actual != issuer_digest {
                return Err(Error::Conflict(
                    "model fork issuer binding is missing or changed on retry".into(),
                ));
            }
            return Ok(true);
        }
        Ok(false)
    }

    async fn insert_live_intent(
        &self,
        intent: LocalForkIntent,
        issuer_digest: Option<[u8; 32]>,
    ) {
        let key = (intent.fork_operation, intent.child_operation);
        self.intents.lock().await.insert(key, intent);
        if let Some(digest) = issuer_digest {
            self.issuer_bindings.lock().await.insert(key, digest);
        }
    }

    async fn resolve_intents(
        &self,
        publication: ModelBatchPublication,
    ) -> Result<Vec<LocalModelForkPlan>> {
        self.refresh_journal_state().await?;
        let mut intents = self
            .intents
            .lock()
            .await
            .values()
            .filter(|intent| intent.publication_operation == Some(publication.operation_id))
            .cloned()
            .collect::<Vec<_>>();
        let intent_order = self.intent_order.lock().await.clone();
        intents.sort_by_key(|intent| {
            let key = (intent.fork_operation, intent.child_operation);
            (
                intent_order.get(&key).copied().unwrap_or(u64::MAX),
                intent.call_id.clone().unwrap_or_default(),
                key,
            )
        });
        if intents.is_empty() {
            let prepared = self.get_for_publication(publication.operation_id).await;
            if !prepared.is_empty() {
                return Ok(prepared);
            }
            return Err(Error::Conflict(
                "completed fork publication has no durable intent".into(),
            ));
        }
        let existing = self
            .plans
            .lock()
            .await
            .iter()
            .filter(|(_, plan)| plan.publication_operation == publication.operation_id)
            .map(|(key, plan)| (*key, plan.clone()))
            .collect::<BTreeMap<_, _>>();
        let resolver = self.resolver.clone();
        let mut plans = Vec::with_capacity(intents.len());
        for intent in intents {
            if publication.parent_operation != intent.parent_operation
                || publication.step != intent.parent_step
                || intent.publication_operation != Some(publication.operation_id)
            {
                return Err(Error::Conflict(
                    "completed fork publication does not match its selected intent".into(),
                ));
            }
            let key = (intent.fork_operation, intent.child_operation);
            if let Some(resolver) = resolver.as_ref()
                && let Some(expected) = resolver.issuer_binding_digest()
                && self.issuer_bindings.lock().await.get(&key) != Some(&expected)
            {
                return Err(Error::Conflict(
                    "durable model fork issuer binding does not match the owner secret".into(),
                ));
            }
            if let Some(plan) = existing.get(&key) {
                plans.push(plan.clone());
                continue;
            }
            let resolver = resolver.clone().ok_or_else(|| {
                Error::Unsupported("local model fork resolver is not bound".into())
            })?;
            let plan = resolver.resolve(intent, publication.clone()).await?;
            self.register(plan.clone()).await?;
            plans.push(plan);
        }
        Ok(plans)
    }

    /// Registers one exact prepared report before a model turn begins.
    pub async fn register(&self, plan: LocalModelForkPlan) -> Result<()> {
        plan.validate()?;
        let operation = plan.request.fork_operation.ok_or_else(|| {
            Error::Invalid("model fork plan requires a fork operation identity".into())
        })?;
        let key = (operation, plan.request.child_operation);
        let mut plans = self.plans.lock().await;
        if let Some(existing) = plans.get(&key)
            && existing.request != plan.request
        {
            return Err(Error::Conflict(
                "model fork operation is already bound to another request".into(),
            ));
        }
        plans.insert(key, plan);
        Ok(())
    }

    async fn get_for_publication(&self, operation: OperationId) -> Vec<LocalModelForkPlan> {
        self.plans
            .lock()
            .await
            .iter()
            .filter(|(_, plan)| plan.publication_operation == operation)
            .map(|(_, plan)| plan.clone())
            .collect()
    }

    async fn has_intent(&self, operation: OperationId) -> Result<bool> {
        self.refresh_journal_state().await?;
        Ok(self
            .intents
            .lock()
            .await
            .values()
            .any(|intent| intent.publication_operation == Some(operation)))
    }

    async fn mark_completed(&self, operation: OperationId, digest: [u8; 32]) -> Result<()> {
        if operation.into_bytes() == [0; 16] || digest == [0; 32] {
            return Err(Error::Invalid(
                "model fork publication completion identity is empty".into(),
            ));
        }
        self.refresh_journal_state().await?;
        if let Some(existing) = self.completed.lock().await.get(&operation).copied() {
            if existing != digest {
                return Err(Error::Conflict(
                    "model fork publication result changed on retry".into(),
                ));
            }
            return Ok(());
        }
        let registry = self.journal.lock().await.clone();
        let Some(registry) = registry else {
            self.completed.lock().await.insert(operation, digest);
            return Ok(());
        };
        let stream = registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let event = StoredEvent::ForkPublicationCompleted { operation, digest };
        let mut last_conflict = None;
        for _ in 0..4 {
            let (observed_tail, _) = load_records_with_tail(&stream).await?;
            self.refresh_journal_state().await?;
            if let Some(existing) = self.completed.lock().await.get(&operation).copied() {
                if existing != digest {
                    return Err(Error::Conflict(
                        "model fork publication result changed on retry".into(),
                    ));
                }
                return Ok(());
            }
            match append_record_at(&stream, event.clone(), observed_tail).await {
                Ok(()) => {
                    self.refresh_journal_state().await?;
                    if self.completed.lock().await.get(&operation).copied() == Some(digest) {
                        return Ok(());
                    }
                    return Err(Error::Conflict(
                        "durable model fork publication receipt was not visible after append"
                            .into(),
                    ));
                }
                Err(error @ Error::Conflict(_)) => {
                    last_conflict = Some(error);
                    self.refresh_journal_state().await?;
                    if let Some(existing) = self.completed.lock().await.get(&operation).copied() {
                        if existing != digest {
                            return Err(Error::Conflict(
                                "model fork publication result changed on retry".into(),
                            ));
                        }
                        return Ok(());
                    }
                }
                Err(error) => {
                    // The storage error may have followed a committed append.
                    // Reconcile the registry before exposing uncertainty and
                    // never append a second completion receipt blindly.
                    self.refresh_journal_state().await?;
                    if self.completed.lock().await.get(&operation).copied() == Some(digest) {
                        return Ok(());
                    }
                    return Err(error);
                }
            }
        }
        Err(last_conflict.unwrap_or_else(|| {
            Error::Conflict("local fork publication receipt did not reach a stable tail".into())
        }))
    }

    async fn completed(&self, operation: OperationId) -> Result<Option<[u8; 32]>> {
        self.refresh_journal_state().await?;
        Ok(self.completed.lock().await.get(&operation).copied())
    }

    async fn replay_intent(&self, input: &LocalForkToolInput) -> Result<Option<LocalForkIntent>> {
        self.refresh_journal_state().await?;
        Ok(self
            .intents
            .lock()
            .await
            .values()
            .find(|intent| {
                intent.child_operation == input.child_operation
                    && intent.task == input.task
                    && intent.prompt == input.prompt
                    && input
                        .fork_operation
                        .is_none_or(|operation| operation == intent.fork_operation)
            })
            .cloned())
    }
}

/// Concrete publisher used by a shared local swarm. It turns a completed
/// model batch into the existing typed report publication and child activation
/// path, including recursive child publishers.
pub struct LocalModelForkPublisher {
    plans: Arc<LocalModelForkPlans>,
    target: Arc<StdMutex<Option<Weak<PersistentLocalSwarm>>>>,
}

impl LocalModelForkPublisher {
    async fn run_scheduled_child(
        owner: Weak<PersistentLocalSwarm>,
        turn: Box<LocalChildTurn>,
    ) -> Result<LocalForkOutcome> {
        let LocalChildTurn {
            request, stream, harness, bundle, admission, max_steps, cancelled, _activation_guard,
        } = *turn;
        let child = TaskId::from_bytes(request.child_operation.into_bytes());
        {
            let swarm = owner.upgrade().ok_or_else(|| {
                Error::Conflict("local child owner was dropped before dispatch".into())
            })?;
            swarm.observe(LocalSwarmObservation::ModelWorkerStarted { task: child });
        }
        // The registry owns this future; it must not own the composition
        // strongly across model/tool awaits.
        let output = PersistentLocalSwarm::run_owned_child_turn(
            harness.clone(), bundle, admission, request.clone(), max_steps, cancelled,
        ).await;
        let swarm = owner.upgrade().ok_or_else(|| {
            Error::Conflict("local child owner was dropped before outcome publication".into())
        })?;
        swarm.finish_child_turn(&stream, child, request.child_operation, &harness, output).await
    }

    fn new(plans: Arc<LocalModelForkPlans>) -> Self {
        Self {
            plans,
            target: Arc::new(StdMutex::new(None)),
        }
    }

    fn bind(&self, target: Weak<PersistentLocalSwarm>) -> Result<()> {
        let mut current = self
            .target
            .lock()
            .map_err(|_| Error::Storage("local fork publisher lock poisoned".into()))?;
        *current = Some(target);
        Ok(())
    }

    fn target(&self) -> Result<Option<Arc<PersistentLocalSwarm>>> {
        let current = self
            .target
            .lock()
            .map_err(|_| Error::Storage("local fork publisher lock poisoned".into()))?;
        Ok(current.as_ref().and_then(Weak::upgrade))
    }
}

impl crate::batch_publication::ModelBatchPublisher for LocalModelForkPublisher {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            name: "acyclic.local-recursive-fork".into(),
            version: "1".into(),
            digest: *blake3::hash(b"acyclic.local-recursive-fork:v1").as_bytes(),
        }
    }

    fn guarantee(&self) -> EffectGuarantee {
        EffectGuarantee::IdempotentRetry
    }

    fn linearizable_reconciliation(&self) -> bool {
        true
    }

    fn publish<'a>(&'a self, publication: ModelBatchPublication) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let publication_digest = crate::contract::canonical_json_digest(&publication)?;
            if let Some(completed) = self
                .plans
                .completed(publication.operation_id)
                .await?
            {
                if completed != publication_digest {
                    return Err(Error::Conflict(
                        "model fork publication result changed on retry".into(),
                    ));
                }
                return Ok(());
            }
            // The publisher is registered for every completed model batch;
            // most batches do not select the fork tool and must complete as a
            // durable no-op. A selected fork is still required to carry its
            // intent, so resolve_intents retains the fail-closed path.
            if !self.plans.has_intent(publication.operation_id).await?
                && self
                    .plans
                    .get_for_publication(publication.operation_id)
                    .await
                    .is_empty()
            {
                return Ok(());
            }
            let plans = self.plans.resolve_intents(publication.clone()).await?;
            let swarm = self.target()?.ok_or_else(|| {
                Error::Conflict("local recursive fork publisher is not bound to a swarm".into())
            })?;
            swarm.workers.ensure_open().await?;
            let first_parent = plans
                .first()
                .map(|plan| plan.parent)
                .ok_or_else(|| Error::Conflict("fork publication resolved no plans".into()))?;
            let parent_harness = swarm.open_session(first_parent).await?;
            let mut parent = parent_harness
                .conversation_aggregate(swarm.config.limits)
                .await?;
            let mut prepared = Vec::with_capacity(plans.len());
            for mut plan in plans {
                if publication.operation_id != plan.publication_operation
                    || publication.parent_operation != plan.request.parent_operation
                    || publication.step != plan.request.parent_step
                {
                    return Err(Error::Conflict(
                        "completed model publication does not match its fork plan".into(),
                    ));
                }
                if plan.parent != first_parent {
                    return Err(Error::Conflict(
                        "one model publication selected children from different parents".into(),
                    ));
                }
                let child = TaskId::from_bytes(plan.request.child_operation.into_bytes());
                if swarm
                    .session(child)
                    .await
                    .is_ok_and(|session| session.phase == LocalSessionPhase::Completed)
                {
                    continue;
                }
                // A retry may arrive after the parent publication succeeded
                // but before child binding. Reuse the exact seed retained by
                // the durable admission only when the parent aggregate proves
                // that publication already exists; otherwise continue through
                // the rebind intent below.
                if let Ok(existing_seed) = swarm.published_seed(child).await {
                    if parent.reducer().fork(&existing_seed.child) == Some(&existing_seed) {
                        prepared.push((plan, existing_seed));
                        continue;
                    }
                }
                // Multiple children selected by one completed batch publish
                // sequentially on the same parent stream. Their immutable
                // captures remain stable, while each seed pins the parent's
                // current stream revision before its append.
                let current_revision = parent.reducer().revision();
                if plan.report.request.parent_revision != current_revision {
                    let proof = plan.rebind_proof.as_ref().ok_or_else(|| {
                        Error::Conflict("rebound fork plan has no preparation proof".into())
                    })?;
                    let old_seed = plan.report.clone().into_seed_with_rebind_proof(proof)?;
                    rebind_report_history(&mut plan.report, current_revision, proof)?;
                    let rebound_seed = plan.report.clone().into_seed_with_rebind_proof(proof)?;
                    // Persist an authenticated rebind intent before changing
                    // either allocation journal, so a crash between report
                    // publication and seed mutation can be replayed without
                    // inventing a new allocation.
                    plan.host.rebind_fork_seed(&old_seed, &rebound_seed).await?;
                }
                let seed = swarm
                    .publish_child_seed_with_publication(
                        plan.request.clone(),
                        plan.stream.clone(),
                        plan.issuer.clone(),
                        &mut parent,
                        plan.report.clone(),
                        publication.clone(),
                        plan.declaration.clone(),
                        plan.rebind_proof.as_ref(),
                    )
                    .await?;
                prepared.push((plan, seed));
            }
            // Every child is now durably admitted and bound to the parent
            // aggregate. Only after that barrier may a child model dispatch.
            for (plan, seed) in prepared {
                let child = TaskId::from_bytes(plan.request.child_operation.into_bytes());
                if swarm.workers.contains(child).await {
                    continue;
                }
                let activation = Box::pin(swarm.prepare_published_child(
                    plan.request, plan.host, plan.stream, plan.issuer, &parent, &seed,
                )).await?;
                if let LocalChildActivation::Ready(turn) = activation {
                    let owner = Arc::downgrade(&swarm);
                    swarm.workers.enqueue(child, Self::run_scheduled_child(owner, turn)).await?;
                }
            }
            self.plans
                .mark_completed(
                    publication.operation_id,
                    publication_digest,
                )
                .await
        })
    }

    fn reconcile<'a>(
        &'a self,
        publication: ModelBatchPublication,
    ) -> BoxFuture<'a, Result<Option<()>>> {
        Box::pin(async move {
            let plans = self
                .plans
                .get_for_publication(publication.operation_id)
                .await;
            if plans.is_empty() {
                if !self.plans.has_intent(publication.operation_id).await? {
                    return Ok(Some(()));
                }
                return Ok(None);
            }
            let digest = crate::contract::canonical_json_digest(&publication)?;
            if self.plans.completed(publication.operation_id).await? == Some(digest) {
                return Ok(Some(()));
            }
            for plan in plans {
                if publication.operation_id != plan.publication_operation {
                    return Err(Error::Conflict(
                        "reconciled model publication does not match its fork plan".into(),
                    ));
                }
            }
            Ok(None)
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalForkToolInput {
    #[serde(default)]
    fork_operation: Option<OperationId>,
    child_operation: OperationId,
    task: String,
    prompt: String,
}

fn bind_local_fork_input(
    context: &ModelToolContext,
    parent: TaskId,
    invocation: &ToolInvocation,
) -> Result<(LocalForkToolInput, OperationId, OperationId)> {
    context.validate_invocation(invocation)?;
    let mut input: LocalForkToolInput = serde_json::from_value(invocation.arguments.clone())
        .map_err(|error| Error::Invalid(format!("local fork arguments are invalid: {error}")))?;
    let publication_operation = context.publication_operation();
    let fork_operation = child_fork_operation(publication_operation, input.child_operation);
    if input
        .fork_operation
        .is_some_and(|value| value != fork_operation)
        || input.child_operation == fork_operation
    {
        return Err(Error::Conflict(
            "local fork tool identity is not bound to this completed model batch".into(),
        ));
    }
    if context.task_id.is_some_and(|task| task != parent) {
        return Err(Error::Unauthorized(
            "local fork tool task binding differs from the authenticated parent".into(),
        ));
    }
    // Replay lookup must be scoped to this completed batch even when the
    // model omitted the optional fork operation from its arguments.
    input.fork_operation = Some(fork_operation);
    Ok((input, publication_operation, fork_operation))
}

fn selected_fork_tool_result(
    fork_operation: OperationId,
    child_operation: OperationId,
) -> ToolResult {
    ToolResult {
        value: json!({
            "status": "selected_after_completed_batch",
            "fork_operation": fork_operation.to_string(),
            "child_operation": child_operation.to_string(),
        }),
    }
}

fn denied_fork_tool_result(
    fork_operation: OperationId,
    child_operation: OperationId,
) -> ToolResult {
    ToolResult {
        value: json!({
            "status": "denied",
            "reason": LOCAL_DEPTH_LIMIT_REASON,
            "fork_operation": fork_operation.to_string(),
            "child_operation": child_operation.to_string(),
        }),
    }
}

fn local_fork_output_schema() -> Value {
    json!({
        "oneOf": [
            {
                "type": "object",
                "required": ["status", "fork_operation", "child_operation"],
                "properties": {
                    "status": {"const": "selected_after_completed_batch"},
                    "fork_operation": {"type": "string"},
                    "child_operation": {"type": "string"}
                },
                "additionalProperties": false
            },
            {
                "type": "object",
                "required": ["status", "reason", "fork_operation", "child_operation"],
                "properties": {
                    "status": {"const": "denied"},
                    "reason": {"const": "depth_limit"},
                    "fork_operation": {"type": "string"},
                    "child_operation": {"type": "string"}
                },
                "additionalProperties": false
            }
        ]
    })
}

struct LocalForkToolExecutor {
    parent: TaskId,
    plans: Arc<LocalModelForkPlans>,
}

impl ToolExecutor for LocalForkToolExecutor {
    fn execute<'a>(&'a self, _invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "local fork requires model batch context".into(),
            ))
        })
    }

    fn execute_in_model_batch<'a>(
        &'a self,
        context: ModelToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let (input, publication_operation, fork_operation) =
                bind_local_fork_input(&context, self.parent, &invocation)?;
            // Depth is a deterministic owner policy. Reject before retaining
            // an intent so an out-of-depth request cannot become an orphaned
            // publication or a later unknown allocation failure.
            if let Err(error) = self.plans.preflight_depth(self.parent).await {
                if is_depth_limit_denial(&error) {
                    return Ok(denied_fork_tool_result(fork_operation, input.child_operation));
                }
                return Err(error);
            }
            let intent = LocalForkIntent {
                parent: self.parent,
                parent_operation: context.parent_operation,
                parent_step: context.step,
                publication_operation: Some(publication_operation),
                fork_operation,
                child_operation: input.child_operation,
                call_id: Some(invocation.call_id),
                task: input.task,
                prompt: input.prompt,
            };
            self.plans.record_intent(intent).await?;
            Ok(selected_fork_tool_result(fork_operation, input.child_operation))
        })
    }

    fn reconcile_in_model_batch<'a>(
        &'a self,
        context: ModelToolContext,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let (input, _publication_operation, fork_operation) =
                bind_local_fork_input(&context, self.parent, &invocation)?;
            if let Some(intent) = self.plans.replay_intent(&input).await? {
                return Ok(Some(selected_fork_tool_result(
                    intent.fork_operation,
                    intent.child_operation,
                )));
            }
            if let Err(error) = self.plans.preflight_depth(self.parent).await {
                if is_depth_limit_denial(&error) {
                    return Ok(Some(denied_fork_tool_result(
                        fork_operation,
                        input.child_operation,
                    )));
                }
                return Err(error);
            }
            Ok(None)
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            if invocation.name != "acyclic.fork_child" {
                return Ok(None);
            }
            let input: LocalForkToolInput =
                serde_json::from_value(invocation.arguments).map_err(|error| {
                    Error::Invalid(format!("local fork arguments are invalid: {error}"))
                })?;
            let Some(intent) = self.plans.replay_intent(&input).await? else {
                return Ok(None);
            };
            Ok(Some(selected_fork_tool_result(
                intent.fork_operation,
                intent.child_operation,
            )))
        })
    }
}

struct LocalForkToolProjection;

impl ToolProjection for LocalForkToolProjection {
    fn project(&self, _invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

fn local_fork_tool(parent: TaskId, plans: Arc<LocalModelForkPlans>) -> Tool {
    Tool {
        definition: ToolDefinition {
            name: "acyclic.fork_child".into(),
            revision: "2".into(),
            description:
                "Request an owner-prepared recursive child after this model batch completes".into(),
            input_schema: json!({
                "type": "object",
                "required": ["child_operation", "task", "prompt"],
                "properties": {
                    "fork_operation": {"type": "string"},
                    "child_operation": {"type": "string"},
                    "task": {"type": "string", "minLength": 1, "maxLength": 4096},
                    "prompt": {"type": "string", "maxLength": 65536}
                },
                "additionalProperties": false
            }),
            output_schema: local_fork_output_schema(),
            model_output_schema: local_fork_output_schema(),
        },
        executor: Arc::new(LocalForkToolExecutor { parent, plans }),
        projection: Arc::new(LocalForkToolProjection),
    }
}

impl LocalSwarmConfig {
    /// Constructs a bounded local composition with conservative fork limits.
    pub fn new(model: Model, limits: Limits) -> Result<Self> {
        limits.validate()?;
        let config = Self {
            model,
            limits,
            maximum_children: 8,
            maximum_depth: 8,
            run_limits: TaskRunLimits::default(),
            project: None,
            budget: LocalSwarmBudgetConfig::default(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Pins the owner-selected source project used by recursive fork
    /// publication. The volume identity is carried into every reopened root
    /// session and is never accepted from a model request.
    pub fn with_project(mut self, project: VolumeRef) -> Result<Self> {
        if project.class() != VolumeClass::Project {
            return Err(Error::Invalid(
                "local swarm source project must be a project volume".into(),
            ));
        }
        project.validate()?;
        self.project = Some(project);
        self.validate()?;
        Ok(self)
    }

    /// Applies explicit session-wide resource ceilings to the local
    /// composition.
    pub fn with_budget_limits(mut self, limits: SwarmBudgetLimits) -> Result<Self> {
        limits.validate()?;
        self.budget.limits = limits;
        self.validate()?;
        Ok(self)
    }

    /// Applies an authenticated owner fence to the local budget journal.
    pub fn with_budget_owner(mut self, owner: SwarmOwnerFence) -> Result<Self> {
        owner.validate()?;
        self.budget.owner = owner;
        self.validate()?;
        Ok(self)
    }

    /// Validates application bounds before opening any provider.
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.budget.validate()?;
        self.run_limits.validate()?;
        if self.maximum_children == 0 || self.maximum_depth == 0 {
            return Err(Error::Invalid("local swarm bounds must be positive".into()));
        }
        if let Some(project) = &self.project {
            project.validate()?;
            if project.class() != VolumeClass::Project {
                return Err(Error::Invalid(
                    "local swarm source project must be a project volume".into(),
                ));
            }
        }
        Ok(())
    }
}

/// A requested child activation. Its model is intentionally absent: the
/// parent operation and completed boundary determine what can be inherited.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalForkRequest {
    /// Parent task in the durable session registry.
    pub parent: TaskId,
    /// Parent operation whose completed batch is the fork boundary.
    pub parent_operation: OperationId,
    /// Completed model/tool step to inherit.
    pub parent_step: u32,
    /// Stable operation identity for the provider fork publication itself.
    #[serde(default)]
    pub fork_operation: Option<OperationId>,
    /// New child operation identity. A caller can retry this exact request.
    pub child_operation: OperationId,
    /// Exact child conversation allocated by the typed fork preparation.
    #[serde(default)]
    pub child_authority: Option<Authority>,
    /// Exact child agent allocated by the typed fork preparation.
    #[serde(default)]
    pub child_agent: Option<AgentId>,
    /// Explicit child task declaration appended after the frozen prefix.
    pub task: String,
    /// Fresh child user input; it is staged in the child's private volume.
    pub prompt: String,
}

impl LocalForkRequest {
    /// Checks the request fields that do not depend on the parent registry.
    pub fn validate(&self) -> Result<()> {
        if self.parent.into_bytes() == [0; 16]
            || self.parent_operation.into_bytes() == [0; 16]
            || self.child_operation.into_bytes() == [0; 16]
            || self
                .fork_operation
                .is_some_and(|value| value.into_bytes() == [0; 16])
        {
            return Err(Error::Invalid(
                "local fork identities must be nonzero".into(),
            ));
        }
        if self.fork_operation == Some(self.child_operation) {
            return Err(Error::Invalid(
                "fork publication and child turn require distinct operation identities".into(),
            ));
        }
        if self.task.trim().is_empty() || self.task.len() > 4 * 1024 {
            return Err(Error::Invalid(
                "local fork task is empty or too large".into(),
            ));
        }
        if let Some(child) = &self.child_authority {
            if child.kind != AggregateKind::Conversation {
                return Err(Error::Invalid(
                    "local fork child authority is not a conversation".into(),
                ));
            }
            child.stream_path()?;
        }
        if self.prompt.len() > 64 * 1024 {
            return Err(Error::Invalid("local fork prompt is too large".into()));
        }
        Ok(())
    }
}

/// Durable state of one local task session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "message")]
pub enum LocalSessionPhase {
    /// The task is the root or was admitted as a child.
    Ready,
    /// A fork publication was admitted but the child has not completed.
    Activating,
    /// The child completed its requested turn.
    Completed,
    /// The owner durably cancelled this task.
    Cancelled,
    /// The last activation or turn failed with a stable message.
    Failed(String),
}

/// Lazy session descriptor returned by listing and lookup.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSwarmSession {
    /// Stable task identity.
    pub task: TaskId,
    /// Direct parent, if this is a child.
    pub parent: Option<TaskId>,
    /// Recursive depth from the root.
    pub depth: usize,
    /// Child task declaration, retained without starting the task.
    pub task_description: String,
    /// Pinned operation used for the latest turn.
    pub operation: Option<OperationId>,
    /// Current durable lifecycle phase.
    pub phase: LocalSessionPhase,
}

/// Authoritative host snapshot for one task. Child descriptors are read from
/// the durable swarm index; conversation and private generation state are
/// loaded only for the requested task.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSwarmSnapshot {
    /// Requested task descriptor.
    pub session: LocalSwarmSession,
    /// Direct children retained by the owner admission index.
    pub children: Vec<LocalSwarmSession>,
    /// Current authoritative conversation stream event tail. This is an
    /// event cursor, not the model message sequence or message count.
    pub conversation_revision: u64,
    /// Generation observed from the task's private filesystem volume. A
    /// metadata-only snapshot leaves this unknown rather than fabricating a
    /// generation value.
    pub workspace_generation: Option<GenerationRef>,
}

/// One interaction visible to the owner-facing local API. Request and
/// resolution remain ref-only; callers must use the task's authenticated
/// storage to resolve or read referenced content.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSwarmApproval {
    /// Task that owns the interaction journal.
    pub task: TaskId,
    /// Durable request ticket.
    pub ticket: InteractionTicket,
    /// Durable response, when one has been committed.
    pub resolution: Option<InteractionResolution>,
}

/// A durable sender receipt returned after the owner host accepts a message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSwarmMessage {
    /// Sending task.
    pub sender: TaskId,
    /// Receiving task.
    pub recipient: TaskId,
    /// Caller supplied retry identity.
    pub message_id: OperationId,
    /// Immutable staged message content.
    pub payload: FileRef,
}

/// Result of one child activation and turn.
#[derive(Clone, Debug)]
pub struct LocalForkOutcome {
    /// Newly activated child identity.
    pub child: TaskId,
    /// Operation identity used by the child turn.
    pub operation: OperationId,
    /// Durable child model result.
    pub output: TurnOutput,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredCompletionRef {
    operation: OperationId,
    file: FileRef,
    digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LocalOperatorChoice {
    operation: OperationId,
    action_digest: [u8; 32],
    approved: bool,
}

/// Persisted declaration of the exact inherited model prefix and child suffix.
/// The declaration is reconstructed as an `InheritedModelContext` only after
/// the parent publication and child seed have been recovered.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalInheritedModelDeclaration {
    /// Completed parent exchange pinned at the fork boundary.
    pub boundary: CompletedModelBoundary,
    /// Explicit child task/identity/fresh-scratch messages.
    pub suffix: Vec<ModelMessage>,
}

impl LocalInheritedModelDeclaration {
    /// Validates and reconstructs the context stage used by model admission.
    pub fn context(&self, limits: Limits) -> Result<InheritedModelContext> {
        InheritedModelContext::new(self.boundary.clone(), self.suffix.clone(), limits)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSession {
    version: u32,
    task: TaskId,
    parent: Option<TaskId>,
    depth: usize,
    task_description: String,
    operation: Option<OperationId>,
    phase: StoredPhase,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "message")]
enum StoredPhase {
    Ready,
    Activating,
    Completed,
    Cancelled,
    Failed(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecord {
    version: u32,
    event: StoredEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
enum StoredEvent {
    Session(StoredSession),
    /// Canonical owner admission for one local task turn. The value is kept
    /// in canonical form so recovery validates the same runtime record that
    /// the execution boundary consumed.
    TaskAdmitted {
        task: TaskId,
        admission: Value,
    },
    /// Atomically records the selected child and host issuer binding.
    ForkIntentSelected {
        intent: LocalForkIntent,
        issuer_digest: Option<[u8; 32]>,
    },
    /// Model-selected child intent retained before completed-batch
    /// publication. The owner allocator resolves it only after publication.
    ForkIntent {
        intent: LocalForkIntent,
    },
    /// Durable receipt that one completed model publication has been fully
    /// admitted and scheduled. The publication digest fences retries from a
    /// substituted batch carrying the same operation identity.
    ForkPublicationCompleted {
        operation: OperationId,
        digest: [u8; 32],
    },
    /// Non-secret owner binding fingerprint retained beside the selected
    /// intent. It prevents a reopen with another host issuer secret.
    ForkIssuerBinding {
        operation: OperationId,
        child_operation: OperationId,
        digest: [u8; 32],
    },
    /// Prepared request retained before provider publication. This event is
    /// replayable but does not authorize child activation by itself.
    ForkPrepared {
        parent: TaskId,
        parent_operation: OperationId,
        parent_step: u32,
        child: TaskId,
        child_operation: OperationId,
        #[serde(default)]
        fork_operation: Option<OperationId>,
        #[serde(default)]
        child_authority: Option<Authority>,
        #[serde(default)]
        child_agent: Option<AgentId>,
        task: String,
        #[serde(default)]
        prompt: String,
        #[serde(default)]
        seed: Option<ForkSeed>,
        #[serde(default)]
        seed_digest: Option<[u8; 32]>,
        #[serde(default)]
        report: Option<ForkReport>,
        #[serde(default)]
        publication: Option<ModelBatchPublication>,
        #[serde(default)]
        declaration: Option<LocalInheritedModelDeclaration>,
        #[serde(default)]
        rebind_proof: Option<ForkRebindProof>,
    },
    ForkAdmitted {
        parent: TaskId,
        parent_operation: OperationId,
        parent_step: u32,
        child: TaskId,
        child_operation: OperationId,
        #[serde(default)]
        fork_operation: Option<OperationId>,
        #[serde(default)]
        child_authority: Option<Authority>,
        #[serde(default)]
        child_agent: Option<AgentId>,
        task: String,
        #[serde(default)]
        prompt: String,
        #[serde(default)]
        seed: Option<ForkSeed>,
        #[serde(default)]
        seed_digest: Option<[u8; 32]>,
        #[serde(default)]
        report: Option<ForkReport>,
        #[serde(default)]
        publication: Option<ModelBatchPublication>,
        #[serde(default)]
        declaration: Option<LocalInheritedModelDeclaration>,
        #[serde(default)]
        rebind_proof: Option<ForkRebindProof>,
    },
    /// Durable single-winner fence for the child model activation. The claim
    /// is recorded after admission and before any provider dispatch so a
    /// second handle cannot start the same child turn concurrently.
    ForkActivationClaimed {
        child: TaskId,
        operation: OperationId,
    },
    ForkCompleted {
        child: TaskId,
        operation: OperationId,
        #[serde(default)]
        output: Option<TurnOutput>,
        #[serde(default)]
        output_ref: Option<FileRef>,
        #[serde(default)]
        output_digest: Option<[u8; 32]>,
    },
    ForkFailed {
        child: TaskId,
        reason: String,
    },
    ForkCancelled {
        child: TaskId,
    },
}

impl From<StoredPhase> for LocalSessionPhase {
    fn from(value: StoredPhase) -> Self {
        match value {
            StoredPhase::Ready => Self::Ready,
            StoredPhase::Activating => Self::Activating,
            StoredPhase::Completed => Self::Completed,
            StoredPhase::Cancelled => Self::Cancelled,
            StoredPhase::Failed(message) => Self::Failed(message),
        }
    }
}

impl From<LocalSessionPhase> for StoredPhase {
    fn from(value: LocalSessionPhase) -> Self {
        match value {
            LocalSessionPhase::Ready => Self::Ready,
            LocalSessionPhase::Activating => Self::Activating,
            LocalSessionPhase::Completed => Self::Completed,
            LocalSessionPhase::Cancelled => Self::Cancelled,
            LocalSessionPhase::Failed(message) => Self::Failed(message),
        }
    }
}

impl From<StoredSession> for LocalSwarmSession {
    fn from(value: StoredSession) -> Self {
        Self {
            task: value.task,
            parent: value.parent,
            depth: value.depth,
            task_description: value.task_description,
            operation: value.operation,
            phase: value.phase.into(),
        }
    }
}

impl From<LocalSwarmSession> for StoredSession {
    fn from(value: LocalSwarmSession) -> Self {
        Self {
            version: REGISTRY_VERSION,
            task: value.task,
            parent: value.parent,
            depth: value.depth,
            task_description: value.task_description,
            operation: value.operation,
            phase: value.phase.into(),
        }
    }
}

/// Durable local recursive application composition.
pub struct PersistentLocalSwarm {
    root: PathBuf,
    workers: child_workers::LocalChildWorkers,
    live: Arc<LocalSwarmLiveState>,
    config: LocalSwarmConfig,
    provider: Arc<dyn ModelProvider>,
    bindings: LocalSwarmBindings,
    model_fork_publisher: Option<Arc<LocalModelForkPublisher>>,
    registry: StreamClient<LocalStream>,
    budget_journal: Arc<Mutex<SwarmBudgetJournal<LocalStream>>>,
    /// Shared provider bindings used by the root and lazily reopened task
    /// harnesses. The resolver must observe the same host and stream domain.
    filesystem_host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    conversation_stream: StreamClient<LocalStream>,
    stream_provider: ProviderRef,
    root_conversation: Authority,
    records: Mutex<BTreeMap<TaskId, LocalSwarmSession>>,
    requests: Mutex<BTreeMap<TaskId, LocalForkRequest>>,
    seeds: Mutex<BTreeMap<TaskId, ForkSeed>>,
    reports: Mutex<BTreeMap<TaskId, ForkReport>>,
    publications: Mutex<BTreeMap<TaskId, ModelBatchPublication>>,
    declarations: Mutex<BTreeMap<TaskId, LocalInheritedModelDeclaration>>,
    outcomes: Mutex<BTreeMap<TaskId, TurnOutput>>,
    completion_refs: Mutex<BTreeMap<TaskId, StoredCompletionRef>>,
    /// Latest owner admission for each task. The append-only registry retains
    /// prior turns; this projection is the record used by dispatch.
    admissions: Mutex<BTreeMap<TaskId, crate::runtime::TaskAdmissionRecord>>,
    /// Last append-only registry sequence incorporated into the in-memory
    /// projection. This is a disposable cursor; the registry remains the
    /// authority and refreshes read only an unseen suffix.
    registry_tail: Mutex<u64>,
    /// Serializes suffix replay and its projection publication within one
    /// swarm handle. The durable stream CAS remains the cross-handle fence.
    registry_refresh: Mutex<()>,
    sessions: Mutex<BTreeMap<TaskId, Arc<PersistentLocalHarness>>>,
    /// Host-only operator choices awaiting resolution. The choice is retained
    /// with the exact ticket binding so a public resolve request cannot swap
    /// an operation or action digest between the private decision and commit.
    operator_choices: Mutex<BTreeMap<String, LocalOperatorChoice>>,
}

impl PersistentLocalSwarm {
    /// Stops accepting child workers and joins their cancelled futures.
    /// Admitted external effects retain their durable recovery fences.
    pub async fn shutdown_workers(&self) {
        self.workers.shutdown().await;
        // LocalStream keeps a cancelled caller's mutation alive until its
        // durability boundary finishes. Do not report composition shutdown
        // while one of those provider-owned tasks can still hold a journal
        // root open.
        self.conversation_stream.drain().await;
        self.registry.drain().await;
    }

    fn observe(&self, observation: LocalSwarmObservation) {
        if let Some(observer) = &self.bindings.observer {
            observer.observe(observation);
        }
    }

    /// Opens or recovers a local swarm. Child sessions remain lazy until a
    /// caller explicitly activates or resumes one.
    pub async fn open(
        root: impl AsRef<Path>,
        config: LocalSwarmConfig,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Self> {
        Self::open_with_bindings(root, config, provider, LocalSwarmBindings::default()).await
    }

    /// Opens or recovers a local swarm with owner supplied communication
    /// services. The bindings are process local and must be reconstructed from
    /// the same authenticated providers after restart.
    pub async fn open_with_bindings(
        root: impl AsRef<Path>,
        mut config: LocalSwarmConfig,
        provider: Arc<dyn ModelProvider>,
        mut bindings: LocalSwarmBindings,
    ) -> Result<Self> {
        config.validate()?;
        crate::model::validate_model_options(
            &config.model.options,
            provider.model_option_policy(),
        )?;
        if bindings.budget_usage_source.is_none() {
            bindings.budget_usage_source = provider.swarm_usage_source();
        }
        let root = root.as_ref().to_path_buf();
        if let Some(resolver) = bindings.filesystem_fork_resolver.as_ref() {
            let resolver_project = resolver.source_project().ok_or_else(|| {
                Error::Invalid(
                    "filesystem fork resolver must expose its owner-selected project".into(),
                )
            })?;
            match config.project.as_ref() {
                Some(project) if project != &resolver_project => {
                    return Err(Error::Conflict(
                        "swarm config project differs from filesystem resolver project".into(),
                    ));
                }
                None => config.project = Some(resolver_project),
                Some(_) => {}
            }
        }
        let (filesystem_host, conversation_stream, stream_provider) =
            if let Some(resolver) = bindings.filesystem_fork_resolver.as_ref() {
                (
                    resolver.host(),
                    resolver.stream(),
                    resolver.stream_provider(),
                )
            } else {
                let filesystem_provider = ProviderRef::new("local", "filesystem", "2")?;
                let host =
                    shared_local_filesystem(root.join("filesystem"), filesystem_provider).await?;
                let stream_provider = ProviderRef::new("local", "stream", "2")?;
                let stream = shared_local_stream(root.join("conversation")).await?;
                (host, stream, stream_provider)
            };
        let model_fork_publisher = if let Some(plans) = bindings.model_fork_plans.clone() {
            if bindings.model_batch_publisher.is_none() {
                let publisher = Arc::new(LocalModelForkPublisher::new(plans));
                bindings.model_batch_publisher = Some(publisher.clone());
                Some(publisher)
            } else {
                None
            }
        } else {
            None
        };
        // Keep the registry's durable path stable while sharing its exclusive
        // local provider handle between same-root swarm instances.
        let registry = shared_local_stream(root.join("swarm")).await?;
        let stream = registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if let Some(plans) = bindings.model_fork_plans.as_ref() {
            plans.bind_journal(registry.clone()).await?;
        }
        let initial_tail = match stream.tail().await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let records = load_records_at(&stream, initial_tail).await?;
        let mut sessions = BTreeMap::new();
        let mut requests = BTreeMap::new();
        let mut seeds = BTreeMap::new();
        let mut reports = BTreeMap::new();
        let mut publications = BTreeMap::new();
        let mut declarations = BTreeMap::new();
        let mut outcomes = BTreeMap::new();
        let mut completion_refs = BTreeMap::new();
        let mut admissions = BTreeMap::new();
        for record in records {
            apply_record(
                &mut sessions,
                &mut requests,
                &mut seeds,
                &mut reports,
                &mut publications,
                &mut declarations,
                &mut outcomes,
                &mut completion_refs,
                &mut admissions,
                record,
            )?;
        }
        let mut registry_tail = initial_tail;
        await_empty_registry_open_barrier(&root).await;
        if sessions.is_empty() {
            let root_task = TaskId::new();
            let root_session = LocalSwarmSession {
                task: root_task,
                parent: None,
                depth: 0,
                task_description: "root".into(),
                operation: None,
                phase: LocalSessionPhase::Ready,
            };
            match append_record_at(
                &stream,
                StoredEvent::Session(root_session.clone().into()),
                initial_tail,
            )
            .await
            {
                Ok(()) => {
                    registry_tail = initial_tail
                        .checked_add(1)
                        .ok_or_else(|| Error::Storage("swarm registry sequence overflow".into()))?;
                    sessions.insert(root_task, root_session);
                }
                Err(Error::Conflict(_)) => {
                    // Another opener won the empty-registry race. Re-read its
                    // committed root instead of surfacing a transient tail
                    // conflict or creating a second local root identity.
                    registry_tail = match stream.tail().await {
                        Ok(tail) => tail,
                        Err(StreamError::NotFound) => 0,
                        Err(error) => return Err(Error::Storage(error.to_string())),
                    };
                    let winner_records = load_records_at(&stream, registry_tail).await?;
                    sessions.clear();
                    requests.clear();
                    seeds.clear();
                    reports.clear();
                    publications.clear();
                    declarations.clear();
                    outcomes.clear();
                    completion_refs.clear();
                    admissions.clear();
                    for record in winner_records {
                        apply_record(
                            &mut sessions,
                            &mut requests,
                            &mut seeds,
                            &mut reports,
                            &mut publications,
                            &mut declarations,
                            &mut outcomes,
                            &mut completion_refs,
                            &mut admissions,
                            record,
                        )?;
                    }
                    if sessions.is_empty() {
                        return Err(Error::Conflict(
                            "local swarm root creation lost its registry race without a winner"
                                .into(),
                        ));
                    }
                }
                Err(error) => return Err(error),
            }
        }
        let root_task = sessions
            .values()
            .find(|session| session.parent.is_none())
            .map(|session| session.task)
            .ok_or_else(|| Error::Storage("swarm registry has no root session".into()))?;
        let budget_session = OperationId::from_bytes(root_task.into_bytes());
        let budget_journal = SwarmBudgetJournal::start(
            &registry,
            budget_session,
            config.budget.owner.clone(),
            config.budget.limits,
        )
        .await?;
        let root_session = open_session_path(&root, root_task);
        let root_harness = Arc::new(
            PersistentLocalHarness::open_with_tools_and_project_on_providers(
                root_session,
                config.model.clone(),
                provider.clone(),
                config.limits,
                bindings.tools_for(root_task)?,
                config.project.clone(),
                filesystem_host.clone(),
                conversation_stream.clone(),
                stream_provider.clone(),
            )
            .await?,
        );
        let root_conversation = root_harness.storage().conversation().clone();
        let mut opened = BTreeMap::new();
        opened.insert(root_task, root_harness);
        let live = shared_local_live_state(&root)?;
        for session in sessions.values() {
            live.cancellation.register(session.task)?;
            if session.phase == LocalSessionPhase::Cancelled {
                live.cancellation.cancel(session.task)?;
            }
        }
        let swarm = Self {
            workers: child_workers::LocalChildWorkers::default(),
            live,
            root,
            config,
            provider,
            bindings,
            model_fork_publisher,
            registry,
            budget_journal: Arc::new(Mutex::new(budget_journal)),
            filesystem_host,
            conversation_stream,
            stream_provider,
            root_conversation,
            records: Mutex::new(sessions),
            requests: Mutex::new(requests),
            seeds: Mutex::new(seeds),
            reports: Mutex::new(reports),
            publications: Mutex::new(publications),
            declarations: Mutex::new(declarations),
            outcomes: Mutex::new(outcomes),
            completion_refs: Mutex::new(completion_refs),
            admissions: Mutex::new(admissions),
            registry_tail: Mutex::new(registry_tail),
            registry_refresh: Mutex::new(()),
            sessions: Mutex::new(opened),
            operator_choices: Mutex::new(BTreeMap::new()),
        };
        swarm.observe(LocalSwarmObservation::HarnessOpened { task: root_task });
        Ok(swarm)
    }

    /// Opens a swarm from the common model/limit arguments.
    pub async fn open_with_model(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
    ) -> Result<Self> {
        Self::open(root, LocalSwarmConfig::new(model, limits)?, provider).await
    }

    /// Opens a model backed swarm with explicit owner services.
    pub async fn open_with_model_and_bindings(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        bindings: LocalSwarmBindings,
    ) -> Result<Self> {
        Self::open_with_bindings(
            root,
            LocalSwarmConfig::new(model, limits)?,
            provider,
            bindings,
        )
        .await
    }

    /// Opens a shared swarm handle with the concrete model fork tool and
    /// completed-batch publisher bound back to this swarm's durable state.
    pub async fn open_shared_with_bindings(
        root: impl AsRef<Path>,
        config: LocalSwarmConfig,
        provider: Arc<dyn ModelProvider>,
        bindings: LocalSwarmBindings,
    ) -> Result<Arc<Self>> {
        let bindings = if let Some(plans) = bindings.model_fork_plans.clone() {
            bindings.with_model_fork_plans(plans)
        } else if let Some(resolver) = bindings.filesystem_fork_resolver.clone() {
            bindings
                .with_model_fork_plans(Arc::new(LocalModelForkPlans::new().with_resolver(resolver)))
        } else {
            bindings
        };
        let swarm = Arc::new(Self::open_with_bindings(root, config, provider, bindings).await?);
        if let Some(plans) = swarm.bindings.model_fork_plans.as_ref() {
            plans.bind_swarm(Arc::downgrade(&swarm))?;
        }
        if let Some(publisher) = &swarm.model_fork_publisher {
            publisher.bind(Arc::downgrade(&swarm))?;
        }
        Ok(swarm)
    }

    /// Opens a shared model-backed swarm using the default local composition.
    pub async fn open_shared_with_model(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        Self::open_shared_with_bindings(
            root,
            LocalSwarmConfig::new(model, limits)?,
            provider,
            LocalSwarmBindings::default(),
        )
        .await
    }

    /// Opens a shared model-backed swarm with authenticated owner services.
    pub async fn open_shared_with_model_and_bindings(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
        bindings: LocalSwarmBindings,
    ) -> Result<Arc<Self>> {
        Self::open_shared_with_bindings(
            root,
            LocalSwarmConfig::new(model, limits)?,
            provider,
            bindings,
        )
        .await
    }

    /// Opens the shared local composition with owner-authenticated recursive
    /// filesystem support. The caller supplies only the model/provider
    /// boundary; provider caches, project ownership, the durable fork
    /// resolver, and its persisted issuer secret stay in Harness.
    pub async fn open_shared_with_model_and_recursive_filesystem(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        limits.validate()?;
        crate::model::validate_model_options(&model.options, provider.model_option_policy())?;
        let root = root.as_ref().to_path_buf();
        let filesystem_provider = ProviderRef::new("local", "filesystem", "2")?;
        let host =
            shared_local_filesystem(root.join("filesystem"), filesystem_provider.clone()).await?;
        let stream_provider = ProviderRef::new("local", "stream", "2")?;
        let stream = shared_local_stream(root.join("conversation")).await?;
        let project = VolumeRef::new(
            filesystem_provider,
            "local-project",
            VolumeClass::Project,
            VolumeOwner::Project("local-swarm".into()),
        )?;
        host.create_volume(&project).await?;
        let mut config = LocalSwarmConfig::new(model.clone(), limits)?;
        config.project = Some(project.clone());
        let mut swarm = Self::open_with_bindings(
            root.clone(),
            config,
            provider.clone(),
            LocalSwarmBindings::default(),
        )
        .await?;
        let root_task = swarm.root_task().await?;
        let root_harness = swarm.sessions.get_mut().remove(&root_task)
            .ok_or_else(|| Error::Storage("new local composition has no root harness".into()))?;
        let root_harness = Arc::try_unwrap(root_harness).map_err(|_| {
            Error::Conflict("new local composition root was exposed before binding".into())
        })?;
        let host_secret = root_harness.signing_key();
        let resolver = Arc::new(
            LocalFilesystemForkResolver::new(host, stream.clone(), stream_provider, project)?
                .with_host_secret(host_secret)?,
        );
        let plans = Arc::new(LocalModelForkPlans::new().with_resolver(resolver.clone()));
        plans.bind_journal(swarm.registry.clone()).await?;
        let publisher = Arc::new(LocalModelForkPublisher::new(plans.clone()));
        let communication = Arc::new(communication_host::SwarmCommunicationHost::new(stream.clone()));
        let waits = Arc::new(crate::communication::StreamWaitStore::new(stream.clone()));
        let budget_usage_source = swarm.bindings.budget_usage_source.clone();
        swarm.bindings = LocalSwarmBindings::communication(
            communication.clone(), Some(waits), Some(swarm.live.clone()),
        )
            .with_filesystem_fork_resolver(resolver)
            .with_model_fork_plans(plans.clone())
            .with_model_batch_publisher(publisher.clone());
        if let Some(source) = budget_usage_source {
            swarm.bindings = swarm.bindings.with_budget_usage_source(source);
        }
        let root_harness = root_harness.with_local_tools(
            model, provider, limits, swarm.bindings.tools_for(root_task)?,
        )?;
        swarm.sessions.get_mut().insert(root_task, Arc::new(root_harness));
        swarm.model_fork_publisher = Some(publisher.clone());
        let swarm = Arc::new(swarm);
        plans.bind_swarm(Arc::downgrade(&swarm))?;
        publisher.bind(Arc::downgrade(&swarm))?;
        communication.bind(Arc::downgrade(&swarm))?;
        Ok(swarm)
    }

    /// Returns the stable root task without opening any child session.
    pub async fn root_task(&self) -> Result<TaskId> {
        self.records
            .lock()
            .await
            .values()
            .find(|session| session.parent.is_none())
            .map(|session| session.task)
            .ok_or_else(|| Error::Storage("swarm root session is missing".into()))
    }

    /// Returns the single durable session budget shared by root and all
    /// recursive children. The journal stream is reopened on every process
    /// start and remains the authority for reservation state.
    pub fn budget_journal(&self) -> Arc<Mutex<SwarmBudgetJournal<LocalStream>>> {
        self.budget_journal.clone()
    }

    /// Returns the latest canonical owner admission for a task. The registry
    /// projection is refreshed before every read so another swarm handle
    /// cannot dispatch under a stale turn binding.
    pub async fn authenticated_admission(
        &self,
        task: TaskId,
    ) -> Result<crate::runtime::TaskAdmissionRecord> {
        self.refresh_registry_state().await?;
        self.admissions
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local task admission {task}")))
    }

    fn local_turn_input_schema() -> Value {
        json!({"type": "string", "maxLength": 65536})
    }

    fn local_execution_contract(
        &self,
        harness: &PersistentLocalHarness,
    ) -> Result<(Value, BTreeSet<String>, [u8; 32])> {
        let contract = harness
            .bundle()
            .execution_contract()
            .cloned()
            .ok_or_else(|| Error::Conflict("local stock executor contract is missing".into()))?;
        let digest = crate::contract::canonical_json_digest(&json!({
            "model": self.config.model,
            "limits": harness.bundle().limits(),
            "run_limits": self.config.run_limits,
            "tools": contract,
            "swarm_policy": {
                "maximum_depth": self.config.maximum_depth,
                "maximum_children": self.config.maximum_children,
                "budget": {
                    "limits": self.config.budget.limits,
                    "owner": self.config.budget.owner,
                },
            },
        }))?;
        Ok((
            contract,
            harness.bundle().execution_requirements().clone(),
            digest,
        ))
    }

    /// Grants safe to retain before a child workspace exists. Volume grants
    /// are deliberately excluded; the child storage authority adds its own
    /// private/project capabilities after typed publication.
    fn child_preallocation_grants(
        tool_contract: &Value,
    ) -> Result<Capabilities> {
        let definitions = tool_contract
            .get("tools")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::Invalid("local execution contract has no tools".into()))?;
        let mut grants = vec![
            "model:generate".to_owned(),
            "mail:send".to_owned(),
            "mail:read".to_owned(),
            "timer:wait".to_owned(),
        ];
        for definition in definitions {
            let name = definition
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("local tool contract has no name".into()))?;
            grants.push(format!("tool:call:{name}"));
        }
        Ok(Capabilities::new(grants))
    }

    /// Admits the exact local turn through the authoritative swarm registry.
    /// The resulting record is later used by budget reservation and model
    /// execution; no turn output or fork request reconstructs its limits.
    pub async fn admit_local_turn(
        &self,
        task: TaskId,
        operation: OperationId,
        prompt: &str,
    ) -> Result<crate::runtime::TaskAdmissionRecord> {
        self.refresh_registry_state().await?;
        if let Some(existing) = self.admissions.lock().await.get(&task).cloned() {
            if existing.operation_id == operation {
                if existing.input == Value::String(prompt.to_owned()) {
                    return Ok(existing);
                }
                return Err(Error::Conflict(
                    "local task admission input changed for the operation".into(),
                ));
            }
        }
        let session = self.session(task).await?;
        let harness = self.open_session(task).await?;
        let (_, requirements, machine_digest) = self.local_execution_contract(&harness)?;
        let admission = crate::runtime::TaskAdmissionRecord::from_parts(
            operation,
            "acyclic.local-swarm.turn",
            "1",
            Value::String(prompt.to_owned()),
            Self::local_turn_input_schema(),
            crate::executor::turn_output_schema(),
            &requirements,
            &machine_digest,
            session.parent,
            Capabilities::new(
                harness
                    .bundle()
                    .capabilities()
                    .iter()
                    .map(str::to_owned)
                    .chain(["mail:send".into(), "mail:read".into(), "timer:wait".into()]),
            ),
            harness.bundle().limits(),
            self.config.run_limits,
            self.provider
                .model_option_policy()
                .map(|policy| policy.identity.clone()),
            None,
            None,
        )?;
        self.persist_local_admission(task, admission).await
    }

    /// Admits a child before its private/project volumes are prepared. The
    /// parent harness supplies the already pinned local authority and model
    /// binding; opening the child harness is intentionally deferred until the
    /// durable admission and budget reservation have succeeded.
    async fn admit_local_child_turn(
        &self,
        task: TaskId,
        operation: OperationId,
        prompt: &str,
        parent: TaskId,
        parent_harness: &PersistentLocalHarness,
    ) -> Result<crate::runtime::TaskAdmissionRecord> {
        self.refresh_registry_state().await?;
        if let Some(existing) = self.admissions.lock().await.get(&task).cloned() {
            if existing.operation_id == operation {
                if existing.input == Value::String(prompt.to_owned()) {
                    return Ok(existing);
                }
                return Err(Error::Conflict(
                    "local child admission input changed for the operation".into(),
                ));
            }
        }
        let (tool_contract, requirements, machine_digest) =
            self.local_execution_contract(parent_harness)?;
        let admission = crate::runtime::TaskAdmissionRecord::from_parts(
            operation,
            "acyclic.local-swarm.turn",
            "1",
            Value::String(prompt.to_owned()),
            Self::local_turn_input_schema(),
            crate::executor::turn_output_schema(),
            &requirements,
            &machine_digest,
            Some(parent),
            Self::child_preallocation_grants(&tool_contract)?,
            parent_harness.bundle().limits(),
            self.config.run_limits,
            self.provider
                .model_option_policy()
                .map(|policy| policy.identity.clone()),
            None,
            None,
        )?;
        self.persist_local_admission(task, admission).await
    }

    /// Persists the exact child turn admission before a resolver allocates
    /// workspace resources. Scheduler code should call this before invoking a
    /// filesystem fork resolver; the returned record is the input to the
    /// canonical budget reservation.
    pub async fn admit_child_turn(
        &self,
        task: TaskId,
        operation: OperationId,
        prompt: &str,
        parent: TaskId,
    ) -> Result<crate::runtime::TaskAdmissionRecord> {
        let parent_harness = self.open_session(parent).await?;
        self.admit_local_child_turn(task, operation, prompt, parent, &parent_harness)
            .await
    }

    /// Performs the durable child admission and budget reservation as one
    /// scheduler boundary. Callers must invoke this before filesystem fork
    /// preparation or provider publication; the journal atomically accounts
    /// active, total, depth, and resource ceilings from this record.
    pub async fn admit_and_reserve_child(
        &self,
        task: TaskId,
        operation: OperationId,
        prompt: &str,
        parent: TaskId,
        idempotency_key: crate::IdempotencyKey,
        depth: u32,
        resources: SwarmResourceRequest,
    ) -> Result<(
        crate::runtime::TaskAdmissionRecord,
        SwarmAdmissionReceipt,
    )> {
        let admission = self.admit_child_turn(task, operation, prompt, parent).await?;
        let parent_admission = self.authenticated_admission(parent).await?;
        let receipt = self
            .reserve_child_budget(
                task,
                idempotency_key,
                Some(parent_admission.operation_id),
                depth,
                resources,
            )
            .await?;
        Ok((admission, receipt))
    }

    async fn persist_local_admission(
        &self,
        task: TaskId,
        admission: crate::runtime::TaskAdmissionRecord,
    ) -> Result<crate::runtime::TaskAdmissionRecord> {
        let registry = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        let append = append_record_at(
            &registry,
            StoredEvent::TaskAdmitted {
                task,
                admission: admission.canonical_value(),
            },
            observed_tail,
        )
        .await;
        match append {
            Ok(()) => {}
            Err(Error::Conflict(_)) => {
                // A second opener may have committed the same admission first.
                // Reconcile that durable winner instead of manufacturing a
                // new operation record or treating the lost CAS as a provider
                // error.
                self.refresh_registry_state().await?;
                if let Some(existing) = self.admissions.lock().await.get(&task).cloned()
                    && existing == admission
                {
                    return Ok(existing);
                }
                return Err(Error::Conflict(
                    "local task admission lost its durable registry race".into(),
                ));
            }
            Err(error) => return Err(error),
        }
        self.admissions.lock().await.insert(task, admission.clone());
        *self.registry_tail.lock().await = observed_tail
            .checked_add(1)
            .ok_or_else(|| Error::Storage("local swarm registry sequence overflow".into()))?;
        Ok(admission)
    }

    /// Atomically reserves a child from the owner-retained task admission.
    /// Callers must invoke this before workspace fork preparation or provider
    /// dispatch; the local fork request is never used to reconstruct limits.
    pub async fn reserve_child_budget(
        &self,
        child_task: TaskId,
        idempotency_key: crate::IdempotencyKey,
        parent_operation_id: Option<OperationId>,
        depth: u32,
        resources: SwarmResourceRequest,
    ) -> Result<SwarmAdmissionReceipt> {
        let admission = self.authenticated_admission(child_task).await?;
        let mut journal = self.budget_journal.lock().await;
        journal
            .reserve_after_admission(
                &admission,
                idempotency_key,
                parent_operation_id,
                depth,
                resources,
            )
            .await
    }

    /// Activates a child only after authoritative publication evidence has
    /// been verified. The returned token is the sole provider dispatch proof.
    pub async fn activate_child_budget(
        &self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        dispatch_id: crate::IdempotencyKey,
        publication: VerifiedForkPublication,
    ) -> Result<SwarmDispatchToken> {
        self.budget_journal
            .lock()
            .await
            .activate_verified_with_dispatch(
                operation_id,
                owner,
                dispatch_id,
                publication,
            )
            .await
    }

    /// Settles a child from the provider-owned cumulative measurement source.
    /// Receipt issuance and durable completion remain one journal path and are
    /// replay-safe after a process restart.
    pub async fn complete_child_budget(
        &self,
        token: &SwarmDispatchToken,
    ) -> Result<SwarmForkReservation> {
        let source = self.bindings.budget_usage_source.clone().ok_or_else(|| {
            Error::Unauthorized("provider usage source is not configured".into())
        })?;
        self.budget_journal
            .lock()
            .await
            .complete_from_source(token, source)
            .await
    }

    /// Returns the host-only signer bound to one task's durable interaction
    /// storage. The caller still has to authenticate the operator decision
    /// before asking this signer to issue an exact resolution scope.
    pub async fn interaction_operator_authorizer(
        &self,
        task: TaskId,
    ) -> Result<InteractionOperatorAuthorizer<LocalStream, LocalAuthorityBackend, LocalObjectBackend>>
    {
        let harness = self.open_session(task).await?;
        harness.storage().interaction_operator_authorizer()
    }

    /// Records one authenticated operator decision against the current ticket.
    /// The terminal control boundary calls this after authenticating its
    /// process-local credential; the durable ticket remains the authority for
    /// operation and action binding.
    pub async fn record_operator_approval(
        &self,
        task: TaskId,
        id: InteractionId,
        approved: bool,
    ) -> Result<()> {
        let approval = self
            .list_approvals(task)
            .await?
            .into_iter()
            .find(|approval| approval.ticket.id.as_bytes() == &id.into_bytes())
            .ok_or_else(|| Error::NotFound(format!("local swarm approval {id}")))?;
        if approval.resolution.is_some() {
            return Err(Error::Conflict(
                "approval is no longer pending operator choice".into(),
            ));
        }
        let binding = approval.ticket.approval.ok_or_else(|| {
            Error::Invalid("approval ticket has no exact operation binding".into())
        })?;
        self.operator_choices.lock().await.insert(
            operator_choice_key(task, id),
            LocalOperatorChoice {
                operation: binding.operation_id,
                action_digest: binding.action_digest,
                approved,
            },
        );
        Ok(())
    }

    /// Resolves a decision recorded by the host operator boundary. The signer
    /// is selected from the exact task and revalidates the immutable ticket
    /// before the existing storage resolver commits the interaction.
    pub async fn resolve_recorded_operator_approval(
        &self,
        task: TaskId,
        id: InteractionId,
        approved: bool,
    ) -> Result<InteractionOutcome> {
        let approval = self
            .list_approvals(task)
            .await?
            .into_iter()
            .find(|approval| approval.ticket.id.as_bytes() == &id.into_bytes())
            .ok_or_else(|| Error::NotFound(format!("local swarm approval {id}")))?;
        let binding = approval.ticket.approval.ok_or_else(|| {
            Error::Invalid("approval ticket has no exact operation binding".into())
        })?;
        let choice = self
            .operator_choices
            .lock()
            .await
            .get(&operator_choice_key(task, id))
            .cloned()
            .ok_or_else(|| {
                Error::Unauthorized("approval requires an authenticated operator choice".into())
            })?;
        if choice.approved != approved
            || choice.operation != binding.operation_id
            || choice.action_digest != binding.action_digest
        {
            return Err(Error::Unauthorized(
                "operator choice does not match the pending approval".into(),
            ));
        }
        let operator = self.interaction_operator_authorizer(task).await?;
        let responder = operator
            .issue_scope(&InteractionApprovalAuthorization {
                interaction_id: id,
                operation_id: binding.operation_id,
                action_digest: binding.action_digest,
                approved,
            })
            .await?;
        let outcome = self
            .resolve_approval(
                task,
                id,
                InteractionResponse::Approval {
                    approved,
                    reason: None,
                },
                &responder,
            )
            .await?;
        self.operator_choices
            .lock()
            .await
            .remove(&operator_choice_key(task, id));
        Ok(outcome)
    }

    /// Lists canonical session descriptors without starting workers or
    /// reading child filesystem content.
    pub async fn sessions(&self) -> Result<Vec<LocalSwarmSession>> {
        self.refresh_registry_state().await?;
        let sessions = self
            .records
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        self.observe(LocalSwarmObservation::SessionList {
            returned: sessions.len(),
        });
        Ok(sessions)
    }

    /// Reads one bounded, refreshed page of canonical session descriptors.
    /// The registry is refreshed before projection so another host handle's
    /// durable updates are visible without retaining a second read cache.
    pub async fn sessions_page(
        &self,
        after: Option<&str>,
        maximum_entries: usize,
    ) -> Result<LocalSwarmPage<LocalSwarmSession>> {
        self.refresh_registry_state().await?;
        let page = {
            let records = self.records.lock().await;
            page_from_sorted(
                records.values().cloned(),
                after,
                maximum_entries,
                |session| session.task.to_string(),
                "session",
            )?
        };
        self.observe(LocalSwarmObservation::SessionPage {
            returned: page.items.len(),
        });
        Ok(page)
    }

    /// Reads the refreshed recursive registry subtree rooted at one task.
    /// No child journal, filesystem volume, or model worker is opened.
    pub async fn recursive_agent_tree(&self, task: TaskId) -> Result<Vec<LocalSwarmAgent>> {
        project_recursive_agent_tree(self.sessions().await?, task)
    }

    /// Reads one descriptor without opening its local journal or filesystem.
    pub async fn session(&self, task: TaskId) -> Result<LocalSwarmSession> {
        self.refresh_registry_state().await?;
        self.records
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm task {task}")))
    }

    /// Reads one task snapshot from the owner-retained index and its
    /// authoritative local providers. This method never starts a model turn
    /// or eagerly opens child sessions.
    pub async fn session_snapshot(&self, task: TaskId) -> Result<LocalSwarmSnapshot> {
        self.refresh_registry_state().await?;
        let sessions = self
            .records
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        self.snapshot_from_sessions(task, &sessions).await
    }

    async fn snapshot_from_sessions(
        &self,
        task: TaskId,
        sessions: &[LocalSwarmSession],
    ) -> Result<LocalSwarmSnapshot> {
        let session = sessions
            .iter()
            .find(|candidate| candidate.task == task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm task {task}")))?;
        let children = sessions
            .iter()
            .filter(|candidate| candidate.parent == Some(task))
            .cloned()
            .collect();
        let conversation_revision = match self.conversation_authority(task).await {
            Some(authority) => self.conversation_tail(&authority).await?,
            None => 0,
        };
        let snapshot = LocalSwarmSnapshot {
            session,
            children,
            conversation_revision,
            workspace_generation: None,
        };
        self.observe(LocalSwarmObservation::SessionSnapshot { task });
        Ok(snapshot)
    }

    /// Reads one metadata-only snapshot and its recursive registry subtree in
    /// one owner-index refresh. No child journal, filesystem volume, or model
    /// worker is opened.
    pub async fn session_snapshot_with_agents(
        &self,
        task: TaskId,
    ) -> Result<(LocalSwarmSnapshot, Vec<LocalSwarmAgent>)> {
        self.refresh_registry_state().await?;
        let sessions = self
            .records
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let snapshot = self.snapshot_from_sessions(task, &sessions).await?;
        let agents = project_recursive_agent_tree(sessions, task)?;
        Ok((
            snapshot,
            agents,
        ))
    }

    /// Finds an already-authenticated descriptor for metadata projection. A
    /// cold child uses its persisted seed/request authority; the root keeps
    /// its bound authority in the local composition descriptor.
    async fn conversation_authority(&self, task: TaskId) -> Option<Authority> {
        if let Some(harness) = self.sessions.lock().await.get(&task).cloned() {
            return Some(harness.storage().conversation().clone());
        }
        if let Some(seed) = self.seeds.lock().await.get(&task) {
            return Some(seed.child.clone());
        }
        if let Some(authority) = self
            .requests
            .lock()
            .await
            .get(&task)
            .and_then(|request| request.child_authority.clone())
        {
            return Some(authority);
        }
        if self
            .records
            .lock()
            .await
            .get(&task)
            .is_some_and(|session| session.parent.is_none())
        {
            return Some(self.root_conversation.clone());
        }
        None
    }

    async fn conversation_tail(&self, authority: &Authority) -> Result<u64> {
        let path = authority.stream_path()?;
        let stream = self
            .conversation_stream
            .stream(path)
            .map_err(|error| Error::Storage(error.to_string()))?;
        match stream.tail().await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(Error::Storage(error.to_string())),
        }
    }

    /// Reads a bounded page of authoritative conversation events. The cursor
    /// is the aggregate revision and is rejected when it is outside the
    /// durable history window.
    pub async fn read_activity(
        &self,
        task: TaskId,
        after_revision: u64,
        limit: usize,
    ) -> Result<Vec<crate::core::Event>> {
        if limit == 0 || limit > 1_024 {
            return Err(Error::Invalid(
                "conversation activity page limit must be between 1 and 1024".into(),
            ));
        }
        let harness = self.open_session(task).await?;
        let events = harness
            .conversation_events(after_revision, limit, self.config.limits)
            .await?;
        self.observe(LocalSwarmObservation::HistoryPage {
            task,
            returned: events.len(),
        });
        Ok(events)
    }

    /// Reads a bounded page of canonical conversation messages by sequence.
    /// Message content remains an immutable FileRef until the caller requests
    /// it through the authenticated private file API. This is an explicit
    /// child-history read and may activate a cold child harness; metadata
    /// listing and snapshots remain activation-free.
    pub async fn read_messages(
        &self,
        task: TaskId,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<ConversationMessage>> {
        if limit == 0 || limit > 1_024 {
            return Err(Error::Invalid(
                "conversation message page limit must be between 1 and 1024".into(),
            ));
        }
        let harness = self.open_session(task).await?;
        let messages = harness
            .storage()
            .conversation_messages(after_sequence, limit, self.config.limits)
            .await?;
        self.observe(LocalSwarmObservation::MessagePage {
            task,
            returned: messages.len(),
        });
        Ok(messages)
    }

    /// Reads one page of owner-authenticated private files. The generation
    /// returned by the first page must be supplied for subsequent pages.
    pub async fn list_files(
        &self,
        task: TaskId,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&str>,
        maximum_entries: u32,
    ) -> Result<crate::conversation::PrivateDirectoryPage> {
        let harness = self.open_session(task).await?;
        let page = harness
            .list_private_directory(path, expected_generation, after, maximum_entries)
            .await?;
        self.observe(LocalSwarmObservation::WorkspacePage {
            task,
            returned: page.entries.len(),
        });
        Ok(page)
    }

    /// Reads one owner-authenticated private file at an optional pinned
    /// generation. No ancestor volume is consulted implicitly.
    pub async fn read_file(
        &self,
        task: TaskId,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, Vec<u8>)> {
        let harness = self.open_session(task).await?;
        let file = harness.read_private_path(path, expected_generation).await?;
        self.observe(LocalSwarmObservation::WorkspaceFile {
            task,
            bytes: file.1.len(),
        });
        Ok(file)
    }

    /// Reconstructs the durable approval journal and projects one bounded page.
    pub async fn approvals_page(
        &self,
        task: TaskId,
        after: Option<&str>,
        maximum_entries: usize,
    ) -> Result<LocalSwarmPage<LocalSwarmApproval>> {
        self.refresh_registry_state().await?;
        page_by_cursor(
            self.list_approvals(task).await?,
            after,
            maximum_entries,
            |approval| approval.ticket.id.to_string(),
            "approval",
        )
    }

    /// Lists durable approval requests retained in one task conversation.
    /// The result contains only immutable tickets and resolutions; request
    /// and decision bytes remain behind their FileRefs.
    pub async fn list_approvals(&self, task: TaskId) -> Result<Vec<LocalSwarmApproval>> {
        let events = self.read_all_activity(task).await?;
        let mut approvals = BTreeMap::new();
        for event in events {
            match event.payload {
                crate::core::EventPayload::InteractionOpened { ticket }
                    if ticket.kind == InteractionKind::Approval =>
                {
                    approvals.insert(
                        ticket.id,
                        LocalSwarmApproval {
                            task,
                            ticket,
                            resolution: None,
                        },
                    );
                }
                crate::core::EventPayload::InteractionResolved { resolution } => {
                    if let Some(approval) = approvals.get_mut(&resolution.id) {
                        approval.resolution = Some(resolution);
                    }
                }
                _ => {}
            }
        }
        Ok(approvals.into_values().collect())
    }

    /// Resolves one approval through the task's owner-authenticated journal.
    pub async fn resolve_approval(
        &self,
        task: TaskId,
        id: crate::InteractionId,
        response: InteractionResponse,
        responder: &Scope,
    ) -> Result<crate::interaction::InteractionOutcome> {
        let approvals = self.list_approvals(task).await?;
        let approval = approvals
            .iter()
            .find(|approval| approval.ticket.id.as_bytes() == &id.into_bytes())
            .ok_or_else(|| Error::NotFound(format!("local swarm approval {id}")))?;
        if approval.resolution.is_some() {
            let resolution = approval
                .resolution
                .as_ref()
                .ok_or_else(|| Error::Storage("approval resolution disappeared".into()))?;
            let (approved, reason) = match &resolution.outcome {
                crate::interaction::InteractionOutcome::Approved => (true, None),
                crate::interaction::InteractionOutcome::Declined => (false, None),
                _ => {
                    return Err(Error::Conflict(
                        "approval resolution has an invalid terminal outcome".into(),
                    ));
                }
            };
            let existing = if let Some(detail) = &resolution.detail {
                let harness = self.open_session(task).await?;
                serde_json::from_slice::<InteractionResponse>(
                    &harness.storage().read(detail).await?,
                )
                .map_err(|error| {
                    Error::Storage(format!("invalid persisted approval response: {error}"))
                })?
            } else {
                InteractionResponse::Approval { approved, reason }
            };
            if existing != response {
                return Err(Error::Conflict(
                    "approval retry changes the previously committed decision".into(),
                ));
            }
            return Ok(resolution.outcome.clone());
        }
        let harness = self.open_session(task).await?;
        harness
            .storage()
            .resolve_interaction_with_scope(id, response, responder)
            .await
    }

    /// Sends a ref-only message through the authenticated durable host.
    /// Parent/child authorization is derived from the immutable swarm index;
    /// model content cannot choose an unrelated recipient.
    pub async fn send_message(
        &self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        body: &[u8],
    ) -> Result<LocalSwarmMessage> {
        if message_id.into_bytes() == [0; 16] {
            return Err(Error::Invalid("swarm message identity is nil".into()));
        }
        if body.len() > self.config.limits.file_bytes as usize {
            return Err(Error::Invalid(
                "swarm message exceeds the configured file bound".into(),
            ));
        }
        let sender_session = self.session(sender).await?;
        let recipient_session = self.session(recipient).await?;
        let target = if sender_session.parent == Some(recipient) {
            MessageTarget::Parent
        } else if recipient_session.parent == Some(sender) {
            MessageTarget::Child
        } else {
            return Err(Error::Unauthorized(
                "swarm messages require a direct parent or child recipient".into(),
            ));
        };
        let host =
            self.bindings.communication_host.clone().ok_or_else(|| {
                Error::Unsupported("durable communication host is not bound".into())
            })?;
        // The sender owns the explicit source. The communication host checks
        // sender read authority and transfers it into recipient-private storage
        // before publishing the inbox record.
        let harness = self.open_session(sender).await?;
        let transfer = crate::communication::message_endpoint_operation(sender, recipient, message_id);
        let payload = harness
            .storage()
            .stage(
                transfer,
                &format!("system/swarm/messages/{transfer}.txt"),
                body,
                "text/plain",
                "message.txt",
            )
            .await?;
        DurableCommunication::new(host)
            .send(MessageRequest {
                sender,
                recipient,
                message_id,
                target,
                payload: payload.clone(),
            })
            .await?;
        Ok(LocalSwarmMessage {
            sender,
            recipient,
            message_id,
            payload,
        })
    }

    /// Reads a bounded durable inbox page for a task.
    pub async fn read_inbox(
        &self,
        task: TaskId,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<crate::scheduler::InboxItem>> {
        self.session(task).await?;
        let host =
            self.bindings.communication_host.clone().ok_or_else(|| {
                Error::Unsupported("durable communication host is not bound".into())
            })?;
        DurableCommunication::new(host)
            .inbox(task, after_sequence, limit)
            .await
    }

    /// Observes an explicit target through the same durable wait path as the
    /// model tool. The request identity and terminal result survive reopening.
    pub async fn wait(&self, request: crate::communication::WaitRequest)
        -> Result<crate::communication::WaitCompletion> {
        let host = self.bindings.communication_host.clone()
            .ok_or_else(|| Error::Unsupported("durable communication host is not bound".into()))?;
        let store = self.bindings.wait_store.clone()
            .ok_or_else(|| Error::Unsupported("durable wait store is not bound".into()))?;
        let cancellation = request.cancellation_id.and_then(|_| {
            self.bindings.cancellation.as_ref()
                .and_then(|source| source.receiver(request.waiter))
        });
        DurableCommunication::new(host).with_wait_store(store).wait(request, cancellation).await
    }

    /// Durably cancels one task and propagates the owner cancellation signal
    /// when a live source is available. The journal append is authoritative;
    /// an observation-only live bridge does not undo the persisted decision.
    pub async fn cancel(&self, task: TaskId) -> Result<LocalSwarmSession> {
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        let session = self.session(task).await?;
        if session.phase == LocalSessionPhase::Cancelled {
            self.live.cancellation.register(task)?;
            self.live.cancellation.cancel(task)?;
            return Ok(session);
        }
        if session.phase == LocalSessionPhase::Completed {
            return Err(Error::Conflict(
                "completed local swarm task cannot be cancelled".into(),
            ));
        }
        let registry = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if let Err(error) = append_record_at(
            &registry,
            StoredEvent::ForkCancelled { child: task },
            observed_tail,
        )
        .await
        {
            self.refresh_registry_state().await?;
            let latest = self.session(task).await?;
            if latest.phase == LocalSessionPhase::Cancelled {
                self.live.cancellation.register(task)?;
                self.live.cancellation.cancel(task)?;
                return Ok(latest);
            }
            return Err(error);
        }
        {
            // Fence only the in-memory projection publication. Host bridges may
            // re-enter this swarm (and the final session lookup refreshes the
            // same projection), so they must run after the fence is released.
            let _refresh = self.registry_refresh.lock().await;
            if let Some(current) = self.records.lock().await.get_mut(&task) {
                current.phase = LocalSessionPhase::Cancelled;
            }
        }
        self.live.cancellation.register(task)?;
        self.live.cancellation.cancel(task)?;
        if let Some(source) = &self.bindings.cancellation {
            let _ = source.cancel(task);
        }
        if let Some(host) = &self.bindings.communication_host
            && let Err(error) = host.cancel(task).await
            && !matches!(error, Error::Unsupported(_))
        {
            return Err(error);
        }
        self.session(task).await
    }

    async fn read_all_activity(&self, task: TaskId) -> Result<Vec<crate::core::Event>> {
        let mut after = 0_u64;
        let mut all = Vec::new();
        loop {
            let page = self.read_activity(task, after, 1_024).await?;
            if page.is_empty() {
                break;
            }
            let next = page
                .last()
                .map(|event| event.revision)
                .ok_or_else(|| Error::Storage("activity page unexpectedly empty".into()))?;
            if next <= after {
                return Err(Error::Storage("activity cursor did not advance".into()));
            }
            after = next;
            all.extend(page);
            if all.len() > MAX_SWARM_ACTIVITY_EVENTS {
                return Err(Error::Invalid(
                    "approval history exceeds the bounded local API page window".into(),
                ));
            }
        }
        Ok(all)
    }

    /// Returns the exact typed seed recorded for a published child.
    pub async fn published_seed(&self, task: TaskId) -> Result<ForkSeed> {
        self.seeds
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm seed {task}")))
    }

    /// Returns the exact prepared report retained before publication.
    pub async fn prepared_report(&self, task: TaskId) -> Result<ForkReport> {
        self.reports
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm fork report {task}")))
    }

    /// Returns a terminal child outcome retained in the swarm registry.
    pub async fn outcome(&self, task: TaskId) -> Result<TurnOutput> {
        if let Some(output) = self.outcomes.lock().await.get(&task).cloned() {
            return Ok(output);
        }
        let Some(reference) = self.completion_refs.lock().await.get(&task).cloned() else {
            return Err(Error::NotFound(format!("local swarm outcome {task}")));
        };
        let harness = self.open_session(task).await?;
        let bytes = harness.storage().read(&reference.file).await?;
        if crate::contract::canonical_json_digest(&bytes)? != reference.digest {
            return Err(Error::Conflict(
                "durable child completion artifact digest changed".into(),
            ));
        }
        let output: TurnOutput = serde_json::from_slice(&bytes).map_err(|error| {
            Error::Storage(format!("invalid child completion artifact: {error}"))
        })?;
        if reference.operation
            != self
                .requests
                .lock()
                .await
                .get(&task)
                .map(|request| request.child_operation)
                .ok_or_else(|| Error::NotFound(format!("local swarm request {task}")))?
        {
            return Err(Error::Conflict(
                "durable child completion artifact operation changed".into(),
            ));
        }
        let _refresh = self.registry_refresh.lock().await;
        self.outcomes.lock().await.insert(task, output.clone());
        Ok(output)
    }

    /// Runs a root prompt under the shared durable local harness.
    pub async fn run_root(&self, operation: OperationId, prompt: &str) -> Result<TurnOutput> {
        let task = self.root_task().await?;
        self.run_existing(task, operation, prompt).await
    }

    /// Runs a known session with an explicit operation identity.
    pub async fn run(
        &self,
        task: TaskId,
        operation: OperationId,
        prompt: &str,
    ) -> Result<TurnOutput> {
        self.run_existing(task, operation, prompt).await
    }

    async fn run_existing(
        &self,
        task: TaskId,
        operation: OperationId,
        prompt: &str,
    ) -> Result<TurnOutput> {
        self.live.cancellation.register(task)?;
        let mut cancelled = self.live.cancellation.receiver(task).ok_or_else(|| {
            Error::Storage("registered task cancellation scope disappeared".into())
        })?;
        let gate = self.task_gate(task)?;
        let _completion_guard = gate.lock().await;
        self.refresh_registry_state().await?;
        let session = self.session(task).await?;
        if session.phase == LocalSessionPhase::Cancelled {
            return Err(Error::Conflict(
                "cancelled local swarm task cannot run again".into(),
            ));
        }
        let parent = session.parent;
        if parent.is_some() {
            let requests = self.requests.lock().await;
            let request = requests.get(&task).ok_or_else(|| {
                Error::Conflict("child session has no retained fork request".into())
            })?;
            if request.child_operation != operation || request.prompt != prompt {
                return Err(Error::Conflict(
                    "child run differs from its retained operation or prompt".into(),
                ));
            }
        }
        let admission = self.admit_local_turn(task, operation, prompt).await?;
        let harness = self.open_session(task).await?;
        self.verify_admitted_task(parent, &admission, &harness).await?;
        let max_steps = u32::try_from(
            self.config
                .run_limits
                .max_steps
                .unwrap_or(self.config.limits.model_steps),
        )
        .map_err(|_| Error::Invalid("task step limit exceeds u32".into()))?;
        self.observe(LocalSwarmObservation::ModelWorkerStarted { task });
        let declaration = self.declarations.lock().await.get(&task).cloned();
        let run = async {
            if let Some(declaration) = declaration {
                let bundle = self.inherited_task_bundle(task, &harness, &declaration)?;
                harness
                    .run_with_admission(&bundle, &admission, prompt, max_steps)
                    .await
            } else {
                harness
                    .run_with_admission(&harness.bundle(), &admission, prompt, max_steps)
                    .await
            }
        };
        let output = tokio::select! {
            biased;
            result = cancellation_requested(&mut cancelled) => {
                result?;
                return Err(Error::Conflict("local swarm task was cancelled while running".into()));
            }
            output = run => output?,
        };
        // The per-task mutex only fences handles in this process.  A second
        // process can cancel the task while the model is running, so the
        // registry must be refreshed before the terminal Session event is
        // appended.  `complete_session` also makes same-operation recovery
        // idempotent while rejecting a different operation key.
        self.complete_session(task, operation).await?;
        Ok(output)
    }

    /// Rejects the legacy boundary-only fork entry point.
    ///
    /// Production model dispatch must use
    /// `publish_and_activate_child_with_publication`, which persists the exact
    /// seed, model publication, and declared recursive context before binding.
    pub async fn fork(&self, request: LocalForkRequest) -> Result<LocalForkOutcome> {
        request.validate()?;
        let parent = self.session(request.parent).await?;
        validate_recursive_depth(parent.depth, self.config.maximum_depth)?;
        let parent_harness = self.open_session(request.parent).await?;
        let _boundary = parent_harness
            .storage()
            .completed_model_boundary(
                request.parent_operation,
                request.parent_step,
                self.config.limits,
            )
            .await?
            .ok_or_else(|| Error::Conflict("fork requires a completed model boundary".into()))?;
        return Err(Error::Conflict(
            "typed fork publication is required; use publish_and_activate_child_with_publication"
                .into(),
        ));
    }

    /// Activates a child after the caller has prepared and published the exact
    /// typed fork seed. The child harness is created only through
    /// `HarnessStorage::from_published_fork`, so its conversation binding and
    /// inherited reference grants come from the committed seed.
    pub async fn activate_published_child(
        &self,
        request: LocalForkRequest,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &StreamAggregate<LocalStream>,
        seed: &ForkSeed,
    ) -> Result<LocalForkOutcome> {
        // Keep the large preparation future off recursive caller frames.
        let activation = Box::pin(self.prepare_published_child(
            request, host, stream, issuer, parent, seed,
        )).await?;
        match activation {
            LocalChildActivation::Completed(outcome) => Ok(outcome),
            LocalChildActivation::Ready(turn) => self.execute_child_turn(turn).await,
        }
    }

    async fn prepare_published_child(
        &self,
        request: LocalForkRequest,
        host: Arc<LocalFilesystemHost>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &StreamAggregate<LocalStream>,
        seed: &ForkSeed,
    ) -> Result<LocalChildActivation> {
        self.refresh_registry_state().await?;
        request.validate()?;
        seed.validate()?;
        if request.fork_operation != Some(seed.operation_id)
            || request.child_authority.as_ref() != Some(&seed.child)
            || request.child_agent != Some(seed.child_agent)
        {
            return Err(Error::Conflict(
                "published fork request is not bound to the immutable seed".into(),
            ));
        }
        if issuer.verifier().audience() != &seed.child {
            return Err(Error::Unauthorized(
                "published fork issuer targets another child authority".into(),
            ));
        }
        let parent_session = self.session(request.parent).await?;
        let parent_admission = self.authenticated_admission(request.parent).await?;
        let parent_harness = self.open_session(request.parent).await?;
        self.verify_admitted_task(parent_session.parent, &parent_admission, &parent_harness)
            .await?;
        let child = TaskId::from_bytes(request.child_operation.into_bytes());
        if let Some(existing) = self.requests.lock().await.get(&child)
            && existing != &request
        {
            return Err(Error::Conflict(
                "child operation is already bound to another fork request".into(),
            ));
        }
        let publication = self.publications.lock().await.get(&child).cloned();
        let declaration = self.declarations.lock().await.get(&child).cloned();
        let stored_publication = publication.clone();
        let stored_declaration = declaration.clone();
        let stored_report = self.reports.lock().await.get(&child).cloned();
        if publication.is_none() || declaration.is_none() {
            return Err(Error::Conflict(
                "typed publication and recursive declaration are required; use publish_and_activate_child_with_publication".into(),
            ));
        }
        let publication = stored_publication.ok_or_else(|| {
            Error::Conflict("published model batch disappeared before admission".into())
        })?;
        let declaration = stored_declaration.ok_or_else(|| {
            Error::Conflict("inherited declaration disappeared before admission".into())
        })?;
        let report = stored_report.ok_or_else(|| {
            Error::Conflict("prepared fork report disappeared before admission".into())
        })?;
        // The declaration was authenticated against the completed parent
        // boundary during admission. Recovery must use those immutable bytes
        // before consulting the parent's mutable current tail.
        declaration.context(self.config.limits)?;
        let boundary = declaration.boundary.clone();
        let declared_suffix = declaration.suffix.clone();
        // Preparation is the only admission path. Its authoritative record
        // retains the request, seed, report and completed model declaration
        // together; activation must never create a second admission.
        let admitted = self.session(child).await?;
        if admitted.parent != Some(request.parent)
            || admitted.operation != Some(request.child_operation)
            || admitted.depth != parent_session.depth + 1
            || self.requests.lock().await.get(&child) != Some(&request)
            || self.seeds.lock().await.get(&child) != Some(seed)
            || self.reports.lock().await.get(&child) != Some(&report)
            || self.publications.lock().await.get(&child) != Some(&publication)
            || self.declarations.lock().await.get(&child) != Some(&declaration)
        {
            return Err(Error::Conflict(
                "child activation differs from its retained admission".into(),
            ));
        }
        match admitted.phase {
            LocalSessionPhase::Completed => {
                return Ok(LocalChildActivation::Completed(LocalForkOutcome {
                    child,
                    operation: request.child_operation,
                    output: self.outcome(child).await?,
                }));
            }
            LocalSessionPhase::Cancelled => {
                return Err(Error::Conflict(
                    "cancelled child operation is terminal and cannot be resurrected".into(),
                ));
            }
            LocalSessionPhase::Activating => {}
            _ => {
                self.update_session(child, |session| {
                    session.phase = LocalSessionPhase::Activating;
                }).await?;
            }
        }
        let storage_parent = parent;
        let registry = self.registry.stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        // The durable claim permits cold recovery, but a live writer still
        // owns the child journal. All handles share this per-child fence.
        let activation_gate = self.task_gate(child)?;
        let activation_guard = activation_gate.try_lock_owned()
            .map_err(|_| Error::Indeterminate(request.child_operation))?;
        let harness =
            match PersistentLocalHarness::from_published_fork_with_tools_and_stream_provider(
                self.config.model.clone(),
                self.provider.clone(),
                self.config.limits,
                host,
                stream,
                issuer,
                storage_parent,
                seed,
                self.bindings.tools_for(child)?,
                self.stream_provider.clone(),
            )
            .await
            {
                Ok(harness) => Arc::new(harness),
                Err(error) => {
                    self.mark_activation_failed_if_safe(
                        child, request.child_operation, None, &error,
                    ).await?;
                    return Err(error);
                }
            };
        let child_admission = self.authenticated_admission(child).await?;
        self.verify_admitted_task(Some(request.parent), &child_admission, &harness)
            .await?;
        self.sessions.lock().await.insert(child, harness.clone());
        self.prepare_child_turn(
            request,
            child,
            registry,
            boundary,
            harness,
            declared_suffix,
            activation_guard,
        )
        .await
    }

    async fn preadmit_published_child(
        &self,
        request: &LocalForkRequest,
        report: &ForkReport,
        seed: &ForkSeed,
        publication: ModelBatchPublication,
        declaration: LocalInheritedModelDeclaration,
        rebind_proof: Option<&ForkRebindProof>,
    ) -> Result<()> {
        request.validate()?;
        if request.child_authority.is_none() || request.child_agent.is_none() {
            return Err(Error::Invalid(
                "typed fork publication requires child authority and agent".into(),
            ));
        }
        let fork_operation = request.fork_operation.ok_or_else(|| {
            Error::Invalid("typed fork publication requires a fork operation identity".into())
        })?;
        if let Some(proof) = rebind_proof {
            report.validate_with_rebind_proof(proof)?;
        } else {
            report.validate()?;
        }
        let parent_storage = self.open_session(request.parent).await?;
        if report.request.parent != *parent_storage.storage().conversation()
            || report.request.operation_id != fork_operation
        {
            return Err(Error::Conflict(
                "fork report is bound to another publication operation or parent".into(),
            ));
        }
        let child = TaskId::from_bytes(request.child_operation.into_bytes());
        // Keep the owner admission durable before publication or activation.
        // Resolver-backed paths perform this before workspace preparation;
        // this idempotent check also covers plans recovered after restart.
        self.admit_local_child_turn(
            child,
            request.child_operation,
            &request.prompt,
            request.parent,
            &parent_storage,
        )
        .await?;
        let child_authority = request.child_authority.as_ref().ok_or_else(|| {
            Error::Invalid("typed fork publication requires child authority".into())
        })?;
        let child_agent = request
            .child_agent
            .ok_or_else(|| Error::Invalid("typed fork publication requires child agent".into()))?;
        if child_authority != &report.request.child || child_agent != report.request.child_agent {
            return Err(Error::Conflict(
                "fork report child binding differs from fork request".into(),
            ));
        }
        let reported_seed = if let Some(proof) = rebind_proof {
            report.clone().into_seed_with_rebind_proof(proof)?
        } else {
            report.clone().into_seed()?
        };
        if &reported_seed != seed {
            return Err(Error::Conflict(
                "prepared fork report does not match the typed seed".into(),
            ));
        }
        seed.validate()?;
        if seed.operation_id != fork_operation
            || seed.parent != report.request.parent
            || seed.child != report.request.child
            || seed.child_agent != report.request.child_agent
            || child_fork_operation(publication.operation_id, request.child_operation)
                != fork_operation
        {
            return Err(Error::Conflict(
                "published fork seed or model publication has the wrong operation binding".into(),
            ));
        }
        declaration.context(self.config.limits)?;
        if publication.parent_operation != request.parent_operation
            || publication.step != request.parent_step
        {
            return Err(Error::Conflict(
                "published model boundary does not match fork request".into(),
            ));
        }
        // Admission and completion share one per-child terminal fence.
        // Narrowing the lock to this child permits a model turn to select a
        // grandchild without recursively taking a global swarm lock.
        let gate = self.task_gate(child)?;
        let _completion_guard = gate.lock().await;
        self.refresh_registry_state().await?;
        let parent = self.session(request.parent).await?;
        if let Some(existing) = self.records.lock().await.get(&child).cloned() {
            if self.requests.lock().await.get(&child) != Some(request)
                || self.seeds.lock().await.get(&child) != Some(seed)
                || self.reports.lock().await.get(&child) != Some(report)
            {
                return Err(Error::Conflict(
                    "existing typed child admission differs from retry".into(),
                ));
            }
            if existing.phase == LocalSessionPhase::Cancelled {
                return Err(Error::Conflict(
                    "cancelled child operation is terminal and cannot be resurrected".into(),
                ));
            }
            if self.publications.lock().await.get(&child) != Some(&publication)
                || self.declarations.lock().await.get(&child) != Some(&declaration)
            {
                return Err(Error::Conflict(
                    "existing typed declaration differs from retry".into(),
                ));
            }
            if existing.phase == LocalSessionPhase::Completed {
                // ForkCompleted is an idempotent acknowledgement. The
                // activation path will replay its bounded output rather than
                // dispatching the child model again.
                return Ok(());
            }
            return Ok(());
        }
        validate_recursive_depth(parent.depth, self.config.maximum_depth)?;
        let child_count = self
            .records
            .lock()
            .await
            .values()
            .filter(|session| session.parent == Some(request.parent) && session.task != child)
            .count();
        if child_count >= self.config.maximum_children {
            return Err(Error::Unauthorized(
                "local swarm child limit exceeded".into(),
            ));
        }
        // Authenticate the immutable declaration while the parent boundary
        // is still current. Later activation may recover from a stale parent
        // tail, but it can only trust a declaration that crossed this check
        // before its durable admission record was written.
        let parent_declaration = self.declarations.lock().await.get(&request.parent).cloned();
        let verified = match parent_declaration {
            Some(parent_declaration) => {
                let inherited = parent_declaration.context(self.config.limits)?;
                parent_storage
                    .storage()
                    .verified_inherited_model_fork_boundary(
                        &publication,
                        self.config.limits,
                        &inherited,
                    )
                    .await?
            }
            None => {
                parent_storage
                    .storage()
                    .verified_model_fork_boundary(&publication, self.config.limits)
                    .await?
            }
        };
        if verified.boundary() != &declaration.boundary {
            return Err(Error::Conflict(
                "published declaration differs from the authenticated model boundary".into(),
            ));
        }
        let registry = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        append_record_at(
            &registry,
            StoredEvent::ForkPrepared {
                parent: request.parent,
                parent_operation: request.parent_operation,
                parent_step: request.parent_step,
                child,
                child_operation: request.child_operation,
                fork_operation: request.fork_operation.clone(),
                child_authority: request.child_authority.clone(),
                child_agent: request.child_agent.clone(),
                task: request.task.clone(),
                prompt: request.prompt.clone(),
                seed: Some(seed.clone()),
                seed_digest: Some(fork_seed_digest(seed)?),
                report: Some(report.clone()),
                publication: Some(publication.clone()),
                declaration: Some(declaration.clone()),
                rebind_proof: rebind_proof.cloned(),
            },
            observed_tail,
        )
        .await?;
        self.refresh_registry_state().await?;
        if self
            .records
            .lock()
            .await
            .get(&child)
            .is_some_and(|session| session.phase == LocalSessionPhase::Cancelled)
        {
            return Err(Error::Conflict(
                "child operation was cancelled during admission preparation".into(),
            ));
        }
        let _refresh = self.registry_refresh.lock().await;
        self.records.lock().await.insert(
            child,
            LocalSwarmSession {
                task: child,
                parent: Some(request.parent),
                depth: parent.depth + 1,
                task_description: request.task.clone(),
                operation: Some(request.child_operation),
                phase: LocalSessionPhase::Activating,
            },
        );
        self.requests.lock().await.insert(child, request.clone());
        self.seeds.lock().await.insert(child, seed.clone());
        self.reports.lock().await.insert(child, report.clone());
        self.publications.lock().await.insert(child, publication);
        self.declarations.lock().await.insert(child, declaration);
        Ok(())
    }

    /// Publishes a prepared report through the typed parent/child aggregates,
    /// then activates the child from the resulting immutable seed.
    pub async fn publish_and_activate_child(
        &self,
        request: LocalForkRequest,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &mut StreamAggregate<LocalStream>,
        report: ForkReport,
    ) -> Result<LocalForkOutcome> {
        let _ = (request, host, stream, issuer, parent, report);
        Err(Error::Conflict(
            "typed model publication and recursive declaration are required; use publish_and_activate_child_with_publication".into(),
        ))
    }

    /// Persists the exact model publication and recursive declaration before
    /// publishing the typed fork, then activates from the resulting seed.
    pub async fn publish_and_activate_child_with_publication(
        &self,
        request: LocalForkRequest,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &mut StreamAggregate<LocalStream>,
        report: ForkReport,
        publication: ModelBatchPublication,
        declaration: LocalInheritedModelDeclaration,
    ) -> Result<LocalForkOutcome> {
        let seed = self
            .publish_child_seed_with_publication(
                request.clone(),
                stream.clone(),
                issuer.clone(),
                parent,
                report,
                publication,
                declaration,
                None,
            )
            .await?;
        self.activate_published_child(request, host, stream, issuer, parent, &seed)
            .await
    }

    /// Performs the durable admission and typed parent/child publication
    /// phase without dispatching the child model. Batch publishers use this
    /// phase for every selected child before any child turn begins.
    async fn publish_child_seed_with_publication(
        &self,
        request: LocalForkRequest,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &mut StreamAggregate<LocalStream>,
        report: ForkReport,
        publication: ModelBatchPublication,
        declaration: LocalInheritedModelDeclaration,
        rebind_proof: Option<&ForkRebindProof>,
    ) -> Result<ForkSeed> {
        request.validate()?;
        if let Some(proof) = rebind_proof {
            report.validate_with_rebind_proof(proof)?;
        } else {
            report.validate()?;
        }
        let parent_storage = self.open_session(request.parent).await?;
        if report.request.parent != *parent_storage.storage().conversation()
            || request.fork_operation != Some(report.request.operation_id)
        {
            return Err(Error::Conflict(
                "fork report is not bound to the requested parent and operation".into(),
            ));
        }
        if request
            .child_authority
            .as_ref()
            .is_some_and(|authority| authority != &report.request.child)
            || request
                .child_agent
                .is_some_and(|agent| agent != report.request.child_agent)
        {
            return Err(Error::Conflict(
                "fork report child authority or agent differs from request".into(),
            ));
        }
        let preview = if let Some(proof) = rebind_proof {
            report.clone().into_seed_with_rebind_proof(proof)?
        } else {
            report.clone().into_seed()?
        };
        self.preadmit_published_child(
            &request,
            &report,
            &preview,
            publication,
            declaration,
            rebind_proof,
        )
        .await?;
        let mut child = StreamAggregate::open(
            &stream,
            preview.child.clone(),
            issuer.verifier(),
            SchemaRegistry::new(),
        )
        .await?;
        let parent_scope = self
            .open_session(request.parent)
            .await?
            .storage()
            .owner_scope()
            .clone();
        let child_scope = issuer.root_for_agent(
            preview.child_agent,
            "fork-bind",
            Capabilities::new(["conversation:bind".to_owned()]),
        );
        let seed = match if let Some(proof) = rebind_proof {
            child
                .spawn_from_report_with_rebind(parent, report, parent_scope, child_scope, proof)
                .await
        } else {
            child
                .spawn_from_report(parent, report, parent_scope, child_scope)
                .await
        } {
            Ok(seed) => seed,
            Err(error) => {
                self.mark_activation_failed_if_safe(
                    TaskId::from_bytes(request.child_operation.into_bytes()),
                    request.child_operation,
                    None,
                    &error,
                )
                .await?;
                return Err(error);
            }
        };
        Ok(seed)
    }

    /// Acquires the durable single-winner fence for a child model turn.
    ///
    /// The in-process task gate prevents duplicate work within one handle,
    /// while this append-at-tail claim fences independent handles and
    /// processes. A claim is cleared by a terminal registry event; an
    /// unresolved claim remains indeterminate until its owner is reconciled,
    /// which is safer than dispatching a second provider request.
    async fn claim_child_activation(
        &self,
        stream: &acyclic_stream::Stream<LocalStream>,
        child: TaskId,
        operation: OperationId,
    ) -> Result<bool> {
        claim_child_activation_on_stream(stream, child, operation).await
    }

    /// A retained activation claim may be retried only after the execution
    /// journal proves that model dispatch was admitted. In that case the
    /// shared executor enters provider reconciliation and cannot issue a
    /// second model request. A claim without `ModelStarted` is still owned by
    /// an in-flight or unknown activation and remains indeterminate.
    async fn child_model_started(
        &self,
        harness: &PersistentLocalHarness,
        operation: OperationId,
    ) -> Result<bool> {
        Ok(harness
            .storage()
            .journal()
            .replay(operation)
            .await?
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. })))
    }

    /// A failure releases the activation claim only when the available journal
    /// proves that no model execution started. Opening failed storage again can
    /// create a new empty journal, so absence of a harness is not such proof.
    async fn mark_activation_failed_if_safe(
        &self,
        child: TaskId,
        operation: OperationId,
        harness: Option<&PersistentLocalHarness>,
        error: &Error,
    ) -> Result<()> {
        if matches!(error, Error::Indeterminate(_)) {
            return Ok(());
        }
        let Some(harness) = harness else {
            return Ok(());
        };
        if !self.child_model_started(harness, operation).await? {
            self.mark_failed(child, error.to_string()).await?;
        }
        Ok(())
    }

    async fn prepare_child_turn(
        &self,
        request: LocalForkRequest,
        child: TaskId,
        stream: acyclic_stream::Stream<LocalStream>,
        boundary: crate::model_input::CompletedModelBoundary,
        harness: Arc<PersistentLocalHarness>,
        declared_suffix: Vec<ModelMessage>,
        activation_guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<LocalChildActivation> {
        // Subscribe before the durable check so cancellation cannot fall
        // between that check and live task registration.
        self.live.cancellation.register(child)?;
        let cancelled = self.live.cancellation.receiver(child).ok_or_else(|| {
            Error::Storage("registered child cancellation scope disappeared".into())
        })?;
        self.refresh_registry_state().await?;
        if self
            .records
            .lock()
            .await
            .get(&child)
            .is_some_and(|session| session.phase == LocalSessionPhase::Cancelled)
        {
            return Err(Error::Conflict(
                "cancelled child operation is terminal and cannot be activated".into(),
            ));
        }
        if let Some(output) = self.outcomes.lock().await.get(&child).cloned() {
            return Ok(LocalChildActivation::Completed(LocalForkOutcome {
                child,
                operation: request.child_operation,
                output,
            }));
        }
        if let Some(output) = self
            .recover_completed_output(&stream, child, request.child_operation, &harness)
            .await?
        {
            return Ok(LocalChildActivation::Completed(LocalForkOutcome {
                child,
                operation: request.child_operation,
                output,
            }));
        }
        if !self
            .claim_child_activation(&stream, child, request.child_operation)
            .await?
        {
            self.refresh_registry_state().await?;
            if let Some(output) = self
                .recover_completed_output(&stream, child, request.child_operation, &harness)
                .await?
            {
                return Ok(LocalChildActivation::Completed(LocalForkOutcome {
                    child,
                    operation: request.child_operation,
                    output,
                }));
            }
            // Only a journaled model admission makes retrying an existing
            // claim safe: run_child_turn will reconcile that admission. If
            // no admission exists, the other owner may still be before
            // dispatch, so do not clear its claim or start another turn.
            if !self
                .child_model_started(&harness, request.child_operation)
                .await?
            {
                return Err(Error::Indeterminate(request.child_operation));
            }
        }
        let declaration = LocalInheritedModelDeclaration {
            boundary,
            suffix: declared_suffix,
        };
        let bundle = match self.inherited_task_bundle(child, &harness, &declaration) {
            Ok(bundle) => bundle,
            Err(error) => {
                self.mark_activation_failed_if_safe(
                    child, request.child_operation, Some(&harness), &error,
                ).await?;
                return Err(error);
            }
        };
        let max_steps = u32::try_from(
            self.config
                .run_limits
                .max_steps
                .unwrap_or(self.config.limits.model_steps),
        )
        .map_err(|_| Error::Invalid("child step limit exceeds u32".into()))?;
        let admission = self.authenticated_admission(child).await?;
        self.verify_admitted_task(Some(request.parent), &admission, &harness)
            .await?;
        Ok(LocalChildActivation::Ready(Box::new(LocalChildTurn {
            request, stream, harness, bundle, admission, max_steps, cancelled,
            _activation_guard: activation_guard,
        })))
    }

    async fn execute_child_turn(&self, turn: Box<LocalChildTurn>) -> Result<LocalForkOutcome> {
        let LocalChildTurn {
            request, stream, harness, bundle, admission, max_steps, cancelled, _activation_guard,
        } = *turn;
        let child = TaskId::from_bytes(request.child_operation.into_bytes());
        self.observe(LocalSwarmObservation::ModelWorkerStarted { task: child });
        let child_result = Self::run_owned_child_turn(
            harness.clone(), bundle, admission, request.clone(), max_steps, cancelled,
        ).await;
        self.finish_child_turn(&stream, child, request.child_operation, &harness, child_result).await
    }

    async fn finish_child_turn(
        &self,
        stream: &acyclic_stream::Stream<LocalStream>,
        child: TaskId,
        operation: OperationId,
        harness: &PersistentLocalHarness,
        child_result: Result<TurnOutput>,
    ) -> Result<LocalForkOutcome> {
        // A cancelled caller may have left a LocalStream mutation owned by
        // the provider. Drain the exact child execution provider before
        // reading or publishing its journal; sibling/root providers may be
        // different authenticated handles.
        harness.storage().stream().drain().await;
        let output = match child_result {
            Ok(output) => output,
            Err(error) => {
                self.mark_activation_failed_if_safe(
                    child, operation, Some(harness), &error,
                ).await?;
                return Err(error);
            }
        };
        self.persist_child_completion(stream, child, operation, harness, output)
            .await
    }

    /// A successful child turn must cross this durable terminal fence before
    /// acknowledgement. The executor itself does not publish completion.
    async fn persist_child_completion(
        &self,
        stream: &acyclic_stream::Stream<LocalStream>,
        child: TaskId,
        operation: OperationId,
        harness: &PersistentLocalHarness,
        output: TurnOutput,
    ) -> Result<LocalForkOutcome> {
        let output_bytes = crate::contract::canonical_json_bytes(&output)?;
        let output_digest = crate::contract::canonical_json_digest(&output_bytes)?;
        let (inline_output, output_ref) = if output_bytes.len() <= MAX_INLINE_COMPLETION_BYTES {
            (Some(output.clone()), None)
        } else {
            let output_ref = harness
                .storage()
                .stage(
                    operation,
                    &format!("system/swarm/completions/{child}.json"),
                    &output_bytes,
                    "application/json",
                    "child-completion.json",
                )
                .await?;
            (None, Some(output_ref))
        };
        // A different process may have cancelled while this model turn was
        // running. Reconcile the durable terminal state before acknowledging
        // completion; a cancellation always wins over an uncommitted turn.
        self.refresh_registry_state().await?;
        if self
            .records
            .lock()
            .await
            .get(&child)
            .is_some_and(|session| session.phase == LocalSessionPhase::Cancelled)
        {
            return Err(Error::Conflict(
                "child operation was cancelled before completion acknowledgement".into(),
            ));
        }
        let completion = StoredEvent::ForkCompleted {
            child,
            operation,
            output: inline_output,
            output_ref,
            output_digest: Some(output_digest),
        };
        let mut published = false;
        for _ in 0..4 {
            let observed_tail = self.refresh_registry_state_with_tail().await?;
            // Validate terminal state on the same registry snapshot used for
            // this CAS. A cancellation appended after this tail is ordered
            // after a successful completion; a cancellation already present
            // must prevent the completion append.
            if self
                .records
                .lock()
                .await
                .get(&child)
                .is_some_and(|session| session.phase == LocalSessionPhase::Cancelled)
            {
                return Err(Error::Conflict(
                    "child operation was cancelled before completion acknowledgement".into(),
                ));
            }
            match append_record_at(stream, completion.clone(), observed_tail).await {
                Ok(()) => {
                    published = true;
                    break;
                }
                Err(Error::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
            self.refresh_registry_state().await?;
            if let Some(recovered) = self
                .recover_completed_output(stream, child, operation, harness)
                .await?
            {
                return Ok(LocalForkOutcome {
                    child,
                    operation,
                    output: recovered,
                });
            }
            if self
                .records
                .lock()
                .await
                .get(&child)
                .is_some_and(|session| session.phase == LocalSessionPhase::Cancelled)
            {
                return Err(Error::Conflict(
                    "child operation was cancelled before completion acknowledgement".into(),
                ));
            }
        }
        if !published {
            return Err(Error::Indeterminate(operation));
        }
        self.refresh_registry_state().await?;
        let _refresh = self.registry_refresh.lock().await;
        self.outcomes.lock().await.insert(child, output.clone());
        Ok(LocalForkOutcome {
            child,
            operation,
            output,
        })
    }

    /// Recovers a completion that was committed to the registry before the
    /// process lost its in-memory outcome map. A committed completion without
    /// a replayable output is terminal and must not run the model again.
    async fn recover_completed_output(
        &self,
        stream: &acyclic_stream::Stream<LocalStream>,
        child: TaskId,
        operation: OperationId,
        harness: &PersistentLocalHarness,
    ) -> Result<Option<TurnOutput>> {
        let records = load_records(stream).await?;
        let mut recovered = None;
        for record in records {
            let StoredEvent::ForkCompleted {
                child: recorded_child,
                operation: recorded_operation,
                output,
                output_ref,
                output_digest,
            } = record.event
            else {
                continue;
            };
            if recorded_child != child {
                continue;
            }
            if recorded_operation != operation {
                return Err(Error::Conflict(
                    "durable child completion is bound to another operation".into(),
                ));
            }
            let output = match (output, output_ref, output_digest) {
                (Some(output), None, digest) => {
                    if let Some(digest) = digest {
                        let bytes = crate::contract::canonical_json_bytes(&output)?;
                        if crate::contract::canonical_json_digest(&bytes)? != digest {
                            return Err(Error::Conflict(
                                "durable inline child completion digest changed".into(),
                            ));
                        }
                    }
                    output
                }
                (None, Some(file), Some(digest)) => {
                    let bytes = harness.storage().read(&file).await?;
                    if crate::contract::canonical_json_digest(&bytes)? != digest {
                        return Err(Error::Conflict(
                            "durable child completion artifact digest changed".into(),
                        ));
                    }
                    serde_json::from_slice(&bytes).map_err(|error| {
                        Error::Storage(format!("invalid child completion artifact: {error}"))
                    })?
                }
                (None, None, _) | (None, Some(_), None) => {
                    return Err(Error::Conflict(
                        "durable child completion has no replayable output".into(),
                    ));
                }
                (Some(_), Some(_), _) => {
                    return Err(Error::Conflict(
                        "durable child completion has duplicate output forms".into(),
                    ));
                }
            };
            if let Some(existing) = &recovered
                && existing != &output
            {
                return Err(Error::Conflict(
                    "durable child completion changed across retries".into(),
                ));
            }
            recovered = Some(output);
        }
        if let Some(output) = recovered.clone() {
            {
                let _refresh = self.registry_refresh.lock().await;
                self.outcomes.lock().await.insert(child, output);
            }
            if self
                .records
                .lock()
                .await
                .get(&child)
                .is_some_and(|session| session.phase != LocalSessionPhase::Completed)
            {
                self.update_session(child, |session| {
                    session.phase = LocalSessionPhase::Completed;
                })
                .await?;
            }
        }
        Ok(recovered)
    }

    /// Own the recursive turn without retaining the swarm composition across
    /// provider awaits. This keeps worker execution separate from its owner's
    /// admission and completion callbacks, and bounds the recursive poll stack.
    async fn run_owned_child_turn(
        harness: Arc<PersistentLocalHarness>,
        bundle: crate::Harness,
        admission: crate::runtime::TaskAdmissionRecord,
        request: LocalForkRequest,
        max_steps: u32,
        mut cancelled: tokio::sync::watch::Receiver<bool>,
    ) -> Result<TurnOutput> {
        // The worker registry already owns and joins this future. Running the
        // model turn directly avoids a second JoinHandle whose Drop path can
        // abort and detach the inner task before its provider cleanup has
        // finished.
        let child_task = Self::run_child_turn(harness, bundle, admission, request, max_steps);
        tokio::pin!(child_task);
        tokio::select! {
            result = &mut child_task => result,
            result = cancellation_requested(&mut cancelled) => {
                // Dropping the pinned worker future cancels it under the
                // registry's ownership. The enclosing worker remains the
                // join boundary for all cleanup that the future owns.
                result.and_then(|()| Err(Error::Conflict("child activation was cancelled".into())))
            },
        }
    }

    async fn run_child_turn(
        harness: Arc<PersistentLocalHarness>,
        bundle: crate::Harness,
        admission: crate::runtime::TaskAdmissionRecord,
        request: LocalForkRequest,
        max_steps: u32,
    ) -> Result<TurnOutput> {
        harness
            .run_with_admission(&bundle, &admission, &request.prompt, max_steps)
            .await
    }

    fn inherited_task_bundle(
        &self,
        task: TaskId,
        harness: &PersistentLocalHarness,
        declaration: &LocalInheritedModelDeclaration,
    ) -> Result<crate::Harness> {
        let builder = harness.storage()
            .inherited_builder(declaration.boundary.clone(), declaration.suffix.clone(),
                self.provider.clone(), self.config.limits)?
            .tools(harness.storage().default_tools(self.config.limits)?)
            .grant("tool:call:acyclic.read_file")
            .grant("tool:call:acyclic.stage_file")
            .grant("tool:call:acyclic.list_files")
            .grant("mail:send")
            .grant("mail:read")
            .grant("timer:wait")
            .limits(self.config.limits);
        self.bindings.tools_for(task)?.install_into(builder)?.build()
    }

    /// Rechecks the owner-retained admission immediately before model
    /// dispatch. A reopened process must not run a task under a changed
    /// parent, numeric limit, or run budget binding.
    async fn verify_admitted_task(
        &self,
        parent: Option<TaskId>,
        admission: &crate::runtime::TaskAdmissionRecord,
        harness: &PersistentLocalHarness,
    ) -> Result<()> {
        if admission.parent != parent
            || admission.limits != self.config.limits
            || admission.run_limits != self.config.run_limits
        {
            return Err(Error::Conflict(
                "local swarm task admission no longer matches its pinned owner binding".into(),
            ));
        }
        let (_, requirements, machine_digest) = self.local_execution_contract(harness)?;
        let expected = crate::runtime::TaskAdmissionRecord::from_parts(
            admission.operation_id,
            "acyclic.local-swarm.turn",
            "1",
            admission.input.clone(),
            Self::local_turn_input_schema(),
            crate::executor::turn_output_schema(),
            &requirements,
            &machine_digest,
            parent,
            admission.grants.clone(),
            admission.limits,
            admission.run_limits,
            admission.policy.clone(),
            admission.extensions.clone(),
            admission.execution.clone(),
        )?;
        if expected.task != admission.task
            || expected.machine != admission.machine
            || expected.input_schema != admission.input_schema
            || expected.output_schema != admission.output_schema
        {
            return Err(Error::Conflict(
                "local swarm task admission identity or contract changed".into(),
            ));
        }
        Ok(())
    }

    /// Replays the exact admitted request after a process interruption.
    pub async fn retry(&self, task: TaskId) -> Result<LocalForkOutcome> {
        let request = self
            .requests
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm fork request {task}")))?;
        self.fork(request).await
    }

    /// Replays a typed child activation after interruption, preserving the
    /// exact request and published seed recorded in the swarm registry.
    pub async fn retry_published_child(
        &self,
        task: TaskId,
        host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
        stream: StreamClient<LocalStream>,
        issuer: AuthorityIssuer,
        parent: &mut StreamAggregate<LocalStream>,
    ) -> Result<LocalForkOutcome> {
        let request = self
            .requests
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm fork request {task}")))?;
        match self.outcome(task).await {
            Ok(output) => return Ok(LocalForkOutcome {
                child: task,
                operation: request.child_operation,
                output,
            }),
            Err(Error::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        // The caller may hold a parent aggregate opened before another
        // process published this seed. Refresh the authenticated parent
        // projection before deciding whether publication must be reconciled;
        // a stale reducer would republish an already-admitted child.
        *parent = self
            .open_session(request.parent)
            .await?
            .conversation_aggregate(self.config.limits)
            .await?;
        let seed = self.published_seed(task).await?;
        if parent.reducer().fork(&seed.child) != Some(&seed) {
            let report = self.prepared_report(task).await?;
            let mut child = StreamAggregate::open(
                &stream,
                seed.child.clone(),
                issuer.verifier(),
                SchemaRegistry::new(),
            )
            .await?;
            let parent_scope = self
                .open_session(request.parent)
                .await?
                .storage()
                .owner_scope()
                .clone();
            let child_scope = issuer.root_for_agent(
                seed.child_agent,
                "fork-bind",
                Capabilities::new(["conversation:bind".to_owned()]),
            );
            let reconciled = child
                .spawn_from_report(parent, report, parent_scope, child_scope)
                .await?;
            if reconciled != seed {
                return Err(Error::Conflict(
                    "reconciled fork seed differs from the admitted seed".into(),
                ));
            }
        }
        self.activate_published_child(request, host, stream, issuer, parent, &seed)
            .await
    }

    /// Reopens a child lazily after recovery. No child is dispatched merely
    /// because it has an activating registry record.
    pub async fn resume(&self, task: TaskId) -> Result<LocalSwarmSession> {
        let session = self.session(task).await?;
        if session.phase == LocalSessionPhase::Activating {
            return Err(Error::Conflict(
                "activation requires an explicit retry with its original fork request".into(),
            ));
        }
        let _ = self.open_session(task).await?;
        Ok(session)
    }

    async fn open_session(&self, task: TaskId) -> Result<Arc<PersistentLocalHarness>> {
        self.refresh_registry_state().await?;
        if !self.records.lock().await.contains_key(&task) {
            return Err(Error::NotFound(format!("local swarm task {task}")));
        }
        if let Some(existing) = self.sessions.lock().await.get(&task).cloned() {
            return Ok(existing);
        }
        let published_seed = self.seeds.lock().await.get(&task).cloned();
        if let Some(seed) = published_seed {
            let session = self.session(task).await?;
            let parent = session.parent.ok_or_else(|| {
                Error::Conflict("published child has no parent session".into())
            })?;
            let resolver = self.bindings.filesystem_fork_resolver.as_ref().ok_or_else(|| {
                Error::Unsupported("reopening a published child requires its filesystem resolver".into())
            })?;
            let secret = resolver.issuer_secret.ok_or_else(|| {
                Error::Unauthorized("local fork resolver has no durable host secret".into())
            })?;
            let operation = session.operation.ok_or_else(|| {
                Error::Conflict("published child has no operation identity".into())
            })?;
            let parent_harness = Box::pin(self.open_session(parent)).await?;
            let parent_aggregate = parent_harness.conversation_aggregate(self.config.limits).await?;
            let harness = Arc::new(
                PersistentLocalHarness::from_published_fork_with_tools_and_stream_provider(
                    self.config.model.clone(), self.provider.clone(), self.config.limits,
                    resolver.host.clone(), resolver.stream.clone(),
                    LocalFilesystemForkResolver::child_issuer(&seed.child, operation, secret),
                    &parent_aggregate, &seed, self.bindings.tools_for(task)?,
                    self.stream_provider.clone(),
                ).await?,
            );
            self.sessions.lock().await.insert(task, harness.clone());
            self.observe(LocalSwarmObservation::HarnessOpened { task });
            return Ok(harness);
        }
        let harness = Arc::new(
            PersistentLocalHarness::open_with_tools_and_project_on_providers(
                open_session_path(&self.root, task),
                self.config.model.clone(),
                self.provider.clone(),
                self.config.limits,
                self.bindings.tools_for(task)?,
                self.config.project.clone(),
                self.filesystem_host.clone(),
                self.conversation_stream.clone(),
                self.stream_provider.clone(),
            )
            .await?,
        );
        self.sessions.lock().await.insert(task, harness.clone());
        self.observe(LocalSwarmObservation::HarnessOpened { task });
        Ok(harness)
    }

    async fn update_session<F>(&self, task: TaskId, update: F) -> Result<()>
    where
        F: FnOnce(&mut LocalSwarmSession),
    {
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        self.update_session_at(observed_tail, task, update).await
    }

    async fn update_session_at<F>(&self, observed_tail: u64, task: TaskId, update: F) -> Result<()>
    where
        F: FnOnce(&mut LocalSwarmSession),
    {
        let stream = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let _refresh = self.registry_refresh.lock().await;
        let mut records = self.records.lock().await;
        let current = records
            .get_mut(&task)
            .ok_or_else(|| Error::NotFound(format!("local swarm task {task}")))?;
        let mut next = current.clone();
        update(&mut next);
        append_record_at(
            &stream,
            StoredEvent::Session(next.clone().into()),
            observed_tail,
        )
        .await?;
        *current = next;
        Ok(())
    }

    /// Commits a model turn's terminal session state against the durable
    /// registry.  The local task gate cannot protect a second process, so a
    /// fresh replay is required immediately before the append.  A retry of
    /// the same operation is an acknowledgement of the already committed
    /// terminal state; a different operation or a cancellation is a conflict.
    async fn complete_session(&self, task: TaskId, operation: OperationId) -> Result<()> {
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        let current = self.session(task).await?;
        if current.phase == LocalSessionPhase::Cancelled {
            return Err(Error::Conflict(
                "local swarm task was cancelled while its model turn was running".into(),
            ));
        }
        if current.phase == LocalSessionPhase::Completed {
            return if current.operation == Some(operation) {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "local swarm task already completed under another operation".into(),
                ))
            };
        }
        if let Some(existing) = current.operation
            && existing != operation
        {
            return Err(Error::Conflict(
                "local swarm task is bound to another operation".into(),
            ));
        }
        match self
            .update_session_at(observed_tail, task, |session| {
                session.operation = Some(operation);
                session.phase = LocalSessionPhase::Completed;
            })
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                // Another handle may have won the append between the refresh
                // and our CAS.  Reconcile once and only accept an identical
                // terminal operation; never turn a cancellation into success.
                self.refresh_registry_state().await?;
                let latest = self.session(task).await?;
                if latest.phase == LocalSessionPhase::Completed
                    && latest.operation == Some(operation)
                {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    fn task_gate(&self, task: TaskId) -> Result<Arc<Mutex<()>>> {
        let mut gates = self.live.task_gates.lock()
            .map_err(|_| Error::Storage("local task gates lock poisoned".into()))?;
        if let Some(gate) = gates.get(&task).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        gates.retain(|_, gate| gate.strong_count() != 0);
        let gate = Arc::new(Mutex::new(()));
        gates.insert(task, Arc::downgrade(&gate));
        Ok(gate)
    }

    /// Reconciles the in-memory index with the append-only registry before a
    /// terminal mutation. This is the cross-process half of the local
    /// completion fence: a second handle sees an admission or cancellation
    /// committed by the first handle before it can dispatch or append again.
    async fn refresh_registry_state(&self) -> Result<()> {
        self.refresh_registry_state_with_tail().await.map(|_| ())
    }

    async fn refresh_registry_state_with_tail(&self) -> Result<u64> {
        let _refresh = self.registry_refresh.lock().await;
        let stream = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let observed_tail = match stream.tail().await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let known_tail = *self.registry_tail.lock().await;
        if observed_tail < known_tail {
            return Err(Error::Conflict(
                "local swarm registry tail moved backwards during refresh".into(),
            ));
        }
        if observed_tail == known_tail {
            return Ok(observed_tail);
        }
        let records = load_records_range(&stream, known_tail, observed_tail).await?;
        let mut sessions = self.records.lock().await.clone();
        let mut requests = self.requests.lock().await.clone();
        let mut seeds = self.seeds.lock().await.clone();
        let mut reports = self.reports.lock().await.clone();
        let mut publications = self.publications.lock().await.clone();
        let mut declarations = self.declarations.lock().await.clone();
        let mut outcomes = self.outcomes.lock().await.clone();
        let mut completion_refs = self.completion_refs.lock().await.clone();
        let mut admissions = self.admissions.lock().await.clone();
        for record in records {
            apply_record(
                &mut sessions,
                &mut requests,
                &mut seeds,
                &mut reports,
                &mut publications,
                &mut declarations,
                &mut outcomes,
                &mut completion_refs,
                &mut admissions,
                record,
            )?;
        }
        // The refresh fence stays held until every derived map has been
        // replaced, so concurrent refreshers cannot replay the same suffix or
        // publish competing intermediate projections.
        *self.records.lock().await = sessions;
        *self.requests.lock().await = requests;
        *self.seeds.lock().await = seeds;
        *self.reports.lock().await = reports;
        *self.publications.lock().await = publications;
        *self.declarations.lock().await = declarations;
        *self.outcomes.lock().await = outcomes;
        *self.completion_refs.lock().await = completion_refs;
        *self.admissions.lock().await = admissions;
        *self.registry_tail.lock().await = observed_tail;
        Ok(observed_tail)
    }

    async fn mark_failed(&self, task: TaskId, reason: String) -> Result<()> {
        let bounded = reason.chars().take(512).collect::<String>();
        let stream = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        // Refresh first, then serialize the local publication. The append is
        // still CAS-protected for other handles: a cancellation or
        // completion that wins the tail race must remain terminal.
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        let append = {
            let _refresh = self.registry_refresh.lock().await;
            let current = self
                .records
                .lock()
                .await
                .get(&task)
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("local swarm task {task}")))?;
            if matches!(
                current.phase,
                LocalSessionPhase::Cancelled | LocalSessionPhase::Completed
            ) {
                return Ok(());
            }
            append_record_at(
                &stream,
                StoredEvent::ForkFailed {
                    child: task,
                    reason: bounded,
                },
                observed_tail,
            )
            .await
        };
        match append {
            Ok(()) => {
                // Replay the committed event through the same reducer used on
                // restart. A terminal event appended by another handle after
                // the failure is therefore retained in the final projection.
                self.refresh_registry_state().await?;
                Ok(())
            }
            Err(error) => {
                self.refresh_registry_state().await?;
                let current = self.session(task).await?;
                if matches!(
                    current.phase,
                    LocalSessionPhase::Cancelled | LocalSessionPhase::Completed
                ) {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }
}

/// Acquires the durable single-winner fence for a child model turn.
///
/// This is kept separate from the swarm projection so the stream CAS remains
/// the authority even when two independently opened swarm handles race. The
/// snapshot tail is sampled before reading records and reused for the append;
/// a tail conflict forces a fresh snapshot instead of permitting a second
/// activation claim.
async fn claim_child_activation_on_stream(
    stream: &acyclic_stream::Stream<LocalStream>,
    child: TaskId,
    operation: OperationId,
) -> Result<bool> {
    for _ in 0..4 {
        let observed_tail = match stream.tail().await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let records = load_records_at(stream, observed_tail).await?;
        let mut claimed = None;
        for record in records {
            match record.event {
                StoredEvent::ForkActivationClaimed {
                    child: recorded_child,
                    operation: recorded_operation,
                } if recorded_child == child => {
                    claimed = Some(recorded_operation);
                }
                StoredEvent::ForkCompleted {
                    child: recorded_child,
                    operation: recorded_operation,
                    ..
                } if recorded_child == child => {
                    if recorded_operation != operation {
                        return Err(Error::Conflict(
                            "terminal child activation belongs to another operation".into(),
                        ));
                    }
                    return Ok(false);
                }
                StoredEvent::ForkCancelled {
                    child: recorded_child,
                } if recorded_child == child => {
                    return Err(Error::Conflict(
                        "cancelled child activation cannot acquire a new claim".into(),
                    ));
                }
                StoredEvent::ForkFailed {
                    child: recorded_child,
                    ..
                }
                if recorded_child == child => {
                    claimed = None;
                }
                _ => {}
            }
        }
        if let Some(existing) = claimed {
            if existing != operation {
                return Err(Error::Conflict(
                    "child activation is already bound to another operation".into(),
                ));
            }
            return Ok(false);
        }
        match append_record_at(
            stream,
            StoredEvent::ForkActivationClaimed { child, operation },
            observed_tail,
        )
        .await
        {
            Ok(()) => return Ok(true),
            Err(Error::Conflict(_)) => continue,
            Err(error) => return Err(error),
        }
    }
    Err(Error::Indeterminate(operation))
}

fn open_session_path(root: &Path, task: TaskId) -> PathBuf {
    root.join("tasks").join(task.to_string())
}

fn operator_choice_key(task: TaskId, id: InteractionId) -> String {
    format!("{task}:{id}")
}

fn normalized_path(path: &Path) -> PathBuf {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|current| current.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn fork_seed_digest(seed: &ForkSeed) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(seed)
}

async fn load_records(stream: &acyclic_stream::Stream<LocalStream>) -> Result<Vec<StoredRecord>> {
    Ok(load_records_with_tail(stream).await?.1)
}

async fn load_records_with_tail(
    stream: &acyclic_stream::Stream<LocalStream>,
) -> Result<(u64, Vec<StoredRecord>)> {
    let tail = match stream.tail().await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => 0,
        Err(error) => return Err(Error::Storage(error.to_string())),
    };
    Ok((tail, load_records_at(stream, tail).await?))
}

async fn load_records_at(
    stream: &acyclic_stream::Stream<LocalStream>,
    tail: u64,
) -> Result<Vec<StoredRecord>> {
    load_records_range(stream, 0, tail).await
}

async fn load_records_range(
    stream: &acyclic_stream::Stream<LocalStream>,
    from: u64,
    tail: u64,
) -> Result<Vec<StoredRecord>> {
    if from > tail {
        return Err(Error::Conflict(
            "local swarm registry range starts after its observed tail".into(),
        ));
    }
    let count = tail - from;
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut records = stream
        .read(
            from,
            u32::try_from(count)
                .map_err(|_| Error::Storage("swarm registry is too large".into()))?,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mut decoded = Vec::new();
    let mut expected_sequence = from;
    while let Some(record) = records.next().await {
        let record = record.map_err(|error| Error::Storage(error.to_string()))?;
        if record.sequence != expected_sequence {
            return Err(Error::Conflict(
                "local swarm registry sequence is not contiguous".into(),
            ));
        }
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Storage("local swarm registry sequence overflow".into()))?;
        let value: StoredRecord = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if value.version != REGISTRY_VERSION {
            return Err(Error::Conflict(
                "unsupported local swarm registry version".into(),
            ));
        }
        decoded.push(value);
    }
    if expected_sequence != tail {
        return Err(Error::Conflict(
            "local swarm registry range ended before its observed tail".into(),
        ));
    }
    Ok(decoded)
}

async fn append_record(
    stream: &acyclic_stream::Stream<LocalStream>,
    event: StoredEvent,
) -> Result<()> {
    let tail = match stream.tail().await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => 0,
        Err(error) => return Err(Error::Storage(error.to_string())),
    };
    append_record_at(stream, event, tail).await
}

async fn append_record_at(
    stream: &acyclic_stream::Stream<LocalStream>,
    event: StoredEvent,
    observed_tail: u64,
) -> Result<()> {
    let bytes = crate::contract::canonical_json_bytes(&StoredRecord {
        version: REGISTRY_VERSION,
        event,
    })?;
    if bytes.len() > MAX_SWARM_RECORD_BYTES {
        return Err(Error::Invalid(
            "local swarm registry record exceeds its durable bound".into(),
        ));
    }
    match stream
        .append_at(bytes, observed_tail)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
    {
        AppendOutcome::Committed(_) => Ok(()),
        AppendOutcome::TailConflict { .. } => Err(Error::Conflict(
            "local swarm registry changed during append".into(),
        )),
    }
}

fn apply_record(
    sessions: &mut BTreeMap<TaskId, LocalSwarmSession>,
    requests: &mut BTreeMap<TaskId, LocalForkRequest>,
    seeds: &mut BTreeMap<TaskId, ForkSeed>,
    reports: &mut BTreeMap<TaskId, ForkReport>,
    publications: &mut BTreeMap<TaskId, ModelBatchPublication>,
    declarations: &mut BTreeMap<TaskId, LocalInheritedModelDeclaration>,
    outcomes: &mut BTreeMap<TaskId, TurnOutput>,
    completion_refs: &mut BTreeMap<TaskId, StoredCompletionRef>,
    admissions: &mut BTreeMap<TaskId, crate::runtime::TaskAdmissionRecord>,
    record: StoredRecord,
) -> Result<()> {
    match record.event {
        StoredEvent::Session(session) => {
            if session.version != REGISTRY_VERSION {
                return Err(Error::Conflict("unsupported local session version".into()));
            }
            let next: LocalSwarmSession = session.into();
            if let Some(existing) = sessions.get(&next.task) {
                if existing.phase == LocalSessionPhase::Cancelled
                    && next.phase != LocalSessionPhase::Cancelled
                {
                    return Err(Error::Conflict(
                        "persisted session completion follows terminal cancellation".into(),
                    ));
                }
                if existing.phase == LocalSessionPhase::Completed
                    && next.phase == LocalSessionPhase::Cancelled
                {
                    return Err(Error::Conflict(
                        "persisted session cancellation follows terminal completion".into(),
                    ));
                }
            }
            sessions.insert(next.task, next);
        }
        StoredEvent::TaskAdmitted { task, admission } => {
            let admission = crate::runtime::TaskAdmissionRecord::from_canonical_value(admission)?;
            if admission.operation_id.into_bytes() == [0; 16] {
                return Err(Error::Conflict("persisted task admission operation is empty".into()));
            }
            if let Some(existing) = admissions.get(&task)
                && existing.operation_id == admission.operation_id
                && existing != &admission
            {
                return Err(Error::Conflict(
                    "persisted task admission changed for the operation".into(),
                ));
            }
            admissions.insert(task, admission);
        }
        StoredEvent::ForkIntent { intent } => {
            intent.validate()?;
        }
        StoredEvent::ForkIntentSelected {
            intent,
            issuer_digest,
        } => {
            intent.validate()?;
            if let Some(digest) = issuer_digest
                && digest == [0; 32]
            {
                return Err(Error::Conflict(
                    "persisted local fork issuer binding is empty".into(),
                ));
            }
        }
        StoredEvent::ForkPublicationCompleted { operation, digest } => {
            if operation.into_bytes() == [0; 16] || digest == [0; 32] {
                return Err(Error::Conflict(
                    "persisted model fork publication receipt is invalid".into(),
                ));
            }
        }
        StoredEvent::ForkIssuerBinding { .. } => {}
        StoredEvent::ForkActivationClaimed { .. } => {}
        StoredEvent::ForkPrepared {
            parent,
            parent_operation,
            parent_step,
            child,
            child_operation,
            fork_operation,
            child_authority,
            child_agent,
            task,
            prompt,
            seed,
            seed_digest,
            report,
            publication,
            declaration,
            rebind_proof,
        }
        | StoredEvent::ForkAdmitted {
            parent,
            parent_operation,
            parent_step,
            child,
            child_operation,
            fork_operation,
            child_authority,
            child_agent,
            task,
            prompt,
            seed,
            seed_digest,
            report,
            publication,
            declaration,
            rebind_proof,
            ..
        } => {
            if rebind_proof.is_some() && (seed.is_none() || report.is_none()) {
                return Err(Error::Conflict(
                    "persisted fork rebound proof has no report and seed".into(),
                ));
            }
            if seed.is_some() != report.is_some() {
                return Err(Error::Conflict(
                    "persisted fork seed and report must be restored together".into(),
                ));
            }
            if let (Some(seed), Some(report)) = (&seed, &report) {
                if let Some(proof) = &rebind_proof {
                    report.validate_with_rebind_proof(proof)?;
                    if report.clone().into_seed_with_rebind_proof(proof)? != *seed {
                        return Err(Error::Conflict(
                            "persisted rebound fork report is not bound to its typed seed".into(),
                        ));
                    }
                } else {
                    report.validate()?;
                    if report.clone().into_seed()? != *seed {
                        return Err(Error::Conflict(
                            "persisted fork report is not bound to its typed seed".into(),
                        ));
                    }
                }
            }
            let parent_session = sessions
                .get(&parent)
                .ok_or_else(|| Error::Storage("fork parent session is missing".into()))?;
            let request = LocalForkRequest {
                parent,
                parent_operation,
                parent_step,
                child_operation,
                fork_operation,
                child_authority: child_authority.clone(),
                child_agent: child_agent.clone(),
                task: task.clone(),
                prompt: prompt.clone(),
            };
            if let Some(existing) = requests.get(&child)
                && existing != &request
            {
                return Err(Error::Conflict(
                    "persisted fork admission changed for the child key".into(),
                ));
            }
            let phase = sessions
                .get(&child)
                .map(|session| session.phase.clone())
                .filter(|phase| matches!(phase, LocalSessionPhase::Cancelled))
                .unwrap_or(LocalSessionPhase::Activating);
            sessions.insert(
                child,
                LocalSwarmSession {
                    task: child,
                    parent: Some(parent),
                    depth: parent_session.depth + 1,
                    task_description: task.clone(),
                    operation: Some(child_operation),
                    phase,
                },
            );
            requests.insert(child, request);
            if let Some(seed) = seed {
                if let Some(expected) = seed_digest {
                    if fork_seed_digest(&seed)? != expected {
                        return Err(Error::Conflict("persisted fork seed digest changed".into()));
                    }
                }
                if let Some(existing) = seeds.get(&child)
                    && existing != &seed
                {
                    return Err(Error::Conflict(
                        "persisted fork seed changed for the child key".into(),
                    ));
                }
                seeds.insert(child, seed);
            }
            if let Some(report) = report {
                if let Some(proof) = &rebind_proof {
                    report.validate_with_rebind_proof(proof)?;
                } else {
                    report.validate()?;
                }
                if let Some(existing) = reports.get(&child)
                    && existing != &report
                {
                    return Err(Error::Conflict(
                        "persisted fork report changed for the child key".into(),
                    ));
                }
                reports.insert(child, report);
            }
            if let Some(publication) = publication {
                if let Some(existing) = publications.get(&child)
                    && existing != &publication
                {
                    return Err(Error::Conflict(
                        "persisted model publication changed for the child key".into(),
                    ));
                }
                publications.insert(child, publication);
            }
            if let Some(declaration) = declaration {
                if let Some(existing) = declarations.get(&child)
                    && existing != &declaration
                {
                    return Err(Error::Conflict(
                        "persisted inherited declaration changed for the child key".into(),
                    ));
                }
                declarations.insert(child, declaration);
            }
        }
        StoredEvent::ForkCompleted {
            child,
            operation,
            output,
            output_ref,
            output_digest,
        } => {
            let session = sessions
                .get_mut(&child)
                .ok_or_else(|| Error::Storage("fork completion child is missing".into()))?;
            if session.phase == LocalSessionPhase::Cancelled {
                return Err(Error::Conflict(
                    "persisted child completion follows a terminal cancellation".into(),
                ));
            }
            if session.operation.is_some_and(|existing| existing != operation)
                || (session.parent.is_some()
                    && requests.get(&child).map(|request| request.child_operation) != Some(operation))
            {
                return Err(Error::Conflict(
                    "persisted child completion changed its admitted operation".into(),
                ));
            }
            match (output, output_ref, output_digest) {
                (Some(output), None, digest) => {
                    if let Some(digest) = digest {
                        let bytes = crate::contract::canonical_json_bytes(&output)?;
                        if crate::contract::canonical_json_digest(&bytes)? != digest {
                            return Err(Error::Conflict(
                                "persisted inline child completion digest changed".into(),
                            ));
                        }
                    }
                    if completion_refs.contains_key(&child)
                        || outcomes.get(&child).is_some_and(|existing| existing != &output)
                    {
                        return Err(Error::Conflict(
                            "persisted child completion changed its terminal output".into(),
                        ));
                    }
                    outcomes.insert(child, output);
                }
                (None, Some(file), Some(digest)) => {
                    let value = StoredCompletionRef {
                        operation,
                        file,
                        digest,
                    };
                    if outcomes.contains_key(&child)
                        || completion_refs.get(&child).is_some_and(|existing| existing != &value)
                    {
                        return Err(Error::Conflict(
                            "persisted child completion reference changed".into(),
                        ));
                    }
                    completion_refs.insert(child, value);
                }
                (None, None, _) | (None, Some(_), None) => {
                    return Err(Error::Conflict(
                        "persisted child completion has no replayable output".into(),
                    ));
                }
                (Some(_), Some(_), _) => {
                    return Err(Error::Conflict(
                        "persisted child completion has duplicate output forms".into(),
                    ));
                }
            }
            session.operation = Some(operation);
            session.phase = LocalSessionPhase::Completed;
        }
        StoredEvent::ForkFailed { child, reason } => {
            if let Some(session) = sessions.get_mut(&child) {
                // Failure is recoverable. Once cancellation or completion is
                // durable, a late failure publication cannot downgrade the
                // terminal state during refresh or restart.
                if !matches!(
                    session.phase,
                    LocalSessionPhase::Cancelled | LocalSessionPhase::Completed
                ) {
                    session.phase = LocalSessionPhase::Failed(reason);
                }
            }
        }
        StoredEvent::ForkCancelled { child } => {
            if let Some(session) = sessions.get_mut(&child) {
                if session.phase == LocalSessionPhase::Completed {
                    return Err(Error::Conflict(
                        "persisted child cancellation follows terminal completion".into(),
                    ));
                }
                session.phase = LocalSessionPhase::Cancelled;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextStage;
    use crate::interaction::Interaction;
    use crate::model::{ModelAttempt, ModelEvent, ModelRequest};
    use futures::{future::BoxFuture, stream::BoxStream};
    use serde_json::{Value, json};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct DepthDeniedResolver;

    impl LocalModelForkResolver for DepthDeniedResolver {
        fn preflight_depth<'a>(&'a self, _parent: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Err(Error::Unauthorized(LOCAL_DEPTH_LIMIT_ERROR.into())) })
        }

        fn resolve<'a>(
            &'a self,
            _intent: LocalForkIntent,
            _publication: ModelBatchPublication,
        ) -> BoxFuture<'a, Result<LocalModelForkPlan>> {
            Box::pin(async {
                Err(Error::Unsupported(
                    "depth-denied resolver must not allocate".into(),
                ))
            })
        }
    }

    fn depth_denied_invocation(
        parent_operation: OperationId,
        child_operation: OperationId,
    ) -> ToolInvocation {
        ToolInvocation::for_model_call(
            parent_operation,
            0,
            "fork-depth-denial".into(),
            "acyclic.fork_child".into(),
            json!({
                "child_operation": child_operation.to_string(),
                "task": "depth denied",
                "prompt": "must remain a typed result"
            }),
        )
    }

    #[tokio::test]
    async fn deterministic_depth_denial_is_typed_and_does_not_retain_intent() -> Result<()> {
        let parent = TaskId::from_bytes([0xD1; 16]);
        let parent_operation = OperationId::from_bytes([0xD2; 16]);
        let child_operation = OperationId::from_bytes([0xD3; 16]);
        let context = ModelToolContext {
            parent_operation,
            step: 0,
            task_id: Some(parent),
        };
        let plans = Arc::new(
            LocalModelForkPlans::new().with_resolver(Arc::new(DepthDeniedResolver)),
        );
        let executor = LocalForkToolExecutor {
            parent,
            plans: plans.clone(),
        };
        let invocation = depth_denied_invocation(parent_operation, child_operation);
        let result = executor
            .execute_in_model_batch(context, invocation.clone())
            .await?;
        assert_eq!(result.value["status"], "denied");
        assert_eq!(result.value["reason"], LOCAL_DEPTH_LIMIT_REASON);
        assert!(!plans.has_intent(context.publication_operation()).await?);

        let recovered = executor
            .reconcile_in_model_batch(context, invocation)
            .await?
            .expect("deterministic denial should be replayable");
        assert_eq!(recovered.value, result.value);
        Ok(())
    }

    #[test]
    fn source_project_selection_only_falls_back_for_root() -> Result<()> {
        let provider = ProviderRef::new("local", "filesystem", "2")?;
        let root = VolumeRef::new(
            provider.clone(),
            "root-project",
            VolumeClass::Project,
            VolumeOwner::Project("root".into()),
        )?;
        let direct_parent = VolumeRef::new(
            provider,
            "parent-project",
            VolumeClass::Project,
            VolumeOwner::Project("root".into()),
        )?;
        assert_eq!(source_project_for_parent(None, true, &root)?, root);
        assert_eq!(
            source_project_for_parent(Some(direct_parent.clone()), false, &root)?,
            direct_parent
        );
        assert!(matches!(
            source_project_for_parent(None, false, &root),
            Err(Error::Conflict(message)) if message.contains("direct parent project binding")
        ));
        Ok(())
    }

    #[test]
    fn recursive_depth_policy_denies_at_the_configured_boundary() {
        assert!(validate_recursive_depth(0, 1).is_ok());
        assert!(matches!(
            validate_recursive_depth(1, 1),
            Err(Error::Unauthorized(message)) if message.contains("depth limit")
        ));
    }

    #[tokio::test]
    async fn admitted_child_context_replays_persisted_boundary_before_live_tail() -> Result<()> {
        let limits = Limits::default();
        let boundary = CompletedModelBoundary::capture(
            ModelRequest {
                model: Model::new("mock", "fork-recovery", "1", Value::Null)?,
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("pinned parent exchange".into()),
                }],
                tools: Vec::new(),
                max_output_tokens: Some(64),
            },
            limits,
        )?;
        let declaration = LocalInheritedModelDeclaration {
            boundary: boundary.clone(),
            suffix: vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("child task; fresh scratch".into()),
            }],
        };
        let context = declaration.context(limits)?;

        // A later parent tail is deliberately unrelated to the admitted
        // declaration. Applying recovery must replay only its frozen bytes
        // and explicit child suffix.
        let live_parent_tail = ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("later parent mutation".into()),
        };
        let applied = context
            .apply(
                &crate::context::ContextInput {
                    input: ModelContent::Text("child prompt".into()),
                    selected_context: None,
                    step: 0,
                    prior_messages: Vec::new(),
                },
                crate::context::Context::default(),
            )
            .await?;
        assert_eq!(
            applied.messages,
            [boundary.request.messages.clone(), declaration.suffix.clone()].concat()
        );
        assert!(!applied.messages.contains(&live_parent_tail));
        Ok(())
    }

    fn test_fork_intent(child: u8) -> LocalForkIntent {
        LocalForkIntent {
            parent: TaskId::from_bytes([1; 16]),
            parent_operation: OperationId::from_bytes([2; 16]),
            parent_step: 3,
            publication_operation: Some(OperationId::from_bytes([4; 16])),
            fork_operation: OperationId::from_bytes([5; 16]),
            child_operation: OperationId::from_bytes([child; 16]),
            call_id: Some(format!("fork-{child}")),
            task: format!("child-{child}"),
            prompt: "preserve this exact prompt".into(),
        }
    }

    #[test]
    fn intent_replay_rejects_zero_issuer_and_conflicting_payloads() {
        let intent = test_fork_intent(6);
        let mut intents = BTreeMap::new();
        let mut order = BTreeMap::new();
        let mut bindings = BTreeMap::new();
        let zero_issuer = StoredRecord {
            version: REGISTRY_VERSION,
            event: StoredEvent::ForkIntentSelected {
                intent: intent.clone(),
                issuer_digest: Some([0; 32]),
            },
        };
        assert!(matches!(
            replay_fork_intent_record(0, zero_issuer, &mut intents, &mut order, &mut bindings),
            Err(Error::Conflict(message)) if message.contains("issuer binding is empty")
        ));

        let selected = StoredRecord {
            version: REGISTRY_VERSION,
            event: StoredEvent::ForkIntent { intent: intent.clone() },
        };
        replay_fork_intent_record(1, selected, &mut intents, &mut order, &mut bindings)
            .expect("first durable intent replays");
        let mut changed = intent;
        changed.prompt = "changed after selection".into();
        let conflicting = StoredRecord {
            version: REGISTRY_VERSION,
            event: StoredEvent::ForkIntent { intent: changed },
        };
        assert!(matches!(
            replay_fork_intent_record(2, conflicting, &mut intents, &mut order, &mut bindings),
            Err(Error::Conflict(message)) if message.contains("intent changed")
        ));
    }

    #[tokio::test]
    async fn fork_intent_selection_reconciles_across_handles_without_duplicate_records(
    ) -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(
            LocalStream::open(root.path(), LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let client = StreamClient::new(provider);
        let first = LocalModelForkPlans::new();
        let second = LocalModelForkPlans::new();
        first.bind_journal(client.clone()).await?;
        second.bind_journal(client.clone()).await?;
        let intent = test_fork_intent(7);

        first.record_intent(intent.clone()).await?;
        assert!(second.has_intent(intent.publication_operation.unwrap()).await?);
        second.record_intent(intent.clone()).await?;

        let stream = client
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let records = load_records(&stream).await?;
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    &record.event,
                    StoredEvent::ForkIntentSelected { intent: recorded, .. }
                        if recorded == &intent
                ))
                .count(),
            1,
            "a retry from another handle must reuse the durable intent"
        );
        Ok(())
    }

    #[tokio::test]
    async fn fork_publication_completion_receipt_survives_restart_and_fences_substitution(
    ) -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(
            LocalStream::open(root.path(), LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let client = StreamClient::new(provider);
        let first = LocalModelForkPlans::new();
        let second = LocalModelForkPlans::new();
        first.bind_journal(client.clone()).await?;
        second.bind_journal(client.clone()).await?;
        let operation = OperationId::from_bytes([8; 16]);
        let digest = [9; 32];

        first.mark_completed(operation, digest).await?;
        assert_eq!(second.completed(operation).await?, Some(digest));
        second.mark_completed(operation, digest).await?;
        assert!(matches!(
            second.mark_completed(operation, [10; 32]).await,
            Err(Error::Conflict(message)) if message.contains("result changed")
        ));

        let stream = client
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let records = load_records(&stream).await?;
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    &record.event,
                    StoredEvent::ForkPublicationCompleted {
                        operation: recorded,
                        digest: recorded_digest,
                    } if *recorded == operation && *recorded_digest == digest
                ))
                .count(),
            1,
            "completion retry must not append a second receipt"
        );

        let reopened = LocalModelForkPlans::new();
        reopened.bind_journal(client).await?;
        assert_eq!(reopened.completed(operation).await?, Some(digest));
        Ok(())
    }

    struct MockModel {
        calls: AtomicUsize,
        requests: Mutex<Vec<ModelRequest>>,
    }

    struct RecordingObserver {
        events: Mutex<Vec<LocalSwarmObservation>>,
    }

    impl LocalSwarmObserver for RecordingObserver {
        fn observe(&self, observation: LocalSwarmObservation) {
            self.events.lock().expect("observer lock").push(observation);
        }
    }

    struct RecordingCommunicationHost {
        observed: Mutex<Vec<TaskId>>,
    }

    impl crate::runtime::DurableTaskHost for RecordingCommunicationHost {
        fn observe_admission<'a>(
            &'a self,
            task_id: TaskId,
        ) -> BoxFuture<'a, Result<crate::runtime::TaskAdmissionRecord>> {
            Box::pin(async move {
                self.observed
                    .lock()
                    .expect("communication host lock")
                    .push(task_id);
                let schema = json!({"type":"object"});
                crate::runtime::TaskAdmissionRecord::from_parts(
                    OperationId::from_bytes(task_id.into_bytes()),
                    "communication.test",
                    "1",
                    json!({}),
                    schema.clone(),
                    schema,
                    &BTreeSet::new(),
                    &[7; 32],
                    None,
                    Capabilities::default(),
                    Limits::default(),
                    TaskRunLimits::default(),
                    None,
                    None,
                    None,
                )
            })
        }

        fn outcome<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<Option<crate::Outcome<Value>>>> {
            Box::pin(async { Ok(None) })
        }

        fn cancel<'a>(&'a self, _task_id: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// A host whose cancellation call can be held at a controlled barrier.
    /// This makes lock-order regressions deterministic: the swarm must release
    /// its projection fence before awaiting the host, then reacquire it for the
    /// final durable session read.
    struct ControlledCancellationHost {
        inner: RecordingCommunicationHost,
        entered: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Barrier>,
    }

    impl crate::runtime::DurableTaskHost for ControlledCancellationHost {
        fn observe_admission<'a>(
            &'a self,
            task_id: TaskId,
        ) -> BoxFuture<'a, Result<crate::runtime::TaskAdmissionRecord>> {
            <RecordingCommunicationHost as crate::runtime::DurableTaskHost>::observe_admission(
                &self.inner,
                task_id,
            )
        }

        fn outcome<'a>(
            &'a self,
            task_id: TaskId,
        ) -> BoxFuture<'a, Result<Option<crate::Outcome<Value>>>> {
            <RecordingCommunicationHost as crate::runtime::DurableTaskHost>::outcome(
                &self.inner,
                task_id,
            )
        }

        fn cancel<'a>(&'a self, _task_id: TaskId) -> BoxFuture<'a, Result<()>> {
            let entered = self.entered.clone();
            let release = self.release.clone();
            Box::pin(async move {
                entered.wait().await;
                release.wait().await;
                Ok(())
            })
        }
    }

    struct ImmediateWaitStore {
        opened: Mutex<Vec<crate::communication::WaitRequest>>,
        completed: Mutex<Vec<crate::communication::WaitCompletion>>,
    }

    impl crate::communication::DurableWaitStore for ImmediateWaitStore {
        fn open<'a>(
            &'a self,
            request: crate::communication::WaitRequest,
        ) -> BoxFuture<'a, Result<Option<crate::communication::WaitCompletion>>> {
            Box::pin(async move {
                self.opened.lock().expect("wait store lock").push(request);
                Ok(None)
            })
        }

        fn complete<'a>(
            &'a self,
            _request: crate::communication::WaitRequest,
            completion: crate::communication::WaitCompletion,
        ) -> BoxFuture<'a, Result<crate::communication::WaitCompletion>> {
            Box::pin(async move {
                self.completed
                    .lock()
                    .expect("wait store lock")
                    .push(completion.clone());
                Ok(completion)
            })
        }
    }

    struct CommunicationModel {
        calls: AtomicUsize,
    }

    impl ModelProvider for CommunicationModel {
        fn generate<'a>(
            &'a self,
            _prepared: crate::model_input::PreparedModelInput,
        ) -> BoxStream<'a, Result<ModelEvent>> {
            if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
                return Box::pin(futures::stream::iter([Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                })]));
            }
            Box::pin(futures::stream::iter([
                Ok(ModelEvent::ToolCall {
                    call_id: "wait-call".into(),
                    name: crate::communication_tools::WAIT_TOOL_NAME.into(),
                    arguments: json!({
                        "kind": "deadline",
                        "deadline_epoch_ms": 1,
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    #[tokio::test]
    async fn concurrent_activation_claims_have_one_durable_winner() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(
            LocalStream::open(root.path(), LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let client = StreamClient::new(provider);
        let first = client
            .stream("swarm/records")
            .map_err(|error| Error::Storage(error.to_string()))?;
        let second = client
            .stream("swarm/records")
            .map_err(|error| Error::Storage(error.to_string()))?;
        let child = TaskId::from_bytes([0xC1; 16]);
        let operation = OperationId::from_bytes([0xD1; 16]);
        let (left, right) = tokio::join!(
            claim_child_activation_on_stream(&first, child, operation),
            claim_child_activation_on_stream(&second, child, operation),
        );
        let results = [left?, right?];
        assert_eq!(results.iter().filter(|claimed| **claimed).count(), 1);
        assert_eq!(results.iter().filter(|claimed| !**claimed).count(), 1);

        let records = load_records(&first).await?;
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record.event,
                    StoredEvent::ForkActivationClaimed { child: recorded, operation: recorded_operation }
                        if recorded == child && recorded_operation == operation
                ))
                .count(),
            1,
            "concurrent handles must persist one activation claim"
        );
        Ok(())
    }

    #[tokio::test]
    async fn terminal_activation_cannot_publish_another_claim() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let client = StreamClient::new(Arc::new(
            LocalStream::open(root.path(), LocalStreamLimits::default())
                .await.map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let stream = client.stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let child = TaskId::from_bytes([0xC2; 16]);
        let operation = OperationId::from_bytes([0xD2; 16]);
        assert!(claim_child_activation_on_stream(&stream, child, operation).await?);
        append_record(&stream, StoredEvent::ForkCancelled { child }).await?;
        let cancelled_tail = stream.tail().await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(matches!(
            claim_child_activation_on_stream(&stream, child, operation).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(stream.tail().await
            .map_err(|error| Error::Storage(error.to_string()))?, cancelled_tail);

        let completed = TaskId::from_bytes([0xC3; 16]);
        let output = TurnOutput {
            text: "durable result".into(), attachments: Vec::new(),
            metadata: Value::Null, steps: 1,
        };
        append_record(&stream, StoredEvent::ForkCompleted {
            child: completed, operation, output: Some(output),
            output_ref: None, output_digest: None,
        }).await?;
        let completed_tail = stream.tail().await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(!claim_child_activation_on_stream(&stream, completed, operation).await?);
        assert!(matches!(
            claim_child_activation_on_stream(
                &stream, completed, OperationId::from_bytes([0xD3; 16]),
            ).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(stream.tail().await
            .map_err(|error| Error::Storage(error.to_string()))?, completed_tail);
        Ok(())
    }

    #[tokio::test]
    async fn cold_completion_replay_rejects_changed_operation_and_output() -> Result<()> {
        for substitution in ["identical", "operation", "output"] {
            let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
            let model = Model::new("mock", "completion-fence", "1", json!({}))?;
            let provider = Arc::new(MockModel {
                calls: AtomicUsize::new(0), requests: Mutex::new(Vec::new()),
            });
            let swarm = PersistentLocalSwarm::open_with_model(
                root.path(), model.clone(), provider.clone(), Limits::default(),
            ).await?;
            let child = swarm.root_task().await?;
            let operation = OperationId::from_bytes([0xD4; 16]);
            let output = TurnOutput {
                text: "original terminal bytes".into(), attachments: Vec::new(),
                metadata: Value::Null, steps: 1,
            };
            let stream = swarm.registry.stream(REGISTRY_STREAM)
                .map_err(|error| Error::Storage(error.to_string()))?;
            append_record(&stream, StoredEvent::ForkCompleted {
                child, operation, output: Some(output.clone()),
                output_ref: None, output_digest: None,
            }).await?;
            let mut replay = output.clone();
            if substitution == "output" { replay.text = "substituted terminal bytes".into(); }
            append_record(&stream, StoredEvent::ForkCompleted {
                child,
                operation: if substitution == "operation" {
                    OperationId::from_bytes([0xD5; 16])
                } else { operation },
                output: Some(replay), output_ref: None, output_digest: None,
            }).await?;
            drop(stream);
            drop(swarm);
            let reopened = PersistentLocalSwarm::open_with_model(
                root.path(), model, provider.clone(), Limits::default(),
            ).await;
            if substitution == "identical" {
                assert_eq!(reopened?.outcome(child).await?, output);
            } else {
                assert!(matches!(reopened, Err(Error::Conflict(_))));
            }
            assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn activation_failure_requires_journal_proof_before_releasing_claim() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let model = Model::new("mock", "activation-failure", "1", json!({}))?;
        let provider = Arc::new(MockModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let swarm = PersistentLocalSwarm::open_with_model(
            root.path(), model.clone(), provider.clone(), Limits::default(),
        ).await?;
        let task = swarm.root_task().await?;
        let harness = swarm.open_session(task).await?;
        let registry = swarm.registry.stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let operation = OperationId::from_bytes([0xD2; 16]);
        assert!(swarm.claim_child_activation(&registry, task, operation).await?);
        let failure = Error::Storage("child storage unavailable".into());

        // Unknown storage and an explicitly uncertain effect must not append
        // ForkFailed, even when the available journal is still empty.
        swarm.mark_activation_failed_if_safe(task, operation, None, &failure).await?;
        swarm.mark_activation_failed_if_safe(
            task, operation, Some(&harness), &Error::Indeterminate(operation),
        ).await?;
        assert!(!swarm.claim_child_activation(&registry, task, operation).await?);

        // Execute through the real local journal, then inject a setup failure.
        // Its durable ModelStarted must retain the original activation claim.
        harness.run(operation, "retain the admitted activation").await?;
        assert!(swarm.child_model_started(&harness, operation).await?);
        swarm.mark_activation_failed_if_safe(
            task, operation, Some(&harness), &failure,
        ).await?;
        let records = load_records(&registry).await?;
        assert!(!records.iter().any(|record| matches!(
            record.event, StoredEvent::ForkFailed { child, .. } if child == task
        )));
        let dispatches = provider.calls.load(Ordering::SeqCst);
        drop(registry);
        drop(harness);
        drop(swarm);

        let reopened = PersistentLocalSwarm::open_with_model(
            root.path(), model, provider.clone(), Limits::default(),
        ).await?;
        let registry = reopened.registry.stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(!reopened.claim_child_activation(&registry, task, operation).await?);
        assert_eq!(provider.calls.load(Ordering::SeqCst), dispatches);

        // A distinct operation with an available empty journal can release its
        // claim. This exercises the proven-not-started branch as well.
        let unstarted = OperationId::from_bytes([0xD3; 16]);
        let harness = reopened.open_session(task).await?;
        assert!(!reopened.child_model_started(&harness, unstarted).await?);
        // The previous claim binds this task to its original operation, so use
        // a fresh composition for the independent pre-dispatch failure.
        drop(harness);
        let fresh_root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let fresh = PersistentLocalSwarm::open_with_model(
            fresh_root.path(), Model::new("mock", "activation-failure", "1", json!({}))?,
            provider, Limits::default(),
        ).await?;
        let fresh_task = fresh.root_task().await?;
        let harness = fresh.open_session(fresh_task).await?;
        let registry = fresh.registry.stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(fresh.claim_child_activation(&registry, fresh_task, unstarted).await?);
        fresh.mark_activation_failed_if_safe(
            fresh_task, unstarted, Some(&harness), &failure,
        ).await?;
        assert_eq!(fresh.session(fresh_task).await?.phase,
            LocalSessionPhase::Failed(failure.to_string()));
        assert!(fresh.claim_child_activation(&registry, fresh_task, unstarted).await?);
        Ok(())
    }

    #[tokio::test]
    async fn denied_model_options_do_not_create_swarm_providers_or_volumes() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(MockModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let model = Model::new("mock", "swarm", "1", json!({"api_key": "private-state"}))?;
        let ordinary = root.path().join("ordinary");
        assert!(matches!(
            PersistentLocalSwarm::open_with_model(
                &ordinary,
                model.clone(),
                provider.clone(),
                Limits::default(),
            )
            .await,
            Err(Error::Invalid(_))
        ));
        assert!(!ordinary.exists(), "denied model created swarm providers");
        let recursive = root.path().join("recursive");
        assert!(matches!(
            PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
                &recursive,
                model,
                provider.clone(),
                Limits::default(),
            )
            .await,
            Err(Error::Invalid(_))
        ));
        assert!(
            !recursive.exists(),
            "denied model created recursive volumes"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn model_wait_uses_the_authenticated_task_in_persistent_swarm() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let host = Arc::new(RecordingCommunicationHost {
            observed: Mutex::new(Vec::new()),
        });
        let waits = Arc::new(ImmediateWaitStore {
            opened: Mutex::new(Vec::new()),
            completed: Mutex::new(Vec::new()),
        });
        let bindings = LocalSwarmBindings::communication(host.clone(), Some(waits.clone()), None);
        let model = Model::new("mock", "communication", "1", json!({}))?;
        let swarm = PersistentLocalSwarm::open_with_model_and_bindings(
            root.path(),
            model,
            Arc::new(CommunicationModel {
                calls: AtomicUsize::new(0),
            }),
            Limits::default(),
            bindings,
        )
        .await?;
        let root_task = swarm.root_task().await?;
        assert!(matches!(
            swarm.read_inbox(TaskId::from_bytes([99; 16]), 0, 1).await,
            Err(Error::NotFound(_))
        ));
        assert!(
            host.observed
                .lock()
                .expect("communication host lock")
                .is_empty()
        );
        swarm
            .run_root(OperationId::from_bytes([82; 16]), "wait for the deadline")
            .await?;
        let observed = host.observed.lock().expect("communication host lock");
        assert!(!observed.is_empty());
        assert!(observed.iter().all(|task| *task == root_task));
        assert_eq!(waits.opened.lock().expect("wait store lock").len(), 1);
        assert_eq!(
            waits.completed.lock().expect("wait store lock").as_slice(),
            &[crate::communication::WaitCompletion::Deadline]
        );
        Ok(())
    }

    #[tokio::test]
    async fn registry_range_rejects_a_missing_record_before_the_pinned_tail() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let swarm = PersistentLocalSwarm::open_with_model(
            root.path(),
            Model::new("mock", "local-swarm", "1", json!({}))?,
            Arc::new(MockModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            Limits::default(),
        )
        .await?;
        let registry = swarm
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let tail = registry
            .tail()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            load_records_range(&registry, 0, tail).await?.len() as u64,
            tail
        );
        for from in [0, tail] {
            let error = load_records_range(&registry, from, tail + 1)
                .await
                .expect_err("an incomplete registry page must not become a projection");
            assert!(matches!(error, Error::Conflict(ref reason)
                if reason == "local swarm registry range ended before its observed tail"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_registry_reads_share_one_serialized_projection_refresh() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let swarm = PersistentLocalSwarm::open_with_model(
            root.path(),
            model,
            Arc::new(MockModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            Limits::default(),
        )
        .await?;
        let root_task = swarm.root_task().await?;
        let mut unseen = swarm.session(root_task).await?;
        unseen.task_description = "unseen registry session".into();
        let registry = swarm
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        append_record(&registry, StoredEvent::Session(unseen.into())).await?;
        let start = Arc::new(tokio::sync::Barrier::new(2));
        let left_start = start.clone();
        let right_start = start.clone();
        let left_read = async {
            left_start.wait().await;
            swarm.sessions().await
        };
        let right_read = async {
            right_start.wait().await;
            swarm.sessions().await
        };
        let (left, right) = tokio::join!(left_read, right_read);
        let left = left?;
        let right = right?;
        assert_eq!(left, right);
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].task_description, "unseen registry session");
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_empty_openers_reconcile_the_winning_root() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let root_path = normalized_path(root.path());
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        *EMPTY_REGISTRY_OPEN_BARRIER
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .expect("empty-registry barrier lock") = Some((root_path, barrier));

        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let provider = Arc::new(MockModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let first = PersistentLocalSwarm::open_with_model(
            root.path(),
            model.clone(),
            provider.clone(),
            Limits::default(),
        );
        let second =
            PersistentLocalSwarm::open_with_model(root.path(), model, provider, Limits::default());
        let opened = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(first, second)
        })
        .await
        .map_err(|_| Error::Storage("concurrent empty opener test timed out".into()))?;
        *EMPTY_REGISTRY_OPEN_BARRIER
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .expect("empty-registry barrier lock") = None;
        let (first, second) = opened;
        let first = first?;
        let second = second?;
        assert_eq!(first.root_task().await?, second.root_task().await?);
        assert_eq!(first.sessions().await?, second.sessions().await?);
        assert_eq!(first.sessions().await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn cold_metadata_snapshot_does_not_open_a_child_harness() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let swarm = PersistentLocalSwarm::open_with_model(
            root.path(),
            model,
            Arc::new(MockModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            Limits::default(),
        )
        .await?;
        let parent = swarm.root_task().await?;
        let child = TaskId::new();
        let registry = swarm
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        append_record(
            &registry,
            StoredEvent::Session(StoredSession {
                version: REGISTRY_VERSION,
                task: child,
                parent: Some(parent),
                depth: 1,
                task_description: "cold child".into(),
                operation: None,
                phase: StoredPhase::Ready,
            }),
        )
        .await?;
        let (snapshot, agents) = swarm.session_snapshot_with_agents(child).await?;
        assert_eq!(snapshot.session.task, child);
        assert_eq!(snapshot.conversation_revision, 0);
        assert_eq!(snapshot.workspace_generation, None);
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].session.task, child);
        assert!(!swarm.sessions.lock().await.contains_key(&child));
        Ok(())
    }

    #[tokio::test]
    async fn cross_handle_metadata_stays_lazy_and_workspace_cas_rejects_stale_publish() -> Result<()>
    {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let provider = Arc::new(MockModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let first_observer = Arc::new(RecordingObserver {
            events: Mutex::new(Vec::new()),
        });
        let second_observer = Arc::new(RecordingObserver {
            events: Mutex::new(Vec::new()),
        });
        let first = PersistentLocalSwarm::open_with_bindings(
            root.path(),
            LocalSwarmConfig::new(model.clone(), Limits::default())?,
            provider.clone(),
            LocalSwarmBindings::default().with_observer(first_observer.clone()),
        )
        .await?;
        let second = PersistentLocalSwarm::open_with_bindings(
            root.path(),
            LocalSwarmConfig::new(model.clone(), Limits::default())?,
            provider,
            LocalSwarmBindings::default().with_observer(second_observer.clone()),
        )
        .await?;
        let parent = first.root_task().await?;
        let child = TaskId::new();
        let registry = first
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        append_record(
            &registry,
            StoredEvent::Session(StoredSession {
                version: REGISTRY_VERSION,
                task: child,
                parent: Some(parent),
                depth: 1,
                task_description: "cross-handle cold child".into(),
                operation: None,
                phase: StoredPhase::Ready,
            }),
        )
        .await?;
        let listed = second.sessions().await?;
        assert!(listed.iter().any(|session| session.task == child));
        assert!(!second.sessions.lock().await.contains_key(&child));
        let page = second.sessions_page(None, 1).await?;
        assert_eq!(page.items.len(), 1);
        let snapshot = second.session_snapshot(child).await?;
        assert_eq!(snapshot.workspace_generation, None);
        assert_eq!(snapshot.conversation_revision, 0);
        assert!(!second.sessions.lock().await.contains_key(&child));
        let _activity = second.read_activity(parent, 0, 1).await?;
        let observations = second_observer
            .events
            .lock()
            .expect("observer lock")
            .clone();
        assert!(observations.contains(&LocalSwarmObservation::SessionList { returned: 2 }));
        assert!(observations.contains(&LocalSwarmObservation::SessionPage { returned: 1 }));
        assert!(observations.contains(&LocalSwarmObservation::SessionSnapshot { task: child }));
        assert!(observations.iter().any(|event| matches!(
            event,
            LocalSwarmObservation::HistoryPage { task, .. } if *task == parent
        )));
        assert!(!observations.contains(&LocalSwarmObservation::HarnessOpened { task: child }));
        assert!(!observations.iter().any(|event| matches!(
            event,
            LocalSwarmObservation::WorkspacePage { .. }
                | LocalSwarmObservation::WorkspaceFile { .. }
                | LocalSwarmObservation::ModelWorkerStarted { .. }
        )));

        let root_harness = first.open_session(parent).await?;
        let volume = root_harness.storage().volume().clone();
        let workspace = workspace_ref(volume.provider().clone(), &volume.storage_name()?)?;
        let head = first.filesystem_host.resolve(&workspace).await?;
        let left_mutations = [crate::filesystem::WorkspaceMutation::PutFile {
            path: "/left.txt".into(),
            bytes: b"left".to_vec(),
        }];
        let right_mutations = [crate::filesystem::WorkspaceMutation::PutFile {
            path: "/right.txt".into(),
            bytes: b"right".to_vec(),
        }];
        let left_key = crate::IdempotencyKey::new("cross-handle-left")?;
        let right_key = crate::IdempotencyKey::new("cross-handle-right")?;
        let left = first.filesystem_host.apply(
            &workspace,
            Some(&head.generation),
            &left_mutations,
            &left_key,
        );
        let right = second.filesystem_host.apply(
            &workspace,
            Some(&head.generation),
            &right_mutations,
            &right_key,
        );
        let (left, right) = tokio::join!(left, right);
        assert!(matches!(
            (left, right),
            (Ok(_), Err(Error::Conflict(_))) | (Err(Error::Conflict(_)), Ok(_))
        ));
        let committed = first.filesystem_host.resolve(&workspace).await?;
        assert_ne!(committed.generation, head.generation);

        drop(second);
        let reopened = PersistentLocalSwarm::open_with_model(
            root.path(),
            model,
            Arc::new(MockModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            Limits::default(),
        )
        .await?;
        assert!(
            reopened
                .sessions()
                .await?
                .iter()
                .any(|session| session.task == child)
        );
        assert!(!reopened.sessions.lock().await.contains_key(&child));
        Ok(())
    }

    #[tokio::test]
    async fn refresh_cannot_overwrite_a_direct_session_publication() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let swarm = PersistentLocalSwarm::open_with_model(
            root.path(),
            model,
            Arc::new(MockModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            Limits::default(),
        )
        .await?;
        let task = swarm.root_task().await?;
        let start = Arc::new(tokio::sync::Barrier::new(2));
        let refresh_start = start.clone();
        let write_start = start.clone();
        let refresh = async {
            refresh_start.wait().await;
            swarm.sessions().await
        };
        let write = async {
            write_start.wait().await;
            swarm
                .update_session(task, |session| {
                    session.task_description = "direct publication".into();
                })
                .await
        };
        let (sessions, written) = tokio::join!(refresh, write);
        let sessions = sessions?;
        written?;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].task, task);
        assert_eq!(sessions[0].phase, LocalSessionPhase::Ready);
        assert!(matches!(
            sessions[0].task_description.as_str(),
            "root" | "direct publication"
        ));
        // The concurrent page may have begun before the writer's durable
        // publication. Once both operations have joined, a fresh page must
        // observe the committed publication exactly.
        let committed = swarm.sessions().await?;
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].task, task);
        assert_eq!(committed[0].phase, LocalSessionPhase::Ready);
        assert_eq!(committed[0].task_description, "direct publication");
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_releases_projection_fence_before_host_and_final_refresh() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let entered = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let host = Arc::new(ControlledCancellationHost {
            inner: RecordingCommunicationHost {
                observed: Mutex::new(Vec::new()),
            },
            entered: entered.clone(),
            release: release.clone(),
        });
        let bindings = LocalSwarmBindings::communication(host, None, None);
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let swarm = Arc::new(
            PersistentLocalSwarm::open_with_model_and_bindings(
                root.path(),
                model,
                Arc::new(MockModel {
                    calls: AtomicUsize::new(0),
                    requests: Mutex::new(Vec::new()),
                }),
                Limits::default(),
                bindings,
            )
            .await?,
        );
        let task = swarm.root_task().await?;
        let mut cancel = {
            let swarm = swarm.clone();
            tokio::spawn(async move { swarm.cancel(task).await })
        };

        // Let the production host call begin, then release it. If cancel held
        // registry_refresh across host.cancel, the subsequent session refresh
        // would deadlock and the bounded join below would fail.
        if tokio::time::timeout(std::time::Duration::from_secs(2), entered.wait())
            .await
            .is_err()
        {
            cancel.abort();
            let _ = cancel.await;
            return Err(Error::Storage("cancellation host call timed out".into()));
        }
        release.wait().await;
        let cancelled = match tokio::time::timeout(std::time::Duration::from_secs(2), &mut cancel)
            .await
        {
            Ok(result) => result
                .map_err(|error| Error::Storage(format!("cancellation task failed: {error}")))??,
            Err(_) => {
                cancel.abort();
                let _ = cancel.await;
                return Err(Error::Storage(
                    "cancellation final refresh timed out".into(),
                ));
            }
        };
        assert_eq!(cancelled.task, task);
        assert_eq!(cancelled.phase, LocalSessionPhase::Cancelled);
        Ok(())
    }

    #[tokio::test]
    async fn restart_preserves_cancel_and_completion_over_late_failure() -> Result<()> {
        let cancel_root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let first = PersistentLocalSwarm::open_with_model(
            cancel_root.path(),
            model.clone(),
            Arc::new(MockModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            Limits::default(),
        )
        .await?;
        let cancel_task = first.root_task().await?;
        let registry = first
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        append_record(&registry, StoredEvent::ForkCancelled { child: cancel_task }).await?;
        append_record(
            &registry,
            StoredEvent::ForkFailed {
                child: cancel_task,
                reason: "late failure".into(),
            },
        )
        .await?;
        drop(first);
        let reopened = PersistentLocalSwarm::open_with_model(
            cancel_root.path(),
            model.clone(),
            Arc::new(MockModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            Limits::default(),
        )
        .await?;
        assert_eq!(
            reopened.session(cancel_task).await?.phase,
            LocalSessionPhase::Cancelled
        );

        let completion_root =
            tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let first = PersistentLocalSwarm::open_with_model(
            completion_root.path(),
            model.clone(),
            Arc::new(MockModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            Limits::default(),
        )
        .await?;
        let completion_task = first.root_task().await?;
        let operation = OperationId::from_bytes([91; 16]);
        let output = TurnOutput {
            text: "completed".into(),
            attachments: Vec::new(),
            metadata: Value::Null,
            steps: 1,
        };
        let output_bytes = crate::contract::canonical_json_bytes(&output)?;
        let output_digest = crate::contract::canonical_json_digest(&output_bytes)?;
        let registry = first
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        append_record(
            &registry,
            StoredEvent::ForkFailed {
                child: completion_task,
                reason: "recoverable failure".into(),
            },
        )
        .await?;
        append_record(
            &registry,
            StoredEvent::ForkCompleted {
                child: completion_task,
                operation,
                output: Some(output),
                output_ref: None,
                output_digest: Some(output_digest),
            },
        )
        .await?;
        append_record(
            &registry,
            StoredEvent::ForkFailed {
                child: completion_task,
                reason: "late completion failure".into(),
            },
        )
        .await?;
        drop(first);
        let reopened = PersistentLocalSwarm::open_with_model(
            completion_root.path(),
            model,
            Arc::new(MockModel {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }),
            Limits::default(),
        )
        .await?;
        let session = reopened.session(completion_task).await?;
        assert_eq!(session.phase, LocalSessionPhase::Completed);
        assert_eq!(session.operation, Some(operation));
        assert_eq!(reopened.outcome(completion_task).await?.text, "completed");
        Ok(())
    }

    impl ModelProvider for MockModel {
        fn generate<'a>(
            &'a self,
            prepared: crate::model_input::PreparedModelInput,
        ) -> BoxStream<'a, Result<ModelEvent>> {
            let request = prepared.request().clone();
            self.requests.lock().expect("request lock").push(request);
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call > 0 {
                return Box::pin(futures::stream::iter([Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                })]));
            }
            Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content {
                    delta: "completed child exchange".into(),
                }),
                Ok(ModelEvent::ToolCall {
                    call_id: format!("stage-{}", self.calls.load(Ordering::SeqCst)),
                    name: "acyclic.stage_file".into(),
                    arguments: json!({
                        "path": "swarm-output.txt",
                        "text": "durable swarm output",
                        "media_type": "text/plain",
                        "display_name": "swarm-output.txt"
                    }),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    #[tokio::test]
    async fn seedless_local_fork_is_rejected_after_completed_boundary() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(MockModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let operation = OperationId::new();
        let limits = Limits::default();
        let swarm = PersistentLocalSwarm::open_with_model(
            root.path(),
            model.clone(),
            provider.clone(),
            limits,
        )
        .await?;
        let root_task = swarm.root_task().await?;
        swarm.run_root(operation, "root request").await?;
        let error = swarm
            .fork(LocalForkRequest {
                parent: root_task,
                parent_operation: operation,
                parent_step: 0,
                fork_operation: None,
                child_operation: OperationId::new(),
                child_authority: None,
                child_agent: None,
                task: "must use typed publication".into(),
                prompt: "child request".into(),
            })
            .await
            .expect_err("seedless fork must not dispatch a child");
        assert!(error.to_string().contains("typed fork publication"));
        assert_eq!(swarm.sessions().await?.len(), 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn fork_refuses_without_a_completed_model_boundary() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(MockModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let swarm =
            PersistentLocalSwarm::open_with_model(root.path(), model, provider, Limits::default())
                .await?;
        let error = swarm
            .fork(LocalForkRequest {
                parent: swarm.root_task().await?,
                parent_operation: OperationId::new(),
                parent_step: 0,
                fork_operation: None,
                child_operation: OperationId::new(),
                child_authority: None,
                child_agent: None,
                task: "must be refused".into(),
                prompt: "no boundary".into(),
            })
            .await
            .expect_err("incomplete parent must not activate a child");
        assert!(error.to_string().contains("completed model boundary"));
        assert_eq!(swarm.sessions().await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn recursive_constructor_reopens_the_pinned_project_and_session_key() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(MockModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let first = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
            root.path(),
            model.clone(),
            provider.clone(),
            Limits::default(),
        )
        .await?;
        let task = first.root_task().await?;
        let first_session = first.session(task).await?;
        let first_key = first.open_session(task).await?.signing_key();
        let first_project = first.config.project.clone();
        assert!(first.bindings.filesystem_fork_resolver.is_some());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        drop(first);

        let reopened = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
            root.path(),
            model,
            provider.clone(),
            Limits::default(),
        )
        .await?;
        assert_eq!(reopened.session(task).await?, first_session);
        assert_eq!(reopened.config.project, first_project);
        assert_eq!(reopened.open_session(task).await?.signing_key(), first_key);
        assert!(reopened.bindings.filesystem_fork_resolver.is_some());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn default_cancellation_stops_root_provider_and_reopens_without_dispatch() -> Result<()> {
        struct StreamGuard(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for StreamGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        struct PendingProvider {
            calls: AtomicUsize,
            started: tokio::sync::Notify,
            dropped: Arc<std::sync::atomic::AtomicBool>,
        }
        impl ModelProvider for PendingProvider {
            fn generate<'a>(&'a self, _input: crate::model_input::PreparedModelInput)
                -> BoxStream<'a, Result<ModelEvent>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let guard = StreamGuard(self.dropped.clone());
                self.started.notify_one();
                Box::pin(futures::stream::once(async move {
                    let _guard = guard;
                    std::future::pending::<Result<ModelEvent>>().await
                }))
            }
            fn reconcile<'a>(&'a self, _attempt: ModelAttempt)
                -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
                panic!("a cancelled root must not reconcile or redispatch its provider");
            }
        }
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(PendingProvider {
            calls: AtomicUsize::new(0), started: tokio::sync::Notify::new(),
            dropped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        let model = Model::new("mock", "default-cancellation", "1", json!({}))?;
        let first = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
            root.path(), model.clone(), provider.clone(), Limits::default(),
        ).await?;
        let task = first.root_task().await?;
        let operation = OperationId::from_bytes([0xC4; 16]);
        let mut running = AbortOnDrop::new(tokio::spawn({
            let first = first.clone();
            async move { first.run_root(operation, "remain pending until cancelled").await }
        }));
        if tokio::time::timeout(std::time::Duration::from_secs(120), provider.started.notified())
            .await.is_err() {
            running.handle.abort();
            let _ = (&mut running).await;
            panic!("root provider did not start; owned run was stopped");
        }
        let second = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
            root.path(), model.clone(), provider.clone(), Limits::default(),
        ).await?;
        second.cancel(task).await?;
        let stopped = match tokio::time::timeout(std::time::Duration::from_secs(5), &mut running).await {
            Ok(result) => result.expect("owned root run panicked"),
            Err(error) => {
                running.handle.abort();
                let _ = (&mut running).await;
                panic!("cancelled root did not stop: {error}");
            }
        };
        assert!(matches!(stopped, Err(Error::Conflict(_))));
        assert!(provider.dropped.load(Ordering::SeqCst));
        assert_eq!(first.session(task).await?.phase, LocalSessionPhase::Cancelled);
        drop(running);
        drop(first);
        drop(second);
        let reopened = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
            root.path(), model, provider.clone(), Limits::default(),
        ).await?;
        assert!(matches!(reopened.run_root(operation, "remain pending until cancelled").await,
            Err(Error::Conflict(_))));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    async fn await_timer_admissions(swarm: &PersistentLocalSwarm, expected: u64) -> Result<()> {
        let task = swarm.root_task().await?;
        let timer = swarm.conversation_stream.stream(format!("harness/v2/swarm-timers/{task}"))
            .map_err(|error| Error::Storage(error.to_string()))?;
        tokio::time::timeout(std::time::Duration::from_secs(120), async {
            loop {
                match timer.tail().await {
                    Ok(tail) if tail >= expected => return Ok(()),
                    Ok(_) | Err(StreamError::NotFound) => tokio::task::yield_now().await,
                    Err(error) => return Err(Error::Storage(error.to_string())),
                }
            }
        }).await.map_err(|error| Error::Storage(format!("wait observation did not start: {error}")))?
    }

    #[tokio::test]
    async fn default_cancellation_retains_live_and_interrupted_waits_after_cold_reopen() -> Result<()> {
        use crate::communication::{WaitCompletion, WaitRequest, WaitTarget};
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(MockModel { calls: AtomicUsize::new(0), requests: Mutex::new(Vec::new()) });
        let model = Model::new("mock", "default-cancelled-wait", "1", json!({}))?;
        let first = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
            root.path(), model.clone(), provider.clone(), Limits::default(),
        ).await?;
        let task = first.root_task().await?;
        let now = first.bindings.communication_host.as_ref().expect("default communication").now_unix_millis();
        let request = WaitRequest {
            operation_id: OperationId::from_bytes([0xC5; 16]), waiter: task,
            target: WaitTarget::Deadline { deadline_epoch_ms: now + 3_600_000 },
            timeout_epoch_ms: None, cancellation_id: Some(OperationId::from_bytes([0xC6; 16])),
        };
        let mut live_wait = AbortOnDrop::new(tokio::spawn({
            let first = first.clone(); let request = request.clone();
            async move { first.wait(request).await }
        }));
        await_timer_admissions(&first, 1).await?;
        let interrupted = WaitRequest {
            operation_id: OperationId::from_bytes([0xC7; 16]),
            cancellation_id: Some(OperationId::from_bytes([0xC8; 16])), ..request.clone()
        };
        let mut interrupted_wait = AbortOnDrop::new(tokio::spawn({
            let first = first.clone(); let request = interrupted.clone();
            async move { first.wait(request).await }
        }));
        await_timer_admissions(&first, 2).await?;
        interrupted_wait.handle.abort();
        assert!((&mut interrupted_wait).await.is_err());
        drop(interrupted_wait);
        first.cancel(task).await?;
        let result = match tokio::time::timeout(std::time::Duration::from_secs(5), &mut live_wait).await {
            Ok(result) => result.expect("live wait task panicked")?,
            Err(error) => {
                live_wait.handle.abort(); let _ = (&mut live_wait).await;
                panic!("live wait did not stop after cancellation: {error}");
            }
        };
        assert_eq!(result, WaitCompletion::Cancelled);
        drop(live_wait);
        drop(first);
        let reopened = PersistentLocalSwarm::open_shared_with_model_and_recursive_filesystem(
            root.path(), model, provider.clone(), Limits::default(),
        ).await?;
        assert_eq!(reopened.wait(request).await?, WaitCompletion::Cancelled);
        assert_eq!(reopened.wait(interrupted.clone()).await?, WaitCompletion::Cancelled);
        assert_eq!(reopened.wait(interrupted).await?, WaitCompletion::Cancelled);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn recorded_operator_choice_requires_the_exact_decision() -> Result<()> {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(MockModel {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let model = Model::new("mock", "local-swarm", "1", json!({}))?;
        let swarm =
            PersistentLocalSwarm::open_with_model(root.path(), model, provider, Limits::default())
                .await?;
        let task = swarm.root_task().await?;
        let interaction = InteractionId::new();
        let operation = OperationId::new();
        let action_digest = [0x72; 32];
        swarm
            .open_session(task)
            .await?
            .storage()
            .open_interaction(
                interaction,
                Interaction::approval("approve exact action", operation, action_digest)?,
            )
            .await?;
        swarm
            .record_operator_approval(task, interaction, false)
            .await?;
        assert!(matches!(
            swarm
                .resolve_recorded_operator_approval(task, interaction, true)
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            swarm
                .resolve_recorded_operator_approval(task, interaction, false)
                .await?,
            InteractionOutcome::Declined
        ));
        Ok(())
    }
}
