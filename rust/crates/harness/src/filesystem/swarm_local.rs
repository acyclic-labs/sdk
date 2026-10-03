//! Durable-local recursive swarm composition.
//!
//! This module owns only the local application composition. Turn execution,
//! model-input admission, completed-batch publication, and provider recovery
//! remain the shared Harness paths. A swarm record is a small durable index of
//! task identities and activation outcomes; each task's journal and private
//! files are still owned by [`PersistentLocalHarness`].

use super::{FilesystemHost, LocalHarnessTools, PersistentLocalHarness};
use crate::{
    batch_publication::ModelBatchPublication,
    AgentId, Error, OperationId, Result, TaskId,
    conversation::Limits,
    core::{AggregateKind, Authority, AuthorityIssuer, Capabilities, SchemaRegistry},
    executor::TurnOutput,
    fork::{ForkReport, ForkSeed},
    model::{Model, ModelContent, ModelMessage, ModelProvider, ModelRole},
    model_input::{CompletedModelBoundary, InheritedModelContext},
    runtime::TaskRunLimits,
    store::StreamAggregate,
};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_stream::{AppendOutcome, LocalStream, LocalStreamLimits, StreamClient, StreamError};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

const REGISTRY_STREAM: &str = "swarm/records";
const REGISTRY_VERSION: u32 = 1;

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
    pub model_batch_publisher:
        Option<Arc<dyn crate::batch_publication::ModelBatchPublisher>>,
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

    fn tools(&self) -> Result<LocalHarnessTools> {
        let Some(host) = self.communication_host.clone() else {
            let tools = LocalHarnessTools::new();
            return Ok(match &self.model_batch_publisher {
                Some(publisher) => tools.with_batch_publisher(publisher.clone()),
                None => tools,
            });
        };
        let tools = LocalHarnessTools::from_registry(
            crate::communication_tools::communication_tools_with_wait_store_and_cancellation(
                host,
                self.wait_store.clone(),
                self.cancellation.clone(),
            )?,
        );
        Ok(match &self.model_batch_publisher {
            Some(publisher) => tools.with_batch_publisher(publisher.clone()),
            None => tools,
        })
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
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates application bounds before opening any provider.
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.run_limits.validate()?;
        if self.maximum_children == 0 || self.maximum_depth == 0 {
            return Err(Error::Invalid("local swarm bounds must be positive".into()));
        }
        Ok(())
    }
}

/// A requested child activation. Its model is intentionally absent: the
/// parent operation and completed boundary determine what can be inherited.
#[derive(Clone, Debug, Eq, PartialEq)]
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
            || self.fork_operation.is_some_and(|value| value.into_bytes() == [0; 16])
        {
            return Err(Error::Invalid("local fork identities must be nonzero".into()));
        }
        if self.fork_operation == Some(self.child_operation) {
            return Err(Error::Invalid(
                "fork publication and child turn require distinct operation identities".into(),
            ));
        }
        if self.task.trim().is_empty() || self.task.len() > 4 * 1024 {
            return Err(Error::Invalid("local fork task is empty or too large".into()));
        }
        if let Some(child) = &self.child_authority {
            if child.kind != AggregateKind::Conversation {
                return Err(Error::Invalid("local fork child authority is not a conversation".into()));
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalSessionPhase {
    /// The task is the root or was admitted as a child.
    Ready,
    /// A fork publication was admitted but the child has not completed.
    Activating,
    /// The child completed its requested turn.
    Completed,
    /// The last activation or turn failed with a stable message.
    Failed(String),
}

/// Lazy session descriptor returned by listing and lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    Failed(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecord {
    version: u32,
    event: StoredEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum StoredEvent {
    Session(StoredSession),
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
    },
    ForkCompleted {
        child: TaskId,
        operation: OperationId,
        #[serde(default)]
        output: Option<TurnOutput>,
    },
    ForkFailed { child: TaskId, reason: String },
}

impl From<StoredPhase> for LocalSessionPhase {
    fn from(value: StoredPhase) -> Self {
        match value {
            StoredPhase::Ready => Self::Ready,
            StoredPhase::Activating => Self::Activating,
            StoredPhase::Completed => Self::Completed,
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
    config: LocalSwarmConfig,
    provider: Arc<dyn ModelProvider>,
    bindings: LocalSwarmBindings,
    registry: StreamClient<LocalStream>,
    records: Mutex<BTreeMap<TaskId, LocalSwarmSession>>,
    requests: Mutex<BTreeMap<TaskId, LocalForkRequest>>,
    seeds: Mutex<BTreeMap<TaskId, ForkSeed>>,
    reports: Mutex<BTreeMap<TaskId, ForkReport>>,
    publications: Mutex<BTreeMap<TaskId, ModelBatchPublication>>,
    declarations: Mutex<BTreeMap<TaskId, LocalInheritedModelDeclaration>>,
    outcomes: Mutex<BTreeMap<TaskId, TurnOutput>>,
    sessions: Mutex<BTreeMap<TaskId, Arc<PersistentLocalHarness>>>,
}

impl PersistentLocalSwarm {
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
        config: LocalSwarmConfig,
        provider: Arc<dyn ModelProvider>,
        bindings: LocalSwarmBindings,
    ) -> Result<Self> {
        config.validate()?;
        let root = root.as_ref().to_path_buf();
        let registry = StreamClient::new(Arc::new(
            LocalStream::open(root.join("swarm"), LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let stream = registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let records = load_records(&stream).await?;
        let mut sessions = BTreeMap::new();
        let mut requests = BTreeMap::new();
        let mut seeds = BTreeMap::new();
        let mut reports = BTreeMap::new();
        let mut publications = BTreeMap::new();
        let mut declarations = BTreeMap::new();
        let mut outcomes = BTreeMap::new();
        for record in records {
            apply_record(
                &mut sessions,
                &mut requests,
                &mut seeds,
                &mut reports,
                &mut publications,
                &mut declarations,
                &mut outcomes,
                record,
            )?;
        }
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
            append_record(
                &stream,
                StoredEvent::Session(root_session.clone().into()),
            )
            .await?;
            sessions.insert(root_task, root_session);
        }
        let root_task = sessions
            .values()
            .find(|session| session.parent.is_none())
            .map(|session| session.task)
            .ok_or_else(|| Error::Storage("swarm registry has no root session".into()))?;
        let root_session = open_session_path(&root, root_task);
        let root_harness = Arc::new(
            PersistentLocalHarness::open_with_tools(
                root_session,
                config.model.clone(),
                provider.clone(),
                config.limits,
                bindings.tools()?,
            )
            .await?,
        );
        let mut opened = BTreeMap::new();
        opened.insert(root_task, root_harness);
        Ok(Self {
            root,
            config,
            provider,
            bindings,
            registry,
            records: Mutex::new(sessions),
            requests: Mutex::new(requests),
            seeds: Mutex::new(seeds),
            reports: Mutex::new(reports),
            publications: Mutex::new(publications),
            declarations: Mutex::new(declarations),
            outcomes: Mutex::new(outcomes),
            sessions: Mutex::new(opened),
        })
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

    /// Lists canonical session descriptors without starting workers or
    /// reading child filesystem content.
    pub async fn sessions(&self) -> Vec<LocalSwarmSession> {
        self.records.lock().await.values().cloned().collect()
    }

    /// Reads one descriptor without opening its local journal or filesystem.
    pub async fn session(&self, task: TaskId) -> Result<LocalSwarmSession> {
        self.records
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm task {task}")))
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
        self.outcomes
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm outcome {task}")))
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
        let harness = self.open_session(task).await?;
        let output = harness.run(operation, prompt).await?;
        self.update_session(task, |session| {
            session.operation = Some(operation);
            session.phase = LocalSessionPhase::Completed;
        })
        .await?;
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
        if parent.depth >= self.config.maximum_depth {
            return Err(Error::Unauthorized("local swarm depth limit exceeded".into()));
        }
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
        let child = TaskId::from_bytes(request.child_operation.into_bytes());
        if let Some(existing) = self.requests.lock().await.get(&child)
            && existing != &request
        {
            return Err(Error::Conflict(
                "child operation is already bound to another fork request".into(),
            ));
        }
        let parent_harness = self.open_session(request.parent).await?;
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
        let declared_suffix = declaration.as_ref().map(|value| value.suffix.clone());
        let (boundary, verified_parent) = match (publication, declaration) {
            (Some(publication), Some(declaration)) => {
                let inherited = declaration.context(self.config.limits)?;
                let verified = parent_harness
                    .storage()
                    .verified_inherited_model_fork_boundary(
                        &publication,
                        self.config.limits,
                        &inherited,
                    )
                    .await?;
                let (boundary, parent) = verified.into_parts();
                (boundary, Some(parent))
            }
            _ => unreachable!("typed publication and declaration were checked together"),
        };
        let storage_parent = verified_parent.as_ref().unwrap_or(parent);
        // The proof above is the admission boundary. Only after it succeeds
        // do we append the authoritative ForkAdmitted record; a prepared
        // record alone cannot make an unverified child runnable.
        if let (Some(publication), Some(declaration)) =
            (stored_publication, stored_declaration)
        {
            let registry = self
                .registry
                .stream(REGISTRY_STREAM)
                .map_err(|error| Error::Storage(error.to_string()))?;
            append_record(
                &registry,
                StoredEvent::ForkAdmitted {
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
                    report: stored_report,
                    publication: Some(publication),
                    declaration: Some(declaration),
                },
            )
            .await?;
        }
        let existing = self.records.lock().await.get(&child).cloned();
        let new_admission = match existing {
            None => true,
            Some(session) => {
                if self.requests.lock().await.get(&child) != Some(&request) {
                    return Err(Error::Conflict("child operation is already a different session".into()));
                }
                if session.phase == LocalSessionPhase::Completed {
                    return Err(Error::Conflict("child operation is already complete".into()));
                }
                if self.seeds.lock().await.get(&child) != Some(seed) {
                    return Err(Error::Conflict("existing child seed differs from published fork".into()));
                }
                false
            }
        };
        let registry = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if new_admission {
            append_record(
                &registry,
                StoredEvent::ForkAdmitted {
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
                    report: None,
                    publication: None,
                    declaration: None,
                },
            )
            .await?;
            self.records.lock().await.insert(
                child,
                LocalSwarmSession {
                    task: child,
                    parent: Some(request.parent),
                    depth: parent_session.depth + 1,
                    task_description: request.task.clone(),
                    operation: Some(request.child_operation),
                    phase: LocalSessionPhase::Activating,
                },
            );
            self.requests.lock().await.insert(child, request.clone());
        } else {
            self.update_session(child, |session| {
                session.phase = LocalSessionPhase::Activating;
            })
            .await?;
        }
        self.seeds.lock().await.insert(child, seed.clone());
        let harness = match PersistentLocalHarness::from_published_fork_with_tools(
            self.config.model.clone(),
            self.provider.clone(),
            self.config.limits,
            host,
            stream,
            issuer,
            storage_parent,
            seed,
            self.bindings.tools()?,
        )
        .await
        {
            Ok(harness) => Arc::new(harness),
            Err(error) => {
                self.mark_failed(child, error.to_string()).await?;
                return Err(error);
            }
        };
        self.sessions.lock().await.insert(child, harness.clone());
        self.activate_child_with_harness(
            request,
            child,
            registry,
            boundary,
            harness,
            declared_suffix,
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
        report.validate()?;
        let parent_storage = self.open_session(request.parent).await?;
        if report.request.parent != *parent_storage.storage().conversation()
            || report.request.operation_id != fork_operation
        {
            return Err(Error::Conflict(
                "fork report is bound to another publication operation or parent".into(),
            ));
        }
        let child_authority = request.child_authority.as_ref().ok_or_else(|| {
            Error::Invalid("typed fork publication requires child authority".into())
        })?;
        let child_agent = request.child_agent.ok_or_else(|| {
            Error::Invalid("typed fork publication requires child agent".into())
        })?;
        if child_authority != &report.request.child || child_agent != report.request.child_agent {
            return Err(Error::Conflict(
                "fork report child binding differs from fork request".into(),
            ));
        }
        let reported_seed = report.clone().into_seed()?;
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
            || publication.operation_id != fork_operation
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
        let child = TaskId::from_bytes(request.child_operation.into_bytes());
        let parent = self.session(request.parent).await?;
        if parent.depth >= self.config.maximum_depth {
            return Err(Error::Unauthorized("local swarm depth limit exceeded".into()));
        }
        let child_count = self
            .records
            .lock()
            .await
            .values()
            .filter(|session| session.parent == Some(request.parent) && session.task != child)
            .count();
        if child_count >= self.config.maximum_children {
            return Err(Error::Unauthorized("local swarm child limit exceeded".into()));
        }
        if let Some(existing) = self.records.lock().await.get(&child).cloned() {
            if self.requests.lock().await.get(&child) != Some(request)
                || self.seeds.lock().await.get(&child) != Some(seed)
                || self.reports.lock().await.get(&child) != Some(report)
            {
                return Err(Error::Conflict(
                    "existing typed child admission differs from retry".into(),
                ));
            }
            if existing.phase == LocalSessionPhase::Completed {
                return Err(Error::Conflict("child operation is already complete".into()));
            }
            if self.publications.lock().await.get(&child) != Some(&publication)
                || self.declarations.lock().await.get(&child) != Some(&declaration)
            {
                return Err(Error::Conflict(
                    "existing typed declaration differs from retry".into(),
                ));
            }
            return Ok(());
        }
        let registry = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        append_record(
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
            },
        )
        .await?;
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
        request.validate()?;
        report.validate()?;
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
                .is_some_and(|agent| *agent != report.request.child_agent)
        {
            return Err(Error::Conflict(
                "fork report child authority or agent differs from request".into(),
            ));
        }
        let preview = report.clone().into_seed()?;
        self.preadmit_published_child(&request, &report, &preview, publication, declaration)
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
        let seed = match child
            .spawn_from_report(parent, report, parent_scope, child_scope)
            .await
        {
            Ok(seed) => seed,
            Err(error) => {
                self.mark_failed(
                    TaskId::from_bytes(request.child_operation.into_bytes()),
                    error.to_string(),
                )
                .await?;
                return Err(error);
            }
        };
        self.activate_published_child(request, host, stream, issuer, parent, &seed)
            .await
    }

    async fn activate_child_with_harness(
        &self,
        request: LocalForkRequest,
        child: TaskId,
        stream: acyclic_stream::Stream<LocalStream>,
        boundary: crate::model_input::CompletedModelBoundary,
        harness: Arc<PersistentLocalHarness>,
        declared_suffix: Option<Vec<ModelMessage>>,
    ) -> Result<LocalForkOutcome> {
        let suffix = declared_suffix.unwrap_or_else(|| {
            vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(format!(
                    "child task: {}; parent: {}; identity: {}; fresh scratch: true",
                    request.task, request.parent, child
                )),
            }]
        });
        let bundle = match harness
            .storage()
            .inherited_builder(
                boundary,
                suffix,
                self.provider.clone(),
                self.config.limits,
            )
            .and_then(|builder| {
                let builder = builder
                    .tools(harness.storage().default_tools(self.config.limits)?)
                    .grant("tool:call:acyclic.read_file")
                    .grant("tool:call:acyclic.stage_file")
                    .grant("tool:call:acyclic.list_files")
                    .limits(self.config.limits);
                self.bindings.tools()?.install_into(builder)?.build()
            }) {
            Ok(bundle) => bundle,
            Err(error) => {
                self.mark_failed(child, error.to_string()).await?;
                return Err(error);
            }
        };
        let output = match self
            .run_child_turn(&harness, &bundle, &request)
            .await
        {
            Ok(output) => output,
            Err(error) => {
                self.mark_failed(child, error.to_string()).await?;
                return Err(error);
            }
        };
        append_record(
            &stream,
            StoredEvent::ForkCompleted {
                child,
                operation: request.child_operation,
                output: Some(output.clone()),
            },
        )
        .await?;
        self.outcomes.lock().await.insert(child, output.clone());
        self.update_session(child, |session| {
            session.phase = LocalSessionPhase::Completed;
        })
        .await?;
        Ok(LocalForkOutcome {
            child,
            operation: request.child_operation,
            output,
        })
    }

    async fn run_child_turn(
        &self,
        harness: &PersistentLocalHarness,
        bundle: &crate::Harness,
        request: &LocalForkRequest,
    ) -> Result<TurnOutput> {
        let content = harness
            .storage()
            .stage(
                request.child_operation,
                &format!("turns/{}/user.txt", request.child_operation),
                request.prompt.as_bytes(),
                "text/plain",
                "prompt.txt",
            )
            .await?;
        let max_steps = u32::try_from(
            self.config
                .run_limits
                .max_steps
                .unwrap_or(self.config.limits.model_steps),
        )
        .map_err(|_| Error::Invalid("child step limit exceeds u32".into()))?;
        harness
            .storage()
            .run_conversation(bundle, request.child_operation, content, vec![], max_steps)
            .await
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
        parent: &StreamAggregate<LocalStream>,
    ) -> Result<LocalForkOutcome> {
        let request = self
            .requests
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm fork request {task}")))?;
        if let Some(output) = self.outcomes.lock().await.get(&task).cloned() {
            return Ok(LocalForkOutcome {
                child: task,
                operation: request.child_operation,
                output,
            });
        }
        let seed = self.published_seed(task).await?;
        if let Ok(report) = self.prepared_report(task).await {
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
        if let Some(existing) = self.sessions.lock().await.get(&task).cloned() {
            return Ok(existing);
        }
        let harness = Arc::new(
            PersistentLocalHarness::open_with_tools(
                open_session_path(&self.root, task),
                self.config.model.clone(),
                self.provider.clone(),
                self.config.limits,
                self.bindings.tools()?,
            )
            .await?,
        );
        self.sessions.lock().await.insert(task, harness.clone());
        Ok(harness)
    }

    async fn update_session<F>(&self, task: TaskId, update: F) -> Result<()>
    where
        F: FnOnce(&mut LocalSwarmSession),
    {
        let stream = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let mut records = self.records.lock().await;
        let current = records
            .get_mut(&task)
            .ok_or_else(|| Error::NotFound(format!("local swarm task {task}")))?;
        let mut next = current.clone();
        update(&mut next);
        append_record(&stream, StoredEvent::Session(next.clone().into())).await?;
        *current = next;
        Ok(())
    }

    async fn mark_failed(&self, task: TaskId, reason: String) -> Result<()> {
        let bounded = reason.chars().take(512).collect::<String>();
        let stream = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        append_record(
            &stream,
            StoredEvent::ForkFailed {
                child: task,
                reason: bounded.clone(),
            },
        )
        .await?;
        let mut records = self.records.lock().await;
        if let Some(session) = records.get_mut(&task) {
            session.phase = LocalSessionPhase::Failed(bounded);
        }
        Ok(())
    }
}

fn open_session_path(root: &Path, task: TaskId) -> PathBuf {
    root.join("tasks").join(task.to_string())
}

fn fork_seed_digest(seed: &ForkSeed) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(seed)
}

async fn load_records(
    stream: &acyclic_stream::Stream<LocalStream>,
) -> Result<Vec<StoredRecord>> {
    let tail = match stream.tail().await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => 0,
        Err(error) => return Err(Error::Storage(error.to_string())),
    };
    if tail == 0 {
        return Ok(Vec::new());
    }
    let mut records = stream
        .read(0, u32::try_from(tail).map_err(|_| Error::Storage("swarm registry is too large".into()))?)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mut decoded = Vec::new();
    while let Some(record) = records.next().await {
        let record = record.map_err(|error| Error::Storage(error.to_string()))?;
        let value: StoredRecord = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if value.version != REGISTRY_VERSION {
            return Err(Error::Conflict("unsupported local swarm registry version".into()));
        }
        decoded.push(value);
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
    let bytes = crate::contract::canonical_json_bytes(&StoredRecord {
        version: REGISTRY_VERSION,
        event,
    })?;
    match stream
        .append_at(bytes, tail)
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
    record: StoredRecord,
) -> Result<()> {
    match record.event {
        StoredEvent::Session(session) => {
            if session.version != REGISTRY_VERSION {
                return Err(Error::Conflict("unsupported local session version".into()));
            }
            sessions.insert(session.task, session.into());
        }
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
            ..
        } => {
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
            sessions.insert(
                child,
                LocalSwarmSession {
                    task: child,
                    parent: Some(parent),
                    depth: parent_session.depth + 1,
                    task_description: task,
                    operation: Some(child_operation),
                    phase: LocalSessionPhase::Activating,
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
                report.validate()?;
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
        } => {
            let session = sessions
                .get_mut(&child)
                .ok_or_else(|| Error::Storage("fork completion child is missing".into()))?;
            session.operation = Some(operation);
            session.phase = LocalSessionPhase::Completed;
            if let Some(output) = output {
                outcomes.insert(child, output);
            }
        }
        StoredEvent::ForkFailed { child, reason } => {
            if let Some(session) = sessions.get_mut(&child) {
                session.phase = LocalSessionPhase::Failed(reason);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::{ModelAttempt, ModelEvent, ModelRequest},
    };
    use futures::{future::BoxFuture, stream::BoxStream};
    use serde_json::{Value, json};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct MockModel {
        calls: AtomicUsize,
        requests: Mutex<Vec<ModelRequest>>,
    }

    impl ModelProvider for MockModel {
        fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
            self.requests.lock().expect("request lock").push(request);
            self.calls.fetch_add(1, Ordering::SeqCst);
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
        assert_eq!(swarm.sessions().await.len(), 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
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
        let swarm = PersistentLocalSwarm::open_with_model(
            root.path(),
            model,
            provider,
            Limits::default(),
        )
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
        assert_eq!(swarm.sessions().await.len(), 1);
        Ok(())
    }
}
