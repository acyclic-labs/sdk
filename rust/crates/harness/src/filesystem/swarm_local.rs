//! Durable-local recursive swarm composition.
//!
//! This module owns only the local application composition. Turn execution,
//! model-input admission, completed-batch publication, and provider recovery
//! remain the shared Harness paths. A swarm record is a small durable index of
//! task identities and activation outcomes; each task's journal and private
//! files are still owned by [`PersistentLocalHarness`].

use super::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemHost,
    InteractionApprovalAuthorization, InteractionOperatorAuthorizer,
    LocalHarnessTools, PersistentLocalHarness, workspace_ref,
};
use crate::{
    AgentId, Capabilities, Error, InteractionId, OperationId, Result, TaskId,
    batch_publication::ModelBatchPublication,
    communication::{DurableCommunication, MessageRequest, MessageTarget},
    conversation::{ConversationMessage, FileRef, Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, EffectGuarantee, SchemaRegistry, Scope},
    executor::TurnOutput,
    fork::{
        Capture, ForkPreparation, ForkReport, ForkRequest, ForkSeed, ForkSelection,
        ResourceRevision,
    },
    interaction::{InteractionKind, InteractionOutcome, InteractionResolution, InteractionResponse, InteractionTicket},
    model::{Model, ModelContent, ModelMessage, ModelProvider, ModelRole},
    model_input::{CompletedModelBoundary, InheritedModelContext},
    registry::ComponentIdentity,
    resources::{GenerationRef, ProviderRef, StreamRef},
    runtime::TaskRunLimits,
    store::StreamAggregate,
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
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};
use tokio::sync::Mutex;

const REGISTRY_STREAM: &str = "swarm/records";
// The issuer-binding event gained a child-operation key and is no longer
// safely decodable as the original single-fork record. Keep recovery
// deliberately fenced at the registry boundary until an explicit migration
// can validate every legacy record.
const REGISTRY_VERSION: u32 = 2;
/// Completion payloads stay small enough for a Stream record. Larger outputs
/// are staged in the child agent-private volume and the registry retains only
/// their immutable reference and digest.
const MAX_INLINE_COMPLETION_BYTES: usize = 64 * 1024;
const MAX_SWARM_RECORD_BYTES: usize = 1024 * 1024;
const MAX_SWARM_ACTIVITY_EVENTS: usize = 65_536;

type LocalFilesystemHost = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

/// LocalStream journals are process-exclusive. Keep one authenticated provider
/// handle per composition root so independently opened swarm handles observe
/// the same journal and CAS boundary.
static LOCAL_STREAM_CACHE: OnceLock<Mutex<BTreeMap<PathBuf, Weak<LocalStream>>>> = OnceLock::new();
static LOCAL_FILESYSTEM_CACHE: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<LocalFilesystemHost>>>,
> = OnceLock::new();

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
        let tools = LocalHarnessTools::from_registry(registry);
        Ok(match &self.model_batch_publisher {
            Some(publisher) => tools.with_batch_publisher(publisher.clone()),
            None => tools,
        })
    }

    fn tools(&self) -> Result<LocalHarnessTools> {
        self.tools_for(TaskId::from_bytes([0; 16]))
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
            return Err(Error::Invalid("model-selected fork intent is invalid".into()));
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

fn rebind_report_history(report: &mut ForkReport, parent_revision: u64) -> Result<()> {
    report.request.parent_revision = parent_revision;
    for (selection, capture) in report
        .request
        .selections
        .iter_mut()
        .zip(report.captures.iter_mut())
    {
        if let ResourceRevision::History(reference) = &selection.revision {
            let rebound = StreamRef::new(
                reference.as_resource().provider().clone(),
                reference.as_resource().key().to_vec(),
                Some(parent_revision.to_string()),
            )?;
            selection.revision = ResourceRevision::History(rebound.clone());
            if let crate::fork::Capture::Captured(resource) = capture {
                resource.source = ResourceRevision::History(rebound.clone());
                resource.revision = ResourceRevision::History(rebound);
            }
        }
    }
    report.validate()
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
            return Err(Error::Invalid("local fork issuer secret cannot be zero".into()));
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
            if parent_session.depth >= swarm.config.maximum_depth {
                return Err(Error::Unauthorized("local swarm depth limit exceeded".into()));
            }
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
                return Err(Error::Unauthorized("local swarm child limit exceeded".into()));
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
            let (boundary, parent) = verified.into_parts();
            let parent_revision = parent.reducer().revision();
            if parent.reducer().authority() != storage.conversation()
                || parent_revision == 0
            {
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
            let existing_declaration = swarm
                .declarations
                .lock()
                .await
                .get(&child_task)
                .cloned();
            if let (Some(report), Some(declaration)) = (existing_report, existing_declaration) {
                report.validate()?;
                let seed = report.clone().into_seed()?;
                if seed.operation_id != intent.fork_operation
                    || seed.parent != *storage.conversation()
                    || seed.child != Self::child_authority(&intent)
                    || seed.child_agent
                        != AgentId::from_bytes(intent.child_operation.into_bytes())
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
                    declaration,
                    host: self.host.clone(),
                    stream: self.stream.clone(),
                    issuer: Self::child_issuer(&seed.child, intent.child_operation, issuer_secret),
                });
            }

            let child_authority = Self::child_authority(&intent);
            let child_agent = AgentId::from_bytes(intent.child_operation.into_bytes());
            let child_issuer = Self::child_issuer(
                &child_authority,
                intent.child_operation,
                issuer_secret,
            );
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
            let parent_project = swarm
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
                            if !matches!(&selection.revision, ResourceRevision::Project { .. }) {
                                return None;
                            }
                            match capture {
                                Capture::Captured(resource) => {
                                    match &resource.revision {
                                        ResourceRevision::Project { volume, .. } => Some(volume.clone()),
                                        _ => None,
                                    }
                                }
                                _ => None,
                            }
                        })
                });
            let (source_project, source_generation) = match parent_project {
                Some(source_project) => {
                    let project_ref = workspace_ref(
                        source_project.provider().clone(),
                        &source_project.storage_name()?,
                    )?;
                    let project_head = self.host.resolve(&project_ref).await?;
                    (source_project, project_head.generation)
                }
                None => {
                    let project_ref = workspace_ref(
                        self.project.provider().clone(),
                        &self.project.storage_name()?,
                    )?;
                    let project_head = self.host.resolve(&project_ref).await?;
                    (self.project.clone(), project_head.generation)
                }
            };
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
                .attach_model_fork_references_from_parts(
                    &boundary,
                    &publication,
                    &parent,
                    &mut request,
                )
                .await?;
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
        self.report.validate()?;
        let seed = self.report.clone().into_seed()?;
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
    issuer_bindings: Mutex<BTreeMap<(OperationId, OperationId), [u8; 32]>>,
    resolver: Option<Arc<dyn LocalModelForkResolver>>,
    journal: Mutex<Option<StreamClient<LocalStream>>>,
}

impl Default for LocalModelForkPlans {
    fn default() -> Self {
        Self {
            plans: Mutex::new(BTreeMap::new()),
            intents: Mutex::new(BTreeMap::new()),
            issuer_bindings: Mutex::new(BTreeMap::new()),
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

    /// Binds a concrete allocator to this opened swarm without exposing the
    /// swarm's mutable registry to the model-facing tool.
    pub fn bind_swarm(&self, swarm: Weak<PersistentLocalSwarm>) -> Result<()> {
        if let Some(resolver) = &self.resolver {
            resolver.bind_swarm(swarm)?;
        }
        Ok(())
    }

    fn apply_intent_record(
        event: StoredEvent,
        intents: &mut BTreeMap<(OperationId, OperationId), LocalForkIntent>,
        issuer_bindings: &mut BTreeMap<(OperationId, OperationId), [u8; 32]>,
    ) -> Result<()> {
        match event {
            StoredEvent::ForkIntent { intent } => {
                let key = (intent.fork_operation, intent.child_operation);
                if let Some(existing) = intents.get(&key)
                    && existing != &intent
                {
                    return Err(Error::Conflict(
                        "durable model fork intent changed during recovery".into(),
                    ));
                }
                intents.insert(key, intent);
            }
            StoredEvent::ForkIntentSelected {
                intent,
                issuer_digest,
            } => {
                let key = (intent.fork_operation, intent.child_operation);
                if let Some(existing) = intents.get(&key)
                    && existing != &intent
                {
                    return Err(Error::Conflict(
                        "durable model fork intent changed during recovery".into(),
                    ));
                }
                intents.insert(key, intent);
                if let Some(digest) = issuer_digest {
                    Self::apply_issuer_binding(key, digest, issuer_bindings)?;
                }
            }
            StoredEvent::ForkIssuerBinding {
                operation,
                child_operation,
                digest,
            } => {
                Self::apply_issuer_binding(
                    (operation, child_operation),
                    digest,
                    issuer_bindings,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_issuer_binding(
        key: (OperationId, OperationId),
        digest: [u8; 32],
        issuer_bindings: &mut BTreeMap<(OperationId, OperationId), [u8; 32]>,
    ) -> Result<()> {
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
        let mut issuer_bindings = self.issuer_bindings.lock().await;
        for record in records {
            Self::apply_intent_record(record.event, &mut intents, &mut issuer_bindings)?;
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
        self.refresh_intents_from_journal().await?;
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

    async fn refresh_intents_from_journal(&self) -> Result<()> {
        let Some(registry) = self.journal.lock().await.clone() else {
            return Ok(());
        };
        let stream = registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let records = load_records(&stream).await?;
        let mut intents = self.intents.lock().await;
        let mut issuer_bindings = self.issuer_bindings.lock().await;
        for record in records {
            Self::apply_intent_record(record.event, &mut intents, &mut issuer_bindings)?;
        }
        Ok(())
    }

    async fn record_intent(&self, intent: LocalForkIntent) -> Result<()> {
        intent.validate()?;
        let key = (intent.fork_operation, intent.child_operation);
        let mut intents = self.intents.lock().await;
        if intents.values().any(|existing| {
            existing.child_operation == intent.child_operation && existing != &intent
        }) {
            return Err(Error::Conflict(
                "child operation is already bound to another durable fork intent".into(),
            ));
        }
        if let Some(existing) = intents.get(&key)
            && existing != &intent
        {
            return Err(Error::Conflict(
                "model fork publication identity is already bound to another intent".into(),
            ));
        }
        let issuer_digest = self
            .resolver
            .as_ref()
            .and_then(|resolver| resolver.issuer_binding_digest());
        if intents.contains_key(&key) {
            if let Some(expected) = issuer_digest
                && self
                    .issuer_bindings
                    .lock()
                    .await
                    .get(&key)
                    != Some(&expected)
            {
                return Err(Error::Conflict(
                    "model fork issuer binding is missing or changed on retry".into(),
                ));
            }
            return Ok(());
        }
        if let Some(registry) = self.journal.lock().await.clone() {
            let stream = registry
                .stream(REGISTRY_STREAM)
                .map_err(|error| Error::Storage(error.to_string()))?;
            append_record(
                &stream,
                StoredEvent::ForkIntentSelected {
                    intent: intent.clone(),
                    issuer_digest,
                },
            )
            .await?;
        }
        intents.insert(key, intent);
        if let Some(digest) = issuer_digest {
            self.issuer_bindings
                .lock()
                .await
                .insert(key, digest);
        }
        Ok(())
    }

    async fn resolve_intents(&self, publication: ModelBatchPublication) -> Result<Vec<LocalModelForkPlan>> {
        self.refresh_intents_from_journal().await?;
        let intents = self
            .intents
            .lock()
            .await
            .values()
            .filter(|intent| intent.publication_operation == Some(publication.operation_id))
            .cloned()
            .collect::<Vec<_>>();
        if intents.is_empty() {
            let prepared = self.get_for_publication(publication.operation_id).await;
            if !prepared.is_empty() {
                return Ok(prepared);
            }
            return Err(Error::Conflict("completed fork publication has no durable intent".into()));
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
                && self
                    .issuer_bindings
                    .lock()
                    .await
                    .get(&key)
                    != Some(&expected)
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

    /// Reconstructs one already admitted child activation from the owner
    /// bindings retained by this plan index. The durable request and report
    /// are the authority here: resolver recovery may only reproduce the
    /// issuer/host services for that exact child, and must never allocate a
    /// replacement report or derive a new child boundary.
    async fn recover_plan(
        &self,
        request: &LocalForkRequest,
        report: &ForkReport,
        publication: &ModelBatchPublication,
        declaration: &LocalInheritedModelDeclaration,
    ) -> Result<LocalModelForkPlan> {
        self.refresh_intents_from_journal().await?;
        let fork_operation = request.fork_operation.ok_or_else(|| {
            Error::Conflict("durable activating child request has no fork operation".into())
        })?;
        let key = (fork_operation, request.child_operation);
        if let Some(plan) = self.plans.lock().await.get(&key).cloned() {
            plan.validate()?;
            if plan.publication_operation != publication.operation_id
                || plan.request != *request
                || plan.report != *report
                || plan.declaration != *declaration
            {
                return Err(Error::Conflict(
                    "live fork plan differs from the durable activation binding".into(),
                ));
            }
            return Ok(plan);
        }
        let intent = self
            .intents
            .lock()
            .await
            .values()
            .find(|intent| {
                intent.parent == request.parent
                    && intent.parent_operation == request.parent_operation
                    && intent.parent_step == request.parent_step
                    && intent.publication_operation == Some(publication.operation_id)
                    && intent.fork_operation == fork_operation
                    && intent.child_operation == request.child_operation
                    && intent.task == request.task
                    && intent.prompt == request.prompt
            })
            .cloned()
            .ok_or_else(|| {
                Error::Conflict(
                    "activating child has no durable model fork intent for its publication".into(),
                )
            })?;
        if let Some(resolver) = self.resolver.as_ref()
            && let Some(expected) = resolver.issuer_binding_digest()
            && self.issuer_bindings.lock().await.get(&key) != Some(&expected)
        {
            return Err(Error::Conflict(
                "durable model fork issuer binding does not match the owner secret".into(),
            ));
        }
        let resolver = self.resolver.clone().ok_or_else(|| {
            Error::Unsupported("local model fork resolver is not bound".into())
        })?;
        let plan = resolver.resolve(intent, publication.clone()).await?;
        plan.validate()?;
        if plan.publication_operation != publication.operation_id
            || plan.request != *request
            || plan.report != *report
            || plan.declaration != *declaration
        {
            return Err(Error::Conflict(
                "reconstructed fork plan differs from the durable activation binding".into(),
            ));
        }
        self.register(plan.clone()).await?;
        Ok(plan)
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

    async fn has_intent(&self, operation: OperationId) -> bool {
        self.intents
            .lock()
            .await
            .values()
            .any(|intent| intent.publication_operation == Some(operation))
    }

    async fn expected_children_for_publication(
        &self,
        operation: OperationId,
    ) -> Result<BTreeSet<TaskId>> {
        self.refresh_intents_from_journal().await?;
        let intents = self.intents.lock().await;
        Ok(intents
            .values()
            .filter(|intent| intent.publication_operation == Some(operation))
            .map(|intent| TaskId::from_bytes(intent.child_operation.into_bytes()))
            .collect())
    }

    async fn replay_intent(
        &self,
        parent: TaskId,
        invocation: &ToolInvocation,
        input: &LocalForkToolInput,
    ) -> Result<Option<LocalForkIntent>> {
        // A second handle may have selected this child after the current
        // process opened its plan cache. Reconcile the durable intent stream
        // before accepting a replay, so the model-facing path uses the same
        // owner projection as publication and recovery.
        self.refresh_intents_from_journal().await?;
        Ok(self
            .intents
            .lock()
            .await
            .values()
            .find(|intent| {
                intent.parent == parent
                    && intent.child_operation == input.child_operation
                    && intent.task == input.task
                    && intent.prompt == input.prompt
                    && intent.call_id.as_deref() == Some(invocation.call_id.as_str())
                    && ToolInvocation::for_model_call(
                        intent.parent_operation,
                        intent.parent_step,
                        invocation.call_id.clone(),
                        "acyclic.fork_child".into(),
                        Value::Null,
                    )
                    .operation_id
                        == invocation.operation_id
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
            // The publisher is registered for every completed model batch;
            // most batches do not select the fork tool and must complete as a
            // durable no-op. A selected fork is still required to carry its
            // intent, so resolve_intents retains the fail-closed path.
            if !self.plans.has_intent(publication.operation_id).await
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
                // Multiple children selected by one completed batch publish
                // sequentially on the same parent stream. Their immutable
                // captures remain stable, while each seed pins the parent's
                // current stream revision before its append.
                let current_revision = parent.reducer().revision();
                if plan.report.request.parent_revision != current_revision {
                    let previous_seed = plan.report.clone().into_seed()?;
                    rebind_report_history(&mut plan.report, current_revision)?;
                    let rebound_seed = plan.report.clone().into_seed()?;
                    plan.host
                        .rebind_fork_seed(&previous_seed, &rebound_seed)
                        .await?;
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
                    )
                    .await?;
                prepared.push((plan, seed));
            }
            // Every child is now durably admitted and bound to the parent
            // aggregate. Only after that barrier may a child model dispatch.
            for (plan, seed) in prepared {
                swarm
                    .activate_published_child(
                        plan.request,
                        plan.host,
                        plan.stream,
                        plan.issuer,
                        &parent,
                        &seed,
                    )
                    .await?;
            }
            Ok(())
        })
    }

    fn reconcile<'a>(
        &'a self,
        publication: ModelBatchPublication,
    ) -> BoxFuture<'a, Result<Option<()>>> {
        Box::pin(async move {
            let plans = self.plans.get_for_publication(publication.operation_id).await;
            let mut expected = self
                .plans
                .expected_children_for_publication(publication.operation_id)
                .await?;
            if plans.is_empty() {
                if !self.plans.has_intent(publication.operation_id).await {
                    return Ok(Some(()));
                }
            } else {
                for plan in &plans {
                    if publication.operation_id != plan.publication_operation {
                        return Err(Error::Conflict(
                            "reconciled model publication does not match its fork plan".into(),
                        ));
                    }
                    expected.insert(TaskId::from_bytes(
                        plan.request.child_operation.into_bytes(),
                    ));
                }
            }
            let Some(swarm) = self.target()? else {
                return Ok(None);
            };
            if swarm
                .model_publication_completed(&publication, &expected)
                .await?
            {
                return Ok(Some(()));
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
            context.validate_invocation(&invocation)?;
            let input: LocalForkToolInput = LocalForkToolInput::deserialize(&invocation.arguments)
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
            if context.task_id.is_some_and(|task| task != self.parent) {
                return Err(Error::Unauthorized(
                    "local fork tool task binding differs from the authenticated parent".into(),
                ));
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
            Ok(ToolResult {
                value: json!({
                    "status": "selected_after_completed_batch",
                    "fork_operation": fork_operation.to_string(),
                    "child_operation": input.child_operation.to_string(),
                }),
            })
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
            invocation.validate()?;
            let input: LocalForkToolInput = LocalForkToolInput::deserialize(&invocation.arguments)
                .map_err(|error| Error::Invalid(format!("local fork arguments are invalid: {error}")))?;
            let Some(intent) = self
                .plans
                .replay_intent(self.parent, &invocation, &input)
                .await?
            else {
                return Ok(None);
            };
            Ok(Some(ToolResult {
                value: json!({
                    "status": "selected_after_completed_batch",
                    "fork_operation": intent.fork_operation.to_string(),
                    "child_operation": intent.child_operation.to_string(),
                }),
            }))
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
            revision: "1".into(),
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
            output_schema: json!({
                "type": "object",
                "required": ["status", "fork_operation", "child_operation"],
                "properties": {
                    "status": {"const": "selected_after_completed_batch"},
                    "fork_operation": {"type": "string"},
                    "child_operation": {"type": "string"}
                },
                "additionalProperties": false
            }),
            model_output_schema: json!({
                "type": "object",
                "required": ["status", "fork_operation", "child_operation"],
                "properties": {
                    "status": {"const": "selected_after_completed_batch"},
                    "fork_operation": {"type": "string"},
                    "child_operation": {"type": "string"}
                },
                "additionalProperties": false
            }),
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

    /// Validates application bounds before opening any provider.
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
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
    /// Current authoritative conversation revision.
    pub conversation_revision: u64,
    /// Generation observed from the task's private filesystem volume.
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Replayable output for a completed root session. Child completions use
    /// the dedicated ForkCompleted event, while root sessions are terminal
    /// Session records and therefore carry their artifact reference here.
    #[serde(default)]
    completion: Option<StoredCompletionRef>,
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
    /// Atomically records the selected child and host issuer binding.
    ForkIntentSelected {
        intent: LocalForkIntent,
        issuer_digest: Option<[u8; 32]>,
    },
    /// Model-selected child intent retained before completed-batch
    /// publication. The owner allocator resolves it only after publication.
    ForkIntent { intent: LocalForkIntent },
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
            completion: None,
        }
    }
}

impl StoredSession {
    fn completed(value: &LocalSwarmSession, completion: StoredCompletionRef) -> Self {
        Self {
            version: REGISTRY_VERSION,
            task: value.task,
            parent: value.parent,
            depth: value.depth,
            task_description: value.task_description.clone(),
            operation: value.operation,
            phase: value.phase.clone().into(),
            completion: Some(completion),
        }
    }
}

/// Durable local recursive application composition.
pub struct PersistentLocalSwarm {
    root: PathBuf,
    config: LocalSwarmConfig,
    provider: Arc<dyn ModelProvider>,
    bindings: LocalSwarmBindings,
    model_fork_publisher: Option<Arc<LocalModelForkPublisher>>,
    registry: StreamClient<LocalStream>,
    /// Shared provider bindings used by the root and lazily reopened task
    /// harnesses. The resolver must observe the same host and stream domain.
    filesystem_host: Arc<FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>>,
    conversation_stream: StreamClient<LocalStream>,
    stream_provider: ProviderRef,
    records: Mutex<BTreeMap<TaskId, LocalSwarmSession>>,
    requests: Mutex<BTreeMap<TaskId, LocalForkRequest>>,
    seeds: Mutex<BTreeMap<TaskId, ForkSeed>>,
    reports: Mutex<BTreeMap<TaskId, ForkReport>>,
    publications: Mutex<BTreeMap<TaskId, ModelBatchPublication>>,
    declarations: Mutex<BTreeMap<TaskId, LocalInheritedModelDeclaration>>,
    outcomes: Mutex<BTreeMap<TaskId, TurnOutput>>,
    completion_refs: Mutex<BTreeMap<TaskId, StoredCompletionRef>>,
    sessions: Mutex<BTreeMap<TaskId, Arc<PersistentLocalHarness>>>,
    /// Per-task live terminal fences. The registry remains the cross-process
    /// authority; these narrow gates prevent duplicate retries without
    /// deadlocking a child turn that recursively activates a grandchild.
    task_gates: Mutex<BTreeMap<TaskId, Arc<Mutex<()>>>>,
    /// Host-only operator choices awaiting resolution. The choice is retained
    /// with the exact ticket binding so a public resolve request cannot swap
    /// an operation or action digest between the private decision and commit.
    operator_choices: Mutex<BTreeMap<String, LocalOperatorChoice>>,
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
        mut config: LocalSwarmConfig,
        provider: Arc<dyn ModelProvider>,
        mut bindings: LocalSwarmBindings,
    ) -> Result<Self> {
        config.validate()?;
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
                let host = shared_local_filesystem(root.join("filesystem"), filesystem_provider)
                    .await?;
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
        let records = load_records(&stream).await?;
        let mut sessions = BTreeMap::new();
        let mut requests = BTreeMap::new();
        let mut seeds = BTreeMap::new();
        let mut reports = BTreeMap::new();
        let mut publications = BTreeMap::new();
        let mut declarations = BTreeMap::new();
        let mut outcomes = BTreeMap::new();
        let mut completion_refs = BTreeMap::new();
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
            append_record(&stream, StoredEvent::Session(root_session.clone().into())).await?;
            sessions.insert(root_task, root_session);
        }
        let root_task = sessions
            .values()
            .find(|session| session.parent.is_none())
            .map(|session| session.task)
            .ok_or_else(|| Error::Storage("swarm registry has no root session".into()))?;
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
        let mut opened = BTreeMap::new();
        opened.insert(root_task, root_harness);
        Ok(Self {
            root,
            config,
            provider,
            bindings,
            model_fork_publisher,
            registry,
            filesystem_host,
            conversation_stream,
            stream_provider,
            records: Mutex::new(sessions),
            requests: Mutex::new(requests),
            seeds: Mutex::new(seeds),
            reports: Mutex::new(reports),
            publications: Mutex::new(publications),
            declarations: Mutex::new(declarations),
            outcomes: Mutex::new(outcomes),
            completion_refs: Mutex::new(completion_refs),
            sessions: Mutex::new(opened),
            task_gates: Mutex::new(BTreeMap::new()),
            operator_choices: Mutex::new(BTreeMap::new()),
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
            bindings.with_model_fork_plans(Arc::new(LocalModelForkPlans::new().with_resolver(resolver)))
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
        let root = root.as_ref().to_path_buf();
        let filesystem_provider = ProviderRef::new("local", "filesystem", "2")?;
        let host = shared_local_filesystem(root.join("filesystem"), filesystem_provider.clone())
            .await?;
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
        let base = Self::open_shared_with_bindings(
            root.clone(),
            config,
            provider.clone(),
            LocalSwarmBindings::default(),
        )
        .await?;
        let root_task = base.root_task().await?;
        let host_secret = base.open_session(root_task).await?.signing_key();
        drop(base);
        let resolver = Arc::new(
            LocalFilesystemForkResolver::new(host, stream, stream_provider, project)?
                .with_host_secret(host_secret)?,
        );
        Self::open_shared_with_model_and_bindings(
            root,
            model,
            provider,
            limits,
            LocalSwarmBindings::default().with_filesystem_fork_resolver(resolver),
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
                Error::Unauthorized(
                    "approval requires an authenticated operator choice".into(),
                )
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
    pub async fn sessions(&self) -> Vec<LocalSwarmSession> {
        // A sibling handle may have appended an admission or terminal record
        // since this process last observed the registry. Keep this legacy
        // infallible listing API live while making its projection durable.
        let _ = self.refresh_registry_state().await;
        self.records.lock().await.values().cloned().collect()
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
        let session = self.session(task).await?;
        let children = self
            .records
            .lock()
            .await
            .values()
            .filter(|candidate| candidate.parent == Some(task))
            .cloned()
            .collect();
        let harness = self.open_session(task).await?;
        let aggregate = harness.conversation_aggregate(self.config.limits).await?;
        let conversation = aggregate
            .reducer()
            .conversation()
            .cloned()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
        let workspace_generation = match harness
            .list_private_directory("", None, None, 1)
            .await
        {
            Ok(page) => Some(page.generation),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        Ok(LocalSwarmSnapshot {
            session,
            children,
            conversation_revision: conversation.messages.len() as u64,
            workspace_generation,
        })
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
        let harness = self.open_session(task).await?;
        harness
            .conversation_events(after_revision, limit, self.config.limits)
            .await
    }

    /// Reads a bounded page of canonical conversation messages by sequence.
    /// Message content remains an immutable FileRef until the caller requests
    /// it through the authenticated private file API.
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
        let state = harness.conversation_state(self.config.limits).await?;
        Ok(state
            .messages
            .into_iter()
            .filter(|message| message.sequence > after_sequence)
            .take(limit)
            .collect())
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
        harness
            .list_private_directory(path, expected_generation, after, maximum_entries)
            .await
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
        harness.read_private_path(path, expected_generation).await
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
                serde_json::from_slice::<InteractionResponse>(&harness.storage().read(detail).await?)
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
        let host = self
            .bindings
            .communication_host
            .clone()
            .ok_or_else(|| Error::Unsupported("durable communication host is not bound".into()))?;
        // Mail payloads are staged into the recipient's own private volume.
        // A sender-owned FileRef would require an implicit sibling read grant
        // and would make an otherwise valid parent/child message unreadable
        // at inbox time. The owner host performs this copy before publishing
        // the ref-only inbox event.
        let harness = self.open_session(recipient).await?;
        let payload = harness
            .storage()
            .stage(
                message_id,
                &format!("system/swarm/messages/{message_id}.txt"),
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
        let host =
            self.bindings.communication_host.clone().ok_or_else(|| {
                Error::Unsupported("durable communication host is not bound".into())
            })?;
        DurableCommunication::new(host)
            .inbox(task, after_sequence, limit)
            .await
    }

    /// Durably cancels one task and propagates the owner cancellation signal
    /// when a live source is available. The journal append is authoritative;
    /// an observation-only live bridge does not undo the persisted decision.
    pub async fn cancel(&self, task: TaskId) -> Result<LocalSwarmSession> {
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        let session = self.session(task).await?;
        if session.phase == LocalSessionPhase::Cancelled {
            return Ok(session);
        }
        if session.phase == LocalSessionPhase::Completed {
            return Err(Error::Conflict(
                "completed local swarm task cannot be cancelled".into(),
            ));
        }
        if matches!(&session.phase, LocalSessionPhase::Failed(_)) {
            return Err(Error::Conflict(
                "failed local swarm task cannot be cancelled".into(),
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
                return Ok(latest);
            }
            return Err(error);
        }
        if let Some(current) = self.records.lock().await.get_mut(&task) {
            current.phase = LocalSessionPhase::Cancelled;
        }
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
        let expected_operation = if let Some(request) = self.requests.lock().await.get(&task) {
            request.child_operation
        } else {
            self.session(task).await?.operation.ok_or_else(|| {
                Error::NotFound(format!("local swarm operation {task}"))
            })?
        };
        if reference.operation != expected_operation {
            return Err(Error::Conflict(
                "durable completion artifact operation changed".into(),
            ));
        }
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
        let gate = self.task_gate(task).await;
        let _completion_guard = gate.lock().await;
        self.refresh_registry_state().await?;
        let session = self.session(task).await?;
        if matches!(
            &session.phase,
            LocalSessionPhase::Cancelled | LocalSessionPhase::Failed(_)
        ) {
            return Err(Error::Conflict(
                "terminal local swarm task cannot run again".into(),
            ));
        }
        // A published child owns one durable turn identity and prompt. Check
        // both while the registry still says the turn is non-terminal, before
        // opening the harness or staging any prompt bytes. A caller that
        // supplies a fresh operation or prompt must not reach model dispatch
        // and only fail later in complete_session.
        if let Some(retained) = self.requests.lock().await.get(&task).cloned() {
            if retained.child_operation != operation || retained.prompt != prompt {
                return Err(Error::Conflict(
                    "local child retry must use its retained operation and prompt".into(),
                ));
            }
        }
        if session.phase == LocalSessionPhase::Completed {
            if session.operation != Some(operation) {
                return Err(Error::Conflict(
                    "local swarm task already completed under another operation".into(),
                ));
            }
            return self.outcome(task).await;
        }
        let mut session = session;
        if session.operation.is_none() {
            self.reserve_session_operation(task, operation).await?;
            // The reservation append is a cross-handle CAS. Reload the
            // durable projection before using the parent or opening a model
            // harness so a cancellation that won the adjacent race cannot
            // fall through from a stale Ready snapshot.
            session = self.session(task).await?;
            if matches!(
                &session.phase,
                LocalSessionPhase::Cancelled | LocalSessionPhase::Failed(_)
            ) {
                return Err(Error::Conflict(
                    "terminal local swarm task cannot run again".into(),
                ));
            }
        } else if session.operation != Some(operation) {
            return Err(Error::Conflict(
                "local swarm task is bound to another operation".into(),
            ));
        }
        let parent = session.parent;
        self.verify_admitted_task(task, parent).await?;
        let harness = self.open_session(task).await?;
        let max_steps = u32::try_from(
            self.config
                .run_limits
                .max_steps
                .unwrap_or(self.config.limits.model_steps),
        )
        .map_err(|_| Error::Invalid("task step limit exceeds u32".into()))?;
        // Opening a harness can cross a process boundary. Reconcile the
        // durable terminal fence once more immediately before model
        // admission so a cancellation committed after reservation cannot
        // fall through from this handle's stale session snapshot.
        self.refresh_registry_state().await?;
        let admitted = self.session(task).await?;
        if matches!(
            &admitted.phase,
            LocalSessionPhase::Cancelled | LocalSessionPhase::Failed(_)
        ) {
            return Err(Error::Conflict(
                "terminal local swarm task cannot dispatch a model turn".into(),
            ));
        }
        if admitted.phase == LocalSessionPhase::Completed {
            if admitted.operation != Some(operation) {
                return Err(Error::Conflict(
                    "local swarm task already completed under another operation".into(),
                ));
            }
            return self.outcome(task).await;
        }
        if admitted.operation != Some(operation) {
            return Err(Error::Conflict(
                "local swarm task lost its durable operation reservation".into(),
            ));
        }
        let output = harness
            .run_with_max_steps(operation, prompt, max_steps)
            .await?;
        // The per-task mutex only fences handles in this process.  A second
        // process can cancel the task while the model is running, so the
        // registry must be refreshed before the terminal Session event is
        // appended.  `complete_session` also makes same-operation recovery
        // idempotent while rejecting a different operation key.
        self.complete_session(task, operation, &harness, &output).await?;
        self.outcomes.lock().await.insert(task, output.clone());
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
            return Err(Error::Unauthorized(
                "local swarm depth limit exceeded".into(),
            ));
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
        self.verify_admitted_task(request.parent, parent_session.parent)
            .await?;
        let child = TaskId::from_bytes(request.child_operation.into_bytes());
        // Recovery and publication can arrive through independent handles.
        // Fence the complete activation, including model dispatch, so a
        // second owner reconciles the first terminal result instead of
        // creating a duplicate child turn.
        let gate = self.task_gate(child).await;
        let _activation_guard = gate.lock().await;
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
        let parent_declaration = self.declarations.lock().await.get(&request.parent).cloned();
        let (boundary, verified_parent) = match (publication, declaration) {
            (Some(publication), Some(_declaration)) => {
                let verified = match parent_declaration {
                    Some(parent_declaration) => {
                        let inherited = parent_declaration.context(self.config.limits)?;
                        parent_harness
                            .storage()
                            .verified_inherited_model_fork_boundary(
                                &publication,
                                self.config.limits,
                                &inherited,
                            )
                            .await?
                    }
                    None => parent_harness
                        .storage()
                        .verified_model_fork_boundary(&publication, self.config.limits)
                        .await?,
                };
                let (boundary, parent) = verified.into_parts();
                (boundary, Some(parent))
            }
            _ => unreachable!("typed publication and declaration were checked together"),
        };
        let storage_parent = verified_parent.as_ref().unwrap_or(parent);
        let publication = stored_publication.ok_or_else(|| {
            Error::Conflict("published model batch disappeared before admission".into())
        })?;
        let declaration = stored_declaration.ok_or_else(|| {
            Error::Conflict("inherited declaration disappeared before admission".into())
        })?;
        let report = stored_report.ok_or_else(|| {
            Error::Conflict("prepared fork report disappeared before admission".into())
        })?;
        let seed_digest = fork_seed_digest(seed)?;
        let existing = self.records.lock().await.get(&child).cloned();
        let mut new_admission = match existing {
            None => true,
            Some(session) => {
                if self.requests.lock().await.get(&child) != Some(&request) {
                    return Err(Error::Conflict(
                        "child operation is already a different session".into(),
                    ));
                }
                if self.seeds.lock().await.get(&child) != Some(seed)
                    || self.reports.lock().await.get(&child) != Some(&report)
                    || self.publications.lock().await.get(&child) != Some(&publication)
                    || self.declarations.lock().await.get(&child) != Some(&declaration)
                {
                    return Err(Error::Conflict(
                        "existing child publication binding differs from published fork".into(),
                    ));
                }
                if session.phase == LocalSessionPhase::Completed {
                    if session.operation != Some(request.child_operation) {
                        return Err(Error::Conflict(
                            "completed child operation differs from the retry binding".into(),
                        ));
                    }
                    let output = self.outcome(child).await?;
                    return Ok(LocalForkOutcome {
                        child,
                        operation: request.child_operation,
                        output,
                    });
                }
                if session.phase == LocalSessionPhase::Cancelled {
                    return Err(Error::Conflict(
                        "cancelled child operation is terminal and cannot be resurrected".into(),
                    ));
                }
                if matches!(&session.phase, LocalSessionPhase::Failed(_)) {
                    return Err(Error::Conflict(
                        "failed child operation is terminal and cannot be resurrected".into(),
                    ));
                }
                false
            }
        };
        let registry = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if new_admission {
            let observed_tail = self.refresh_registry_state_with_tail().await?;
            let latest_parent = self.session(request.parent).await?;
            if latest_parent.depth >= self.config.maximum_depth {
                return Err(Error::Unauthorized("local swarm depth limit exceeded".into()));
            }
            let latest_child_count = self
                .records
                .lock()
                .await
                .values()
                .filter(|session| {
                    session.parent == Some(request.parent) && session.task != child
                })
                .count();
            if latest_child_count >= self.config.maximum_children {
                return Err(Error::Unauthorized("local swarm child limit exceeded".into()));
            }
            if let Some(session) = self.records.lock().await.get(&child).cloned() {
                if session.phase == LocalSessionPhase::Cancelled {
                    return Err(Error::Conflict(
                        "cancelled child operation is terminal and cannot be resurrected".into(),
                    ));
                }
                if matches!(&session.phase, LocalSessionPhase::Failed(_)) {
                    return Err(Error::Conflict(
                        "failed child operation is terminal and cannot be resurrected".into(),
                    ));
                }
                if session.phase == LocalSessionPhase::Completed {
                    if session.operation != Some(request.child_operation) {
                        return Err(Error::Conflict(
                            "completed child operation differs from the retry binding".into(),
                        ));
                    }
                    return Ok(LocalForkOutcome {
                        child,
                        operation: request.child_operation,
                        output: self.outcome(child).await?,
                    });
                }
                // Another handle admitted this exact child while the
                // preparation checks ran. Its observed tail is newer than
                // the stale projection that selected this branch.
                new_admission = false;
            }
            if new_admission {
                let append = append_record_at(
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
                        seed_digest: Some(seed_digest),
                        report: Some(report.clone()),
                        publication: Some(publication.clone()),
                        declaration: Some(declaration.clone()),
                    },
                    observed_tail,
                )
                .await;
                if let Err(error) = append {
                    // Another process may have won the append-at-tail race. A
                    // refresh turns that benign retry into the same admission;
                    // unrelated conflicts still surface to the caller.
                    self.refresh_registry_state().await?;
                    let reconciled = self.requests.lock().await.get(&child) == Some(&request)
                        && self.seeds.lock().await.get(&child) == Some(seed)
                        && self.reports.lock().await.get(&child) == Some(&report)
                        && self.publications.lock().await.get(&child) == Some(&publication)
                        && self.declarations.lock().await.get(&child) == Some(&declaration);
                    if !reconciled {
                        return Err(error);
                    }
                    new_admission = false;
                }
                if new_admission {
                    self.refresh_registry_state().await?;
                    if self
                        .records
                        .lock()
                        .await
                        .get(&child)
                        .is_some_and(|session| {
                            session.phase == LocalSessionPhase::Cancelled
                                || matches!(&session.phase, LocalSessionPhase::Failed(_))
                        })
                    {
                        return Err(Error::Conflict(
                            "child operation became terminal during admission".into(),
                        ));
                    }
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
                    self.seeds.lock().await.insert(child, seed.clone());
                    self.reports.lock().await.insert(child, report.clone());
                    self.publications.lock().await.insert(child, publication.clone());
                    self.declarations.lock().await.insert(child, declaration.clone());
                }
            }
        }
        if !new_admission {
            let phase = self
                .records
                .lock()
                .await
                .get(&child)
                .map(|session| session.phase.clone());
            if phase == Some(LocalSessionPhase::Cancelled) {
                return Err(Error::Conflict(
                    "cancelled child operation is terminal and cannot be resurrected".into(),
                ));
            }
            if matches!(&phase, Some(LocalSessionPhase::Failed(_))) {
                return Err(Error::Conflict(
                    "failed child operation is terminal and cannot be resurrected".into(),
                ));
            }
            if phase == Some(LocalSessionPhase::Completed) {
                // The admission loser observed a terminal winner while it
                // was preparing the child. Preserve that terminal state and
                // replay its durable output; never reopen the session as
                // Activating or dispatch a duplicate turn.
                return Ok(LocalForkOutcome {
                    child,
                    operation: request.child_operation,
                    output: self.outcome(child).await?,
                });
            }
            if phase != Some(LocalSessionPhase::Activating) {
                self.update_session(child, |session| {
                    session.phase = LocalSessionPhase::Activating;
                })
                .await?;
            }
        }
        let harness = match PersistentLocalHarness::from_published_fork_with_tools_and_stream_provider(
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
                self.mark_failed(child, error.to_string()).await?;
                return Err(error);
            }
        };
        self.verify_admitted_task(child, Some(request.parent))
            .await?;
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
        let child_agent = request
            .child_agent
            .ok_or_else(|| Error::Invalid("typed fork publication requires child agent".into()))?;
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
            || child_fork_operation(publication.operation_id, request.child_operation) != fork_operation
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
        let parent_session = self.session(request.parent).await?;
        // Validate every immutable execution binding before the registry can
        // admit a child or the filesystem resolver can allocate its private
        // workspace. Activation repeats this check immediately before model
        // dispatch, but publication must not create effects for a stale owner
        // admission or a provider policy that has drifted after restart.
        self.verify_admitted_task(request.parent, parent_session.parent)
            .await?;
        self.verify_model_provider_binding()?;
        // Admission and completion share one per-child terminal fence.
        // Narrowing the lock to this child permits a model turn to select a
        // grandchild without recursively taking a global swarm lock.
        let gate = self.task_gate(child).await;
        let _completion_guard = gate.lock().await;
        self.refresh_registry_state().await?;
        let parent = self.session(request.parent).await?;
        if parent.depth >= self.config.maximum_depth {
            return Err(Error::Unauthorized(
                "local swarm depth limit exceeded".into(),
            ));
        }
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
            if matches!(&existing.phase, LocalSessionPhase::Failed(_)) {
                return Err(Error::Conflict(
                    "failed child operation is terminal and cannot be resurrected".into(),
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
            .is_some_and(|session| {
                session.phase == LocalSessionPhase::Cancelled
                    || matches!(&session.phase, LocalSessionPhase::Failed(_))
            })
        {
            return Err(Error::Conflict(
                "child operation became terminal during admission preparation".into(),
            ));
        }
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
    ) -> Result<ForkSeed> {
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
                .is_some_and(|agent| agent != report.request.child_agent)
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
        Ok(seed)
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
        self.refresh_registry_state().await?;
        if self
            .records
            .lock()
            .await
            .get(&child)
            .is_some_and(|session| {
                session.phase == LocalSessionPhase::Cancelled
                    || matches!(&session.phase, LocalSessionPhase::Failed(_))
            })
        {
            return Err(Error::Conflict(
                "terminal child operation cannot be activated".into(),
            ));
        }
        if let Some(output) = self.outcomes.lock().await.get(&child).cloned() {
            return Ok(LocalForkOutcome {
                child,
                operation: request.child_operation,
                output,
            });
        }
        if let Some(output) = self
            .recover_completed_output(&stream, child, request.child_operation, &harness)
            .await?
        {
            return Ok(LocalForkOutcome {
                child,
                operation: request.child_operation,
                output,
            });
        }
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
            .inherited_builder(boundary, suffix, self.provider.clone(), self.config.limits)
            .and_then(|builder| {
                let builder = builder
                    .tools(harness.storage().default_tools(self.config.limits)?)
                    .grant("tool:call:acyclic.read_file")
                    .grant("tool:call:acyclic.stage_file")
                    .grant("tool:call:acyclic.list_files")
                    .limits(self.config.limits);
                self.bindings
                    .tools_for(child)?
                    .install_into(builder)?
                    .build()
            }) {
            Ok(bundle) => bundle,
            Err(error) => {
                self.mark_failed(child, error.to_string()).await?;
                return Err(error);
            }
        };
        let output = match self.run_child_turn(&harness, &bundle, &request).await {
            Ok(output) => output,
            Err(error) => return Err(error),
        };
        let output_bytes = crate::contract::canonical_json_bytes(&output)?;
        let output_digest = crate::contract::canonical_json_digest(&output_bytes)?;
        let (inline_output, output_ref) = if output_bytes.len() <= MAX_INLINE_COMPLETION_BYTES {
            (Some(output.clone()), None)
        } else {
            let output_ref = harness
                .storage()
                .stage(
                    request.child_operation,
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
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        if self
            .records
            .lock()
            .await
            .get(&child)
            .is_some_and(|session| {
                session.phase == LocalSessionPhase::Cancelled
                    || matches!(&session.phase, LocalSessionPhase::Failed(_))
            })
        {
            return Err(Error::Conflict(
                "child operation became terminal before completion acknowledgement".into(),
            ));
        }
        // The refresh above is also the authoritative tail observation for
        // the completion append. A plain append_record would read the tail
        // again after this check, allowing two handles to race through the
        // same terminal acknowledgement. Reconcile an append conflict to
        // the durable completion and replay that result instead of appending
        // a second completion or returning a locally generated answer.
        if let Some(existing) = self
            .recover_completed_output(&stream, child, request.child_operation, &harness)
            .await?
        {
            return Ok(LocalForkOutcome {
                child,
                operation: request.child_operation,
                output: existing,
            });
        }
        // The tail and projection above form one admission snapshot. Recheck
        // terminal state immediately before the CAS append so a cancellation
        // or completion that won the preceding refresh cannot be overwritten
        // by this worker's terminal event.
        let terminal = self.records.lock().await.get(&child).cloned();
        if terminal
            .as_ref()
            .is_some_and(|session| {
                session.phase == LocalSessionPhase::Cancelled
                    || matches!(&session.phase, LocalSessionPhase::Failed(_))
            })
        {
            return Err(Error::Conflict(
                "child operation became terminal before completion acknowledgement".into(),
            ));
        }
        if terminal
            .as_ref()
            .is_some_and(|session| session.phase == LocalSessionPhase::Completed)
        {
            if terminal.and_then(|session| session.operation) != Some(request.child_operation) {
                return Err(Error::Conflict(
                    "completed child operation differs from the completion binding".into(),
                ));
            }
            return Ok(LocalForkOutcome {
                child,
                operation: request.child_operation,
                output: self.outcome(child).await?,
            });
        }
        let completion = StoredEvent::ForkCompleted {
            child,
            operation: request.child_operation,
            output: inline_output,
            output_ref,
            output_digest: Some(output_digest),
        };
        if let Err(error) = append_record_at(&stream, completion, observed_tail).await {
            self.refresh_registry_state().await?;
            if let Some(existing) = self
                .recover_completed_output(&stream, child, request.child_operation, &harness)
                .await?
            {
                return Ok(LocalForkOutcome {
                    child,
                    operation: request.child_operation,
                    output: existing,
                });
            }
            return Err(error);
        }
        self.refresh_registry_state().await?;
        self.outcomes.lock().await.insert(child, output.clone());
        Ok(LocalForkOutcome {
            child,
            operation: request.child_operation,
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
        let mut replay_form = None;
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
                    if replay_form == Some(false) {
                        return Err(Error::Conflict(
                            "persisted child completion mixes inline and referenced outputs"
                                .into(),
                        ));
                    }
                    replay_form = Some(true);
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
                    if replay_form == Some(true) {
                        return Err(Error::Conflict(
                            "persisted child completion mixes inline and referenced outputs"
                                .into(),
                        ));
                    }
                    replay_form = Some(false);
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
            self.outcomes.lock().await.insert(child, output);
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

    /// Rechecks the owner-retained admission immediately before model
    /// dispatch. A reopened process must not run a task under a changed
    /// parent, numeric limit, or run budget binding.
    async fn verify_admitted_task(&self, task: TaskId, parent: Option<TaskId>) -> Result<()> {
        self.verify_model_provider_binding()?;
        let Some(host) = &self.bindings.communication_host else {
            return Ok(());
        };
        let admission = host.observe_admission(task).await?;
        if admission.parent != parent
            || admission.limits != self.config.limits
            || admission.run_limits != self.config.run_limits
        {
            return Err(Error::Conflict(
                "local swarm task admission no longer matches its pinned owner binding".into(),
            ));
        }
        Ok(())
    }

    /// Checks the provider-owned model option policy without constructing a
    /// fabricated model request or invoking provider admission. This is
    /// deliberately part of the publication barrier so an invalid or drifted
    /// policy cannot be discovered after child allocation.
    fn verify_model_provider_binding(&self) -> Result<()> {
        if let Some(policy) = self.provider.model_option_policy() {
            return policy.validate(&self.config.model.options);
        }
        let empty = self.config.model.options.is_null()
            || self
                .config
                .model
                .options
                .as_object()
                .is_some_and(|value| value.is_empty());
        if empty {
            Ok(())
        } else {
            Err(Error::Invalid(
                "model options require a registered provider policy".into(),
            ))
        }
    }

    /// Replays the exact admitted request after a process interruption.
    pub async fn retry(&self, task: TaskId) -> Result<LocalForkOutcome> {
        self.recover_activation(task).await
    }

    /// Recovers one admitted child activation from the durable task identity.
    ///
    /// All owner services stay in the swarm composition. Recovery reloads the
    /// original request, seed, report, publication, declaration, and issuer
    /// binding, then reuses the typed spawn/activate path. A caller cannot
    /// supply a replacement authority, workspace, prefix, or provider
    /// service.
    pub async fn recover_activation(&self, task: TaskId) -> Result<LocalForkOutcome> {
        self.refresh_registry_state().await?;
        let session = self.session(task).await?;
        if session.phase == LocalSessionPhase::Completed {
            return Ok(LocalForkOutcome {
                child: task,
                operation: session.operation.ok_or_else(|| {
                    Error::Conflict("completed local child has no operation binding".into())
                })?,
                output: self.outcome(task).await?,
            });
        }
        if session.phase == LocalSessionPhase::Cancelled {
            return Err(Error::Conflict(
                "cancelled child operation is terminal and cannot be resurrected".into(),
            ));
        }
        if let LocalSessionPhase::Failed(reason) = &session.phase {
            return Err(Error::Conflict(format!(
                "failed child operation is terminal and cannot be resurrected: {reason}"
            )));
        }
        if session.phase != LocalSessionPhase::Activating {
            return Err(Error::Conflict(
                "activation recovery requires a durably activating child".into(),
            ));
        }
        let request = self
            .requests
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::Conflict("activating child has no durable fork request".into()))?;
        let seed = self
            .seeds
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::Conflict("activating child has no durable fork seed".into()))?;
        let report = self
            .reports
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::Conflict("activating child has no durable fork report".into()))?;
        let publication = self
            .publications
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| {
                Error::Conflict("activating child has no durable model publication".into())
            })?;
        let declaration = self
            .declarations
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| {
                Error::Conflict("activating child has no durable recursive declaration".into())
            })?;
        let plans = self.bindings.model_fork_plans.as_ref().ok_or_else(|| {
            Error::Conflict("activating child has no retained owner fork services".into())
        })?;
        let plan = plans
            .recover_plan(&request, &report, &publication, &declaration)
            .await?;
        let recovered_seed = report.clone().into_seed()?;
        if recovered_seed != seed {
            return Err(Error::Conflict(
                "durable activating child seed differs from its fork report".into(),
            ));
        }
        let parent = self.open_session(request.parent).await?;
        let parent_aggregate = parent
            .storage()
            .conversation_aggregate(self.config.limits)
            .await?;
        self.activate_published_child(
            request,
            plan.host,
            plan.stream,
            plan.issuer,
            &parent_aggregate,
            &seed,
        )
        .await
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
        // A restarted handle must resolve the durable request and terminal
        // outcome before it reconstructs the child aggregate. Otherwise a
        // stale in-memory map can replay an older report or redispatch a
        // child that another handle has already completed.
        self.refresh_registry_state().await?;
        let request = self
            .requests
            .lock()
            .await
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("local swarm fork request {task}")))?;
        if let Ok(output) = self.outcome(task).await {
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

    /// Reopens a child after recovery. An activating record is reconstructed
    /// from its durable request, seed, report, publication, and declaration;
    /// only that exact owner binding may resume its pending turn.
    pub async fn resume(&self, task: TaskId) -> Result<LocalSwarmSession> {
        self.refresh_registry_state().await?;
        let session = self.session(task).await?;
        if session.phase == LocalSessionPhase::Activating {
            let request = self
                .requests
                .lock()
                .await
                .get(&task)
                .cloned()
                .ok_or_else(|| {
                    Error::Conflict(
                        "activating child has no durable fork request to resume".into(),
                    )
                })?;
            let seed = self
                .seeds
                .lock()
                .await
                .get(&task)
                .cloned()
                .ok_or_else(|| Error::Conflict("activating child has no durable fork seed".into()))?;
            let report = self
                .reports
                .lock()
                .await
                .get(&task)
                .cloned()
                .ok_or_else(|| {
                    Error::Conflict("activating child has no durable fork report".into())
                })?;
            let publication = self
                .publications
                .lock()
                .await
                .get(&task)
                .cloned()
                .ok_or_else(|| {
                    Error::Conflict("activating child has no durable model publication".into())
                })?;
            let declaration = self
                .declarations
                .lock()
                .await
                .get(&task)
                .cloned()
                .ok_or_else(|| {
                    Error::Conflict("activating child has no durable recursive declaration".into())
                })?;
            let plans = self.bindings.model_fork_plans.as_ref().ok_or_else(|| {
                Error::Conflict("activating child cannot reconstruct its owner issuer".into())
            })?;
            let resolver = plans.resolver.clone().ok_or_else(|| {
                Error::Conflict("activating child has no durable fork resolver".into())
            })?;
            let fork_operation = request.fork_operation.ok_or_else(|| {
                Error::Conflict("activating child request has no fork operation".into())
            })?;
            let intent = LocalForkIntent {
                parent: request.parent,
                parent_operation: request.parent_operation,
                parent_step: request.parent_step,
                publication_operation: Some(publication.operation_id),
                fork_operation,
                child_operation: request.child_operation,
                call_id: None,
                task: request.task.clone(),
                prompt: request.prompt.clone(),
            };
            let plan = resolver.resolve(intent, publication.clone()).await?;
            plan.validate()?;
            if plan.publication_operation != publication.operation_id
                || plan.request != request
                || plan.report != report
                || plan.declaration != declaration
                || plan.report.clone().into_seed()? != seed
            {
                return Err(Error::Conflict(
                    "durable activating child reconstruction changed its fork binding".into(),
                ));
            }
            let parent = self.open_session(request.parent).await?;
            let parent_aggregate = parent
                .storage()
                .conversation_aggregate(self.config.limits)
                .await?;
            let _ = self
                .activate_published_child(
                    request,
                    plan.host,
                    plan.stream,
                    plan.issuer,
                    &parent_aggregate,
                    &seed,
                )
                .await?;
            return self.session(task).await;
        }
        let _ = self.open_session(task).await?;
        self.session(task).await
    }

    async fn open_session(&self, task: TaskId) -> Result<Arc<PersistentLocalHarness>> {
        self.refresh_registry_state().await?;
        if let Some(existing) = self.sessions.lock().await.get(&task).cloned() {
            return Ok(existing);
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
        Ok(harness)
    }

    async fn update_session<F>(&self, task: TaskId, update: F) -> Result<()>
    where
        F: FnOnce(&mut LocalSwarmSession),
    {
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        self.update_session_at(observed_tail, task, update).await
    }

    /// Reserves the first operation for a root session before opening the
    /// model harness. The registry append is the cross-handle fence; a stale
    /// caller cannot dispatch under a different operation after another
    /// handle has claimed the task.
    async fn reserve_session_operation(
        &self,
        task: TaskId,
        operation: OperationId,
    ) -> Result<()> {
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        let current = self.session(task).await?;
        if matches!(
            &current.phase,
            LocalSessionPhase::Cancelled | LocalSessionPhase::Failed(_)
        ) {
            return Err(Error::Conflict(
                "terminal local swarm task cannot reserve another operation".into(),
            ));
        }
        if let Some(existing) = current.operation {
            return if existing == operation {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "local swarm task is bound to another operation".into(),
                ))
            };
        }
        match self
            .update_session_at(observed_tail, task, |session| {
                session.operation = Some(operation);
            })
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.refresh_registry_state().await?;
                match self.session(task).await?.operation {
                    Some(existing) if existing == operation => Ok(()),
                    Some(_) => Err(Error::Conflict(
                        "local swarm task is bound to another operation".into(),
                    )),
                    None => Err(error),
                }
            }
        }
    }

    async fn update_session_at<F>(
        &self,
        observed_tail: u64,
        task: TaskId,
        update: F,
    ) -> Result<()>
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
    async fn complete_session(
        &self,
        task: TaskId,
        operation: OperationId,
        harness: &PersistentLocalHarness,
        output: &TurnOutput,
    ) -> Result<()> {
        self.refresh_registry_state().await?;
        let current = self.session(task).await?;
        if matches!(
            &current.phase,
            LocalSessionPhase::Cancelled | LocalSessionPhase::Failed(_)
        ) {
            return Err(Error::Conflict(
                "terminal local swarm task cannot complete its model turn".into(),
            ));
        }
        if current.phase == LocalSessionPhase::Completed {
            if current.operation != Some(operation) {
                return Err(Error::Conflict(
                    "local swarm task already completed under another operation".into(),
                ));
            }
            return if self.outcome(task).await? == *output {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "local swarm task completed with a different output".into(),
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
        let output_bytes = crate::contract::canonical_json_bytes(output)?;
        let output_digest = crate::contract::canonical_json_digest(&output_bytes)?;
        let output_file = harness
            .storage()
            .stage(
                operation,
                &format!("system/swarm/completions/{task}.json"),
                &output_bytes,
                "application/json",
                "root-completion.json",
            )
            .await?;
        let completion = StoredCompletionRef {
            operation,
            file: output_file,
            digest: output_digest,
        };
        // Staging the artifact may itself cross a process boundary. Reload
        // the session after staging and use that exact registry tail for the
        // terminal Session CAS, so cancellation remains authoritative.
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        let current = self.session(task).await?;
        if matches!(
            &current.phase,
            LocalSessionPhase::Cancelled | LocalSessionPhase::Failed(_)
        ) {
            return Err(Error::Conflict(
                "terminal local swarm task cannot accept a completion acknowledgement".into(),
            ));
        }
        if current.phase == LocalSessionPhase::Completed {
            if current.operation != Some(operation) {
                return Err(Error::Conflict(
                    "local swarm task already completed under another operation".into(),
                ));
            }
            return if self.outcome(task).await? == *output {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "local swarm task completed with a different output".into(),
                ))
            };
        }
        if current.operation != Some(operation) {
            return Err(Error::Conflict(
                "local swarm task lost its durable operation reservation".into(),
            ));
        }
        let next = LocalSwarmSession {
            operation: Some(operation),
            phase: LocalSessionPhase::Completed,
            ..current.clone()
        };
        let stream = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        match self
            .append_session_completion_at(&stream, observed_tail, &next, completion.clone())
            .await
        {
            Ok(()) => {
                let mut records = self.records.lock().await;
                let current = records
                    .get_mut(&task)
                    .ok_or_else(|| Error::NotFound(format!("local swarm task {task}")))?;
                *current = next;
                self.completion_refs.lock().await.insert(task, completion);
                Ok(())
            }
            Err(error) => {
                // Another handle may have won the append between the refresh
                // and our CAS.  Reconcile once and only accept an identical
                // terminal operation; never turn a cancellation into success.
                self.refresh_registry_state().await?;
                let latest = self.session(task).await?;
                if latest.phase == LocalSessionPhase::Completed
                    && latest.operation == Some(operation)
                {
                    if self.outcome(task).await? == *output {
                        Ok(())
                    } else {
                        Err(Error::Conflict(
                            "local swarm task completed with a different output".into(),
                        ))
                    }
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn append_session_completion_at(
        &self,
        stream: &acyclic_stream::Stream<LocalStream>,
        observed_tail: u64,
        session: &LocalSwarmSession,
        completion: StoredCompletionRef,
    ) -> Result<()> {
        append_record_at(
            stream,
            StoredEvent::Session(StoredSession::completed(session, completion)),
            observed_tail,
        )
        .await
    }

    async fn task_gate(&self, task: TaskId) -> Arc<Mutex<()>> {
        let mut gates = self.task_gates.lock().await;
        gates
            .entry(task)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Reconciles the in-memory index with the append-only registry before a
    /// terminal mutation. This is the cross-process half of the local
    /// completion fence: a second handle sees an admission or cancellation
    /// committed by the first handle before it can dispatch or append again.
    async fn refresh_registry_state(&self) -> Result<()> {
        self.refresh_registry_state_with_tail().await.map(|_| ())
    }

    async fn refresh_registry_state_with_tail(&self) -> Result<u64> {
        let stream = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let observed_tail = match stream.tail().await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let records = load_records_at(&stream, observed_tail).await?;
        let mut sessions = BTreeMap::new();
        let mut requests = BTreeMap::new();
        let mut seeds = BTreeMap::new();
        let mut reports = BTreeMap::new();
        let mut publications = BTreeMap::new();
        let mut declarations = BTreeMap::new();
        let mut outcomes = BTreeMap::new();
        let mut completion_refs = BTreeMap::new();
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
                record,
            )?;
        }
        if !sessions.is_empty() {
            *self.records.lock().await = sessions;
        }
        *self.requests.lock().await = requests;
        *self.seeds.lock().await = seeds;
        *self.reports.lock().await = reports;
        *self.publications.lock().await = publications;
        *self.declarations.lock().await = declarations;
        *self.outcomes.lock().await = outcomes;
        *self.completion_refs.lock().await = completion_refs;
        Ok(observed_tail)
    }

    /// Reports whether every child selected by one model publication has a
    /// durable completion record. Publisher reconciliation uses this registry
    /// projection so a cold reopen does not trust a process-local cache.
    async fn model_publication_completed(
        &self,
        publication: &ModelBatchPublication,
        expected_children: &BTreeSet<TaskId>,
    ) -> Result<bool> {
        if expected_children.is_empty() {
            return Ok(false);
        }
        self.refresh_registry_state().await?;
        let sessions = self.records.lock().await.clone();
        let publications = self.publications.lock().await.clone();
        let outcomes = self.outcomes.lock().await.clone();
        let completion_refs = self.completion_refs.lock().await.clone();
        Ok(expected_children.iter().all(|child| {
            publications
                .get(child)
                .is_some_and(|stored| stored == publication)
                && sessions
                    .get(child)
                    .is_some_and(|session| session.phase == LocalSessionPhase::Completed)
                && (outcomes.contains_key(child) || completion_refs.contains_key(child))
        }))
    }

    async fn mark_failed(&self, task: TaskId, reason: String) -> Result<()> {
        let bounded = reason.chars().take(512).collect::<String>();
        let observed_tail = self.refresh_registry_state_with_tail().await?;
        let current = self.session(task).await?;
        if current.phase == LocalSessionPhase::Completed {
            return Ok(());
        }
        if current.phase == LocalSessionPhase::Cancelled {
            return Err(Error::Conflict(
                "cancelled local swarm task cannot be overwritten by failure".into(),
            ));
        }
        if let LocalSessionPhase::Failed(existing) = &current.phase {
            return if existing == &bounded {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "local swarm failure reason changed during retry".into(),
                ))
            };
        }
        let stream = self
            .registry
            .stream(REGISTRY_STREAM)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if let Err(error) = append_record_at(
            &stream,
            StoredEvent::ForkFailed {
                child: task,
                reason: bounded.clone(),
            },
            observed_tail,
        )
        .await
        {
            self.refresh_registry_state().await?;
            let latest = self.session(task).await?;
            return if latest.phase == LocalSessionPhase::Completed {
                Ok(())
            } else if latest.phase == LocalSessionPhase::Cancelled {
                Err(Error::Conflict(
                    "cancelled local swarm task won the failure race".into(),
                ))
            } else if let LocalSessionPhase::Failed(existing) = latest.phase {
                if existing == bounded {
                    Ok(())
                } else {
                    Err(Error::Conflict(
                        "local swarm failure reason changed during retry".into(),
                    ))
                }
            } else {
                Err(error)
            };
        }
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
    let tail = match stream.tail().await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => 0,
        Err(error) => return Err(Error::Storage(error.to_string())),
    };
    load_records_at(stream, tail).await
}

async fn load_records_at(
    stream: &acyclic_stream::Stream<LocalStream>,
    tail: u64,
) -> Result<Vec<StoredRecord>> {
    if tail == 0 {
        return Ok(Vec::new());
    }
    let mut records = stream
        .read(
            0,
            u32::try_from(tail)
                .map_err(|_| Error::Storage("swarm registry is too large".into()))?,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mut decoded = Vec::new();
    let mut expected_sequence = 0_u64;
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
    record: StoredRecord,
) -> Result<()> {
    match record.event {
        StoredEvent::Session(session) => {
            if session.version != REGISTRY_VERSION {
                return Err(Error::Conflict("unsupported local session version".into()));
            }
            let completion = session.completion.clone();
            let next: LocalSwarmSession = session.into();
            if let Some(existing) = sessions.get(&next.task) {
                if existing.operation.is_some() && existing.operation != next.operation {
                    return Err(Error::Conflict(
                        "persisted session operation binding changed".into(),
                    ));
                }
                if existing.phase == LocalSessionPhase::Cancelled
                    && next.phase != LocalSessionPhase::Cancelled
                {
                    return Err(Error::Conflict(
                        "persisted session completion follows terminal cancellation".into(),
                    ));
                }
                match (&existing.phase, &next.phase) {
                    (LocalSessionPhase::Completed, LocalSessionPhase::Completed)
                    | (LocalSessionPhase::Cancelled, LocalSessionPhase::Cancelled) => {}
                    (LocalSessionPhase::Failed(existing), LocalSessionPhase::Failed(next)) => {
                        if existing != next {
                            return Err(Error::Conflict(
                                "persisted session failure reason changed".into(),
                            ));
                        }
                    }
                    (LocalSessionPhase::Completed, _)
                    | (LocalSessionPhase::Cancelled, _)
                    | (LocalSessionPhase::Failed(_), _) => {
                        return Err(Error::Conflict(
                            "persisted session moved backwards from a terminal state".into(),
                        ));
                    }
                    _ => {}
                }
            }
            if let Some(reference) = completion {
                if next.phase != LocalSessionPhase::Completed
                    || next.operation != Some(reference.operation)
                {
                    return Err(Error::Conflict(
                        "persisted session completion is not bound to its terminal operation"
                            .into(),
                    ));
                }
                if let Some(existing) = completion_refs.get(&next.task)
                    && existing != &reference
                {
                    return Err(Error::Conflict(
                        "persisted session completion reference changed".into(),
                    ));
                }
                completion_refs.insert(next.task, reference);
            }
            sessions.insert(next.task, next);
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
        StoredEvent::ForkIssuerBinding { .. } => {}
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
            // A replayed admission is metadata for the child operation; it
            // must never move a terminal projection back to Activating. This
            // matters when a stale publisher appends or replays its admission
            // after another handle has already completed or failed the turn.
            let phase = sessions
                .get(&child)
                .map(|session| session.phase.clone())
                .map(|phase| match phase {
                    LocalSessionPhase::Completed => LocalSessionPhase::Completed,
                    LocalSessionPhase::Cancelled => LocalSessionPhase::Cancelled,
                    LocalSessionPhase::Failed(reason) => LocalSessionPhase::Failed(reason),
                    LocalSessionPhase::Ready | LocalSessionPhase::Activating => {
                        LocalSessionPhase::Activating
                    }
                })
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
            output_ref,
            output_digest,
        } => {
            if requests
                .get(&child)
                .is_none_or(|request| request.child_operation != operation)
            {
                return Err(Error::Conflict(
                    "persisted child completion is bound to another operation".into(),
                ));
            }
            let session = sessions
                .get_mut(&child)
                .ok_or_else(|| Error::Storage("fork completion child is missing".into()))?;
            if session.phase == LocalSessionPhase::Cancelled {
                return Err(Error::Conflict(
                    "persisted child completion follows a terminal cancellation".into(),
                ));
            }
            if matches!(&session.phase, LocalSessionPhase::Failed(_)) {
                return Err(Error::Conflict(
                    "persisted child completion follows a terminal failure".into(),
                ));
            }
            if session.phase == LocalSessionPhase::Completed
                && session.operation != Some(operation)
            {
                return Err(Error::Conflict(
                    "persisted child completion changes the terminal operation".into(),
                ));
            }
            session.operation = Some(operation);
            session.phase = LocalSessionPhase::Completed;
            match (output, output_ref, output_digest) {
                (Some(output), None, digest) => {
                    if completion_refs.contains_key(&child) {
                        return Err(Error::Conflict(
                            "persisted child completion mixes inline and referenced outputs"
                                .into(),
                        ));
                    }
                    if let Some(digest) = digest {
                        let bytes = crate::contract::canonical_json_bytes(&output)?;
                        if crate::contract::canonical_json_digest(&bytes)? != digest {
                            return Err(Error::Conflict(
                                "persisted inline child completion digest changed".into(),
                            ));
                        }
                    }
                    if let Some(existing) = outcomes.get(&child)
                        && existing != &output
                    {
                        return Err(Error::Conflict(
                            "persisted child completion output changed".into(),
                        ));
                    }
                    outcomes.insert(child, output);
                }
                (None, Some(file), Some(digest)) => {
                    if outcomes.contains_key(&child) {
                        return Err(Error::Conflict(
                            "persisted child completion mixes inline and referenced outputs"
                                .into(),
                        ));
                    }
                    let value = StoredCompletionRef {
                        operation,
                        file,
                        digest,
                    };
                    if let Some(existing) = completion_refs.get(&child)
                        && existing != &value
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
        }
        StoredEvent::ForkFailed { child, reason } => {
            if let Some(session) = sessions.get_mut(&child) {
                if matches!(
                    &session.phase,
                    LocalSessionPhase::Completed | LocalSessionPhase::Cancelled
                ) {
                    return Ok(());
                }
                if let LocalSessionPhase::Failed(existing) = &session.phase {
                    if existing != &reason {
                        return Err(Error::Conflict(
                            "persisted child failure reason changed".into(),
                        ));
                    }
                    return Ok(());
                }
                session.phase = LocalSessionPhase::Failed(reason);
            }
        }
        StoredEvent::ForkCancelled { child } => {
            if let Some(session) = sessions.get_mut(&child) {
                if session.phase == LocalSessionPhase::Completed {
                    return Err(Error::Conflict(
                        "persisted child cancellation follows terminal completion".into(),
                    ));
                }
                if matches!(&session.phase, LocalSessionPhase::Failed(_)) {
                    return Err(Error::Conflict(
                        "persisted child cancellation follows terminal failure".into(),
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
    use crate::model::{ModelAttempt, ModelEvent, ModelRequest};
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
        fn generate<'a>(&'a self, prepared: crate::model_input::PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
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

    #[test]
    fn completion_and_failure_cannot_rebind_a_child_operation() -> Result<()> {
        let parent = TaskId::new();
        let child = TaskId::new();
        let expected = OperationId::new();
        let mut sessions = BTreeMap::from([
            (
                parent,
                LocalSwarmSession {
                    task: parent,
                    parent: None,
                    depth: 0,
                    task_description: "root".into(),
                    operation: None,
                    phase: LocalSessionPhase::Ready,
                },
            ),
            (
                child,
                LocalSwarmSession {
                    task: child,
                    parent: Some(parent),
                    depth: 1,
                    task_description: "child".into(),
                    operation: Some(expected),
                    phase: LocalSessionPhase::Activating,
                },
            ),
        ]);
        let mut requests = BTreeMap::from([(
            child,
            LocalForkRequest {
                parent,
                parent_operation: OperationId::new(),
                parent_step: 0,
                fork_operation: Some(OperationId::new()),
                child_operation: expected,
                child_authority: None,
                child_agent: None,
                task: "child".into(),
                prompt: "prompt".into(),
            },
        )]);
        let mut seeds = BTreeMap::new();
        let mut reports = BTreeMap::new();
        let mut publications = BTreeMap::new();
        let mut declarations = BTreeMap::new();
        let mut outcomes = BTreeMap::new();
        let mut completion_refs = BTreeMap::new();
        let wrong = StoredRecord {
            version: REGISTRY_VERSION,
            event: StoredEvent::ForkCompleted {
                child,
                operation: OperationId::new(),
                output: Some(TurnOutput {
                    text: "wrong".into(),
                    attachments: Vec::new(),
                    metadata: Value::Null,
                    steps: 0,
                }),
                output_ref: None,
                output_digest: None,
            },
        };
        assert!(apply_record(
            &mut sessions,
            &mut requests,
            &mut seeds,
            &mut reports,
            &mut publications,
            &mut declarations,
            &mut outcomes,
            &mut completion_refs,
            wrong,
        )
        .is_err());
        assert_eq!(sessions[&child].phase, LocalSessionPhase::Activating);
        apply_record(
            &mut sessions,
            &mut requests,
            &mut seeds,
            &mut reports,
            &mut publications,
            &mut declarations,
            &mut outcomes,
            &mut completion_refs,
            StoredRecord {
                version: REGISTRY_VERSION,
                event: StoredEvent::ForkCompleted {
                    child,
                    operation: expected,
                    output: Some(TurnOutput {
                        text: "done".into(),
                        attachments: Vec::new(),
                        metadata: Value::Null,
                        steps: 1,
                    }),
                    output_ref: None,
                    output_digest: None,
                },
            },
        )?;
        apply_record(
            &mut sessions,
            &mut requests,
            &mut seeds,
            &mut reports,
            &mut publications,
            &mut declarations,
            &mut outcomes,
            &mut completion_refs,
            StoredRecord {
                version: REGISTRY_VERSION,
                event: StoredEvent::ForkFailed {
                    child,
                    reason: "late provider failure".into(),
                },
            },
        )?;
        assert_eq!(sessions[&child].phase, LocalSessionPhase::Completed);
        assert_eq!(outcomes[&child].text, "done");
        Ok(())
    }

    #[test]
    fn failed_child_is_terminal_against_cancel_and_completion() -> Result<()> {
        let parent = TaskId::new();
        let child = TaskId::new();
        let operation = OperationId::new();
        let mut sessions = BTreeMap::from([(
            child,
            LocalSwarmSession {
                task: child,
                parent: Some(parent),
                depth: 1,
                task_description: "child".into(),
                operation: Some(operation),
                phase: LocalSessionPhase::Activating,
            },
        )]);
        let mut requests = BTreeMap::from([(
            child,
            LocalForkRequest {
                parent,
                parent_operation: OperationId::new(),
                parent_step: 0,
                fork_operation: Some(OperationId::new()),
                child_operation: operation,
                child_authority: None,
                child_agent: None,
                task: "child".into(),
                prompt: "prompt".into(),
            },
        )]);
        let mut seeds = BTreeMap::new();
        let mut reports = BTreeMap::new();
        let mut publications = BTreeMap::new();
        let mut declarations = BTreeMap::new();
        let mut outcomes = BTreeMap::new();
        let mut completion_refs = BTreeMap::new();
        let record = |event| StoredRecord {
            version: REGISTRY_VERSION,
            event,
        };

        apply_record(
            &mut sessions,
            &mut requests,
            &mut seeds,
            &mut reports,
            &mut publications,
            &mut declarations,
            &mut outcomes,
            &mut completion_refs,
            record(StoredEvent::ForkFailed {
                child,
                reason: "provider stopped".into(),
            }),
        )?;
        assert!(matches!(
            &sessions[&child].phase,
            LocalSessionPhase::Failed(reason) if reason == "provider stopped"
        ));
        assert!(apply_record(
            &mut sessions,
            &mut requests,
            &mut seeds,
            &mut reports,
            &mut publications,
            &mut declarations,
            &mut outcomes,
            &mut completion_refs,
            record(StoredEvent::ForkCancelled { child }),
        )
        .is_err());
        assert!(apply_record(
            &mut sessions,
            &mut requests,
            &mut seeds,
            &mut reports,
            &mut publications,
            &mut declarations,
            &mut outcomes,
            &mut completion_refs,
            record(StoredEvent::ForkCompleted {
                child,
                operation,
                output: Some(TurnOutput {
                    text: "late completion".into(),
                    attachments: Vec::new(),
                    metadata: Value::Null,
                    steps: 1,
                }),
                output_ref: None,
                output_digest: None,
            }),
        )
        .is_err());
        assert!(matches!(
            &sessions[&child].phase,
            LocalSessionPhase::Failed(reason) if reason == "provider stopped"
        ));
        Ok(())
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
        assert_eq!(swarm.sessions().await.len(), 1);
        Ok(())
    }
}
