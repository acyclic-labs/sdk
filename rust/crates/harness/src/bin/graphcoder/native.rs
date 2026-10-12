//! Native composition of existing local providers, task admission and wire handles.
#![allow(missing_docs, reason = "private binary configuration and composition, not a public SDK contract")]

use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness::{
    Admission, Error, OperationId, Result, TaskId,
    conversation::{ContentPublisher as _, Limits, VolumeRef},
    context::{Context, ContextAttribute, ContextDiscovery, ContextPipeline, ContextPlacement,
        ContextRenderMode, DiscoveredContext, SourceStage},
    core::{Authority, AuthorityIssuer, AuthorityVerifier, SchemaRegistry, Scope, Snapshot},
    distributed::{WorkLease, WorkPull, Worker},
    executor::TurnOutput,
    filesystem::{FilesystemContentPublisher, FilesystemContentVerifier, FilesystemForkVerifier, FilesystemHost,
        FilesystemProjectMergeVerifier, FilesystemResourceCreationProvider, FilesystemTaskRuntime,
        ForkPreparationMachine, MailboxModelMachine, StockTurnMachine,
        StreamHistoryForkVerifier, TaskWorkerAttempt, TaskWorkerOutcome},
    fork::{CompositeForkVerifier, ForkReport},
    model::{Model, ModelContent, ModelMessage, ModelRole},
    resources::ProviderRef,
    resource_preparation::ResourcePreparationRequest,
    runtime::{DurableTaskHost as _, RuntimeScope, RuntimeTask, TaskContext, TaskDefinition, TaskRegistry, TaskRunLimits},
    scheduler::{LeaseFence, SessionLimits},
    store::{HistoryReadLimits, HistoryReader, StreamAggregate},
    wire,
    wire_api::{HarnessWireApi, OperationControlRequest, negotiate,
        operation_status, validate_cancel_request, validate_observe_request, validate_resume_protocol},
    wire_codec::{decode_command_envelope, encode_authority, encode_event_envelope},
    workflow::{MachineRegistry, WorkflowCommand},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, LocalStream};
use futures::stream;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};

use super::terminal::{MAX_FRAME_BYTES, MAX_PAGE_EVENTS};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityBinding {
    pub authority: Authority,
    pub issuer: String,
    pub key: [u8; 32],
    pub scope: Scope,
}

impl AuthorityBinding {
    pub(super) fn verifier(&self) -> Result<AuthorityVerifier> {
        let verifier = AuthorityIssuer::new(&self.issuer, self.key, self.authority.clone()).verifier(&mut None).expect("verifier copy");
        verifier.verify(&self.scope)?;
        Ok(verifier)
    }
}

/// A producer-owned explicit local composition.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub filesystem_root: PathBuf,
    pub stream_root: PathBuf,
    pub filesystem_provider: ProviderRef,
    pub stream_provider: ProviderRef,
    pub private_volume: VolumeRef,
    pub owner: AuthorityBinding,
    pub observed_authorities: Vec<AuthorityBinding>,
    pub generation: String,
    pub model_invocation_ledger: PathBuf,
    pub limits: Limits,
    pub run_limits: TaskRunLimits,
    pub session_limits: SessionLimits,
    pub concurrency: usize,
    #[serde(default)]
    pub deterministic_delay_ms: u32,
    #[serde(default)]
    pub fork_sources: Vec<ForkSourceConfig>,
    #[serde(default)]
    pub tools: super::native_tools::NativeToolsConfig,
    #[serde(default)]
    pub context: WorkflowContext,
    #[serde(default)]
    pub native_publications: Vec<super::native_writeback::NativePublicationConfig>,
    /// Actual coordinator session graph root for explicit creator-only source policy.
    #[serde(default)]
    pub project_source_root: Option<TaskId>,
}

impl Config {
    /// Decode one finite producer configuration, independently of input frames.
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        use std::io::Read as _;
        const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
        let file = std::fs::File::open(path).map_err(|error| Error::Storage(error.to_string()))?;
        let mut bytes = Vec::new();
        file.take(MAX_CONFIG_BYTES + 1).read_to_end(&mut bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(Error::Invalid("native configuration exceeds one MiB".into()));
        }
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(format!("native configuration: {error}")))
    }
}

pub(super) type Files = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
pub(super) type Runtime = FilesystemTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

/// Producer-retained original caller checkpoint and its exact owner binding.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkSourceConfig {
    pub caller: TaskId,
    pub owner: AuthorityBinding,
    pub project: VolumeRef,
    pub snapshot: Snapshot,
    pub request: acyclic_harness::fork::ForkRequest,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowContext {
    pub discovery: Option<ContextDiscovery>,
    pub discovered: Option<DiscoveredContext>,
    #[serde(default)]
    pub attributes: Vec<ContextAttribute>,
    pub attribute_mode: Option<ContextRenderMode>,
}

impl WorkflowContext {
    fn pipeline(&self) -> Result<ContextPipeline> {
        let mut pipeline = ContextPipeline::default();
        if let Some(discovered) = &self.discovered {
            pipeline = pipeline.with(Arc::new(discovered.clone().stage(ContextPlacement::Prepend)?));
        }
        for attribute in &self.attributes {
            attribute.validate()?;
            let mode = self.attribute_mode.unwrap_or(ContextRenderMode::Prompt);
            let text = serde_json::to_string(attribute).map_err(|error| Error::Invalid(error.to_string()))?;
            pipeline = pipeline.with(Arc::new(SourceStage::new(
                attribute.type_name.clone(), attribute.state_revision.clone(),
                Arc::new(Context { messages: vec![ModelMessage {
                    role: ModelRole::System, content: ModelContent::Text(format!("{mode:?}:{text}")),
                }], ..Context::default() }),
                if mode == ContextRenderMode::Update { ContextPlacement::Append } else { ContextPlacement::Prepend },
            )));
        }
        Ok(pipeline)
    }
}

pub struct NativeSession {
    pub(super) config: Config,
    pub(super) stream: std::sync::Arc<LocalStream>,
    pub(super) files: Arc<Files>,
    pub(super) runtime: Arc<Runtime>,
    context: std::sync::RwLock<WorkflowContext>,
    model: Model,
    model_provider: Arc<super::provider::Deterministic>,
}

impl NativeSession {
    pub async fn open(config: Config) -> Result<Self> {
        if !config.filesystem_root.is_absolute() || !config.stream_root.is_absolute()
            || config.generation.is_empty() || config.generation.len() > 128
            || config.observed_authorities.len() > 64 || config.concurrency == 0
            || config.deterministic_delay_ms > 5000 || config.fork_sources.len() > 64
            || config.native_publications.len() > 64
        {
            return Err(Error::Invalid("native composition requires absolute paths and finite bounds".into()));
        }
        config.limits.validate()?;
        if !cfg!(feature = "native-execution")
            && (!config.native_publications.is_empty() || !config.tools.commands.is_empty()) {
            return Err(Error::Unsupported("native providers require native-execution".into()));
        }
        let verifier = config.owner.verifier()?;
        for binding in &config.observed_authorities {
            binding.verifier()?;
        }
        for source in &config.fork_sources {
            source.owner.verifier()?;
            source.project.validate()?;
            source.request.validate()?;
            if source.request.parent != source.owner.authority
                || source.request.parent_revision != source.snapshot.revision
            {
                return Err(Error::Unauthorized("original fork source request differs from its authenticated cut".into()));
            }
            acyclic_harness::core::Reducer::restore(
                source.snapshot.clone(), source.owner.verifier()?, SchemaRegistry::default(),
            )?;
        }
        for publication in &config.native_publications {
            publication.validate()?;
        }
        acyclic_native_runtime::initialize_native_io_services()
            .map_err(|error| Error::Storage(format!("native IO service startup: {error}")))?;
        #[cfg(feature = "native-execution")]
        acyclic_fs::initialize_native_mount_services()
            .map_err(|error| Error::Storage(format!("native mount service startup: {error}")))?;
        let files = Arc::new(FilesystemHost::new(
            acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&config.filesystem_root)).await
                .map_err(|error| Error::Storage(error.to_string()))?,
            config.filesystem_provider.clone(),
        )?);
        let stream = Arc::new(
            LocalStream::open(&config.stream_root, Default::default()).await
                .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let source = super::native_tools::source(&config, stream.clone(), files.clone())?;
        if let Some(graph_root) = config.project_source_root {
            let policy = acyclic_harness::filesystem::FsRootProjectSourceProvider::new(
                files.clone(), &config.owner.authority, graph_root, None,
            )?;
            source.with_project_source_policy(
                AuthorityIssuer::new(&config.owner.issuer, config.owner.key, config.owner.authority.clone()),
                Arc::new(policy),
            )?;
        }
        let toolset = source.tools(MAX_FRAME_BYTES as u64)?;
        let machine = Arc::new(StockTurnMachine::new());
        let mut tasks = TaskRegistry::default();
        tasks.register(machine.definition()?)?;
        let preparation = Arc::new(ForkPreparationMachine::new());
        tasks.register(preparation.definition()?)?;
        let mailbox = Arc::new(MailboxModelMachine::default());
        tasks.register(TaskDefinition::<serde_json::Value, serde_json::Value>::resumable(
            mailbox.clone(), serde_json::json!({
                "type":"object","required":["receive","model"],
                "properties":{"receive":{"type":"object"},"model":{"type":"object"}},
                "additionalProperties":false,
            }), serde_json::json!({"type":"object"}),
        )?.requires("host")?.requires("model")?)?;
        let mut machines = MachineRegistry::default();
        machines.register(machine)?;
        machines.register(preparation)?;
        machines.register(mailbox)?;
        let scope = RuntimeScope::new(config.owner.scope.capabilities().clone(), config.limits)?
            .with_run_limits(config.run_limits)?;
        let creation = Arc::new(FilesystemResourceCreationProvider::new(files.clone(), &AuthorityIssuer::new(&config.owner.issuer, config.owner.key, config.owner.authority.clone()), #[cfg(not(target_arch = "wasm32"))] None)?);
        let runtime = Arc::new(Runtime::open_with_creation_providers(
            stream.clone(), files.clone(), config.private_volume.clone(), verifier,
            config.owner.scope.clone(), scope, tasks, machines, toolset.tools,
            config.session_limits, config.concurrency, MAX_FRAME_BYTES as u64,
            Arc::new(acyclic_stream::SystemUnixMillisClock), None, vec![creation], toolset.resumable_tools,
        ).await?.with_recursive_source(source.clone()));
        source.bind_runtime(&runtime)?;
        let model = super::provider::model(config.deterministic_delay_ms);
        let model_provider = Arc::new(super::provider::Deterministic::new(
            config.deterministic_delay_ms, &config.model_invocation_ledger,
        )?);
        let mut context = config.context.clone();
        if context.discovered.is_none() {
            if let Some(discovery) = &context.discovery {
                let reader = FilesystemContentVerifier::new(files.clone(), &config.owner.verifier()?,
                    &config.owner.scope, MAX_FRAME_BYTES as u64, None)?;
                context.discovered = Some(discovery.capture(&reader).await?);
            }
        }
        context.pipeline()?;
        Ok(Self {
            context: std::sync::RwLock::new(context),
            config, stream, files, runtime,
            model, model_provider,
        })
    }

    pub fn config(&self) -> Config {
        let mut config = self.config.clone();
        config.context = self.context.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        config
    }

    pub async fn configure_context(&self, mut context: WorkflowContext) -> Result<String> {
        if let Some(discovery) = &context.discovery {
            let reader = FilesystemContentVerifier::new(self.files.clone(), &self.config.owner.verifier()?,
                &self.config.owner.scope, MAX_FRAME_BYTES as u64, None)?;
            context.discovered = Some(discovery.capture(&reader).await?);
        }
        context.pipeline()?;
        let rendered = format!("context-json={}\n", serde_json::to_string(&context)
            .map_err(|error| Error::Invalid(error.to_string()))?);
        *self.context.write().map_err(|_| Error::Storage("context lock poisoned".into()))? = context;
        Ok(rendered)
    }

    pub async fn select_fork(&self, caller: TaskId, lease: &WorkLease, operation: OperationId,
        history: acyclic_harness::fork::ForkHistoryPolicy) -> Result<String> {
        self.caller_context(caller, lease).await?;
        let source = self.config.fork_sources.iter().find(|source|
            source.caller == caller && source.request.operation_id == operation)
            .ok_or_else(|| Error::NotFound("original fork source".into()))?;
        let reducer = acyclic_harness::core::Reducer::restore(source.snapshot.clone(),
            source.owner.verifier()?, SchemaRegistry::default())?;
        let request = source.request.clone().with_history_policy(&reducer, history, &mut None)?;
        Ok(format!("fork-selection-json={}\n", serde_json::to_string(&request)
            .map_err(|error| Error::Invalid(error.to_string()))?))
    }

    pub fn start_cursor(&self) -> wire::ReplayCursor {
        wire::ReplayCursor {
            authority: Some(encode_authority(&self.config.owner.authority)),
            generation: self.config.generation.clone(), revision: 0,
        }
    }

    fn binding(&self, authority: &wire::Authority) -> Result<&AuthorityBinding> {
        std::iter::once(&self.config.owner).chain(&self.config.observed_authorities)
            .find(|binding| encode_authority(&binding.authority) == *authority)
            .ok_or_else(|| Error::Unauthorized("authority is not in this native composition".into()))
    }

    /// Queue one typed stock task, inheriting from the actual retained parent context when selected.
    pub async fn queue(&self, operation: OperationId, command: WorkflowCommand,
        parent: Option<TaskId>) -> Result<Admission<RuntimeTask<TurnOutput>>>
    {
        let definition = self.runtime.harness().task("acyclic.stock_turn@1")?;
        if let Some(parent) = parent {
            let admission = self.runtime.task_host().observe_admission(parent).await?;
            self.runtime.harness().durable_context(parent, admission.operation_id).await?
                .admit(operation, &definition, command).await
        } else {
            self.runtime.harness().admit(operation, &definition, command, None).await
        }
    }

    /// Admit exact producer-authored preparation input under its retained caller.
    pub async fn queue_preparation(
        &self,
        operation: OperationId,
        parent: TaskId,
        caller_lease: &WorkLease,
        request: ResourcePreparationRequest,
    ) -> Result<Admission<RuntimeTask<ForkReport>>> {
        request.validate(&mut None)?;
        if request.child.parent != Some(parent) {
            return Err(Error::Unauthorized("preparation child belongs to a different caller".into()));
        }
        let definition = self.runtime.harness().task("acyclic.fork_preparation@1")?;
        let admission = self.runtime.task_host().observe_admission(parent).await?;
        if admission.operation_id != caller_lease.operation.operation_id {
            return Err(Error::Unauthorized("preparation lease belongs to a different admitted caller".into()));
        }
        self.runtime.task_context(parent, LeaseFence::from(&caller_lease.reservation)).await?
            .admit(operation, &definition, request).await
    }

    pub async fn prompt_command(&self, text: &str, operation: OperationId) -> Result<WorkflowCommand> {
        let bytes = serde_json::to_vec(&acyclic_harness::filesystem::ModelTaskCommand {
            input: ModelContent::Text(text.into()), selected_context: None,
            max_steps: 1, max_output_tokens: Some(256), mailbox_operation: None, selected_output_volume: None,
        }).map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(Error::Invalid("published prompt command exceeds terminal bounds".into()));
        }
        let publisher = FilesystemContentPublisher::new(
            self.files.clone(), &self.config.private_volume, &self.config.owner.verifier()?,
            &self.config.owner.scope, MAX_FRAME_BYTES as u64, None,
        )?;
        let payload = publisher.stage_once(operation, &format!("graphcoder-inputs/{operation}.json"),
            &bytes, "application/json", "GraphCoder prompt command").await?;
        Ok(WorkflowCommand { operation_id: operation,
            kind: acyclic_harness::filesystem::MODEL_TASK_COMMAND_KIND.into(), payload })
    }

    pub async fn claim(&self, task: TaskId, worker: &Worker) -> Result<WorkPull> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        self.runtime.poll_task_wake(task, None).await?;
        self.runtime.task_host().pull_operation(worker, admission.operation_id).await
    }

    pub async fn cancel_task(&self, task: TaskId) -> Result<String> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        self.runtime.task_host().cancel(task).await?;
        Ok(format!("cancellation requested task={task} admission-operation={}; completion requires authoritative observation\n", admission.operation_id))
    }

    pub async fn resume(&self, lease: WorkLease) -> Result<String> {
        let commands = self.runtime.commands().with_model(
            self.model.clone(), self.model_provider.clone(),
            self.context.read().map_err(|_| Error::Storage("context lock poisoned".into()))?.pipeline()?,
            Default::default(),
        );
        render_attempt(self.runtime.resume_task(lease, &commands, 16, None).await)
    }

    /// Resolve the caller through canonical admission and verify its actual retained fence before granting any scoped file/mail/workflow handle.
    pub async fn caller_context(&self, caller: TaskId, lease: &WorkLease) -> Result<TaskContext> {
        let (retained, _) = self.runtime.harness()
            .reconcile_admission(lease.operation.operation_id).await?
            .ok_or_else(|| Error::NotFound("original caller admission".into()))?;
        if retained != caller {
            return Err(Error::Unauthorized("original lease belongs to a different caller task".into()));
        }
        self.runtime.task_context(caller, LeaseFence::from(&lease.reservation)).await
    }

    /// Bind a conversation to the retained caller's authenticated allocation, never to a display label or a newly chosen child agent UUID.
    pub async fn caller_binding(&self, caller: TaskId) -> Result<&AuthorityBinding> {
        let inspection = self.runtime.inspect_source(caller, caller).await?;
        let agent = inspection.agent()?;
        let binding = self.config.fork_sources.iter().rev()
            .find(|source| source.caller == caller).map(|source| &source.owner)
            .or_else(|| self.config.tools.callers.iter().find(|source| source.task == caller).map(|source| &source.owner))
            .or_else(|| self.config.tools.forks.iter().find(|source| source.admission.task_id() == caller).map(|source| &source.child))
            .ok_or_else(|| Error::Unsupported("caller conversation provider binding unavailable".into()))?;
        binding.verifier()?;
        if binding.scope.agent() != Some(agent) {
            return Err(Error::Unauthorized("conversation binding differs from actual created caller".into()));
        }
        Ok(binding)
    }

    pub async fn queue_mailbox(
        &self, operation: OperationId, caller: TaskId, lease: &WorkLease,
        input: serde_json::Value,
    ) -> Result<Admission<RuntimeTask<serde_json::Value>>> {
        let definition = self.runtime.harness().task("acyclic.mailbox_model@1")?;
        self.caller_context(caller, lease).await?.admit(operation, &definition, input).await
    }

    /// Publish only an authoritative successful preparation and admit exactly its retained child, under the original caller fence and creator proofs.
    pub async fn publish_preparation(&self, caller: TaskId, lease: &WorkLease, preparation: TaskId) -> Result<String> {
        self.caller_context(caller, lease).await?;
        let admission = self.runtime.task_host().observe_admission(preparation).await?;
        if admission.parent != Some(caller) || admission.task.name != "acyclic.fork_preparation" {
            return Err(Error::Unauthorized("preparation does not belong to this original caller".into()));
        }
        let input: ResourcePreparationRequest = serde_json::from_value(admission.input)
            .map_err(|error| Error::Invalid(format!("retained preparation input: {error}")))?;
        input.validate(&mut None)?;
        let Some(acyclic_harness::Outcome::Succeeded(value)) = self.runtime.task_host().outcome(preparation).await? else {
            return Err(Error::Conflict("preparation has no authoritative successful report".into()));
        };
        let report: ForkReport = serde_json::from_value(value)
            .map_err(|error| Error::Invalid(format!("retained fork report: {error}")))?;
        report.validate()?;
        if input.child.parent != Some(caller)
            || serde_json::to_value(&report.request).map_err(|error| Error::Invalid(error.to_string()))? != input.payload
        {
            return Err(Error::Unauthorized("report changed the exact retained preparation/caller".into()));
        }
        let parent_binding = self.config.fork_sources.iter().find(|source| {
            source.caller == caller && source.owner.authority == report.request.parent
                && source.snapshot.revision == report.request.parent_revision
        }).ok_or_else(|| Error::Unsupported("original source publication binding unavailable".into()))?;
        let child_binding = self.config.observed_authorities.iter()
            .find(|binding| binding.authority == report.request.child)
            .ok_or_else(|| Error::Unsupported("explicit child conversation provider binding unavailable".into()))?;
        let content = Arc::new(FilesystemContentVerifier::new(
            self.files.clone(), &parent_binding.owner.verifier()?, &parent_binding.owner.scope,
            MAX_FRAME_BYTES as u64, None,
        )?);
        let mut parent = StreamAggregate::open_with_read_limits(
            &self.stream, parent_binding.owner.authority.clone(), parent_binding.owner.verifier()?,
            SchemaRegistry::default(), HistoryReadLimits { maximum_events: 256, maximum_bytes: 4 * 1024 * 1024 },
        ).await?.with_content_verifier(content)
            .with_fork_verifier(Arc::new(CompositeForkVerifier::new(&[
                Arc::new(FilesystemForkVerifier::new(self.files.clone(), MAX_FRAME_BYTES as u64)?),
                Arc::new(StreamHistoryForkVerifier::new(&self.config.stream_provider, &mut None)?),
            ], &mut None)?));
        let mut child = StreamAggregate::open_with_read_limits(
            &self.stream, child_binding.authority.clone(), child_binding.verifier()?,
            SchemaRegistry::default(), HistoryReadLimits { maximum_events: 256, maximum_bytes: 4 * 1024 * 1024 },
        ).await?;
        let seed = child.spawn_from_report(&mut parent, report,
            parent_binding.owner.scope.clone(), child_binding.scope.clone()).await?;
        let operation = input.child.operation_id;
        let admitted = self.runtime.task_host().admit_owned(input.child, LeaseFence::from(&lease.reservation)).await?;
        Ok(format!("fork-json={}\n", serde_json::json!({
            "preparation": preparation, "new_child_agent": seed.child_agent,
            "child_admission_operation": operation, "admission": admitted, "seed": seed,
        })))
    }

    pub async fn reconcile(&self, operation: OperationId) -> Result<String> {
        let found = self.runtime.harness().reconcile_admission(operation).await?;
        Ok(super::terminal::text(&format!("admission operation={operation} retained={found:?}\n")))
    }

    pub async fn children(&self, task: TaskId, revision: Option<u64>, after: Option<String>) -> Result<String> {
        let page = self.runtime.harness().children(task, revision, after, MAX_PAGE_EVENTS).await?;
        let encoded = serde_json::to_string(&page).map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(format!("{}children-json={encoded}\n",
            super::terminal::text(&format!("children parent-task={task} {page:?}\n"))))
    }

    pub async fn outcome(&self, task: TaskId) -> Result<String> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        let outcome = self.runtime.task_host().outcome(task).await?;
        let encoded = serde_json::to_string(&serde_json::json!({
            "task": task, "admission_operation": admission.operation_id, "outcome": outcome,
        })).map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(format!("{}task-json={encoded}\n", super::terminal::text(
            &format!("task={task} admission-operation={} outcome={outcome:?}\n", admission.operation_id))))
    }

}

pub fn lease_hex(lease: &WorkLease) -> Result<String> {
    Ok(hex::encode(serde_json::to_vec(lease)
        .map_err(|error| Error::Invalid(error.to_string()))?))
}

pub fn render_attempt(attempt: TaskWorkerAttempt) -> Result<String> {
    match attempt {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task }) => {
            Ok(format!("worker completed task={task}; observe /outcome for the authoritative result\n"))
        }
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended { task, revision }) => {
            Ok(format!("worker suspended task={task} checkpoint={revision}; no active future retained\n"))
        }
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) => {
            Ok(format!("worker yielded lease-json-hex={}\n", lease_hex(&lease)?))
        }
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) => {
            Ok(format!("worker reconciling lease-json-hex={}; no new work pulled\n", lease_hex(&lease)?))
        }
        TaskWorkerAttempt::Unresolved { lease, error } => {
            Ok(format!("worker unresolved lease-json-hex={} error={}\n",
                lease_hex(&lease)?, super::terminal::text(&error.to_string())))
        }
    }
}

impl HarnessWireApi for NativeSession {
    fn handshake<'a>(&'a self, request: wire::HandshakeRequest) -> BoxProviderFuture<'a, Result<wire::HandshakeResponse>> {
        Box::pin(async move { negotiate(&request, &wire::CapabilitySet::default()) })
    }

    fn submit<'a>(&'a self, envelope: wire::CommandEnvelope) -> BoxProviderFuture<'a, Result<wire::Admission>> {
        Box::pin(async move {
            let identity = envelope.operation.clone();
            let (authority, command) = decode_command_envelope(envelope)?;
            let wire_authority = encode_authority(&authority);
            let binding = self.binding(&wire_authority)?;
            let verifier = binding.verifier()?;
            let reader = Arc::new(FilesystemContentVerifier::new(
                self.files.clone(), &verifier, &binding.scope, MAX_FRAME_BYTES as u64, None,
            )?);
            let mut aggregate = StreamAggregate::open(&self.stream, authority, verifier, SchemaRegistry::new()).await?
                .with_content_verifier(reader)
                .with_fork_verifier(Arc::new(CompositeForkVerifier::new(&[
                    Arc::new(FilesystemForkVerifier::new(self.files.clone(), MAX_FRAME_BYTES as u64)?),
                    Arc::new(StreamHistoryForkVerifier::new(&self.config.stream_provider, &mut None)?),
                ], &mut None)?))
                .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(self.files.clone())));
            let (state, error) = match aggregate.execute(command, #[cfg(feature = "filesystem")] None, &mut None).await {
                Ok(_) => (wire::AdmissionState::Accepted, None),
                Err(error @ Error::Indeterminate(_)) => (wire::AdmissionState::Indeterminate, Some(acyclic_harness::encode_error(&error))),
                Err(error) => (wire::AdmissionState::Rejected, Some(acyclic_harness::encode_error(&error))),
            };
            Ok(wire::Admission { operation: identity, state: state as i32, error })
        })
    }

    fn replay<'a>(&'a self, request: wire::ResumeRequest) -> BoxProviderFuture<'a, Result<BoxProviderStream<'static, Result<wire::Delivery>>>> {
        Box::pin(async move {
            validate_resume_protocol(&request)?;
            if request.cursors.len() != 1 {
                return Err(Error::Invalid("terminal inspection requires one explicit aggregate cursor".into()));
            }
            let cursor = request.cursors.into_iter().next()
                .ok_or_else(|| Error::Invalid("cursor missing".into()))?;
            if cursor.generation != self.config.generation {
                return Err(Error::Conflict("native replay generation changed".into()));
            }
            let authority = cursor.authority.as_ref().ok_or_else(|| Error::Invalid("cursor authority missing".into()))?;
            let binding = self.binding(authority)?;
            let reader = HistoryReader::new(&self.stream, &binding.authority, binding.verifier()?)?;
            let pinned = reader.pin(cursor.revision, None).await?;
            let page = reader.read_page(&pinned, HistoryReadLimits {
                maximum_events: MAX_PAGE_EVENTS as u32,
                maximum_bytes: (MAX_FRAME_BYTES - 8192) as u64,
            }).await?;
            if page.events.is_empty() {
                return Ok(Box::pin(stream::empty()) as BoxProviderStream<'static, Result<wire::Delivery>>);
            }
            let events = page.events.iter().map(|event|
                encode_event_envelope(&binding.authority, event)
            ).collect::<Result<Vec<_>>>()?;
            let delivery = wire::Delivery {
                authority: cursor.authority, generation: cursor.generation,
                from_revision: cursor.revision, through_revision: page.cursor.after_revision,
                events, live: false,
            };
            Ok(Box::pin(stream::iter([Ok(delivery)])) as BoxProviderStream<'static, Result<wire::Delivery>>)
        })
    }

    fn authorize_operation_control<'a>(&'a self, request: &'a OperationControlRequest) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move { self.runtime.task_host().authorize_operation_control(request).await })
    }

    fn observe<'a>(&'a self, request: wire::ObserveRequest) -> BoxProviderFuture<'a, Result<wire::OperationStatus>> {
        Box::pin(async move {
            let control = validate_observe_request(&request)?;
            let state = self.runtime.task_host().observe_control(&control).await?;
            Ok(operation_status(&state))
        })
    }

    fn cancel<'a>(&'a self, request: wire::CancelRequest) -> BoxProviderFuture<'a, Result<wire::CancelResponse>> {
        Box::pin(async move {
            let control = validate_cancel_request(&request)?;
            let state = self.runtime.task_host().cancel_control(&control).await?;
            Ok(wire::CancelResponse {
                status: Some(operation_status(&state)),
                operation: Some(wire::OperationIdentity {
                    operation_id: control.control.operation_id.to_string(),
                    idempotency_key: control.idempotency_key.0,
                }),
            })
        })
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
