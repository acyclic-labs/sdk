//! Local providers, task control and the bounded terminal protocol.
#![allow(missing_docs, reason = "private binary composition")]
mod config;
mod fork;
mod tasks;
mod wire;
pub use config::{AuthorityBinding, Config, ForkSourceConfig, WorkflowContext};
pub use tasks::{lease_hex, render_attempt};

use crate::terminal::{MAX_FRAME_BYTES, MAX_PAGE_EVENTS};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness::{
    Admission, Error, OperationId, Result, TaskId,
    context::{
        Context, ContextAttribute, ContextDiscovery, ContextPipeline, ContextPlacement,
        ContextRenderMode, DiscoveredContext, SourceStage,
    },
    conversation::{Limits, VolumeRef},
    core::{Authority, AuthorityIssuer, AuthorityVerifier, SchemaRegistry, Scope, Snapshot},
    distributed::{WorkLease, WorkPull, Worker},
    executor::TurnOutput,
    filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemForkVerifier,
        FilesystemHost, FilesystemProjectMergeVerifier, FilesystemResourceCreationProvider,
        FilesystemTaskRuntime, ForkPreparationMachine, MailboxModelMachine, StockTurnMachine,
        StreamHistoryForkVerifier, TaskWorkerAttempt, TaskWorkerOutcome,
    },
    fork::{CompositeForkVerifier, ForkReport},
    model::{ModelContent, ModelMessage, ModelRole},
    resource_preparation::ResourcePreparationRequest,
    resources::ProviderRef,
    runtime::{
        RuntimeScope, RuntimeTask, TaskContext, TaskDefinition, TaskRegistry, TaskRunLimits,
        TaskStateProvider,
    },
    scheduler::{LeaseFence, SessionLimits},
    store::{HistoryReadLimits, HistoryReader, StreamAggregate},
};
use acyclic_stream::LocalStream;
use std::sync::Arc;

pub const HISTORY_LIMITS: HistoryReadLimits = HistoryReadLimits {
    maximum_events: 256,
    maximum_bytes: 4 * 1024 * 1024,
};

pub fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}

pub fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

/// One `KIND-json=VALUE` line the qualification scripts parse.
pub fn json_line(kind: &str, value: &impl serde::Serialize) -> Result<String> {
    Ok(format!(
        "{kind}-json={}\n",
        serde_json::to_string(value).map_err(invalid)?
    ))
}

/// Opens one conversation aggregate with the local content, fork and merge verifiers.
pub async fn open_aggregate(
    stream: &Arc<LocalStream>,
    files: &Arc<Files>,
    stream_provider: &ProviderRef,
    binding: &AuthorityBinding,
) -> Result<StreamAggregate<LocalStream>> {
    let verifier = binding.verifier()?;
    let content = FilesystemContentVerifier::new(
        files.clone(),
        &verifier,
        &binding.scope,
        MAX_FRAME_BYTES as u64,
        None,
    )?;
    let forks = CompositeForkVerifier::new(
        &[
            FilesystemForkVerifier::new(files.clone(), MAX_FRAME_BYTES as u64, None, None)?,
            StreamHistoryForkVerifier::new(stream_provider, &mut None)?,
        ],
        &mut None,
    )?;
    Ok(StreamAggregate::open(
        stream,
        binding.authority.clone(),
        verifier,
        SchemaRegistry::new(),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(content)
    .with_fork_verifier(forks)
    .with_merge_verifier(FilesystemProjectMergeVerifier::new(files.clone(), None)?))
}

pub(super) type Files = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
pub(super) type Runtime =
    FilesystemTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
pub struct NativeSession {
    pub(super) config: Config,
    pub(super) stream: Arc<LocalStream>,
    pub(super) files: Arc<Files>,
    pub(super) runtime: Arc<Runtime>,
    context: std::sync::RwLock<WorkflowContext>,
    model_provider: Arc<crate::provider::Deterministic>,
}
impl NativeSession {
    pub async fn open(mut config: Config) -> Result<Self> {
        let verifier = config.validate()?;
        acyclic_native_runtime::initialize_native_io_services()
            .map_err(|error| Error::Storage(format!("native IO service startup: {error}")))?;
        #[cfg(feature = "native-execution")]
        acyclic_fs::initialize_native_mount_services()
            .map_err(|error| Error::Storage(format!("native mount service startup: {error}")))?;
        let files = FilesystemHost::new(
            acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&config.filesystem_root))
                .await
                .map_err(storage)?,
            &config.filesystem_provider,
            None,
        )?;
        let stream = Arc::new(
            LocalStream::open(&config.stream_root, Default::default())
                .await
                .map_err(storage)?,
        );
        let runtime = runtime(&config, stream.clone(), files.clone(), verifier).await?;
        let model_provider = Arc::new(crate::provider::Deterministic::new(
            config.deterministic_delay_ms,
            &config.model_invocation_ledger,
        )?);
        let context = std::mem::take(&mut config.context);
        let session = Self {
            config,
            stream,
            files,
            runtime,
            context: std::sync::RwLock::new(WorkflowContext::default()),
            model_provider,
        };
        let context = if context.discovered.is_none() {
            session.discover(context).await?
        } else {
            context
        };
        context.pipeline()?;
        *session.context.write().map_err(storage)? = context;
        Ok(session)
    }
    async fn discover(&self, mut context: WorkflowContext) -> Result<WorkflowContext> {
        if let Some(discovery) = &context.discovery {
            let reader = FilesystemContentVerifier::new(
                self.files.clone(),
                &self.config.owner.verifier()?,
                &self.config.owner.scope,
                MAX_FRAME_BYTES as u64,
                None,
            )?;
            context.discovered = Some(discovery.capture(reader.as_ref()).await?);
        }
        Ok(context)
    }
    pub fn config(&self) -> Config {
        let mut config = self.config.clone();
        config.context = self
            .context
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        config
    }
    pub async fn configure_context(&self, context: WorkflowContext) -> Result<String> {
        let context = self.discover(context).await?;
        context.pipeline()?;
        let rendered = json_line("context", &context)?;
        *self.context.write().map_err(storage)? = context;
        Ok(rendered)
    }
}

use acyclic_harness::workflow::{MachineRegistry, WorkflowCommand};

pub(super) async fn runtime(
    config: &Config,
    stream: Arc<LocalStream>,
    files: Arc<Files>,
    verifier: AuthorityVerifier,
) -> Result<Arc<Runtime>> {
    let source = crate::native_tools::source(config, stream.clone(), files.clone())?;
    let issuer = AuthorityIssuer::new(
        &config.owner.issuer,
        config.owner.key,
        config.owner.authority.clone(),
    );
    let creation = Arc::new(FilesystemResourceCreationProvider::new(
        files.clone(),
        &issuer,
        #[cfg(not(target_arch = "wasm32"))]
        None,
    )?);
    if let Some(graph_root) = config.project_source_root {
        let policy = acyclic_harness::filesystem::FsRootProjectSourceProvider::new(
            files.clone(),
            &config.owner.authority,
            graph_root,
            None,
        )?;
        source.with_project_source_policy(issuer, Arc::new(policy))?;
    }
    let toolset = source.tools(MAX_FRAME_BYTES as u64)?;
    let stock = Arc::new(StockTurnMachine::new());
    let preparation = Arc::new(ForkPreparationMachine::new());
    let mailbox = Arc::new(MailboxModelMachine::default());
    let mut tasks = TaskRegistry::default();
    tasks.register(stock.definition()?)?;
    tasks.register(preparation.definition()?)?;
    tasks.register(
        TaskDefinition::<serde_json::Value, serde_json::Value>::resumable(
            mailbox.clone(),
            serde_json::json!({
                "type":"object","required":["receive","model"],
                "properties":{"receive":{"type":"object"},"model":{"type":"object"}},
                "additionalProperties":false,
            }),
            serde_json::json!({"type":"object"}),
        )?
        .requires("host")?
        .requires("model")?,
    )?;
    let mut machines = MachineRegistry::default();
    machines.register(stock, &mut None)?;
    machines.register(preparation, &mut None)?;
    machines.register(mailbox, &mut None)?;
    let scope = RuntimeScope::new(config.owner.scope.capabilities().clone(), config.limits)?
        .with_run_limits(config.run_limits, &mut None)?;
    let runtime = Arc::new(
        Runtime::open_with_creation_providers(
            stream,
            files,
            config.private_volume.clone(),
            verifier,
            config.owner.scope.clone(),
            scope,
            tasks,
            machines,
            toolset.tools,
            config.session_limits,
            config.concurrency,
            MAX_FRAME_BYTES as u64,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            None,
            vec![creation],
            toolset.resumable_tools,
        )
        .await?
        .with_recursive_source(source.clone()),
    );
    source.bind_runtime(&runtime)?;
    Ok(runtime)
}
#[cfg(test)]
#[path = "../native_tests.rs"]
mod tests;
