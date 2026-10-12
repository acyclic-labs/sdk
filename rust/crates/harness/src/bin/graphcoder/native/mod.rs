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
        let files = Arc::new(FilesystemHost::new(
            acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&config.filesystem_root))
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
            config.filesystem_provider.clone(),
        )?);
        let stream = Arc::new(
            LocalStream::open(&config.stream_root, Default::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let source = crate::native_tools::source(&config, stream.clone(), files.clone())?;
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
                stream.clone(),
                files.clone(),
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
        let model_provider = Arc::new(crate::provider::Deterministic::new(
            config.deterministic_delay_ms,
            &config.model_invocation_ledger,
        )?);
        let mut context = std::mem::take(&mut config.context);
        if context.discovered.is_none() {
            if let Some(discovery) = &context.discovery {
                let reader = FilesystemContentVerifier::new(
                    files.clone(),
                    &config.owner.verifier()?,
                    &config.owner.scope,
                    MAX_FRAME_BYTES as u64,
                    None,
                )?;
                context.discovered = Some(discovery.capture(&reader).await?);
            }
        }
        context.pipeline()?;
        Ok(Self {
            config,
            stream,
            files,
            runtime,
            context: std::sync::RwLock::new(context),
            model_provider,
        })
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
    pub async fn configure_context(&self, mut context: WorkflowContext) -> Result<String> {
        if let Some(discovery) = &context.discovery {
            let reader = FilesystemContentVerifier::new(
                self.files.clone(),
                &self.config.owner.verifier()?,
                &self.config.owner.scope,
                MAX_FRAME_BYTES as u64,
                None,
            )?;
            context.discovered = Some(discovery.capture(&reader).await?);
        }
        context.pipeline()?;
        let rendered = format!(
            "context-json={}\n",
            serde_json::to_string(&context).map_err(|error| Error::Invalid(error.to_string()))?
        );
        *self
            .context
            .write()
            .map_err(|_| Error::Storage("context lock poisoned".into()))? = context;
        Ok(rendered)
    }
}

use acyclic_harness::workflow::{MachineRegistry, WorkflowCommand};
#[cfg(test)]
#[path = "../native_tests.rs"]
mod tests;
