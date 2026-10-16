//! Durable native defaults over the public filesystem and task runtime.

use crate::{
    Admission, AgentId, Capabilities, Error, OperationId, Result, TaskId,
    context::ContextPipeline,
    conversation::{
        ContentPublisher, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer, AuthorityVerifier, Scope},
    filesystem::{
        FilesystemContentPublisher, FilesystemHost, FilesystemResourceCreationProvider,
        FilesystemTaskRuntime, ForkPreparationMachine, StockTurnMachine,
    },
    model::{Model, ModelContent, ModelProvider},
    runtime::{RuntimeScope, RuntimeTask, TaskRegistry, TaskRunLimits},
    scheduler::SessionLimits,
    tool::{Tool, ToolRegistry},
    workflow::{MachineRegistry, WorkflowCommand},
};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_stream::LocalStream;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

mod admission;
#[cfg(feature = "native-execution")]
mod checkout_tools;
mod compaction;
mod config;
#[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
mod mcp;
mod owner;
mod owner_record;
mod runtime;
#[cfg(test)]
mod tests;
mod turn;
#[cfg(feature = "native-execution")]
mod workspace;
pub use config::LocalHarnessConfig;
use config::LocalProject;
#[cfg(feature = "native-execution")]
pub use workspace::LocalWorkspace;

/// Local durable filesystem host.
pub type LocalFilesystem = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
/// Local durable task host and its execution journals.
pub(crate) type LocalTaskRuntime =
    FilesystemTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

/// A checkout or volume, durable authority, journals and stock executor.
pub struct LocalHarness {
    files: Arc<LocalFilesystem>,
    runtime: Arc<LocalTaskRuntime>,
    task_files: Arc<crate::task_volume::TaskFiles<LocalStream>>,
    project: VolumeRef,
    scratch: VolumeRef,
    journal: VolumeRef,
    verifier: AuthorityVerifier,
    signed: Scope,
    model: Model,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    limits: Limits,
    compaction: crate::context::CompactionPolicy,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    mcp_diagnostics: Vec<crate::mcp::McpServerFailure>,
    goal_continuations: Option<crate::goal::GoalContinuationPolicy>,
    // Outermost lock: admission -> filesystem/provider locks. Never held during
    // model/tool execution, and provider callbacks never acquire admission.
    admission: crate::sync::owned::Arc<crate::sync::owned::Mutex<()>>,
    #[cfg(feature = "native-execution")]
    workspace: Option<Arc<workspace::LocalWorkspace>>,
}

impl LocalHarness {
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn task_stream(&self) -> Arc<LocalStream> {
        self.runtime.task_host().workflow_stream()
    }

    /// Opens durable local state and assembles the stock host before admitting work.
    pub fn open(config: LocalHarnessConfig) -> impl Future<Output = Result<Self>> {
        Box::pin(async move {
            config.limits.validate(&mut None)?;
            config.session_limits.validate(&mut None)?;
            if config.concurrency == 0 {
                return Err(Error::Invalid("local concurrency is zero".into()));
            }
            let (model, provider) = config.model;
            let context = config.context;
            model.validate()?;
            let mut tools = ToolRegistry::default();
            if config.default_tools {
                crate::tools::bind_defaults(
                    &mut tools,
                    &mut Vec::new(),
                    None,
                    true,
                    cfg!(feature = "native-execution"),
                )?;
            }
            for tool in config.tools {
                tools.register((tool, None), &mut None, None)?;
            }
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            let installation = mcp::install(
                &config.project,
                |path| config.settings.map_or_else(|| mcp::user_settings(path), Ok),
                tools,
                config.mcp_limits,
            )
            .await?;
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            let tools = installation.tools;
            let compaction = compaction::select(
                config.compaction,
                &model,
                provider.as_ref(),
                &tools,
                config.limits,
            )?;
            acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
            #[cfg(feature = "native-execution")]
            acyclic_fs::initialize_native_mount_services().map_err(storage)?;
            #[cfg(not(feature = "native-execution"))]
            if matches!(config.project, LocalProject::Workspace(_)) {
                return Err(Error::Unsupported(
                    "checkout workspaces require native-execution".into(),
                ));
            }
            if let LocalProject::Workspace(path) = &config.project {
                if !path.is_dir() {
                    return Err(Error::Invalid("local workspace is not a directory".into()));
                }
            }
            let directory = match config.state_directory {
                Some(path) => path,
                None => owner::state_directory(&config.project)?,
            };
            let directory = owner::directory(&directory)?;
            let owner = owner::Owner::open(&directory, &config.project)?;
            let filesystem =
                acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(match &config.project {
                    LocalProject::Workspace(_) => directory.join("filesystem"),
                    LocalProject::Volume { directory, .. } => directory.clone(),
                }))
                .await
                .map_err(storage)?;
            let provider_ref = owner.project.provider();
            let files = FilesystemHost::new(filesystem.clone(), provider_ref, None)?;
            let scratch = VolumeRef::new(
                provider_ref.clone(),
                format!("scratch-{}", owner.agent),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(owner.agent),
                &mut None,
            )?;
            let journal = VolumeRef::new(
                provider_ref.clone(),
                format!("journal-{}", owner.agent),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(owner.agent),
                &mut None,
            )?;
            let task_volume = VolumeRef::new(
                provider_ref.clone(),
                format!("tasks-{}", owner.agent),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(owner.agent),
                &mut None,
            )?;
            files.create_volume(&task_volume).await?;
            files.create_volume(&scratch).await?;
            files.create_volume(&journal).await?;
            #[cfg(feature = "native-execution")]
            let workspace = match &config.project {
                LocalProject::Workspace(path) => Some(
                    workspace::open(
                        &filesystem,
                        &owner.project,
                        &scratch,
                        path,
                        &directory,
                        owner.workspace_identity,
                        workspace::attach_operation(owner.task),
                        false,
                    )
                    .await?,
                ),
                LocalProject::Volume { .. } => Some(
                    workspace::open_volume(
                        &filesystem,
                        &owner.project,
                        &scratch,
                        &directory,
                        workspace::attach_operation(owner.task),
                    )
                    .await?,
                ),
            };
            #[cfg(feature = "native-execution")]
            let files = match &workspace {
                Some(workspace) => files.with_project_family(
                    workspace.view().workspace().id(),
                    workspace.store.clone(),
                )?,
                None => files,
            };
            // A configured project is opened, never silently replaced by an empty volume.
            files
                .resolve(
                    &crate::filesystem::workspace_ref(
                        provider_ref.clone(),
                        &owner.project.storage_name(&mut None)?,
                    )?,
                    None,
                )
                .await?;
            #[cfg(feature = "native-execution")]
            let workspace = workspace.map(Arc::new);
            #[cfg(feature = "native-execution")]
            let context = match &workspace {
                Some(workspace) => context.with(workspace.context_stage()),
                None => context,
            };
            let stream = Arc::new(
                LocalStream::open(directory.join("stream"), Default::default())
                    .await
                    .map_err(storage)?,
            );
            let issuer = AuthorityIssuer::new(
                "acyclic.local",
                owner.key,
                Authority {
                    kind: AggregateKind::Task,
                    id: owner.task.to_string(),
                },
            );
            let verifier = issuer.verifier(&mut None)?;
            let (runtime, signed, task_files) = runtime::open(runtime::Configuration {
                files: files.clone(),
                #[cfg(feature = "native-execution")]
                workspace: workspace.as_ref().map(Arc::clone),
                stream,
                task_volume: &task_volume,
                issuer: &issuer,
                verifier: &verifier,
                agent: owner.agent,
                project: &owner.project,
                scratch: &scratch,
                journal: &journal,
                tools: &tools,
                #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
                capabilities: &installation.capabilities,
                limits: config.limits,
                run_limits: config.run_limits,
                session_limits: config.session_limits,
                concurrency: config.concurrency,
                clock: config.clock,
            })
            .await?;
            Ok(Self {
                files,
                runtime,
                task_files,
                project: owner.project.clone(),
                scratch,
                journal,
                verifier,
                signed,
                model,
                provider,
                context,
                limits: config.limits,
                compaction,
                #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
                mcp_diagnostics: installation.diagnostics,
                goal_continuations: config.goal_continuations,
                admission: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
                #[cfg(feature = "native-execution")]
                workspace,
            })
        })
    }

    /// Trusted host's task-file volume; clients render its bounded live queries.
    pub fn task_files(&self) -> &Arc<crate::task_volume::TaskFiles<LocalStream>> {
        &self.task_files
    }

    /// Durable content and project host.
    #[must_use]
    pub const fn filesystem(&self) -> &Arc<LocalFilesystem> {
        &self.files
    }
    /// Selected project view.
    #[must_use]
    pub const fn project(&self) -> &VolumeRef {
        &self.project
    }
    /// Agent-owned scratch content, distinct from host journal storage.
    #[must_use]
    pub const fn scratch(&self) -> &VolumeRef {
        &self.scratch
    }
    /// Host-only journal storage.
    #[must_use]
    pub const fn journal_volume(&self) -> &VolumeRef {
        &self.journal
    }
    /// Complete enabled tool registry.
    #[must_use]
    pub fn tools(&self) -> &ToolRegistry {
        self.runtime.tool_registry()
    }

    /// Optional configured servers that failed startup; required failures reject opening.
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    #[must_use]
    pub fn mcp_diagnostics(&self) -> &[crate::mcp::McpServerFailure] {
        &self.mcp_diagnostics
    }

    /// Canonical checkout path, absent for a configured volume.
    #[cfg(feature = "native-execution")]
    #[must_use]
    pub fn workspace_path(&self) -> Option<&Path> {
        self.workspace
            .as_ref()
            .map(|workspace| workspace.path.as_path())
    }
    /// Source-backed checkout view, absent for configured volumes.
    #[cfg(feature = "native-execution")]
    #[must_use]
    pub fn workspace(&self) -> Option<&Arc<LocalWorkspace>> {
        self.workspace.as_ref()
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
