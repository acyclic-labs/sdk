//! Durable native defaults over the public filesystem and task runtime.

use crate::{
    Admission, AgentId, Capabilities, Error, OperationId, Result, TaskId,
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
mod bind;
#[cfg(feature = "native-execution")]
mod checkout_tools;
mod children;
mod command_machine;
mod command_views;
mod commands;
mod compact;
mod compaction;
mod config;
mod context;
mod files;
mod inheritance;
mod owner;
mod owner_record;
mod routing;
mod runtime;
#[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
mod settings;
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
pub type LocalTaskRuntime =
    FilesystemTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

/// A checkout or volume, durable authority, journals and stock executor.
pub struct LocalHarness {
    files: Arc<LocalFilesystem>,
    runtime: Arc<LocalTaskRuntime>,
    task_files: Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>,
    project: VolumeRef,
    scratch: VolumeRef,
    journal: VolumeRef,
    verifier: AuthorityVerifier,
    issuer: Option<AuthorityIssuer>,
    signed: Scope,
    model: Model,
    /// Unbound: each model command binds it to its executing task's reader.
    provider: Arc<dyn ModelProvider>,
    context_manifest: Option<crate::conversation::FileRef>,
    #[cfg(feature = "hooks")]
    settings: Arc<crate::native_settings::SettingsConfig>,
    #[cfg(test)]
    context_read_observer:
        Option<for<'a> fn(&'a Self, &'a str) -> acyclic_stream::BoxProviderFuture<'a, Result<()>>>,
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
    #[cfg(feature = "native-execution")]
    checkout_context: bool,
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
            let config = config.prepare_state()?;
            let state_directory = config.resolved_state_directory()?;
            let (model, provider) = config.model;
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
            #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
            let settings_configuration =
                settings::configuration(&config.project, config.settings, config.hermetic)?;
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            let mcp = crate::native_settings::NativeSettings::load_mcp(Arc::clone(
                &settings_configuration,
            ))
            .await?;
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            let mcp_workspace =
                std::fs::canonicalize(config.project.root()).map_err(Error::process_io)?;
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            let installation = mcp
                .install(tools, &mcp_workspace, config.mcp_limits)
                .await?;
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            let tools = installation.tools;
            let compaction = compaction::select(
                config.compaction,
                &model,
                provider.as_ref(),
                &tools,
                config.limits,
            )
            .await?;
            #[cfg(feature = "native-execution")]
            let native_mounts = tools.get("bash", None, None)?.is_some();
            acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
            #[cfg(feature = "native-execution")]
            if native_mounts {
                workspace::volumes::require_available(acyclic_fs::probe_native_mount())?;
                acyclic_fs::initialize_native_mount_services().map_err(storage)?;
            }
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
            let checkout = match &config.project {
                LocalProject::Workspace(path) => Some(path.as_path()),
                LocalProject::Volume { .. } => None,
            };
            let directory = owner::separated_directory(&state_directory, checkout)?;
            let owner = owner::Owner::open(&directory, &config.project)?;
            let options = acyclic_fs::LocalOptions::new(match &config.project {
                LocalProject::Workspace(_) => directory.join("filesystem"),
                LocalProject::Volume { directory, .. } => directory.clone(),
            });
            let state_owner = config.test_state;
            let filesystem = match &state_owner {
                Some(owner) if matches!(&config.project, LocalProject::Workspace(_)) => {
                    acyclic_fs::Fs::local_with_ownership_anchor(options, owner.clone()).await
                }
                _ => acyclic_fs::Fs::local(options).await,
            }
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
            // This standalone composition owns its private companion namespace.
            // Retain the existing owned WAL instead of reopening/checkpointing it
            // for every transaction. Embedded bind() retains shared-store semantics.
            #[cfg(feature = "native-execution")]
            let store =
                acyclic_fs::LocalCoreStateStore::open_shared_owner(directory.join("workspace"))
                    .map_err(storage)?
                    .with_ownership_anchor(state_owner.clone());
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
                        native_mounts,
                        store,
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
                        native_mounts,
                        store,
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
            let stream = Arc::new(
                LocalStream::open_with_clock_and_anchor(
                    directory.join("stream"),
                    Default::default(),
                    config.clock.clone(),
                    state_owner,
                )
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
            let local = Self {
                files,
                runtime,
                task_files,
                project: owner.project.clone(),
                scratch,
                journal,
                verifier,
                issuer: Some(issuer),
                signed,
                model,
                provider,
                context_manifest: config.context_manifest,
                #[cfg(test)]
                context_read_observer: None,
                #[cfg(feature = "native-execution")]
                checkout_context: matches!(config.project, LocalProject::Workspace(_)),
                #[cfg(feature = "hooks")]
                settings: settings_configuration,
                limits: config.limits,
                compaction,
                #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
                mcp_diagnostics: installation.diagnostics,
                goal_continuations: config.goal_continuations,
                admission: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
                #[cfg(feature = "native-execution")]
                workspace,
            };
            Ok(local)
        })
    }

    /// Trusted host's task-file volume; clients render its bounded live queries.
    pub fn task_files(&self) -> &Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>> {
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
