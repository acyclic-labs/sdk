use super::*;

struct Opening {
    config: LocalHarnessConfig,
    state_directory: PathBuf,
    tools: ToolRegistry,
    compaction: crate::context::CompactionPolicy,
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    settings: Arc<crate::native_settings::SettingsConfig>,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    capabilities: Capabilities,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    diagnostics: Vec<crate::mcp::McpServerFailure>,
    #[cfg(feature = "native-execution")]
    native_mounts: bool,
}

struct Storage {
    directory: PathBuf,
    owner: owner::Owner,
    files: Arc<LocalFilesystem>,
    scratch: VolumeRef,
    journal: VolumeRef,
    task_volume: VolumeRef,
    #[cfg(feature = "native-execution")]
    workspace: Option<Arc<workspace::LocalWorkspace>>,
}

pub(super) async fn open(config: LocalHarnessConfig) -> Result<LocalHarness> {
    let opening = Opening::configure(config).await?;
    let storage = opening.storage().await?;
    opening.finish(storage).await
}

impl Opening {
    async fn configure(config: LocalHarnessConfig) -> Result<Self> {
        config.limits.validate(&mut None)?;
        config.session_limits.validate(&mut None)?;
        if config.concurrency == 0 {
            return Err(Error::Invalid("local concurrency is zero".into()));
        }
        let mut config = config.prepare_state()?;
        let state_directory = config.resolved_state_directory()?;
        config.model.0.validate()?;
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
        for tool in std::mem::take(&mut config.tools) {
            tools.register((tool, None), &mut None, None)?;
        }
        #[cfg(feature = "search-http")]
        if config.web_tools {
            web::WebExecutor::bind(&mut tools, config.search_provider.take())?;
        }
        #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
        let settings_configuration =
            settings::configuration(&config.project, config.settings.take(), config.hermetic)?;
        #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
        let mcp =
            crate::native_settings::NativeSettings::load_mcp(Arc::clone(&settings_configuration))
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
            config.compaction.take(),
            &config.model.0,
            config.model.1.as_ref(),
            &tools,
            config.limits,
        )
        .await?;
        #[cfg(feature = "native-execution")]
        let native_mounts = tools
            .get(crate::tools::shell::PLATFORM.name, None, None)?
            .is_some();
        #[cfg(feature = "native-execution")]
        if native_mounts {
            checkout_tools::shell::executable()?;
        }
        Ok(Self {
            config,
            state_directory,
            tools,
            compaction,
            #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
            settings: settings_configuration,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            capabilities: installation.capabilities,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            diagnostics: installation.diagnostics,
            #[cfg(feature = "native-execution")]
            native_mounts,
        })
    }

    async fn storage(&self) -> Result<Storage> {
        acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
        #[cfg(feature = "native-execution")]
        acyclic_fs::watch::initialize_native_watch_services().map_err(storage)?;
        #[cfg(feature = "native-execution")]
        if self.native_mounts {
            workspace::volumes::require_available(acyclic_fs::probe_native_mount())?;
            acyclic_fs::initialize_native_mount_services().map_err(storage)?;
        }
        #[cfg(not(feature = "native-execution"))]
        if matches!(self.config.project, LocalProject::Workspace(_)) {
            return Err(Error::Unsupported(
                "checkout workspaces require native-execution".into(),
            ));
        }
        if let LocalProject::Workspace(path) = &self.config.project
            && !path.is_dir()
        {
            return Err(Error::Invalid("local workspace is not a directory".into()));
        }
        let checkout = match &self.config.project {
            LocalProject::Workspace(path) => Some(path.as_path()),
            LocalProject::Volume { .. } => None,
        };
        let directory = owner::separated_directory(&self.state_directory, checkout)?;
        let owner = owner::Owner::open(&directory, &self.config.project)?;
        let options = acyclic_fs::LocalOptions::new(match &self.config.project {
            LocalProject::Workspace(_) => directory.join("filesystem"),
            LocalProject::Volume { directory, .. } => directory.clone(),
        });
        let state_owner = self.config.test_state.clone();
        let filesystem = match &state_owner {
            Some(owner) if matches!(&self.config.project, LocalProject::Workspace(_)) => {
                acyclic_fs::Fs::local_with_ownership_anchor(options, owner.clone()).await
            }
            _ => acyclic_fs::Fs::local(options).await,
        }
        .map_err(storage)?;
        let provider_ref = owner.project.provider();
        let files = FilesystemHost::new(filesystem.clone(), provider_ref, None)?;
        let PrivateVolumes {
            scratch,
            journal,
            task_volume,
        } = PrivateVolumes::open(&files, &owner).await?;
        #[cfg(feature = "native-execution")]
        let workspace = self
            .workspace(&filesystem, &directory, &owner, &scratch)
            .await?;
        #[cfg(feature = "native-execution")]
        let files = match &workspace {
            Some(workspace) => files
                .with_project_family(workspace.view().workspace().id(), workspace.store.clone())?,
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
        let files = retain_skills(
            files,
            directory.join("skill-sources"),
            &journal,
            workspace.as_ref(),
            self.user_roots(),
        )?;
        Ok(Storage {
            directory,
            owner,
            files,
            scratch,
            journal,
            task_volume,
            #[cfg(feature = "native-execution")]
            workspace,
        })
    }

    #[cfg(feature = "native-execution")]
    async fn workspace(
        &self,
        filesystem: &acyclic_fs::LocalFs,
        directory: &Path,
        owner: &owner::Owner,
        scratch: &VolumeRef,
    ) -> Result<Option<Arc<workspace::LocalWorkspace>>> {
        // This standalone composition owns its private companion namespace.
        // Retain the existing owned WAL instead of reopening/checkpointing it
        // for every transaction. Embedded bind() retains shared-store semantics.
        let store = acyclic_fs::LocalCoreStateStore::open_shared_owner(directory.join("workspace"))
            .map_err(storage)?
            .with_ownership_anchor(self.config.test_state.clone());
        // This process now holds the state exclusively. Before anything
        // reads the checkout or records an interruption, every tool tree a
        // dead previous owner left is dead; owners in this process share it.
        let state = workspace::OwnerState {
            store,
            processes: Some(fence_stale_processes(directory.join("processes")).await?),
        };
        let workspace = match &self.config.project {
            LocalProject::Workspace(path) => Some(
                workspace::open(
                    filesystem,
                    &owner.project,
                    scratch,
                    path,
                    directory,
                    owner.workspace_identity,
                    workspace::attach_operation(owner.task),
                    false,
                    self.native_mounts,
                    state,
                )
                .await?,
            ),
            LocalProject::Volume { .. } => Some(
                workspace::open_volume(
                    filesystem,
                    &owner.project,
                    scratch,
                    directory,
                    workspace::attach_operation(owner.task),
                    self.native_mounts,
                    state,
                )
                .await?,
            ),
        };
        Ok(workspace.map(Arc::new))
    }

    #[cfg(feature = "native-execution")]
    fn user_roots(&self) -> Vec<PathBuf> {
        #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
        {
            self.settings.skill_roots().to_vec()
        }
        #[cfg(not(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")))]
        {
            Vec::new()
        }
    }

    async fn finish(self, opened: Storage) -> Result<LocalHarness> {
        let Storage {
            directory,
            owner,
            files,
            scratch,
            journal,
            task_volume,
            #[cfg(feature = "native-execution")]
            workspace,
        } = opened;
        let stream = Arc::new(
            LocalStream::open_with_clock_and_anchor(
                directory.join("stream"),
                Default::default(),
                self.config.clock.clone(),
                self.config.test_state,
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
            tools: &self.tools,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            capabilities: &self.capabilities,
            limits: self.config.limits,
            run_limits: self.config.run_limits,
            session_limits: self.config.session_limits,
            concurrency: self.config.concurrency,
            clock: Arc::clone(&self.config.clock),
        })
        .await?;
        let local = LocalHarness {
            files,
            runtime,
            task_files,
            project: owner.project.clone(),
            scratch,
            journal,
            verifier,
            issuer: Some(issuer),
            signed,
            model: self.config.model.0,
            provider: self.config.model.1,
            context_manifest: self.config.context_manifest,
            skill_cache: context::skills::cache(),
            #[cfg(test)]
            context_read_observer: None,
            #[cfg(feature = "native-execution")]
            checkout_context: matches!(self.config.project, LocalProject::Workspace(_)),
            #[cfg(feature = "hooks")]
            settings: self.settings,
            limits: self.config.limits,
            compaction: self.compaction,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            mcp_diagnostics: self.diagnostics,
            goal_continuations: self.config.goal_continuations,
            admission: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
            publication: crate::sync::owned::Arc::new(crate::sync::owned::Mutex::new(())),
            drivers: orphans::Drivers::new(),
            #[cfg(feature = "native-execution")]
            workspace,
            #[cfg(all(test, feature = "native-execution"))]
            environment_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(feature = "native-execution")]
            clock: self.config.clock,
            #[cfg(feature = "native-execution")]
            environment_platform: self.config.environment_platform,
        };
        Ok(local)
    }
}

#[cfg(feature = "native-execution")]
pub(super) fn retain_skills(
    files: Arc<LocalFilesystem>,
    directory: PathBuf,
    journal: &VolumeRef,
    workspace: Option<&Arc<workspace::LocalWorkspace>>,
    user_roots: Vec<PathBuf>,
) -> Result<Arc<LocalFilesystem>> {
    files.with_native_skills(crate::context::native_skills::NativeSkills::new(
        directory,
        journal.clone(),
        workspace.map(|workspace| {
            (
                workspace.view(),
                workspace.filesystem.clone(),
                workspace.store.clone(),
            )
        }),
        user_roots,
    )?)
}

struct PrivateVolumes {
    scratch: VolumeRef,
    journal: VolumeRef,
    task_volume: VolumeRef,
}
impl PrivateVolumes {
    async fn open(files: &LocalFilesystem, owner: &owner::Owner) -> Result<Self> {
        let scratch = VolumeRef::new(
            owner.project.provider().clone(),
            format!("scratch-{}", owner.agent),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner.agent),
            &mut None,
        )?;
        let journal = VolumeRef::new(
            owner.project.provider().clone(),
            format!("journal-{}", owner.agent),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner.agent),
            &mut None,
        )?;
        let task_volume = VolumeRef::new(
            owner.project.provider().clone(),
            format!("tasks-{}", owner.agent),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner.agent),
            &mut None,
        )?;
        files.create_volume(&task_volume).await?;
        files.create_volume(&scratch).await?;
        files.create_volume(&journal).await?;
        Ok(Self {
            scratch,
            journal,
            task_volume,
        })
    }
}

/// This owner's process custody. The first owner of the directory in this
/// process kills every tool tree a dead previous owner left, awaiting each
/// tree's exit; a later owner in the same process shares the live custody.
#[cfg(feature = "native-execution")]
async fn fence_stale_processes(
    path: std::path::PathBuf,
) -> Result<Arc<acyclic_native_runtime::ProcessCustody>> {
    tokio::task::spawn_blocking(move || acyclic_native_runtime::ProcessCustody::acquire(&path))
        .await
        .map_err(storage)?
        .map_err(storage)
}
