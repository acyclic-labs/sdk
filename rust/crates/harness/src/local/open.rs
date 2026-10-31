use super::*;

struct Opening {
    config: LocalHarnessConfig,
    /// The agents' defaults, from which their tools and turns are built.
    preset: crate::preset::Preset,
    state_location: config::StateLocation,
    tools: ToolRegistry,
    compaction: crate::context::CompactionPolicy,
    #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    settings: Option<Arc<crate::native_settings::SettingsConfig>>,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    capabilities: Capabilities,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    diagnostics: Vec<crate::mcp::McpServerFailure>,
    #[cfg(any(test, feature = "native-execution"))]
    native_mounts: bool,
}

struct Storage {
    issuer: AuthorityRoot,
    owner: owner::Owner,
    files: Arc<LocalFilesystem>,
    #[cfg(all(target_os = "linux", feature = "native-execution"))]
    volume_filesystem: acyclic_fs::LocalFs,
    /// The Stream provider holding the filesystem's volume authority.
    volume_stream: Arc<LocalStream>,
    scratch: VolumeRef,
    journal: VolumeRef,
    #[cfg(any(test, feature = "native-execution"))]
    workspace: Option<Arc<workspace::LocalWorkspace>>,
}

pub(crate) async fn open(config: LocalHarnessConfig) -> Result<LocalHarness> {
    let opening = Opening::configure(config).await?;
    let storage = opening.storage().await?;
    opening.finish(storage).await
}

impl Opening {
    async fn configure(config: LocalHarnessConfig) -> Result<Self> {
        validate_configuration(&config)?;
        // Settings resolve once, before the model and approvers they select.
        #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
        let (config, settings_configuration) = {
            let mut config = config;
            if config.without_user_settings {
                config.settings = None;
                (config.resolve(None, true)?, None)
            } else if config.model.is_none() && config.models.is_none() {
                (config, None)
            } else {
                let injected = config.settings.is_some();
                let settings = settings::configuration(
                    &config.project,
                    config.settings.take(),
                    config.hermetic,
                )?;
                (config.resolve(Some(&settings), injected)?, Some(settings))
            }
        };
        if config.approval_issuer.is_some() {
            return Err(Error::Invalid(
                "approval_authority belongs to an embedded runtime binding".into(),
            ));
        }
        config.approval_policy.bind()?;
        for approver in &config.approval_policy.approvers {
            if let crate::approvals::Approver::Llm { model, .. } = approver {
                config
                    .approval_models
                    .get(model)
                    .ok_or_else(|| {
                        Error::Invalid(format!("approver model {model} is not independently bound"))
                    })?
                    .0
                    .validate()?;
            }
        }
        let mut config = config.prepare_state()?;
        let state_location = config.state_location()?;
        let passive = config.model.is_none();
        if passive && (!config.default_tools.is_empty() || !config.tools.is_empty()) {
            return Err(Error::Invalid(
                "native host coordination cannot register tools".into(),
            ));
        }
        if let Some((model, _)) = &config.model {
            model.validate()?;
        }
        let preset = config.preset_value();
        let tools = configure_tools(&preset, &mut config)?;
        #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
        let installation = match &settings_configuration {
            Some(settings) => {
                let mcp =
                    crate::native_settings::NativeSettings::load_mcp(Arc::clone(settings)).await?;
                let root =
                    std::fs::canonicalize(config.project.root()).map_err(Error::process_io)?;
                mcp.install(
                    tools,
                    &root,
                    config.mcp_limits,
                    Arc::new(crate::secrets::EnvSecrets),
                )
                .await?
            }
            None => crate::mcp::McpInstallation {
                tools,
                capabilities: Capabilities::default(),
                diagnostics: Vec::new(),
            },
        };
        #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
        let tools = installation.tools;
        let compaction = if let Some((model, provider)) = &config.model {
            compaction::select(
                config.compaction.take(),
                model,
                provider.as_ref(),
                &tools,
                config.limits,
            )
            .await?
        } else {
            crate::context::CompactionPolicy::Disabled
        };
        #[cfg(any(test, feature = "native-execution"))]
        let native_mounts = passive
            || tools
                .get(crate::tools::shell::PLATFORM.name, None, None)?
                .is_some();
        #[cfg(any(test, feature = "native-execution"))]
        if native_mounts && !passive {
            checkout_tools::shell::executable()?;
        }
        Ok(Self {
            config,
            preset,
            state_location,
            tools,
            compaction,
            #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
            settings: settings_configuration,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            capabilities: installation.capabilities,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            diagnostics: installation.diagnostics,
            #[cfg(any(test, feature = "native-execution"))]
            native_mounts,
        })
    }

    /// Captures the checkout and selects its control directory, refusing a
    /// caller's overlapping control directory before capture can publish the
    /// checkout's incarnation token.
    fn checkout(&self) -> Result<(PathBuf, Option<owner::Checkout>)> {
        let selected_directory = match (&self.state_location, &self.config.project) {
            (config::StateLocation::Directory(directory), LocalProject::Workspace(path)) => {
                Some(owner::separated_directory(directory, Some(path))?)
            }
            (config::StateLocation::ProjectRoot(root), LocalProject::Workspace(path)) => {
                owner::separated_directory(root, Some(path))?;
                None
            }
            _ => None,
        };
        let checkout = match &self.config.project {
            LocalProject::Workspace(path) => Some(owner::Checkout::capture(path)?),
            LocalProject::Volume { .. } => None,
        };
        let directory = match selected_directory {
            Some(directory) => directory,
            None => owner::separated_directory(
                &self
                    .state_location
                    .resolve(&self.config.project, checkout.as_ref())?,
                checkout.as_ref().map(|checkout| checkout.path.as_path()),
            )?,
        };
        Ok((directory, checkout))
    }

    async fn storage(&self) -> Result<Storage> {
        acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
        #[cfg(any(test, feature = "native-execution"))]
        acyclic_fs::watch::initialize_native_watch_services().map_err(storage)?;
        #[cfg(any(test, feature = "native-execution"))]
        if self.native_mounts {
            if matches!(self.config.project, LocalProject::Workspace(_)) {
                workspace::volumes::require_available(acyclic_fs::probe_native_mount())?;
            }
            acyclic_fs::initialize_native_mount_services().map_err(storage)?;
        }
        #[cfg(not(any(test, feature = "native-execution")))]
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
        let (directory, checkout) = self.checkout()?;
        #[cfg(test)]
        let checkout = match (&self.config.project, owner::RECAPTURE.get()) {
            (LocalProject::Workspace(path), true) => Some(owner::Checkout::capture(path)?),
            _ => checkout,
        };
        let custody_directory = self
            .config
            .without_user_settings
            .then(|| directory.join("signing-custody"));
        let custody = match custody_directory.as_deref() {
            Some(directory) => {
                acyclic_native_runtime::signing_key::SigningCustody::PrivateFile(directory)
            }
            None => acyclic_native_runtime::signing_key::SigningCustody::Configured,
        };
        let owner =
            owner::Owner::open(&directory, &self.config.project, checkout.as_ref(), custody)?;
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
        let volume_stream = filesystem.stream_provider();
        let provider_ref = owner.project.provider();
        let files = FilesystemHost::new(filesystem.clone(), provider_ref, None)?;
        let issuer = AuthorityRoot::bootstrap(
            "acyclic.local",
            owner
                .key_ref
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("local owner has no signing custody".into()))?,
            custody,
            Authority {
                kind: AggregateKind::Task,
                id: owner.task.to_string(),
            },
        )?;
        let PrivateVolumes { scratch, journal } = PrivateVolumes::open(&files, &owner).await?;
        #[cfg(any(test, feature = "native-execution"))]
        let workspace = self
            .workspace(&filesystem, &directory, &owner, &issuer, &scratch, checkout)
            .await?;
        #[cfg(any(test, feature = "native-execution"))]
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
        #[cfg(any(test, feature = "native-execution"))]
        let files = retain_skills(
            files,
            directory.join("skill-sources"),
            &journal,
            workspace.as_ref(),
            self.user_roots(),
        )?;
        Ok(Storage {
            issuer,
            owner,
            files,
            #[cfg(all(target_os = "linux", feature = "native-execution"))]
            volume_filesystem: filesystem,
            volume_stream,
            scratch,
            journal,
            #[cfg(any(test, feature = "native-execution"))]
            workspace,
        })
    }

    #[cfg(any(test, feature = "native-execution"))]
    async fn workspace(
        &self,
        filesystem: &acyclic_fs::LocalFs,
        directory: &Path,
        owner: &owner::Owner,
        issuer: &AuthorityRoot,
        scratch: &VolumeRef,
        checkout: Option<owner::Checkout>,
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
            processes: if self.config.model.is_none() {
                None
            } else {
                Some(fence_stale_processes(directory.join("processes")).await?)
            },
        };
        let attach = workspace::Attach {
            volume: &owner.project,
            scratch,
            directory,
            operation: workspace::attach_operation(owner.task),
            native_mounts: self.native_mounts,
            authority: Some((issuer, owner.agent)),
        };
        let workspace = match &self.config.project {
            LocalProject::Workspace(_) => {
                let root = checkout
                    .ok_or_else(|| Error::Invalid("checkout capture is absent".into()))?
                    .root;
                Some(workspace::open(filesystem, &attach, root, false, state).await?)
            }
            LocalProject::Volume { .. } => {
                Some(workspace::open_volume(filesystem, &attach, state).await?)
            }
        };
        Ok(workspace.map(Arc::new))
    }

    #[cfg(any(test, feature = "native-execution"))]
    fn user_roots(&self) -> Vec<PathBuf> {
        #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
        {
            self.settings
                .as_ref()
                .map_or_else(Vec::new, |settings| settings.skill_roots().to_vec())
        }
        #[cfg(not(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")))]
        {
            Vec::new()
        }
    }

    async fn finish(self, opened: Storage) -> Result<LocalHarness> {
        let Storage {
            issuer,
            owner,
            files,
            #[cfg(all(target_os = "linux", feature = "native-execution"))]
            volume_filesystem,
            volume_stream,
            scratch,
            journal,
            #[cfg(any(test, feature = "native-execution"))]
            workspace,
        } = opened;
        let verifier = issuer.verifier(&mut None)?;
        context::skills::seed(&files, &scratch).await?;
        let volumes = (&owner.project, &scratch, &journal);
        let (scope, signed) = scopes(&self, &issuer, owner.agent, volumes)?;
        let reader = crate::filesystem::FilesystemContentVerifier::new(
            files.clone(),
            &verifier,
            &signed,
            self.config.limits.file_bytes,
            None,
        )?;
        // A checkout's commands and file tools both write through its
        // checkout tools; a configured volume takes ordinary content writes.
        #[cfg(any(test, feature = "native-execution"))]
        let (workspace_tools, checkout_writer) = match &workspace {
            Some(workspace) => {
                let (tools, writer) = workspace
                    .executor(
                        (&owner.project, &files, self.config.limits.file_bytes),
                        self.config.clock.clone(),
                        &signed,
                        &verifier,
                    )
                    .await?;
                (Some(tools), Some(writer))
            }
            None => (None, None),
        };
        #[cfg(not(any(test, feature = "native-execution")))]
        let (workspace_tools, checkout_writer) = (None, None);
        let writer: Option<Arc<dyn crate::conversation::ContentPublisher>> =
            match self.config.project {
                LocalProject::Volume { .. } => Some(FilesystemContentPublisher::new(
                    files.clone(),
                    &owner.project,
                    &verifier,
                    &signed,
                    self.config.limits.file_bytes,
                    None,
                )?),
                LocalProject::Workspace(_) => checkout_writer,
            };
        let harness = crate::runtime::Bindings {
            scope,
            content: Some(crate::runtime::ContentBindings {
                volume: owner.project.clone(),
                reader,
                writer,
            }),
            workspace_tools,
            ..crate::runtime::Bindings::local()
        }
        .build()?;
        Ok(LocalHarness {
            files,
            #[cfg(all(target_os = "linux", feature = "native-execution"))]
            volume_filesystem,
            harness,
            tools: self.tools,
            root: owner.agent,
            root_authority: Some(issuer),
            agent_stream: Some(volume_stream),
            agents: tokio::sync::OnceCell::new(),
            project: owner.project.clone(),
            scratch,
            journal,
            verifier,
            signed,
            preset: self.preset,
            approval_models: self.config.approval_models,
            agent: self.config.model,
            profiles: self.config.profiles,
            context_manifest: self.config.context_manifest,
            served: self.config.served,
            active_directory: self.config.active_directory,
            skill_cache: context::skills::cache(),
            #[cfg(test)]
            context_read_observer: None,
            #[cfg(any(test, feature = "native-execution"))]
            checkout_context: matches!(self.config.project, LocalProject::Workspace(_)),
            #[cfg(any(test, feature = "hooks"))]
            settings: self.settings,
            limits: self.config.limits,
            compaction: self.compaction,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            mcp_diagnostics: self.diagnostics,
            goal_continuations: self.config.goal_continuations,
            #[cfg(any(test, feature = "native-execution"))]
            workspace,
            #[cfg(test)]
            environment_reads: std::sync::atomic::AtomicUsize::new(0),
            clock: self.config.clock,
            #[cfg(any(test, feature = "native-execution"))]
            environment_platform: self.config.environment_platform,
        })
    }
}

/// The agent turns' runtime scope, and the owner's signed scope over it and
/// its private journal: the project, scratch and journal volumes, in order.
fn scopes(
    opening: &Opening,
    issuer: &AuthorityRoot,
    agent: AgentId,
    volumes: (&VolumeRef, &VolumeRef, &VolumeRef),
) -> Result<(RuntimeScope, Scope)> {
    let (project, scratch, journal) = volumes;
    let mut grants = Vec::new();
    if opening.config.model.is_some() {
        grants.extend([
            "model:generate".to_owned(),
            crate::contract::capability::INTERACTION_ROUTE.to_owned(),
            "interaction:open".to_owned(),
        ]);
    }
    for volume in [project, scratch] {
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            grants.push(volume.capability(operation, &mut None)?);
        }
        grants.push(volume.directory_read_capability("", &mut None)?);
    }
    grants.extend(opening.preset.tool_grants(&opening.tools)?);
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    grants.extend(
        opening
            .capabilities
            .iter()
            // Registry definitions above already supply every tool-call grant.
            .filter(|grant| !grant.starts_with("tool:call:"))
            .map(str::to_owned),
    );
    grants.push(journal.directory_read_capability("inputs", &mut None)?);
    let scope = RuntimeScope::new(
        Capabilities::new(grants.iter().map(String::as_str)),
        opening.config.limits,
    )?
    .with_run_limits(opening.config.run_limits, &mut None)?;
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        grants.push(journal.capability(operation, &mut None)?);
    }
    let signed =
        issuer.bootstrap_scope("owner", &Capabilities::new(grants), Some(agent), &mut None)?;
    Ok((scope, signed))
}

fn validate_configuration(config: &LocalHarnessConfig) -> Result<()> {
    config.limits.validate(&mut None)?;
    if !config.active_directory.is_empty() {
        crate::conversation::validate_content_path(&config.active_directory, &mut None)?;
    }
    Ok(())
}

#[cfg(any(test, feature = "native-execution"))]
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
        files.create_volume(&scratch).await?;
        files.create_volume(&journal).await?;
        Ok(Self { scratch, journal })
    }
}

fn configure_tools(
    preset: &crate::preset::Preset,
    config: &mut LocalHarnessConfig,
) -> Result<ToolRegistry> {
    let tools = preset.tools(
        |_| cfg!(any(test, feature = "native-execution")),
        std::mem::take(&mut config.tools),
    )?;
    #[cfg(feature = "search-http")]
    if config.model.is_none() && preset.web_tools {
        return Err(Error::Invalid(
            "native host coordination cannot register web tools".into(),
        ));
    }
    #[cfg(feature = "search-http")]
    let tools = if preset.web_tools {
        let mut tools = tools;
        web::WebExecutor::bind(&mut tools, config.search_provider.take())?;
        tools
    } else {
        tools
    };
    Ok(tools)
}

/// This owner's process custody. The first owner of the directory in this
/// process kills every tool tree a dead previous owner left, awaiting each
/// tree's exit; a later owner in the same process shares the live custody.
#[cfg(any(test, feature = "native-execution"))]
async fn fence_stale_processes(
    path: std::path::PathBuf,
) -> Result<Arc<acyclic_native_runtime::ProcessCustody>> {
    tokio::task::spawn_blocking(move || acyclic_native_runtime::ProcessCustody::acquire(&path))
        .await
        .map_err(storage)?
        .map_err(storage)
}
