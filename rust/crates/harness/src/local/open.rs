use super::*;

#[cfg(any(test, not(target_arch = "wasm32")))]
pub(super) fn uses_native_execution(tools: &ToolRegistry) -> Result<bool> {
    Ok(tools
        .get(crate::tools::shell::PLATFORM.name, None, None)?
        .is_some())
}

struct Opening {
    config: LocalHarnessConfig,
    /// The agents' defaults, from which their tools and turns are built.
    preset: crate::preset::Preset,
    tools: ToolRegistry,
    compaction: crate::context::CompactionPolicy,
    /// The configured compaction choice, reselected when `agent.ts` changes
    /// the model.
    compaction_choice: Option<crate::context::CompactionPolicy>,
    /// Resolves a model `agent.ts` selects; without one the host's binding
    /// is fixed.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    models: Option<Arc<config::ModelResolver>>,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    settings: Option<Arc<crate::native_settings::SettingsConfig>>,
    #[cfg(not(target_arch = "wasm32"))]
    capabilities: Capabilities,
    #[cfg(not(target_arch = "wasm32"))]
    diagnostics: Vec<crate::mcp::McpServerFailure>,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    native_execution: bool,
}

struct Storage {
    issuer: AuthorityRoot,
    owner: owner::Owner,
    files: Arc<LocalFilesystem>,
    #[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
    volume_filesystem: acyclic_fs::LocalFs,
    /// The Stream provider holding the filesystem's volume authority.
    volume_stream: Arc<LocalStream>,
    scratch: VolumeRef,
    journal: VolumeRef,
    shared: VolumeRef,
    #[cfg(any(test, not(target_arch = "wasm32")))]
    workspace: Option<Arc<workspace::LocalWorkspace>>,
}

pub(crate) async fn open(config: LocalHarnessConfig) -> Result<LocalHarness> {
    let mut opening = Opening::configure(config)?;
    opening.config = opening.config.prepare_state()?;
    if let Some(refusal) = opening.host_refusals().into_iter().next() {
        return Err(refusal);
    }
    let opening = opening.activate().await?;
    let storage = opening.storage().await?;
    opening.finish(storage).await
}

/// Read-only local prerequisites. Never captures a checkout, opens storage,
/// creates signing authority or starts configured MCP children.
pub(crate) async fn doctor(config: LocalHarnessConfig) -> Vec<Error> {
    let mut refusals = config.preflight();
    if let Err(error) = Opening::configure(config) {
        refusals.push(error);
    }
    refusals
}

impl Opening {
    fn configure(config: LocalHarnessConfig) -> Result<Self> {
        validate_configuration(&config)?;
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let models = config.models.as_ref().map(|(models, _)| Arc::clone(models));
        // Settings resolve once, before the model and approvers they select.
        #[cfg(any(test, not(target_arch = "wasm32")))]
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
        let mut config = config;
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
        let tools = configure_tools(&mut config)?;
        let compaction = if let Some((model, provider)) = &config.model {
            crate::context::CompactionPolicy::for_model(
                config.compaction.clone(),
                model,
                provider.as_ref(),
            )?
        } else {
            crate::context::CompactionPolicy::Disabled
        };
        #[cfg(any(test, not(target_arch = "wasm32")))]
        // Coordination uses the existing checkout. Only our own native shell
        // needs its auxiliary volumes projected before a command can run.
        let native_execution = uses_native_execution(&tools)?;
        let compaction_choice = config.compaction.take();
        Ok(Self {
            config,
            preset,
            tools,
            compaction_choice,
            compaction,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            models,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            settings: settings_configuration,
            #[cfg(not(target_arch = "wasm32"))]
            capabilities: Capabilities::default(),
            #[cfg(not(target_arch = "wasm32"))]
            diagnostics: Vec::new(),
            #[cfg(any(test, not(target_arch = "wasm32")))]
            native_execution,
        })
    }

    /// Activation belongs exclusively to opening, after the read-only checks.
    async fn activate(self) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut opening = self;
            let installation = match &opening.settings {
                Some(settings) => {
                    let root = std::fs::canonicalize(opening.config.project.root())
                        .map_err(Error::process_io)?;
                    crate::mcp::McpConfig::install_watched(
                        opening.tools,
                        Arc::clone(settings),
                        &root,
                        opening.config.mcp_limits,
                        Arc::new(crate::secrets::EnvSecrets),
                    )
                    .await?
                }
                None => crate::mcp::McpInstallation {
                    tools: opening.tools,
                    capabilities: Capabilities::default(),
                    diagnostics: Vec::new(),
                },
            };
            opening.tools = installation.tools;
            opening.capabilities = installation.capabilities;
            opening.diagnostics = installation.diagnostics;
            Ok(opening)
        }
        #[cfg(target_arch = "wasm32")]
        Ok(self)
    }

    /// Captures the checkout and selects its control directory, refusing a
    /// caller's overlapping control directory before opening host state.
    fn checkout(&self) -> Result<(PathBuf, Option<owner::Checkout>)> {
        let state_location = self.config.state_location()?;
        let selected_directory = match (&state_location, &self.config.project) {
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
                &state_location.resolve(&self.config.project, checkout.as_ref())?,
                checkout.as_ref().map(|checkout| checkout.path.as_path()),
            )?,
        };
        Ok((directory, checkout))
    }

    /// The host's own refusals, before any state is touched: each names what
    /// is missing and its recovery. Opening stops at the first.
    fn host_refusals(&self) -> Vec<Error> {
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let native = self.native_execution;
        #[cfg(not(any(test, not(target_arch = "wasm32"))))]
        let native = true;
        let mut refusals = self.config.native_refusals(native);
        refusals.extend(self.config.state_refusals());
        refusals
    }

    // Physical commands use the same native shell preflight, but project no volume.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    fn native_mounts(&self) -> bool {
        self.native_execution && self.config.workspace_mounting == WorkspaceMounting::Native
    }

    fn storage(&self) -> acyclic_stream::BoxProviderFuture<'_, Result<Storage>> {
        Box::pin(self.initialize_storage())
    }

    async fn initialize_storage(&self) -> Result<Storage> {
        acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
        #[cfg(any(test, not(target_arch = "wasm32")))]
        acyclic_fs::watch::initialize_native_watch_services().map_err(storage)?;
        #[cfg(any(test, not(target_arch = "wasm32")))]
        if self.native_mounts() {
            acyclic_fs::initialize_native_mount_services_async()
                .await
                .map_err(storage)?;
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
        let custody = match (
            self.config.signing_store.as_deref(),
            custody_directory.as_deref(),
        ) {
            (Some(store), _) => acyclic_native_runtime::signing_key::SigningCustody::Store(store),
            (None, Some(directory)) => {
                acyclic_native_runtime::signing_key::SigningCustody::PrivateFile(directory)
            }
            (None, None) => acyclic_native_runtime::signing_key::SigningCustody::Configured,
        };
        let owner = owner::Owner::open(
            &directory,
            &self.config.project,
            checkout.as_ref(),
            custody,
            self.config.test_state.as_ref(),
        )?;
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
        // Alarm time is the authority's: this ledger decides each alarm
        // consumption with its own commit clock.
        volume_stream
            .install_commit_admission(acyclic_actors::local::alarm_admission())
            .map_err(storage)?;
        let provider_ref = owner.project.provider();
        let files = FilesystemHost::new(filesystem.clone(), provider_ref, None)?
            .with_changes(filesystem.stream_provider())?;
        let issuer = AuthorityRoot::bootstrap(
            "acyclic.local",
            &owner.key_ref,
            custody,
            Authority {
                kind: AggregateKind::Task,
                id: owner.task.to_string(),
            },
        )?;
        let SessionVolumes {
            scratch,
            journal,
            shared,
        } = SessionVolumes::open(&files, &owner).await?;
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let workspace = self
            .workspace(
                &filesystem,
                &directory,
                &owner,
                &issuer,
                (&scratch, &shared),
                checkout,
            )
            .await?;
        #[cfg(any(test, not(target_arch = "wasm32")))]
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
        #[cfg(any(test, not(target_arch = "wasm32")))]
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
            #[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
            volume_filesystem: filesystem,
            volume_stream,
            scratch,
            journal,
            shared,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            workspace,
        })
    }

    #[cfg(any(test, not(target_arch = "wasm32")))]
    async fn workspace(
        &self,
        filesystem: &acyclic_fs::LocalFs,
        directory: &Path,
        owner: &owner::Owner,
        issuer: &AuthorityRoot,
        volumes: (&VolumeRef, &VolumeRef),
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
            scratch: volumes.0,
            shared: Some(volumes.1),
            directory,
            operation: workspace::attach_operation(owner.task),
            native_mounts: self.native_mounts(),
            mounting: self.config.workspace_mounting,
            native_mount_probe: &self.config.native_mount_probe,
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

    #[cfg(any(test, not(target_arch = "wasm32")))]
    fn user_roots(&self) -> Vec<PathBuf> {
        #[cfg(any(test, not(target_arch = "wasm32")))]
        {
            self.settings
                .as_ref()
                .map_or_else(Vec::new, |settings| settings.skill_roots().to_vec())
        }
        #[cfg(not(any(test, not(target_arch = "wasm32"))))]
        {
            Vec::new()
        }
    }

    fn finish(
        self,
        opened: Storage,
    ) -> acyclic_stream::BoxProviderFuture<'static, Result<LocalHarness>> {
        Box::pin(self.assemble(opened))
    }

    async fn assemble(self, opened: Storage) -> Result<LocalHarness> {
        let Storage {
            issuer,
            owner,
            files,
            #[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
            volume_filesystem,
            volume_stream,
            scratch,
            journal,
            shared,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            workspace,
        } = opened;
        let verifier = issuer.verifier(&mut None)?;
        let seed = files.seed_layer().await?;
        let system = if self.config.system_files.is_empty() {
            None
        } else {
            Some(files.system_layer(&self.config.system_files).await?)
        };
        let volumes = (
            &owner.project,
            &scratch,
            &journal,
            &seed.0,
            &shared,
            system.as_ref().map(|layer| &layer.volume),
        );
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
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let (workspace_tools, checkout_writer) = match &workspace {
            Some(workspace) => {
                let (tools, writer) = workspace
                    .executor(
                        (&owner.project, &files, self.config.limits.file_bytes),
                        self.config.clock.clone(),
                        &signed,
                        &verifier,
                        None,
                    )
                    .await?;
                (Some(tools), Some(writer))
            }
            None => (None, None),
        };
        #[cfg(not(any(test, not(target_arch = "wasm32"))))]
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
            targets: workspace_tools.map(super::agents::home).transpose()?,
            seed: Some(crate::layers::PinnedLayer {
                volume: seed.0.clone(),
                generation: seed.1.clone(),
            }),
            system,
            ..crate::runtime::Bindings::local()
        }
        .build()?;
        let volumes = agent_settings::Volumes {
            files: &files,
            verifier: &verifier,
            signed: &signed,
            home: &scratch,
            journal: &journal,
            maximum_bytes: self.config.limits.file_bytes,
        };
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let settings_tree = agent_settings::Tree::new(&volumes).await?;
        // `plugins.ts` selects the plugin hooks as the session starts.
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let plugin_hooks = agent_settings::plugins(
            self.config.question,
            &settings_tree,
            owner.agent,
            self.config.clock.as_ref(),
        )
        .await?;
        // `agent.ts` selects the model and its sampling as the session starts.
        let (agent, sampling) = match self.config.model.clone() {
            Some(started) => {
                #[cfg(not(any(test, not(target_arch = "wasm32"))))]
                let settings_tree = agent_settings::Tree::new(&volumes).await?;
                #[cfg(any(test, not(target_arch = "wasm32")))]
                let resolver = self
                    .models
                    .as_deref()
                    .map(|models| move |selection: &str| models.resolve(selection));
                #[cfg(any(test, not(target_arch = "wasm32")))]
                let resolve = resolver.as_ref().map(|resolve| {
                    resolve as &(dyn Fn(&str) -> Result<(Model, Arc<dyn ModelProvider>)> + Sync)
                });
                #[cfg(not(any(test, not(target_arch = "wasm32"))))]
                let resolve = None;
                let settings = agent_settings::admit(
                    &settings_tree,
                    owner.agent,
                    started.clone(),
                    resolve,
                    (&self.compaction, &self.compaction_choice),
                    self.config.clock.as_ref(),
                )
                .await?;
                (Some(settings.model), settings.sampling)
            }
            None => (None, None),
        };
        Ok(LocalHarness {
            topology: self.config.topology,
            files,
            #[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
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
            shared,
            seed,
            verifier,
            signed,
            preset: self.preset,
            question_plugin: self.config.question,
            agent_tools: self
                .config
                .default_tools
                .into_iter()
                .filter(|name| crate::tools::AGENT_TOOLS.contains(&name.as_str()))
                .collect(),
            approval_models: self.config.approval_models,
            agent,
            models: self.models,
            sampling,
            served: self.config.served,
            active_directory: self.config.active_directory,
            skill_cache: context::skills::cache(),
            #[cfg(test)]
            context_read_observer: None,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            checkout_context: matches!(self.config.project, LocalProject::Workspace(_)),
            #[cfg(any(test, not(target_arch = "wasm32")))]
            settings: self.settings,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            plugin_hooks,
            limits: self.config.limits,
            compaction: self.compaction_choice,
            #[cfg(not(target_arch = "wasm32"))]
            mcp_diagnostics: self.diagnostics,
            goal_continuations: self.config.goal_continuations,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            workspace,
            told: std::sync::Mutex::default(),
            clock: self.config.clock,
            #[cfg(any(test, not(target_arch = "wasm32")))]
            environment_platform: self.config.environment_platform,
        })
    }
}

/// The agent turns' runtime scope, and the owner's signed scope over it and
/// its private journal: the project, home, journal, seed and shared volumes, in
/// order. The seed is read-only: no scope grants writing it.
fn scopes(
    opening: &Opening,
    issuer: &AuthorityRoot,
    agent: AgentId,
    volumes: (
        &VolumeRef,
        &VolumeRef,
        &VolumeRef,
        &VolumeRef,
        &VolumeRef,
        Option<&VolumeRef>,
    ),
) -> Result<(RuntimeScope, Scope)> {
    let (project, scratch, journal, seed, shared, system) = volumes;
    let mut grants = Vec::new();
    if opening.config.model.is_some() {
        grants.extend([
            "model:generate".to_owned(),
            crate::contract::capability::INTERACTION_ROUTE.to_owned(),
            "interaction:open".to_owned(),
        ]);
    }
    for volume in [project, scratch, shared] {
        for operation in [
            VolumeOperation::Read,
            VolumeOperation::Write,
            VolumeOperation::Delete,
        ] {
            grants.push(volume.capability(operation, &mut None)?);
        }
        grants.push(volume.directory_read_capability("", &mut None)?);
    }
    for volume in std::iter::once(seed).chain(system) {
        grants.extend([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.directory_read_capability("", &mut None)?,
        ]);
    }
    grants.extend(crate::tools::call_grants(
        &opening.tools,
        opening.config.default_tools.iter().map(String::as_str),
    )?);
    #[cfg(not(target_arch = "wasm32"))]
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

#[cfg(any(test, not(target_arch = "wasm32")))]
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

struct SessionVolumes {
    scratch: VolumeRef,
    journal: VolumeRef,
    shared: VolumeRef,
}
impl SessionVolumes {
    async fn open(files: &LocalFilesystem, owner: &owner::Owner) -> Result<Self> {
        let scratch = VolumeRef::new(
            owner.project.provider().clone(),
            format!("home-{}", owner.agent),
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
        let shared = VolumeRef::new(
            owner.project.provider().clone(),
            format!("shared-{}", owner.task),
            VolumeClass::SessionShared,
            VolumeOwner::Session(owner.task.to_string()),
            &mut None,
        )?;
        files.create_volume(&scratch).await?;
        files.create_volume(&journal).await?;
        files.create_volume(&shared).await?;
        Ok(Self {
            scratch,
            journal,
            shared,
        })
    }
}

fn configure_tools(config: &mut LocalHarnessConfig) -> Result<ToolRegistry> {
    let mut names = config.default_tools.clone();
    if config.model.is_some() && !names.iter().any(|name| name == "ask_question") {
        names.push("ask_question".into());
    }
    let tools = crate::tools::registry(&names, |_| true, std::mem::take(&mut config.tools))?;
    #[cfg(not(target_arch = "wasm32"))]
    if config.model.is_none() && config.web_tools {
        return Err(Error::Invalid(
            "native host coordination cannot register web tools".into(),
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    let tools = if config.web_tools {
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
#[cfg(any(test, not(target_arch = "wasm32")))]
async fn fence_stale_processes(
    path: std::path::PathBuf,
) -> Result<Arc<acyclic_native_runtime::ProcessCustody>> {
    acyclic_native_runtime::spawn_blocking(move || {
        acyclic_native_runtime::ProcessCustody::acquire(&path)
    })
    .await
    .map_err(storage)?
    .map_err(storage)
}
