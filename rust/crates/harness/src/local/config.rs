use super::*;

/// Resolves ambient or injected roots before checkout capture can write metadata.
pub(super) enum StateLocation {
    Directory(PathBuf),
    ProjectRoot(PathBuf),
}

impl StateLocation {
    pub(super) fn resolve(
        &self,
        project: &LocalProject,
        checkout: Option<&owner::Checkout>,
    ) -> Result<PathBuf> {
        match self {
            Self::Directory(directory) => Ok(directory.clone()),
            Self::ProjectRoot(root) => owner::state_directory_in(root, project, checkout),
        }
    }
}

/// Storage selected for the agent's project view.
pub(super) enum LocalProject {
    /// A normal checkout, opened lazily without importing its files.
    Workspace(PathBuf),
    /// An existing project volume in a local filesystem directory.
    Volume {
        /// Existing local filesystem provider directory.
        directory: PathBuf,
        /// Existing project volume within that provider.
        volume: VolumeRef,
    },
}

/// Durable local composition with a required model binding.
pub struct LocalHarnessConfig {
    pub(super) active_directory: String,
    pub(super) goal_continuations: Option<crate::goal::GoalContinuationPolicy>,
    pub(super) project: LocalProject,
    pub(super) state_directory: Option<PathBuf>,
    /// Refuses ambient state and settings discovery; set only by `hermetic()`.
    pub(super) hermetic: bool,
    /// Test-owned state root, retained by every store opened in it.
    pub(super) test_state: Option<acyclic_native_runtime::OwnershipAnchor>,
    pub(super) model: Option<(Model, Arc<dyn ModelProvider>)>,
    /// Resolves the model selection, approver models and search at open.
    #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    pub(super) models: Option<(Arc<ModelResolver>, Option<String>)>,
    pub(super) context_manifest: Option<crate::conversation::FileRef>,
    pub(super) tools: Vec<Tool>,
    /// Standard tools enabled, by name.
    pub(super) default_tools: Vec<String>,
    /// System prompt of every agent turn.
    pub(super) prompt: Option<String>,
    /// Open children each agent may have at once.
    pub(super) max_children: Option<u32>,
    /// Hosts the agents for [`crate::server::serve`].
    pub(super) served: Option<Arc<dyn super::Verifier>>,
    pub(super) approval_policy: crate::approvals::ApprovalPolicy,
    pub(super) approval_issuer: Option<AuthorityRoot>,
    pub(super) approval_models: std::collections::BTreeMap<String, (Model, Arc<dyn ModelProvider>)>,
    #[cfg(feature = "search-http")]
    pub(super) search_provider: Option<Arc<dyn crate::search::SearchProvider>>,
    #[cfg(feature = "search-http")]
    pub(super) web_tools: bool,
    pub(super) limits: Limits,
    pub(super) run_limits: TaskRunLimits,
    pub(super) compaction: Option<crate::context::CompactionPolicy>,
    #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    pub(crate) settings: Option<crate::native_settings::SettingsConfig>,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    pub(super) mcp_limits: crate::mcp::McpClientLimits,
    pub(super) clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    #[cfg(any(test, feature = "native-execution"))]
    pub(crate) environment_platform: crate::native_environment::PlatformPolicy,
}

#[cfg(any(
    test,
    feature = "native-execution",
    feature = "hooks",
    feature = "mcp-stdio",
    feature = "mcp-http"
))]
impl LocalProject {
    pub(super) fn root(&self) -> &Path {
        match self {
            Self::Workspace(path) => path,
            Self::Volume { directory, .. } => directory,
        }
    }
}

impl LocalHarnessConfig {
    /// Selects a checkout path and its stock model binding.
    #[must_use]
    pub fn new(workspace: impl Into<PathBuf>, model: (Model, Arc<dyn ModelProvider>)) -> Self {
        Self::configuration(workspace.into(), Some(model))
    }

    /// Selects a checkout whose model resolves at open through `models`: `selection`, else the
    /// preset's, else the native settings' configured model, else the resolver's default. Approver
    /// models resolve through `models` too; the settings' search provider backs the preset's
    /// web tools.
    #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    #[must_use]
    pub fn resolving(
        workspace: impl Into<PathBuf>,
        models: Arc<ModelResolver>,
        selection: Option<String>,
    ) -> Self {
        let mut config = Self::configuration(workspace.into(), None);
        config.models = Some((models, selection));
        config
    }

    /// The one resolution of a [`Self::resolving`] composition under its native `settings`;
    /// `injected` settings keep search off the environment's native sources.
    #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    pub(super) fn resolve(
        mut self,
        settings: &crate::native_settings::SettingsConfig,
        injected: bool,
    ) -> Result<Self> {
        let Some((models, selection)) = self.models.take() else {
            return Ok(self);
        };
        let selection = match selection {
            Some(selection) => selection,
            None => settings
                .configured_model()?
                .unwrap_or_else(|| models.default.clone()),
        };
        self.model = Some(models.resolve(&selection)?);
        for model in self.approval_policy.models() {
            self.approval_models
                .insert(model.into(), models.resolve(model)?);
        }
        #[cfg(feature = "search-http")]
        {
            let search = crate::search::SearchConfig::from_file(&settings.search_config_path())?;
            let search = if injected {
                search.without_native_sources()
            } else {
                search
            };
            // The preset alone selects web tools; this only binds their provider.
            if let Some(provider) = search.provider()? {
                self.search_provider = Some(provider);
            }
        }
        #[cfg(not(feature = "search-http"))]
        let _ = injected;
        Ok(self)
    }

    /// Opens workspace coordination without a model, tools, settings or agent execution.
    /// The trusted harness owner must start this outside the native host sandbox.
    #[cfg(feature = "native-execution")]
    #[must_use]
    pub fn native_host(workspace: impl Into<PathBuf>) -> Self {
        let mut config = Self::configuration(workspace.into(), None);
        config.default_tools.clear();
        config.hermetic = true;
        config.goal_continuations = None;
        config.compaction = Some(crate::context::CompactionPolicy::Disabled);
        config
    }

    fn configuration(workspace: PathBuf, model: Option<(Model, Arc<dyn ModelProvider>)>) -> Self {
        Self {
            active_directory: String::new(),
            goal_continuations: Some(Default::default()),
            project: LocalProject::Workspace(workspace),
            state_directory: None,
            hermetic: false,
            test_state: None,
            model,
            #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
            models: None,
            context_manifest: None,
            tools: Vec::new(),
            default_tools: crate::tools::default_names(),
            prompt: None,
            max_children: None,
            served: None,
            approval_policy: Default::default(),
            approval_issuer: None,
            approval_models: Default::default(),
            #[cfg(feature = "search-http")]
            search_provider: None,
            #[cfg(feature = "search-http")]
            web_tools: false,
            limits: Limits::default(),
            run_limits: TaskRunLimits::default(),
            compaction: None,
            #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
            settings: None,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            mcp_limits: crate::mcp::McpClientLimits::default(),
            clock: Arc::new(acyclic_stream::SystemUnixMillisClock),
            #[cfg(any(test, feature = "native-execution"))]
            environment_platform: crate::native_environment::PlatformPolicy::Native,
        }
    }
    /// Selects instruction scope relative to the workspace boundary.
    /// Empty selects root instructions; traversal outside the workspace is rejected.
    #[must_use]
    pub fn active_directory(mut self, directory: impl Into<String>) -> Self {
        self.active_directory = directory.into();
        self
    }
    /// Selects the instruction scope containing an explicitly supplied host working directory.
    /// A working directory outside the bound project selects root instructions only.
    #[cfg(any(test, feature = "native-execution"))]
    pub fn working_directory(self, directory: &Path) -> Result<Self> {
        let root = std::fs::canonicalize(self.project.root()).map_err(storage)?;
        let directory = std::fs::canonicalize(directory).map_err(storage)?;
        let relative = directory.strip_prefix(root).ok();
        let active = relative
            .map(|path| {
                path.components()
                    .map(|component| {
                        component
                            .as_os_str()
                            .to_str()
                            .map(str::to_owned)
                            .ok_or_else(|| Error::Invalid("instruction scope is not UTF-8".into()))
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(|parts| parts.join("/"))
            })
            .transpose()?
            .unwrap_or_default();
        Ok(self.active_directory(active))
    }
    /// Opens an existing volume in the local filesystem directory passed to `new`.
    #[must_use]
    pub fn volume(mut self, volume: VolumeRef) -> Self {
        let directory = match self.project {
            LocalProject::Workspace(path) => path,
            LocalProject::Volume { directory, .. } => directory,
        };
        self.project = LocalProject::Volume { directory, volume };
        self
    }
    /// Selects the caller-owned durable state directory, outside the checkout.
    #[must_use]
    pub fn state_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.state_directory = Some(directory.into());
        self.test_state = None;
        self
    }
    /// Refuses ambient state/settings discovery and allocates state lazily unless explicitly rooted.
    #[must_use]
    pub fn hermetic(mut self) -> Self {
        self.hermetic = true;
        self
    }
    /// Embedded bindings without a checkout never allocate an unused state root.
    pub(super) fn prepare_state(mut self) -> Result<Self> {
        if self.hermetic && self.state_directory.is_none() {
            // The default-selector tests inject a private fixture root. They
            // still cannot reach the process user's ambient state directory.
            #[cfg(test)]
            if owner::STATE_ROOT.with_borrow(Option::is_some) {
                return Ok(self);
            }
            #[cfg(test)]
            TEMPORARY_STATES.with(|count| count.set(count.get() + 1));
            let state = tempfile::tempdir().map_err(storage)?;
            self.state_directory = Some(state.path().to_path_buf());
            self.test_state = Some(acyclic_native_runtime::OwnershipAnchor::new(state));
        }
        Ok(self)
    }
    /// The one resolution of this composition's durable state directory: the injected directory, else the
    /// project's default under the per-user root.
    #[cfg(any(test, feature = "native-execution"))]
    pub(super) fn resolved_state_directory(&self) -> Result<PathBuf> {
        let location = self.state_location()?;
        if let StateLocation::Directory(directory) = location {
            return Ok(directory);
        }
        if let (StateLocation::ProjectRoot(root), LocalProject::Workspace(path)) =
            (&location, &self.project)
        {
            owner::separated_directory(root, Some(path))?;
        }
        let checkout = match &self.project {
            LocalProject::Workspace(path) => Some(owner::Checkout::capture(path)?),
            LocalProject::Volume { .. } => None,
        };
        location.resolve(&self.project, checkout.as_ref())
    }
    pub(super) fn state_location(&self) -> Result<StateLocation> {
        match &self.state_directory {
            Some(directory) => crate::native_environment::state(Some(directory))
                .map(StateLocation::Directory)
                .ok_or_else(|| {
                    Error::Unsupported("injected state directory is unavailable".into())
                }),
            #[cfg(test)]
            None if owner::STATE_ROOT.with_borrow(Option::is_some) => {
                Ok(StateLocation::ProjectRoot(owner::user_state_root()?))
            }
            None if self.hermetic => Err(Error::Unsupported(
                "hermetic compositions must prepare or inject state".into(),
            )),
            None => owner::user_state_root().map(StateLocation::ProjectRoot),
        }
    }
    /// Overrides default per-turn AGENTS.md/CLAUDE.md capture with an immutable manifest.
    #[must_use]
    pub fn context_manifest(mut self, reference: crate::conversation::FileRef) -> Self {
        self.context_manifest = Some(reference);
        self
    }
    /// Binds ordered approval policy data to every enabled tool, including defaults.
    #[must_use]
    pub fn approval_policy(mut self, policy: crate::approvals::ApprovalPolicy) -> Self {
        self.approval_policy = policy;
        self
    }
    /// Supplies the existing runtime's authority for embedded automatic and human
    /// approvers. Its scopes are verified by that runtime's interaction owner.
    #[must_use]
    pub fn approval_authority(mut self, issuer: AuthorityRoot) -> Self {
        self.approval_issuer = Some(issuer);
        self
    }
    /// Binds an independent approver model; it receives its own prompt and no tools.
    #[must_use]
    pub fn approval_model(
        mut self,
        selection: impl Into<String>,
        model: (Model, Arc<dyn ModelProvider>),
    ) -> Self {
        self.approval_models.insert(selection.into(), model);
        self
    }
    /// Hosts the agent tree as a self-hosted endpoint does: admitted under
    /// this harness's own owner scope, exactly as a managed host admits its
    /// verified caller, instead of as the tree's root custody.
    #[must_use]
    #[cfg(feature = "serve")]
    pub(crate) fn served(mut self, verifier: Arc<dyn super::Verifier>) -> Self {
        self.served = Some(verifier);
        self
    }
    /// Applies `preset`'s prompt, tool selection, limits and approval policy. Its model is
    /// the fallback selection of a [`Self::resolving`] composition; [`Self::new`] binds its own.
    /// Web tools need the `search-http` feature.
    #[must_use]
    pub fn preset(mut self, preset: &crate::preset::Preset) -> Self {
        #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
        if let Some((_, selection @ None)) = &mut self.models {
            selection.clone_from(&preset.model);
        }
        self.default_tools.clone_from(&preset.tools);
        self.prompt.clone_from(&preset.prompt);
        self.max_children = preset.max_children;
        #[cfg(feature = "search-http")]
        {
            self.web_tools = preset.web_tools;
        }
        self.limits = preset.execution_limits(self.limits);
        self.approval_policy = preset.approval_policy.clone();
        self
    }
    /// The preset this configuration applies: its prompt, tool selection,
    /// open-children bound and approval policy, its step limit already in
    /// [`Limits`].
    pub(super) fn preset_value(&self) -> crate::preset::Preset {
        crate::preset::Preset {
            model: None,
            prompt: self.prompt.clone(),
            tools: self.default_tools.clone(),
            #[cfg(feature = "search-http")]
            web_tools: self.web_tools,
            #[cfg(not(feature = "search-http"))]
            web_tools: false,
            max_steps: None,
            max_children: self.max_children,
            approval_policy: self.approval_policy.clone(),
        }
    }
    /// Disables the default set, retaining every custom tool.
    #[must_use]
    pub fn without_default_tools(mut self) -> Self {
        self.default_tools.clear();
        self
    }
    /// Adds one custom tool to the enabled default set.
    #[must_use]
    pub fn tool(mut self, tool: Tool) -> Self {
        self.tools.push(tool);
        self
    }
    /// Adds custom tools to the enabled default set.
    #[must_use]
    pub fn tools(mut self, tools: impl IntoIterator<Item = Tool>) -> Self {
        self.tools.extend(tools);
        self
    }
    /// Enables credential-free HTTP retrieval. Search requires a configured provider.
    #[cfg(feature = "search-http")]
    #[must_use]
    pub fn web_tools(mut self) -> Self {
        self.web_tools = true;
        self
    }

    /// Binds a host-owned search provider without serializing its credentials.
    #[cfg(feature = "search-http")]
    #[must_use]
    pub fn search_provider(mut self, provider: Arc<dyn crate::search::SearchProvider>) -> Self {
        self.search_provider = Some(provider);
        self.web_tools = true;
        self
    }

    /// Prebinds web tools before opening an embedded runtime and issuing its grants.
    #[cfg(feature = "search-http")]
    pub fn bind_web_tools(&self, registry: &mut ToolRegistry) -> Result<()> {
        if self.web_tools {
            super::web::WebExecutor::bind(registry, self.search_provider.clone())?;
        }
        Ok(())
    }

    #[cfg(feature = "search-http")]
    pub(super) fn validate_web_binding(
        &self,
        registry: &ToolRegistry,
        verifier: &AuthorityVerifier,
        signed: &Scope,
    ) -> Result<()> {
        if !self.web_tools {
            return Ok(());
        }
        verifier.verify(signed, &mut None)?;
        for definition in crate::search::web::definitions(self.search_provider.is_some())? {
            if !signed
                .capabilities()
                .contains(&format!("tool:call:{}", definition.name))
            {
                return Err(Error::Unauthorized(
                    "requested web tools require prebound signed call grants".into(),
                ));
            }
            if registry
                .get(&definition.name, None, None)?
                .is_none_or(|tool| tool.definition != definition)
            {
                return Err(Error::Unsupported(
                    "prebind requested web tools before opening the task runtime".into(),
                ));
            }
        }
        Ok(())
    }

    /// Selects content, projection and model-loop bounds.
    #[must_use]
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    /// Selects durable execution bounds.
    #[must_use]
    pub fn run_limits(mut self, limits: TaskRunLimits) -> Self {
        self.run_limits = limits;
        self
    }
    /// Selects the finite goal policy; `None` disables automatic continuations.
    #[must_use]
    pub fn goal_continuations(
        mut self,
        policy: Option<crate::goal::GoalContinuationPolicy>,
    ) -> Self {
        self.goal_continuations = policy;
        self
    }
    /// Selects an explicit compaction policy, validated without changing its budgets.
    #[must_use]
    pub fn compaction(mut self, policy: crate::context::CompactionPolicy) -> Self {
        self.compaction = Some(policy);
        self
    }
    /// Selects native settings locations and explicit project trust using the shared loader.
    #[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    #[must_use]
    pub fn settings(mut self, settings: crate::native_settings::SettingsConfig) -> Self {
        self.settings = Some(settings);
        self
    }
    /// Selects independent finite MCP discovery and tool-exchange bounds.
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    #[must_use]
    pub fn mcp_limits(mut self, limits: crate::mcp::McpClientLimits) -> Self {
        self.mcp_limits = limits;
        self
    }
    /// Selects the clock used by durable admission and leases.
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn acyclic_stream::UnixMillisClock>) -> Self {
        self.clock = clock;
        self
    }
}

#[cfg(test)]
std::thread_local! {
    pub(super) static TEMPORARY_STATES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A model selection's bound provider.
#[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
type ModelBinding = (Model, Arc<dyn ModelProvider>);

/// Resolves model selections to providers once: every composition resolving through one
/// resolver reuses the provider of a selection it already resolved; an unseen selection,
/// or a new resolver over a changed catalog, resolves anew.
#[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
pub struct ModelResolver {
    default: String,
    resolve: Box<dyn Fn(&str) -> Result<ModelBinding> + Send + Sync>,
    resolved: std::sync::Mutex<std::collections::BTreeMap<String, ModelBinding>>,
}

#[cfg(any(test, feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
impl ModelResolver {
    /// Resolves through `resolve`, selecting `default` when nothing else selects a model.
    pub fn new(
        default: impl Into<String>,
        resolve: impl Fn(&str) -> Result<ModelBinding> + Send + Sync + 'static,
    ) -> Self {
        Self {
            default: default.into(),
            resolve: Box::new(resolve),
            resolved: Default::default(),
        }
    }

    fn resolve(&self, selection: &str) -> Result<ModelBinding> {
        let mut resolved = self
            .resolved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(model) = resolved.get(selection) {
            return Ok(model.clone());
        }
        let model = (self.resolve)(selection)?;
        resolved.insert(selection.into(), model.clone());
        Ok(model)
    }
}

#[cfg(all(test, not(loom)))]
mod resolver_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // 1k compositions over one resolver construct each selection's provider
    // once; a changed selection, here a changed settings model, resolves anew.
    #[test]
    fn shared_resolver_reuses_providers_and_resolves_changed_models_anew() -> Result<()> {
        let constructed = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&constructed);
        let models = Arc::new(ModelResolver::new("first", move |selection| {
            counter.fetch_add(1, Ordering::Relaxed);
            let provider: Arc<dyn ModelProvider> =
                Arc::new(crate::test_support::ScriptedModelProvider::new(Vec::new()));
            Ok((
                Model::new(
                    "test".into(),
                    selection.into(),
                    "1".into(),
                    serde_json::Value::Null,
                )?,
                provider,
            ))
        }));
        let workspace = tempfile::tempdir().map_err(storage)?;
        let codex = tempfile::tempdir().map_err(storage)?;
        let settings = || {
            crate::native_settings::SettingsConfig::new(workspace.path(), workspace.path())
                .codex_home(codex.path())
        };
        let selected = |config: LocalHarnessConfig| {
            config
                .model
                .map(|(model, _)| model.name)
                .ok_or_else(|| Error::Invalid("model unresolved".into()))
        };
        for _ in 0..1000 {
            let config = LocalHarnessConfig::resolving(workspace.path(), Arc::clone(&models), None);
            assert_eq!(selected(config.resolve(&settings(), true)?)?, "first");
        }
        assert_eq!(constructed.load(Ordering::Relaxed), 1);
        std::fs::write(codex.path().join("config.toml"), "model = \"second\"\n")
            .map_err(storage)?;
        let config = LocalHarnessConfig::resolving(workspace.path(), Arc::clone(&models), None);
        assert!(selected(config.resolve(&settings(), true)?)?.ends_with("second"));
        let explicit = LocalHarnessConfig::resolving(
            workspace.path(),
            Arc::clone(&models),
            Some("first".into()),
        );
        assert_eq!(selected(explicit.resolve(&settings(), true)?)?, "first");
        assert_eq!(constructed.load(Ordering::Relaxed), 2);
        Ok(())
    }
}
