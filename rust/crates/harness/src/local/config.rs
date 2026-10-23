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
    pub(super) goal_continuations: Option<crate::goal::GoalContinuationPolicy>,
    pub(super) project: LocalProject,
    pub(super) state_directory: Option<PathBuf>,
    /// Refuses ambient state and settings discovery; set only by `hermetic()`.
    pub(super) hermetic: bool,
    /// Test-owned state root, retained by every store opened in it.
    pub(super) test_state: Option<acyclic_native_runtime::OwnershipAnchor>,
    pub(super) model: Option<(Model, Arc<dyn ModelProvider>)>,
    pub(super) context_manifest: Option<crate::conversation::FileRef>,
    pub(super) tools: Vec<Tool>,
    pub(super) default_tools: bool,
    #[cfg(feature = "search-http")]
    pub(super) search_provider: Option<Arc<dyn crate::search::SearchProvider>>,
    #[cfg(feature = "search-http")]
    pub(super) web_tools: bool,
    pub(super) limits: Limits,
    pub(super) run_limits: TaskRunLimits,
    pub(super) session_limits: SessionLimits,
    pub(super) concurrency: usize,
    pub(super) compaction: Option<crate::context::CompactionPolicy>,
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    pub(crate) settings: Option<crate::native_settings::SettingsConfig>,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    pub(super) mcp_limits: crate::mcp::McpClientLimits,
    pub(super) clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    #[cfg(feature = "native-execution")]
    pub(crate) environment_platform: crate::native_environment::PlatformPolicy,
}

#[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
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

    /// Opens workspace coordination without a model, tools, settings or agent execution.
    /// The trusted harness owner must start this outside the native host sandbox.
    #[cfg(feature = "native-execution")]
    #[must_use]
    pub fn native_host(workspace: impl Into<PathBuf>) -> Self {
        let mut config = Self::configuration(workspace.into(), None);
        config.default_tools = false;
        config.hermetic = true;
        config.goal_continuations = None;
        config.compaction = Some(crate::context::CompactionPolicy::Disabled);
        config
    }

    fn configuration(workspace: PathBuf, model: Option<(Model, Arc<dyn ModelProvider>)>) -> Self {
        Self {
            goal_continuations: Some(Default::default()),
            project: LocalProject::Workspace(workspace),
            state_directory: None,
            hermetic: false,
            test_state: None,
            model,
            context_manifest: None,
            tools: Vec::new(),
            default_tools: true,
            #[cfg(feature = "search-http")]
            search_provider: None,
            #[cfg(feature = "search-http")]
            web_tools: false,
            limits: Limits::default(),
            run_limits: TaskRunLimits::default(),
            session_limits: SessionLimits {
                active_tasks: 16,
                total_tasks: 4096,
                depth: 16,
                model_steps: 65_536,
            },
            concurrency: 1,
            compaction: None,
            #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
            settings: None,
            #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
            mcp_limits: crate::mcp::McpClientLimits::default(),
            clock: Arc::new(acyclic_stream::SystemUnixMillisClock),
            #[cfg(feature = "native-execution")]
            environment_platform: crate::native_environment::PlatformPolicy::Native,
        }
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
    /// Disables the default set, retaining every custom tool.
    #[must_use]
    pub fn without_default_tools(mut self) -> Self {
        self.default_tools = false;
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
    /// Selects durable admission bounds.
    #[must_use]
    pub fn session_limits(mut self, limits: SessionLimits) -> Self {
        self.session_limits = limits;
        self
    }
    /// Selects the finite goal admission policy; `None` disables automatic continuations.
    #[must_use]
    pub fn goal_continuations(
        mut self,
        policy: Option<crate::goal::GoalContinuationPolicy>,
    ) -> Self {
        self.goal_continuations = policy;
        self
    }
    /// Selects maximum concurrent local tasks.
    #[must_use]
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
    /// Selects an explicit compaction policy, validated without changing its budgets.
    #[must_use]
    pub fn compaction(mut self, policy: crate::context::CompactionPolicy) -> Self {
        self.compaction = Some(policy);
        self
    }
    /// Selects native settings locations and explicit project trust using the shared loader.
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
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
