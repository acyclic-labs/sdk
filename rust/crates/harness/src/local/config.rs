use super::*;

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
    pub(super) model: (Model, Arc<dyn ModelProvider>),
    pub(super) context_manifest: Option<crate::conversation::FileRef>,
    pub(super) tools: Vec<Tool>,
    pub(super) default_tools: bool,
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
        Self {
            goal_continuations: Some(Default::default()),
            project: LocalProject::Workspace(workspace.into()),
            state_directory: None,
            model,
            context_manifest: None,
            tools: Vec::new(),
            default_tools: true,
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
        self
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
