use super::*;

/// Storage selected for the agent's project view.
pub enum LocalProject {
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

impl From<PathBuf> for LocalProject {
    fn from(path: PathBuf) -> Self {
        Self::Workspace(path)
    }
}
impl From<&Path> for LocalProject {
    fn from(path: &Path) -> Self {
        Self::Workspace(path.into())
    }
}
impl From<&PathBuf> for LocalProject {
    fn from(path: &PathBuf) -> Self {
        Self::Workspace(path.clone())
    }
}

impl From<&str> for LocalProject {
    fn from(path: &str) -> Self {
        Self::Workspace(path.into())
    }
}
impl From<String> for LocalProject {
    fn from(path: String) -> Self {
        Self::Workspace(path.into())
    }
}

/// Durable local composition with a required model binding.
pub struct LocalHarnessConfig {
    pub(super) project: LocalProject,
    pub(super) model: (Model, Arc<dyn ModelProvider>),
    pub(super) context: ContextPipeline,
    pub(super) tools: Vec<Tool>,
    pub(super) default_tools: bool,
    pub(super) limits: Limits,
    pub(super) run_limits: TaskRunLimits,
    pub(super) session_limits: SessionLimits,
    pub(super) concurrency: usize,
    pub(super) compaction: Option<crate::context::CompactionPolicy>,
    pub(super) clock: Arc<dyn acyclic_stream::UnixMillisClock>,
}

impl LocalHarnessConfig {
    /// Selects a checkout or configured volume and its stock model binding.
    #[must_use]
    pub fn new(project: impl Into<LocalProject>, model: (Model, Arc<dyn ModelProvider>)) -> Self {
        Self {
            project: project.into(),
            model,
            context: ContextPipeline::default(),
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
            clock: Arc::new(acyclic_stream::SystemUnixMillisClock),
        }
    }
    /// Selects the stock model's context pipeline.
    #[must_use]
    pub fn context(mut self, context: ContextPipeline) -> Self {
        self.context = context;
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
    /// Selects the clock used by durable admission and leases.
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn acyclic_stream::UnixMillisClock>) -> Self {
        self.clock = clock;
        self
    }
}
