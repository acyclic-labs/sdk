use super::*;

/// Immutable runtime bindings that every agent turn context shares.
pub struct AgentHarness {
    pub(crate) workspace_tools: Option<Arc<dyn crate::tool::ToolExecutor>>,
    pub(super) scope: RuntimeScope,
    pub(super) content: Option<ContentBindings>,
    pub(super) artifacts: Option<ContentBindings>,
}

/// Provider-owned, scope-bound file handles. The reader verifies every exact
/// version it returns; the writer publishes only to its own volume.
#[derive(Clone)]
pub struct ContentBindings {
    /// Authenticated, exact-version read and residency boundary.
    pub reader: Arc<dyn ContentResidencyVerifier>,
    /// Authenticated writer for one volume; read-only agents omit it.
    pub writer: Option<Arc<dyn ContentPublisher>>,
}

/// Runtime bindings assembled before agent turns run.
#[derive(Default)]
pub struct Bindings {
    /// Executor for the agent's selected checkout or fork view.
    pub workspace_tools: Option<Arc<dyn crate::tool::ToolExecutor>>,
    /// Effective authority and bounds.
    pub scope: RuntimeScope,
    /// Optional owner-bound content access for tools.
    pub content: Option<ContentBindings>,
    /// Independently selected immutable artifact storage.
    pub artifacts: Option<ContentBindings>,
}

impl Bindings {
    /// Starts a local composition with no implicit authority or providers.
    #[must_use]
    pub fn local() -> Self {
        Self::default()
    }

    /// Validates the immutable composition.
    pub fn build(self) -> Result<Arc<AgentHarness>> {
        self.scope.limits.validate(&mut None)?;
        Ok(Arc::new(AgentHarness {
            workspace_tools: self.workspace_tools,
            scope: self.scope,
            content: self.content,
            artifacts: self.artifacts,
        }))
    }
}

impl AgentHarness {
    /// The context for one agent turn. Its scope is this runtime's (tools,
    /// limits, run limits) plus the agent's volume `grants`; the agent's
    /// private volume reads and writes through `private`, and other volumes
    /// keep this runtime's own content bindings.
    pub fn agent_context(
        self: &Arc<Self>,
        grants: &Capabilities,
        private: ContentBindings,
    ) -> Result<TaskContext> {
        if private.writer.is_none() {
            return Err(Error::Invalid("agent private content has no writer".into()));
        }
        let mut scope = self.scope.clone();
        scope.grants = Capabilities::new(scope.grants.iter().chain(grants.iter()));
        Ok(TaskContext {
            harness: Arc::clone(self),
            scope,
            output_content: private,
            journal: None,
        })
    }
}
