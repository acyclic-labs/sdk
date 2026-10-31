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
    /// Selected volume identity, independent of whether publication is allowed.
    pub volume: VolumeRef,
    /// Authenticated, exact-version read and residency boundary.
    pub reader: Arc<dyn ContentResidencyVerifier>,
    /// Authenticated writer for one volume; read-only agents omit it.
    pub writer: Option<Arc<dyn ContentPublisher>>,
}

impl ContentBindings {
    fn validate(&self) -> Result<()> {
        self.volume.validate(&mut None)?;
        if self
            .writer
            .as_ref()
            .is_some_and(|writer| writer.volume() != &self.volume)
        {
            return Err(Error::Invalid(
                "content writer belongs to another selected volume".into(),
            ));
        }
        Ok(())
    }
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
        if let Some(content) = &self.content {
            content.validate()?;
        }
        if let Some(artifacts) = &self.artifacts {
            artifacts.validate()?;
        }
        Ok(Arc::new(AgentHarness {
            workspace_tools: self.workspace_tools,
            scope: self.scope,
            content: self.content,
            artifacts: self.artifacts,
        }))
    }
}

impl AgentHarness {
    /// Selects an agent's workspace executor and project content together,
    /// preserving this composition's scope and independent artifact storage.
    /// The providers must already be bound to the selected project authority.
    pub fn with_workspace(
        &self,
        workspace_tools: Option<Arc<dyn crate::tool::ToolExecutor>>,
        content: ContentBindings,
    ) -> Result<Arc<Self>> {
        content.validate()?;
        Ok(Arc::new(Self {
            workspace_tools,
            content: Some(content),
            scope: self.scope.clone(),
            artifacts: self.artifacts.clone(),
        }))
    }

    /// The context for one agent turn. Its scope is this runtime's (tools,
    /// limits, run limits) plus the agent's volume `grants`; the agent's
    /// private volume reads and writes through `private`, and other volumes
    /// keep this runtime's own content bindings.
    pub fn agent_context(
        self: &Arc<Self>,
        grants: &Capabilities,
        private: ContentBindings,
    ) -> Result<TaskContext> {
        private.validate()?;
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
            agent: None,
        })
    }
}
