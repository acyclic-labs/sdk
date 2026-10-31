use super::*;

/// Immutable runtime bindings that every agent turn context shares.
pub struct AgentHarness {
    pub(crate) targets: Option<Arc<dyn super::TargetResolver>>,
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
    /// The agent's execution targets; its home runs its checkout or fork view.
    pub targets: Option<Arc<dyn super::TargetResolver>>,
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
            targets: self.targets,
            scope: self.scope,
            content: self.content,
            artifacts: self.artifacts,
        }))
    }
}

impl AgentHarness {
    /// Selects an agent's execution targets and project content together,
    /// preserving this composition's scope and independent artifact storage.
    /// The providers must already be bound to the selected project authority.
    #[must_use]
    pub fn with_workspace(
        &self,
        targets: Option<Arc<dyn super::TargetResolver>>,
        content: ContentBindings,
    ) -> Arc<Self> {
        Arc::new(Self {
            targets,
            content: Some(content),
            scope: self.scope.clone(),
            artifacts: self.artifacts.clone(),
        })
    }

    /// The agent's execution targets, when any are bound.
    #[must_use]
    pub fn targets(&self) -> Option<&Arc<dyn super::TargetResolver>> {
        self.targets.as_ref()
    }

    /// The executor of the target `target` names, its home when omitted.
    ///
    /// # Errors
    /// [`Error::Unsupported`] when no targets are bound; [`Error::Target`]
    /// when the named one cannot take the call.
    pub fn target(&self, target: Option<&str>) -> Result<Arc<super::Target>> {
        self.targets
            .as_ref()
            .ok_or_else(|| Error::Unsupported("workspace tools are not bound".into()))?
            .resolve(target)
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
