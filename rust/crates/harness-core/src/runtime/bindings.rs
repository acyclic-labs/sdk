use super::*;

/// Immutable runtime bindings that every agent turn context shares.
pub struct AgentHarness {
    pub(crate) targets: Option<Arc<dyn super::TargetResolver>>,
    pub(crate) filesystem: Option<Arc<dyn FilesystemExecutor>>,
    pub(super) scope: RuntimeScope,
    pub(super) content: Option<ContentBindings>,
    pub(super) artifacts: Option<ContentBindings>,
    pub(super) seed: Option<crate::layers::PinnedLayer>,
    pub(super) configuration: [Option<crate::layers::PinnedLayer>; 4],
    pub(super) system: Option<crate::layers::PinnedLayer>,
}

/// Filesystem commands bound to one provider and immutable volume-name mapping.
/// The identity is recorded before dispatch; it is routing evidence, never a grant.
pub trait FilesystemExecutor: crate::tool::ToolExecutor {
    /// Identity of the complete provider and volume-name binding.
    fn binding_identity(&self) -> [u8; 32];
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
    /// The agent's execution targets; its home runs its checkout or fork view.
    pub targets: Option<Arc<dyn super::TargetResolver>>,
    /// Provider-backed filesystem commands, independently of machine targets.
    pub filesystem: Option<Arc<dyn FilesystemExecutor>>,
    /// Effective authority and bounds.
    pub scope: RuntimeScope,
    /// Optional owner-bound content access for tools.
    pub content: Option<ContentBindings>,
    /// Independently selected immutable artifact storage.
    pub artifacts: Option<ContentBindings>,
    /// The read-only seed layer under every agent's home (see
    /// [`crate::seed`]), read through the content reader under the scope's
    /// read grants. Without it the effective view is the home alone.
    pub seed: Option<crate::layers::PinnedLayer>,
    /// Original read-only user/org, scope, template and parent sources, in
    /// that order. Reads retain each supplied generation and ordinary source
    /// grants; an absent source contributes no files or authority.
    pub configuration: [Option<crate::layers::PinnedLayer>; 4],
    /// Host-owned read-only system files, shared and pinned for this run.
    pub system: Option<crate::layers::PinnedLayer>,
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
        for layer in self
            .seed
            .iter()
            .chain(self.configuration.iter().flatten())
            .chain(self.system.iter())
        {
            layer.validate()?;
        }
        let layers = self
            .seed
            .iter()
            .chain(self.configuration.iter().flatten())
            .chain(self.system.iter());
        for (index, layer) in layers.clone().enumerate() {
            if layers.clone().take(index).any(|earlier| {
                earlier.volume == layer.volume && earlier.generation != layer.generation
            }) {
                return Err(Error::Invalid(
                    "read-only sources cannot select different generations of the same volume"
                        .into(),
                ));
            }
        }
        Ok(Arc::new(AgentHarness {
            targets: self.targets,
            filesystem: self.filesystem,
            scope: self.scope,
            content: self.content,
            artifacts: self.artifacts,
            seed: self.seed,
            configuration: self.configuration,
            system: self.system,
        }))
    }
}

impl AgentHarness {
    /// Original immutable issuer sources, in increasing authority order.
    pub fn configuration_layers(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&'static str, &crate::layers::PinnedLayer)> {
        use crate::layers::{PARENT_PATH, SCOPE_PATH, SEED_PATH, TEMPLATE_PATH, USER_ORG_PATH};
        self.seed
            .as_ref()
            .map(|layer| (SEED_PATH, layer))
            .into_iter()
            .chain(
                [USER_ORG_PATH, SCOPE_PATH, TEMPLATE_PATH, PARENT_PATH]
                    .into_iter()
                    .zip(self.configuration.iter())
                    .filter_map(|(path, layer)| layer.as_ref().map(|layer| (path, layer))),
            )
    }

    pub(super) fn read_only_layers(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&'static str, &crate::layers::PinnedLayer)> {
        self.configuration_layers().chain(
            self.system
                .as_ref()
                .map(|layer| (crate::layers::SYSTEM_PATH, layer)),
        )
    }

    /// Selects an agent's execution targets, project and retained home together.
    /// Providers must already be bound to the selected agent's authority.
    pub fn with_workspace(
        &self,
        targets: Option<Arc<dyn super::TargetResolver>>,
        content: ContentBindings,
        home: ContentBindings,
    ) -> Result<Arc<Self>> {
        content.validate()?;
        home.validate()?;
        Ok(Arc::new(Self {
            targets,
            filesystem: None,
            content: Some(content),
            scope: self.scope.clone(),
            artifacts: Some(home),
            seed: self.seed.clone(),
            configuration: self.configuration.clone(),
            system: self.system.clone(),
        }))
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
        private.validate()?;
        if self
            .read_only_layers()
            .any(|(_, layer)| layer.volume == private.volume)
        {
            return Err(Error::Invalid(
                "writable home cannot also be an issuer configuration source".into(),
            ));
        }
        if private.writer.is_none() {
            return Err(Error::Invalid("agent private content has no writer".into()));
        }
        let mut scope = self.scope.clone();
        scope.grants = Capabilities::new(scope.grants.iter().chain(grants.iter()));
        Ok(TaskContext {
            harness: Arc::clone(self),
            scope,
            output_content: private,
            journal_output: None,
            journal: None,
            agent: None,
            children: None,
        })
    }
}
