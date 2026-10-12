use super::*;

/// Immutable typed runtime assembled from task/tool registries and providers.

pub struct AgentHarness {
    pub(super) tasks: TaskRegistry,
    pub(super) tools: ToolRegistry,
    pub(super) resumable_tools: ResumableToolRegistry,
    pub(super) live: TaskGroup,
    pub(super) concurrency: usize,
    pub(super) scope: RuntimeScope,
    pub(super) extension_runtime: Option<Arc<ExtensionRuntime>>,
    pub(super) host: Option<Arc<dyn DurableTaskHost>>,
    pub(super) state: Option<Arc<dyn TaskStateProvider>>,
    pub(super) spawner: Option<Arc<dyn TaskSpawner>>,
    pub(super) execution: Option<Arc<dyn ExecutionProvider>>,
    pub(super) interactions: Option<Arc<dyn InteractionRouter>>,
    pub(super) interaction_resolver: Option<Arc<dyn InteractionResolver>>,
    pub(super) content: Option<ContentBindings>,
    pub(super) artifacts: Option<ContentBindings>,
    pub(super) policy: Option<Arc<dyn ToolPolicy>>,
    pub(super) policy_identity: Option<ComponentIdentity>,
    pub(super) model: Option<ModelBinding>,
    pub(super) context: ContextPipeline,
    pub(super) fork_preparer: Option<ForkBinding>,
    pub(super) workspaces: Option<Arc<dyn crate::merge::ProjectWorkspaceProvider>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_configuration_custody: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for AgentHarness {
    fn clone(&self) -> Self {
        Self {
            tasks: self.tasks.clone(), tools: self.tools.clone(),
            resumable_tools: self.resumable_tools.clone(), live: self.live.clone(),
            concurrency: self.concurrency, scope: self.scope.clone(),
            extension_runtime: self.extension_runtime.clone(), host: self.host.clone(),
            state: self.state.clone(), spawner: self.spawner.clone(),
            execution: self.execution.clone(), interactions: self.interactions.clone(),
            interaction_resolver: self.interaction_resolver.clone(), content: self.content.clone(),
            artifacts: self.artifacts.clone(), policy: self.policy.clone(),
            policy_identity: self.policy_identity.clone(), model: self.model.clone(),
            context: self.context.clone(), fork_preparer: self.fork_preparer.clone(),
            workspaces: self.workspaces.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_configuration_custody: None,
        }
    }
}

#[derive(Clone)]
pub(super) struct ModelBinding {
    pub(super) model: Model,
    pub(super) provider: Arc<dyn ModelProvider>,
}

#[derive(Clone)]
pub(super) struct ForkBinding {
    pub(super) parent: crate::core::Authority,
    pub(super) revision: u64,
    pub(super) preparer: Arc<dyn crate::fork::ForkPreparer>,
}

/// Provider-owned, scope-bound file handles.

#[derive(Clone)]
pub struct ContentBindings {
    /// Authenticated, exact-version read and residency boundary.
    pub reader: Arc<dyn ContentResidencyVerifier>,
    /// Authenticated writer for one volume; read-only agents omit it.
    pub writer: Option<Arc<dyn ContentPublisher>>,
}

/// Artifact access uses immutable file references.

pub type ArtifactBindings = ContentBindings;

/// Immutable admission bindings assembled before tasks can run.

pub struct Bindings {
    /// Version-pinned typed task definitions.
    pub tasks: TaskRegistry,
    /// Version-pinned tool definitions and executors.
    pub tools: ToolRegistry,
    /// Exact resumable tool implementations, never substituted by live executors.
    pub resumable_tools: ResumableToolRegistry,
    /// Effective authority and bounds.
    pub scope: RuntimeScope,
    /// Maximum simultaneously running local members in one group.
    pub concurrency: usize,
    /// Optional durable scheduler and effect/wait boundary.
    pub durable_host: Option<Arc<dyn DurableTaskHost>>,
    /// Owner-bound retained task state and observation, independent of admission.
    pub state: Option<Arc<dyn TaskStateProvider>>,
    /// Independently replaceable durable admission and batch boundary.
    pub spawner: Option<Arc<dyn TaskSpawner>>,
    /// Qualified route packaging a compatible task build, spawner, and state
    pub execution: Option<Arc<dyn ExecutionProvider>>,
    /// Optional local interaction router.
    pub interactions: Option<Arc<dyn InteractionRouter>>,
    /// Owner-mediated, signed CAS interaction resolution.
    pub interaction_resolver: Option<Arc<dyn InteractionResolver>>,
    /// Optional owner-bound content access for tasks and tools.
    pub content: Option<ContentBindings>,
    /// Independently selected immutable artifact storage.
    pub artifacts: Option<ArtifactBindings>,
    /// Optional invocation policy; absence permits calls with explicit grants.
    pub policy: Option<Arc<dyn ToolPolicy>>,
    /// Parent-bound fork capture; publication remains with the owning aggregate.
    pub fork_preparer: Option<Arc<dyn crate::fork::ForkPreparer>>,
    /// Parent-bound project fork and inspected join provider.
    pub workspaces: Option<Arc<dyn crate::merge::ProjectWorkspaceProvider>>,
}

impl Bindings {
    /// Starts a local composition with no implicit authority or providers.
    #[must_use]
    pub fn local() -> Self {
        Self {
            tasks: TaskRegistry::default(),
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::default(),
            concurrency: tokio::sync::Semaphore::MAX_PERMITS,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
    }

    /// Selects a qualified execution provider for new durable admissions.
    #[must_use]
    pub fn execution(mut self, value: Arc<dyn ExecutionProvider>) -> Self {
        self.execution = Some(value);
        self
    }

    /// Validates all dependency edges and seals the runtime composition.
    pub fn build(self) -> Result<Arc<AgentHarness>> {
        if self.fork_preparer.is_some() && !self.scope.grants().contains(capability::FORK_PUBLISH) {
            return Err(Error::Unauthorized(
                "fork preparer requires fork:publish in the root scope".into(),
            ));
        }
        if self.execution.is_some() && self.durable_host.is_some() {
            return Err(Error::Conflict(
                "execution route and legacy durable host are both bound".into(),
            ));
        }
        let execution = self.execution;
        let state = self
            .state
            .or_else(|| execution.as_ref().map(|route| route.state()));
        let spawner = self
            .spawner
            .or_else(|| execution.as_ref().map(|route| route.spawner()));
        let host_available = self.durable_host.is_some();
        let state_available = host_available || state.is_some();
        let spawner_available = host_available || spawner.is_some();
        let durable_ready = host_available || (state_available && spawner_available);
        let runtime = AgentHarness::with_policy_internal(
            self.tasks,
            self.tools,
            self.resumable_tools,
            self.scope,
            self.concurrency,
            self.durable_host,
            self.interactions,
            self.content,
            self.artifacts,
            self.policy,
            durable_ready,
            state_available,
            spawner_available,
            None,
        )?;
        let runtime = match state {
            Some(state) => runtime.bind_state(state)?,
            None => runtime,
        };
        let runtime = match spawner {
            Some(spawner) => runtime.bind_spawner(spawner)?,
            None => runtime,
        };
        let runtime = match execution {
            Some(execution) => runtime.bind_execution(execution)?,
            None => runtime,
        };
        let runtime = match self.interaction_resolver {
            Some(resolver) => runtime.bind_interaction_resolver(resolver),
            None => runtime,
        };
        let runtime = match self.fork_preparer {
            Some(preparer) => runtime.bind_fork_preparer(preparer)?,
            None => runtime,
        };
        Ok(match self.workspaces {
            Some(workspaces) => runtime.bind_workspaces(workspaces)?,
            None => runtime,
        })
    }
}

impl Default for Bindings {
    fn default() -> Self {
        Self::local()
    }
}
