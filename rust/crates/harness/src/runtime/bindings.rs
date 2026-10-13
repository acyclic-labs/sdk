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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_configuration_custody: Option<acyclic_fs::NativeAllocation>,
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

impl Clone for AgentHarness {
    fn clone(&self) -> Self {
        Self {
            tasks: self.tasks.clone(),
            tools: self.tools.clone(),
            resumable_tools: self.resumable_tools.clone(),
            live: self.live.clone(),
            concurrency: self.concurrency,
            scope: self.scope.clone(),
            extension_runtime: self.extension_runtime.clone(),
            host: self.host.clone(),
            state: self.state.clone(),
            spawner: self.spawner.clone(),
            execution: self.execution.clone(),
            interactions: self.interactions.clone(),
            interaction_resolver: self.interaction_resolver.clone(),
            content: self.content.clone(),
            artifacts: self.artifacts.clone(),
            policy: self.policy.clone(),
            policy_identity: self.policy_identity.clone(),
            model: self.model.clone(),
            context: self.context.clone(),
            fork_preparer: self.fork_preparer.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_configuration_custody: None,
        }
    }
}

impl ForkBinding {
    pub(super) fn new(
        preparer: Arc<dyn crate::fork::ForkPreparer>,
        startup: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let (parent, revision) = preparer.parent_snapshot();
        if parent.kind != crate::core::AggregateKind::Conversation || revision == 0 {
            return Err(crate::conversation::content_error(
                startup,
                crate::wire::ErrorCode::Invalid,
                "fork preparer needs a bound parent conversation revision",
            ));
        }
        let parent = crate::executor::with_history_io(startup, |work| {
            let path = parent.stream_path(work)?;
            let capacity = path.capacity() as u64;
            drop(path);
            if let Some(work) = work.as_deref_mut() {
                work.release_owned(capacity)?;
            }
            match work.as_deref_mut() {
                Some(work) => work.clone_authority(parent),
                None => Ok(parent.clone()),
            }
        })?;
        Ok(Self {
            parent,
            revision,
            preparer,
        })
    }
}

/// Provider-owned, scope-bound file handles. The reader verifies every exact
#[derive(Clone)]
pub struct ContentBindings {
    /// Authenticated, exact-version read and residency boundary.
    pub reader: Arc<dyn ContentResidencyVerifier>,
    /// Authenticated writer for one volume; read-only agents omit it.
    pub writer: Option<Arc<dyn ContentPublisher>>,
}

/// Immutable admission bindings assembled before tasks can run. Each optional
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
    pub artifacts: Option<ContentBindings>,
    /// Optional invocation policy; absence permits calls with explicit grants.
    pub policy: Option<Arc<dyn ToolPolicy>>,
    /// Parent-bound fork capture; publication remains with the owning aggregate.
    pub fork_preparer: Option<Arc<dyn crate::fork::ForkPreparer>>,
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
        }
    }

    /// Validates the immutable composition under caller IO limits.
    pub fn build(
        self,
        startup: Option<&crate::conversation::ContentIo>,
    ) -> Result<Arc<AgentHarness>> {
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        if startup.is_some() {
            return Err(Error::Unsupported(
                "native startup is unavailable on this platform".into(),
            ));
        }
        let native = task_original_work(startup)?;
        if self.fork_preparer.is_some() && !self.scope.grants().contains(capability::FORK_PUBLISH) {
            return Err(crate::conversation::content_error(
                startup,
                crate::wire::ErrorCode::Unauthorized,
                "fork preparer requires fork:publish in the root scope",
            ));
        }
        if self.execution.is_some() && self.durable_host.is_some() {
            return Err(crate::conversation::content_error(
                startup,
                crate::wire::ErrorCode::Conflict,
                "execution route and durable host are both bound",
            ));
        }
        if native.is_some()
            && self.execution.is_some()
            && (self.state.is_none() || self.spawner.is_none())
        {
            return Err(crate::conversation::content_error(
                startup,
                crate::wire::ErrorCode::Unsupported,
                "native route requires owned state and spawner bindings",
            ));
        }
        let durable_ready = self.durable_host.is_some()
            || self.execution.is_some()
            || (self.state.is_some() && self.spawner.is_some());
        if self.concurrency == 0 {
            return Err(crate::conversation::content_error(
                startup,
                crate::wire::ErrorCode::Invalid,
                "runtime concurrency must be positive",
            ));
        }
        crate::model::charge_items(startup, 1)?;
        crate::executor::with_history_io(startup, |work| self.scope.run_limits().validate(work))?;
        let mut policy_identity = self
            .policy
            .as_ref()
            .filter(|_| native.is_none())
            .map(|policy| policy.identity());
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let (Some(work), Some(policy)) = (native, &self.policy) {
            policy.supports_native_policy(work)?;
            let identity = policy.identity_with_native_work(work)?;
            check_policy_metadata(&identity.0, &identity.1, work)?;
            validate_policy_identity(&identity.0, startup)?;
            startup
                .expect("native startup observer")
                .absorb_native_custody(identity.1)?;
            policy_identity = Some(identity.0);
        }
        if native.is_none()
            && let Some(identity) = &policy_identity
        {
            validate_policy_identity(identity, startup)?;
        }
        if self.resumable_tools.definitions().next().is_some() && !durable_ready {
            return Err(crate::conversation::content_error(
                startup,
                crate::wire::ErrorCode::Invalid,
                "resumable tools require durable task admission and state",
            ));
        }
        for definition in self.resumable_tools.definitions() {
            crate::model::charge_items(startup, 1)?;
            if let Some(adapter) =
                self.tools
                    .get(&definition.name, Some(&definition.revision), startup)?
            {
                let pinned = crate::executor::with_history_io(startup, |work| {
                    self.resumable_tools
                        .get(&definition.name, &definition.revision, work)
                })?
                .ok_or_else(|| {
                    crate::conversation::content_error(
                        startup,
                        crate::wire::ErrorCode::Invalid,
                        "resumable registry differs from its definitions",
                    )
                })?;
                let machine = task_machine_identity(pinned.machine.as_ref(), startup)?;
                if adapter.definition != *definition
                    || adapter.executor.resumable_machine_identity(native)? != Some(machine)
                {
                    return Err(crate::conversation::content_error(
                        startup,
                        crate::wire::ErrorCode::Conflict,
                        "live and resumable implementations differ",
                    ));
                }
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let dependency_io = native
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let dependency_observer = dependency_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let dependency_observer = None;
        super::dependencies::validate_task_dependencies(&self, dependency_observer)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        drop(dependency_io);
        let execution = self.execution;
        let bound_state = self
            .state
            .or_else(|| execution.as_ref().map(|route| route.state()));
        let bound_spawner = self
            .spawner
            .or_else(|| execution.as_ref().map(|route| route.spawner()));
        let Self {
            tasks,
            tools,
            resumable_tools,
            scope,
            concurrency,
            durable_host: host,
            interactions,
            interaction_resolver,
            content,
            artifacts,
            policy,
            fork_preparer,
            ..
        } = self;
        let concurrency = scope.concurrency_bound(concurrency);
        let spawner = bound_spawner.or_else(|| {
            host.as_ref()
                .map(|host| Arc::clone(host) as Arc<dyn TaskSpawner>)
        });
        let state = bound_state.or_else(|| {
            host.as_ref()
                .map(|host| Arc::clone(host) as Arc<dyn TaskStateProvider>)
        });
        if spawner.is_some() && state.is_none() {
            return Err(crate::conversation::content_error(
                startup,
                crate::wire::ErrorCode::Invalid,
                "durable spawner requires a state/observation host",
            ));
        }
        if let Some(route) = &execution {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let identity = match native {
                Some(work) => std::borrow::Cow::Borrowed(route.identity_with_native_work(work)?),
                None => std::borrow::Cow::Owned(route.identity()),
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let identity = route.identity();
            validate_policy_identity(&identity, startup)?;
        }
        let fork_preparer = fork_preparer
            .map(|preparer| ForkBinding::new(preparer, startup))
            .transpose()?;
        let mut runtime = AgentHarness {
            extension_runtime: scope.extension_runtime(),
            live: TaskGroup::new(concurrency, native)?,
            tasks,
            tools,
            resumable_tools,
            scope,
            concurrency,
            host,
            state,
            spawner,
            execution,
            interactions,
            interaction_resolver,
            content,
            artifacts,
            policy,
            policy_identity,
            model: None,
            context: ContextPipeline::default(),
            fork_preparer,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_configuration_custody: None,
        };
        runtime.assert_durable_policy_bindings(native)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = startup {
            io.retain_owned(std::mem::size_of::<AgentHarness>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<AgentHarness>() as u64)?;
            runtime.native_configuration_custody = Some(io.take_native_custody(io.owned_bytes()?)?);
        }
        Ok(Arc::new(runtime))
    }
}

impl Default for Bindings {
    fn default() -> Self {
        Self::local()
    }
}
