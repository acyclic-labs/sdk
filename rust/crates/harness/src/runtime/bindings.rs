use super::*;

/// Immutable typed runtime assembled from task/tool registries and providers.
pub struct AgentHarness {
    pub(super) tasks: TaskRegistry,
    pub(super) tools: ToolRegistry,
    pub(crate) workspace_tools: Option<Arc<dyn crate::tool::ToolExecutor>>,
    pub(super) resumable_tools: ResumableToolRegistry,
    pub(super) live: TaskGroup,
    pub(super) concurrency: usize,
    pub(super) scope: RuntimeScope,
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
            workspace_tools: self.workspace_tools.clone(),
            resumable_tools: self.resumable_tools.clone(),
            live: self.live.clone(),
            concurrency: self.concurrency,
            scope: self.scope.clone(),
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
    /// Executor for the task's selected checkout or fork view.
    pub workspace_tools: Option<Arc<dyn crate::tool::ToolExecutor>>,
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
            workspace_tools: None,
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

    fn validate_configuration(
        &self,
        startup: Option<&crate::conversation::ContentIo>,
        native: Option<&NativeWork>,
    ) -> Result<()> {
        for (invalid, code, reason) in [
            (
                self.fork_preparer.is_some()
                    && !self.scope.grants().contains(capability::FORK_PUBLISH),
                crate::wire::ErrorCode::Unauthorized,
                "fork preparer requires fork:publish in the root scope",
            ),
            (
                self.execution.is_some() && self.durable_host.is_some(),
                crate::wire::ErrorCode::Conflict,
                "execution route and durable host are both bound",
            ),
            (
                native.is_some()
                    && self.execution.is_some()
                    && (self.state.is_none() || self.spawner.is_none()),
                crate::wire::ErrorCode::Unsupported,
                "native route requires owned state and spawner bindings",
            ),
            (
                self.concurrency == 0,
                crate::wire::ErrorCode::Invalid,
                "runtime concurrency must be positive",
            ),
        ] {
            if invalid {
                return Err(crate::conversation::content_error(startup, code, reason));
            }
        }
        crate::model::charge_items(startup, 1)?;
        crate::executor::with_history_io(startup, |work| self.scope.run_limits().validate(work))?;
        Ok(())
    }

    fn validate_task_bindings(
        &self,
        startup: Option<&crate::conversation::ContentIo>,
        native: Option<&NativeWork>,
    ) -> Result<()> {
        let durable_ready = self.durable_host.is_some()
            || self.execution.is_some()
            || (self.state.is_some() && self.spawner.is_some());
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
        super::dependencies::validate_task_dependencies(
            self.tasks.0.len(),
            |index| {
                let definition = self.tasks.0.get(index).ok_or_else(|| {
                    crate::conversation::content_error(
                        startup,
                        crate::wire::ErrorCode::Invalid,
                        "task dependency index is invalid",
                    )
                })?;
                let identity = definition.identity();
                Ok((&identity.name, &identity.version, definition.requirements()))
            },
            |requirement| super::dependencies::requirement_available(self, requirement, startup),
            startup,
        )?;
        Ok(())
    }

    fn validated_policy_identity(
        &self,
        startup: Option<&crate::conversation::ContentIo>,
        native: Option<&NativeWork>,
    ) -> Result<Option<ComponentIdentity>> {
        let policy_identity = self
            .policy
            .as_ref()
            .filter(|_| native.is_none())
            .map(|policy| policy.identity());
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let policy_identity = if let (Some(work), Some(policy)) = (native, &self.policy) {
            let (identity, custody) = policy.identity_with_native_work(work)?;
            check_policy_metadata(&identity, &custody, work)?;
            validate_policy_identity(&identity, startup)?;
            startup
                .ok_or_else(|| {
                    crate::conversation::content_error(
                        startup,
                        crate::wire::ErrorCode::Invalid,
                        "native startup observer is missing",
                    )
                })?
                .absorb_native_custody(custody)?;
            Some(identity)
        } else {
            policy_identity
        };
        if native.is_none()
            && let Some(identity) = &policy_identity
        {
            validate_policy_identity(identity, startup)?;
        }
        Ok(policy_identity)
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
        self.validate_configuration(startup, native)?;
        let policy_identity = self.validated_policy_identity(startup, native)?;
        self.validate_task_bindings(startup, native)?;
        let state = self
            .state
            .or_else(|| self.execution.as_ref().map(|route| route.state()))
            .or_else(|| {
                self.durable_host
                    .as_ref()
                    .map(|host| Arc::clone(host) as Arc<dyn TaskStateProvider>)
            });
        let spawner = self
            .spawner
            .or_else(|| self.execution.as_ref().map(|route| route.spawner()))
            .or_else(|| {
                self.durable_host
                    .as_ref()
                    .map(|host| Arc::clone(host) as Arc<dyn TaskSpawner>)
            });
        if spawner.is_some() && state.is_none() {
            return Err(crate::conversation::content_error(
                startup,
                crate::wire::ErrorCode::Invalid,
                "durable spawner requires a state/observation host",
            ));
        }
        if let Some(route) = &self.execution {
            let identity = match native {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Some(work) => std::borrow::Cow::Borrowed(route.identity_with_native_work(work)?),
                _ => std::borrow::Cow::Owned(route.identity()),
            };
            validate_policy_identity(&identity, startup)?;
        }
        let concurrency = self.scope.concurrency_bound(self.concurrency);
        let fork_preparer = self
            .fork_preparer
            .map(|preparer| ForkBinding::new(preparer, startup))
            .transpose()?;
        let runtime = AgentHarness {
            live: TaskGroup::new(concurrency, native)?,
            tasks: self.tasks,
            tools: self.tools,
            workspace_tools: self.workspace_tools,
            resumable_tools: self.resumable_tools,
            scope: self.scope,
            concurrency,
            host: self.durable_host,
            state,
            spawner,
            execution: self.execution,
            interactions: self.interactions,
            interaction_resolver: self.interaction_resolver,
            content: self.content,
            artifacts: self.artifacts,
            policy: self.policy,
            policy_identity,
            model: None,
            context: ContextPipeline::default(),
            fork_preparer,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_configuration_custody: None,
        };
        runtime.assert_durable_policy_bindings(native)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut runtime = runtime;
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
