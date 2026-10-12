use super::*;

impl AgentHarness {
/// Derives an immutable application view with no more authority or resources than its parent.

pub fn scoped(self: &Arc<Self>, grants: Capabilities, limits: Limits) -> Result<Arc<Self>> {
        let mut child = self.as_ref().clone();
        child.scope = self.scope.narrow(grants, limits)?;
        child.fork_preparer = None;
        child.workspaces = None;
        Ok(Arc::new(child))
    }

/// Narrows the effective task execution bounds for newly admitted work.

pub fn scoped_run_limits(self: &Arc<Self>, limits: TaskRunLimits) -> Result<Arc<Self>> {
        let mut child = self.as_ref().clone();
        child.scope = self.scope.clone().with_run_limits(limits, &mut None)?;
        child.live = TaskGroup::new(child.scope.concurrency_bound(self.concurrency), None)?;
        child.fork_preparer = None;
        child.workspaces = None;
        Ok(Arc::new(child))
    }

/// Selects a different qualified execution route for newly admitted work under attenuated authority.

pub fn scoped_execution(
        self: &Arc<Self>,
        grants: Capabilities,
        limits: Limits,
        execution: Arc<dyn ExecutionProvider>,
    ) -> Result<Arc<Self>> {
        let scoped = self.scoped(grants, limits)?;
        let mut routed = scoped.as_ref().clone();
        routed.host = None;
        routed.state = Some(execution.state());
        routed.spawner = Some(execution.spawner());
        routed.execution = None;
        let routed = Arc::new(routed);
        routed.assert_durable_policy_bindings(None)?;
        routed.bind_execution(execution)
    }

/// Binds a resolver separately from interaction presentation/routing.

#[must_use]
pub fn bind_interaction_resolver(
        self: &Arc<Self>,
        resolver: Arc<dyn InteractionResolver>,
    ) -> Arc<Self> {
        let mut bound = self.as_ref().clone();
        bound.interaction_resolver = Some(resolver);
        Arc::new(bound)
    }

/// Submits one signed, versioned answer through its conversation owner.

pub async fn resolve_answer(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> Result<ResolutionReceipt> {
        let receipt = self
            .interaction_resolver
            .as_ref()
            .ok_or_else(|| Error::Unsupported("interaction resolver is not bound".into()))?
            .resolve_answer(operation_id, scope, id, expected_version, response)
            .await?;
        receipt.validate()?;
        if receipt.operation_id != operation_id
            || receipt.version != expected_version
            || receipt.id
                != uuid::Uuid::parse_str(&id.to_string())
                    .map_err(|error| Error::Invalid(error.to_string()))?
            || !matches!(receipt.outcome, InteractionOutcome::Answered { .. })
        {
            return Err(Error::Conflict(
                "interaction resolver returned another answer".into(),
            ));
        }
        Ok(receipt)
    }

/// Submits one signed decision for the ticket's exact approved intent.

pub async fn resolve_approval(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> Result<ResolutionReceipt> {
        let receipt = self
            .interaction_resolver
            .as_ref()
            .ok_or_else(|| Error::Unsupported("interaction resolver is not bound".into()))?
            .resolve_approval(operation_id, scope, id, expected_version, approved, reason)
            .await?;
        receipt.validate()?;
        if receipt.operation_id != operation_id
            || receipt.version != expected_version
            || receipt.id
                != uuid::Uuid::parse_str(&id.to_string())
                    .map_err(|error| Error::Invalid(error.to_string()))?
            || !matches!(
                (&receipt.outcome, approved),
                (InteractionOutcome::Approved, true) | (InteractionOutcome::Declined, false)
            )
        {
            return Err(Error::Conflict(
                "interaction resolver returned another decision".into(),
            ));
        }
        Ok(receipt)
    }

/// Validates task dependencies once before accepting work.

pub fn new(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
    ) -> Result<Arc<Self>> {
        Self::with_content(tasks, tools, scope, concurrency, host, None, None)
    }

/// Binds owner-mediated content access.

pub fn with_content(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>,
        content: Option<ContentBindings>,
    ) -> Result<Arc<Self>> {
        Self::with_policy(
            tasks,
            tools,
            scope,
            concurrency,
            host,
            interactions,
            content,
            None,
            None,
        )
    }

/// Seals a replaceable tool policy into the immutable runtime composition.

#[allow(
        clippy::too_many_arguments,
        reason = "each independently replaceable runtime boundary is explicit"
    )]
pub fn with_policy(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>,
        content: Option<ContentBindings>,
        artifacts: Option<ArtifactBindings>,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Arc<Self>> {
        let durable_ready = host.is_some();
        let host_available = host.is_some();
        Self::with_policy_internal(
            tasks,
            tools,
            ResumableToolRegistry::default(),
            scope,
            concurrency,
            host,
            interactions,
            content,
            artifacts,
            policy,
            durable_ready,
            host_available,
            host_available,
            None,
        )
    }

/// Builds the same original runtime composition from paid configuration copies.

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[allow(clippy::too_many_arguments, reason = "the original runtime provider configuration plus native work")]
pub fn with_policy_with_native_work(
        tasks: &TaskRegistry, tools: &ToolRegistry, resumable_tools: &ResumableToolRegistry,
        scope: &RuntimeScope,
        concurrency: usize, host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>, content: Option<ContentBindings>,
        artifacts: Option<ArtifactBindings>, policy: Option<Arc<dyn ToolPolicy>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if let Some(policy) = &policy { policy.supports_native_policy(work)?; }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        scope.run_limits().validate_observed(Some(&io))?;
        if concurrency == 0 {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "runtime concurrency must be positive" });
        }
        let tasks = tasks.clone_with_native_work(work)?;
        let tools = tools.clone_with_native_work(work)?;
        io.absorb_native_custody(tools.1)?;
        let resumable_tools = resumable_tools.clone_with_native_work(work)?;
        io.absorb_native_custody(resumable_tools.1)?;
        let scope = scope.clone_with_native_work(work)?;
        io.absorb_native_custody(scope.1)?;
        let host_available = host.is_some();
        Self::with_policy_internal(
            tasks, tools.0, resumable_tools.0, scope.0, concurrency,
            host, interactions, content, artifacts, policy,
            host_available, host_available, host_available, Some(&io),
        )
    }

#[allow(
        clippy::too_many_arguments,
        reason = "each independently replaceable runtime boundary is explicit"
    )]
pub(super) fn with_policy_internal(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        resumable_tools: ResumableToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>,
        content: Option<ContentBindings>,
        artifacts: Option<ArtifactBindings>,
        policy: Option<Arc<dyn ToolPolicy>>,
        durable_ready: bool,
        state_available: bool,
        spawner_available: bool,
        startup: StartupIo<'_>,
    ) -> Result<Arc<Self>> {
        if concurrency == 0 {
            return Err(native_startup::error(startup, crate::wire::ErrorCode::Invalid,
                "runtime concurrency must be positive"));
        }
        scope.run_limits().validate_observed(startup)?;
        let concurrency = scope.concurrency_bound(concurrency);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_policy_identity = if let Some(io) = startup {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "startup requires original native work",
            })?;
            let identity = match &policy {
                Some(policy) => {
                    policy.supports_native_policy(work)?;
                    let identity = policy.identity_with_native_work(work)?;
                    validate_policy_identity_with_native_work(&identity.0, &identity.1, work)?;
                    io.absorb_native_custody(identity.1)?;
                    Some(identity.0)
                }
                None => None,
            };
            if let Some(host) = &host
                && TaskStateProvider::policy_identity_with_native_work(host.as_ref(), work)? != identity.as_ref()
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "runtime and durable host policies differ" });
            }
            identity
        } else { None };
        if startup.is_none() {
            if let Some(policy) = &policy { validate_policy_identity(&policy.identity())?; }
            if host.as_ref().is_some_and(|host| {
                TaskStateProvider::policy_identity(host.as_ref()) != policy.as_ref().map(|policy| policy.identity())
            }) {
                return Err(Error::Conflict("runtime and durable host policies differ".into()));
            }
        }
        if resumable_tools.definitions().next().is_some() && !durable_ready {
            return Err(native_startup::error(startup, crate::wire::ErrorCode::Invalid,
                "resumable tools require durable task admission and state"));
        }
        for definition in resumable_tools.definitions() {
            native_startup::item(startup, 0)?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let adapter = if let Some(io) = startup {
                tools.get_version(&definition.name, &definition.revision, Some(io))?
            } else { tools.get_version(&definition.name, &definition.revision, None)? };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let adapter = tools.get_version(&definition.name, &definition.revision, None)?;
            if let Some(adapter) = adapter {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let pinned = if let Some(io) = startup {
                    resumable_tools.get(&definition.name, &definition.revision, &mut Some(&mut io.history_io()))?
                } else { resumable_tools.get(&definition.name, &definition.revision, &mut None)? };
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let pinned = resumable_tools.get(&definition.name, &definition.revision, &mut None)?;
                let pinned = pinned.ok_or_else(|| native_startup::error(startup,
                    crate::wire::ErrorCode::Invalid, "resumable registry differs from its definitions"))?;
                let same_adapter = adapter.definition == *definition && {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    let same_identity = if let Some(io) = startup {
                        let work = io.original_native_work().ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported, reason: "startup requires original native work",
                        })?;
                        adapter.executor.resumable_machine_identity_with_native_work(work)?
                            == Some(pinned.machine.identity_with_native_work(work)?)
                    } else {
                        adapter.executor.resumable_machine_identity().as_ref()
                            == Some(pinned.machine.identity())
                    };
                    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                    let same_identity = adapter.executor.resumable_machine_identity().as_ref()
                        == Some(pinned.machine.identity());
                    same_identity
                };
                if same_adapter { continue; }
                return Err(native_startup::error(startup, crate::wire::ErrorCode::Conflict,
                    "live and resumable implementations differ"));
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let dependency_io = startup.map(|io| {
            crate::filesystem::PreparationIo::from_native_work(io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "startup requires original native work",
            })?)
        }).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let dependency_observer = dependency_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let dependency_observer = None;
        validate_task_dependencies(
            &tasks,
            &tools,
            &resumable_tools,
            &scope,
            durable_ready,
            state_available,
            spawner_available,
            interactions.is_some(),
            content.as_ref(),
            artifacts.as_ref(),
            policy.is_some(),
            dependency_observer,
        )?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        drop(dependency_io);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let policy_identity = if startup.is_some() { native_policy_identity }
            else { policy.as_ref().map(|policy| policy.identity()) };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let policy_identity = policy.as_ref().map(|policy| policy.identity());
        let extension_runtime = scope.extension_runtime();
        let spawner = host.as_ref().map(|host| Arc::clone(host) as Arc<dyn TaskSpawner>);
        let state = host.as_ref().map(|host| Arc::clone(host) as Arc<dyn TaskStateProvider>);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let live = if let Some(io) = startup {
            TaskGroup::new(concurrency, Some(io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "startup requires original native work",
            })?))?
        } else { TaskGroup::new(concurrency, None)? };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let live = TaskGroup::new(concurrency, None)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_configuration_custody = if let Some(io) = startup {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
            Some(io.take_native_custody(io.owned_bytes()?)?)
        } else { None };
        Ok(Arc::new(Self {
            tasks,
            tools,
            resumable_tools,
            live,
            concurrency,
            scope,
            extension_runtime,
            host,
            state,
            spawner,
            execution: None,
            interactions,
            interaction_resolver: None,
            content,
            artifacts,
            policy,
            policy_identity,
            model: None,
            context: ContextPipeline::default(),
            fork_preparer: None,
            workspaces: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_configuration_custody,
        }))
    }

/// Resolves one registered task with exact Rust input/output types.

pub fn task<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        self.tasks.get(name)
    }

/// Resolves one model-visible tool definition by exact registered name.

pub fn tool(&self, name: &str) -> Result<ToolDefinition> {
        self.tools
            .get(name, None)?
            .map(|tool| tool.definition.clone())
            .ok_or_else(|| Error::NotFound(format!("tool {name}")))
    }

/// Verifies an already staged input or output through a bound content or artifact owner and the runtime's effective read grant.

pub async fn verify_readable_file(&self, file: &FileRef) -> Result<()> {
        file.validate(&mut None)?;
        self.scope.limits.validate_file(file, &mut None)?;
        if !read_granted(self.scope.grants(), file)? {
            return Err(Error::Unauthorized(
                "task scope cannot read this file".into(),
            ));
        }
        let mut failure = Error::Unsupported("content or artifact reader is not bound".into());
        for binding in [self.content.as_ref(), self.artifacts.as_ref()]
            .into_iter()
            .flatten()
        {
            match verified_content_bytes(binding.reader.as_ref(), file, (file).descriptor().byte_length(), None).await.map(|(bytes, _)| bytes) {
                Ok(_) => return Ok(()),
                Err(error) => failure = error,
            }
        }
        Err(failure)
    }
}
