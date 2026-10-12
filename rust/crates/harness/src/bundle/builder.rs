use super::*;

/// Rust-first composition root. Every stock-loop dependency is explicit.
#[derive(Default)]
pub struct HarnessBuilder {
    name: Option<String>,
    executor: Option<Arc<dyn Executor>>,
    agent_loop: Option<Arc<dyn AgentLoop>>,
    journal: Option<Arc<dyn ExecutionJournal>>,
    model: Option<(Model, Arc<dyn ModelProvider>)>,
    context: ContextPipeline,
    compaction: crate::context::CompactionPolicy,
    bindings: Bindings,
    capabilities: Vec<String>,
    limits: Limits,
    bound_scope: Option<RuntimeScope>,
    original_extensions: Option<(Option<ExtensionAdmission>, SchemaRegistry)>,
    extension_runtime: Option<Arc<ExtensionRuntime>>,
}

impl HarnessBuilder {
    /// Starts an empty builder; `build` rejects missing execution bindings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Installs one immutable runtime binding set.
    #[must_use]
    pub fn bindings(mut self, value: Bindings) -> Self {
        self.capabilities = value.scope.grants().iter().map(str::to_owned).collect();
        self.limits = value.scope.limits();
        self.original_extensions = None;
        self.extension_runtime = value.scope.extension_runtime();
        self.bound_scope = Some(value.scope.clone());
        self.bindings = value;
        self
    }

    /// Pins a human-readable implementation name.
    #[must_use]
    pub fn name(mut self, value: impl Into<String>) -> Self {
        self.name = Some(value.into());
        self
    }

    /// Replaces the complete agent loop; stock model/context/tool bindings then become invalid.
    #[must_use]
    pub fn executor(mut self, value: Arc<dyn Executor>) -> Self {
        self.executor = Some(value);
        self
    }

    /// Replaces agent behavior with a typed live loop over `TaskContext`.
    #[must_use]
    pub fn agent_loop(mut self, value: Arc<dyn AgentLoop>) -> Self {
        self.agent_loop = Some(value);
        self
    }

    /// Binds the owner-controlled ref-only execution journal and content store.
    #[must_use]
    pub fn journal(mut self, value: Arc<dyn ExecutionJournal>) -> Self {
        self.journal = Some(value);
        self
    }

    /// Selects one immutable model and its replaceable provider.
    #[must_use]
    pub fn model(mut self, model: Model, provider: Arc<dyn ModelProvider>) -> Self {
        self.model = Some((model, provider));
        self
    }

    /// Selects the exact ordered context stages.
    #[must_use]
    pub fn context(mut self, value: ContextPipeline) -> Self {
        self.context = value;
        self
    }

    /// Replaces or disables stock threshold compaction for this composition.
    #[must_use]
    pub fn compaction(mut self, value: crate::context::CompactionPolicy) -> Self {
        self.compaction = value;
        self
    }

    /// Selects the versioned tool registry.
    #[must_use]
    pub fn tools(mut self, value: ToolRegistry) -> Self {
        self.bindings.tools = value;
        self
    }

    /// Registers one pinned tool without replacing the rest of the registry.
    pub fn tool(mut self, value: Tool) -> Result<Self> {
        self.bindings
            .tools
            .register((value, None), &mut None, None)?;
        Ok(self)
    }

    /// Selects exact resumable tool state machines, independently of live tools.
    #[must_use]
    pub fn resumable_tools(mut self, value: ResumableToolRegistry) -> Self {
        self.bindings.resumable_tools = value;
        self
    }

    /// Selects pinned typed task definitions for the same composition.
    #[must_use]
    pub fn tasks(mut self, value: TaskRegistry) -> Self {
        self.bindings.tasks = value;
        self
    }

    /// Registers one pinned typed task without replacing the rest of the registry.
    pub fn task<I: 'static, O: 'static>(mut self, value: TaskDefinition<I, O>) -> Result<Self>
    where
        TaskDefinition<I, O>: Send + Sync,
    {
        self.bindings.tasks.register(value)?;
        Ok(self)
    }

    /// Binds durable admission and reconciliation independently of the model loop.
    #[must_use]
    pub fn durable_host(mut self, value: Arc<dyn DurableTaskHost>) -> Self {
        self.bindings.durable_host = Some(value);
        self
    }

    /// Replaces retained task state and observation independently of admission.
    #[must_use]
    pub fn state(mut self, value: Arc<dyn TaskStateProvider>) -> Self {
        self.bindings.state = Some(value);
        self
    }

    /// Replaces durable task admission while retaining the separately bound state host for observation, resume, cancellation, and effects.
    #[must_use]
    pub fn spawner(mut self, value: Arc<dyn TaskSpawner>) -> Self {
        self.bindings.spawner = Some(value);
        self
    }

    /// Selects a qualified task-build and environment route for new work.
    #[must_use]
    pub fn execution(mut self, value: Arc<dyn ExecutionProvider>) -> Self {
        self.bindings.execution = Some(value);
        self
    }

    /// Binds local addressable interactions independently of model and tools.
    #[must_use]
    pub fn interactions(mut self, value: Arc<dyn InteractionRouter>) -> Self {
        self.bindings.interactions = Some(value);
        self
    }

    /// Binds signed, owner-mediated CAS replies independently of presentation.
    #[must_use]
    pub fn interaction_resolver(mut self, value: Arc<dyn InteractionResolver>) -> Self {
        self.bindings.interaction_resolver = Some(value);
        self
    }

    /// Binds one replaceable tool policy evaluated before live and durable dispatch.
    #[must_use]
    pub fn policy(mut self, value: Arc<dyn ToolPolicy>) -> Self {
        self.bindings.policy = Some(value);
        self
    }

    /// Binds owner-authenticated file staging and resolution for tasks/tools.
    #[must_use]
    pub fn content(mut self, value: crate::runtime::ContentBindings) -> Self {
        self.bindings.content = Some(value);
        self
    }

    /// Binds immutable artifact storage independently of conversation files.
    #[must_use]
    pub fn artifacts(mut self, value: crate::runtime::ArtifactBindings) -> Self {
        self.bindings.artifacts = Some(value);
        self
    }

    /// Binds fork capture for one exact parent revision.
    #[must_use]
    pub fn fork_preparer(mut self, value: Arc<dyn crate::fork::ForkPreparer>) -> Self {
        self.bindings.fork_preparer = Some(value);
        self
    }

    /// Binds parent-authorized project forks and inspected joins.
    #[must_use]
    pub fn workspaces(mut self, value: Arc<dyn crate::merge::ProjectWorkspaceProvider>) -> Self {
        self.bindings.workspaces = Some(value);
        self
    }

    /// Sets the local live-task concurrency bound.
    #[must_use]
    pub fn concurrency(mut self, value: usize) -> Self {
        self.bindings.concurrency = value;
        self
    }

    /// Grants an explicit capability to this immutable composition.
    #[must_use]
    pub fn grant(mut self, value: impl Into<String>) -> Self {
        self.capabilities.push(value.into());
        self
    }

    /// Configures checked runtime content and rendering bounds.
    #[must_use]
    pub fn limits(mut self, value: Limits) -> Self {
        self.limits = value;
        self
    }

    /// Captures an authenticated agent's committed extension selection for this bundle and every task admitted through it.
    pub fn extensions_from(mut self, agent: &Reducer) -> Result<Self> {
        if self.original_extensions.is_some() {
            return Err(Error::Unauthorized(
                "builder already captured its original extension selection".into(),
            ));
        }
        let original = agent.original_extension_binding()?;
        if let Some(bound) = self.bound_scope.take() {
            self.bound_scope = Some(bound.with_original_extensions(original)?);
        } else {
            self.original_extensions = Some(original);
        }
        Ok(self)
    }

    /// Attaches a linked native extension composition.
    #[must_use]
    pub fn extensions(mut self, runtime: Arc<ExtensionRuntime>) -> Self {
        self.extension_runtime = Some(runtime);
        self
    }

    /// Validates dependency exclusivity and constructs the immutable runtime binding.
    pub fn build(self) -> Result<HarnessBundle> {
        self.limits.validate(&mut None)?;
        let journal = self.journal;
        let name = self.name.unwrap_or_else(|| "harness".into());
        if name.trim().is_empty() {
            return Err(Error::Invalid("harness name is empty".into()));
        }
        let capabilities = Capabilities::new(self.capabilities);
        let scope = if let Some(bound) = self.bound_scope {
            bound.narrow(capabilities.clone(), self.limits)?
        } else {
            let root = RuntimeScope::new(capabilities.clone(), self.limits)?;
            if let Some(original) = self.original_extensions {
                root.with_original_extensions(original)?
            } else {
                root.with_replayed_extensions(None)?
            }
        };
        let scope = if let Some(runtime) = self.extension_runtime {
            scope.with_extension_runtime(runtime)?
        } else {
            scope
        };
        let mut bindings = self.bindings;
        bindings.scope = scope.clone();
        let agent_loop = self.agent_loop;
        if let Some(component) = &agent_loop {
            let component = Arc::clone(component);
            bindings.tasks.register(TaskDefinition::live(
                "acyclic.agent_loop",
                "2",
                move |context, input: AgentInput| component.run(context, input),
            )?)?;
        }
        let stock_task = self.executor.is_none() && agent_loop.is_none();
        let executor = if let Some(executor) = self.executor {
            Some(executor)
        } else if agent_loop.is_some() {
            None
        } else {
            let journal = journal
                .as_ref()
                .ok_or_else(|| Error::Invalid("execution journal binding is missing".into()))?;
            let (model, provider) = self
                .model
                .as_ref()
                .ok_or_else(|| Error::Invalid("model binding is missing".into()))?;
            if !capabilities.contains("model:generate") {
                return Err(Error::Unauthorized(
                    "stock model loop requires model:generate".into(),
                ));
            }
            bindings.tools.definitions(None)?;
            let executor = Arc::new(
                StockExecutor::new(
                    model.clone(),
                    Arc::clone(provider),
                    self.context.clone(),
                    bindings.tools.clone(),
                )
                .with_compaction_policy(self.compaction)
                .with_limits(self.limits)
                .with_tool_authority(scope, bindings.policy.clone())?,
            ) as Arc<dyn Executor>;
            let task_executor = Arc::clone(&executor);
            let task_journal = Arc::clone(journal);
            bindings.tasks.register(TaskDefinition::live(
                "acyclic.stock_turn",
                "2",
                move |context, input: TurnInput| {
                    let executor = Arc::clone(&task_executor);
                    let journal = Arc::clone(&task_journal);
                    async move {
                        executor
                            .execute_with_context(&context, input, journal.as_ref())
                            .await
                    }
                },
            )?)?;
            Some(executor)
        };
        let runtime = bindings.build()?;
        let runtime = if let Some((model, provider)) = self.model {
            runtime.bind_model(model, provider)?
        } else {
            runtime
        }
        .bind_context(self.context);
        if executor.is_some() && journal.is_none() {
            return Err(Error::Invalid(
                "execution journal binding is missing".into(),
            ));
        }
        if capabilities.iter().any(|value| value.trim().is_empty()) {
            return Err(Error::Invalid("empty capability grant".into()));
        }
        Ok(HarnessBundle {
            name,
            executor,
            journal,
            capabilities,
            limits: self.limits,
            runtime,
            agent_loop: agent_loop.is_some(),
            stock_task,
        })
    }
}
