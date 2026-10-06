//! Code-defined named bundles; bundles are constructors, not a configuration language.

use crate::{
    Capabilities, Error, Result,
    agent_loop::{AgentInput, AgentLoop, AgentRunOutput},
    context::ContextPipeline,
    conversation::Limits,
    core::{ExtensionAdmission, Reducer},
    durable_tool::{ResumableTool, ResumableToolRegistry},
    executor::{ExecutionJournal, Executor, StockExecutor, TurnInput, TurnOutput},
    extension::{ExtensionRuntime, NativeExtensionBundle},
    model::{Model, ModelProvider},
    runtime::{
        AgentHarness, Bindings, DurableTaskHost, ExecutionProvider, InteractionResolver,
        InteractionRouter, RuntimeScope, TaskDefinition, TaskRegistry, TaskSpawner,
        TaskStateProvider, ToolPolicy,
    },
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use futures::future::BoxFuture;
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use std::sync::Arc;

const CODING_TOOLS: &[(&str, &str)] = &[
    (
        "acyclic.filesystem",
        "Read and mutate the durable workspace filesystem.",
    ),
    (
        "acyclic.edit",
        "Apply bounded structured edits to workspace files.",
    ),
    (
        "acyclic.search",
        "Search bounded workspace paths and contents.",
    ),
    (
        "acyclic.shell",
        "Execute an admitted non-interactive shell command.",
    ),
    (
        "acyclic.pty",
        "Operate an admitted interactive terminal session.",
    ),
    (
        "acyclic.lsp",
        "Query language-server diagnostics and symbols.",
    ),
    ("acyclic.browser", "Operate an admitted browser session."),
    ("acyclic.web", "Fetch or search admitted network resources."),
    (
        "acyclic.mcp",
        "Invoke an admitted Model Context Protocol tool.",
    ),
    (
        "acyclic.skills",
        "Load and apply a versioned runtime skill.",
    ),
    (
        "acyclic.delegation",
        "Dispatch or coordinate an admitted child task.",
    ),
    (
        "acyclic.compaction",
        "Persist a deterministic context compaction revision.",
    ),
];

/// Single host boundary implementing the complete public coding tool vocabulary.
pub trait CodingToolHost: Send + Sync {
    /// Returns the pinned contract implemented by this host for a stock tool.
    /// The factory checks its name and registers its exact revision and schemas;
    /// a generic catch-all schema must not be silently invented by the runtime.
    fn definition(&self, name: &str, description: &str) -> Result<ToolDefinition>;

    /// Executes one already admitted tool invocation.
    fn execute<'a>(
        &'a self,
        tool: &'a str,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<ToolResult>>;

    /// Reconciles an interrupted invocation without redispatching it.
    fn reconcile<'a>(
        &'a self,
        tool: &'a str,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>>;

    /// Projects a successful result into model-visible structured context.
    fn project(
        &self,
        tool: &str,
        invocation: &ToolInvocation,
        result: &ToolResult,
    ) -> Result<Value>;
}

struct HostedCodingTool {
    name: &'static str,
    host: Arc<dyn CodingToolHost>,
}

impl ToolExecutor for HostedCodingTool {
    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        self.host.execute(self.name, invocation)
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        self.host.reconcile(self.name, invocation)
    }
}

impl ToolProjection for HostedCodingTool {
    fn project(&self, invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        self.host.project(self.name, invocation, result)
    }
}

/// Constructs the complete validated coding registry over one explicit host boundary.
#[allow(
    clippy::needless_pass_by_value,
    reason = "the registry owns cloned host handles"
)]
pub fn coding_tools(host: Arc<dyn CodingToolHost>) -> Result<ToolRegistry> {
    let mut registry = ToolRegistry::new();
    for &(name, description) in CODING_TOOLS {
        let definition = host.definition(name, description)?;
        if definition.name != name {
            return Err(Error::Invalid(format!(
                "coding host returned {} for {name}",
                definition.name
            )));
        }
        validate_coding_schema(&definition.input_schema, name, "input")?;
        validate_coding_schema(&definition.output_schema, name, "output")?;
        registry.register(Tool {
            definition,
            executor: Arc::new(HostedCodingTool {
                name,
                host: host.clone(),
            }),
            projection: Arc::new(HostedCodingTool {
                name,
                host: host.clone(),
            }),
        })?;
    }
    Ok(registry)
}

fn validate_coding_schema(schema: &Value, name: &str, role: &str) -> Result<()> {
    let strict = schema.as_object().is_some_and(|schema| {
        schema.get("type").and_then(Value::as_str) == Some("object")
            && schema.get("additionalProperties") == Some(&Value::Bool(false))
            && schema
                .get("properties")
                .and_then(Value::as_object)
                .is_some_and(|properties| !properties.is_empty())
    });
    if !strict {
        return Err(Error::Invalid(format!(
            "coding tool {name} requires a closed, nonempty object {role} schema"
        )));
    }
    Ok(())
}

/// Immutable code-defined runtime composition.
#[derive(Clone)]
pub struct HarnessBundle {
    /// Human-readable constructor identity.
    name: String,
    /// Complete replaceable executor.
    executor: Option<Arc<dyn Executor>>,
    /// Capabilities requested by this composition.
    capabilities: Capabilities,
    /// Central content, projection, and loop bounds for this composition.
    limits: Limits,
    /// Typed task runtime sharing this bundle's immutable tool contracts.
    runtime: Arc<AgentHarness>,
    journal: Option<Arc<dyn ExecutionJournal>>,
    agent_loop: bool,
}

/// Rust-first composition root. Every stock-loop dependency is explicit.
#[derive(Default)]
pub struct HarnessBuilder {
    name: Option<String>,
    executor: Option<Arc<dyn Executor>>,
    agent_loop: Option<Arc<dyn AgentLoop>>,
    journal: Option<Arc<dyn ExecutionJournal>>,
    model: Option<Model>,
    provider: Option<Arc<dyn ModelProvider>>,
    context: ContextPipeline,
    bindings: Bindings,
    capabilities: Vec<String>,
    limits: Limits,
    bound_scope: Option<RuntimeScope>,
    extensions: Option<ExtensionAdmission>,
    extension_runtime: Option<Arc<ExtensionRuntime>>,
}

impl HarnessBuilder {
    /// Starts an empty builder; `build` rejects missing execution bindings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Installs one immutable runtime binding set. Later builder calls may
    /// replace individual entries or register additional tasks and tools.
    /// Provider ownership is transferred, not duplicated across runtimes.
    #[must_use]
    pub fn bindings(mut self, value: Bindings) -> Self {
        self.capabilities = value.scope.grants().iter().map(str::to_owned).collect();
        self.limits = value.scope.limits();
        self.extensions = value.scope.extensions().cloned();
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
    /// It can spawn recursive work without model turns or a turn journal.
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
        self.model = Some(model);
        self.provider = Some(provider);
        self
    }

    /// Selects the exact ordered context stages.
    #[must_use]
    pub fn context(mut self, value: ContextPipeline) -> Self {
        self.context = value;
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
        self.bindings.tools.register(value)?;
        Ok(self)
    }

    /// Selects exact resumable tool state machines, independently of live tools.
    #[must_use]
    pub fn resumable_tools(mut self, value: ResumableToolRegistry) -> Self {
        self.bindings.resumable_tools = value;
        self
    }

    /// Registers one pinned resumable tool without adding a live fallback.
    pub fn resumable_tool(mut self, value: ResumableTool) -> Result<Self> {
        self.bindings.resumable_tools.register(value)?;
        Ok(self)
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

    /// Replaces durable task admission while retaining the separately bound
    /// state host for observation, resume, cancellation, and effects.
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

    /// Binds fork capture for one exact parent revision. Recompose after that
    /// conversation advances; an old binding cannot select a newer prefix.
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

    /// Captures an authenticated agent's committed extension selection for
    /// this bundle and every task admitted through it.
    pub fn extensions_from(mut self, agent: &Reducer) -> Result<Self> {
        self.extensions = agent.extension_admission()?;
        Ok(self)
    }

    /// Attaches a linked native extension composition. Required extension
    /// tasks retain exact executable versions at admission.
    #[must_use]
    pub fn extensions(mut self, runtime: Arc<ExtensionRuntime>) -> Self {
        self.extension_runtime = Some(runtime);
        self
    }

    /// Links a native bundle into this composition's complete binding surface.
    pub fn extension_bundle(mut self, bundle: NativeExtensionBundle) -> Result<Self> {
        let runtime = bundle.into_runtime();
        if let Some(existing) = &self.extension_runtime
            && !Arc::ptr_eq(existing, &runtime)
        {
            return Err(Error::Conflict(
                "composition already has another extension runtime".into(),
            ));
        }
        runtime.link_into_bindings(&mut self.bindings)?;
        self.extension_runtime = Some(runtime);
        Ok(self)
    }

    /// Validates dependency exclusivity and constructs the immutable runtime binding.
    pub fn build(self) -> Result<HarnessBundle> {
        self.limits.validate()?;
        let journal = self.journal;
        let name = self.name.unwrap_or_else(|| "harness".into());
        if name.trim().is_empty() {
            return Err(Error::Invalid("harness name is empty".into()));
        }
        let capabilities = Capabilities::new(self.capabilities);
        let scope = if let Some(bound) = self.bound_scope {
            if bound.extensions() != self.extensions.as_ref() {
                return Err(Error::Conflict(
                    "bound scope extension selection cannot be changed".into(),
                ));
            }
            bound.narrow(capabilities.clone(), self.limits)?
        } else {
            RuntimeScope::new(capabilities.clone(), self.limits)?
                .with_replayed_extensions(self.extensions)?
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
        let tools = bindings.tools.clone();
        let policy = bindings.policy.clone();
        let runtime = bindings.build()?;
        let runtime = match (&self.model, &self.provider) {
            (Some(model), Some(provider)) => {
                runtime.bind_model(model.clone(), Arc::clone(provider))?
            }
            (None, None) => runtime,
            _ => {
                return Err(Error::Invalid(
                    "model and provider must be bound together".into(),
                ));
            }
        }
        .bind_context(self.context.clone());
        let executor = if let Some(executor) = self.executor {
            Some(executor)
        } else if agent_loop.is_some() {
            None
        } else {
            if journal.is_none() {
                return Err(Error::Invalid(
                    "execution journal binding is missing".into(),
                ));
            }
            let model = self
                .model
                .ok_or_else(|| Error::Invalid("model binding is missing".into()))?;
            let provider = self
                .provider
                .ok_or_else(|| Error::Invalid("model provider binding is missing".into()))?;
            if !capabilities.contains("model:generate") {
                return Err(Error::Unauthorized(
                    "stock model loop requires model:generate".into(),
                ));
            }
            tools.definitions()?;
            Some(Arc::new(
                StockExecutor::new(model, provider, self.context, tools)
                    .with_limits(self.limits)
                    .with_tool_authority(scope, policy)?,
            ) as Arc<dyn Executor>)
        };
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
        })
    }
}

impl HarnessBundle {
    /// Starts a Rust-first composition using the same builder as local presets.
    #[must_use]
    pub fn builder() -> HarnessBuilder {
        HarnessBuilder::new()
    }

    /// Creates a new immutable view pinned to a later parent fork snapshot.
    /// Already admitted tasks retain the original provider and revision.
    pub fn at_parent_snapshot(&self, preparer: Arc<dyn crate::fork::ForkPreparer>) -> Result<Self> {
        let mut next = self.clone();
        next.runtime = self.runtime.at_parent_snapshot(preparer)?;
        Ok(next)
    }

    /// Returns the constructor identity pinned at composition.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the immutable effective capability set.
    #[must_use]
    pub const fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Returns the immutable executor bound to this composition.
    #[must_use]
    pub fn executor(&self) -> Option<Arc<dyn Executor>> {
        self.executor.clone()
    }

    /// Runs or resumes an admitted typed turn through the pinned journal.
    pub async fn run(&self, input: TurnInput) -> Result<TurnOutput> {
        if input.max_steps == 0
            || usize::try_from(input.max_steps).unwrap_or(usize::MAX) > self.limits.model_steps
        {
            return Err(Error::Invalid(
                "turn exceeds the bound model step limit".into(),
            ));
        }
        let executor = self
            .executor
            .as_ref()
            .ok_or_else(|| Error::Unsupported("turn executor is not bound".into()))?;
        let journal = self
            .journal
            .as_ref()
            .ok_or_else(|| Error::Unsupported("turn journal is not bound".into()))?;
        executor.execute(input, journal.as_ref()).await
    }

    /// Runs the typed custom agent loop as a local task, preserving its full
    /// terminal outcome and task identity. Durable loops use resumable tasks.
    pub async fn run_agent(&self, input: AgentInput) -> Result<AgentRunOutput> {
        if !self.agent_loop {
            return Err(Error::Unsupported("custom agent loop is not bound".into()));
        }
        if input.prompt.is_empty() || input.prompt.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid(
                "agent prompt exceeds limits or is empty".into(),
            ));
        }
        if input.attachments.len() > self.limits.attachments {
            return Err(Error::Invalid("agent attachments exceed limit".into()));
        }
        for attachment in &input.attachments {
            attachment.validate()?;
            self.limits.validate_file(&attachment.file)?;
            self.runtime.verify_readable_file(&attachment.file).await?;
        }
        let definition = self
            .runtime
            .task::<AgentInput, crate::agent_loop::AgentOutput>("acyclic.agent_loop@2")?;
        let task = self.runtime.spawn(&definition, input).await?;
        let task_id = task.identity();
        let outcome = task.result().await?;
        if let crate::Outcome::Succeeded(output) = &outcome {
            if output.text.len() as u64 > self.limits.file_bytes
                || output.attachments.len() > self.limits.attachments
                || crate::contract::canonical_json_bytes(&output.metadata)?.len() as u64
                    > self.limits.file_bytes
            {
                return Err(Error::Invalid("agent output exceeds limits".into()));
            }
            for attachment in &output.attachments {
                attachment.validate()?;
                self.limits.validate_file(&attachment.file)?;
                self.runtime.verify_readable_file(&attachment.file).await?;
            }
        }
        Ok(AgentRunOutput { task_id, outcome })
    }

    /// Returns the checked limits bound to this composition.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }

    /// Returns the pinned typed task runtime.
    #[must_use]
    pub fn runtime(&self) -> Arc<AgentHarness> {
        self.runtime.clone()
    }

    /// Resolves a lost task admission acknowledgement by its original ID.
    pub async fn reconcile_admission(
        &self,
        operation_id: crate::OperationId,
    ) -> Result<Option<(crate::TaskId, crate::registry::ComponentIdentity)>> {
        self.runtime.reconcile_admission(operation_id).await
    }

    /// Captures selected resources through the bound parent-controlled provider.
    pub async fn prepare_fork(
        &self,
        request: crate::fork::ForkRequest,
    ) -> Result<crate::fork::ForkReport> {
        self.runtime.prepare_fork(request).await
    }

    /// Reconciles an uncertain preparation using its original full request.
    pub async fn reconcile_fork(
        &self,
        request: crate::fork::ForkRequest,
    ) -> Result<Option<crate::fork::ForkReport>> {
        self.runtime.reconcile_fork(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;

    struct UnusedForkPreparer(crate::core::Authority, u64);

    impl crate::fork::ForkPreparer for UnusedForkPreparer {
        fn parent_snapshot(&self) -> (&crate::core::Authority, u64) {
            (&self.0, self.1)
        }

        fn prepare<'a>(
            &'a self,
            _: crate::fork::ForkRequest,
        ) -> BoxFuture<'a, Result<crate::fork::ForkReport>> {
            async { Err(Error::Unsupported("unused test preparer".into())) }.boxed()
        }

        fn reconcile<'a>(
            &'a self,
            _: crate::fork::ForkRequest,
        ) -> BoxFuture<'a, Result<Option<crate::fork::ForkReport>>> {
            async { Ok(None) }.boxed()
        }
    }

    struct DelegatingLoop;

    impl AgentLoop for DelegatingLoop {
        fn run(
            &self,
            context: crate::runtime::TaskContext,
            input: AgentInput,
        ) -> BoxFuture<'static, Result<crate::agent_loop::AgentOutput>> {
            Box::pin(async move {
                let definition = context.task::<String, String>("example.echo")?;
                let child = context.spawn(&definition, input.prompt).await?;
                let text = match child.result().await? {
                    crate::Outcome::Succeeded(text) => text,
                    crate::Outcome::Failed { message } => return Err(Error::Conflict(message)),
                    crate::Outcome::Cancelled => {
                        return Err(Error::Conflict("child cancelled".into()));
                    }
                    crate::Outcome::Indeterminate { operation_id } => {
                        return Err(Error::Indeterminate(operation_id));
                    }
                };
                Ok(crate::agent_loop::AgentOutput::text(text))
            })
        }
    }

    #[tokio::test]
    async fn custom_agent_loop_uses_public_task_context_without_turn_journal() -> Result<()> {
        let bundle = HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .grant("task:spawn:example.echo@1")
            .task(TaskDefinition::live(
                "example.echo",
                "1",
                |_context, value: String| async move { Ok(value) },
            )?)?
            .build()?;
        assert!(bundle.executor().is_none());
        let result = bundle.run_agent(AgentInput::text("hello")).await?;
        assert!(
            matches!(result.outcome, crate::Outcome::Succeeded(output) if output.text == "hello")
        );
        assert!(!result.task_id.is_empty());
        Ok(())
    }

    #[test]
    fn fork_provider_requires_a_root_grant_before_runtime_construction() -> Result<()> {
        let preparer = || {
            Arc::new(UnusedForkPreparer(
                crate::core::Authority {
                    kind: crate::core::AggregateKind::Conversation,
                    id: "parent".into(),
                },
                1,
            ))
        };
        assert!(matches!(
            HarnessBuilder::new()
                .agent_loop(Arc::new(DelegatingLoop))
                .fork_preparer(preparer())
                .build(),
            Err(Error::Unauthorized(_))
        ));
        let bound = HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .fork_preparer(preparer())
            .grant("fork:publish")
            .build()?;
        assert!(bound.capabilities().contains("fork:publish"));
        Ok(())
    }

    #[tokio::test]
    async fn fork_binding_cannot_capture_a_different_parent_revision() -> Result<()> {
        let bundle = HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .fork_preparer(Arc::new(UnusedForkPreparer(
                crate::core::Authority {
                    kind: crate::core::AggregateKind::Conversation,
                    id: "parent".into(),
                },
                1,
            )))
            .grant("fork:publish")
            .build()?;
        let request: crate::fork::ForkRequest =
            serde_json::from_str(include_str!("../fixtures/v2/fork-request.json"))
                .map_err(|error: serde_json::Error| Error::Invalid(error.to_string()))?;
        assert!(matches!(
            bundle.prepare_fork(request).await,
            Err(Error::Conflict(_))
        ));
        let parent = crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: "parent".into(),
        };
        assert!(matches!(
            bundle.at_parent_snapshot(Arc::new(UnusedForkPreparer(parent.clone(), 1))),
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            bundle.at_parent_snapshot(Arc::new(UnusedForkPreparer(
                crate::core::Authority {
                    kind: crate::core::AggregateKind::Conversation,
                    id: "other".into(),
                },
                2
            ))),
            Err(Error::Unauthorized(_))
        ));
        let advanced = bundle.at_parent_snapshot(Arc::new(UnusedForkPreparer(parent, 2)))?;
        assert!(advanced.capabilities().contains("fork:publish"));
        Ok(())
    }

    struct UnusedJournal;

    impl ExecutionJournal for UnusedJournal {
        fn replay<'a>(
            &'a self,
            _: crate::OperationId,
        ) -> BoxFuture<'a, Result<Vec<crate::executor::ExecutionRecord>>> {
            async { Err(Error::Unsupported("unused test journal".into())) }.boxed()
        }
        fn append<'a>(
            &'a self,
            _: crate::OperationId,
            _: String,
            _: crate::executor::ExecutionEvent,
        ) -> BoxFuture<'a, Result<()>> {
            async { Err(Error::Unsupported("unused test journal".into())) }.boxed()
        }
        fn stage<'a>(
            &'a self,
            _: crate::OperationId,
            _: String,
            _: Vec<u8>,
            _: &'static str,
        ) -> BoxFuture<'a, Result<crate::conversation::FileRef>> {
            async { Err(Error::Unsupported("unused test journal".into())) }.boxed()
        }
        fn load<'a>(
            &'a self,
            _: &'a crate::conversation::FileRef,
        ) -> BoxFuture<'a, Result<Vec<u8>>> {
            async { Err(Error::Unsupported("unused test journal".into())) }.boxed()
        }
        fn open_interaction<'a>(
            &'a self,
            _: crate::InteractionId,
            _: crate::interaction::Interaction,
        ) -> BoxFuture<'a, Result<()>> {
            async { Err(Error::Unsupported("unused test journal".into())) }.boxed()
        }
        fn interaction_outcome<'a>(
            &'a self,
            _: crate::InteractionId,
        ) -> BoxFuture<'a, Result<Option<crate::interaction::InteractionOutcome>>> {
            async { Err(Error::Unsupported("unused test journal".into())) }.boxed()
        }
    }

    #[test]
    fn builder_rejects_missing_or_conflicting_bindings() -> Result<()> {
        assert!(HarnessBuilder::new().build().is_err());
        let model = Model::new("example", "model", "1", Value::Null)?;
        struct Provider;
        impl ModelProvider for Provider {
            fn generate<'a>(
                &'a self,
                _: crate::model::ModelRequest,
            ) -> futures::stream::BoxStream<'a, Result<crate::model::ModelEvent>> {
                Box::pin(futures::stream::empty())
            }
            fn reconcile<'a>(
                &'a self,
                _: crate::model::ModelAttempt,
            ) -> BoxFuture<'a, Result<Option<Vec<crate::model::ModelEvent>>>> {
                async { Ok(None) }.boxed()
            }
        }
        let bundle = HarnessBuilder::new()
            .name("test")
            .model(model.clone(), Arc::new(Provider))
            .journal(Arc::new(UnusedJournal))
            .grant("model:generate")
            .build()?;
        assert_eq!(bundle.name, "test");
        assert!(bundle.capabilities.contains("model:generate"));
        let stock_executor = bundle
            .executor()
            .ok_or_else(|| Error::Storage("stock executor missing".into()))?;
        HarnessBuilder::new()
            .model(model, Arc::new(Provider))
            .journal(Arc::new(UnusedJournal))
            .executor(Arc::clone(&stock_executor))
            .build()?;
        let custom = HarnessBuilder::new()
            .executor(Arc::clone(&stock_executor))
            .journal(Arc::new(UnusedJournal))
            .tools(coding_tools(Arc::new(Host {
                definition_name: None,
            }))?)
            .build()?;
        assert_eq!(
            custom.runtime().tool("acyclic.filesystem")?.name,
            "acyclic.filesystem"
        );
        let scoped_bindings = || -> Result<Bindings> {
            let mut bindings = Bindings::local();
            let root = RuntimeScope::new(
                Capabilities::new(["tool:call:acyclic.filesystem".to_owned()]),
                Limits::default(),
            )?;
            bindings.scope = root.narrow(root.grants().clone(), root.limits())?;
            bindings.tools = coding_tools(Arc::new(Host {
                definition_name: None,
            }))?;
            Ok(bindings)
        };
        let composed = HarnessBuilder::new()
            .bindings(scoped_bindings()?)
            .executor(Arc::clone(&stock_executor))
            .journal(Arc::new(UnusedJournal))
            .build()?;
        assert!(
            composed
                .capabilities()
                .contains("tool:call:acyclic.filesystem")
        );
        assert_eq!(
            composed.runtime().tool("acyclic.filesystem")?.name,
            "acyclic.filesystem"
        );
        assert!(
            HarnessBuilder::new()
                .bindings(scoped_bindings()?)
                .grant("tool:call:acyclic.shell")
                .executor(Arc::clone(&stock_executor))
                .journal(Arc::new(UnusedJournal))
                .build()
                .is_err()
        );
        assert!(
            HarnessBuilder::new()
                .bindings(Bindings::local())
                .grant("tool:call:acyclic.shell")
                .executor(Arc::clone(&stock_executor))
                .journal(Arc::new(UnusedJournal))
                .build()
                .is_err()
        );
        assert!(
            HarnessBuilder::new()
                .grant("tool:call:acyclic.shell")
                .executor(Arc::clone(&stock_executor))
                .journal(Arc::new(UnusedJournal))
                .build()?
                .capabilities()
                .contains("tool:call:acyclic.shell")
        );
        Ok(())
    }

    struct Host {
        definition_name: Option<&'static str>,
    }

    impl CodingToolHost for Host {
        fn definition(&self, name: &str, description: &str) -> Result<ToolDefinition> {
            Ok(ToolDefinition {
                name: self.definition_name.unwrap_or(name).into(),
                revision: "1".into(),
                description: description.into(),
                input_schema: json!({"type": "object", "properties": {
                    "request": {"type": "string"}
                }, "required": ["request"], "additionalProperties": false}),
                output_schema: json!({"type": "object", "properties": {
                    "tool": {"type": "string"}
                }, "required": ["tool"], "additionalProperties": false}),
            })
        }

        fn execute<'a>(
            &'a self,
            tool: &'a str,
            _: ToolInvocation,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            async move {
                Ok(ToolResult {
                    value: json!({"tool": tool}),
                })
            }
            .boxed()
        }

        fn reconcile<'a>(
            &'a self,
            _: &'a str,
            _: ToolInvocation,
        ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            async { Ok(None) }.boxed()
        }

        fn project(&self, tool: &str, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
            Ok(json!({"tool": tool, "result": result.value}))
        }
    }

    #[tokio::test]
    async fn coding_factory_builds_an_executable_complete_registry() -> Result<()> {
        let host: Arc<dyn CodingToolHost> = Arc::new(Host {
            definition_name: None,
        });
        let registry = coding_tools(host)?;
        assert_eq!(registry.definitions()?.len(), CODING_TOOLS.len());
        for &(name, _) in CODING_TOOLS {
            let tool = registry
                .get(name)
                .ok_or_else(|| Error::NotFound(name.into()))?;
            let invocation = ToolInvocation {
                operation_id: crate::OperationId::new(),
                call_id: format!("call-{name}"),
                name: name.into(),
                arguments: json!({"request": "test"}),
            };
            let result = tool.executor.execute(invocation.clone()).await?;
            assert_eq!(result.value, json!({"tool": name}));
            assert_eq!(
                tool.projection.project(&invocation, &result)?,
                json!({"tool": name, "result": {"tool": name}})
            );
        }
        Ok(())
    }

    #[test]
    fn coding_contract_rejects_catch_all_and_empty_schemas() {
        for schema in [
            json!(true),
            json!({}),
            json!({"type": "object"}),
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
        ] {
            assert!(matches!(
                validate_coding_schema(&schema, "acyclic.shell", "input"),
                Err(Error::Invalid(_))
            ));
        }
    }

    #[test]
    fn coding_factory_rejects_a_host_that_renames_a_tool() {
        let host: Arc<dyn CodingToolHost> = Arc::new(Host {
            definition_name: Some("acyclic.other"),
        });
        assert!(matches!(coding_tools(host), Err(Error::Invalid(_))));
    }
}
