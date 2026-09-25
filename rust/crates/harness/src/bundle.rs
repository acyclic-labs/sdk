//! Code-defined named bundles; bundles are constructors, not a configuration language.

use crate::{
    Capabilities, Error, Result,
    context::ContextPipeline,
    conversation::Limits,
    executor::{ExecutionJournal, Executor, StockExecutor, TurnInput, TurnOutput},
    model::{Model, ModelProvider},
    runtime::{
        AgentHarness, Bindings, DurableTaskHost, InteractionRouter, RuntimeScope, TaskRegistry,
        ToolPolicy,
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

/// Immutable code-defined runtime composition.
#[derive(Clone)]
pub struct HarnessBundle {
    /// Human-readable constructor identity.
    name: String,
    /// Complete replaceable executor.
    executor: Arc<dyn Executor>,
    /// Capabilities requested by this composition.
    capabilities: Capabilities,
    /// Central content, projection, and loop bounds for this composition.
    limits: Limits,
    /// Typed task runtime sharing this bundle's immutable tool contracts.
    runtime: Arc<AgentHarness>,
    journal: Arc<dyn ExecutionJournal>,
}

/// Rust-first composition root. Every stock-loop dependency is explicit.
#[derive(Default)]
pub struct HarnessBuilder {
    name: Option<String>,
    executor: Option<Arc<dyn Executor>>,
    journal: Option<Arc<dyn ExecutionJournal>>,
    model: Option<Model>,
    provider: Option<Arc<dyn ModelProvider>>,
    context: ContextPipeline,
    tools: ToolRegistry,
    capabilities: Vec<String>,
    limits: Limits,
    tasks: TaskRegistry,
    durable_host: Option<Arc<dyn DurableTaskHost>>,
    interactions: Option<Arc<dyn InteractionRouter>>,
    policy: Option<Arc<dyn ToolPolicy>>,
    content: Option<crate::runtime::ContentBindings>,
    concurrency: Option<usize>,
}

impl HarnessBuilder {
    /// Starts an empty builder; `build` rejects missing execution bindings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
        self.tools = value;
        self
    }

    /// Selects pinned typed task definitions for the same composition.
    #[must_use]
    pub fn tasks(mut self, value: TaskRegistry) -> Self {
        self.tasks = value;
        self
    }

    /// Binds durable admission and reconciliation independently of the model loop.
    #[must_use]
    pub fn durable_host(mut self, value: Arc<dyn DurableTaskHost>) -> Self {
        self.durable_host = Some(value);
        self
    }

    /// Binds local addressable interactions independently of model and tools.
    #[must_use]
    pub fn interactions(mut self, value: Arc<dyn InteractionRouter>) -> Self {
        self.interactions = Some(value);
        self
    }

    /// Binds one replaceable tool policy evaluated before live and durable dispatch.
    #[must_use]
    pub fn policy(mut self, value: Arc<dyn ToolPolicy>) -> Self {
        self.policy = Some(value);
        self
    }

    /// Binds owner-authenticated file staging and resolution for tasks/tools.
    #[must_use]
    pub fn content(mut self, value: crate::runtime::ContentBindings) -> Self {
        self.content = Some(value);
        self
    }

    /// Sets the local live-task concurrency bound.
    #[must_use]
    pub fn concurrency(mut self, value: usize) -> Self {
        self.concurrency = Some(value);
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

    /// Validates dependency exclusivity and constructs the immutable runtime binding.
    pub fn build(self) -> Result<HarnessBundle> {
        self.limits.validate()?;
        let journal = self
            .journal
            .ok_or_else(|| Error::Invalid("execution journal binding is missing".into()))?;
        let name = self.name.unwrap_or_else(|| "harness".into());
        if name.trim().is_empty() {
            return Err(Error::Invalid("harness name is empty".into()));
        }
        let capabilities = Capabilities::new(self.capabilities);
        let scope = RuntimeScope::new(capabilities.clone(), self.limits)?;
        let runtime = Bindings {
            tasks: self.tasks,
            tools: self.tools.clone(),
            scope: scope.clone(),
            concurrency: self.concurrency.unwrap_or(64),
            durable_host: self.durable_host,
            interactions: self.interactions,
            content: self.content,
            policy: self.policy.clone(),
        }
        .build()?;
        let executor = if let Some(executor) = self.executor {
            if self.model.is_some()
                || self.provider.is_some()
                || !self.context.stage_names().is_empty()
            {
                return Err(Error::Invalid(
                    "custom executor cannot silently ignore stock bindings".into(),
                ));
            }
            executor
        } else {
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
            self.tools.definitions()?;
            Arc::new(
                StockExecutor::new(model, provider, self.context, self.tools)
                    .with_limits(self.limits)
                    .with_tool_authority(scope, self.policy)?,
            )
        };
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
        })
    }
}

impl HarnessBundle {
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
    pub fn executor(&self) -> Arc<dyn Executor> {
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
        self.executor.execute(input, self.journal.as_ref()).await
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;

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
        assert!(
            HarnessBuilder::new()
                .model(model, Arc::new(Provider))
                .journal(Arc::new(UnusedJournal))
                .executor(Arc::clone(&bundle.executor))
                .build()
                .is_err()
        );
        let custom = HarnessBuilder::new()
            .executor(Arc::clone(&bundle.executor))
            .journal(Arc::new(UnusedJournal))
            .tools(coding_tools(Arc::new(Host {
                definition_name: None,
            }))?)
            .build()?;
        assert_eq!(
            custom.runtime().tool("acyclic.filesystem")?.name,
            "acyclic.filesystem"
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
                input_schema: json!({"type": "object", "properties": {}, "additionalProperties": false}),
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
                call_id: format!("call-{name}"),
                name: name.into(),
                arguments: json!({}),
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
    fn coding_factory_rejects_a_host_that_renames_a_tool() {
        let host: Arc<dyn CodingToolHost> = Arc::new(Host {
            definition_name: Some("acyclic.other"),
        });
        assert!(matches!(coding_tools(host), Err(Error::Invalid(_))));
    }
}
