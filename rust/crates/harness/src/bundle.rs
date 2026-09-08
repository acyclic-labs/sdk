//! Code-defined named bundles; bundles are constructors, not a configuration language.

use crate::{
    Capabilities, Error, Result,
    context::ContextPipeline,
    executor::{Executor, StockExecutor},
    model::{Model, ModelProvider},
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use futures::future::BoxFuture;
use serde_json::{Value, json};
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
        registry.register(Tool {
            definition: ToolDefinition {
                name: name.into(),
                revision: "1".into(),
                description: description.into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({}),
            },
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
    pub name: String,
    /// Complete replaceable executor.
    pub executor: Arc<dyn Executor>,
    /// Capabilities requested by this composition.
    pub capabilities: Capabilities,
}

impl HarnessBundle {
    /// Wraps any custom executor without changing the runtime core.
    #[must_use]
    pub fn custom(
        name: impl Into<String>,
        executor: Arc<dyn Executor>,
        capabilities: Capabilities,
    ) -> Self {
        Self {
            name: name.into(),
            executor,
            capabilities,
        }
    }

    /// Smallest useful stock model loop, with no tools or hidden context stages.
    #[must_use]
    pub fn minimal(model: Model, provider: Arc<dyn ModelProvider>) -> Self {
        Self::custom(
            "minimal",
            Arc::new(StockExecutor::new(
                model,
                provider,
                ContextPipeline::default(),
                ToolRegistry::default(),
            )),
            Capabilities::new(["model:generate"]),
        )
    }

    /// Stock loop composed from caller-selected stages and tools.
    #[must_use]
    pub fn standard(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
        tools: ToolRegistry,
    ) -> Self {
        Self::custom(
            "default",
            Arc::new(StockExecutor::new(model, provider, context, tools)),
            Capabilities::new(["model:generate", "effect:run"]),
        )
    }

    /// Validates and constructs the complete coding bundle from public tools.
    pub fn coding(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
        tools: ToolRegistry,
    ) -> Result<Self> {
        let missing = CODING_TOOLS
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| tools.get(name).is_none())
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(Error::Invalid(format!(
                "coding bundle is missing tools: {}",
                missing.join(", ")
            )));
        }
        Ok(Self::custom(
            "coding",
            Arc::new(StockExecutor::new(model, provider, context, tools)),
            Capabilities::new(["model:generate", "effect:run", "sandbox:use"]),
        ))
    }

    /// Constructs the complete coding loop from one explicit host implementation.
    pub fn coding_with_host(
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
        host: Arc<dyn CodingToolHost>,
    ) -> Result<Self> {
        Self::coding(model, provider, context, coding_tools(host)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;

    struct Host;

    impl CodingToolHost for Host {
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
        let registry = coding_tools(Arc::new(Host))?;
        assert_eq!(registry.definitions().len(), CODING_TOOLS.len());
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
}
