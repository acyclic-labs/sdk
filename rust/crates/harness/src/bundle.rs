//! Code-defined named bundles; bundles are constructors, not a configuration language.

use crate::{
    Capabilities, Error, Result,
    context::ContextPipeline,
    executor::{Executor, StockExecutor},
    model::{Model, ModelProvider},
    tool::ToolRegistry,
};
use std::sync::Arc;

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
        const REQUIRED: &[&str] = &[
            "acyclic.filesystem",
            "acyclic.edit",
            "acyclic.search",
            "acyclic.shell",
            "acyclic.pty",
            "acyclic.lsp",
            "acyclic.browser",
            "acyclic.web",
            "acyclic.mcp",
            "acyclic.skills",
            "acyclic.delegation",
            "acyclic.compaction",
        ];
        let missing = REQUIRED
            .iter()
            .copied()
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
}
