//! Ordered function-based context assembly.

use crate::{Result, model::ModelMessage};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Replaceable memory/retrieval/skill source used by reusable stock stages.
pub trait ContextSource: Send + Sync {
    /// Resolves model-visible messages for the current step.
    fn load<'a>(&'a self, input: &'a ContextInput) -> BoxFuture<'a, Result<Vec<ModelMessage>>>;
}

/// Placement of source messages relative to existing context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextPlacement {
    /// Insert before current messages.
    Prepend,
    /// Insert after current messages.
    Append,
}

/// Reusable stage for memory, retrieval, or skills.
pub struct SourceStage {
    name: String,
    revision: String,
    source: Arc<dyn ContextSource>,
    placement: ContextPlacement,
}

impl SourceStage {
    /// Creates a named source stage; `memory`, `retrieval`, and `skills` are ordinary names.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        revision: impl Into<String>,
        source: Arc<dyn ContextSource>,
        placement: ContextPlacement,
    ) -> Self {
        Self {
            name: name.into(),
            revision: revision.into(),
            source,
            placement,
        }
    }
}

impl ContextStage for SourceStage {
    fn name(&self) -> &str {
        &self.name
    }

    fn contract(&self) -> Value {
        serde_json::json!({
            "name": self.name,
            "revision": self.revision,
            "placement": match self.placement {
                ContextPlacement::Prepend => "prepend",
                ContextPlacement::Append => "append",
            },
        })
    }

    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        mut context: Context,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            let mut loaded = self.source.load(input).await?;
            match self.placement {
                ContextPlacement::Prepend => {
                    loaded.append(&mut context.messages);
                    context.messages = loaded;
                }
                ContextPlacement::Append => context.messages.append(&mut loaded),
            }
            Ok(context)
        })
    }
}

/// Deterministic reusable compaction stage retaining a bounded message tail.
pub struct CompactionStage {
    /// Maximum messages retained after compaction.
    pub max_messages: usize,
    /// Optional caller-produced summary prepended when compaction occurs.
    pub summary: Option<ModelMessage>,
}

impl ContextStage for CompactionStage {
    fn name(&self) -> &str {
        "compaction"
    }

    fn contract(&self) -> Value {
        serde_json::json!({
            "name": self.name(),
            "revision": "1",
            "max_messages": self.max_messages,
            "summary": self.summary,
        })
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        mut context: Context,
    ) -> BoxFuture<'a, Result<Context>> {
        Box::pin(async move {
            if self.max_messages == 0 {
                return Err(crate::Error::Invalid(
                    "compaction max_messages must be positive".into(),
                ));
            }
            if context.messages.len() > self.max_messages {
                let keep = self
                    .max_messages
                    .saturating_sub(usize::from(self.summary.is_some()));
                let split = context.messages.len().saturating_sub(keep);
                let mut retained = context.messages.split_off(split);
                if let Some(summary) = &self.summary {
                    retained.insert(0, summary.clone());
                }
                context.messages = retained;
            }
            Ok(context)
        })
    }
}

/// Mutable context assembled for one model step.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Context {
    /// Ordered model-visible messages.
    pub messages: Vec<ModelMessage>,
    /// Stage-owned structured metadata that remains reconstructable.
    pub metadata: Value,
}

/// Inputs visible to every context stage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextInput {
    /// Current durable turn input.
    pub input: Value,
    /// Zero-based executor step.
    pub step: u32,
    /// Model-visible results accumulated by earlier executor steps.
    pub prior_messages: Vec<ModelMessage>,
}

/// Replaceable ordered context transformation.
pub trait ContextStage: Send + Sync {
    /// Stable stage name used for diagnostics and composition.
    fn name(&self) -> &str;

    /// Immutable serializable identity included in durable execution binding.
    fn contract(&self) -> Value;

    /// Transforms context; stage order is the order supplied by application code.
    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        context: Context,
    ) -> BoxFuture<'a, Result<Context>>;
}

/// Explicit ordered pipeline; no hidden framework stages are inserted.
#[derive(Clone, Default)]
pub struct ContextPipeline(Vec<Arc<dyn ContextStage>>);

impl ContextPipeline {
    /// Creates a pipeline in exact execution order.
    #[must_use]
    pub fn new(stages: impl IntoIterator<Item = Arc<dyn ContextStage>>) -> Self {
        Self(stages.into_iter().collect())
    }

    /// Appends one stage and returns the pipeline for code-first composition.
    #[must_use]
    pub fn with(mut self, stage: Arc<dyn ContextStage>) -> Self {
        self.0.push(stage);
        self
    }

    /// Runs every stage in declared order.
    pub async fn run(&self, input: &ContextInput) -> Result<Context> {
        let mut context = Context {
            messages: input.prior_messages.clone(),
            metadata: Value::Null,
        };
        for stage in &self.0 {
            context = stage.apply(input, context).await?;
        }
        Ok(context)
    }

    /// Returns stage names in exact execution order.
    #[must_use]
    pub fn stage_names(&self) -> Vec<&str> {
        self.0.iter().map(|stage| stage.name()).collect()
    }

    /// Returns immutable stage contracts in exact execution order.
    #[must_use]
    pub fn contracts(&self) -> Vec<Value> {
        self.0.iter().map(|stage| stage.contract()).collect()
    }
}
