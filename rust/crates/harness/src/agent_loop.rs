//! Code-first agent behavior over the same typed task context as tools and tasks.

use crate::{Outcome, Result, conversation::Attachment, runtime::TaskContext};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One typed invocation of a custom agent loop. The host stages this live
/// task's input before any durable admission; canonical conversations remain
/// separately file-backed and authoritative.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInput {
    /// Current human prompt, bounded before local admission.
    pub prompt: String,
    /// Ordered immutable input files resolved only under task grants.
    pub attachments: Vec<Attachment>,
}

impl AgentInput {
    /// Simple text invocation.
    #[must_use]
    pub fn text(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            attachments: Vec::new(),
        }
    }
}

/// One custom-loop result. Canonical publication stages its text and refs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOutput {
    /// User-visible text produced by this invocation.
    pub text: String,
    /// Already staged output files and artifacts.
    pub attachments: Vec<Attachment>,
    /// Provider- or application-owned structured metadata.
    pub metadata: Value,
}

impl AgentOutput {
    /// Simple text result with no additional files.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            attachments: Vec::new(),
            metadata: Value::Null,
        }
    }
}

/// Replaceable live behavior. Durable loops implement the separate resumable
/// task machine contract; this future is never claimed to survive host loss.
pub trait AgentLoop: Send + Sync {
    /// Runs with the same scoped model, context, tool, task, interaction, and
    /// file operations that application-authored tasks receive.
    fn run(
        &self,
        context: TaskContext,
        input: AgentInput,
    ) -> BoxFuture<'static, Result<AgentOutput>>;
}

/// Stable local task identity and full terminal outcome of one loop invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentRunOutput {
    /// Task identity retained even when the loop fails or is cancelled.
    pub task_id: String,
    /// Explicit task outcome, including uncertainty.
    pub outcome: Outcome<AgentOutput>,
}
