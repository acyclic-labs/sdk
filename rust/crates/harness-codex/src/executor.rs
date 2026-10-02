//! `CodexExecutor`: the public surface (A1). The turn itself is task A7.

use crate::{
    events::CodexEvent,
    meter::{Unmetered, UsageMeter},
};
use acyclic_harness::{
    Error, Result,
    executor::{ExecutionJournal, Executor, TurnInput, TurnOutput},
    runtime::{RuntimeScope, ToolPolicy},
    tool::ToolRegistry,
};
use futures::{FutureExt as _, future::BoxFuture};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc, time::Instant};

/// Where the proxy forwards model calls.
#[derive(Clone, Debug, PartialEq)]
pub struct Upstream {
    /// Responses API base, e.g. `https://api.openai.com/v1`.
    pub base_url: String,
    /// The real key. Only the proxy holds it; Codex gets a dummy.
    pub api_key: String,
    /// Fields merged into every request body (e.g. `reasoning`).
    pub extra_body: Value,
}

/// Everything one Codex turn needs besides tools.
#[derive(Clone, Debug)]
pub struct CodexConfig {
    /// The `codex` binary; must report [`crate::CODEX_VERSION`].
    pub binary: PathBuf,
    /// Upstream model id, e.g. `gpt-5.5`.
    pub model: String,
    /// Where model calls go.
    pub upstream: Upstream,
    /// The directory Codex works in (`-C`).
    pub workspace: PathBuf,
    /// The role prompt, written to `CODEX_HOME/AGENTS.md`.
    pub instructions: String,
    /// Whether Codex may use its own subagents.
    pub subagents: bool,
    /// Cap on proxied model calls for the turn.
    pub max_steps: u32,
    /// Wall-clock limit; past it Codex is terminated.
    pub deadline: Option<Instant>,
    /// Durable directory for per-operation `CODEX_HOME`s. Codex keeps its
    /// session there, so a crashed turn can only resume if this survives.
    pub state_dir: PathBuf,
    /// Continue this Codex thread instead of starting one (follow-up turns).
    pub resume_thread: Option<String>,
}

/// Receives every Codex event as it arrives, for logs and trace spans.
pub trait CodexObserver: Send + Sync {
    /// Called once per parsed line, in order.
    fn event(&self, event: &CodexEvent);
}

/// Runs a turn as one `codex exec --json` process.
pub struct CodexExecutor {
    config: CodexConfig,
    tools: ToolRegistry,
    scope: Option<RuntimeScope>,
    policy: Option<Arc<dyn ToolPolicy>>,
    meter: Arc<dyn UsageMeter>,
    observer: Option<Arc<dyn CodexObserver>>,
}

impl CodexExecutor {
    /// Creates an executor. No tools are offered until a scope grants them.
    #[must_use]
    pub fn new(config: CodexConfig, tools: ToolRegistry) -> Self {
        Self {
            config,
            tools,
            scope: None,
            policy: None,
            meter: Arc::new(Unmetered),
            observer: None,
        }
    }

    /// Exposes the tools `scope` grants, under `policy`, as the stock loop does.
    ///
    /// # Errors
    /// When the policy identity is invalid.
    pub fn with_tool_authority(
        mut self,
        scope: RuntimeScope,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Self> {
        if let Some(policy) = &policy
            && policy.identity().digest == [0; 32]
        {
            return Err(Error::Invalid("tool policy identity has no digest".into()));
        }
        self.scope = Some(scope);
        self.policy = policy;
        Ok(self)
    }

    /// Meters and caps every model call.
    #[must_use]
    pub fn with_meter(mut self, meter: Arc<dyn UsageMeter>) -> Self {
        self.meter = meter;
        self
    }

    /// Streams Codex events to `observer`.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn CodexObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// The configuration this executor runs with.
    #[must_use]
    pub const fn config(&self) -> &CodexConfig {
        &self.config
    }
}

impl Executor for CodexExecutor {
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        async move {
            let _ = (
                input,
                journal,
                &self.tools,
                &self.scope,
                &self.policy,
                &self.meter,
                &self.observer,
            );
            Err(Error::Unsupported(
                "codex executor turn is not built yet (A6, A7)".into(),
            ))
        }
        .boxed()
    }
}
