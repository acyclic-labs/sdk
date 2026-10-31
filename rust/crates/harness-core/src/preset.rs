//! Shared agent defaults for local, browser and hosted compositions.
use crate::approvals::ApprovalPolicy;
use serde::{Deserialize, Serialize};

/// The common agent instruction, shared by every host.
#[cfg(feature = "filesystem")]
pub(crate) const AGENT_PROMPT: &str = "You are a coding agent working in the user's checkout.
Read relevant code with rg before editing. Persist until the task is done; ask only when blocked. Run the covering tests or build, then report the result, checks and anything unfinished.
Run independent tool calls in parallel. Delegate independent work with spawn, giving each child a self-contained brief and explicit ownership. Children inherit settled conversation in their forks: use send for updates, wait for their reports, inspect their diffs and merge accepted branches with fs and rerun the covering tests. For a large change, give a reviewer the task, diff and test output.
Plans and notes go in .volumes/scratch/plans/ and .volumes/scratch/memory.md; GOAL.md is used only when looping on a goal. Scratch at .volumes/scratch is private retained storage for large tool output and working files, never merged into the project.
Follow the stated approval policy. A refused action is a tool failure: report it or choose an allowed action; never treat it as executed.";

/// Host-backed tools and approvals. Instructions and execution capacity have
/// one shared source rather than per-preset overrides.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct Preset {
    /// Standard tools enabled, by name (see [`crate::tools::catalog`]); a
    /// name outside the catalog is refused when the harness opens.
    pub tools: Vec<String>,
    /// Whether credential-free web retrieval is enabled; search also needs a
    /// configured provider.
    pub web_tools: bool,
    /// Open children each agent may have at once; `None` is unbounded.
    #[cfg_attr(feature = "wasm", tsify(type = "number | null"))]
    pub max_children: Option<u32>,
    /// Ordered approval chain bound to every enabled tool.
    pub approval_policy: ApprovalPolicy,
}

impl Preset {
    /// Standard tools, web retrieval, unbounded children and approval rules
    /// that accept every action.
    #[must_use]
    pub fn graphcoder() -> Self {
        Self {
            tools: crate::tools::default_names(),
            web_tools: true,
            max_children: None,
            approval_policy: ApprovalPolicy::accept_all("graphcoder-default"),
        }
    }
}
