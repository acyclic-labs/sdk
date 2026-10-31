//! Shared agent defaults for local, browser and hosted compositions.
use crate::approvals::ApprovalPolicy;
use serde::{Deserialize, Serialize};

/// The common agent instruction, shared by every host.
#[cfg(feature = "filesystem")]
pub(crate) const AGENT_PROMPT: &str = "You are a coding agent working in the user's checkout.
Read relevant code with rg before editing. Persist until the task is done; ask only when blocked. Run the covering tests or build, then report the result, checks and anything unfinished.
Run independent tool calls in parallel. Delegate with spawn {name, task, role}, giving each child explicit ownership. Workers fork your conversation; advisors and reviewers see only their self-contained brief and sent files. A brief or plan says what was tried and learned. Use send for updates, wait when the next step depends on a report, inspect the diff and explicitly fs merge accepted worker branches. Give a fresh reviewer the task, diff and actual check output. At consequential decisions, send an advisor an immutable versioned plan; it returns a revision based on that version, never edits or merges into your tree.
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
            approval_policy: ApprovalPolicy::accept_all("graphcoder-default"),
        }
    }
}
