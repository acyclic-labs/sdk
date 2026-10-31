//! An agent's defaults as one plain value. A host takes a preset whole or
//! overrides any field, then applies it to its harness composition; the same
//! value configures a local harness and a hosted one.
use crate::approvals::ApprovalPolicy;
use serde::{Deserialize, Serialize};

/// `GraphCoder`'s system prompt.
const GRAPHCODER_PROMPT: &str = "You are GraphCoder, a coding agent working in the user's checkout.
Read relevant code with rg before editing. Persist until the task is done; ask only when blocked. Run the covering tests or build, then report the result, checks and anything unfinished.
Run independent tool calls in parallel. Delegate independent work with spawn, giving each child a self-contained brief and explicit ownership. Children work in forks: use send for updates, wait for their reports, inspect their diffs and merge accepted branches with fs.
Keep durable goals in .volumes/scratch/GOAL.md and plans in .volumes/scratch/plans/. Scratch is private retained storage for notes, plans and large tool output, never merged into the project.
Follow the stated approval policy. A refused action is a tool failure: report it or choose an allowed action; never treat it as executed.";

/// The defaults an agent runs with: its model, prompt, tool selection, limits
/// and approvals.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct Preset {
    /// Model selection (`provider/model`). `None` takes the host's configured
    /// model, then its provider catalog's default.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    pub model: Option<String>,
    /// System prompt opening every agent turn's context; `None` sends none.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    pub prompt: Option<String>,
    /// Standard tools enabled, by name (see [`crate::tools::catalog`]); a
    /// name outside the catalog is refused when the harness opens.
    pub tools: Vec<String>,
    /// Whether credential-free web retrieval is enabled; search also needs a
    /// configured provider.
    pub web_tools: bool,
    /// Model rounds per turn; each round may run several tool calls. `None`
    /// keeps the harness limit.
    #[cfg_attr(feature = "wasm", tsify(type = "number | null"))]
    pub max_steps: Option<usize>,
    /// Open children each agent may have at once; `None` is unbounded.
    #[cfg_attr(feature = "wasm", tsify(type = "number | null"))]
    pub max_children: Option<u32>,
    /// Ordered approval chain bound to every enabled tool.
    pub approval_policy: ApprovalPolicy,
}

impl Preset {
    /// `GraphCoder`'s defaults: the host's model, a coding-agent prompt, every
    /// standard tool and the web tools, unbounded children, and approvals
    /// that accept every action.
    #[must_use]
    pub fn graphcoder() -> Self {
        Self {
            model: None,
            prompt: Some(GRAPHCODER_PROMPT.to_owned()),
            tools: crate::tools::default_names(),
            web_tools: true,
            max_steps: None,
            max_children: None,
            approval_policy: ApprovalPolicy::accept_all("graphcoder-default"),
        }
    }
}
