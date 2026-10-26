//! An agent's defaults as one plain value. A host takes a preset whole or
//! overrides any field, then applies it to its harness composition; the same
//! value configures a local harness and a hosted one.
use crate::approvals::ApprovalPolicy;
use serde::{Deserialize, Serialize};

/// The defaults an agent runs with: its model, tool selection, step limit
/// and approvals.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preset {
    /// Model selection (`provider/model`). `None` takes the host's configured
    /// model, then its provider catalog's default.
    pub model: Option<String>,
    /// Whether the harness's default tool set is enabled.
    pub default_tools: bool,
    /// Whether credential-free web retrieval is enabled; search also needs a
    /// configured provider.
    pub web_tools: bool,
    /// Model and tool steps per turn; `None` keeps the harness limit.
    pub max_steps: Option<usize>,
    /// Ordered approval chain bound to every enabled tool.
    pub approval_policy: ApprovalPolicy,
}

impl Preset {
    /// `GraphCoder`'s defaults: the host's model, the default and web tools,
    /// and approvals that accept every action.
    #[must_use]
    pub fn graphcoder() -> Self {
        Self {
            model: None,
            default_tools: true,
            web_tools: true,
            max_steps: None,
            approval_policy: ApprovalPolicy::accept_all("graphcoder-default"),
        }
    }
}
