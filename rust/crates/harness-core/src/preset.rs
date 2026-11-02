//! Shared agent defaults for local, browser and hosted compositions.
use crate::approvals::ApprovalPolicy;
use serde::{Deserialize, Serialize};

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
