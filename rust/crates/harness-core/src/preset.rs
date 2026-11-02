//! Shared agent defaults for local, browser and hosted compositions.
use crate::approvals::ApprovalPolicy;
use serde::{Deserialize, Serialize};

/// Shared approval policy. Tools are selected by their original host registry,
/// rather than copied into a second preset configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct Preset {
    /// Ordered approval chain bound to every enabled tool.
    pub approval_policy: ApprovalPolicy,
}

impl Preset {
    /// GraphCoder's approval rules, accepting every action.
    #[must_use]
    pub fn graphcoder() -> Self {
        Self {
            approval_policy: ApprovalPolicy::accept_all("graphcoder-default"),
        }
    }
}
