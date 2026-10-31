//! Semantic profile values shared by admission and protocol projection.
use serde::{Deserialize, Serialize};

/// Reasoning baseline pinned for an epoch, independent of role updates.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "mode",
    content = "effort",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Reasoning {
    /// No reasoning control is admitted for this model.
    Disabled,
    /// Provider effort control, with summaries for display.
    Effort(Effort),
    /// Adaptive thinking with the selected effort.
    Adaptive(Effort),
    /// Provider thinking-level control.
    Level(Effort),
}

/// Model-independent effort from the admitted profile files.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effort {
    /// Low effort.
    Low,
    /// Medium effort.
    Medium,
    /// High effort.
    High,
    /// Adaptive effort, only where the provider admits it.
    Adaptive,
}

/// Cache semantics selected by the model profile.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cache {
    /// Provider-managed implicit caching.
    Automatic,
    /// Short-lived explicit breakpoint.
    Ephemeral,
    /// Hour-lived explicit breakpoint.
    Hour,
    /// Model-specific retained cache object.
    Explicit,
}
