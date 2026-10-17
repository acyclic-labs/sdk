use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Create a child in a lazy fork of the calling task's workspace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpawnInput {
    /// Task instructions for the child.
    pub task: String,
    /// Optional child name, scoped to this parent.
    pub name: Option<String>,
    /// Optional capability subset; child turns need model:generate and mail:read.
    /// Omitted means inherit the parent's capabilities.
    pub grants: Option<Vec<String>>,
}

/// Queue one immutable child message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SendInput {
    /// Child reference returned by spawn.
    pub agent: String,
    /// Message text.
    pub message: String,
}

/// Which requested children must settle before wait returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WaitMode {
    /// Return when any requested child settles.
    Any,
    /// Return when all requested children settle.
    All,
}

/// Wait for child answers or failures, with bounded immutable result references.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WaitInput {
    /// Child references returned by spawn.
    pub agents: Vec<String>,
    /// Defaults to all.
    pub mode: Option<WaitMode>,
}

/// Stop and drain a child and its descendants, then freeze its retained history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArchiveInput {
    /// Child reference returned by spawn.
    pub agent: String,
}
