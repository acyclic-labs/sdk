//! The owner-written `.tasks/<id>/status.json` codec, shared by the writer and
//! every follower so neither side can drift from the other.
use super::{Error, Result};
use serde::{Deserialize, Serialize};

/// Lifecycle projected from the authenticated task owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    /// Admitted and not yet settled.
    Pending,
    /// Settled successfully.
    Completed,
    /// Cancelled before settling.
    Cancelled,
    /// Settled with a failure.
    Failed,
    /// Settled without a knowable outcome.
    Indeterminate,
    /// Hidden by the user; files are retained.
    Deleted,
}

/// One task's lifecycle file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStatus {
    /// The task this file describes.
    pub task: crate::TaskId,
    /// Admitting parent, if any.
    pub parent: Option<crate::TaskId>,
    /// Latest lifecycle.
    pub state: TaskState,
    /// The task settled and waits for the user.
    pub attention: bool,
    /// Host-attested agent owning the task, if any.
    pub owner_agent: Option<crate::AgentId>,
}

impl TaskStatus {
    /// Decodes untrusted status bytes; unknown fields and states are rejected.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|_| Error::InvalidSegment)
    }

    /// The canonical bytes `parse` accepts back unchanged.
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|_| Error::InvalidSegment)
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
