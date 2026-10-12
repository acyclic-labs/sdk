use super::*;
use serde::{Deserialize, Serialize};

/// Final SDK snapshot for an approved mapping.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeProcessPublishedVolume {
    /// Manifest position.
    pub index: usize,
    /// Qualified SDK result.
    pub snapshot: acyclic_fs::NativePublicationSnapshot,
}

/// Authenticated launch and final observation.
#[derive(Debug, Serialize)]
pub struct NativeProcessReceipt {
    /// Original task.
    pub task: crate::TaskId,
    /// Approved command.
    pub command: OperationId,
    /// Original dispatch fence.
    pub original_fence: crate::scheduler::LeaseFence,
    /// Effect observation.
    pub effect: EffectObservation,
    /// Whether settlement committed.
    pub observed: bool,
    /// Retained process output.
    pub diagnostic: Option<crate::conversation::FileRef>,
    /// Qualified publications.
    pub published: Vec<NativeProcessPublishedVolume>,
    /// Original operation work.
    pub work: acyclic_fs::WorkCounters,
    #[serde(skip)]
    pub(super) custody: acyclic_fs::NativeAllocation,
}
