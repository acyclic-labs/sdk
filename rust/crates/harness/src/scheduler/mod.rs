//! Deterministic durable scheduling and structured concurrency.

mod apply;
mod declarations;
mod events;
mod inbox;
mod orchestration;
mod physical;
mod sessions;
mod state;
#[cfg(test)]
mod tests;
mod validation;

pub use declarations::{
    DurableOwner, EntrypointRef, LeaseFence, OperationSpec, Orchestration, ParentLink, Reservation,
    ResourceRequest, SessionLimits,
};
pub use events::SchedulerEvent;
pub use inbox::{InboxItem, TaskInbox};
pub use orchestration::{
    AssemblyKind, OrchestrationDecision, assembly_invocation_digest, reduction_invocation_digest,
};
pub use state::{OperationPhase, OperationState, WorkflowSuspension};
pub(crate) use validation::require_execution_owner;

use crate::store::HistoryIo;
use crate::wire::ErrorCode::{Conflict, Invalid, NotFound, Unauthorized, Unsupported};
use crate::{
    Error, OperationId, Outcome, Result, TaskId, conversation::FileRef, core::Authority,
    resources::CheckpointRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TaskModelClaims {
    ceiling: u64,
    attempts: BTreeMap<OperationId, BTreeMap<u32, [u8; 32]>>,
}
impl TaskModelClaims {
    fn used(&self) -> u64 {
        self.attempts.values().map(|steps| steps.len() as u64).sum()
    }
}

/// Pure projection of dependency, capacity, ownership and cancellation events.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Scheduler {
    operations: BTreeMap<OperationId, OperationState>,
    child_slots: BTreeMap<(OperationId, String), OperationId>,
    completion_order: Vec<OperationId>,
    #[serde(default)]
    sessions: BTreeMap<OperationId, SessionLimits>,
    #[serde(default)]
    model_claims: BTreeMap<OperationId, TaskModelClaims>,
}
impl Scheduler {
    /// Creates an empty projection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: BTreeMap::new(),
            child_slots: BTreeMap::new(),
            completion_order: Vec::new(),
            sessions: BTreeMap::new(),
            model_claims: BTreeMap::new(),
        }
    }
    /// Returns one operation's current projection.
    #[must_use]
    pub fn operation(&self, id: OperationId) -> Option<&OperationState> {
        self.operations.get(&id)
    }
    /// Structured children in stable slot order.
    pub fn children(&self, parent: OperationId) -> impl Iterator<Item = (&str, &OperationState)> {
        self.child_slots
            .range((parent, String::new())..)
            .take_while(move |((id, _), _)| *id == parent)
            .filter_map(|((_, slot), child)| {
                self.operations
                    .get(child)
                    .map(|state| (slot.as_str(), state))
            })
    }
}

fn observe(work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}
pub(crate) fn diagnostic(
    work: &Option<&mut dyn HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_ref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        Unauthorized => Error::Unauthorized(reason.into()),
        Conflict => Error::Conflict(reason.into()),
        NotFound => Error::NotFound(reason.into()),
        Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}
fn revision(revision: u64, work: &Option<&mut dyn HistoryIo>) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| diagnostic(work, Invalid, "revision exhausted"))
}
