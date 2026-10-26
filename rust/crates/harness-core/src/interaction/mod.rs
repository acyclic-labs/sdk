//! Typed, durable, executor-neutral open interactions.

use crate::contract::capability;
use crate::{Error, OperationId, Result, conversation::FileRef};
use crate::{
    store::{guarded_boundary as boundary, operations::original_native_decode_mode},
    wire::ErrorCode::{Conflict, Invalid},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use uuid::Uuid;

mod kind;
pub use kind::InteractionKind;
mod approval;
pub use approval::ApprovalBinding;
mod decision_file;
pub use decision_file::{ApprovalDecisionFile, ApprovalVerdict};
pub(crate) use decision_file::{LEGACY_DECISION_PRINCIPAL, PublishedApprovalDecision};
mod ticket;
pub use ticket::InteractionTicket;
mod outcome;
pub use outcome::InteractionOutcome;
mod resolution;
pub use resolution::InteractionResolution;
mod receipt;
pub use receipt::ResolutionReceipt;
mod request;
pub use request::{ApprovalRoute, ApprovalSubject, ChoiceOption, Interaction};
mod response;
pub use response::InteractionResponse;
mod codec;
use codec::*;
#[cfg(test)]
mod tests;

/// Stable identity of the interaction that `operation` opens within `scope`:
/// the agent turn (or the bound task) whose execution raised it. Every opener
/// and every responder derives the same id, on the supplied IO context.
pub fn scoped_id(
    scope: [u8; 16],
    operation: OperationId,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<crate::InteractionId> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.hashed_bytes((b"harness/v2/task-interaction\0".len() + 32) as u64)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-interaction\0");
    hasher.update(&scope);
    hasher.update(&operation.into_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(crate::InteractionId::from_bytes(bytes))
}
