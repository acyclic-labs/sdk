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
pub(crate) use decision_file::principal_hex;
mod ticket;
pub use ticket::InteractionTicket;
mod outcome;
pub use outcome::InteractionOutcome;
mod resolution;
pub use resolution::InteractionResolution;
mod receipt;
pub use receipt::ResolutionReceipt;
mod request;
pub use request::{ChoiceOption, Interaction};
mod response;
pub use response::InteractionResponse;
mod codec;
use codec::*;
#[cfg(test)]
mod tests;
