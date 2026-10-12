//! Typed, durable, executor-neutral open interactions.

use crate::contract::capability;
use crate::{Error, OperationId, Result, conversation::FileRef};
use serde::{Deserialize, Serialize, de::Error as _};
use serde_json::Value;
use std::collections::BTreeSet;
use uuid::Uuid;

mod kind;
pub use kind::InteractionKind;
mod approval;
pub use approval::ApprovalBinding;
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
