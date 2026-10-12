//! Ordered function-based context assembly.

use crate::contract::next_revision;
use crate::{
    Result,
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef, Limits},
    model::{ModelContent, ModelMessage, ModelRole},
    projection::SelectedModelContext,
};
use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey as StreamIdempotencyKey, MAX_RECORD_BYTES,
    ReadRequest, StreamError, StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
mod accounting;
pub use accounting::*;
mod selection;
pub(crate) use selection::place_messages_with_io;
pub use selection::*;
mod discovery;
pub use discovery::*;
mod compaction;
pub(crate) use compaction::clone_context_prefix;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use compaction::clone_retention_native;
pub use compaction::*;
mod snapshot;
pub use snapshot::*;

mod checkpoint;
pub use checkpoint::*;
mod durable;
pub use durable::*;
mod validation;
pub(crate) use validation::{
    checked_compaction_budget, restrict_context_limits, validate_compaction,
    validate_projected_context,
};
mod value;
pub use value::*;
mod pipeline;
pub use pipeline::*;
mod source;
pub use source::*;
mod compaction_stage;
pub use compaction_stage::*;
mod allocations;
use allocations::append_context_messages;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use allocations::{context_contract_native, reserve_context_vec_native};
pub(crate) use validation::verify_file;
use validation::{validate_context_refs, validate_source_messages};
use value::{BorrowedContext, ContextMessages};
#[cfg(test)]
mod tests;
