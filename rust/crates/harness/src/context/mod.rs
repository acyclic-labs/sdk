//! Ordered function-based context assembly.

use crate::contract::next_revision;
use crate::{
    Result,
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef, Limits},
    model::{ModelContent, ModelMessage, ModelRole},
    projection::SelectedModelContext,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey as StreamIdempotencyKey, MAX_RECORD_BYTES,
    ReadRequest, StreamError, StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::StreamExt as _;
mod accounting;
pub use accounting::*;
mod selection;
pub(crate) use selection::place_messages_with_io;
pub use selection::*;
mod discovery;
pub use discovery::*;
mod compaction;
pub use compaction::*;
pub(crate) use compaction::clone_context_prefix;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use compaction::clone_retention_native;
mod snapshot;
pub use snapshot::*;

mod checkpoint;
pub use checkpoint::*;
mod durable;
pub use durable::*;
mod validation;
pub use validation::*;
mod value;
pub use value::*;
mod pipeline;
pub use pipeline::*;
mod source;
pub use source::*;
mod compaction_stage;
pub use compaction_stage::*;
mod allocations;
pub use allocations::*;
use durable::{context_stream_error, decode_context_json};
pub(crate) use validation::verify_file;
use validation::validate_context_refs;
use value::{ContextMessages, BorrowedContext};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use allocations::{context_contract_native, reserve_context_vec_native};
use allocations::append_context_messages;
#[cfg(test)]
mod tests;
