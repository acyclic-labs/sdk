//! Shared domain/wire conversion for every Stream transport and persistence adapter.

use bytes::Bytes;

#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type WireMeter<'a> = Option<&'a Arc<dyn NativeReadMeter>>;
#[cfg(target_arch = "wasm32")]
pub(crate) type WireMeter<'a> = Option<&'a ()>;

use crate::memory::NativeStreamOutput;

use crate::{
    AppendOutcome, AppendReceipt, CommitCondition, CommitConflict, CommitId, CommitMutation,
    CommitOutcome, CommittedAppend, CommittedEnvelope, CommittedFork, CommittedMutation,
    ForkReceipt, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, Record, StreamError,
    StreamPath, wire,
};

mod conversion;
mod encoding;
mod frames;
mod funding;
pub use conversion::*;
pub(crate) use encoding::CanonicalWire;
pub use frames::{read_response_batch, read_response_records, read_response_wire};
