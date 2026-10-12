//! Shared domain/wire conversion for every Stream transport and persistence adapter.

use bytes::Bytes;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type WireMeter<'a> = Option<&'a Arc<dyn NativeReadMeter>>;
#[cfg(target_arch = "wasm32")]
pub(crate) type WireMeter<'a> = Option<&'a ()>;

use crate::memory::MemoryOutput;

use crate::{
    AppendOutcome, AppendReceipt, CommitCondition, CommitConflict, CommitId, CommitMutation,
    CommitOutcome, CommittedAppend, CommittedEnvelope, CommittedFork, CommittedMutation,
    ForkReceipt, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, Record, StreamError,
    StreamPath, wire,
};

mod funding;
mod encoding;
mod conversion;
mod frames;
pub(crate) use encoding::CanonicalWire;
pub use conversion::*;
pub use frames::{read_response_wire, read_response_batch, read_response_records};
#[cfg(test)]
pub(crate) use frames::zstd_read_response;
