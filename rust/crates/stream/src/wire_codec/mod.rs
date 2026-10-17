//! Shared domain/wire conversion for every Stream transport and persistence adapter.

use bytes::Bytes;

use crate::{
    AppendOutcome, AppendReceipt, CommitCondition, CommitConflict, CommitId, CommitMutation,
    CommitOutcome, CommittedAppend, CommittedEnvelope, CommittedFork, CommittedMutation,
    ForkReceipt, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, Record, StreamError,
    StreamPath, wire,
};

#[cfg(test)]
mod read_response_tests;

include!("conversion_0.rs");

include!("conversion_1.rs");

include!("records.rs");

pub(crate) mod requests;

pub mod session;

mod watermarks;
pub use watermarks::watermarks_from_wire;

#[cfg(any(feature = "grpc", all(feature = "wasm", target_arch = "wasm32")))]
mod status;
#[cfg(any(feature = "grpc", all(feature = "wasm", target_arch = "wasm32")))]
pub(crate) use status::status;

include!("read_at.rs");
