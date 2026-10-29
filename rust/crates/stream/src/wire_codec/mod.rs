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

mod head;
mod receipt;
mod revision;
pub use head::{
    fork_origin_from_wire, fork_origin_to_wire, head_from_wire, head_to_wire, heads_from_response,
    heads_from_wire, heads_request_to_wire, heads_to_wire,
};
pub use receipt::{fork_receipt_from_wire, receipt_from_wire, receipt_to_wire};
pub use revision::{
    cut_from_wire, cut_proven_by, cut_to_wire, issued_from_wire, revision_from_wire,
    revision_to_wire, tail_response,
};

#[cfg(any(feature = "grpc", all(feature = "wasm", target_arch = "wasm32")))]
mod status;
#[cfg(any(feature = "grpc", all(feature = "wasm", target_arch = "wasm32")))]
pub(crate) use status::{error_status, status};
