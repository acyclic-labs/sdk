//! Shared domain/wire conversion for every Stream transport and persistence adapter.

use bytes::Bytes;

use acyclic_stream_contract_core::{
    AppendOutcome, AppendReceipt, CommitCondition, CommitConflict, CommitId, CommitMutation,
    CommitOutcome, CommittedAppend, CommittedEnvelope, CommittedFork, CommittedMutation,
    ForkReceipt, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, Record, StreamError,
    StreamPath,
};
use acyclic_stream_wire::wire;

#[cfg(test)]
mod read_response_tests;

#[cfg(test)]
mod origin_tests;

include!("conversion_0.rs");

include!("conversion_1.rs");

include!("records.rs");

/// Canonical request encoders and their exact encoded lengths.
pub mod requests;

pub mod session;

mod watermarks;
pub use watermarks::watermarks_from_wire;

mod head;
mod provider_cursor;
mod receipt;
mod record_frames;
mod revision;
pub use acyclic_stream_contract_core::revision::revision_to_wire;
pub use head::{
    fork_origin_from_wire, fork_origin_to_wire, head_from_wire, head_to_wire, heads_from_response,
    heads_from_wire, heads_request_to_wire, heads_to_wire,
};
pub use provider_cursor::provider_cursor;
pub use receipt::{fork_receipt_from_wire, receipt_from_wire, receipt_to_wire};
pub use record_frames::record_frames;
pub use revision::revision_from;
#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use revision::selection_for_follow;
pub use revision::{
    cut_from_wire, cut_proven_by, cut_to_wire, issued_from_wire, opened_to_wire,
    revision_from_wire, selection_from_opened, selection_to_wire, tail_response,
};

pub use acyclic_stream_contract_core::status;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod status_contract;
pub use status::status;

#[cfg(test)]
mod change_golden_tests;
#[cfg(test)]
mod record_codec_tests;
