#![doc = include_str!("../README.md")]
/// Session admission version. Checked before credentials or the first request.
pub const SESSION_PROTOCOL_VERSION: u32 = 3;

/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;
pub mod completion;
pub mod demand;
#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

use crate::OwnedBytes as Bytes;
use acyclic_stream_wire::wire;
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use std::future::Future;
use thiserror::Error;

mod pending;
#[cfg(not(target_arch = "wasm32"))]
pub use pending::NativePending;
pub use pending::{AppendPending, Pending};
#[doc(hidden)]
pub mod change;
#[doc(hidden)]
pub mod follow_filter;
#[doc(hidden)]
pub mod interest;
pub use follow_filter::FollowFilter;
mod operation_claim;
pub mod preparation;
#[doc(hidden)]
pub mod query_sync;
pub mod request;
pub mod runtime;
/// gRPC status classification for stream categories.
pub mod status;
pub mod trace;
#[cfg(all(loom, not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod verification;
// The publication models in change and interest wake through this probe.
#[cfg(test)]
#[cfg(loom)]
#[path = "../../stream/publication-models/probe.rs"]
mod probe;
pub use operation_claim::{
    OperationAttempt, OperationClaim, OperationClaims, OperationObservation, OperationTermination,
};

/// Maximum opaque record body.
pub const MAX_RECORD_BYTES: usize = wire::StreamLimit::MaxRecordBytes as usize;
/// Maximum records, participants, mutations, or path segments in one request.
pub const MAX_ITEMS: usize = wire::StreamLimit::MaxItems as usize;
/// Records replayed per page when a follow resumes from a cursor.
pub const REPLAY_PAGE: u32 = 1_024;
const _: () = assert!(REPLAY_PAGE as usize == MAX_ITEMS);
/// Maximum canonical application command, including metadata.
pub const MAX_COMMAND_BYTES: usize = wire::StreamLimit::MaxCommandBytes as usize;
/// Filesystem resource index prefix width. Complete paths remain in metadata
/// and are authorized after coarse indexed selection.
pub const RESOURCE_INDEX_PREFIX_BYTES: usize = 1024;
/// Maximum caller retry-identity width.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = wire::StreamLimit::MaxIdempotencyKeyBytes as usize;
/// Maximum canonical path text accepted by the single-command wire format.
pub const MAX_PATH_BYTES: usize = wire::StreamLimit::MaxPathBytes as usize;

#[path = "lib/path.rs"]
#[doc(hidden)]
pub mod path;
pub use path::StreamPath;
#[path = "lib/revision.rs"]
pub mod revision;
pub use revision::{Revision, RevisionCut, RevisionScope};
mod provenance;
pub use provenance::{ForkCut, Provenance};

include!("lib/domain.rs");
include!("lib/write.rs");
include!("lib/head.rs");
include!("lib/receipt.rs");

#[doc(hidden)]
pub mod changes;
#[doc(hidden)]
pub mod record_headers;
pub use changes::{
    ChangeEntryRef, changes_chunk_len, changes_context, changes_encoded_len, decode_changes,
    is_changes,
};
pub use record_headers::{RecordAttribute, RecordAttributeIter, RecordAttributes, RecordHeaders};

mod prepared_capacity;
pub use prepared_capacity::PreparedRecordCapacity;

include!("lib/provider_context.rs");
include!("lib/provider.rs");
include!("lib/native_provider.rs");
include!("lib/provider_ext.rs");

include!("lib/stream.rs");

include!("lib/ephemeral_lease.rs");

include!("lib/error.rs");

#[cfg(not(target_arch = "wasm32"))]
pub mod alloc;
mod owned_bytes;
#[cfg(all(feature = "js-bytes", target_arch = "wasm32"))]
pub use acyclic_stream_contract_bytes::javascript as javascript_bytes;
#[cfg(any(test, feature = "test-support"))]
pub use owned_bytes::hashed_on_this_thread;
pub use owned_bytes::{
    OwnedBytes, OwnedBytesBacking, SealedBytes, UniqueOwnedBytes, content_digest,
};
#[cfg(not(target_arch = "wasm32"))]
pub use owned_bytes::{PreparedOwnedBytes, PreparedOwnedBytesError, PreparedOwnedBytesFailure};
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub mod native;

#[cfg(all(test, not(target_arch = "wasm32")))]
use acyclic_stream_contract_bytes::test_meter;
#[cfg(not(target_arch = "wasm32"))]
pub use native::{
    LocalStreamFailure, NativeProviderFuture, NativeRecordStream, NativeStreamOutput,
    NativeStreamResult, PreparedNativeCommit,
};

#[cfg(test)]
#[path = "lib/path_text_tests.rs"]
mod path_text_tests;
