#![doc = include_str!("../README.md")]
/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;
#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

use std::sync::Arc;

use crate::OwnedBytes as Bytes;
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use std::future::Future;
use thiserror::Error;

#[cfg(any(
    test,
    feature = "conformance",
    feature = "http",
    feature = "test-support",
    feature = "model-checking"
))]
pub mod conformance;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod filtered_follow_tests;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod follow_tests;
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub mod grpc;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod head_tests;
pub mod http_response;
pub mod pages;
mod pending;
pub mod persistence;
#[cfg(all(feature = "postgres", not(target_arch = "wasm32")))]
pub mod postgres;
pub mod preparation;
#[cfg(not(target_arch = "wasm32"))]
pub use pending::NativePending;
pub use pending::Pending;
mod projection;
pub mod pull;
pub mod request;
pub mod runtime;
#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub mod session;
#[cfg(any(
    test,
    feature = "conformance",
    feature = "http",
    feature = "test-support",
    feature = "model-checking"
))]
pub mod testing;
// The WASM adapter consumes this module on browser builds; native builds keep
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub mod http;
// Request projection is shared by the native HTTP client and the browser adapter.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod browser;
#[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
mod journal_provider;
#[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
pub use journal_provider::{JournalStream, JournalStreamLimits};
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
mod change;
#[cfg(any(
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
mod http_codec;
mod http_validation;
mod interest;
#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
mod journal;
#[cfg(feature = "local")]
mod local;
mod memory;
mod operation_claim;
#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
mod replicated;
pub use operation_claim::{
    OperationAttempt, OperationClaim, OperationClaims, OperationObservation, OperationTermination,
};
mod follow_filter;
mod follow_index;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "grpc", feature = "http", feature = "local")
))]
mod obs;
#[doc(hidden)]
pub mod query_sync;
mod snapshot;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use browser::{BrowserStream, BrowserStreamLimits};
pub use follow_filter::FollowFilter;
/// Follows signed trust Streams into the one Auth trust follower.
#[cfg(not(target_arch = "wasm32"))]
pub mod trust_feed;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::js_error as wasm_error;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::{
    WasmFollow, WasmSessionAuthority, WasmStream, serve_wasm_grpc_session, serve_wasm_session,
};
/// Conversion helpers between the canonical Rust domain types and generated wire messages.
pub mod wire_codec;

#[cfg(all(loom, feature = "local"))]
#[path = "memory/recovery_model.rs"]
/// Bounded schedule verification of production query concurrency paths.
pub mod verification;
#[cfg(all(loom, feature = "local"))]
#[doc(hidden)]
pub use local::replay_model::{
    negative_control_acknowledges_an_unflushed_original, replay_and_flush_preserve_durability,
};
#[cfg(all(loom, feature = "local"))]
#[doc(hidden)]
pub use memory::browser_follow_model as browser_follow_verification;

/// Generated canonical Stream v1 protocol.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "generated prost and tonic bindings"
)]
pub mod wire {
    // All session data variants have total equality, including repeated headers.
    impl Eq for SessionData {}
    include!(concat!(env!("OUT_DIR"), "/acyclic.stream.v1.rs"));
}
/// Canonical public descriptor set used by compatibility gates.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-stream-v1.bin"));
#[cfg(all(feature = "local", feature = "test-support"))]
pub use local::{CutSimulationError, PersistenceCut, fail_cut_truncation};
#[cfg(feature = "local")]
pub use local::{
    LocalDurability, LocalStream, LocalStreamError, LocalStreamFailure, LocalStreamLimits,
    deferring_durability, deferring_provider_durability, requiring_durability,
};
pub use memory::{MemoryLimits, MemoryStream};

/// Maximum opaque record body.
pub const MAX_RECORD_BYTES: usize = wire::StreamLimit::MaxRecordBytes as usize;
/// Shared session credit quantum and current hierarchy depth bound.
pub const MAX_ITEMS: usize = wire::StreamLimit::MaxItems as usize;
const REPLAY_PAGE: u32 = 1_024;
/// Materialized hierarchy page size; larger requests continue through next_after.
pub const CHILDREN_PAGE_ITEMS: u32 = 1_024;
/// Maximum canonical application command, including metadata.
pub const MAX_COMMAND_BYTES: usize = wire::StreamLimit::MaxCommandBytes as usize;

/// Bytes indexed from a filesystem resource path. The complete lossless path
/// remains in change metadata and is authorized after coarse indexed selection.
/// This controls index granularity; generic record keys have no width ceiling.
pub const RESOURCE_INDEX_PREFIX_BYTES: usize = 1024;

/// Route-to-response families for the hosted HTTP projection.
pub const HTTP_RESPONSE_CONTRACT: &[(&str, &str)] = &[
    ("head", "head"),
    ("heads", "heads"),
    ("idempotency/inspect", "observation"),
    ("tail", "tail"),
    ("append", "append"),
    ("fork", "fork"),
    ("read", "read"),
    ("children/page", "children_page"),
    ("commit", "commit"),
    ("commits/read", "envelope"),
    ("tokens/create", "token"),
];

/// Canonical operation vocabulary accepted by Stream access-token grants.
pub const TOKEN_OPERATIONS: &[&str] = &[
    "list", "read", "follow", "append", "fork", "create", "commit",
];

#[path = "lib/path.rs"]
mod path;
pub use path::StreamPath;
#[path = "lib/revision.rs"]
pub mod revision;
pub use revision::{AppliedRevisions, Revision, RevisionCut, RevisionScope, RevisionTracker};
mod provenance;
pub use provenance::{ForkCut, Provenance};

include!("lib/domain.rs");
include!("lib/write.rs");
include!("lib/head.rs");
include!("lib/receipt.rs");

mod changes;
mod record_headers;
pub use changes::{
    ChangeEntryRef, READER_FORMATS, WRITER_FORMAT, changes_chunk_len, changes_encoded_len,
    decode_changes, is_changes,
};
pub use record_headers::{RecordAttribute, RecordAttributeIter, RecordAttributes, RecordHeaders};

include!("lib/provider_context.rs");
include!("lib/provider.rs");
include!("lib/native_provider.rs");
include!("lib/provider_ext.rs");

include!("lib/stream.rs");

include!("lib/error.rs");

include!("lib/tests.rs");

mod owned_bytes;
#[cfg(any(test, feature = "test-support"))]
pub use owned_bytes::hashed_on_this_thread;
#[cfg(all(feature = "js-bytes", target_arch = "wasm32"))]
pub use owned_bytes::javascript as javascript_bytes;
pub use owned_bytes::{
    OwnedBytes, OwnedBytesBacking, SealedBytes, UniqueOwnedBytes, content_digest,
};
#[cfg(not(target_arch = "wasm32"))]
pub use owned_bytes::{PreparedOwnedBytes, PreparedOwnedBytesError, PreparedOwnedBytesFailure};
#[cfg(not(target_arch = "wasm32"))]
mod native;

#[cfg(not(target_arch = "wasm32"))]
pub use native::{
    NativeProviderFuture, NativeRecordStream, NativeStreamOutput, NativeStreamResult,
    PreparedNativeCommit, PreparedRecordCapacity,
};
#[cfg(test)]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod test_latency;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod test_meter;
#[cfg(all(
    test,
    not(target_arch = "wasm32"),
    any(feature = "grpc", feature = "local")
))]
mod test_trace;

#[cfg(test)]
#[path = "lib/path_text_tests.rs"]
mod path_text_tests;
