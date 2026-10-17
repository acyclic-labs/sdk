#![doc = include_str!("../README.md")]

use std::sync::Arc;

use crate::OwnedBytes as Bytes;
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use std::future::Future;
use thiserror::Error;

pub mod conformance;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod follow_tests;
#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod head_tests;
pub mod http_response;
pub mod persistence;
pub mod preparation;
pub mod request;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub mod session;
// The WASM adapter consumes this module on browser builds; native builds keep
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub mod http;
// Request projection is shared by the native HTTP client and the browser adapter.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod browser;
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), acyclic_stream_loom))]
mod change;
#[cfg(any(
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
mod http_codec;
mod http_validation;
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), acyclic_stream_loom))]
mod interest;
#[cfg(any(feature = "local", all(feature = "wasm", target_arch = "wasm32")))]
mod journal;
#[cfg(feature = "local")]
mod local;
mod memory;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "grpc", feature = "http", feature = "local")
))]
mod obs;
#[doc(hidden)]
pub mod query_sync;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use browser::{BrowserStream, BrowserStreamLimits};
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::WasmStream;
/// Conversion helpers between the canonical Rust domain types and generated wire messages.
pub mod wire_codec;

#[cfg(all(acyclic_loom, feature = "local"))]
#[path = "memory/recovery_model.rs"]
/// Bounded schedule verification of production query concurrency paths.
pub mod verification;
#[cfg(all(acyclic_loom, feature = "local"))]
#[doc(hidden)]
pub use local::replay_model::{
    negative_control_acknowledges_an_unflushed_original, replay_and_flush_preserve_durability,
};
#[cfg(all(acyclic_loom, acyclic_stream_loom, feature = "local"))]
#[doc(hidden)]
pub use memory::browser_follow_model as browser_follow_verification;
#[cfg(all(acyclic_loom, feature = "local"))]
/// Bounded models of the real child-discovery providers.
pub use memory::discovery_model as discovery_verification;

/// Largest bearer credential, in bytes, that an SDK client accepts. It matches the
pub const MAX_BEARER_TOKEN_BYTES: usize = 12 * 1024;

/// Maximum caller-supplied private CA bundle bytes accepted by the HTTP and gRPC clients.
pub const MAX_CA_CERTIFICATE_BYTES: usize = 64 * 1024;

/// Generated canonical Stream v1 protocol.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "generated prost and tonic bindings"
)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/acyclic.stream.v1.rs"));
}
/// Canonical public descriptor set used by compatibility gates.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-stream-v1.bin"));
#[cfg(feature = "local")]
pub use local::{
    LocalDurability, LocalStream, LocalStreamError, LocalStreamLimits, deferring_durability,
};
#[cfg(all(feature = "local", feature = "test-support"))]
pub use local::{CutSimulationError, PersistenceCut, fail_cut_truncation};
pub use memory::{MemoryLimits, MemoryStream};

/// Maximum opaque record body.
pub const MAX_RECORD_BYTES: usize = wire::StreamLimit::MaxRecordBytes as usize;
/// Maximum records, participants, mutations, or path segments in one request.
pub const MAX_ITEMS: usize = wire::StreamLimit::MaxItems as usize;
const REPLAY_PAGE: u32 = 1_024;
const _: () = assert!(REPLAY_PAGE as usize == MAX_ITEMS);
/// Maximum canonical application command, including metadata.
pub const MAX_COMMAND_BYTES: usize = wire::StreamLimit::MaxCommandBytes as usize;
/// Historical minimum replay window, preserved for source compatibility.
pub const MIN_IDEMPOTENCY_RETENTION_SECS: u64 = 24 * 60 * 60;
/// Maximum caller retry-identity width.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = wire::StreamLimit::MaxIdempotencyKeyBytes as usize;
/// Maximum canonical path text accepted by the single-command wire format.
pub const MAX_PATH_BYTES: usize = wire::StreamLimit::MaxPathBytes as usize;

/// Route-to-response families for the hosted HTTP projection.
pub const HTTP_RESPONSE_CONTRACT: &[(&str, &str)] = &[
    ("delivery/acknowledge", "empty"),
    ("idempotency/inspect", "observation"),
    ("tail", "sequence"),
    ("append", "append"),
    ("fork", "fork"),
    ("read", "records"),
    ("children", "children"),
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

include!("lib/domain.rs");

include!("lib/provider.rs");

include!("lib/stream.rs");

include!("lib/error.rs");

include!("lib/tests.rs");

mod owned_bytes;
pub use owned_bytes::OwnedBytes;
#[cfg(not(target_arch = "wasm32"))]
pub use owned_bytes::{PreparedOwnedBytes, PreparedOwnedBytesError, PreparedOwnedBytesFailure};
#[cfg(not(target_arch = "wasm32"))]
mod native;

#[cfg(not(target_arch = "wasm32"))]
pub use native::{
    NativeProviderFuture, NativeRecordStream, NativeStreamOutput, NativeStreamResult,
    PreparedNativeCommit,
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
