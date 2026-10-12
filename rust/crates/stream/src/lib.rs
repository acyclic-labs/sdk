#![doc = include_str!("../README.md")]

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use thiserror::Error;

pub mod conformance;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod http_response;
pub mod persistence;
mod preparation;
pub mod request;
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub mod http;
// Request projection is shared by the native HTTP client and the browser adapter.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod browser;
#[cfg(any(
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
mod http_codec;
mod http_validation;
#[cfg(any(feature = "local", all(feature = "wasm", target_arch = "wasm32")))]
mod journal;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod local;
mod memory;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "grpc", feature = "http", feature = "local")
))]
mod obs;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use browser::{BrowserStream, BrowserStreamLimits};
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
/// Conversion helpers between the canonical Rust domain types and generated wire messages.
pub mod wire_codec;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test_meter;
mod owned_bytes;
pub use owned_bytes::OwnedBytes;
#[cfg(not(target_arch = "wasm32"))]
pub use owned_bytes::{PreparedOwnedBytes, PreparedOwnedBytesError, PreparedOwnedBytesFailure};

/// Largest bearer credential, in bytes, that an SDK client accepts.
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
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../proto/stream/v1/stream_descriptor.bin");
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use local::{
    LocalDurability, LocalStream, LocalStreamError, LocalStreamLimits, deferring_durability,
};
pub use memory::{MemoryLimits, MemoryStream};

/// Maximum opaque record body.
pub const MAX_RECORD_BYTES: usize = wire::StreamLimit::MaxRecordBytes as usize;
/// Maximum records, participants, mutations, or path segments in one request.
pub const MAX_ITEMS: usize = wire::StreamLimit::MaxItems as usize;
const REPLAY_PAGE: u32 = 1_024;
const _: () = assert!(REPLAY_PAGE as usize == MAX_ITEMS);
/// Maximum canonical application command, including metadata.
pub const MAX_COMMAND_BYTES: usize = wire::StreamLimit::MaxCommandBytes as usize;
/// Maximum caller retry-identity width.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = wire::StreamLimit::MaxIdempotencyKeyBytes as usize;
/// Maximum canonical path text accepted by the single-command wire format.
pub const MAX_PATH_BYTES: usize = wire::StreamLimit::MaxPathBytes as usize;

/// Route-to-response families for the hosted HTTP projection.
pub const HTTP_RESPONSE_CONTRACT: &[(&str, &str)] = &[
    ("idempotency/inspect", "observation"),
    ("delivery/acknowledge", "empty"),
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

mod path;
mod domain;
mod provider;
mod stream;
mod error;
pub use path::StreamPath;
pub use domain::*;
pub use provider::*;
pub use stream::{Stream, Replay};
pub use error::StreamError;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::{NativeStreamOutput, NativeStreamResult, NativeRecordStream, NativeProviderFuture, PreparedNativeCommit};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::{NativeCommitRecordSlot, capture_commit};
#[cfg(test)]
mod token_operation_tests {
    use super::TOKEN_OPERATIONS;

    #[test]
    fn inventory_is_ordered_and_unique() {
        assert_eq!(
            TOKEN_OPERATIONS,
            &[
                "list", "read", "follow", "append", "fork", "create", "commit",
            ]
        );
        let mut sorted = TOKEN_OPERATIONS.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), TOKEN_OPERATIONS.len());
    }
}
