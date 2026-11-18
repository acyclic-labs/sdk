#![doc = include_str!("../README.md")]
#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;

/// Compiler-linked public method admission.
pub mod authorization {
    use acyclic_auth::action as auth_action;
    include!(concat!(env!("OUT_DIR"), "/authorization.rs"));
}
pub use authorization::{AUTHORIZATION, ObjectsMethod};
mod availability;
pub mod batch;
pub mod binary;
mod blob;
mod error;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use blob::codec_fuzz;
pub mod conformance;
mod convert;
#[cfg(feature = "conformance")]
mod counted;
#[cfg(not(target_arch = "wasm32"))]
pub mod grpc;
#[cfg(not(target_arch = "wasm32"))]
pub mod http;
mod identity;
pub mod json;
#[cfg(not(target_arch = "wasm32"))]
pub mod local;
mod manifest_builder;
mod medium;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
mod native_error;
mod observation;
#[cfg(not(target_arch = "wasm32"))]
mod operation;
mod ownership;
mod provider;
pub mod route;
mod route_inventory;
#[cfg(not(target_arch = "wasm32"))]
pub mod s3;
mod store;
#[cfg(test)]
mod tests;

pub use acyclic_stream::SealedBytes as Sealed;
pub use availability::{Availability, AvailabilityCursor, AvailabilitySource};
pub use blob::{ByteRange, ContentDigests, ContentHasher, Digest, Kind, Manifest, Part};
pub use error::Error;
pub use manifest_builder::ManifestBuilder;
#[cfg(any(test, feature = "test-support"))]
pub use medium::{Fault, Injected};
pub use medium::{Medium, MediumError, MediumKey, MemoryMedium};
#[cfg(not(target_arch = "wasm32"))]
pub use native_error::{NativeObjectsCause, NativeObjectsError};
pub use observation::{Observation, ObservationSource, Observed, ObservedVersion};
pub use ownership::Guarded;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use ownership::native_input_meter;
#[cfg(not(target_arch = "wasm32"))]
pub use ownership::{
    ObjectsBatch, ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt, native_future,
};
pub use provider::{
    BATCH_WINDOW, Blobs, Collection, Delete, Deletion, Head, Object, Page, Put, Released, Retained,
    RootId, Usage, Verified,
};
pub use store::{Account, MemoryObjects, Store};

#[cfg(not(target_arch = "wasm32"))]
pub use grpc::{ConnectError, GrpcObjects};
#[cfg(not(target_arch = "wasm32"))]
pub use http::HttpObjects;
#[cfg(not(target_arch = "wasm32"))]
pub use local::{LocalClose, LocalCloseError, LocalMedium, LocalObjects};

/// Largest blob stored inline in a retained stream record instead of as a blob.
///
/// Contents at or below this size belong in the record that references them:
/// they inherit the stream's replication, create no blob reference and are
/// never collected separately. Larger contents are blobs or manifests.
pub const INLINE_THRESHOLD: usize = 64 * 1024;

/// Generated Objects v1 public request, response and transport bindings.
#[allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "generated prost and tonic bindings"
)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/acyclic.objects.v1.rs"));
}

/// Canonical Objects v1 descriptor, including service definitions.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-objects-v1.bin"));

pub use route::{HTTP_ROUTES, HTTP_STREAM_ROUTES};
