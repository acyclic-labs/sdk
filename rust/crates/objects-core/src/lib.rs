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
#[doc(hidden)]
pub mod availability;
pub mod batch;
pub mod binary;
#[doc(hidden)]
pub mod blob;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use blob::codec_fuzz;
#[doc(hidden)]
pub mod convert;
#[cfg(feature = "conformance")]
mod counted;
#[doc(hidden)]
pub mod error;
pub use error::grpc_status_code_typescript;
#[doc(hidden)]
pub mod identity;
pub mod json;
mod manifest_builder;
#[doc(hidden)]
pub mod medium;
#[cfg(not(target_arch = "wasm32"))]
mod native_error;
mod observation;
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub mod operation;
mod ownership;
#[doc(hidden)]
pub mod provider;
pub mod route;

pub use acyclic_stream_contract::SealedBytes as Sealed;
pub use availability::{Availability, AvailabilityCursor, AvailabilitySource};
pub use blob::{ByteRange, ContentDigests, ContentHasher, Digest, Kind, Manifest, Part};
pub use error::{Error, ReadLimitExceeded};
pub use manifest_builder::ManifestBuilder;
#[cfg(any(test, feature = "test-support"))]
pub use medium::{Fault, Injected};
pub use medium::{Medium, MediumError, MediumKey, MemoryMedium};
#[cfg(not(target_arch = "wasm32"))]
pub use native_error::{NativeObjectsCause, NativeObjectsError};
pub use observation::{Observation, ObservationSource, Observed, ObservedVersion};
pub use ownership::Guarded;
#[cfg(not(target_arch = "wasm32"))]
pub use ownership::native_input_meter;
#[cfg(not(target_arch = "wasm32"))]
pub use ownership::{
    ObjectsBatch, ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt, native_future,
};
pub use provider::{
    BATCH_WINDOW, Blobs, Collection, Delete, Deletion, Head, Object, Page, Put, Released, Retained,
    RootId, Usage, Verified,
};

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
