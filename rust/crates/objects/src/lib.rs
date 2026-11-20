#![doc = include_str!("../README.md")]
#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;

pub use acyclic_objects_core::authorization;
pub use acyclic_objects_core::authorization::{AUTHORIZATION, ObjectsMethod};
pub use acyclic_objects_core::batch;
pub use acyclic_objects_core::binary;
pub(crate) use acyclic_objects_core::blob;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod codec_fuzz;
pub mod conformance;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use acyclic_objects_core::convert;
pub(crate) use acyclic_objects_core::error;
pub(crate) use acyclic_objects_core::identity;
pub use acyclic_objects_core::json;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use acyclic_objects_core::medium;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use acyclic_objects_core::native_input_meter;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use acyclic_objects_core::operation;
#[cfg(not(target_arch = "wasm32"))]
mod native;
pub(crate) use acyclic_objects_core::availability;
pub(crate) use acyclic_objects_core::provider;
pub use acyclic_objects_core::route;
pub use acyclic_objects_core::wire;
#[cfg(not(target_arch = "wasm32"))]
pub mod grpc;
#[cfg(not(target_arch = "wasm32"))]
pub mod http;
#[cfg(not(target_arch = "wasm32"))]
pub mod local;
#[cfg(not(target_arch = "wasm32"))]
pub mod s3;
mod store;
#[cfg(test)]
mod tests;

pub use acyclic_objects_core::{
    Availability, AvailabilityCursor, AvailabilitySource, BATCH_WINDOW, Blobs, ByteRange,
    Collection, ContentDigests, ContentHasher, Delete, Deletion, Digest, Error, Guarded, Head,
    Kind, Manifest, ManifestBuilder, Medium, MediumError, MediumKey, MemoryMedium, Object,
    Observation, ObservationSource, Observed, ObservedVersion, Page, Part, Put, ReadLimitExceeded,
    Released, Retained, RootId, Sealed, Usage, Verified,
};
#[cfg(any(test, feature = "test-support"))]
pub use acyclic_objects_core::{Fault, Injected};
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_objects_core::{
    NativeObjectsCause, NativeObjectsError, ObjectsBatch, ObjectsNativeFuture,
    ObjectsNativeRequest, ObjectsPutReceipt, native_future,
};
pub use store::{Account, MemoryObjects, Store};

/// Largest blob stored inline in a retained stream record instead of as a blob.
///
/// Contents at or below this size belong in the record that references them:
/// they inherit the stream's replication, create no blob reference and are
/// never collected separately. Larger contents are blobs or manifests.
pub const INLINE_THRESHOLD: usize = 64 * 1024;

#[cfg(not(target_arch = "wasm32"))]
pub use grpc::{ConnectError, GrpcObjects};
#[cfg(not(target_arch = "wasm32"))]
pub use http::HttpObjects;
#[cfg(not(target_arch = "wasm32"))]
pub use local::{LocalClose, LocalCloseError, LocalMedium, LocalObjects};

pub use acyclic_objects_core::{FILE_DESCRIPTOR_SET, HTTP_ROUTES, HTTP_STREAM_ROUTES};
