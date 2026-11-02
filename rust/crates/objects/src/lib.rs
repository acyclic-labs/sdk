#![doc = include_str!("../README.md")]
/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;

mod availability;
mod batch;
#[cfg(feature = "binary")]
pub mod binary;
mod blob;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use blob::codec_fuzz;
pub mod conformance;
#[cfg(any(objects_client, objects_server))]
mod convert;
#[cfg(feature = "conformance")]
mod counted;
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub mod grpc;
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub mod http;
#[cfg(feature = "json")]
pub mod json;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub mod local;
mod medium;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
mod native_error;
mod observation;
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
mod operation;
mod ownership;
mod provider;
#[cfg(objects_server)]
pub mod route;
#[cfg(all(feature = "s3", not(target_arch = "wasm32")))]
pub mod s3;
mod store;
#[cfg(test)]
mod tests;

pub use acyclic_stream::SealedBytes as Sealed;
pub use availability::{Availability, AvailabilityCursor, AvailabilitySource};
pub use blob::{ByteRange, Digest, Kind, Manifest, Part};
pub use medium::{Medium, MediumError, MemoryMedium};
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

#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub use grpc::{ConnectError, GrpcObjects};
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub use http::HttpObjects;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
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

/// Canonical HTTP route, input message, and output message inventory, relative to `/v1/objects/`.
pub const HTTP_ROUTES: &[(&str, &str, &str)] = &[
    ("blobs/put", "PutBlobRequest", "PutResponse"),
    ("blobs/put-batch", "PutBlobsRequest", "PutBlobsResponse"),
    ("blobs/put-manifest", "PutManifestRequest", "PutResponse"),
    ("blobs/get", "GetBlobRequest", "GetBlobResponse"),
    ("blobs/head", "HeadBlobRequest", "BlobInfo"),
    ("blobs/delete", "DeleteBlobRequest", "DeleteBlobResponse"),
    ("blobs/list", "ListBlobsRequest", "ListBlobsResponse"),
    ("blobs/retain", "RetainRequest", "RetainResponse"),
    ("blobs/release", "ReleaseRequest", "ReleaseResponse"),
    ("blobs/collect", "CollectRequest", "CollectResponse"),
    ("blobs/usage", "UsageRequest", "UsageResponse"),
];

/// Streaming routes, with one canonical Protobuf length-delimited message per
/// frame. The first frame opens the observation before verification starts.
pub const HTTP_STREAM_ROUTES: &[(&str, &str, &str)] = &[(
    "blobs/availability",
    "AvailabilityRequest",
    "AvailabilityCursor",
)];

/// Largest caller-supplied PEM CA, client certificate, or private key.
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
const MAX_PEM_BYTES: usize = 64 * 1024;
/// Largest bearer credential, in bytes, that an SDK client accepts.
pub use acyclic_auth::credential::MAX_CREDENTIAL_BYTES as MAX_BEARER_TOKEN_BYTES;

/// Semantic service failure or the original local operation stop.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("Objects operation failed ({code:?})")]
pub struct Error {
    /// Public semantic category, suitable for programmatic handling.
    pub code: wire::ErrorCode,
    /// Original local stop, when the caller's operation budget ended. Neither
    /// cancellation nor expiry proves that a remote mutation did not commit.
    #[source]
    pub stopped: Option<acyclic_auth::operation::Stopped>,
}

impl Error {
    /// Classifies a canonical gRPC status when no typed service detail exists.
    /// Unknown statuses remain unavailable; this does not decide a write's outcome.
    #[must_use]
    pub fn from_grpc_status(status: i32) -> Self {
        match status {
            3 => wire::ErrorCode::InvalidArgument,
            5 => wire::ErrorCode::NotFound,
            7 | 16 => wire::ErrorCode::AccessDenied,
            8 => wire::ErrorCode::QuotaExceeded,
            11 => wire::ErrorCode::RangeNotSatisfiable,
            12 => wire::ErrorCode::Unsupported,
            15 => wire::ErrorCode::Corrupt,
            4 => wire::ErrorCode::DeadlineElapsed,
            _ => wire::ErrorCode::Unavailable,
        }
        .into()
    }

    /// Classifies an HTTP status when no typed service detail exists.
    /// Unknown statuses remain unavailable; this does not decide a write's outcome.
    #[must_use]
    pub fn from_http_status(status: u16) -> Self {
        match status {
            400 => wire::ErrorCode::InvalidArgument,
            401 | 403 => wire::ErrorCode::AccessDenied,
            404 => wire::ErrorCode::NotFound,
            409 => wire::ErrorCode::Conflict,
            413 | 429 => wire::ErrorCode::QuotaExceeded,
            416 => wire::ErrorCode::RangeNotSatisfiable,
            501 => wire::ErrorCode::Unsupported,
            _ => wire::ErrorCode::Unavailable,
        }
        .into()
    }
}

impl From<wire::ErrorCode> for Error {
    fn from(code: wire::ErrorCode) -> Self {
        Self {
            code,
            stopped: None,
        }
    }
}
impl From<acyclic_auth::operation::Stopped> for Error {
    fn from(stopped: acyclic_auth::operation::Stopped) -> Self {
        Self {
            code: match stopped {
                acyclic_auth::operation::Stopped::Deadline => wire::ErrorCode::DeadlineElapsed,
                acyclic_auth::operation::Stopped::Cancelled => wire::ErrorCode::OutcomeUnknown,
            },
            stopped: Some(stopped),
        }
    }
}
