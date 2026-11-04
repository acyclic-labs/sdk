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
pub const MAX_BEARER_TOKEN_BYTES: usize = 12 * 1024;

/// Semantic service failure or the original local operation stop.
///
/// Its `source` is the local stop or, for a remote failure, the server's
/// [`Cause`](acyclic_auth::refusal::Cause).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    /// Public semantic category, suitable for programmatic handling.
    pub code: wire::ErrorCode,
    /// Original local stop, when the caller's operation budget ended. Neither
    /// cancellation nor expiry proves that a remote mutation did not commit.
    pub stopped: Option<acyclic_auth::operation::Stopped>,
    /// What the server said beyond `code`: its cause, and the member's
    /// original status when that said more than `code`'s canonical status.
    /// A diagnostic only; recovery follows `code`.
    pub refusal: Option<acyclic_auth::refusal::Refusal>,
}
impl Error {
    /// A failure of `code` the server attributes to `cause`.
    #[must_use]
    pub fn caused(code: wire::ErrorCode, cause: acyclic_auth::refusal::Cause) -> Self {
        Self {
            code,
            stopped: None,
            refusal: Some(acyclic_auth::refusal::Refusal::caused(cause)),
        }
    }
    /// The failure a transport detail reports, unless it names no canonical
    /// semantic code.
    #[must_use]
    pub fn from_detail(detail: wire::ErrorDetail) -> Option<Self> {
        let code = wire::ErrorCode::try_from(detail.code).ok()?;
        (code != wire::ErrorCode::Unspecified).then(|| Self {
            code,
            stopped: None,
            refusal: acyclic_auth::refusal::Cause::reported(
                detail.component,
                detail.reason,
                detail.request_id,
            )
            .map(acyclic_auth::refusal::Refusal::caused),
        })
    }
    /// The server's attribution, when it reported one.
    #[must_use]
    pub fn cause(&self) -> Option<&acyclic_auth::refusal::Cause> {
        self.refusal.as_ref()?.cause()
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Objects operation failed ({:?})", self.code)?;
        match &self.refusal {
            Some(refusal) => write!(formatter, "{refusal}"),
            None => Ok(()),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.stopped {
            Some(stopped) => Some(stopped),
            None => self
                .cause()
                .map(|cause| cause as &(dyn std::error::Error + 'static)),
        }
    }
}
impl From<wire::ErrorCode> for Error {
    fn from(code: wire::ErrorCode) -> Self {
        Self {
            code,
            stopped: None,
            refusal: None,
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
            refusal: None,
        }
    }
}
/// The one detail every transport carries for a failure: its code and cause.
impl From<Error> for wire::ErrorDetail {
    fn from(error: Error) -> Self {
        error_detail(
            error.code,
            error
                .refusal
                .and_then(acyclic_auth::refusal::Refusal::into_cause),
        )
    }
}

impl From<&Error> for wire::ErrorDetail {
    fn from(error: &Error) -> Self {
        error_detail(error.code, error.cause().cloned())
    }
}

fn error_detail(
    code: wire::ErrorCode,
    cause: Option<acyclic_auth::refusal::Cause>,
) -> wire::ErrorDetail {
    let cause = cause.unwrap_or_default();
    wire::ErrorDetail {
        code: code as i32,
        request_id: cause.request_id,
        component: cause.component,
        reason: cause.reason,
    }
}
