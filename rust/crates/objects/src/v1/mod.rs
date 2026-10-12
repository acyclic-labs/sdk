//! Logical bucket/key Objects contract replacing public version history and snapshots.
pub mod conformance;
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub mod http;
#[cfg(feature = "json")]
pub mod json;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub mod local;
mod memory;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) use memory::persistence::Publication;
pub mod request;
pub mod response;
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
mod upload;

/// Canonical HTTP route, input message, and output message inventory, relative to `/v1/objects/`.
pub const HTTP_ROUTES: &[(&str, &str, &str)] = &[
    ("buckets/create", "CreateBucketRequest", "Bucket"),
    ("buckets/head", "HeadBucketRequest", "Bucket"),
    (
        "buckets/delete",
        "DeleteBucketRequest",
        "DeleteBucketResponse",
    ),
    ("objects/put", "PutObjectRequest", "ObjectInfo"),
    ("objects/get", "GetObjectRequest", "GetObjectResponse"),
    ("objects/head", "HeadObjectRequest", "HeadObjectResponse"),
    (
        "objects/delete",
        "DeleteObjectRequest",
        "DeleteObjectResponse",
    ),
    ("objects/list", "ListObjectsRequest", "ListObjectsResponse"),
    (
        "multipart/create",
        "CreateMultipartRequest",
        "MultipartUpload",
    ),
    ("multipart/upload-part", "UploadPartRequest", "UploadedPart"),
    (
        "multipart/list-parts",
        "ListPartsRequest",
        "ListPartsResponse",
    ),
    (
        "multipart/complete",
        "CompleteMultipartRequest",
        "ObjectInfo",
    ),
    (
        "multipart/abort",
        "AbortMultipartRequest",
        "AbortMultipartResponse",
    ),
];
pub use memory::{MemoryObjects, MemoryOptions};

#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub mod grpc;
#[cfg(all(test, feature = "grpc", not(target_arch = "wasm32")))]
mod grpc_tests;
#[cfg(all(test, feature = "http", not(target_arch = "wasm32")))]
mod http_tests;
#[cfg(test)]
mod tests;

/// Generated Objects v1 public request, response and transport bindings.
#[allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "generated prost and tonic bindings"
)]
pub mod wire {
    include!("../generated/acyclic.objects.v1.rs");
}

/// Largest caller-supplied PEM CA, client certificate, or private key.
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
const MAX_PEM_BYTES: usize = 64 * 1024;
/// Largest bearer credential, in bytes, that an SDK client accepts.
pub const MAX_BEARER_TOKEN_BYTES: usize = 12 * 1024;

/// Canonical Objects v1 descriptor, including streaming service definitions.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../generated/acyclic-objects-v1.bin");

/// Canonical semantic failure; infrastructure diagnostics remain operator-only.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("Objects operation failed ({code:?})")]
pub struct Error {
    /// Public semantic category, suitable for programmatic handling.
    pub code: wire::ErrorCode,
}
impl From<wire::ErrorCode> for Error {
    fn from(code: wire::ErrorCode) -> Self {
        Self { code }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod native_error;
mod object;
mod ownership;
#[cfg(not(target_arch = "wasm32"))]
pub use native_error::{NativeObjectsCause, NativeObjectsError, NativeObjectsSharedFailure};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native_error::{NativeTransportErrors, native_preparation_error};
pub use object::{BodyStream, Download, Object, UploadBody};
pub use ownership::Guarded;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use ownership::native_input_meter;
#[cfg(not(target_arch = "wasm32"))]
pub use ownership::{
    ObjectsBatch, ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt, native_future,
};

/// Transport-independent logical Objects interface.
#[async_trait::async_trait]
pub trait ObjectsProvider: Send + Sync {
    /// Publishes ordered, individually atomic mutations in one native admission.
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, acyclic_stream::OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        vec![Err(wire::ErrorCode::Unsupported.into()); requests.len()]
    }

    /// Reads ordered, individually bounded selections under one native admission.
    async fn get_batch(
        &self,
        requests: Vec<(wire::GetObjectRequest, u64)>,
    ) -> Vec<Result<Object, Error>> {
        requests
            .into_iter()
            .map(|_| Err(wire::ErrorCode::Unsupported.into()))
            .collect()
    }

    /// Reads a genuine batch using the caller's original allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        _requests: ObjectsNativeRequest<Vec<(wire::GetObjectRequest, u64)>>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError> {
        Err(NativeObjectsError::from(NativeObjectsCause::Unsupported))
    }

    /// Publishes a genuine batch with the current caller's original admissions.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        _requests: ObjectsNativeRequest<Vec<(wire::PutObjectHeader, acyclic_stream::OwnedBytes)>>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
        Err(NativeObjectsError::from(NativeObjectsCause::Unsupported))
    }

    /// Creates a tenant-scoped logical bucket.
    async fn create_bucket(
        &self,
        request: wire::CreateBucketRequest,
    ) -> Result<wire::Bucket, Error>;
    /// Inspects an existing logical bucket.
    async fn head_bucket(&self, request: wire::HeadBucketRequest) -> Result<wire::Bucket, Error>;
    /// Deletes an empty logical bucket.
    async fn delete_bucket(
        &self,
        request: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error>;
    /// Publishes one complete value and atomically evaluates its precondition.
    async fn put(
        &self,
        header: wire::PutObjectHeader,
        body: acyclic_stream::OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error>;

    /// Publishes through the same engine with the current original native meter.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        _request: ObjectsNativeRequest<(wire::PutObjectHeader, acyclic_stream::OwnedBytes)>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError> {
        Err(NativeObjectsError::from(NativeObjectsCause::Unsupported))
    }
    /// Reads one complete selected representation within the caller's allocation bound.
    async fn get(
        &self,
        request: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error>;

    /// Reads through the same provider using its original-meter body producer.
    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        _request: ObjectsNativeRequest<wire::GetObjectRequest>,
        _maximum_bytes: u64,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        Err(NativeObjectsError::from(NativeObjectsCause::Unsupported))
    }
    /// Reads metadata without allocating body bytes.
    async fn head(
        &self,
        request: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error>;
    /// Deletes the current logical value with an optional atomic precondition.
    async fn delete(
        &self,
        request: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error>;
    /// Traverses a live eventual listing in lexical key order.
    async fn list(
        &self,
        request: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error>;
    /// Starts independently staged multipart work.
    async fn create_multipart(
        &self,
        request: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error>;
    /// Replaces one staged part and returns its exact receipt.
    async fn upload_part(
        &self,
        header: wire::UploadPartHeader,
        body: acyclic_stream::OwnedBytes,
    ) -> Result<wire::UploadedPart, Error>;
    /// Paginates staged parts by part number.
    async fn list_parts(
        &self,
        request: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error>;
    /// Atomically publishes the selected parts and evaluates current-value conditions.
    async fn complete_multipart(
        &self,
        request: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error>;
    /// Aborts staged multipart work.
    async fn abort_multipart(
        &self,
        request: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error>;
}
