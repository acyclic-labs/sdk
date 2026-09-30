//! Logical bucket/key Objects contract replacing public version history and snapshots.
//!
//! Reads and listings may lag mutations. Single-object publication and its conditions are
//! atomic. Service-owned retained bytes are a private service contract, not public history.
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub mod http;
#[cfg(feature = "json")]
pub mod json;
mod memory;
pub mod request;
pub mod response;
#[cfg(any(feature = "grpc", all(feature = "http", not(target_arch = "wasm32"))))]
mod upload;

/// Canonical HTTP route, input message, and output message inventory, relative to `/v2/objects/`.
/// Streaming routes carry bounded NDJSON frames; see the Objects v2 HTTP contract document.
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
#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(all(test, feature = "grpc"))]
mod grpc_tests;
#[cfg(all(test, feature = "http", not(target_arch = "wasm32")))]
mod http_tests;
#[cfg(test)]
mod tests;

/// Generated Objects v2 public request, response and transport bindings.
#[allow(missing_docs, clippy::all, clippy::pedantic, clippy::too_many_lines)]
pub mod wire {
    include!("../generated/acyclic.objects.v2.rs");
}

/// Canonical Objects v2 descriptor, including streaming service definitions.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../generated/acyclic-objects-v2.bin");

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

/// One buffered selection for callers that explicitly bound allocation.
/// Streaming transports also expose framed upload/download APIs.
#[derive(Clone, Debug, PartialEq)]
pub struct Object {
    /// Representation metadata and optional selected byte range.
    pub header: wire::GetObjectHeader,
    /// Complete selected bytes, with no torn representation.
    pub body: bytes::Bytes,
}

/// A validated header followed by bounded decoded chunks. Dropping it cancels the read.
pub struct Download {
    /// Metadata for the complete selected representation.
    pub header: wire::GetObjectHeader,
    /// Decoded body chunks; a truncated body or terminal service error is an error item.
    pub body: futures::stream::BoxStream<'static, Result<bytes::Bytes, Error>>,
}
/// Caller-owned upload chunks. A source failure aborts publication, preserving its category.
pub type UploadBody = futures::stream::BoxStream<'static, Result<bytes::Bytes, Error>>;
impl Download {
    /// Collects chunks within an explicit allocation bound, preserving terminal failures.
    pub async fn collect(self, maximum_bytes: u64) -> Result<Object, Error> {
        use futures::StreamExt;
        let mut body = bytes::BytesMut::new();
        let mut stream = self.body;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let size = (body.len() as u64)
                .checked_add(chunk.len() as u64)
                .ok_or(Error::from(wire::ErrorCode::QuotaExceeded))?;
            if size > maximum_bytes || usize::try_from(size).is_err() {
                return Err(wire::ErrorCode::QuotaExceeded.into());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Object {
            header: self.header,
            body: body.freeze(),
        })
    }
}

/// Transport-independent logical Objects interface.
#[async_trait::async_trait]
pub trait ObjectsProvider: Send + Sync {
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
        body: bytes::Bytes,
    ) -> Result<wire::ObjectInfo, Error>;
    /// Reads one complete selected representation within the caller's allocation bound.
    async fn get(
        &self,
        request: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error>;
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
        body: bytes::Bytes,
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
