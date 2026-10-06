//! One transport-neutral Objects client for native and browser consumers.

use crate::v2::{Download, Error, Object, ObjectsProvider, UploadBody, wire};
use futures::StreamExt;

/// Maximum response size used by the platform HTTP fallback.
pub const DEFAULT_HTTP_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Preferred transport before endpoint negotiation.
#[cfg(not(target_arch = "wasm32"))]
pub const DEFAULT_TRANSPORT: &str = "grpc";
/// Preferred transport in browser builds.
#[cfg(target_arch = "wasm32")]
pub const DEFAULT_TRANSPORT: &str = "http";

/// Configuration or endpoint negotiation failure.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The native transport could not be configured or verified.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("gRPC connection failed: {0}")]
    Grpc(#[from] crate::v2::grpc::ConnectError),
    /// The HTTP transport could not be configured or verified.
    #[error("HTTP connection failed: {0}")]
    Http(#[from] crate::v2::Error),
    /// The endpoint did not advertise Objects.
    #[error("endpoint has no compatible Objects transport")]
    IncompatibleEndpoint,
}

/// Authenticated Objects client selecting the best available transport.
#[derive(Clone)]
pub struct Client {
    inner: Backend,
}

#[derive(Clone)]
enum Backend {
    #[cfg(not(target_arch = "wasm32"))]
    Grpc(crate::v2::grpc::GrpcObjects),
    Http(crate::v2::http::HttpObjects),
}

impl Client {
    /// The transport selected after a verified endpoint handshake.
    #[must_use]
    pub const fn transport(&self) -> &'static str {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(_) => "grpc",
            Backend::Http(_) => "http",
        }
    }

    /// Stream a complete object upload through the selected transport.
    pub async fn put_stream(
        &self,
        header: wire::PutObjectHeader,
        body: UploadBody,
    ) -> Result<wire::ObjectInfo, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.put_stream(header, body).await,
            Backend::Http(client) => client.put_stream(header, body).await,
        }
    }

    /// Stream one staged multipart part through the selected transport.
    pub async fn upload_part_stream(
        &self,
        header: wire::UploadPartHeader,
        body: UploadBody,
    ) -> Result<wire::UploadedPart, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.upload_part_stream(header, body).await,
            Backend::Http(client) => client.upload_part_stream(header, body).await,
        }
    }

    /// Start a bounded streamed download through the selected transport.
    pub async fn get_stream(
        &self,
        request: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Download, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.get_stream(request, maximum_bytes).await,
            Backend::Http(client) => client.get_stream(request, maximum_bytes).await,
        }
    }
}

/// Connect using the best compatible transport and verify the Rust-owned identity first.
pub async fn connect(
    endpoint: impl AsRef<str>,
    token: impl AsRef<str>,
) -> Result<Client, ConnectError> {
    connect_with_trust(endpoint.as_ref(), token.as_ref(), None).await
}

/// Connect with an optional connection-scoped private CA certificate.
pub async fn connect_with_ca_certificate(
    endpoint: impl AsRef<str>,
    token: impl AsRef<str>,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    connect_with_trust(endpoint.as_ref(), token.as_ref(), ca).await
}

async fn connect_with_trust(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    #[cfg(not(target_arch = "wasm32"))]
    if endpoint.starts_with("https://") {
        match crate::v2::grpc::GrpcObjects::connect_verified_with_ca_certificate(
            endpoint, token, ca,
        )
        .await
        {
            Ok(Some(inner)) => {
                return Ok(Client {
                    inner: Backend::Grpc(inner),
                });
            }
            Ok(None) | Err(crate::v2::grpc::ConnectError::Transport(_)) => {}
            Err(error) => return Err(ConnectError::Grpc(error)),
        }
    }
    let inner = crate::v2::http::HttpObjects::with_ca_certificate(
        endpoint,
        token,
        DEFAULT_HTTP_RESPONSE_BYTES,
        ca,
    )?;
    if !inner.verify_handshake().await? {
        return Err(ConnectError::IncompatibleEndpoint);
    }
    Ok(Client {
        inner: Backend::Http(inner),
    })
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl ObjectsProvider for Client {
    async fn create_bucket(
        &self,
        request: wire::CreateBucketRequest,
    ) -> Result<wire::Bucket, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.create_bucket(request).await,
            Backend::Http(client) => client.create_bucket(request).await,
        }
    }
    async fn head_bucket(&self, request: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.head_bucket(request).await,
            Backend::Http(client) => client.head_bucket(request).await,
        }
    }
    async fn delete_bucket(
        &self,
        request: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.delete_bucket(request).await,
            Backend::Http(client) => client.delete_bucket(request).await,
        }
    }
    async fn put(
        &self,
        header: wire::PutObjectHeader,
        body: bytes::Bytes,
    ) -> Result<wire::ObjectInfo, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        let body = futures::stream::once(async move { Ok(body) }).boxed();
        #[cfg(target_arch = "wasm32")]
        let body = futures::stream::once(async move { Ok(body) }).boxed_local();
        self.put_stream(header, body).await
    }
    async fn get(
        &self,
        request: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        self.get_stream(request, maximum_bytes)
            .await?
            .collect(maximum_bytes)
            .await
    }
    async fn head(
        &self,
        request: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.head(request).await,
            Backend::Http(client) => client.head(request).await,
        }
    }
    async fn delete(
        &self,
        request: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.delete(request).await,
            Backend::Http(client) => client.delete(request).await,
        }
    }
    async fn list(
        &self,
        request: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.list(request).await,
            Backend::Http(client) => client.list(request).await,
        }
    }
    async fn create_multipart(
        &self,
        request: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.create_multipart(request).await,
            Backend::Http(client) => client.create_multipart(request).await,
        }
    }
    async fn upload_part(
        &self,
        header: wire::UploadPartHeader,
        body: bytes::Bytes,
    ) -> Result<wire::UploadedPart, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        let body = futures::stream::once(async move { Ok(body) }).boxed();
        #[cfg(target_arch = "wasm32")]
        let body = futures::stream::once(async move { Ok(body) }).boxed_local();
        self.upload_part_stream(header, body).await
    }
    async fn list_parts(
        &self,
        request: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.list_parts(request).await,
            Backend::Http(client) => client.list_parts(request).await,
        }
    }
    async fn complete_multipart(
        &self,
        request: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.complete_multipart(request).await,
            Backend::Http(client) => client.complete_multipart(request).await,
        }
    }
    async fn abort_multipart(
        &self,
        request: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.abort_multipart(request).await,
            Backend::Http(client) => client.abort_multipart(request).await,
        }
    }
}
