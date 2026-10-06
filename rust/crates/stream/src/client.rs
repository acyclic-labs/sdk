//! One authenticated hosted Stream API on every platform.
use crate::*;
use async_trait::async_trait;
use std::sync::Arc;

/// Preferred transport before authenticated endpoint negotiation.
#[cfg(not(target_arch = "wasm32"))]
pub const DEFAULT_TRANSPORT: &str = "grpc";
/// Browser preference before authenticated endpoint negotiation.
#[cfg(target_arch = "wasm32")]
pub const DEFAULT_TRANSPORT: &str = "http";
/// Default response bound for the HTTP provider.
///
/// This alias intentionally resolves to the crate-level Rust contract so the
/// native facade and WASM/generated adapters cannot drift to separate limits.
pub const DEFAULT_HTTP_RESPONSE_BYTES: usize = crate::DEFAULT_HTTP_RESPONSE_BYTES;

/// Configuration or authenticated transport negotiation failure.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Invalid endpoint or credential configuration.
    #[error("invalid Stream configuration: {0}")]
    Configuration(String),
    /// Failed non-mutating transport probe.
    #[error("Stream transport probe failed: {0}")]
    Transport(String),
    /// The endpoint rejected the authenticated handshake.
    #[error("Stream handshake rejected with HTTP status {0}")]
    HttpStatus(u16),
    /// The endpoint did not prove the required contract identity and capabilities.
    #[error("Stream contract negotiation failed: {0}")]
    Negotiation(String),
}

/// Provider selected internally before the first application operation.
#[derive(Clone)]
pub struct HostedProvider {
    inner: Backend,
}

#[derive(Clone)]
enum Backend {
    #[cfg(not(target_arch = "wasm32"))]
    Grpc(crate::grpc::Client),
    Http(crate::http::HttpStream),
}

/// Platform-independent hosted client, including streaming and recovery.
pub type Client = StreamClient<HostedProvider>;

impl Client {
    /// The transport selected by a verified non-mutating handshake.
    #[must_use]
    pub fn transport(&self) -> &'static str {
        match &self.provider.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(_) => "grpc",
            Backend::Http(_) => "http",
        }
    }
}

/// Connect using the best compatible transport without an application probe.
///
/// # Errors
/// Authentication and identity failures are terminal. An absent native gRPC
/// control service allows an authenticated HTTP handshake before selection.
pub async fn connect(
    endpoint: impl AsRef<str>,
    token: impl AsRef<str>,
) -> Result<Client, ConnectError> {
    connect_with_trust(endpoint.as_ref(), token.as_ref(), None).await
}

async fn connect_with_trust(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    crate::http_validation::validate_endpoint(endpoint)
        .map_err(|error| ConnectError::Configuration(error.into()))?;
    if token.trim().is_empty() {
        return Err(ConnectError::Configuration(
            "empty bearer credential".into(),
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    if endpoint.starts_with("https://") {
        match crate::grpc::Client::connect_verified(endpoint, token, ca).await {
            Ok(Some(inner)) => {
                return Ok(StreamClient::new(Arc::new(HostedProvider {
                    inner: Backend::Grpc(inner),
                })));
            }
            Ok(None) | Err(crate::grpc::ConnectError::Endpoint(_)) => {}
            Err(error) => return Err(ConnectError::Negotiation(error.to_string())),
        }
    }
    let inner = crate::http::HttpStream::with_ca_certificate(
        endpoint,
        token,
        DEFAULT_HTTP_RESPONSE_BYTES,
        ca,
    )
    .map_err(|error| ConnectError::Configuration(error.to_string()))?;
    if !inner.verify_handshake().await? {
        return Err(ConnectError::Negotiation(
            "endpoint has no compatible Stream transport".into(),
        ));
    }
    Ok(StreamClient::new(Arc::new(HostedProvider {
        inner: Backend::Http(inner),
    })))
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl StreamProvider for HostedProvider {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => {
                StreamProvider::inspect_idempotency(inner, idempotency_key).await
            }
            Backend::Http(inner) => {
                StreamProvider::inspect_idempotency(inner, idempotency_key).await
            }
        }
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::tail(inner, path).await,
            Backend::Http(inner) => StreamProvider::tail(inner, path).await,
        }
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::bounds(inner, path).await,
            Backend::Http(inner) => StreamProvider::bounds(inner, path).await,
        }
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::append(inner, request).await,
            Backend::Http(inner) => StreamProvider::append(inner, request).await,
        }
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::fork(inner, request).await,
            Backend::Http(inner) => StreamProvider::fork(inner, request).await,
        }
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::read(inner, request).await,
            Backend::Http(inner) => StreamProvider::read(inner, request).await,
        }
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::follow(inner, path, from).await,
            Backend::Http(inner) => StreamProvider::follow(inner, path, from).await,
        }
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::children(inner, request).await,
            Backend::Http(inner) => StreamProvider::children(inner, request).await,
        }
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::children_page(inner, request).await,
            Backend::Http(inner) => StreamProvider::children_page(inner, request).await,
        }
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::commit(inner, request).await,
            Backend::Http(inner) => StreamProvider::commit(inner, request).await,
        }
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => {
                StreamProvider::commit_before(inner, request, deadline_unix_millis).await
            }
            Backend::Http(inner) => {
                StreamProvider::commit_before(inner, request, deadline_unix_millis).await
            }
        }
    }
    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(inner) => StreamProvider::read_commit(inner, commit_id).await,
            Backend::Http(inner) => StreamProvider::read_commit(inner, commit_id).await,
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "client_tests.rs"]
mod tests;
