//! Platform-default Workers client.
//!
//! Native targets verify the Rust-owned control identity over authenticated
//! gRPC before admitting an application operation. If that control transport
//! is absent or unavailable, the same endpoint is verified over bounded
//! Protobuf-JSON. Browser targets use that HTTP transport directly.

use crate::wire;

/// Maximum response size used by the platform HTTP fallback.
pub const DEFAULT_HTTP_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// The transport selected by [`connect`] after negotiation.
#[cfg(not(target_arch = "wasm32"))]
pub const DEFAULT_TRANSPORT: &str = "grpc";
/// The transport selected by [`connect`] in browser builds.
#[cfg(target_arch = "wasm32")]
pub const DEFAULT_TRANSPORT: &str = "http";

/// Client setup, transport, or canonical service failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid endpoint, credential, or negotiation configuration.
    #[error("Workers client configuration failed: {0}")]
    Configuration(String),
    /// Transport setup or network failure.
    #[error("Workers transport failure: {0}")]
    Transport(String),
    /// The service rejected an operation or handshake.
    #[error("Workers service failure")]
    Service {
        /// gRPC status code when the rejection came from gRPC.
        grpc_code: Option<i32>,
        /// HTTP status when the rejection came from HTTP.
        http_status: Option<u16>,
        /// Canonical semantic error detail, when present.
        detail: Option<wire::Error>,
    },
    /// A configured response bound was exceeded.
    #[error("Workers response exceeds configured bound")]
    ResponseTooLarge,
    /// The service response did not match the Rust-owned descriptor.
    #[error("malformed Workers response")]
    MalformedResponse,
}

/// Error returned while creating the platform-default client.
pub type ConnectError = Error;

impl From<crate::http::Error> for Error {
    fn from(error: crate::http::Error) -> Self {
        match error {
            crate::http::Error::InvalidArgument => {
                Self::Configuration("invalid endpoint or credential".into())
            }
            crate::http::Error::Transport(error) => Self::Transport(error.to_string()),
            crate::http::Error::ResponseTooLarge => Self::ResponseTooLarge,
            crate::http::Error::MalformedResponse => Self::MalformedResponse,
            crate::http::Error::Service { status, detail } => Self::Service {
                grpc_code: None,
                http_status: Some(status),
                detail,
            },
        }
    }
}

impl Error {
    fn from_http(error: crate::http::Error) -> Self {
        error.into()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Error {
    fn from_grpc(status: tonic::Status) -> Self {
        Self::Service {
            grpc_code: Some(status.code() as i32),
            http_status: None,
            detail: crate::grpc::error_detail(&status),
        }
    }

    fn from_grpc_connect(error: crate::grpc::ConnectError) -> Self {
        match error {
            crate::grpc::ConnectError::RemoteStatus(status) => Self::Configuration(format!(
                "Workers control handshake rejected: {}",
                status.message()
            )),
            crate::grpc::ConnectError::Negotiation(error) => Self::Configuration(error),
            crate::grpc::ConnectError::InsecureEndpoint => {
                Self::Configuration("Workers endpoints must use https".into())
            }
            crate::grpc::ConnectError::InvalidCredential => {
                Self::Configuration("invalid Workers bearer credential".into())
            }
            crate::grpc::ConnectError::InvalidCaCertificate => {
                Self::Configuration("invalid Workers private CA certificate".into())
            }
            crate::grpc::ConnectError::Transport(error) => Self::Transport(error.to_string()),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
enum Backend {
    Grpc(crate::grpc::Client),
    Http(crate::http::Client),
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
enum Backend {
    Http(crate::http::Client),
}

/// Authenticated Workers client selecting the best available transport.
#[derive(Clone)]
pub struct Client {
    inner: Backend,
}

impl Client {
    /// Return the transport selected during connection setup.
    #[must_use]
    pub const fn transport(&self) -> &'static str {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(_) => "grpc",
            Backend::Http(_) => "http",
        }
    }
}

/// Connect to a Workers service using the best compatible platform transport.
pub async fn connect(
    endpoint: impl AsRef<str>,
    token: impl AsRef<str>,
) -> Result<Client, ConnectError> {
    connect_with_trust(endpoint.as_ref(), token.as_ref(), None).await
}

/// Connect with a caller-pinned private CA certificate.
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
        match crate::grpc::connect_verified(endpoint, token, ca).await {
            Ok(Some(inner)) => return Ok(Client { inner: Backend::Grpc(inner) }),
            Ok(None) | Err(crate::grpc::ConnectError::Transport(_)) => {}
            Err(error) => return Err(Error::from_grpc_connect(error)),
        }
    }

    let inner = crate::http::Client::new(endpoint, token, DEFAULT_HTTP_RESPONSE_BYTES)?;
    if !inner.verify_handshake().await? {
        return Err(Error::Configuration(
            "endpoint has no compatible Workers transport".into(),
        ));
    }
    Ok(Client { inner: Backend::Http(inner) })
}

include!("generated/platform-client-methods.rs");

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "client_tests.rs"]
mod tests;

#[cfg(test)]
mod client_unit_tests {
    use super::{DEFAULT_HTTP_RESPONSE_BYTES, DEFAULT_TRANSPORT};

    #[test]
    fn default_transport_is_platform_owned() {
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(DEFAULT_TRANSPORT, "grpc");
        #[cfg(target_arch = "wasm32")]
        assert_eq!(DEFAULT_TRANSPORT, "http");
        assert!(DEFAULT_HTTP_RESPONSE_BYTES > 0);
    }
}
