//! Authenticated client with one API across native and browser platforms.

/// Maximum response size for the platform HTTP transport.
pub const DEFAULT_HTTP_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// Preferred transport before authenticated endpoint negotiation.
#[cfg(not(target_arch = "wasm32"))]
pub const DEFAULT_TRANSPORT: &str = "grpc";
/// Preferred transport before authenticated endpoint negotiation.
#[cfg(target_arch = "wasm32")]
pub const DEFAULT_TRANSPORT: &str = "http";

/// Configuration, transport, or canonical service error on any platform.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Endpoint or credential configuration failed.
    #[error("client configuration failed: {0}")]
    Configuration(String),
    /// Network transport failed.
    #[error("transport failed: {0}")]
    Transport(String),
    /// The remote service rejected the operation.
    #[error("remote service rejected the operation: {message}")]
    Service {
        /// Remote error message.
        message: String,
        /// gRPC status code when provided by the selected transport.
        grpc_code: Option<i32>,
        /// HTTP status code when provided by the selected transport.
        http_status: Option<u16>,
        /// Canonical service error detail.
        detail: Option<crate::wire::Error>,
    },
    /// The request could not be encoded or routed.
    #[error("invalid request")]
    InvalidArgument,
    /// The response exceeded the configured limit.
    #[error("response exceeds configured bound")]
    ResponseTooLarge,
    /// The response was not valid canonical data.
    #[error("malformed response")]
    MalformedResponse,
}

impl Error {
    #[cfg(not(target_arch = "wasm32"))]
    fn from_grpc(status: tonic::Status) -> Self {
        Self::Service {
            detail: crate::grpc::error_detail(&status),
            message: status.message().to_owned(),
            grpc_code: Some(status.code() as i32),
            http_status: None,
        }
    }

    fn from_http(error: crate::http::Error) -> Self {
        match error {
            crate::http::Error::InvalidArgument => Self::InvalidArgument,
            crate::http::Error::Transport(error) => Self::Transport(error.to_string()),
            crate::http::Error::ResponseTooLarge => Self::ResponseTooLarge,
            crate::http::Error::MalformedResponse => Self::MalformedResponse,
            crate::http::Error::Service { status, detail } => Self::Service {
                message: format!("HTTP status {status}"),
                grpc_code: None,
                http_status: Some(status),
                detail,
            },
        }
    }
}

/// Configuration error returned by [`connect`].
pub type ConnectError = Error;

/// Authenticated client with identical operations and return types on all platforms.
#[derive(Clone)]
pub struct Client {
    inner: Backend,
}

#[derive(Clone)]
enum Backend {
    #[cfg(not(target_arch = "wasm32"))]
    Grpc(crate::grpc::Client),
    Http(crate::http::Client),
}

impl Client {
    /// The transport selected after a verified endpoint handshake.
    pub const fn transport(&self) -> &'static str {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(_) => "grpc",
            Backend::Http(_) => "http",
        }
    }
}

/// Connect using the best compatible transport before sending an application call.
///
/// # Errors
/// Rejects invalid credentials, endpoint identity, or unsupported capabilities.
pub async fn connect(
    endpoint: impl AsRef<str>,
    token: impl AsRef<str>,
) -> Result<Client, ConnectError> {
    let endpoint = endpoint.as_ref();
    let token = token.as_ref();
    #[cfg(not(target_arch = "wasm32"))]
    if endpoint.starts_with("https://") {
        match crate::grpc::connect_verified(endpoint, token).await {
            Ok(Some(inner)) => {
                return Ok(Client {
                    inner: Backend::Grpc(inner),
                });
            }
            Ok(None) | Err(crate::grpc::ConnectError::Transport(_)) => {}
            Err(error) => return Err(Error::Configuration(error.to_string())),
        }
    }
    let inner = crate::http::Client::new(endpoint, token, DEFAULT_HTTP_RESPONSE_BYTES)
        .map_err(Error::from_http)?;
    if !inner.verify_handshake().await.map_err(Error::from_http)? {
        return Err(Error::Configuration(
            "endpoint has no compatible actors transport".into(),
        ));
    }
    Ok(Client {
        inner: Backend::Http(inner),
    })
}

include!("generated/platform-client-methods.rs");
