//! Authenticated client with one API across native and browser platforms.

/// Maximum response size for the platform HTTP transport.
pub const DEFAULT_HTTP_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// The automatically selected transport.
#[cfg(not(target_arch = "wasm32"))]
pub const DEFAULT_TRANSPORT: &str = "grpc";
/// The automatically selected transport.
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

    #[cfg(target_arch = "wasm32")]
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
    #[cfg(not(target_arch = "wasm32"))]
    inner: crate::grpc::Client,
    #[cfg(target_arch = "wasm32")]
    inner: crate::http::Client,
}

/// Connect using the platform's best available transport.
///
/// # Errors
/// Rejects invalid configuration or a failed connection.
pub async fn connect(
    endpoint: impl AsRef<str>,
    token: impl AsRef<str>,
) -> Result<Client, ConnectError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::grpc::connect(endpoint.as_ref(), token.as_ref())
            .await
            .map(|inner| Client { inner })
            .map_err(|error| Error::Configuration(error.to_string()))
    }
    #[cfg(target_arch = "wasm32")]
    {
        crate::http::Client::new(
            endpoint.as_ref(),
            token.as_ref(),
            DEFAULT_HTTP_RESPONSE_BYTES,
        )
        .map(|inner| Client { inner })
        .map_err(Error::from_http)
    }
}

include!("generated/platform-client-methods.rs");
