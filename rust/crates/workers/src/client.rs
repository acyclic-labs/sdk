//! Platform-default Workers client.
//!
//! Native targets use authenticated gRPC and browser targets use the bounded
//! Protobuf-JSON client. Explicit [`crate::grpc`] and [`crate::http`] modules
//! remain available when a caller needs to pin a transport.

/// The response bound used by the browser/default HTTP client.
pub const DEFAULT_HTTP_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// The transport selected by [`connect`].
#[cfg(not(target_arch = "wasm32"))]
pub const DEFAULT_TRANSPORT: &str = "grpc";
/// The transport selected by [`connect`].
#[cfg(target_arch = "wasm32")]
pub const DEFAULT_TRANSPORT: &str = "http";

/// The platform-default authenticated client.
#[cfg(not(target_arch = "wasm32"))]
pub type Client = crate::grpc::Client;
/// The platform-default authenticated client.
#[cfg(target_arch = "wasm32")]
pub type Client = crate::http::Client;

/// Error returned while creating the platform-default client.
#[cfg(not(target_arch = "wasm32"))]
pub type ConnectError = crate::grpc::ConnectError;
/// Error returned while creating the platform-default client.
#[cfg(target_arch = "wasm32")]
pub type ConnectError = crate::http::Error;

/// Connect to a Workers service using the platform's default transport.
///
/// Native builds select authenticated gRPC. Browser builds select bounded
/// Protobuf-JSON over HTTP and use [`DEFAULT_HTTP_RESPONSE_BYTES`]. Consumers
/// do not need feature flags or platform checks.
pub async fn connect(
    endpoint: impl AsRef<str>,
    token: impl AsRef<str>,
) -> Result<Client, ConnectError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::grpc::connect(endpoint.as_ref(), token.as_ref()).await
    }

    #[cfg(target_arch = "wasm32")]
    {
        crate::http::Client::new(
            endpoint.as_ref(),
            token.as_ref(),
            DEFAULT_HTTP_RESPONSE_BYTES,
        )
    }
}

#[cfg(test)]
mod tests {
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
