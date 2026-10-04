//! Platform-default hosted Stream client.
//!
//! Native targets select the authenticated gRPC provider. The explicit
//! [`crate::grpc`] and [`crate::http`] modules remain available for callers
//! that need to pin a transport or configure a private CA.

use std::sync::Arc;

/// The transport selected by [`connect`].
#[cfg(not(target_arch = "wasm32"))]
pub const DEFAULT_TRANSPORT: &str = "grpc";
#[cfg(target_arch = "wasm32")]
/// Browser targets use the authenticated HTTP provider.
pub const DEFAULT_TRANSPORT: &str = "http";

/// The platform-default hosted Stream client.
#[cfg(not(target_arch = "wasm32"))]
pub type Client = crate::StreamClient<crate::grpc::Client>;
#[cfg(target_arch = "wasm32")]
/// Browser Stream client backed by bounded HTTP requests.
pub type Client = crate::StreamClient<crate::http::HttpStream>;

/// Error returned while creating the platform-default client.
#[cfg(not(target_arch = "wasm32"))]
pub type ConnectError = crate::grpc::ConnectError;
#[cfg(target_arch = "wasm32")]
/// Browser HTTP connection configuration error.
pub type ConnectError = crate::http::ConnectError;

/// Connect to a hosted Stream service using the platform's default transport.
///
/// Native builds select authenticated gRPC. Browser callers use the generated
/// Rust/WASM boundary, which selects HTTP internally.
pub async fn connect(
    endpoint: impl AsRef<str>,
    token: impl AsRef<str>,
) -> Result<Client, ConnectError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        Ok(crate::StreamClient::new(Arc::new(
            crate::grpc::Client::connect(endpoint.as_ref(), token.as_ref()).await?,
        )))
    }
    #[cfg(target_arch = "wasm32")]
    {
        Ok(crate::StreamClient::new(Arc::new(
            crate::http::HttpStream::new(
                endpoint.as_ref(),
                token.as_ref(),
                DEFAULT_HTTP_RESPONSE_BYTES,
            )?,
        )))
    }
}

/// Default bound for browser HTTP responses.
pub const DEFAULT_HTTP_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[cfg(test)]
mod tests {
    #[test]
    fn default_transport_is_platform_owned() {
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(super::DEFAULT_TRANSPORT, "grpc");
        #[cfg(target_arch = "wasm32")]
        assert_eq!(super::DEFAULT_TRANSPORT, "http");
        assert!(super::DEFAULT_HTTP_RESPONSE_BYTES > 0);
    }
}
