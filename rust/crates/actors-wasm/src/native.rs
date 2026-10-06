//! Native Actors transport wrapper.
//!
//! Native connections deliberately delegate to the canonical Actors client so
//! endpoint, credential, TLS, and error handling stay identical for every
//! Rust-owned facade.

pub use acyclic_actors::client::{Client, ConnectError};

/// Connect using the canonical authenticated native Actors client.
///
/// # Errors
/// Returns an error for invalid configuration or an unavailable TLS endpoint.
pub async fn connect(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    acyclic_actors::client::connect_with_ca_certificate(endpoint, token, ca).await
}
