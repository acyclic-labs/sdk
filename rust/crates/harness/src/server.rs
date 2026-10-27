//! The harness server: the one entry point that serves an agent tree to
//! remote clients, for a managed host and a self-deployed one alike. It
//! hosts the tree through the same [`crate::hosting::Server`] (admission,
//! routing, binding publication, approvals and spawn) and speaks the same
//! stream session protocol over gRPC; a client reaches either with
//! [`crate::local::Connection::connect`], the endpoint being its only
//! difference. A host supplies only what [`ServerConfig`] names.
use crate::{Error, LocalHarness, LocalHarnessConfig, Result};
use std::sync::Arc;

/// This server's identity, announced in every [`Endpoint`]: a client or a
/// test checks that an endpoint is served by this entry point.
pub const IDENTITY: &str = concat!("acyclic-harness/", env!("CARGO_PKG_VERSION"));

/// The server's TLS identity: its PEM certificate chain and private key,
/// and the URL clients reach it at, which the certificate names.
pub struct TlsIdentity {
    /// The `https` URL a remote client connects to: the server's public
    /// name, not the address it listens on (`0.0.0.0` reaches no one).
    pub url: String,
    /// PEM certificate chain.
    pub certificate: Vec<u8>,
    /// PEM private key.
    pub key: Vec<u8>,
}

/// Everything a host supplies to [`serve`].
pub struct ServerConfig {
    /// The harness the tree runs on: its storage, model, preset (prompt,
    /// tools, limits, approval policy) and settings.
    pub harness: LocalHarnessConfig,
    /// The TLS identity; without one only a loopback address is served,
    /// over plain HTTP.
    pub tls: Option<TlsIdentity>,
}

/// What a client needs to reach a running server.
#[derive(Debug, serde::Serialize)]
pub struct Endpoint {
    /// The server's [`IDENTITY`].
    pub server: &'static str,
    /// The URL a client connects to.
    pub endpoint: String,
    /// The endpoint credential (`ACYCLIC_API_KEY`): the credential minted for
    /// the tree's root, base64url without padding.
    pub credential: String,
    /// The principal the credential authenticates, which an approval policy
    /// names to route decisions to its holder.
    pub principal: String,
}

/// Opens `config`'s harness and serves its agent tree on `listener` until
/// `shutdown`. `ready` receives the endpoint once it accepts connections.
///
/// # Errors
/// When the harness cannot be opened or hosted, a non-loopback address has
/// no TLS identity, or the listener fails.
pub async fn serve(
    listener: tokio::net::TcpListener,
    config: ServerConfig,
    ready: impl FnOnce(&Endpoint),
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let address = listener
        .local_addr()
        .map_err(|error| Error::Storage(error.to_string()))?;
    if config.tls.is_none() && !address.ip().is_loopback() {
        return Err(Error::Invalid(
            "plain HTTP serves only a loopback address; give a TLS identity".into(),
        ));
    }
    let harness = Arc::new(LocalHarness::open(config.harness.served()).await?);
    harness
        .serve_endpoint(listener, config.tls, ready, shutdown)
        .await
}
