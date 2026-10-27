//! The harness server: the one entry point that serves an agent tree to
//! remote clients, for a managed host and a self-deployed one alike. It
//! hosts the tree through the same [`crate::hosting::Server`] (admission,
//! routing, binding publication, approvals and spawn) and speaks the same
//! stream session protocol over gRPC; a client reaches either with
//! [`crate::local::Connection::connect`], the endpoint being its only
//! difference. A host supplies only what [`ServerConfig`] names.
pub use crate::local::{Custody, KeyFileVerifier, Verifier};
use crate::{Error, LocalHarnessConfig, Result};
use acyclic_fs::{AsyncObjectStore, distributed::AuthorityProvider};
use acyclic_stream::BoxProviderFuture;
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

/// Everything a host supplies to [`serve`]: the harness and its seams. A
/// managed host and a self-deployed one differ only in these values.
pub struct ServerConfig<C = LocalHarnessConfig> {
    /// The harness the tree runs on: its storage, model, preset (prompt,
    /// tools, limits, approval policy) and settings.
    pub harness: C,
    /// Verifies the tree's caller and each client's credential;
    /// [`KeyFileVerifier`] by default.
    pub verifier: Arc<dyn Verifier>,
    /// The TLS identity; without one only a loopback address is served,
    /// over plain HTTP.
    pub tls: Option<TlsIdentity>,
}

/// A host's typed storage composition, opened before any session is admitted.
/// Native filesystem publication keeps the concrete Stream provider throughout.
pub trait Composition: Send + 'static {
    /// Stream storage shared by the agents and their filesystem authority.
    type Stream: AuthorityProvider + 'static;
    /// Immutable object storage used by the same filesystem.
    type Objects: AsyncObjectStore + 'static;
    /// Keeps the host's runtime and resources alive until serving ends.
    type Owner: Send + Sync + 'static;
    /// Opens the host through the canonical admitted hosting API, its tree's
    /// caller verified by `verifier`.
    fn open(
        self,
        verifier: &Arc<dyn Verifier>,
    ) -> BoxProviderFuture<'static, Result<Hosted<Self::Stream, Self::Objects, Self::Owner>>>;
}

impl<C> ServerConfig<C> {
    /// `harness` served with the self-deployed defaults: the key-file
    /// verifier and no TLS.
    #[must_use]
    pub fn new(harness: C) -> Self {
        Self {
            harness,
            verifier: Arc::new(KeyFileVerifier),
            tls: None,
        }
    }
}

/// An already admitted tree, its clock and the owner of its running runtime.
/// Pass this as [`ServerConfig::harness`] to serve any typed storage pair through
/// the same endpoint as the default local composition.
pub struct Hosted<P: AuthorityProvider + 'static, O: 'static, L> {
    server: Arc<crate::hosting::Server<P, O>>,
    clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    resources: Option<Arc<dyn acyclic_stream::session::ResourceProvider>>,
    _owner: L,
}

impl<P, O, L> Hosted<P, O, L>
where
    P: AuthorityProvider + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Retains `owner` while serving `server`. The owner must keep this tree's
    /// runtime running and stop it when dropped; serving starts no second loop.
    /// `resources` supplies the host's supported resource follows. With `None`,
    /// resource follows fail closed; ordinary Stream follows remain available.
    #[must_use]
    pub fn new(
        server: Arc<crate::hosting::Server<P, O>>,
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
        owner: L,
        resources: Option<Arc<dyn acyclic_stream::session::ResourceProvider>>,
    ) -> Self {
        Self {
            server,
            clock,
            resources,
            _owner: owner,
        }
    }
}

impl<P, O, L> Composition for Hosted<P, O, L>
where
    P: AuthorityProvider + 'static,
    O: AsyncObjectStore + 'static,
    L: Send + Sync + 'static,
{
    type Stream = P;
    type Objects = O;
    type Owner = L;
    /// An already hosted tree's caller was admitted by its host; `verifier`
    /// verifies only its clients' credentials.
    fn open(self, _: &Arc<dyn Verifier>) -> BoxProviderFuture<'static, Result<Self>> {
        Box::pin(async move { Ok(self) })
    }
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
pub async fn serve<C: Composition>(
    listener: tokio::net::TcpListener,
    config: ServerConfig<C>,
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
    let host = config.harness.open(&config.verifier).await?;
    let server = &host.server;
    let bound = server.bound();
    let authority = config.verifier.session(&Custody {
        issuer: bound.issuer(),
        clock: &host.clock,
    })?;
    let observation = bound
        .client_credential(server.root(), server.resource(), host.clock.clone())
        .await
        .map_err(failed)?;
    let service = acyclic_stream::session::Service::new(
        bound.agents().provider().clone(),
        authority,
        "self-hosted".into(),
        acyclic_stream::session::Limits::default(),
    )
    .with_products(host.resources.clone(), server.clone());
    let mut builder = tonic::transport::Server::builder();
    let endpoint = if let Some(tls) = config.tls {
        if !tls.url.starts_with("https://") {
            return Err(Error::Invalid("a TLS server's URL is an https URL".into()));
        }
        builder = builder
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(
                tonic::transport::Identity::from_pem(tls.certificate, tls.key),
            ))
            .map_err(failed)?;
        tls.url
    } else {
        format!("http://{address}")
    };
    use base64::Engine as _;
    ready(&Endpoint {
        server: IDENTITY,
        endpoint,
        credential: base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(&observation.credential),
        principal: observation.principal,
    });
    use acyclic_stream::wire::stream_session_service_server::StreamSessionServiceServer;
    let result = builder
        .add_service(StreamSessionServiceServer::new(service))
        .serve_with_incoming_shutdown(
            tonic::transport::server::TcpIncoming::from(listener),
            shutdown,
        )
        .await
        .map_err(failed);
    drop(host);
    result
}

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
