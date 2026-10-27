//! [`crate::server::serve`]'s session service over this harness's agent tree,
//! admitted under its own owner scope as a managed host admits its verified
//! caller, served over gRPC to every client holding the one credential minted
//! for its root.
use super::{LocalHarness, storage};
use crate::server::{Endpoint, IDENTITY, TlsIdentity};
use crate::{Error, Result};
use acyclic_stream::wire::stream_session_service_server::StreamSessionServiceServer;
use base64::Engine as _;
use std::sync::Arc;

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

impl LocalHarness {
    /// Serves this served harness's agent tree on `listener` until
    /// `shutdown`, over TLS with `tls`, else plain HTTP.
    pub(crate) async fn serve_endpoint(
        self: &Arc<Self>,
        listener: tokio::net::TcpListener,
        tls: Option<TlsIdentity>,
        ready: impl FnOnce(&Endpoint),
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        let address = listener.local_addr().map_err(failed)?;
        self.agents().await?;
        let hosted = self
            .agents
            .get()
            .ok_or_else(|| Error::Conflict("agents are not hosted".into()))?;
        let server = Arc::clone(hosted.server());
        let stream = self
            .agent_stream
            .clone()
            .ok_or_else(|| Error::Unsupported("an embedded harness has no agent stream".into()))?;
        let bound = server.bound();
        let observation = bound
            .client_credential(server.root(), server.resource(), Arc::clone(&self.clock))
            .await
            .map_err(storage)?;
        let changes = bound.changes().map_err(storage)?;
        let service = acyclic_stream::session::Service::new(
            stream,
            observation.authority,
            "self-hosted".into(),
            acyclic_stream::session::Limits::default(),
        )
        .with_products(Some(Arc::new(changes)), server.clone());
        let mut builder = tonic::transport::Server::builder();
        let scheme = if let Some(tls) = tls {
            builder = builder
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(
                    tonic::transport::Identity::from_pem(tls.certificate, tls.key),
                ))
                .map_err(failed)?;
            "https"
        } else {
            "http"
        };
        ready(&Endpoint {
            server: IDENTITY,
            endpoint: format!("{scheme}://{address}"),
            credential: base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(&observation.credential),
            principal: observation.principal,
        });
        builder
            .add_service(StreamSessionServiceServer::new(service))
            .serve_with_incoming_shutdown(
                tonic::transport::server::TcpIncoming::from(listener),
                shutdown,
            )
            .await
            .map_err(failed)
    }
}
