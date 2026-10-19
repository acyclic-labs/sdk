use super::*;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tonic::transport::{ClientTlsConfig, Endpoint};

impl HttpStream {
    /// Receives canonical ALL/LATEST data on the same session as ordinary follows.
    pub async fn follow_data(
        &self,
        follow: wire::SessionFollow,
        from: u64,
    ) -> Result<
        futures::stream::BoxStream<'static, Result<wire::SessionData, StreamError>>,
        StreamError,
    > {
        self.session_client()?.follow_data(follow, from).await
    }
    pub(super) fn session_client(&self) -> Result<crate::grpc::Client, StreamError> {
        shared::shared(&self.sessions, || {
            let channel = self.session_endpoint.connect_lazy();
            let client = if let Some(credential) = &self.credential {
                crate::grpc::Client::from_channel_with_credential(channel, credential.clone())
            } else {
                let authorization = self
                    .authorization
                    .as_ref()
                    .ok_or(StreamError::AccessDenied)?
                    .to_str()
                    .map_err(|_| StreamError::Unavailable)?
                    .parse()
                    .map_err(|_| StreamError::Unavailable)?;
                crate::grpc::Client::from_channel(channel, authorization)
            };
            Ok(client
                .with_session_verifier(Arc::clone(&self.session_verifier))
                .with_operation_claims(Arc::clone(&self.operation_claims)))
        })
    }
}

pub(super) fn session_endpoint(
    endpoint: &Url,
    ca: Option<&[u8]>,
) -> Result<
    (
        Endpoint,
        Arc<dyn rustls::client::danger::ServerCertVerifier>,
    ),
    ConnectError,
> {
    let roots = ca
        .map(CertificateDer::pem_slice_iter)
        .into_iter()
        .flatten()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ConnectError)?;
    let verifier = Arc::new(
        rustls_platform_verifier::Verifier::new_with_extra_roots(
            roots,
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .map_err(|_| ConnectError)?,
    );
    let mut transport = Endpoint::from_shared(endpoint.to_string())
        .map_err(|_| ConnectError)?
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(1));
    if endpoint.scheme() == "https" {
        transport = transport
            .tls_config_with_verifier(ClientTlsConfig::new(), verifier.clone())
            .map_err(|_| ConnectError)?;
    }
    Ok((transport, verifier))
}
