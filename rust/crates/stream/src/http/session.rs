use super::*;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tonic::transport::{ClientTlsConfig, Endpoint};

impl HttpStream {
    pub(super) fn session_client(&self) -> Result<crate::grpc::Client, StreamError> {
        shared::shared(&self.sessions, || {
            let authorization = self
                .authorization
                .to_str()
                .map_err(|_| StreamError::Unavailable)?
                .parse()
                .map_err(|_| StreamError::Unavailable)?;
            let client = crate::grpc::Client::from_channel(
                self.session_endpoint.connect_lazy(),
                authorization,
            );
            Ok(client.with_session_verifier(Arc::clone(&self.session_verifier)))
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
