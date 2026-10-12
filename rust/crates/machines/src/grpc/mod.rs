use super::*;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint as TonicEndpoint, Identity,
};
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const WATCH_RECONCILE_INTERVAL: Duration = Duration::from_secs(60);
const OBSERVATION_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_CONSECUTIVE_UNKNOWN_INSPECTIONS: u8 = 3;
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Remote mutual-TLS identity. Key material is borrowed and never retained by the client.
pub struct Tls<'a> {
    pub ca: &'a [u8],
    pub certificate: &'a [u8],
    pub private_key: &'a [u8],
}

/// Opaque account credential metadata shared by all generated requests.
#[derive(Clone, Default)]
struct BearerAuth(Option<MetadataValue<Ascii>>);

impl BearerAuth {
    fn new(token: &str) -> Result<Self, ProviderError> {
        if token.trim().is_empty() {
            return Err(ProviderError::Invalid(
                "invalid Machines bearer credential".into(),
            ));
        }
        let mut value: MetadataValue<Ascii> = format!("Bearer {token}")
            .parse()
            .map_err(|_| ProviderError::Invalid("invalid Machines bearer credential".into()))?;
        value.set_sensitive(true);
        Ok(Self(Some(value)))
    }
}

impl Interceptor for BearerAuth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = &self.0 {
            request
                .metadata_mut()
                .insert("authorization", value.clone());
        }
        Ok(request)
    }
}

type Client =
    wire::machines_service_client::MachinesServiceClient<InterceptedService<Channel, BearerAuth>>;

/// Authenticated remote Machines provider.
#[derive(Clone)]
pub struct GrpcProvider {
    client: Client,
}

#[cfg(target_os = "linux")]
fn owner_private_socket(
    path: &std::path::Path,
    expected_uid: u32,
) -> std::io::Result<std::fs::Metadata> {
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != expected_uid
        || metadata.mode() & 0o077 != 0
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Machines socket is not an owner-private socket",
        ));
    }
    Ok(metadata)
}

impl GrpcProvider {
    /// Connects with mutual TLS and an optional account bearer.
    pub async fn connect(
        uri: &str,
        tls: Tls<'_>,
        token: Option<&str>,
    ) -> Result<Self, ProviderError> {
        let auth = token.map(BearerAuth::new).transpose()?.unwrap_or_default();
        if !uri.starts_with("https://") {
            return Err(ProviderError::Invalid(
                "remote Machines endpoint must use https".into(),
            ));
        }
        let endpoint = TonicEndpoint::from_shared(uri.to_owned())
            .map_err(|_| ProviderError::Invalid("invalid Machines endpoint".into()))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(tls.ca))
                    .identity(Identity::from_pem(tls.certificate, tls.private_key)),
            )
            .map_err(|_| ProviderError::Invalid("invalid Machines TLS identity".into()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        Ok(Self::grpc_with_auth(channel, auth))
    }

    /// Connects to an owner-confined Linux socket with an optional account bearer.
    #[cfg(target_os = "linux")]
    pub async fn connect_local(
        path: &std::path::Path,
        token: Option<&str>,
    ) -> Result<Self, ProviderError> {
        use std::os::unix::fs::MetadataExt as _;
        use tokio::net::UnixStream;
        use tower::service_fn;

        let auth = token.map(BearerAuth::new).transpose()?.unwrap_or_default();
        if !path.is_absolute() {
            return Err(ProviderError::Invalid(
                "local Machines socket must be absolute".into(),
            ));
        }
        let path = path.to_path_buf();
        let expected_uid = rustix::process::geteuid().as_raw();
        let channel = TonicEndpoint::from_static("http://[::]:50051")
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_with_connector(service_fn(move |_| {
                let path = path.clone();
                async move {
                    let before = owner_private_socket(&path, expected_uid)?;
                    let stream = UnixStream::connect(&path).await?;
                    let peer = stream.peer_cred()?;
                    let after = owner_private_socket(&path, expected_uid)?;
                    if peer.uid() != expected_uid
                        || after.dev() != before.dev()
                        || after.ino() != before.ino()
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "Machines socket identity changed or peer is foreign",
                        ));
                    }
                    Ok(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        Ok(Self::grpc_with_auth(channel, auth))
    }

    /// Reports that owner-confined local sockets require Linux.
    #[cfg(not(target_os = "linux"))]
    pub async fn connect_local(
        _path: &std::path::Path,
        token: Option<&str>,
    ) -> Result<Self, ProviderError> {
        token.map(BearerAuth::new).transpose()?;
        Err(ProviderError::Unsupported(
            "local Machines sockets require Linux".into(),
        ))
    }

    fn grpc_with_auth(channel: Channel, auth: BearerAuth) -> Self {
        let client =
            wire::machines_service_client::MachinesServiceClient::with_interceptor(channel, auth)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Self { client }
    }
}

mod codec;
mod provider;
mod recovery;

use codec::*;
use recovery::*;

#[cfg(test)]
mod tests;
