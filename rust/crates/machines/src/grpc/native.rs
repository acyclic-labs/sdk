use super::*;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint as TonicEndpoint, Identity,
};
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
};

const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Remote mutual-TLS identity. Key material is borrowed and never retained by the client.
pub struct Tls<'a> {
    pub ca: &'a [u8],
    pub certificate: &'a [u8],
    pub private_key: &'a [u8],
}

/// Opaque account credential metadata shared by all generated requests.
#[derive(Clone, Default)]
pub struct BearerAuth(pub(super) Option<MetadataValue<Ascii>>);

impl BearerAuth {
    pub(super) fn new(token: &str) -> Result<Self, ProviderError> {
        if !acyclic_auth::credential::valid_credential(token) {
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

pub(super) type Client =
    wire::machines_service_client::MachinesServiceClient<InterceptedService<Channel, BearerAuth>>;

/// Authenticated remote Machines provider.
#[derive(Clone)]
pub struct NativeEndpoint {
    channel: Channel,
    pub(super) authorization: BearerAuth,
    credential: Option<CredentialProvider>,
}

#[cfg(target_os = "linux")]
pub(super) fn owner_private_socket(
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

impl Provider<NativeEndpoint> {
    /// Resolve the current callback for each physical RPC, including watch reopenings.
    pub fn with_credential_provider(mut self, credential: CredentialProvider) -> Self {
        self.endpoint.authorization = BearerAuth::default();
        self.endpoint.credential = Some(credential);
        self
    }

    /// Establish mutual TLS without obtaining a bearer until the first request.
    pub async fn connect_with_credential(
        uri: &str,
        tls: Tls<'_>,
        credential: CredentialProvider,
    ) -> Result<Self, ProviderError> {
        Ok(Self::connect(uri, tls, None)
            .await?
            .with_credential_provider(credential))
    }

    /// Connects to a public HTTPS service using an account credential callback.
    /// Public certificate roots are used unless an explicit private CA is added.
    /// No client certificate or credential is sent into a guest.
    pub async fn connect_service(
        uri: &str,
        credential: CredentialProvider,
        ca: Option<&[u8]>,
    ) -> Result<Self, ProviderError> {
        let mut tls = ClientTlsConfig::new().with_webpki_roots();
        if let Some(ca) = ca {
            tls = tls.ca_certificate(Certificate::from_pem(ca));
        }
        Ok(Self::connect_authenticated(uri, tls, None)
            .await?
            .with_credential_provider(credential))
    }

    /// Connects with mutual TLS and an optional account bearer.
    pub async fn connect(
        uri: &str,
        tls: Tls<'_>,
        token: Option<&str>,
    ) -> Result<Self, ProviderError> {
        Self::connect_authenticated(
            uri,
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(tls.ca))
                .identity(Identity::from_pem(tls.certificate, tls.private_key)),
            token,
        )
        .await
    }

    async fn connect_authenticated(
        uri: &str,
        tls: ClientTlsConfig,
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
            .tls_config(tls)
            .map_err(|_| ProviderError::Invalid("invalid Machines TLS identity".into()))?;
        if endpoint
            .uri()
            .authority()
            .is_none_or(|value| value.as_str().contains('@'))
            || endpoint.uri().query().is_some()
        {
            return Err(ProviderError::Invalid("Machines endpoint admission failed; recovery: use HTTPS without embedded credentials or a query".into()));
        }
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

    pub(super) fn grpc_with_auth(channel: Channel, authorization: BearerAuth) -> Self {
        Self::over(NativeEndpoint {
            channel,
            authorization,
            credential: None,
        })
    }
}

impl NativeEndpoint {
    pub(super) async fn authorization(&self) -> Result<BearerAuth, ProviderError> {
        if let Some(provider) = &self.credential {
            let token = provider.resolve().await.map_err(|_| {
                ProviderError::Rejected("Machines credential renewal failed".into())
            })?;
            BearerAuth::new(&token)
        } else {
            Ok(self.authorization.clone())
        }
    }

    async fn authenticated_client(&self) -> Result<Client, ProviderError> {
        let authorization = self.authorization().await?;
        Ok(
            wire::machines_service_client::MachinesServiceClient::with_interceptor(
                self.channel.clone(),
                authorization,
            )
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES),
        )
    }
}

impl transport::Endpoint for NativeEndpoint {
    type Client = Client;
    type Deadline = crate::provider::deadline::GuestDeadline;

    async fn client(&self) -> Result<Self::Client, ProviderError> {
        self.authenticated_client().await
    }
    fn absolute_deadline(&self, expiry: u64) -> Result<Self::Deadline, ProviderError> {
        Self::Deadline::absolute(expiry)
    }
}
