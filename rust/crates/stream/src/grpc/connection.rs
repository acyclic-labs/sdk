use super::*;

impl Client {
    /// Connects eagerly through TLS and HTTP/2, retaining the resulting channel for later operations.
    pub async fn connect_eager(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, ConnectError> {
        Self::connect_eager_tls(endpoint, bearer_token, None).await
    }

    /// Eagerly connects trusting exclusively the caller-pinned private CA.
    pub async fn connect_eager_with_ca_certificate(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectError> {
        Self::connect_eager_tls(endpoint, bearer_token, Some(certificate_pem.as_ref())).await
    }

    /// Builds a TLS endpoint pool, optionally trusting an exclusive private CA.
    pub async fn connect<I, S>(
        endpoints: I,
        bearer_token: impl AsRef<str>,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let certificate_pem = certificate_pem.map(bounded_ca_certificate).transpose()?;
        let channels = Self::endpoints_with_tls(endpoints, certificate_pem)?
            .into_iter()
            .map(|endpoint| endpoint.connect_lazy())
            .collect::<Vec<_>>()
            .into();
        let authorization = Self::authorization(bearer_token.as_ref())?;
        Ok(Self::from_channels(channels, authorization))
    }

    pub(super) async fn connect_eager_tls(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Self, ConnectError> {
        let authorization = Self::authorization(bearer_token.as_ref())?;
        let certificate_pem = certificate_pem.map(bounded_ca_certificate).transpose()?;
        let endpoint = Self::endpoints_with_tls([endpoint], certificate_pem)?
            .pop()
            .expect("one endpoint was configured");
        let channel = endpoint.connect().await?;
        Ok(Self::from_channels(vec![channel].into(), authorization))
    }

    pub(super) fn endpoints_with_tls<I, S>(
        endpoints: I,
        certificate_pem: Option<&[u8]>,
    ) -> Result<Vec<Endpoint>, ConnectError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let verifier = if certificate_pem.is_none() {
            Some(Arc::new(
                rustls_platform_verifier::Verifier::new(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .map_err(ConnectError::PlatformVerifier)?,
            ))
        } else {
            None
        };
        let mut configured = Vec::new();
        for endpoint in endpoints.into_iter().take(MAX_ENDPOINTS + 1) {
            if configured.len() == MAX_ENDPOINTS {
                return Err(ConnectError::EndpointLimit);
            }
            let endpoint = endpoint.as_ref();
            if endpoint.len() > MAX_ENDPOINT_URI_BYTES {
                return Err(ConnectError::EndpointLimit);
            }
            let endpoint = Endpoint::from_shared(endpoint.to_owned())?
                .connect_timeout(OPERATION_ENDPOINT_ATTEMPT_TIMEOUT);
            if endpoint.uri().scheme_str() != Some("https") {
                return Err(ConnectError::InsecureEndpoint);
            }
            configured.push(if let Some(verifier) = &verifier {
                endpoint.tls_config_with_verifier(ClientTlsConfig::new(), verifier.clone())?
            } else {
                endpoint.tls_config(
                    ClientTlsConfig::new()
                        .ca_certificates(certificate_pem.map(Certificate::from_pem)),
                )?
            });
        }
        if configured.is_empty() {
            return Err(ConnectError::NoEndpoints);
        }
        Ok(configured)
    }

    pub(super) fn authorization(bearer_token: &str) -> Result<MetadataValue<Ascii>, ConnectError> {
        if bearer_token.trim().is_empty()
            || bearer_token.len() > MAX_BEARER_TOKEN_BYTES
            || bearer_token
                .bytes()
                .any(|byte| matches!(byte, b'\r' | b'\n'))
        {
            return Err(ConnectError::InvalidCredential);
        }
        let mut authorization = format!("Bearer {bearer_token}")
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_| ConnectError::InvalidCredential)?;
        authorization.set_sensitive(true);
        Ok(authorization)
    }

    pub(super) fn from_channels(
        channels: Arc<[Channel]>,
        authorization: MetadataValue<Ascii>,
    ) -> Self {
        Self {
            channels,
            authorization,
            preferred: Arc::new(AtomicUsize::new(0)),
            acknowledge_deliveries: true,
        }
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "callers derive index via `(start + offset) % channels.len()` against this same slice, so it is always in-bounds"
    )]
    pub(super) fn service(
        channels: &[Channel],
        index: usize,
    ) -> wire::stream_service_client::StreamServiceClient<Channel> {
        wire::stream_service_client::StreamServiceClient::new(channels[index].clone())
    }

    pub(super) fn request<T>(&self, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
    }
}
