use super::*;

impl GrpcObjects {
    /// Sets the finite deadline for streamed PUT, multipart parts and complete GET bodies.
    pub fn with_transfer_timeout(
        mut self,
        timeout: std::time::Duration,
    ) -> Result<Self, ConnectError> {
        if timeout.is_zero() || timeout > MAX_TRANSFER_TIMEOUT {
            return Err(ConnectError::InvalidConfiguration);
        }
        self.transfer_timeout = timeout;
        Ok(self)
    }

    /// Connects with optional private CA trust and PEM client certificate/key identity.
    pub async fn connect(
        endpoint: &str,
        token: &str,
        ca: Option<&[u8]>,
        identity: Option<(&[u8], &[u8])>,
    ) -> Result<Self, ConnectError> {
        let identity = identity
            .map(|(certificate, key)| {
                if certificate.is_empty()
                    || certificate.len() > super::MAX_PEM_BYTES
                    || key.is_empty()
                    || key.len() > super::MAX_PEM_BYTES
                {
                    return Err(ConnectError::InvalidConfiguration);
                }
                Ok(Identity::from_pem(certificate, key))
            })
            .transpose()?;
        let endpoint = Endpoint::from_shared(endpoint.to_owned())?;
        if endpoint.uri().scheme_str() != Some("https")
            || endpoint
                .uri()
                .authority()
                .is_none_or(|authority| authority.as_str().contains('@'))
            || endpoint.uri().query().is_some()
            || endpoint.uri().path() != "/"
            || token.trim().is_empty()
            || token.len() > super::MAX_BEARER_TOKEN_BYTES
        {
            return Err(ConnectError::InvalidConfiguration);
        }
        let mut authorization: MetadataValue<Ascii> = format!("Bearer {token}")
            .parse()
            .map_err(|_| ConnectError::InvalidConfiguration)?;
        authorization.set_sensitive(true);
        let mut tls = ClientTlsConfig::new();
        if let Some(ca) = ca {
            if ca.is_empty() || ca.len() > super::MAX_PEM_BYTES {
                return Err(ConnectError::InvalidConfiguration);
            }
            tls = tls.ca_certificate(Certificate::from_pem(ca));
        }
        if let Some(identity) = identity {
            tls = tls.identity(identity);
        }
        let endpoint = if ca.is_some() {
            endpoint.tls_config(tls)?
        } else {
            let verifier = rustls_platform_verifier::Verifier::new(std::sync::Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .map_err(ConnectError::PlatformVerifier)?;
            endpoint.tls_config_with_verifier(tls, std::sync::Arc::new(verifier))?
        };
        let channel = endpoint
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(MAX_TRANSFER_TIMEOUT)
            .connect()
            .await?;
        Ok(Self {
            channel,
            authorization,
            transfer_timeout: REQUEST_TIMEOUT,
        })
    }

    pub(super) fn authenticated<T>(&self, value: T, timeout: std::time::Duration) -> Request<T> {
        let mut request = Request::new(value);
        request.set_timeout(timeout);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
    }

    pub(super) fn buckets(&self) -> wire::buckets_service_client::BucketsServiceClient<Channel> {
        wire::buckets_service_client::BucketsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES)
    }

    pub(super) fn objects(&self) -> wire::objects_service_client::ObjectsServiceClient<Channel> {
        wire::objects_service_client::ObjectsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES)
    }

    pub(super) fn multipart(
        &self,
    ) -> wire::multipart_service_client::MultipartServiceClient<Channel> {
        wire::multipart_service_client::MultipartServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES)
    }
}
