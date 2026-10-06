//! Authenticated client for the generated Actors v1 service.

use crate::wire;
use acyclic_sdk_contract_wire::{BEARER_NO_CRLF, credential};
use prost::Message;
use tonic::{
    Code, Request, Status,
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

/// Bearer metadata applied to every generated RPC.
#[derive(Clone)]
pub struct BearerAuth(MetadataValue<Ascii>);

impl Interceptor for BearerAuth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}

/// Generated client with account authentication on every request.
pub type Client = wire::actors_service_client::ActorsServiceClient<
    tonic::service::interceptor::InterceptedService<Channel, BearerAuth>,
>;

/// Client configuration error.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The authenticated control service rejected the handshake.
    #[error("Actors control handshake rejected: {0}")]
    RemoteStatus(Status),
    /// The endpoint did not prove the required Rust-owned contract identity.
    #[error("Actors control identity mismatch: {0}")]
    Negotiation(String),
    /// Endpoint must use authenticated TLS.
    #[error("Actors endpoints must use https")]
    InsecureEndpoint,
    /// Credential must be valid nonempty HTTP metadata.
    #[error("invalid Actors bearer credential")]
    InvalidCredential,
    /// Private CA must contain between one byte and 64 KiB.
    #[error("invalid Actors private CA")]
    InvalidCaCertificate,
    /// URI or TLS connection failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Connect using standard TLS roots and account-bound bearer metadata.
///
/// # Errors
/// Returns an error for invalid configuration or an unavailable TLS endpoint.
pub async fn connect(endpoint: &str, token: &str) -> Result<Client, ConnectError> {
    connect_with_ca_certificate(endpoint, token, None).await
}

/// Connect with an optional additional private CA, scoped to this connection.
///
/// # Errors
/// Returns an error for invalid configuration, CA bytes, or TLS connection.
pub async fn connect_with_ca_certificate(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    let (channel, authorization) = connect_channel_with_ca_certificate(endpoint, token, ca).await?;
    Ok(
        wire::actors_service_client::ActorsServiceClient::with_interceptor(
            channel,
            BearerAuth(authorization),
        )
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(16 * 1024 * 1024),
    )
}

/// Connect after proving the independent Rust-owned Actors identity.
///
/// An absent or unavailable control service returns `None`; authentication,
/// identity, and protocol failures remain terminal so they cannot silently
/// switch the caller to another wire contract.
pub async fn connect_verified(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Option<Client>, ConnectError> {
    use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
    use crate::control_wire::protocol::v1::{Capability, CapabilitySet, HandshakeRequest, ProtocolIdentity};

    let (channel, authorization) = connect_channel_with_ca_certificate(endpoint, token, ca).await?;
    let family = BindingFamily::Actors;
    let version = control::control_protocol_version(family);
    let mut probe = crate::control_wire::transport::v1::protocol_service_client::ProtocolServiceClient::new(channel.clone())
        .max_decoding_message_size(control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES)
        .max_encoding_message_size(control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES);
    let mut request = Request::new(HandshakeRequest {
        protocol: Some(ProtocolIdentity {
            version: version.into(),
            descriptor_digest: control::archived_descriptor_digest(family),
        }),
        required: Some(CapabilitySet {
            capabilities: vec![Capability {
                name: family.name().into(),
                version: version.into(),
            }],
        }),
    });
    request.metadata_mut().insert("authorization", authorization.clone());
    request.metadata_mut().insert(
        control::FAMILY_METADATA_KEY,
        MetadataValue::from_static(family.name()),
    );
    request.set_timeout(std::time::Duration::from_secs(10));
    let response = match probe.handshake(request).await {
        Ok(response) => response.into_inner(),
        Err(status) if matches!(status.code(), Code::Unimplemented | Code::Unavailable | Code::DeadlineExceeded) => {
            return Ok(None);
        }
        Err(status) => return Err(ConnectError::RemoteStatus(status)),
    };
    control::validate_handshake_response(
        family,
        version,
        &[control::RequiredCapability {
            name: family.name(),
            version,
        }],
        &response.encode_to_vec(),
        control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES,
    )
    .map_err(|error| ConnectError::Negotiation(format!("{error:?}")))?;
    Ok(Some(
        wire::actors_service_client::ActorsServiceClient::with_interceptor(
            channel,
            BearerAuth(authorization),
        )
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(16 * 1024 * 1024),
    ))
}

async fn connect_channel_with_ca_certificate(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<(Channel, MetadataValue<Ascii>), ConnectError> {
    let valid_endpoint = reqwest::Url::parse(endpoint).is_ok_and(|url| {
        url.scheme() == "https"
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    });
    if !valid_endpoint {
        return Err(ConnectError::InsecureEndpoint);
    }
    if !credential::validate(BEARER_NO_CRLF, token) {
        return Err(ConnectError::InvalidCredential);
    }
    let mut authorization: MetadataValue<Ascii> = format!("Bearer {token}")
        .parse()
        .map_err(|_| ConnectError::InvalidCredential)?;
    authorization.set_sensitive(true);
    let mut tls = ClientTlsConfig::new().with_webpki_roots();
    if let Some(ca) = ca {
        if ca.is_empty() || ca.len() > 64 * 1024 {
            return Err(ConnectError::InvalidCaCertificate);
        }
        tls = tls.ca_certificate(Certificate::from_pem(ca));
    }
    let channel = Endpoint::from_shared(endpoint.to_owned())?
        .tls_config(tls)?
        .connect()
        .await?;
    Ok((channel, authorization))
}

/// Decode the canonical semantic error carried in gRPC status details.
#[must_use]
pub fn error_detail(status: &Status) -> Option<wire::Error> {
    let detail = wire::Error::decode(status.details()).ok()?;
    (wire::ErrorCode::try_from(detail.code).ok()? != wire::ErrorCode::Unspecified).then_some(detail)
}
